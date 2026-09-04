//! MQTT control surface.
//!
//! Publishes each speaker's now-playing state and album art, and relays volume between
//! the Spotify/AirPlay senders and an MQTT-controlled amplifier.
//!
//! The volume topics read backwards from the usual convention, deliberately:
//! `volume_topic` is the *amplifier's* state topic, which lucyfer subscribes to, and
//! `volume_topic_set` is the amplifier's *command* topic, which lucyfer publishes to.
//! The loop is: user moves the slider in Spotify -> we publish `volume_topic_set` -> the
//! amplifier applies it and mirrors it back on `volume_topic` -> we push that value into
//! every source so both sliders agree.
//!
//! Nothing is ever published to `volume_topic_set` before a value has been *received* on
//! `volume_topic`. A speaker whose amplifier has not reported in stays silent rather than
//! commanding a default level, because guessing here means a loud noise.

use crate::config::{Config, MqttConfig, SpeakerConfig};
use crate::source::{SourceKind, SpeakerRegistry, speaker_id};
use crate::state::{Playback, SpeakerState, StateEvent, StateHub};
use anyhow::{Context, Result};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rumqttc::{AsyncClient, ClientError, Event, LastWill, MqttOptions, Packet, QoS};
use serde::Serialize;
use std::time::Duration;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::Instant;

/// Album art larger than this is dropped rather than published: base64 inflates by 4/3
/// and brokers commonly cap payloads well below a megabyte.
const MAX_ART_BYTES: usize = 1024 * 1024;

/// Volume echoes within this many percent of the value we last received are treated as
/// our own change coming back, not a user action, and are not republished.
///
/// It has to be this loose because AirPlay round-trips a level through `level * 100` as a
/// `u8`, the sender's dB scale, and back through `volume_level()` over a 30 dB range,
/// which is lossy by more than a percent. Spotify's u16 round-trip is near-exact.
const VOLUME_ECHO_TOLERANCE: f32 = 2.0;

/// Minimum spacing between two status publishes whose only difference is the playback
/// position. Position ticks arrive about once a second per source and would otherwise be
/// the bulk of the traffic.
const POSITION_ONLY_INTERVAL: Duration = Duration::from_secs(5);

const AVAILABILITY_ONLINE: &str = "online";
const AVAILABILITY_OFFLINE: &str = "offline";

// --- wire format ---

#[derive(Debug, Clone, Serialize, PartialEq)]
struct TrackPayload {
    title: String,
    artists: Vec<String>,
    album: Option<String>,
    duration_ms: u32,
    uri: String,
}

/// The published shape of a speaker. Deliberately a separate struct from
/// [`SpeakerState`]: the wire format should not shift every time the internal state grows
/// a field.
#[derive(Debug, Clone, Serialize, PartialEq)]
struct StatusPayload {
    name: String,
    /// "spotify" | "airplay" | "none"
    source: &'static str,
    playback: Playback,
    /// 0-100.
    volume: f32,
    active_user: Option<String>,
    position_ms: u32,
    shuffle: bool,
    repeat: bool,
    track: Option<TrackPayload>,
}

impl StatusPayload {
    fn from_state(state: &SpeakerState) -> Self {
        Self {
            name: state.name.clone(),
            source: state.source.map(SourceKind::label).unwrap_or("none"),
            playback: state.playback,
            volume: to_percent(state.volume),
            active_user: state.active_user.clone(),
            position_ms: state.position_ms,
            shuffle: state.shuffle,
            repeat: state.repeat,
            track: state.track.as_ref().map(|t| TrackPayload {
                title: t.name.clone(),
                artists: t.artists.clone(),
                album: t.album.clone(),
                duration_ms: t.duration_ms,
                uri: t.uri.clone(),
            }),
        }
    }

    /// True when the two differ only in playback position — the case worth rate-limiting.
    fn position_only_change(&self, other: &Self) -> bool {
        Self {
            position_ms: other.position_ms,
            ..self.clone()
        } == *other
    }
}

// --- per-speaker bookkeeping ---

struct Speaker {
    id: String,
    status_topic: String,
    albumart_topic: String,
    volume_topic: Option<String>,
    volume_topic_set: Option<String>,

    last_status: Option<StatusPayload>,
    last_status_at: Option<Instant>,
    /// Identity of the last album art published, so an unchanged cover is not re-encoded
    /// onto the wire on every status update.
    last_art_key: Option<ArtKey>,

    /// Set once a value has arrived on `volume_topic`. Until then nothing is ever
    /// published to `volume_topic_set`.
    primed: bool,
    /// Last value received on `volume_topic` (0-100), for echo suppression.
    last_incoming: f32,
    /// Last value we published to `volume_topic_set` (0-100).
    last_published: Option<f32>,
}

/// Cheap identity of a cover: the Spotify URL, or the length and a rolling hash of the
/// AirPlay bytes. Comparing this avoids base64-encoding a cover just to discover it is
/// the one already published.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ArtKey {
    Url(String),
    Bytes { len: usize, hash: u64 },
}

impl Speaker {
    fn new(cfg: &SpeakerConfig, prefix: &str) -> Self {
        let mqtt_name = cfg.mqtt_name();
        Self {
            id: speaker_id(&cfg.name),
            status_topic: format!("{prefix}/{mqtt_name}/status"),
            albumart_topic: format!("{prefix}/{mqtt_name}/albumart"),
            volume_topic: cfg.volume_topic.clone(),
            volume_topic_set: cfg.volume_topic_set.clone(),
            last_status: None,
            last_status_at: None,
            last_art_key: None,
            primed: false,
            last_incoming: f32::NAN,
            last_published: None,
        }
    }

    /// Forget what has been published so the next update republishes everything. Called
    /// after a reconnect, since the broker may have restarted and lost our retained
    /// messages. Volume priming is deliberately *not* reset: the amplifier's level is
    /// still what it was, and re-priming from scratch buys nothing.
    fn forget_published(&mut self) {
        self.last_status = None;
        self.last_status_at = None;
        self.last_art_key = None;
    }

    /// Whether a source-originated volume change should be forwarded to the amplifier.
    fn should_publish_volume(&self, percent: f32) -> bool {
        if !self.primed {
            return false;
        }
        // Our own value coming back through the source, not a user action.
        if (percent - self.last_incoming).abs() <= VOLUME_ECHO_TOLERANCE {
            return false;
        }
        match self.last_published {
            Some(last) => (percent - last).abs() > VOLUME_ECHO_TOLERANCE,
            None => true,
        }
    }
}

// --- entry point ---

/// Run the MQTT client until the process shuts down. Never returns while the broker is
/// reachable; transport errors are retried by rumqttc.
pub async fn run(cfg: &Config, hub: StateHub, registry: SpeakerRegistry) -> Result<()> {
    let mqtt = cfg
        .mqtt
        .as_ref()
        .expect("run() is only called when mqtt is configured");
    let (host, port) = mqtt.host_port()?;
    let prefix = mqtt.prefix.trim_end_matches('/').to_string();
    let availability_topic = format!("{prefix}/availability");

    let mut opts = MqttOptions::new(mqtt.client_id(), &host, port);
    opts.set_keep_alive(Duration::from_secs(30));
    // Album art data: URLs are the only large payload; give them room in both directions.
    opts.set_max_packet_size(MAX_ART_BYTES * 2, MAX_ART_BYTES * 2);
    opts.set_last_will(LastWill::new(
        &availability_topic,
        AVAILABILITY_OFFLINE,
        QoS::AtLeastOnce,
        true,
    ));
    if let Some(user) = &mqtt.username {
        opts.set_credentials(user, resolve_password(mqtt)?.unwrap_or_default());
    }

    let mut speakers: Vec<Speaker> = cfg
        .speakers
        .iter()
        .map(|sp| Speaker::new(sp, &prefix))
        .collect();

    tracing::info!(
        "connecting to MQTT broker {host}:{port} as '{}', prefix '{prefix}'",
        mqtt.client_id()
    );

    let (client, mut eventloop) = AsyncClient::new(opts, 64);
    let mut events = hub.subscribe();

    loop {
        tokio::select! {
            // Biased so incoming volume and connection handling are never starved by a
            // busy state stream.
            biased;

            packet = eventloop.poll() => match packet {
                Ok(Event::Incoming(Packet::ConnAck(_))) => {
                    tracing::info!("MQTT connected");
                    if let Err(e) = on_connected(
                        &client,
                        &availability_topic,
                        &mut speakers,
                        &hub,
                    ).await {
                        tracing::warn!("MQTT post-connect setup failed: {e}");
                    }
                }
                Ok(Event::Incoming(Packet::Publish(p))) => {
                    on_incoming(&p.topic, &p.payload, &mut speakers, &registry);
                }
                Ok(_) => {}
                Err(e) => {
                    // rumqttc reconnects on its own; this only paces the log and the
                    // retry so an unreachable broker does not spin.
                    tracing::warn!("MQTT connection error: {e}");
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
            },

            event = events.recv() => match event {
                Ok(StateEvent::SpeakerUpdate { speaker }) => {
                    let Some(sp) = speakers.iter_mut().find(|s| s.id == speaker.id) else {
                        continue;
                    };
                    publish_speaker(&client, sp, &speaker, &hub).await;
                    publish_volume(&client, sp, &speaker).await;
                }
                Err(RecvError::Lagged(n)) => {
                    // The state stream outran us. The next update carries the current
                    // truth anyway, but drop the dedup cache so nothing stale sticks.
                    tracing::warn!("MQTT publisher lagged {n} state update(s)");
                    for sp in &mut speakers {
                        sp.forget_published();
                    }
                }
                Err(RecvError::Closed) => {
                    tracing::info!("state hub closed, stopping MQTT client");
                    return Ok(());
                }
            },
        }
    }
}

fn resolve_password(mqtt: &MqttConfig) -> Result<Option<String>> {
    mqtt.resolve_password().context("resolving MQTT password")
}

/// Re-establish everything the broker forgets across a reconnect: subscriptions, the
/// availability flag, and every retained payload.
async fn on_connected(
    client: &AsyncClient,
    availability_topic: &str,
    speakers: &mut [Speaker],
    hub: &StateHub,
) -> Result<(), ClientError> {
    client
        .publish(
            availability_topic,
            QoS::AtLeastOnce,
            true,
            AVAILABILITY_ONLINE,
        )
        .await?;

    for sp in speakers.iter_mut() {
        if let Some(topic) = &sp.volume_topic {
            client.subscribe(topic.clone(), QoS::AtLeastOnce).await?;
        }
        sp.forget_published();
    }

    for state in hub.all() {
        if let Some(sp) = speakers.iter_mut().find(|s| s.id == state.id) {
            publish_speaker(client, sp, &state, hub).await;
        }
    }
    Ok(())
}

/// A value on a speaker's `volume_topic`: the amplifier telling us where it actually is.
fn on_incoming(topic: &str, payload: &[u8], speakers: &mut [Speaker], registry: &SpeakerRegistry) {
    let Some(sp) = speakers
        .iter_mut()
        .find(|s| s.volume_topic.as_deref() == Some(topic))
    else {
        return;
    };
    let Some(percent) = parse_volume(payload) else {
        tracing::warn!(
            "speaker '{}': unparseable volume on {topic}: {:?}",
            sp.id,
            String::from_utf8_lossy(payload)
        );
        return;
    };

    sp.primed = true;
    sp.last_incoming = percent;
    // Our published command has landed; stop measuring echoes against it.
    sp.last_published = None;

    if let Some(audio) = registry.get(&sp.id) {
        tracing::debug!("speaker '{}': amplifier reports volume {percent:.1}", sp.id);
        audio.set_desired_volume(percent / 100.0);
    }
}

/// Publish this speaker's status and, when the cover changed, its album art.
async fn publish_speaker(
    client: &AsyncClient,
    sp: &mut Speaker,
    state: &SpeakerState,
    hub: &StateHub,
) {
    let payload = StatusPayload::from_state(&state.extrapolated());

    let send = match (&sp.last_status, sp.last_status_at) {
        (Some(last), Some(at)) => {
            // Nothing changed, or only the position ticked and it did so recently:
            // position updates arrive about once a second and would otherwise be the bulk
            // of the traffic.
            !(payload == *last
                || (last.position_only_change(&payload) && at.elapsed() < POSITION_ONLY_INTERVAL))
        }
        _ => true,
    };

    if send {
        match serde_json::to_string(&payload) {
            Ok(json) => {
                if let Err(e) = client
                    .publish(&sp.status_topic, QoS::AtLeastOnce, true, json)
                    .await
                {
                    tracing::warn!("publishing {}: {e}", sp.status_topic);
                    return;
                }
                sp.last_status = Some(payload);
                sp.last_status_at = Some(Instant::now());
            }
            Err(e) => tracing::warn!("serializing status for '{}': {e}", sp.id),
        }
    }

    publish_albumart(client, sp, state, hub).await;
}

/// Album art as a bare URL: Spotify's CDN link verbatim, AirPlay's raw bytes as a `data:`
/// URL. An empty retained payload clears it.
async fn publish_albumart(
    client: &AsyncClient,
    sp: &mut Speaker,
    state: &SpeakerState,
    hub: &StateHub,
) {
    let art_url = state.track.as_ref().and_then(|t| t.art_url.clone());
    let artwork = hub.get_artwork(&sp.id);

    let key = match (&art_url, &artwork) {
        (Some(url), _) => Some(ArtKey::Url(url.clone())),
        (None, Some(art)) => Some(ArtKey::Bytes {
            len: art.bytes.len(),
            hash: fnv1a(&art.bytes),
        }),
        (None, None) => None,
    };
    if key == sp.last_art_key {
        return;
    }

    let payload = match (&key, artwork) {
        (Some(ArtKey::Url(url)), _) => url.clone(),
        (Some(ArtKey::Bytes { .. }), Some(art)) => {
            if art.bytes.len() > MAX_ART_BYTES {
                tracing::warn!(
                    "speaker '{}': album art is {} bytes, over the {MAX_ART_BYTES} byte limit; not publishing",
                    sp.id,
                    art.bytes.len()
                );
                return;
            }
            format!(
                "data:{};base64,{}",
                art.content_type,
                BASE64.encode(&art.bytes)
            )
        }
        // Nothing to show: clear the retained message.
        _ => String::new(),
    };

    if let Err(e) = client
        .publish(&sp.albumart_topic, QoS::AtLeastOnce, true, payload)
        .await
    {
        tracing::warn!("publishing {}: {e}", sp.albumart_topic);
        return;
    }
    sp.last_art_key = key;
}

/// Forward a user-initiated volume change to the amplifier's command topic.
async fn publish_volume(client: &AsyncClient, sp: &mut Speaker, state: &SpeakerState) {
    let Some(topic) = sp.volume_topic_set.clone() else {
        return;
    };
    let percent = to_percent(state.volume);
    if !sp.should_publish_volume(percent) {
        return;
    }

    tracing::debug!("speaker '{}': commanding amplifier to {percent:.1}", sp.id);
    // Not retained: a retained command replayed when the broker restarts is exactly the
    // unexpected volume jump this whole design avoids.
    if let Err(e) = client
        .publish(&topic, QoS::AtLeastOnce, false, format!("{percent:.1}"))
        .await
    {
        tracing::warn!("publishing {topic}: {e}");
        return;
    }
    sp.last_published = Some(percent);
}

// --- helpers ---

fn to_percent(level: f32) -> f32 {
    // One decimal is well past what any source can actually resolve, and keeps the
    // published number stable instead of jittering in the noise.
    (level.clamp(0.0, 1.0) * 1000.0).round() / 10.0
}

/// Parse a volume from an amplifier's payload: a bare number, clamped to 0-100.
fn parse_volume(payload: &[u8]) -> Option<f32> {
    let v: f32 = std::str::from_utf8(payload).ok()?.trim().parse().ok()?;
    v.is_finite().then(|| v.clamp(0.0, 100.0))
}

/// FNV-1a, only ever compared against itself to spot a changed cover.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn speaker() -> Speaker {
        Speaker {
            id: "kitchen".into(),
            status_topic: "lucyfer/kitchen/status".into(),
            albumart_topic: "lucyfer/kitchen/albumart".into(),
            volume_topic: Some("amp/volume".into()),
            volume_topic_set: Some("amp/volume/set".into()),
            last_status: None,
            last_status_at: None,
            last_art_key: None,
            primed: false,
            last_incoming: f32::NAN,
            last_published: None,
        }
    }

    #[test]
    fn volumes_parse() {
        assert_eq!(parse_volume(b"55"), Some(55.0));
        assert_eq!(parse_volume(b" 42.5 "), Some(42.5));
        // Out of range is clamped, not rejected: the amplifier is the authority.
        assert_eq!(parse_volume(b"140"), Some(100.0));
        assert_eq!(parse_volume(b"-5"), Some(0.0));

        assert_eq!(parse_volume(b""), None);
        assert_eq!(parse_volume(b"loud"), None);
        assert_eq!(parse_volume(b"NaN"), None);
        assert_eq!(parse_volume(b"{\"value\": 30}"), None);
    }

    #[test]
    fn nothing_is_published_before_the_amplifier_reports() {
        let sp = speaker();
        // This is the guard that stops a connecting Spotify session from commanding a
        // volume the amplifier never asked for.
        assert!(!sp.should_publish_volume(50.0));
    }

    #[test]
    fn our_own_value_coming_back_is_not_republished() {
        let mut sp = speaker();
        sp.primed = true;
        sp.last_incoming = 55.0;

        // Spotify echoing the level we just pushed into it.
        assert!(!sp.should_publish_volume(55.0));
        // AirPlay's lossy dB round-trip of the same level.
        assert!(!sp.should_publish_volume(56.4));
        // A real user action.
        assert!(sp.should_publish_volume(70.0));
    }

    #[test]
    fn a_command_is_not_resent_while_the_amplifier_catches_up() {
        let mut sp = speaker();
        sp.primed = true;
        sp.last_incoming = 55.0;
        sp.last_published = Some(70.0);

        assert!(!sp.should_publish_volume(70.0));
        assert!(sp.should_publish_volume(80.0));
    }

    #[test]
    fn position_only_changes_are_detected() {
        let base = StatusPayload {
            name: "Kitchen".into(),
            source: "spotify",
            playback: Playback::Playing,
            volume: 50.0,
            active_user: None,
            position_ms: 1000,
            shuffle: false,
            repeat: false,
            track: None,
        };
        let ticked = StatusPayload {
            position_ms: 2000,
            ..base.clone()
        };
        assert!(base.position_only_change(&ticked));

        let paused = StatusPayload {
            playback: Playback::Paused,
            ..ticked.clone()
        };
        assert!(!base.position_only_change(&paused));
    }

    #[test]
    fn percent_conversion_is_stable() {
        assert_eq!(to_percent(0.0), 0.0);
        assert_eq!(to_percent(1.0), 100.0);
        assert_eq!(to_percent(0.555), 55.5);
        assert_eq!(to_percent(2.0), 100.0);
    }
}

/// End-to-end tests against a real mosquitto broker. They exercise [`run`] itself — the
/// publish path, the subscribe path, and above all the guarantee that connecting never
/// commands a volume. Each skips (passing) when no `mosquitto` binary is on PATH.
#[cfg(test)]
mod broker_tests {
    use super::*;
    use crate::dante::SpeakerSink;
    use crate::source::{SpeakerAudio, SpeakerRegistry};
    use crate::state::TrackInfo;
    use std::io::Write;
    use std::process::{Child, Command, Stdio};
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::time::timeout;

    struct Broker {
        child: Child,
        port: u16,
    }

    impl Drop for Broker {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    /// A throwaway broker on a free loopback port, or `None` if mosquitto is missing.
    fn start_broker() -> Option<Broker> {
        Command::new("mosquitto").arg("-h").output().ok()?;
        // Bind :0 to have the OS pick a free port, then release it for mosquitto.
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .ok()?
            .local_addr()
            .ok()?
            .port();

        let conf = std::env::temp_dir().join(format!("lucyfer-test-{port}.conf"));
        let mut f = std::fs::File::create(&conf).ok()?;
        writeln!(f, "listener {port} 127.0.0.1").ok()?;
        writeln!(f, "allow_anonymous true").ok()?;
        drop(f);

        let child = Command::new("mosquitto")
            .arg("-c")
            .arg(&conf)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .ok()?;

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if std::net::TcpStream::connect(("127.0.0.1", port)).is_ok() {
                return Some(Broker { child, port });
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        None
    }

    fn config(port: u16) -> Config {
        serde_yaml::from_str(&format!(
            r#"
dante:
  interface: "127.0.0.1"
mqtt:
  broker: "127.0.0.1:{port}"
  prefix: "lucyfer"
speakers:
  - name: "Kitchen"
    volume_topic: "amps/kitchen/volume"
    volume_topic_set: "amps/kitchen/volume/set"
"#
        ))
        .expect("test config parses")
    }

    /// A hub and registry holding one speaker, wired the way `main` wires them.
    fn speaker_fixture() -> (StateHub, SpeakerRegistry, Arc<SpeakerAudio>) {
        let hub = StateHub::new();
        hub.register(SpeakerState::new(
            "kitchen".into(),
            "Kitchen".into(),
            vec![SourceKind::Spotify],
        ));
        let audio = Arc::new(SpeakerAudio::new(
            "kitchen".into(),
            "Kitchen".into(),
            hub.clone(),
            Arc::new(SpeakerSink::detached()),
        ));
        let registry = SpeakerRegistry::new();
        registry.insert(audio.clone());
        (hub, registry, audio)
    }

    /// A subscriber that records everything under `topic` into a shared vec.
    fn spy(port: u16, topic: &str) -> Arc<Mutex<Vec<(String, String)>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut opts = MqttOptions::new(format!("spy-{topic}"), "127.0.0.1", port);
        opts.set_keep_alive(Duration::from_secs(5));
        let (client, mut eventloop) = AsyncClient::new(opts, 32);
        let topic = topic.to_string();
        let out = seen.clone();
        tokio::spawn(async move {
            client.subscribe(&topic, QoS::AtLeastOnce).await.unwrap();
            while let Ok(event) = eventloop.poll().await {
                if let Event::Incoming(Packet::Publish(p)) = event {
                    out.lock().unwrap().push((
                        p.topic.clone(),
                        String::from_utf8_lossy(&p.payload).to_string(),
                    ));
                }
            }
        });
        seen
    }

    /// Poll `seen` until `f` accepts it, or give up after `dur`.
    async fn wait_for<T>(
        seen: &Arc<Mutex<Vec<(String, String)>>>,
        dur: Duration,
        mut f: impl FnMut(&[(String, String)]) -> Option<T>,
    ) -> Option<T> {
        timeout(dur, async {
            loop {
                if let Some(v) = f(&seen.lock().unwrap().clone()) {
                    return v;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .ok()
    }

    #[tokio::test]
    async fn publishes_status_and_album_art() {
        let Some(broker) = start_broker() else {
            eprintln!("mosquitto not available; skipping");
            return;
        };
        let (hub, registry, _audio) = speaker_fixture();
        let seen = spy(broker.port, "lucyfer/#");
        let cfg = config(broker.port);
        let task = tokio::spawn({
            let (hub, registry) = (hub.clone(), registry.clone());
            async move { run(&cfg, hub, registry).await }
        });

        hub.update("kitchen", |s| {
            s.source = Some(SourceKind::Spotify);
            s.playback = Playback::Playing;
            s.volume = 0.42;
            s.track = Some(TrackInfo {
                uri: "spotify:track:abc".into(),
                name: "Test Track".into(),
                artists: vec!["An Artist".into()],
                album: Some("An Album".into()),
                duration_ms: 213_000,
                art_url: Some("https://i.scdn.co/image/abc".into()),
            });
        });

        let status = wait_for(&seen, Duration::from_secs(5), |msgs| {
            msgs.iter()
                .find(|(t, p)| t == "lucyfer/kitchen/status" && p.contains("Test Track"))
                .map(|(_, p)| p.clone())
        })
        .await
        .expect("status published");

        let json: serde_json::Value = serde_json::from_str(&status).unwrap();
        assert_eq!(json["source"], "spotify");
        assert_eq!(json["playback"], "playing");
        assert_eq!(json["volume"], 42.0);
        assert_eq!(json["track"]["title"], "Test Track");
        assert_eq!(json["track"]["artists"][0], "An Artist");
        assert_eq!(json["track"]["album"], "An Album");
        assert_eq!(json["track"]["duration_ms"], 213_000);

        // Spotify supplies a CDN URL, which is republished verbatim.
        let art = wait_for(&seen, Duration::from_secs(5), |msgs| {
            msgs.iter()
                .find(|(t, _)| t == "lucyfer/kitchen/albumart")
                .map(|(_, p)| p.clone())
        })
        .await
        .expect("album art published");
        assert_eq!(art, "https://i.scdn.co/image/abc");

        // The bridge announces itself.
        let avail = wait_for(&seen, Duration::from_secs(5), |msgs| {
            msgs.iter()
                .find(|(t, _)| t == "lucyfer/availability")
                .map(|(_, p)| p.clone())
        })
        .await;
        assert_eq!(avail.as_deref(), Some(AVAILABILITY_ONLINE));

        task.abort();
    }

    #[tokio::test]
    async fn airplay_cover_bytes_become_a_data_url() {
        let Some(broker) = start_broker() else {
            eprintln!("mosquitto not available; skipping");
            return;
        };
        let (hub, registry, _audio) = speaker_fixture();
        let seen = spy(broker.port, "lucyfer/kitchen/albumart");
        let cfg = config(broker.port);
        let task = tokio::spawn({
            let (hub, registry) = (hub.clone(), registry.clone());
            async move { run(&cfg, hub, registry).await }
        });

        let jpeg = vec![0xFF, 0xD8, 0xFF, 0xE0, 0x11, 0x22];
        hub.set_artwork(
            "kitchen",
            crate::state::Artwork {
                bytes: jpeg.clone(),
                content_type: "image/jpeg",
            },
        );
        // AirPlay leaves `art_url` empty; the bytes are what carry the cover.
        hub.update("kitchen", |s| {
            s.source = Some(SourceKind::Airplay);
            s.playback = Playback::Playing;
        });

        let art = wait_for(&seen, Duration::from_secs(5), |msgs| {
            msgs.iter()
                .find(|(_, p)| p.starts_with("data:"))
                .map(|(_, p)| p.clone())
        })
        .await
        .expect("data URL published");
        assert_eq!(
            art,
            format!("data:image/jpeg;base64,{}", BASE64.encode(&jpeg))
        );

        task.abort();
    }

    /// The central safety property: a speaker whose amplifier has not reported in must
    /// never have a volume commanded at it, however much its source changes volume.
    #[tokio::test]
    async fn never_commands_a_volume_before_the_amplifier_reports() {
        let Some(broker) = start_broker() else {
            eprintln!("mosquitto not available; skipping");
            return;
        };
        let (hub, registry, _audio) = speaker_fixture();
        let seen = spy(broker.port, "amps/kitchen/volume/set");
        let cfg = config(broker.port);
        let task = tokio::spawn({
            let (hub, registry) = (hub.clone(), registry.clone());
            async move { run(&cfg, hub, registry).await }
        });

        // A Spotify session connecting and announcing its own volume, repeatedly.
        for level in [0.5, 0.9, 0.2] {
            hub.update("kitchen", |s| {
                s.playback = Playback::Playing;
                s.volume = level;
            });
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;

        assert!(
            seen.lock().unwrap().is_empty(),
            "commanded a volume before the amplifier reported: {:?}",
            seen.lock().unwrap()
        );
        task.abort();
    }

    /// The full relay: amplifier -> sources, then user -> amplifier, with no loop.
    #[tokio::test]
    async fn relays_volume_both_ways_without_looping() {
        let Some(broker) = start_broker() else {
            eprintln!("mosquitto not available; skipping");
            return;
        };
        let (hub, registry, audio) = speaker_fixture();
        let seen = spy(broker.port, "amps/kitchen/volume/set");
        let cfg = config(broker.port);
        let task = tokio::spawn({
            let (hub, registry) = (hub.clone(), registry.clone());
            async move { run(&cfg, hub, registry).await }
        });
        // Let the client connect and subscribe before the amplifier reports.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Inbound: the amplifier says where it is; that must reach the sources.
        let mut opts = MqttOptions::new("amp", "127.0.0.1", broker.port);
        opts.set_keep_alive(Duration::from_secs(5));
        let (amp, mut amp_loop) = AsyncClient::new(opts, 32);
        tokio::spawn(async move { while amp_loop.poll().await.is_ok() {} });
        amp.publish("amps/kitchen/volume", QoS::AtLeastOnce, true, "55")
            .await
            .unwrap();

        let applied = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(v) = audio.desired_volume() {
                    return v;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("amplifier volume reached the sources");
        assert!((applied - 0.55).abs() < 0.001, "got {applied}");

        // The source echoing that same level back must not be forwarded on.
        hub.update("kitchen", |s| s.volume = 0.55);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert!(
            seen.lock().unwrap().is_empty(),
            "echo was forwarded as a command: {:?}",
            seen.lock().unwrap()
        );

        // Outbound: the user moves the slider in the Spotify app.
        hub.update("kitchen", |s| s.volume = 0.8);
        let cmd = wait_for(&seen, Duration::from_secs(5), |msgs| {
            msgs.first().map(|(_, p)| p.clone())
        })
        .await
        .expect("user change was commanded to the amplifier");
        assert_eq!(cmd, "80.0");

        // The amplifier mirrors it back; that must not produce a second command.
        amp.publish("amps/kitchen/volume", QoS::AtLeastOnce, true, "80")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        hub.update("kitchen", |s| s.volume = 0.8);
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "the relay looped: {:?}",
            seen.lock().unwrap()
        );

        task.abort();
    }
}
