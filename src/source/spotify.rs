//! Spotify Connect source: a per-speaker zeroconf discovery loop that, on each new set
//! of credentials, tears down any prior session and builds a fresh
//! Session/Player/Spirc wired to this speaker's Dante pacing queue.

use super::{CommandResult, PushResult, SourceControl, SourceKind, SpeakerAudio, speaker_id};
use crate::audio::queue::Frame;
use crate::audio::resampler::SpeakerResampler;
use crate::config::{SpeakerConfig, SpotifyConfig};
use crate::state::{Playback, StateHub, TrackInfo, now_ms};
use anyhow::{Context, Result};
use futures_util::StreamExt;
use librespot_connect::{ConnectConfig, Spirc};
use librespot_core::config::{DeviceType, SessionConfig};
use librespot_core::{authentication::Credentials, cache::Cache, session::Session};
use librespot_discovery::Discovery;
use librespot_metadata::audio::item::{AudioItem, UniqueFields};
use librespot_playback::audio_backend::{Sink, SinkError, SinkResult};
use librespot_playback::config::{Bitrate, PlayerConfig};
use librespot_playback::convert::Converter;
use librespot_playback::decoder::AudioPacket;
use librespot_playback::mixer::softmixer::SoftMixer;
use librespot_playback::mixer::{Mixer, MixerConfig, NoOpVolume, VolumeGetter};
use librespot_playback::player::{Player, PlayerEvent, PlayerEventChannel};
use sha1::{Digest, Sha1};
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

/// librespot decodes at a fixed 44.1 kHz.
const SPOTIFY_RATE: u32 = 44_100;

// --- control surface ---

/// The API's handle on a speaker's current Spirc (if a session is connected).
pub struct SpotifyControl {
    spirc: Arc<Mutex<Option<Spirc>>>,
}

impl SpotifyControl {
    fn new(spirc: Arc<Mutex<Option<Spirc>>>) -> Self {
        Self { spirc }
    }

    fn with_spirc<F: FnOnce(&Spirc) -> Result<(), librespot_core::Error>>(
        &self,
        f: F,
    ) -> CommandResult {
        let guard = self.spirc.lock().unwrap();
        match guard.as_ref() {
            Some(spirc) => match f(spirc) {
                Ok(()) => CommandResult::Ok,
                Err(e) => CommandResult::Failed(e.to_string()),
            },
            None => CommandResult::Inactive,
        }
    }
}

impl SourceControl for SpotifyControl {
    fn kind(&self) -> SourceKind {
        SourceKind::Spotify
    }
    fn play(&self) -> CommandResult {
        self.with_spirc(|s| {
            s.activate()?;
            s.play()
        })
    }
    fn pause(&self) -> CommandResult {
        self.with_spirc(|s| s.pause())
    }
    fn play_pause(&self) -> CommandResult {
        self.with_spirc(|s| s.play_pause())
    }
    fn next(&self) -> CommandResult {
        self.with_spirc(|s| s.next())
    }
    fn previous(&self) -> CommandResult {
        self.with_spirc(|s| s.prev())
    }
    fn seek(&self, position_ms: u32) -> CommandResult {
        self.with_spirc(|s| s.set_position_ms(position_ms))
    }
    fn set_volume(&self, level: f32) -> CommandResult {
        let v = (level.clamp(0.0, 1.0) * u16::MAX as f32) as u16;
        self.with_spirc(|s| s.set_volume(v))
    }

    /// Another source took the speaker: pause, keeping the Connect session alive so the
    /// user can hand playback straight back.
    fn yield_now(&self) {
        if let CommandResult::Failed(e) = self.pause() {
            tracing::warn!("spotify yield_now: pause failed: {e}");
        }
    }
}

// --- audio sink ---

/// A librespot audio `Sink` that resamples the decoded stream and pushes it into the
/// speaker's pacing queue. Blocking on a full queue is what paces librespot.
struct DanteSink {
    resampler: SpeakerResampler,
    audio: Arc<SpeakerAudio>,
    scratch: Vec<Frame>,
}

impl DanteSink {
    fn new(out_rate: u32, audio: Arc<SpeakerAudio>) -> Self {
        let resampler =
            SpeakerResampler::new(SPOTIFY_RATE, out_rate).unwrap_or(SpeakerResampler::Bypass);
        Self {
            resampler,
            audio,
            scratch: Vec::with_capacity(4096),
        }
    }
}

impl Sink for DanteSink {
    fn start(&mut self) -> SinkResult<()> {
        self.resampler.reset();
        Ok(())
    }

    fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }

    fn write(&mut self, packet: AudioPacket, _converter: &mut Converter) -> SinkResult<()> {
        let samples = match packet.samples() {
            Ok(s) => s,
            // Passthrough / raw packets are not expected (decoded PCM only).
            Err(e) => return Err(SinkError::OnWrite(e.to_string())),
        };

        self.scratch.clear();
        self.resampler.process(samples, &mut self.scratch);

        match self.audio.push_blocking(SourceKind::Spotify, &self.scratch) {
            PushResult::Written => Ok(()),
            // Another source owns the speaker. Swallow the audio (the queue must not see
            // it) but keep the session alive, so a later `claim` resumes seamlessly.
            PushResult::Preempted => Ok(()),
            PushResult::Disconnected => {
                Err(SinkError::NotConnected("dante ring writer stopped".into()))
            }
        }
    }
}

// --- discovery / session loop ---

fn device_id(name: &str) -> String {
    hex::encode(Sha1::digest(name.as_bytes()))
}

/// Run the discovery/session loop for one speaker. Returns only if discovery terminates
/// (fatal mDNS error).
pub async fn run_speaker(
    speaker: SpeakerConfig,
    spotify: SpotifyConfig,
    audio: Arc<SpeakerAudio>,
    dante_rate: u32,
    hub: StateHub,
) -> Result<()> {
    let device_id = device_id(&speaker.name);
    let session_config = SessionConfig {
        device_id: device_id.clone(),
        ..Default::default()
    };

    let zeroconf_ip: Vec<IpAddr> = match &spotify.interface_ip {
        Some(ip) => vec![
            ip.parse()
                .with_context(|| format!("parsing spotify.interface_ip '{ip}'"))?,
        ],
        None => vec![],
    };

    let mut discovery = Discovery::builder(device_id.clone(), session_config.client_id.clone())
        .name(speaker.name.clone())
        .device_type(DeviceType::Speaker)
        .port(0)
        .zeroconf_ip(zeroconf_ip)
        .launch()
        .with_context(|| format!("launching discovery for '{}'", speaker.name))?;

    tracing::info!(
        "speaker '{}' discoverable over Spotify Connect (device_id {})",
        speaker.name,
        device_id
    );

    // The active Spirc lives in `spirc_slot` (Spirc is not Clone) so the registered
    // SpotifyControl keeps working across session restarts; we only track its background
    // task here so we can abort it on the next credential.
    let spirc_slot: Arc<Mutex<Option<Spirc>>> = Arc::new(Mutex::new(None));
    audio.register_control(Arc::new(SpotifyControl::new(spirc_slot.clone())));
    let mut active_task: Option<JoinHandle<()>> = None;

    while let Some(credentials) = discovery.next().await {
        tracing::info!(
            "speaker '{}' received credentials, (re)starting session",
            speaker.name
        );
        if let Some(task) = active_task.take() {
            if let Some(spirc) = spirc_slot.lock().unwrap().take() {
                let _ = spirc.shutdown();
            }
            audio.release(SourceKind::Spotify);
            task.abort();
        }

        match start_session(
            &speaker,
            &spotify,
            &session_config,
            dante_rate,
            credentials,
            audio.clone(),
            hub.clone(),
        )
        .await
        {
            Ok((spirc, spirc_task)) => {
                *spirc_slot.lock().unwrap() = Some(spirc);
                active_task = Some(spirc_task);
            }
            Err(e) => {
                tracing::error!("speaker '{}' failed to start session: {e:#}", speaker.name);
            }
        }
    }

    tracing::warn!("speaker '{}' discovery stream ended", speaker.name);
    Ok(())
}

async fn start_session(
    speaker: &SpeakerConfig,
    spotify: &SpotifyConfig,
    session_config: &SessionConfig,
    dante_rate: u32,
    credentials: Credentials,
    audio: Arc<SpeakerAudio>,
    hub: StateHub,
) -> Result<(Spirc, JoinHandle<()>)> {
    let cache = match &spotify.cache_dir {
        Some(dir) => {
            let base = PathBuf::from(dir).join(speaker_id(&speaker.name));
            Some(
                Cache::new(Some(&base), Some(&base), Some(&base.join("audio")), None)
                    .context("opening cache")?,
            )
        }
        None => None,
    };

    let session = Session::new(session_config.clone(), cache);

    let mixer: Arc<dyn Mixer> =
        Arc::new(SoftMixer::open(MixerConfig::default()).context("opening mixer")?);
    let volume_getter: Box<dyn VolumeGetter + Send> = if speaker.apply_volume {
        mixer.get_soft_volume()
    } else {
        Box::new(NoOpVolume)
    };

    let player_config = PlayerConfig {
        bitrate: bitrate_from(spotify.bitrate),
        position_update_interval: Some(Duration::from_secs(1)),
        ..Default::default()
    };

    let sink_audio = audio.clone();
    let player = Player::new(player_config, session.clone(), volume_getter, move || {
        Box::new(DanteSink::new(dante_rate, sink_audio))
    });

    tokio::spawn(pump_events(player.get_player_event_channel(), hub, audio));

    let connect_config = ConnectConfig {
        name: speaker.name.clone(),
        device_type: DeviceType::Speaker,
        initial_volume: speaker
            .initial_volume
            .map(|v| (v.clamp(0.0, 1.0) * u16::MAX as f32) as u16)
            .unwrap_or(u16::MAX / 2),
        ..Default::default()
    };

    let (spirc, spirc_task) = Spirc::new(connect_config, session, credentials, player, mixer)
        .await
        .context("creating Spirc")?;

    let task = tokio::spawn(spirc_task);
    Ok((spirc, task))
}

// --- player events -> state hub ---

/// Translate librespot `PlayerEvent`s into `SpeakerState` updates, claiming and
/// releasing the speaker as playback starts and stops.
///
/// Every now-playing update is gated on Spotify still owning the speaker: a preempted
/// session keeps running (so it can be handed control back instantly) and must not
/// overwrite the AirPlay-owned view of the speaker in the meantime.
async fn pump_events(mut rx: PlayerEventChannel, hub: StateHub, audio: Arc<SpeakerAudio>) {
    let id = audio.id.clone();
    let owns = || audio.is_owner(SourceKind::Spotify);

    while let Some(event) = rx.recv().await {
        match event {
            PlayerEvent::Playing { position_ms, .. } => {
                audio.claim(SourceKind::Spotify);
                hub.update(&id, |s| {
                    s.playback = Playback::Playing;
                    s.position_ms = position_ms;
                    s.position_captured_at_ms = now_ms();
                });
            }
            PlayerEvent::TrackChanged { audio_item } => {
                if owns() {
                    let info = track_info(&audio_item);
                    hub.clear_artwork(&id);
                    hub.update(&id, |s| s.track = Some(info));
                }
            }
            PlayerEvent::Paused { position_ms, .. } => {
                if owns() {
                    hub.update(&id, |s| {
                        s.playback = Playback::Paused;
                        s.position_ms = position_ms;
                        s.position_captured_at_ms = now_ms();
                    });
                }
                audio.release(SourceKind::Spotify);
            }
            PlayerEvent::Stopped { .. } => {
                if owns() {
                    hub.update(&id, |s| s.playback = Playback::Stopped);
                }
                audio.release(SourceKind::Spotify);
            }
            PlayerEvent::Loading { position_ms, .. } => {
                if owns() {
                    hub.update(&id, |s| {
                        s.playback = Playback::Loading;
                        s.position_ms = position_ms;
                        s.position_captured_at_ms = now_ms();
                    });
                }
            }
            PlayerEvent::Seeked { position_ms, .. }
            | PlayerEvent::PositionChanged { position_ms, .. }
            | PlayerEvent::PositionCorrection { position_ms, .. } => {
                if owns() {
                    hub.update(&id, |s| {
                        s.position_ms = position_ms;
                        s.position_captured_at_ms = now_ms();
                    });
                }
            }
            // Shuffle and repeat are Spotify-only concepts, so they track the session
            // regardless of who owns the speaker. Volume is a shared field and must not
            // clobber the AirPlay-reported one.
            PlayerEvent::VolumeChanged { volume } => {
                if owns() {
                    hub.update(&id, |s| s.volume = volume as f32 / u16::MAX as f32);
                }
            }
            PlayerEvent::ShuffleChanged { shuffle } => {
                hub.update(&id, |s| s.shuffle = shuffle);
            }
            PlayerEvent::RepeatChanged { context, track } => {
                hub.update(&id, |s| s.repeat = context || track);
            }
            PlayerEvent::SessionConnected { user_name, .. } => {
                if owns() || audio.owner().is_none() {
                    hub.update(&id, |s| s.active_user = Some(user_name));
                }
            }
            PlayerEvent::SessionDisconnected { .. } => {
                if owns() {
                    hub.update(&id, |s| {
                        s.active_user = None;
                        s.playback = Playback::Inactive;
                    });
                }
                audio.release(SourceKind::Spotify);
            }
            _ => {}
        }
    }

    // Channel closed: the player was torn down.
    audio.release(SourceKind::Spotify);
    if audio.owner().is_none() {
        hub.update(&id, |s| s.playback = Playback::Inactive);
    }
}

fn track_info(item: &AudioItem) -> TrackInfo {
    let (artists, album) = match &item.unique_fields {
        UniqueFields::Track { artists, album, .. } => (
            artists.0.iter().map(|a| a.name.clone()).collect(),
            Some(album.clone()),
        ),
        UniqueFields::Local { artists, album, .. } => (
            artists.clone().map(|a| vec![a]).unwrap_or_default(),
            album.clone(),
        ),
        UniqueFields::Episode { show_name, .. } => (vec![show_name.clone()], None),
    };
    // Covers are sorted largest-first; take the largest.
    let art_url = item.covers.first().map(|c| c.url.clone());
    TrackInfo {
        uri: item.uri.clone(),
        name: item.name.clone(),
        artists,
        album,
        duration_ms: item.duration_ms,
        art_url,
    }
}

fn bitrate_from(kbps: u32) -> Bitrate {
    match kbps {
        96 => Bitrate::Bitrate96,
        320 => Bitrate::Bitrate320,
        _ => Bitrate::Bitrate160,
    }
}
