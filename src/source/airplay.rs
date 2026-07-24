//! AirPlay source: one shairplay `RaopServer` per speaker, feeding the same Dante
//! pacing queue Spotify uses.
//!
//! **AirPlay 1 only.** shairplay's `ap2` handlers deliver no metadata, artwork, volume
//! or remote-control callbacks — they exist solely on the AP1 path — so the crate is
//! pulled in with default features and the receiver advertises as a classic RAOP
//! device. That is what makes the now-playing state and transport controls work.
//!
//! Unlike Spotify, AirPlay cannot be back-pressured: its callbacks run on tokio tasks
//! (parking one would stall a runtime worker) and the sender streams in real time
//! against its own free-running 44.1 kHz clock. So audio goes through
//! [`SpeakerAudio::push_realtime`], which drops rather than blocks, and a claim
//! prefills silence to give that clock drift headroom in both directions.

use super::{CommandResult, PushResult, SourceControl, SourceKind, SpeakerAudio};
use crate::audio::queue::Frame;
use crate::audio::resampler::SpeakerResampler;
use crate::config::{AirPlayConfig, SpeakerConfig};
use crate::state::{Artwork, Playback, StateHub, TrackInfo, now_ms};
use anyhow::{Context, Result};
use sha1::{Digest, Sha1};
use shairplay::{
    AudioFormat, AudioHandler, AudioSession, BindConfig, RaopServer, RemoteCommand, RemoteControl,
    ShairplayError, TrackMetadata,
};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// AirPlay 1 progress timestamps are RTP ticks at this rate, always.
const RTP_RATE: f64 = 44_100.0;

/// Volume range Apple senders use, in dB (plus -144 for mute).
const VOLUME_DB_RANGE: f32 = 30.0;

/// How long without audio before a playing stream is reported as paused. AirPlay 1 has
/// no pause message — the sender simply stops transmitting RTP.
const PAUSE_AFTER: Duration = Duration::from_millis(1500);

/// Shared between the `AudioHandler` (RTSP thread) and each `AudioSession` (RTP task).
struct AirPlayState {
    id: String,
    name: String,
    hub: StateHub,
    audio: Arc<SpeakerAudio>,
    dante_rate: u32,
    apply_volume: bool,
    /// Last reported sender volume, in dB. -144 means muted.
    volume_db: AtomicU32,
    /// `now_ms()` of the last audio packet, for the paused heuristic.
    last_frame_at: AtomicU64,
    /// Whether `Playback::Playing` has already been published. `hub.update` broadcasts
    /// to every WebSocket client, so the audio hot path must only touch it on a
    /// transition, not once per packet.
    reported_playing: AtomicBool,
    /// Open RTSP connections. iOS opens several in parallel and drops the losers, so
    /// state is only torn down when the last one closes.
    connections: AtomicUsize,
    /// Bumped on every new cover so the artwork URL busts client caches.
    art_version: AtomicU64,
    remote: Mutex<Option<Arc<dyn RemoteControl>>>,
    /// Frames dropped since the last warning, for rate-limited logging.
    dropped: AtomicU64,
}

impl AirPlayState {
    fn volume_db(&self) -> f32 {
        f32::from_bits(self.volume_db.load(Ordering::Relaxed))
    }

    /// Sender dB -> the 0.0-1.0 level reported by the API.
    fn volume_level(&self) -> f32 {
        let db = self.volume_db();
        if db <= -VOLUME_DB_RANGE * 2.0 {
            return 0.0;
        }
        ((db + VOLUME_DB_RANGE) / VOLUME_DB_RANGE).clamp(0.0, 1.0)
    }

    /// Sender dB -> a linear gain for `apply_volume`.
    fn volume_gain(&self) -> f32 {
        let db = self.volume_db();
        if db <= -VOLUME_DB_RANGE * 2.0 {
            return 0.0;
        }
        10f32.powf(db.min(0.0) / 20.0)
    }

    fn owns(&self) -> bool {
        self.audio.is_owner(SourceKind::Airplay)
    }

    /// Apply a state update only while AirPlay actually drives the speaker, so a
    /// preempted (but still connected) sender cannot clobber the Spotify view.
    fn update_if_owner<F: FnOnce(&mut crate::state::SpeakerState)>(&self, f: F) {
        if self.owns() {
            self.hub.update(&self.id, f);
        }
    }

    fn note_dropped(&self, frames: usize) {
        if frames == 0 {
            return;
        }
        let total = self.dropped.fetch_add(frames as u64, Ordering::Relaxed) + frames as u64;
        // The sender's clock free-runs against the Dante media clock, so occasional
        // drops are expected; only shout once they add up to something audible.
        if total >= self.dante_rate as u64 / 10 {
            self.dropped.store(0, Ordering::Relaxed);
            tracing::warn!(
                "speaker '{}': AirPlay queue overflow, dropped ~{} frames \
                 (sender clock drift; increase audio.pacing_buffer_ms if persistent)",
                self.name,
                total
            );
        }
    }
}

// --- control surface ---

/// Transport control over shairplay's DACP remote (AirPlay 1).
///
/// `RemoteControl::send_command` opens a blocking TCP connection to the sender, so every
/// command is dispatched off-thread and the result is optimistic: failures are logged,
/// not returned.
pub struct AirPlayControl {
    state: Arc<AirPlayState>,
}

impl AirPlayControl {
    fn send(&self, cmd: RemoteCommand) -> CommandResult {
        let Some(remote) = self.state.remote.lock().unwrap().clone() else {
            return CommandResult::Inactive;
        };
        let name = self.state.name.clone();
        let described = format!("{cmd:?}");
        let run = move || {
            if let Err(e) = remote.send_command(cmd) {
                tracing::warn!("speaker '{name}': AirPlay {described} failed: {e}");
            }
        };
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn_blocking(run);
            }
            Err(_) => {
                std::thread::spawn(run);
            }
        }
        CommandResult::Ok
    }
}

impl SourceControl for AirPlayControl {
    fn kind(&self) -> SourceKind {
        SourceKind::Airplay
    }
    fn play(&self) -> CommandResult {
        self.send(RemoteCommand::Play)
    }
    fn pause(&self) -> CommandResult {
        self.send(RemoteCommand::Pause)
    }
    fn play_pause(&self) -> CommandResult {
        // DACP has a single playpause verb; Play and Pause both map onto it.
        self.send(RemoteCommand::Play)
    }
    fn next(&self) -> CommandResult {
        self.send(RemoteCommand::NextTrack)
    }
    fn previous(&self) -> CommandResult {
        self.send(RemoteCommand::PreviousTrack)
    }
    fn seek(&self, _position_ms: u32) -> CommandResult {
        // DACP has no seek verb.
        CommandResult::Unsupported
    }
    fn set_volume(&self, level: f32) -> CommandResult {
        self.send(RemoteCommand::SetVolume(
            (level.clamp(0.0, 1.0) * 100.0).round() as u8,
        ))
    }
    fn yield_now(&self) {
        self.send(RemoteCommand::Pause);
    }
}

// --- shairplay callbacks ---

struct AirPlayHandler {
    state: Arc<AirPlayState>,
}

impl AudioHandler for AirPlayHandler {
    fn audio_init(&self, format: AudioFormat) -> Box<dyn AudioSession> {
        tracing::info!(
            "speaker '{}': AirPlay stream starting ({} Hz, {} ch)",
            self.state.name,
            format.sample_rate,
            format.channels
        );
        self.state.audio.claim(SourceKind::Airplay);
        self.state.last_frame_at.store(now_ms(), Ordering::Relaxed);
        self.state.reported_playing.store(false, Ordering::Relaxed);
        self.state.update_if_owner(|s| s.playback = Playback::Loading);

        let resampler = SpeakerResampler::new(format.sample_rate, self.state.dante_rate)
            .unwrap_or(SpeakerResampler::Bypass);
        Box::new(AirPlaySession {
            state: self.state.clone(),
            resampler,
            channels: format.channels.max(1) as usize,
            stereo: Vec::with_capacity(4096),
            frames: Vec::with_capacity(4096),
        })
    }

    fn on_volume(&self, volume: f32) {
        self.state
            .volume_db
            .store(volume.to_bits(), Ordering::Relaxed);
        let level = self.state.volume_level();
        self.state.update_if_owner(|s| s.volume = level);
    }

    fn on_metadata(&self, metadata: &TrackMetadata) {
        let art_url = artwork_url(&self.state.id, self.state.art_version.load(Ordering::Relaxed));
        let info = TrackInfo {
            uri: format!("airplay:{}", self.state.id),
            name: metadata.title.clone().unwrap_or_else(|| "Unknown".into()),
            artists: metadata.artist.clone().into_iter().collect(),
            album: metadata.album.clone(),
            duration_ms: metadata.duration_ms.unwrap_or(0),
            art_url,
        };
        self.state.update_if_owner(|s| s.track = Some(info));
    }

    fn on_coverart(&self, coverart: &[u8]) {
        let Some(content_type) = sniff_image(coverart) else {
            return;
        };
        let version = self.state.art_version.fetch_add(1, Ordering::Relaxed) + 1;
        self.state.hub.set_artwork(
            &self.state.id,
            Artwork {
                bytes: coverart.to_vec(),
                content_type,
            },
        );
        let url = artwork_url(&self.state.id, version);
        self.state.update_if_owner(|s| {
            if let Some(track) = s.track.as_mut() {
                track.art_url = url;
            }
        });
    }

    fn on_progress(&self, start: u32, current: u32, end: u32) {
        let position_ms = rtp_to_ms(current.wrapping_sub(start));
        let duration_ms = rtp_to_ms(end.wrapping_sub(start));
        self.state.update_if_owner(|s| {
            s.position_ms = position_ms;
            s.position_captured_at_ms = now_ms();
            if let Some(track) = s.track.as_mut()
                && duration_ms > 0
            {
                track.duration_ms = duration_ms;
            }
        });
    }

    fn on_remote_control(&self, remote: Arc<dyn RemoteControl>) {
        tracing::debug!("speaker '{}': AirPlay remote control available", self.state.name);
        *self.state.remote.lock().unwrap() = Some(remote);
        self.state
            .audio
            .register_control(Arc::new(AirPlayControl {
                state: self.state.clone(),
            }));
    }

    fn on_client_connected(&self, addr: &str) {
        self.state.connections.fetch_add(1, Ordering::Relaxed);
        tracing::info!("speaker '{}': AirPlay client {addr} connected", self.state.name);
        let addr = addr.to_string();
        self.state.update_if_owner(|s| s.active_user = Some(addr));
    }

    fn on_client_disconnected(&self, addr: &str) {
        // iOS opens parallel connections and abandons the losers, so only the last one
        // closing means the sender is really gone.
        let remaining = self
            .state
            .connections
            .fetch_sub(1, Ordering::Relaxed)
            .saturating_sub(1);
        if remaining > 0 {
            return;
        }
        tracing::info!("speaker '{}': AirPlay client {addr} disconnected", self.state.name);

        let was_owner = self.state.owns();
        self.state.reported_playing.store(false, Ordering::Relaxed);
        self.state.audio.release(SourceKind::Airplay);
        self.state.audio.clear_control(SourceKind::Airplay);
        *self.state.remote.lock().unwrap() = None;
        if was_owner {
            self.state.hub.clear_artwork(&self.state.id);
            self.state.hub.update(&self.state.id, |s| {
                s.playback = Playback::Inactive;
                s.active_user = None;
                s.track = None;
                s.position_ms = 0;
            });
        }
    }

    fn on_error(&self, error: &ShairplayError) {
        tracing::warn!("speaker '{}': AirPlay error: {error}", self.state.name);
    }
}

struct AirPlaySession {
    state: Arc<AirPlayState>,
    resampler: SpeakerResampler,
    channels: usize,
    /// Interleaved stereo scratch (downmixed and volume-scaled) fed to the resampler.
    stereo: Vec<f32>,
    /// Resampler output.
    frames: Vec<Frame>,
}

impl AudioSession for AirPlaySession {
    fn audio_process(&mut self, samples: &[f32]) {
        if samples.is_empty() {
            return;
        }
        if !self.state.owns() {
            // Preempted but still streaming: throw the audio away and leave the state
            // alone until the sender is handed the speaker back.
            return;
        }

        let gain = if self.state.apply_volume {
            self.state.volume_gain()
        } else {
            1.0
        };

        self.stereo.clear();
        to_stereo(samples, self.channels, gain, &mut self.stereo);

        self.frames.clear();
        self.resampler.process_f32(&self.stereo, &mut self.frames);
        if self.frames.is_empty() {
            return;
        }

        let (result, dropped) = self
            .state
            .audio
            .push_realtime(SourceKind::Airplay, &self.frames);
        match result {
            PushResult::Written => self.state.note_dropped(dropped),
            PushResult::Preempted => return,
            PushResult::Disconnected => {
                tracing::error!(
                    "speaker '{}': Dante ring writer stopped; discarding AirPlay audio",
                    self.state.name
                );
                return;
            }
        }

        self.state.last_frame_at.store(now_ms(), Ordering::Relaxed);
        if !self.state.reported_playing.swap(true, Ordering::Relaxed) {
            self.state.update_if_owner(|s| s.playback = Playback::Playing);
        }
    }

    fn audio_flush(&mut self) {
        self.resampler.reset();
        self.state.audio.flush();
    }
}

impl Drop for AirPlaySession {
    fn drop(&mut self) {
        tracing::debug!("speaker '{}': AirPlay stream ended", self.state.name);
        self.state.audio.release(SourceKind::Airplay);
    }
}

// --- server lifecycle ---

/// Start this speaker's AirPlay receiver and run the paused-detection watchdog.
/// Never returns while the server is healthy.
pub async fn run_speaker(
    speaker: SpeakerConfig,
    cfg: AirPlayConfig,
    port: u16,
    audio: Arc<SpeakerAudio>,
    dante_rate: u32,
    hub: StateHub,
) -> Result<()> {
    let state = Arc::new(AirPlayState {
        id: audio.id.clone(),
        name: speaker.name.clone(),
        hub,
        audio,
        dante_rate,
        apply_volume: speaker.apply_volume,
        volume_db: AtomicU32::new(0f32.to_bits()),
        last_frame_at: AtomicU64::new(0),
        reported_playing: AtomicBool::new(false),
        connections: AtomicUsize::new(0),
        art_version: AtomicU64::new(0),
        remote: Mutex::new(None),
        dropped: AtomicU64::new(0),
    });

    let mut bind = BindConfig::new().port(port);
    if let Some(ip) = &cfg.interface_ip {
        let addr: IpAddr = ip
            .parse()
            .with_context(|| format!("parsing airplay.interface_ip '{ip}'"))?;
        bind = bind.addrs([addr]);
    }

    let mut server = RaopServer::builder()
        .name(speaker.name.clone())
        .hwaddr(hwaddr(&speaker.name))
        .bind(bind)
        .output_max_channels(2)
        .build(Arc::new(AirPlayHandler {
            state: state.clone(),
        }))
        .with_context(|| format!("building AirPlay server for '{}'", speaker.name))?;

    server
        .start()
        .await
        .with_context(|| format!("starting AirPlay server for '{}'", speaker.name))?;
    tracing::info!(
        "speaker '{}' discoverable over AirPlay (RTSP port {})",
        speaker.name,
        port
    );

    watch_for_pause(state).await;
    Ok(())
}

/// AirPlay 1 senders signal a pause by simply going quiet, so infer it from a gap in
/// the audio stream.
async fn watch_for_pause(state: Arc<AirPlayState>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(500));
    loop {
        ticker.tick().await;
        if !state.owns() {
            continue;
        }
        let last = state.last_frame_at.load(Ordering::Relaxed);
        if last == 0 || now_ms().saturating_sub(last) < PAUSE_AFTER.as_millis() as u64 {
            continue;
        }
        // `swap` here is what makes this fire once per pause rather than every tick;
        // the next audio packet re-arms it.
        if state.reported_playing.swap(false, Ordering::Relaxed) {
            state.update_if_owner(|s| s.playback = Playback::Paused);
        }
    }
}

// --- helpers ---

/// A stable, locally-administered MAC derived from the speaker name. Keeping it stable
/// across restarts keeps senders' cached device identity valid.
fn hwaddr(name: &str) -> Vec<u8> {
    let digest = Sha1::digest(name.as_bytes());
    let mut addr = digest[..6].to_vec();
    addr[0] = (addr[0] | 0x02) & !0x01;
    addr
}

pub fn artwork_url(id: &str, version: u64) -> Option<String> {
    (version > 0).then(|| format!("/api/v1/speakers/{id}/artwork?v={version}"))
}

fn rtp_to_ms(ticks: u32) -> u32 {
    (ticks as f64 * 1000.0 / RTP_RATE) as u32
}

fn sniff_image(data: &[u8]) -> Option<&'static str> {
    if data.starts_with(&[0xFF, 0xD8, 0xFF]) {
        Some("image/jpeg")
    } else if data.starts_with(&[0x89, b'P', b'N', b'G']) {
        Some("image/png")
    } else {
        None
    }
}

/// Downmix to interleaved stereo and apply `gain`. Mono is duplicated; anything wider
/// than stereo keeps its first two channels (shairplay is asked for at most 2 anyway).
fn to_stereo(samples: &[f32], channels: usize, gain: f32, out: &mut Vec<f32>) {
    match channels {
        1 => {
            for &s in samples {
                out.push(s * gain);
                out.push(s * gain);
            }
        }
        n => {
            for frame in samples.chunks_exact(n) {
                out.push(frame[0] * gain);
                out.push(frame[1] * gain);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hwaddr_is_stable_and_locally_administered() {
        let a = hwaddr("Living Room");
        assert_eq!(a.len(), 6);
        assert_eq!(a, hwaddr("Living Room"));
        assert_ne!(a, hwaddr("Kitchen"));
        // Locally administered (bit 1 set), unicast (bit 0 clear).
        assert_eq!(a[0] & 0x03, 0x02);
    }

    #[test]
    fn rtp_ticks_convert_to_milliseconds() {
        assert_eq!(rtp_to_ms(0), 0);
        assert_eq!(rtp_to_ms(44_100), 1000);
        assert_eq!(rtp_to_ms(22_050), 500);
    }

    #[test]
    fn mono_is_duplicated_and_scaled() {
        let mut out = Vec::new();
        to_stereo(&[1.0, -1.0], 1, 0.5, &mut out);
        assert_eq!(out, vec![0.5, 0.5, -0.5, -0.5]);
    }

    #[test]
    fn extra_channels_are_dropped() {
        let mut out = Vec::new();
        to_stereo(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0], 3, 1.0, &mut out);
        assert_eq!(out, vec![1.0, 2.0, 4.0, 5.0]);
    }

    #[test]
    fn image_sniffing() {
        assert_eq!(sniff_image(&[0xFF, 0xD8, 0xFF, 0xE0]), Some("image/jpeg"));
        assert_eq!(sniff_image(b"\x89PNG\r\n"), Some("image/png"));
        assert_eq!(sniff_image(b"not an image"), None);
    }

    #[test]
    fn artwork_url_only_once_a_cover_arrived() {
        assert_eq!(artwork_url("kitchen", 0), None);
        assert_eq!(
            artwork_url("kitchen", 3).as_deref(),
            Some("/api/v1/speakers/kitchen/artwork?v=3")
        );
    }
}
