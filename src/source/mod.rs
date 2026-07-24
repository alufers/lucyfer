//! Audio sources and the per-speaker arbitration between them.
//!
//! A speaker is a single stereo pair of Dante TX channels that is advertised
//! simultaneously by every enabled source (Spotify Connect and AirPlay). Only one
//! source may drive those channels at a time, so each speaker owns a [`SpeakerAudio`]:
//! the pacing queue plus a lock-free "who owns this speaker" flag and the registered
//! per-source control surfaces.
//!
//! Arbitration is **last writer wins**: whichever source starts playing most recently
//! claims the speaker, and the displaced source is gracefully told to stop
//! ([`SourceControl::yield_now`] — `spirc.pause()` for Spotify, a DACP pause for
//! AirPlay) and gated off the queue. Both network sessions stay alive, so switching
//! back is immediate.

pub mod airplay;
pub mod spotify;

use crate::audio::queue::{DiscardRequest, Frame, QueueProducer};
use crate::state::StateHub;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Which audio source a speaker is being driven by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceKind {
    Spotify,
    Airplay,
}

/// Owner tag stored in [`SpeakerAudio::owner`]. 0 means "nobody".
const OWNER_NONE: u8 = 0;

impl SourceKind {
    fn tag(self) -> u8 {
        match self {
            SourceKind::Spotify => 1,
            SourceKind::Airplay => 2,
        }
    }

    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(SourceKind::Spotify),
            2 => Some(SourceKind::Airplay),
            _ => None,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            SourceKind::Spotify => "spotify",
            SourceKind::Airplay => "airplay",
        }
    }
}

/// Outcome of a transport command issued through the API.
pub enum CommandResult {
    Ok,
    /// No session from any source has connected to this speaker yet.
    Inactive,
    /// The owning source cannot do this (e.g. seeking over AirPlay 1).
    Unsupported,
    Failed(String),
}

/// One source's control surface for a speaker, as used by the REST/WS API.
///
/// Implemented by `spotify::SpotifyControl` (over `Spirc`) and
/// `airplay::AirPlayControl` (over shairplay's DACP `RemoteControl`).
pub trait SourceControl: Send + Sync {
    fn kind(&self) -> SourceKind;
    fn play(&self) -> CommandResult;
    fn pause(&self) -> CommandResult;
    fn play_pause(&self) -> CommandResult;
    fn next(&self) -> CommandResult;
    fn previous(&self) -> CommandResult;
    fn seek(&self, position_ms: u32) -> CommandResult;
    /// `level` is 0.0 - 1.0.
    fn set_volume(&self, level: f32) -> CommandResult;
    /// Another source has taken the speaker: stop playing. Best effort, must not block.
    fn yield_now(&self);
}

/// Result of handing frames to a speaker's queue.
#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    Written,
    /// Another source took the speaker mid-write; the caller's audio was dropped.
    Preempted,
    /// The ring writer is gone, so nothing will ever drain the queue again.
    Disconnected,
}

/// One speaker's audio path: the pacing queue, the current owner, and the registered
/// source controls.
pub struct SpeakerAudio {
    pub id: String,
    pub name: String,
    hub: StateHub,
    producer: Mutex<QueueProducer>,
    discard: DiscardRequest,
    owner: AtomicU8,
    controls: Mutex<HashMap<SourceKind, Arc<dyn SourceControl>>>,
    /// Queue depth a real-time source is topped back up to. See [`Self::push_realtime`].
    realtime_target_frames: usize,
}

impl SpeakerAudio {
    pub fn new(
        id: String,
        name: String,
        hub: StateHub,
        producer: QueueProducer,
        realtime_target_frames: usize,
    ) -> Self {
        let discard = producer.discard_request();
        Self {
            id,
            name,
            hub,
            producer: Mutex::new(producer),
            discard,
            owner: AtomicU8::new(OWNER_NONE),
            controls: Mutex::new(HashMap::new()),
            realtime_target_frames,
        }
    }

    // --- control registration ---

    pub fn register_control(&self, control: Arc<dyn SourceControl>) {
        self.controls
            .lock()
            .unwrap()
            .insert(control.kind(), control);
    }

    pub fn clear_control(&self, kind: SourceKind) {
        self.controls.lock().unwrap().remove(&kind);
    }

    fn control(&self, kind: SourceKind) -> Option<Arc<dyn SourceControl>> {
        self.controls.lock().unwrap().get(&kind).cloned()
    }

    /// The control an API command should be routed to: the owning source if any,
    /// otherwise the only registered one, otherwise Spotify (whose `play` can activate
    /// an idle Connect session, which AirPlay has no equivalent for).
    pub fn command_target(&self) -> Option<Arc<dyn SourceControl>> {
        if let Some(owner) = self.owner()
            && let Some(ctl) = self.control(owner)
        {
            return Some(ctl);
        }
        let controls = self.controls.lock().unwrap();
        if controls.len() == 1 {
            return controls.values().next().cloned();
        }
        controls
            .get(&SourceKind::Spotify)
            .or_else(|| controls.get(&SourceKind::Airplay))
            .cloned()
    }

    // --- ownership ---

    pub fn owner(&self) -> Option<SourceKind> {
        SourceKind::from_tag(self.owner.load(Ordering::Acquire))
    }

    pub fn is_owner(&self, kind: SourceKind) -> bool {
        self.owner.load(Ordering::Acquire) == kind.tag()
    }

    /// Take the speaker for `kind`, gracefully stopping whoever had it.
    ///
    /// Idempotent: re-claiming for the current owner does nothing, so sources can call
    /// this on every "started playing" event.
    pub fn claim(&self, kind: SourceKind) {
        let previous = self.owner.swap(kind.tag(), Ordering::AcqRel);
        if previous == kind.tag() {
            return;
        }

        if let Some(loser) = SourceKind::from_tag(previous) {
            tracing::info!(
                "speaker '{}': {} takes over from {}",
                self.name,
                kind.label(),
                loser.label()
            );
            if let Some(ctl) = self.control(loser) {
                ctl.yield_now();
            }
        } else {
            tracing::info!("speaker '{}': {} started playing", self.name, kind.label());
        }

        // Drop whatever the displaced source had already queued so its audio never
        // reaches Dante. A real-time producer re-centres itself on its next push.
        self.discard.request();
        self.publish_owner();
    }

    /// Give up the speaker if `kind` currently holds it.
    pub fn release(&self, kind: SourceKind) {
        if self
            .owner
            .compare_exchange(kind.tag(), OWNER_NONE, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        self.discard.request();
        self.publish_owner();
    }

    fn publish_owner(&self) {
        let owner = self.owner();
        self.hub.update(&self.id, |s| s.source = owner);
    }

    // --- audio ---

    /// Push all frames, parking while the queue is full. Blocking here is what paces a
    /// decode-ahead source (Spotify); it must only be called from a dedicated thread.
    ///
    /// The producer lock is released between attempts so a source switch never waits on
    /// a parked writer.
    pub fn push_blocking(&self, kind: SourceKind, mut frames: &[Frame]) -> PushResult {
        while !frames.is_empty() {
            if !self.is_owner(kind) {
                return PushResult::Preempted;
            }
            let pushed = {
                let mut producer = self.producer.lock().unwrap();
                if !producer.is_consumer_alive() {
                    return PushResult::Disconnected;
                }
                producer.push_some(frames)
            };
            if pushed == 0 {
                // Queue full: wait for the ring writer to drain some. One frame at
                // 48 kHz is ~20 us; parking ~1 ms beats busy-spinning.
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            frames = &frames[pushed..];
        }
        PushResult::Written
    }

    /// Push what currently fits and drop the rest, never blocking.
    ///
    /// Used by real-time sources (AirPlay), whose callbacks run on tokio tasks — parking
    /// there would stall a runtime worker — and which cannot be back-pressured anyway.
    ///
    /// Such a source runs on its own free-running clock, so its queue depth drifts
    /// against the Dante media clock in both directions. Before every push the queue is
    /// topped back up to `realtime_target_frames` if it has fallen near empty, which
    /// both establishes the initial cushion and re-establishes it after a drain (a
    /// source switch, a flush, or drift). Overflow at the other end is handled by
    /// dropping the tail. Either way the correction is a glitch, but a bounded one:
    /// without it the queue would sit at zero and underrun continuously.
    ///
    /// Returns the number of frames dropped alongside the result.
    pub fn push_realtime(&self, kind: SourceKind, frames: &[Frame]) -> (PushResult, usize) {
        if !self.is_owner(kind) {
            return (PushResult::Preempted, frames.len());
        }
        let mut producer = self.producer.lock().unwrap();
        if !producer.is_consumer_alive() {
            return (PushResult::Disconnected, frames.len());
        }
        if self.realtime_target_frames > 0 {
            let queued = producer.occupied_len();
            if queued < self.realtime_target_frames / 4 {
                producer.push_silence(self.realtime_target_frames - queued);
            }
        }
        let pushed = producer.push_some(frames);
        (PushResult::Written, frames.len() - pushed)
    }

    /// Ask the ring writer to drop everything queued (AirPlay flush / seek).
    pub fn flush(&self) {
        self.discard.request();
    }
}

/// Registry of every speaker's audio path, keyed by id, for the API layer.
#[derive(Clone, Default)]
pub struct SpeakerRegistry {
    map: Arc<Mutex<HashMap<String, Arc<SpeakerAudio>>>>,
}

impl SpeakerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, audio: Arc<SpeakerAudio>) {
        self.map.lock().unwrap().insert(audio.id.clone(), audio);
    }

    pub fn get(&self, id: &str) -> Option<Arc<SpeakerAudio>> {
        self.map.lock().unwrap().get(id).cloned()
    }
}

/// Derive a stable id/slug from a speaker name.
pub fn speaker_id(name: &str) -> String {
    let mut slug = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            slug.push(c.to_ascii_lowercase());
        } else if !slug.ends_with('-') {
            slug.push('-');
        }
    }
    slug.trim_matches('-').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::audio::queue;
    use std::sync::atomic::AtomicUsize;

    struct FakeControl {
        kind: SourceKind,
        yields: Arc<AtomicUsize>,
    }

    impl SourceControl for FakeControl {
        fn kind(&self) -> SourceKind {
            self.kind
        }
        fn play(&self) -> CommandResult {
            CommandResult::Ok
        }
        fn pause(&self) -> CommandResult {
            CommandResult::Ok
        }
        fn play_pause(&self) -> CommandResult {
            CommandResult::Ok
        }
        fn next(&self) -> CommandResult {
            CommandResult::Ok
        }
        fn previous(&self) -> CommandResult {
            CommandResult::Ok
        }
        fn seek(&self, _position_ms: u32) -> CommandResult {
            CommandResult::Ok
        }
        fn set_volume(&self, _level: f32) -> CommandResult {
            CommandResult::Ok
        }
        fn yield_now(&self) {
            self.yields.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn audio(prefill: usize) -> (Arc<SpeakerAudio>, queue::QueueConsumer) {
        let hub = StateHub::new();
        hub.register(crate::state::SpeakerState::new(
            "kitchen".into(),
            "Kitchen".into(),
            true,
            vec![SourceKind::Spotify, SourceKind::Airplay],
        ));
        let (prod, cons) = queue::channel(4096);
        let audio = Arc::new(SpeakerAudio::new(
            "kitchen".into(),
            "Kitchen".into(),
            hub,
            prod,
            prefill,
        ));
        (audio, cons)
    }

    #[test]
    fn claim_preempts_and_yields_the_loser() {
        let (audio, _cons) = audio(0);
        let spotify_yields = Arc::new(AtomicUsize::new(0));
        let airplay_yields = Arc::new(AtomicUsize::new(0));
        audio.register_control(Arc::new(FakeControl {
            kind: SourceKind::Spotify,
            yields: spotify_yields.clone(),
        }));
        audio.register_control(Arc::new(FakeControl {
            kind: SourceKind::Airplay,
            yields: airplay_yields.clone(),
        }));

        audio.claim(SourceKind::Spotify);
        assert_eq!(audio.owner(), Some(SourceKind::Spotify));
        assert_eq!(spotify_yields.load(Ordering::Relaxed), 0);

        // AirPlay takes over: Spotify is told to stop, exactly once.
        audio.claim(SourceKind::Airplay);
        assert_eq!(audio.owner(), Some(SourceKind::Airplay));
        assert_eq!(spotify_yields.load(Ordering::Relaxed), 1);

        // Re-claiming for the current owner is a no-op.
        audio.claim(SourceKind::Airplay);
        assert_eq!(airplay_yields.load(Ordering::Relaxed), 0);

        // And back again.
        audio.claim(SourceKind::Spotify);
        assert_eq!(airplay_yields.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn release_only_affects_the_current_owner() {
        let (audio, _cons) = audio(0);
        audio.claim(SourceKind::Spotify);
        audio.release(SourceKind::Airplay);
        assert_eq!(audio.owner(), Some(SourceKind::Spotify));
        audio.release(SourceKind::Spotify);
        assert_eq!(audio.owner(), None);
    }

    #[test]
    fn non_owner_pushes_are_dropped() {
        let (audio, mut cons) = audio(0);
        audio.claim(SourceKind::Airplay);

        assert_eq!(
            audio.push_blocking(SourceKind::Spotify, &[[1, 1]]),
            PushResult::Preempted
        );
        let (result, dropped) = audio.push_realtime(SourceKind::Spotify, &[[1, 1], [2, 2]]);
        assert_eq!(result, PushResult::Preempted);
        assert_eq!(dropped, 2);

        let (result, dropped) = audio.push_realtime(SourceKind::Airplay, &[[7, 8]]);
        assert_eq!(result, PushResult::Written);
        assert_eq!(dropped, 0);
        // Only the owner's frame made it into the queue.
        assert_eq!(cons.pop(), Some([7, 8]));
        assert_eq!(cons.pop(), None);
    }

    #[test]
    fn realtime_push_reports_drops_when_full() {
        let (audio, _cons) = audio(0);
        audio.claim(SourceKind::Airplay);
        let frames = vec![[1, 1]; 5000];
        let (result, dropped) = audio.push_realtime(SourceKind::Airplay, &frames);
        assert_eq!(result, PushResult::Written);
        // Capacity is 4096, so the tail is dropped rather than blocking.
        assert_eq!(dropped, 5000 - 4096);
    }

    #[test]
    fn realtime_push_tops_the_queue_up_to_target() {
        let (audio, mut cons) = audio(1024);
        audio.claim(SourceKind::Airplay);
        // The claim's discard drains whatever the displaced source left behind; the
        // cushion is (re)built by the next push, so it survives that drain.
        assert!(cons.take_discard_request());

        let (result, dropped) = audio.push_realtime(SourceKind::Airplay, &[[7, 8]]);
        assert_eq!(result, PushResult::Written);
        assert_eq!(dropped, 0);
        // Still cushioned, so this push adds no further silence.
        audio.push_realtime(SourceKind::Airplay, &[[9, 9]]);

        // Exactly one cushion, then both real frames in order.
        let mut silence = 0;
        while cons.pop() == Some([0, 0]) {
            silence += 1;
        }
        assert_eq!(silence, 1024);
        assert_eq!(cons.pop(), Some([9, 9]));
        assert_eq!(cons.pop(), None);
    }

    #[test]
    fn slug_from_name() {
        assert_eq!(speaker_id("Living Room"), "living-room");
        assert_eq!(speaker_id("  Kitchen!! "), "kitchen");
    }
}
