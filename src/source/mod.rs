pub mod airplay;
pub mod spotify;

use crate::dante::{Frame, SpeakerSink};
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

/// Outcome of a transport command issued by the control layer.
pub enum CommandResult {
    Ok,
    /// No session from any source has connected to this speaker yet.
    Inactive,
    /// The owning source cannot do this (e.g. seeking over AirPlay 1).
    Unsupported,
    Failed(String),
}

#[allow(dead_code)]
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

#[allow(dead_code)]
pub fn dispatch(
    control: &dyn SourceControl,
    action: &str,
    position_ms: Option<u32>,
    level: Option<f32>,
) -> Result<CommandResult, String> {
    let result = match action {
        "play" => control.play(),
        "pause" => control.pause(),
        "playpause" => control.play_pause(),
        "next" => control.next(),
        "previous" => control.previous(),
        "seek" => {
            let pos = position_ms.ok_or_else(|| "seek requires position_ms".to_string())?;
            control.seek(pos)
        }
        "volume" => {
            let level = level.ok_or_else(|| "volume requires level".to_string())?;
            control.set_volume(level)
        }
        other => return Err(format!("unknown action '{other}'")),
    };
    Ok(result)
}

/// Result of handing frames to a speaker's Dante sink.
#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    Written,
    /// Another source took the speaker mid-write; the caller's audio was dropped.
    Preempted,
}

/// One speaker's audio path: the Dante sink, the current owner, and the registered
/// source controls.
pub struct SpeakerAudio {
    pub id: String,
    pub name: String,
    hub: StateHub,
    sink: Arc<SpeakerSink>,
    owner: AtomicU8,
    controls: Mutex<HashMap<SourceKind, Arc<dyn SourceControl>>>,
}

impl SpeakerAudio {
    pub fn new(id: String, name: String, hub: StateHub, sink: Arc<SpeakerSink>) -> Self {
        Self {
            id,
            name,
            hub,
            sink,
            owner: AtomicU8::new(OWNER_NONE),
            controls: Mutex::new(HashMap::new()),
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

    #[allow(dead_code)]
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

        // Scrub whatever the displaced source had already written so its audio never
        // reaches Dante. The new owner anchors a fresh buffer on its first write.
        self.sink.reset();
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
        self.sink.reset();
        self.publish_owner();
    }

    fn publish_owner(&self) {
        let owner = self.owner();
        self.hub.update(&self.id, |s| s.source = owner);
    }

    // --- audio ---

    pub fn push_blocking(&self, kind: SourceKind, mut frames: &[Frame]) -> PushResult {
        while !frames.is_empty() {
            if !self.is_owner(kind) {
                return PushResult::Preempted;
            }
            let written = self.sink.try_write(frames);
            if written == 0 {
                // Buffer full (or no media clock yet). One frame at 48 kHz is ~20 us;
                // parking ~1 ms beats busy-spinning.
                std::thread::sleep(Duration::from_millis(1));
                continue;
            }
            frames = &frames[written..];
        }
        PushResult::Written
    }

    pub fn push_realtime(&self, kind: SourceKind, frames: &[Frame]) -> (PushResult, usize) {
        if !self.is_owner(kind) {
            return (PushResult::Preempted, frames.len());
        }
        let written = self.sink.write_realtime(frames);
        (PushResult::Written, frames.len() - written)
    }

    pub fn flush(&self) {
        self.sink.reset();
    }
}

/// Registry of every speaker's audio path, keyed by id, for the control layer.
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

    // Command surface, kept for the upcoming MQTT client.
    #[allow(dead_code)]
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
    use crate::dante::SpeakerSink;
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

    /// A speaker whose sink has no device behind it: writes go nowhere, which is all
    /// these tests need (they exercise arbitration, not audio).
    fn audio() -> Arc<SpeakerAudio> {
        let hub = StateHub::new();
        hub.register(crate::state::SpeakerState::new(
            "kitchen".into(),
            "Kitchen".into(),
            true,
            vec![SourceKind::Spotify, SourceKind::Airplay],
        ));
        Arc::new(SpeakerAudio::new(
            "kitchen".into(),
            "Kitchen".into(),
            hub,
            Arc::new(SpeakerSink::detached()),
        ))
    }

    #[test]
    fn claim_preempts_and_yields_the_loser() {
        let audio = audio();
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
        let audio = audio();
        audio.claim(SourceKind::Spotify);
        audio.release(SourceKind::Airplay);
        assert_eq!(audio.owner(), Some(SourceKind::Spotify));
        audio.release(SourceKind::Spotify);
        assert_eq!(audio.owner(), None);
    }

    #[test]
    fn non_owner_pushes_are_dropped() {
        let audio = audio();
        audio.claim(SourceKind::Airplay);

        assert_eq!(
            audio.push_blocking(SourceKind::Spotify, &[[1, 1]]),
            PushResult::Preempted
        );
        let (result, dropped) = audio.push_realtime(SourceKind::Spotify, &[[1, 1], [2, 2]]);
        assert_eq!(result, PushResult::Preempted);
        assert_eq!(dropped, 2);
    }

    #[test]
    fn slug_from_name() {
        assert_eq!(speaker_id("Living Room"), "living-room");
        assert_eq!(speaker_id("  Kitchen!! "), "kitchen");
    }
}
