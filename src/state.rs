//! Shared per-speaker playback state: a snapshot map (for REST reads) plus a
//! broadcast channel of updates (for WebSocket streaming).
//!
//! Artwork is kept in a side table rather than on `SpeakerState`: Spotify supplies a
//! CDN URL, but AirPlay pushes raw JPEG/PNG bytes that we have to serve ourselves.

use crate::source::SourceKind;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Playback {
    /// No session from any source has connected to this speaker yet.
    Inactive,
    Stopped,
    Playing,
    Paused,
    Loading,
}

#[derive(Debug, Clone, Serialize)]
pub struct TrackInfo {
    pub uri: String,
    pub name: String,
    pub artists: Vec<String>,
    pub album: Option<String>,
    pub duration_ms: u32,
    pub art_url: Option<String>,
}

/// Album art bytes pushed by a source that has no public URL for them (AirPlay).
#[derive(Debug, Clone)]
pub struct Artwork {
    pub bytes: Vec<u8>,
    pub content_type: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct SpeakerState {
    pub id: String,
    pub name: String,
    pub apply_volume: bool,
    /// Sources this speaker is advertised on, in configuration order.
    pub sources: Vec<SourceKind>,
    /// Which source currently drives the Dante channels, if any.
    pub source: Option<SourceKind>,
    pub playback: Playback,
    pub active_user: Option<String>,
    /// 0.0 - 1.0
    pub volume: f32,
    pub track: Option<TrackInfo>,
    pub position_ms: u32,
    /// Unix ms when `position_ms` was captured; clients extrapolate while playing.
    pub position_captured_at_ms: u64,
    pub shuffle: bool,
    pub repeat: bool,
}

impl SpeakerState {
    pub fn new(id: String, name: String, apply_volume: bool, sources: Vec<SourceKind>) -> Self {
        Self {
            id,
            name,
            apply_volume,
            sources,
            source: None,
            playback: Playback::Inactive,
            active_user: None,
            volume: 0.5,
            track: None,
            position_ms: 0,
            position_captured_at_ms: now_ms(),
            shuffle: false,
            repeat: false,
        }
    }

    /// A copy with `position_ms` advanced to the present if currently playing.
    pub fn extrapolated(&self) -> Self {
        let mut s = self.clone();
        if s.playback == Playback::Playing {
            let elapsed = now_ms().saturating_sub(s.position_captured_at_ms);
            s.position_ms = s.position_ms.saturating_add(elapsed as u32);
            if let Some(t) = &s.track {
                s.position_ms = s.position_ms.min(t.duration_ms);
            }
            s.position_captured_at_ms = now_ms();
        }
        s
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StateEvent {
    SpeakerUpdate { speaker: SpeakerState },
}

#[derive(Clone)]
pub struct StateHub {
    snapshot: Arc<RwLock<HashMap<String, SpeakerState>>>,
    order: Arc<RwLock<Vec<String>>>,
    artwork: Arc<RwLock<HashMap<String, Artwork>>>,
    events: broadcast::Sender<StateEvent>,
}

impl StateHub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self {
            snapshot: Arc::new(RwLock::new(HashMap::new())),
            order: Arc::new(RwLock::new(Vec::new())),
            artwork: Arc::new(RwLock::new(HashMap::new())),
            events: tx,
        }
    }

    /// Register a speaker in its initial (inactive) state.
    pub fn register(&self, state: SpeakerState) {
        self.order.write().unwrap().push(state.id.clone());
        self.snapshot
            .write()
            .unwrap()
            .insert(state.id.clone(), state);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<StateEvent> {
        self.events.subscribe()
    }

    /// Apply a mutation to a speaker's state and broadcast the result.
    pub fn update<F: FnOnce(&mut SpeakerState)>(&self, id: &str, f: F) {
        let updated = {
            let mut map = self.snapshot.write().unwrap();
            let Some(state) = map.get_mut(id) else {
                return;
            };
            f(state);
            state.clone()
        };
        let _ = self.events.send(StateEvent::SpeakerUpdate { speaker: updated });
    }

    pub fn get(&self, id: &str) -> Option<SpeakerState> {
        self.snapshot.read().unwrap().get(id).map(|s| s.extrapolated())
    }

    /// Store album art bytes for a speaker, replacing any previous cover.
    pub fn set_artwork(&self, id: &str, artwork: Artwork) {
        self.artwork
            .write()
            .unwrap()
            .insert(id.to_string(), artwork);
    }

    pub fn get_artwork(&self, id: &str) -> Option<Artwork> {
        self.artwork.read().unwrap().get(id).cloned()
    }

    pub fn clear_artwork(&self, id: &str) {
        self.artwork.write().unwrap().remove(id);
    }

    /// All speakers in registration order, with positions extrapolated.
    pub fn all(&self) -> Vec<SpeakerState> {
        let map = self.snapshot.read().unwrap();
        self.order
            .read()
            .unwrap()
            .iter()
            .filter_map(|id| map.get(id).map(|s| s.extrapolated()))
            .collect()
    }
}

impl Default for StateHub {
    fn default() -> Self {
        Self::new()
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
