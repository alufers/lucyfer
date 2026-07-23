//! Translate librespot `PlayerEvent`s into `SpeakerState` updates on the hub.

use crate::state::{Playback, StateHub, TrackInfo, now_ms};
use librespot_metadata::audio::item::{AudioItem, UniqueFields};
use librespot_playback::player::{PlayerEvent, PlayerEventChannel};

pub async fn pump(mut rx: PlayerEventChannel, hub: StateHub, id: String) {
    while let Some(event) = rx.recv().await {
        match event {
            PlayerEvent::TrackChanged { audio_item } => {
                let info = track_info(&audio_item);
                hub.update(&id, |s| s.track = Some(info));
            }
            PlayerEvent::Playing { position_ms, .. } => {
                hub.update(&id, |s| {
                    s.playback = Playback::Playing;
                    s.position_ms = position_ms;
                    s.position_captured_at_ms = now_ms();
                });
            }
            PlayerEvent::Paused { position_ms, .. } => {
                hub.update(&id, |s| {
                    s.playback = Playback::Paused;
                    s.position_ms = position_ms;
                    s.position_captured_at_ms = now_ms();
                });
            }
            PlayerEvent::Stopped { .. } => {
                hub.update(&id, |s| {
                    s.playback = Playback::Stopped;
                });
            }
            PlayerEvent::Loading { position_ms, .. } => {
                hub.update(&id, |s| {
                    s.playback = Playback::Loading;
                    s.position_ms = position_ms;
                    s.position_captured_at_ms = now_ms();
                });
            }
            PlayerEvent::Seeked { position_ms, .. }
            | PlayerEvent::PositionChanged { position_ms, .. }
            | PlayerEvent::PositionCorrection { position_ms, .. } => {
                hub.update(&id, |s| {
                    s.position_ms = position_ms;
                    s.position_captured_at_ms = now_ms();
                });
            }
            PlayerEvent::VolumeChanged { volume } => {
                hub.update(&id, |s| s.volume = volume as f32 / u16::MAX as f32);
            }
            PlayerEvent::ShuffleChanged { shuffle } => {
                hub.update(&id, |s| s.shuffle = shuffle);
            }
            PlayerEvent::RepeatChanged { context, track } => {
                hub.update(&id, |s| s.repeat = context || track);
            }
            PlayerEvent::SessionConnected { user_name, .. } => {
                hub.update(&id, |s| s.active_user = Some(user_name));
            }
            PlayerEvent::SessionDisconnected { .. } => {
                hub.update(&id, |s| {
                    s.active_user = None;
                    s.playback = Playback::Inactive;
                });
            }
            _ => {}
        }
    }
    // Channel closed: the player was torn down.
    hub.update(&id, |s| s.playback = Playback::Inactive);
}

fn track_info(item: &AudioItem) -> TrackInfo {
    let (artists, album) = match &item.unique_fields {
        UniqueFields::Track { artists, album, .. } => (
            artists.0.iter().map(|a| a.name.clone()).collect(),
            Some(album.clone()),
        ),
        UniqueFields::Local {
            artists, album, ..
        } => (
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
