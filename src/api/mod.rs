//! REST + WebSocket HTTP API (axum).

pub mod dto;
mod rest;
mod ws;

use crate::source::{CommandResult, SourceControl, SpeakerRegistry};
use crate::state::StateHub;
use axum::Router;
use axum::routing::{get, post};
use std::sync::Arc;
use tower_http::cors::CorsLayer;

#[derive(Clone)]
pub struct AppState {
    pub hub: StateHub,
    pub registry: SpeakerRegistry,
}

pub fn router(hub: StateHub, registry: SpeakerRegistry) -> Router {
    let state = Arc::new(AppState { hub, registry });
    Router::new()
        .route("/healthz", get(rest::healthz))
        .route("/api/v1/speakers", get(rest::list_speakers))
        .route("/api/v1/speakers/{id}", get(rest::get_speaker))
        .route("/api/v1/speakers/{id}/play", post(rest::play))
        .route("/api/v1/speakers/{id}/pause", post(rest::pause))
        .route("/api/v1/speakers/{id}/playpause", post(rest::play_pause))
        .route("/api/v1/speakers/{id}/next", post(rest::next))
        .route("/api/v1/speakers/{id}/previous", post(rest::previous))
        .route("/api/v1/speakers/{id}/seek", post(rest::seek))
        .route("/api/v1/speakers/{id}/volume", post(rest::volume))
        .route("/api/v1/speakers/{id}/artwork", get(rest::artwork))
        .route("/api/v1/ws", get(ws::ws_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

/// Apply a named action to whichever source currently drives a speaker. Shared by REST
/// and WS. `Err` means the request itself was malformed (400).
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
