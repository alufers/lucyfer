//! REST + WebSocket HTTP API (axum).

pub mod dto;
mod rest;
mod ws;

use crate::speaker::{CommandResult, SpeakerHandle, SpeakerRegistry};
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

/// Apply a named action to a speaker handle. Shared by REST and WS.
pub fn dispatch(
    handle: &SpeakerHandle,
    action: &str,
    position_ms: Option<u32>,
    level: Option<f32>,
) -> Result<CommandResult, String> {
    let result = match action {
        "play" => handle.with_spirc(|s| {
            s.activate()?;
            s.play()
        }),
        "pause" => handle.with_spirc(|s| s.pause()),
        "playpause" => handle.with_spirc(|s| s.play_pause()),
        "next" => handle.with_spirc(|s| s.next()),
        "previous" => handle.with_spirc(|s| s.prev()),
        "seek" => {
            let pos = position_ms.ok_or_else(|| "seek requires position_ms".to_string())?;
            handle.with_spirc(|s| s.set_position_ms(pos))
        }
        "volume" => {
            let level = level.ok_or_else(|| "volume requires level".to_string())?;
            let v = (level.clamp(0.0, 1.0) * u16::MAX as f32) as u16;
            handle.with_spirc(|s| s.set_volume(v))
        }
        other => return Err(format!("unknown action '{other}'")),
    };
    Ok(result)
}
