//! REST handlers.

use super::dto::{ErrorResponse, SeekRequest, VolumeRequest};
use super::{AppState, dispatch};
use crate::speaker::CommandResult;
use crate::state::SpeakerState;
use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Redirect, Response};
use std::sync::Arc;

pub async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

pub async fn list_speakers(State(state): State<Arc<AppState>>) -> Json<Vec<SpeakerState>> {
    Json(state.hub.all())
}

pub async fn get_speaker(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    match state.hub.get(&id) {
        Some(s) => Json(s).into_response(),
        None => not_found(&id),
    }
}

pub async fn artwork(State(state): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    match state.hub.get(&id).and_then(|s| s.track.and_then(|t| t.art_url)) {
        Some(url) => Redirect::temporary(&url).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: "no artwork available".into(),
            }),
        )
            .into_response(),
    }
}

pub async fn play(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    run(&s, &id, "play", None, None)
}
pub async fn pause(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    run(&s, &id, "pause", None, None)
}
pub async fn play_pause(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    run(&s, &id, "playpause", None, None)
}
pub async fn next(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    run(&s, &id, "next", None, None)
}
pub async fn previous(State(s): State<Arc<AppState>>, Path(id): Path<String>) -> Response {
    run(&s, &id, "previous", None, None)
}

pub async fn seek(
    State(s): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SeekRequest>,
) -> Response {
    run(&s, &id, "seek", Some(body.position_ms), None)
}

pub async fn volume(
    State(s): State<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<VolumeRequest>,
) -> Response {
    run(&s, &id, "volume", None, Some(body.level))
}

fn run(state: &AppState, id: &str, action: &str, position_ms: Option<u32>, level: Option<f32>) -> Response {
    let Some(handle) = state.registry.get(id) else {
        return not_found(id);
    };
    match dispatch(&handle, action, position_ms, level) {
        Ok(CommandResult::Ok) => StatusCode::NO_CONTENT.into_response(),
        Ok(CommandResult::Inactive) => (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "speaker_inactive".into(),
            }),
        )
            .into_response(),
        Ok(CommandResult::Failed(e)) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse { error: e }),
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    }
}

fn not_found(id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: format!("unknown speaker '{id}'"),
        }),
    )
        .into_response()
}
