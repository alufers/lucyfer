//! Request/response and WebSocket message shapes for the API.

use crate::state::SpeakerState;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct SeekRequest {
    pub position_ms: u32,
}

#[derive(Debug, Deserialize)]
pub struct VolumeRequest {
    /// 0.0 - 1.0
    pub level: f32,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Server -> client WebSocket frames.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsServerMsg {
    Snapshot { speakers: Vec<SpeakerState> },
    SpeakerUpdate { speaker: SpeakerState },
    Ack { speaker_id: String, action: String },
    Error { message: String },
}

/// Client -> server WebSocket frames.
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsClientMsg {
    Command {
        speaker_id: String,
        action: String,
        #[serde(default)]
        position_ms: Option<u32>,
        #[serde(default)]
        level: Option<f32>,
    },
}
