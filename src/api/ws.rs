//! WebSocket handler: streams state snapshots/updates and accepts commands.

use super::dto::{WsClientMsg, WsServerMsg};
use super::{AppState, dispatch};
use crate::source::CommandResult;
use crate::state::StateEvent;
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.hub.subscribe();

    // Initial snapshot.
    let snapshot = WsServerMsg::Snapshot {
        speakers: state.hub.all(),
    };
    if send(&mut socket, &snapshot).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Ok(StateEvent::SpeakerUpdate { speaker }) => {
                        let msg = WsServerMsg::SpeakerUpdate { speaker };
                        if send(&mut socket, &msg).await.is_err() {
                            return;
                        }
                    }
                    Err(RecvError::Lagged(_)) => {
                        // Fell behind: resend a full snapshot.
                        let msg = WsServerMsg::Snapshot { speakers: state.hub.all() };
                        if send(&mut socket, &msg).await.is_err() {
                            return;
                        }
                    }
                    Err(RecvError::Closed) => return,
                }
            }
            incoming = socket.recv() => {
                match incoming {
                    Some(Ok(Message::Text(text))) => {
                        if handle_client_msg(&mut socket, &state, &text).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => return,
                    Some(Ok(_)) => {} // ignore binary/ping/pong
                    Some(Err(_)) => return,
                }
            }
        }
    }
}

async fn handle_client_msg(
    socket: &mut WebSocket,
    state: &AppState,
    text: &str,
) -> Result<(), axum::Error> {
    let msg: WsClientMsg = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(e) => {
            return send(
                socket,
                &WsServerMsg::Error {
                    message: format!("invalid message: {e}"),
                },
            )
            .await;
        }
    };

    match msg {
        WsClientMsg::Command {
            speaker_id,
            action,
            position_ms,
            level,
        } => {
            let Some(audio) = state.registry.get(&speaker_id) else {
                return send(
                    socket,
                    &WsServerMsg::Error {
                        message: format!("unknown speaker '{speaker_id}'"),
                    },
                )
                .await;
            };
            let Some(control) = audio.command_target() else {
                return send(
                    socket,
                    &WsServerMsg::Error {
                        message: format!("speaker '{speaker_id}' inactive"),
                    },
                )
                .await;
            };
            let reply = match dispatch(control.as_ref(), &action, position_ms, level) {
                Ok(CommandResult::Ok) => WsServerMsg::Ack { speaker_id, action },
                Ok(CommandResult::Inactive) => WsServerMsg::Error {
                    message: format!("speaker '{speaker_id}' inactive"),
                },
                Ok(CommandResult::Unsupported) => WsServerMsg::Error {
                    message: format!("action '{action}' is unsupported by the active source"),
                },
                Ok(CommandResult::Failed(e)) => WsServerMsg::Error { message: e },
                Err(e) => WsServerMsg::Error { message: e },
            };
            send(socket, &reply).await
        }
    }
}

async fn send(socket: &mut WebSocket, msg: &WsServerMsg) -> Result<(), axum::Error> {
    let text = serde_json::to_string(msg).unwrap_or_else(|_| "{}".to_string());
    socket.send(Message::Text(text.into())).await
}
