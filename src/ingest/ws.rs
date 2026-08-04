//! WebSocket transports.
//!
//! * `/ws/ingest` — bidirectional agent stream: the agent sends event JSON
//!   (single object or array) as text frames, the server replies with an ack
//!   frame per message.
//! * `/ws/feed`  — read-only live feed for browsers: every stored event is
//!   pushed as one JSON text frame.

use crate::model::{Ack, Transport};
use crate::state::{AppState, MAX_INGEST_PAYLOAD_BYTES};
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};

pub fn ws_router() -> Router<AppState> {
    Router::new()
        .route("/ws/ingest", get(ingest_upgrade))
        .route("/ws/feed", get(feed_upgrade))
}

async fn ingest_upgrade(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.max_message_size(MAX_INGEST_PAYLOAD_BYTES)
        .max_frame_size(MAX_INGEST_PAYLOAD_BYTES)
        .on_upgrade(move |socket| ingest_session(state, socket))
}

async fn ingest_session(state: AppState, mut socket: WebSocket) {
    while let Some(message) = socket.recv().await {
        let message = match message {
            Ok(message) => message,
            Err(_) => break,
        };
        match message {
            Message::Text(text) => {
                let ack = match state.ingest_json(Transport::Websocket, &text) {
                    Ok(stored) => Ack::ok(stored.last().map(|event| event.seq).unwrap_or(0)),
                    Err(error) => Ack::err(error),
                };
                let payload =
                    serde_json::to_string(&ack).unwrap_or_else(|_| "{\"ok\":false}".to_string());
                if socket.send(Message::Text(payload)).await.is_err() {
                    break;
                }
            }
            Message::Ping(data) => {
                if socket.send(Message::Pong(data)).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
}

async fn feed_upgrade(State(state): State<AppState>, ws: WebSocketUpgrade) -> Response {
    ws.max_message_size(MAX_INGEST_PAYLOAD_BYTES)
        .max_frame_size(MAX_INGEST_PAYLOAD_BYTES)
        .on_upgrade(move |socket| feed_session(state, socket))
}

async fn feed_session(state: AppState, socket: WebSocket) {
    let mut receiver = state.feed.subscribe();
    let (mut sink, mut stream) = socket.split();
    loop {
        tokio::select! {
            broadcast = receiver.recv() => {
                match broadcast {
                    Ok(json) => {
                        if sink.send(Message::Text(json)).await.is_err() {
                            break;
                        }
                    }
                    // Skip ahead if this subscriber lagged behind the ring.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            incoming = stream.next() => {
                match incoming {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                }
            }
        }
    }
}
