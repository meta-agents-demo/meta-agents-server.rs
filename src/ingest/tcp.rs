//! Raw TCP transport: newline-delimited JSON. Each line is one event (or an
//! array of events); the server answers each line with one ack line.

use crate::model::{Ack, Transport};
use crate::state::AppState;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

pub async fn serve(listener: TcpListener, state: AppState) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!("tcp accept error: {e}");
                continue;
            }
        };
        tracing::debug!("tcp connection from {peer}");
        let state = state.clone();
        tokio::spawn(async move {
            let (read_half, mut write_half) = stream.into_split();
            let mut lines = BufReader::new(read_half).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                if line.trim().is_empty() {
                    continue;
                }
                let ack = match state.ingest_json(Transport::Tcp, &line) {
                    Ok(stored) => Ack::ok(stored.last().map(|s| s.seq).unwrap_or(0)),
                    Err(e) => Ack::err(e),
                };
                let mut payload =
                    serde_json::to_string(&ack).unwrap_or_else(|_| "{\"ok\":false}".to_string());
                payload.push('\n');
                if write_half.write_all(payload.as_bytes()).await.is_err() {
                    break;
                }
            }
        });
    }
}
