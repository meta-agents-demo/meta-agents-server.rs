//! UDP transport: each datagram carries one JSON event (or an array).
//! Fire-and-forget by design, but an ack datagram is sent back to the source
//! address on a best-effort basis so connectors *can* confirm delivery.

use crate::model::{Ack, Transport};
use crate::state::AppState;
use tokio::net::UdpSocket;

const MAX_DATAGRAM: usize = 64 * 1024;

pub async fn serve(socket: UdpSocket, state: AppState) {
    let mut buf = vec![0u8; MAX_DATAGRAM];
    loop {
        let (len, peer) = match socket.recv_from(&mut buf).await {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!("udp recv error: {e}");
                continue;
            }
        };
        let payload = String::from_utf8_lossy(&buf[..len]);
        let ack = match state.ingest_json(Transport::Udp, &payload) {
            Ok(stored) => Ack::ok(stored.last().map(|s| s.seq).unwrap_or(0)),
            Err(e) => {
                tracing::debug!("udp ingest error from {peer}: {e}");
                Ack::err(e)
            }
        };
        if let Ok(json) = serde_json::to_string(&ack) {
            // Best effort; the sender may not be listening for a reply.
            let _ = socket.send_to(json.as_bytes(), peer).await;
        }
    }
}
