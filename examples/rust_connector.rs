//! Minimal Rust connector: registers an agent over HTTP, then streams events
//! over all four transports (HTTP, WebSocket, TCP, UDP).
//!
//! Run the server first, then:
//!     cargo run --example rust_connector
//! Override targets with META_AGENTS_HTTP_ADDR / _TCP_ADDR / _UDP_ADDR.

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_tungstenite::tungstenite::Message;

fn addr(env: &str, default: &str) -> String {
    std::env::var(env).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let http = addr("META_AGENTS_HTTP_ADDR", "127.0.0.1:7700");
    let tcp = addr("META_AGENTS_TCP_ADDR", "127.0.0.1:7701");
    let udp = addr("META_AGENTS_UDP_ADDR", "127.0.0.1:7702");
    let agent = "claude-rust-example";

    // 1. Register over HTTP REST.
    let client = reqwest::Client::new();
    let res = client
        .post(format!("http://{http}/api/agents"))
        .json(&json!({"name": agent, "provider": "claude", "session": "example-session"}))
        .send()
        .await?;
    println!("register: {}", res.status());

    // 2. Introspection + task progress over HTTP REST.
    for ev in [
        json!({"type": "introspection", "agent": agent, "kind": "thought",
               "content": "Planning the demo run"}),
        json!({"type": "task", "agent": agent, "task_id": "demo-root",
               "title": "Demo pipeline", "status": "running", "percent": 25, "meta": true}),
    ] {
        let res = client
            .post(format!("http://{http}/api/events"))
            .json(&ev)
            .send()
            .await?;
        println!("http event: {}", res.status());
    }

    // 3. Metacognition over the bidirectional WebSocket.
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{http}/ws/ingest")).await?;
    for (confidence, strategy) in [(0.4, None), (0.75, Some("chain-of-thought"))] {
        let mut ev = json!({"type": "metacognition", "agent": agent, "confidence": confidence});
        if let Some(s) = strategy {
            ev["strategy"] = json!(s);
        }
        ws.send(Message::Text(ev.to_string())).await?;
        if let Some(Ok(ack)) = ws.next().await {
            println!("ws ack: {}", ack.to_text().unwrap_or("<binary>"));
        }
    }
    ws.close(None).await?;

    // 4. Task progress over raw TCP (newline-delimited JSON).
    let stream = tokio::net::TcpStream::connect(&tcp).await?;
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();
    let ev = json!({"type": "task", "agent": agent, "task_id": "demo-root",
                    "title": "Demo pipeline", "status": "running", "percent": 80});
    write_half.write_all(format!("{ev}\n").as_bytes()).await?;
    if let Ok(Some(ack)) = lines.next_line().await {
        println!("tcp ack: {ack}");
    }

    // 5. A lesson learned over UDP (fire-and-forget, ack is best-effort).
    let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
    let ev = json!({"type": "learning", "agent": agent, "topic": "transports",
                    "lesson": "UDP suits high-volume low-stakes telemetry"});
    socket.send_to(ev.to_string().as_bytes(), &udp).await?;
    let mut buf = [0u8; 1024];
    if let Ok(Ok((len, _))) = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        socket.recv_from(&mut buf),
    )
    .await
    {
        println!("udp ack: {}", String::from_utf8_lossy(&buf[..len]));
    }

    println!("done — open http://{http}/agents/{agent}");
    Ok(())
}
