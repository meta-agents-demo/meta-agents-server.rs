use meta_agents_server::model::{Event, Transport};
use meta_agents_server::state::MAX_INGEST_PAYLOAD_BYTES;
use meta_agents_server::storage::StorageConfig;
use meta_agents_server::{start, ServerConfig, ServerHandle};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

async fn boot() -> ServerHandle {
    start(ServerConfig {
        http_addr: "127.0.0.1:0".parse().expect("literal"),
        tcp_addr: "127.0.0.1:0".parse().expect("literal"),
        udp_addr: "127.0.0.1:0".parse().expect("literal"),
        storage: StorageConfig::default(),
    })
    .await
    .expect("server boots")
}

#[tokio::test]
async fn invalid_http_batch_is_atomic_and_next_sequence_starts_at_one() {
    let server = boot().await;
    let base = format!("http://{}", server.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base}/api/events"))
        .header("content-type", "application/json")
        .body(
            json!([
                {"type": "heartbeat", "agent": "would-have-been-first"},
                {
                    "type": "task",
                    "agent": "invalid-second",
                    "task_id": "task-1",
                    "title": "invalid progress",
                    "status": "running",
                    "percent": 101
                }
            ])
            .to_string(),
        )
        .send()
        .await
        .expect("invalid batch request");
    assert_eq!(response.status(), 400);

    let agents: Vec<Value> = client
        .get(format!("{base}/api/agents"))
        .send()
        .await
        .expect("agents request")
        .json()
        .await
        .expect("agents JSON");
    assert!(
        agents.is_empty(),
        "invalid batch partially created an agent"
    );

    let recent: Vec<Value> = client
        .get(format!("{base}/api/events/recent"))
        .send()
        .await
        .expect("recent request")
        .json()
        .await
        .expect("recent JSON");
    assert!(recent.is_empty(), "invalid batch partially stored an event");

    let response = client
        .post(format!("{base}/api/events"))
        .json(&json!({"type": "heartbeat", "agent": "first-real-event"}))
        .send()
        .await
        .expect("valid event request");
    assert_eq!(response.status(), 202);
    let acknowledgements: Vec<Value> = response.json().await.expect("ack JSON");
    assert_eq!(acknowledgements[0]["seq"], 1);
}

#[tokio::test]
async fn http_rejects_oversized_bodies_before_json_parsing() {
    let server = boot().await;
    let response = reqwest::Client::new()
        .post(format!("http://{}/api/events", server.http_addr))
        .header("content-type", "application/json")
        .body("x".repeat(MAX_INGEST_PAYLOAD_BYTES + 1))
        .send()
        .await
        .expect("oversized request");
    assert_eq!(response.status(), 413);
    assert!(server.state.storage.recent_events(10).is_empty());
}

#[tokio::test]
async fn tcp_rejects_oversized_frames_and_closes_the_connection() {
    let server = boot().await;
    let stream = tokio::net::TcpStream::connect(server.tcp_addr)
        .await
        .expect("tcp connects");
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    write_half
        .write_all(&vec![b'x'; MAX_INGEST_PAYLOAD_BYTES + 1])
        .await
        .expect("oversized frame body");
    write_half.write_all(b"\n").await.expect("frame delimiter");

    let line = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("oversized ack in time")
        .expect("tcp read")
        .expect("ack line");
    let ack: Value = serde_json::from_str(&line).expect("ack JSON");
    assert_eq!(ack["ok"], false);
    assert!(ack["error"]
        .as_str()
        .expect("error text")
        .contains("payload exceeds"));

    let closed = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("connection closes in time")
        .expect("tcp read after rejection");
    assert!(closed.is_none());
    assert!(server.state.storage.recent_events(10).is_empty());
}

#[tokio::test]
async fn tcp_recovers_after_non_utf8_frame_without_storing_it() {
    let server = boot().await;
    let stream = tokio::net::TcpStream::connect(server.tcp_addr)
        .await
        .expect("tcp connects");
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();

    write_half
        .write_all(&[0xff, b'\n'])
        .await
        .expect("invalid UTF-8 frame");
    let invalid_ack = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("invalid UTF-8 ack in time")
        .expect("tcp read")
        .expect("invalid UTF-8 ack line");
    let invalid_ack: Value = serde_json::from_str(&invalid_ack).expect("ack JSON");
    assert_eq!(invalid_ack["ok"], false);
    assert_eq!(invalid_ack["error"], "payload must be UTF-8");
    assert!(server.state.storage.recent_events(10).is_empty());

    write_half
        .write_all(b"{\"type\":\"heartbeat\",\"agent\":\"after-invalid-utf8\"}\n")
        .await
        .expect("valid frame");
    let valid_ack = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
        .await
        .expect("valid ack in time")
        .expect("tcp read")
        .expect("valid ack line");
    let valid_ack: Value = serde_json::from_str(&valid_ack).expect("ack JSON");
    assert_eq!(valid_ack["ok"], true);
    assert_eq!(valid_ack["seq"], 1);
}

#[tokio::test]
async fn read_side_limit_is_capped_at_one_thousand_events() {
    let server = boot().await;
    for index in 0..1_005 {
        server
            .state
            .ingest(
                Transport::Internal,
                Event::Heartbeat {
                    agent: format!("agent-{index}"),
                },
            )
            .expect("seed event");
    }

    let events: Vec<Value> = reqwest::Client::new()
        .get(format!(
            "http://{}/api/events/recent?limit={}",
            server.http_addr,
            usize::MAX
        ))
        .send()
        .await
        .expect("recent events request")
        .json()
        .await
        .expect("recent events JSON");
    assert_eq!(events.len(), 1_000);
    assert!(events.windows(2).all(|window| {
        window[0]["seq"].as_u64().expect("left seq") < window[1]["seq"].as_u64().expect("right seq")
    }));
}
