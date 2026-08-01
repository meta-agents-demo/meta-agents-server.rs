//! End-to-end integration test: boot the real server on ephemeral ports, fire
//! events over all four transports (HTTP, WebSocket, TCP, UDP), and assert
//! everything shows up in the query API, the task tree, the lessons store and
//! the live feed.

use futures_util::{SinkExt, StreamExt};
use meta_agents_server::storage::StorageConfig;
use meta_agents_server::{start, ServerConfig, ServerHandle};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio_tungstenite::tungstenite::Message;

async fn boot() -> ServerHandle {
    let config = ServerConfig {
        http_addr: "127.0.0.1:0".parse().expect("literal"),
        tcp_addr: "127.0.0.1:0".parse().expect("literal"),
        udp_addr: "127.0.0.1:0".parse().expect("literal"),
        storage: StorageConfig::default(),
    };
    start(config).await.expect("server boots")
}

#[tokio::test]
async fn events_over_all_four_transports_show_up_in_queries() {
    let server = boot().await;
    let base = format!("http://{}", server.http_addr);
    let client = reqwest::Client::new();

    // --- 1. HTTP: register an agent and post an introspection event.
    let res = client
        .post(format!("{base}/api/agents"))
        .json(&json!({"name": "claude-1", "provider": "claude", "session": "sess-42"}))
        .send()
        .await
        .expect("register over http");
    assert_eq!(res.status(), 201);

    let res = client
        .post(format!("{base}/api/events"))
        .json(&json!({
            "type": "introspection",
            "agent": "claude-1",
            "kind": "thought",
            "content": "http works"
        }))
        .send()
        .await
        .expect("post event over http");
    assert_eq!(res.status(), 202);
    let acks: Vec<Value> = res.json().await.expect("ack json");
    assert_eq!(acks[0]["ok"], true);

    // Invalid events are rejected with 400.
    let res = client
        .post(format!("{base}/api/events"))
        .json(&json!({"type": "metacognition", "agent": "claude-1", "confidence": 7.0}))
        .send()
        .await
        .expect("post invalid event");
    assert_eq!(res.status(), 400);

    // --- 2. WebSocket: subscribe to the feed first, then ingest over WS.
    let (mut feed, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/ws/feed", server.http_addr))
            .await
            .expect("feed connects");

    let (mut ingest, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/ws/ingest", server.http_addr))
            .await
            .expect("ingest ws connects");
    let ws_event = json!({
        "type": "metacognition",
        "agent": "gpt-1",
        "confidence": 0.65,
        "strategy": "tree-of-thought"
    });
    ingest
        .send(Message::Text(ws_event.to_string()))
        .await
        .expect("send ws event");
    let ack = tokio::time::timeout(Duration::from_secs(5), ingest.next())
        .await
        .expect("ws ack in time")
        .expect("ws stream open")
        .expect("ws frame ok");
    let ack: Value = serde_json::from_str(ack.to_text().expect("text frame")).expect("ack json");
    assert_eq!(ack["ok"], true, "ws ingest acked: {ack}");

    // The feed subscriber sees the WS event too.
    let fed = tokio::time::timeout(Duration::from_secs(5), feed.next())
        .await
        .expect("feed frame in time")
        .expect("feed open")
        .expect("feed frame ok");
    let fed: Value = serde_json::from_str(fed.to_text().expect("text frame")).expect("feed json");
    assert_eq!(fed["type"], "metacognition");
    assert_eq!(fed["transport"], "websocket");

    // --- 3. TCP: newline-delimited JSON, one ack line per event line.
    let stream = tokio::net::TcpStream::connect(server.tcp_addr)
        .await
        .expect("tcp connects");
    let (read_half, mut write_half) = stream.into_split();
    let mut lines = BufReader::new(read_half).lines();
    let tcp_events = [
        json!({"type": "task", "agent": "gemini-1", "task_id": "root-1",
               "title": "Meta research plan", "status": "running", "percent": 40, "meta": true}),
        json!({"type": "task", "agent": "gemini-1", "task_id": "child-1", "parent_id": "root-1",
               "title": "Collect sources", "status": "done", "percent": 100}),
        json!({"type": "task", "agent": "claude-1", "task_id": "root-1",
               "title": "Meta research plan", "status": "running", "percent": 55}),
    ];
    for ev in &tcp_events {
        write_half
            .write_all(format!("{ev}\n").as_bytes())
            .await
            .expect("tcp write");
        let ack = tokio::time::timeout(Duration::from_secs(5), lines.next_line())
            .await
            .expect("tcp ack in time")
            .expect("tcp read ok")
            .expect("tcp ack line");
        let ack: Value = serde_json::from_str(&ack).expect("tcp ack json");
        assert_eq!(ack["ok"], true, "tcp acked: {ack}");
    }

    // --- 4. UDP: JSON datagrams (ack comes back best-effort).
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("udp client binds");
    let udp_event = json!({
        "type": "learning",
        "agent": "gemini-1",
        "topic": "rust-async",
        "lesson": "UDP datagrams need no connection"
    });
    udp.send_to(udp_event.to_string().as_bytes(), server.udp_addr)
        .await
        .expect("udp send");
    let mut buf = [0u8; 2048];
    let (len, _) = tokio::time::timeout(Duration::from_secs(5), udp.recv_from(&mut buf))
        .await
        .expect("udp ack in time")
        .expect("udp recv ok");
    let ack: Value = serde_json::from_slice(&buf[..len]).expect("udp ack json");
    assert_eq!(ack["ok"], true, "udp acked: {ack}");

    // --- Now assert everything is visible through the query API.
    let agents: Vec<Value> = client
        .get(format!("{base}/api/agents"))
        .send()
        .await
        .expect("list agents")
        .json()
        .await
        .expect("agents json");
    let names: Vec<&str> = agents
        .iter()
        .map(|a| a["name"].as_str().expect("name"))
        .collect();
    for expected in ["claude-1", "gpt-1", "gemini-1"] {
        assert!(names.contains(&expected), "{expected} in {names:?}");
    }
    let claude = agents
        .iter()
        .find(|a| a["name"] == "claude-1")
        .expect("claude present");
    assert_eq!(claude["provider"], "claude");
    assert_eq!(claude["live"], true);
    assert_eq!(claude["sessions"][0], "sess-42");
    let gpt = agents
        .iter()
        .find(|a| a["name"] == "gpt-1")
        .expect("gpt present");
    assert_eq!(gpt["last_confidence"], 0.65);
    assert_eq!(gpt["current_strategy"], "tree-of-thought");

    // Per-agent events, transports intact.
    let events: Vec<Value> = client
        .get(format!("{base}/api/agents/claude-1/events"))
        .send()
        .await
        .expect("agent events")
        .json()
        .await
        .expect("events json");
    assert!(events
        .iter()
        .any(|e| e["type"] == "introspection" && e["transport"] == "http"));
    assert!(events
        .iter()
        .any(|e| e["type"] == "task" && e["transport"] == "tcp"));

    // Task tree: root has both agents and became a meta-task; child links up.
    let tasks: Vec<Value> = client
        .get(format!("{base}/api/tasks"))
        .send()
        .await
        .expect("tasks")
        .json()
        .await
        .expect("tasks json");
    let root = tasks
        .iter()
        .find(|t| t["task_id"] == "root-1")
        .expect("root task");
    assert_eq!(root["meta"], true);
    assert_eq!(root["percent"], 55);
    let root_agents = root["agents"].as_array().expect("agents array");
    assert_eq!(root_agents.len(), 2);
    let child = tasks
        .iter()
        .find(|t| t["task_id"] == "child-1")
        .expect("child task");
    assert_eq!(child["parent_id"], "root-1");
    assert_eq!(child["status"], "done");

    // Lessons: queryable by topic, and the UDP-ingested lesson is there.
    let lessons: Vec<Value> = client
        .get(format!("{base}/api/lessons?topic=rust"))
        .send()
        .await
        .expect("lessons")
        .json()
        .await
        .expect("lessons json");
    assert_eq!(lessons.len(), 1);
    assert_eq!(lessons[0]["agent"], "gemini-1");
    assert_eq!(lessons[0]["transport"], Value::Null); // lessons are their own read model
    assert!(lessons[0]["lesson"]
        .as_str()
        .expect("lesson text")
        .contains("UDP"));
    let none: Vec<Value> = client
        .get(format!("{base}/api/lessons?topic=nomatch"))
        .send()
        .await
        .expect("lessons empty")
        .json()
        .await
        .expect("lessons json");
    assert!(none.is_empty());

    // Recent events include all four transports.
    let recent: Vec<Value> = client
        .get(format!("{base}/api/events/recent"))
        .send()
        .await
        .expect("recent")
        .json()
        .await
        .expect("recent json");
    for transport in ["http", "websocket", "tcp", "udp"] {
        assert!(
            recent.iter().any(|e| e["transport"] == transport),
            "recent events include {transport}"
        );
    }
}

#[tokio::test]
async fn ui_pages_render() {
    let server = boot().await;
    let base = format!("http://{}", server.http_addr);
    let client = reqwest::Client::new();

    // Seed one agent with data so pages have content.
    for ev in [
        json!({"type": "register", "agent": {"name": "claude-ui", "provider": "claude"}}),
        json!({"type": "metacognition", "agent": "claude-ui", "confidence": 0.4}),
        json!({"type": "metacognition", "agent": "claude-ui", "confidence": 0.8,
               "strategy": "self-consistency"}),
        json!({"type": "task", "agent": "claude-ui", "task_id": "t1",
               "title": "Render the UI", "status": "running", "percent": 60}),
        json!({"type": "learning", "agent": "claude-ui", "topic": "ssr",
               "lesson": "render_to_string needs no wasm"}),
    ] {
        let res = client
            .post(format!("{base}/api/events"))
            .json(&ev)
            .send()
            .await
            .expect("seed event");
        assert_eq!(res.status(), 202);
    }

    for (path, needle) in [
        ("/", "claude-ui"),
        ("/agents/claude-ui", "<svg"),
        ("/agents/claude-ui", "self-consistency"),
        ("/tasks", "Render the UI"),
        ("/lessons", "render_to_string needs no wasm"),
        ("/lessons?topic=ssr", "render_to_string"),
        ("/assets/style.css", "--series-1"),
        ("/assets/live.js", "/ws/feed"),
    ] {
        let res = client
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {path}: {e}"));
        assert_eq!(res.status(), 200, "GET {path}");
        let body = res.text().await.expect("body");
        assert!(body.contains(needle), "{path} contains {needle:?}");
    }

    // Unknown agent 404s.
    let res = client
        .get(format!("{base}/agents/nobody"))
        .send()
        .await
        .expect("unknown agent page");
    assert_eq!(res.status(), 404);
}
