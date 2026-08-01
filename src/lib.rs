//! meta-agents-server: one Rust binary that ingests AI-agent introspection,
//! metacognition, task-progress and lessons-learned events over HTTP,
//! WebSocket, raw TCP and UDP — and serves a server-rendered web UI to
//! visualize all of it.

pub mod ingest;
pub mod model;
pub mod state;
pub mod storage;
pub mod ui;

use crate::state::AppState;
use crate::storage::{LruStorage, StorageConfig};
use anyhow::Context;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, UdpSocket};
use tokio::task::JoinHandle;

/// Bind configuration for all three sockets.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// HTTP + WebSocket + web UI.
    pub http_addr: SocketAddr,
    /// Raw TCP, newline-delimited JSON.
    pub tcp_addr: SocketAddr,
    /// UDP JSON datagrams.
    pub udp_addr: SocketAddr,
    pub storage: StorageConfig,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            http_addr: "127.0.0.1:7700".parse().expect("valid literal"),
            tcp_addr: "127.0.0.1:7701".parse().expect("valid literal"),
            udp_addr: "127.0.0.1:7702".parse().expect("valid literal"),
            storage: StorageConfig::default(),
        }
    }
}

/// A running server: the actually-bound addresses plus task handles.
pub struct ServerHandle {
    pub http_addr: SocketAddr,
    pub tcp_addr: SocketAddr,
    pub udp_addr: SocketAddr,
    pub state: AppState,
    tasks: Vec<JoinHandle<()>>,
}

impl ServerHandle {
    /// Wait forever (until every listener task exits).
    pub async fn wait(mut self) {
        for task in std::mem::take(&mut self.tasks) {
            let _ = task.await;
        }
    }

    /// Stop all listener tasks.
    pub fn abort(&self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

/// Build the complete axum router (UI + REST API + WebSockets).
pub fn app_router(state: AppState) -> Router {
    Router::new()
        .merge(ui::ui_router())
        .merge(ingest::http::api_router())
        .merge(ingest::ws::ws_router())
        .with_state(state)
}

/// Bind all sockets and start serving. Returns once everything is listening,
/// with the actually-bound addresses (use port 0 to pick free ports).
pub async fn start(config: ServerConfig) -> anyhow::Result<ServerHandle> {
    let storage = Arc::new(LruStorage::new(config.storage));
    let state = AppState::new(storage);

    let http_listener = TcpListener::bind(config.http_addr)
        .await
        .with_context(|| format!("binding HTTP listener on {}", config.http_addr))?;
    let http_addr = http_listener.local_addr().context("http local_addr")?;

    let tcp_listener = TcpListener::bind(config.tcp_addr)
        .await
        .with_context(|| format!("binding TCP listener on {}", config.tcp_addr))?;
    let tcp_addr = tcp_listener.local_addr().context("tcp local_addr")?;

    let udp_socket = UdpSocket::bind(config.udp_addr)
        .await
        .with_context(|| format!("binding UDP socket on {}", config.udp_addr))?;
    let udp_addr = udp_socket.local_addr().context("udp local_addr")?;

    let app = app_router(state.clone());
    let http_task = tokio::spawn(async move {
        if let Err(e) = axum::serve(http_listener, app).await {
            tracing::error!("http server exited: {e}");
        }
    });
    let tcp_state = state.clone();
    let tcp_task = tokio::spawn(async move {
        ingest::tcp::serve(tcp_listener, tcp_state).await;
    });
    let udp_state = state.clone();
    let udp_task = tokio::spawn(async move {
        ingest::udp::serve(udp_socket, udp_state).await;
    });

    tracing::info!("web UI + REST + WebSocket on http://{http_addr}");
    tracing::info!("raw TCP (newline-delimited JSON) on {tcp_addr}");
    tracing::info!("UDP (JSON datagrams) on {udp_addr}");

    Ok(ServerHandle {
        http_addr,
        tcp_addr,
        udp_addr,
        state,
        tasks: vec![http_task, tcp_task, udp_task],
    })
}
