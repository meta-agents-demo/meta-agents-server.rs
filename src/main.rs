use clap::Parser;
use meta_agents_server::storage::StorageConfig;
use meta_agents_server::{start, ServerConfig};
use std::net::SocketAddr;

/// meta-agents-server: ingest AI-agent introspection, metacognition, task
/// progress and lessons over HTTP / WebSocket / TCP / UDP, and serve a web UI
/// to visualize it. Run in the foreground; for daemon use see the sample
/// systemd unit and launchd plist in deploy/.
#[derive(Parser, Debug)]
#[command(name = "meta-agents-server", version, about)]
struct Args {
    /// Bind address for the web UI, REST API and WebSockets.
    #[arg(long, env = "META_AGENTS_HTTP_ADDR", default_value = "127.0.0.1:7700")]
    http_addr: SocketAddr,

    /// Bind address for the raw TCP transport (newline-delimited JSON).
    #[arg(long, env = "META_AGENTS_TCP_ADDR", default_value = "127.0.0.1:7701")]
    tcp_addr: SocketAddr,

    /// Bind address for the UDP transport (JSON datagrams).
    #[arg(long, env = "META_AGENTS_UDP_ADDR", default_value = "127.0.0.1:7702")]
    udp_addr: SocketAddr,

    /// Max distinct agents kept in memory.
    #[arg(long, env = "META_AGENTS_MAX_AGENTS", default_value_t = 256)]
    max_agents: usize,

    /// Per-agent event ring capacity.
    #[arg(long, env = "META_AGENTS_EVENTS_PER_AGENT", default_value_t = 512)]
    events_per_agent: usize,

    /// Global recent-events ring capacity.
    #[arg(long, env = "META_AGENTS_RECENT_EVENTS", default_value_t = 1024)]
    recent_events: usize,

    /// Max task nodes kept in memory.
    #[arg(long, env = "META_AGENTS_MAX_TASKS", default_value_t = 512)]
    max_tasks: usize,

    /// Max lessons-learned entries kept in memory.
    #[arg(long, env = "META_AGENTS_MAX_LESSONS", default_value_t = 512)]
    max_lessons: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "meta_agents_server=info".into()),
        )
        .init();

    let args = Args::parse();
    let config = ServerConfig {
        http_addr: args.http_addr,
        tcp_addr: args.tcp_addr,
        udp_addr: args.udp_addr,
        storage: StorageConfig {
            max_agents: args.max_agents,
            events_per_agent: args.events_per_agent,
            recent_events: args.recent_events,
            sessions_per_agent: 16,
            max_tasks: args.max_tasks,
            max_lessons: args.max_lessons,
        },
    };

    let handle = start(config).await?;
    handle.wait().await;
    Ok(())
}
