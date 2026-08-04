//! Executable configuration and lifecycle composition.
//!
//! Transport implementations, storage, routing, and UI remain in the library;
//! this module owns only CLI parsing, logging startup, `ServerConfig`
//! construction, and waiting on the running server.

use std::net::SocketAddr;

use clap::Parser;
use meta_agents_server::storage::StorageConfig;
use meta_agents_server::{start, ServerConfig};

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

impl Args {
    fn into_server_config(self) -> ServerConfig {
        ServerConfig {
            http_addr: self.http_addr,
            tcp_addr: self.tcp_addr,
            udp_addr: self.udp_addr,
            storage: StorageConfig {
                max_agents: self.max_agents,
                events_per_agent: self.events_per_agent,
                recent_events: self.recent_events,
                sessions_per_agent: 16,
                max_tasks: self.max_tasks,
                max_lessons: self.max_lessons,
            },
        }
    }
}

/// Parse process configuration, start all transports, and wait for completion.
///
/// # Errors
///
/// Returns an error when one of the HTTP, TCP, or UDP listeners cannot bind.
pub async fn run() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "meta_agents_server=info".into()),
        )
        .init();

    let config = Args::parse().into_server_config();
    let handle = start(config).await?;
    handle.wait().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_cli_maps_to_the_existing_server_contract() {
        let config = Args::try_parse_from(["meta-agents-server"])
            .expect("default command line")
            .into_server_config();
        assert_eq!(config.http_addr, "127.0.0.1:7700".parse().unwrap());
        assert_eq!(config.tcp_addr, "127.0.0.1:7701".parse().unwrap());
        assert_eq!(config.udp_addr, "127.0.0.1:7702".parse().unwrap());
        assert_eq!(config.storage.max_agents, 256);
        assert_eq!(config.storage.events_per_agent, 512);
        assert_eq!(config.storage.recent_events, 1024);
        assert_eq!(config.storage.sessions_per_agent, 16);
        assert_eq!(config.storage.max_tasks, 512);
        assert_eq!(config.storage.max_lessons, 512);
    }

    #[test]
    fn explicit_cli_values_map_to_the_correct_transport_and_capacity() {
        let config = Args::try_parse_from([
            "meta-agents-server",
            "--http-addr",
            "0.0.0.0:8800",
            "--tcp-addr",
            "127.0.0.1:8801",
            "--udp-addr",
            "127.0.0.1:8802",
            "--max-agents",
            "17",
            "--events-per-agent",
            "31",
            "--recent-events",
            "47",
            "--max-tasks",
            "61",
            "--max-lessons",
            "73",
        ])
        .expect("valid explicit command line")
        .into_server_config();
        assert_eq!(config.http_addr, "0.0.0.0:8800".parse().unwrap());
        assert_eq!(config.tcp_addr, "127.0.0.1:8801".parse().unwrap());
        assert_eq!(config.udp_addr, "127.0.0.1:8802".parse().unwrap());
        assert_eq!(config.storage.max_agents, 17);
        assert_eq!(config.storage.events_per_agent, 31);
        assert_eq!(config.storage.recent_events, 47);
        assert_eq!(config.storage.sessions_per_agent, 16);
        assert_eq!(config.storage.max_tasks, 61);
        assert_eq!(config.storage.max_lessons, 73);
    }

    #[test]
    fn malformed_transport_addresses_fail_before_any_listener_starts() {
        assert!(
            Args::try_parse_from(["meta-agents-server", "--http-addr", "not-a-socket"]).is_err()
        );
    }

    #[test]
    fn help_preserves_the_product_description() {
        let help = Args::try_parse_from(["meta-agents-server", "--help"])
            .expect_err("help exits before startup")
            .to_string();
        assert!(help.contains("ingest AI-agent introspection"));
        assert!(help.contains("daemon use"));
    }

    #[test]
    fn executable_remains_a_thin_tokio_adapter() {
        let main = include_str!("main.rs");
        assert!(main.lines().count() <= 6);
        for bootstrap_symbol in [
            "clap::Parser",
            "StorageConfig",
            "ServerConfig",
            "tracing_subscriber",
            "handle.wait",
        ] {
            assert!(!main.contains(bootstrap_symbol));
        }
    }
}
