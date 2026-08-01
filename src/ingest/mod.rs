//! Ingestion transports: HTTP REST + WebSocket (axum), raw TCP (newline
//! delimited JSON) and UDP (JSON datagrams). All four accept the same event
//! schema from [`crate::model`].

pub mod http;
pub mod tcp;
pub mod udp;
pub mod ws;
