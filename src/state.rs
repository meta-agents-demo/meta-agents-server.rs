//! Shared application state: the storage handle plus the live-feed broadcast
//! channel every transport publishes into.

use crate::model::{Event, StoredEvent, Transport};
use crate::storage::Storage;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Agents are considered live if they reported anything within this window.
pub const LIVENESS_WINDOW_MS: u64 = 30_000;

#[derive(Clone)]
pub struct AppState {
    pub storage: Arc<dyn Storage>,
    /// Serialized [`StoredEvent`] JSON, fanned out to `/ws/feed` subscribers.
    pub feed: broadcast::Sender<String>,
    pub started_at_ms: u64,
}

impl AppState {
    pub fn new(storage: Arc<dyn Storage>) -> Self {
        let (feed, _) = broadcast::channel(256);
        AppState {
            storage,
            feed,
            started_at_ms: crate::model::now_ms(),
        }
    }

    /// Store an event and publish it to the live feed. Every transport funnels
    /// through here so behavior stays identical across HTTP/WS/TCP/UDP.
    pub fn ingest(&self, transport: Transport, event: Event) -> Result<StoredEvent, String> {
        let stored = self.storage.ingest(transport, event)?;
        if let Ok(json) = serde_json::to_string(&stored) {
            // No subscribers is fine; ignore the send error.
            let _ = self.feed.send(json);
        }
        Ok(stored)
    }

    /// Parse a JSON payload (one event or an array of events) and ingest it.
    pub fn ingest_json(
        &self,
        transport: Transport,
        payload: &str,
    ) -> Result<Vec<StoredEvent>, String> {
        let trimmed = payload.trim();
        if trimmed.is_empty() {
            return Err("empty payload".to_string());
        }
        if trimmed.starts_with('[') {
            let events: Vec<Event> =
                serde_json::from_str(trimmed).map_err(|e| format!("invalid JSON: {e}"))?;
            events
                .into_iter()
                .map(|ev| self.ingest(transport, ev))
                .collect()
        } else {
            let event: Event =
                serde_json::from_str(trimmed).map_err(|e| format!("invalid JSON: {e}"))?;
            Ok(vec![self.ingest(transport, event)?])
        }
    }
}
