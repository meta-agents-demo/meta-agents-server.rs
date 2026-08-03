//! Shared application state: the storage handle plus the live-feed broadcast
//! channel every transport publishes into.

use crate::model::{Event, StoredEvent, Transport};
use crate::storage::{LruStorage, Storage};
use std::sync::Arc;
use tokio::sync::broadcast;

/// Agents are considered live if they reported anything within this window.
pub const LIVENESS_WINDOW_MS: u64 = 30_000;
/// Maximum accepted JSON payload across HTTP, WebSocket, TCP and UDP ingestion.
pub const MAX_INGEST_PAYLOAD_BYTES: usize = 64 * 1024;
/// Maximum number of events accepted in one JSON array.
pub const MAX_INGEST_BATCH_EVENTS: usize = 256;

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
    ///
    /// Arrays are validated in full before the first real write, so a malformed
    /// event cannot leave an earlier sibling committed or published to the live
    /// feed. Transport adapters also enforce this byte limit before dispatch.
    pub fn ingest_json(
        &self,
        transport: Transport,
        payload: &str,
    ) -> Result<Vec<StoredEvent>, String> {
        if payload.len() > MAX_INGEST_PAYLOAD_BYTES {
            return Err(format!(
                "payload exceeds {MAX_INGEST_PAYLOAD_BYTES} byte limit"
            ));
        }

        let trimmed = payload.trim();
        if trimmed.is_empty() {
            return Err("empty payload".to_string());
        }

        let events = if trimmed.starts_with('[') {
            let events: Vec<Event> =
                serde_json::from_str(trimmed).map_err(|e| format!("invalid JSON: {e}"))?;
            if events.is_empty() {
                return Err("event batch must not be empty".to_string());
            }
            if events.len() > MAX_INGEST_BATCH_EVENTS {
                return Err(format!(
                    "event batch exceeds {MAX_INGEST_BATCH_EVENTS} event limit"
                ));
            }
            events
        } else {
            vec![serde_json::from_str(trimmed).map_err(|e| format!("invalid JSON: {e}"))?]
        };

        // Reuse the production storage validator against an isolated store.
        // This deliberately happens before any mutation of the real store or
        // feed. The batch limit keeps the validation pass bounded.
        let validator = LruStorage::default();
        for event in &events {
            validator.ingest(Transport::Internal, event.clone())?;
        }

        events
            .into_iter()
            .map(|event| self.ingest(transport, event))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageConfig;
    use serde_json::json;

    fn state() -> AppState {
        AppState::new(Arc::new(LruStorage::new(StorageConfig {
            max_agents: 8,
            events_per_agent: 8,
            recent_events: 16,
            sessions_per_agent: 4,
            max_tasks: 8,
            max_lessons: 8,
        })))
    }

    #[test]
    fn invalid_batch_is_atomic_and_does_not_consume_sequence_numbers() {
        let state = state();
        let mut feed = state.feed.subscribe();
        let payload = json!([
            {"type": "heartbeat", "agent": "valid-first"},
            {
                "type": "task",
                "agent": "invalid-second",
                "task_id": "task-1",
                "parent_id": null,
                "title": "invalid progress",
                "status": "running",
                "percent": 101,
                "meta": false
            }
        ])
        .to_string();

        let error = state
            .ingest_json(Transport::Http, &payload)
            .expect_err("invalid second event must reject the entire batch");
        assert!(error.contains("percent 101"));
        assert!(state.storage.agents().is_empty());
        assert!(state.storage.recent_events(100).is_empty());
        assert!(feed.try_recv().is_err(), "invalid batch reached the live feed");

        let stored = state
            .ingest_json(
                Transport::Http,
                r#"{"type":"heartbeat","agent":"first-real-event"}"#,
            )
            .expect("valid event after rejected batch");
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].seq, 1, "rejected batch consumed a sequence number");
    }

    #[test]
    fn valid_batch_is_committed_in_order_with_one_transport() {
        let state = state();
        let stored = state
            .ingest_json(
                Transport::Tcp,
                r#"[
                    {"type":"heartbeat","agent":"agent-a"},
                    {"type":"heartbeat","agent":"agent-b"}
                ]"#,
            )
            .expect("valid batch");

        assert_eq!(stored.len(), 2);
        assert_eq!(stored[0].seq, 1);
        assert_eq!(stored[1].seq, 2);
        assert!(stored.iter().all(|event| event.transport == Transport::Tcp));
        assert_eq!(state.storage.recent_events(100).len(), 2);
    }

    #[test]
    fn empty_and_oversized_batches_fail_closed_without_mutation() {
        let state = state();
        assert_eq!(
            state.ingest_json(Transport::Http, "[]").unwrap_err(),
            "event batch must not be empty"
        );

        let events: Vec<_> = (0..=MAX_INGEST_BATCH_EVENTS)
            .map(|index| json!({"type": "heartbeat", "agent": format!("agent-{index}")}))
            .collect();
        let error = state
            .ingest_json(Transport::Http, &serde_json::to_string(&events).unwrap())
            .expect_err("oversized batch");
        assert!(error.contains("event batch exceeds"));
        assert!(state.storage.agents().is_empty());
        assert!(state.storage.recent_events(100).is_empty());
    }

    #[test]
    fn payload_byte_limit_is_checked_before_json_parsing() {
        let state = state();
        let payload = "x".repeat(MAX_INGEST_PAYLOAD_BYTES + 1);
        let error = state
            .ingest_json(Transport::Websocket, &payload)
            .expect_err("oversized payload");
        assert_eq!(
            error,
            format!("payload exceeds {MAX_INGEST_PAYLOAD_BYTES} byte limit")
        );
        assert!(state.storage.agents().is_empty());
    }
}
