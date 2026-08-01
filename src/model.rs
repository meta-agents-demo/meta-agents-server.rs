//! Shared event schema. Every transport (HTTP, WebSocket, TCP, UDP) accepts
//! exactly these serde-JSON shapes.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Which LLM vendor an agent belongs to.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Chatgpt,
    Claude,
    Gemini,
    #[default]
    Other,
}

impl Provider {
    pub fn label(&self) -> &'static str {
        match self {
            Provider::Chatgpt => "ChatGPT",
            Provider::Claude => "Claude",
            Provider::Gemini => "Gemini",
            Provider::Other => "Other",
        }
    }
}

/// Payload used to register (or re-register) an agent.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AgentRegistration {
    /// Unique agent name; also the key used by every other event.
    pub name: String,
    #[serde(default)]
    pub provider: Provider,
    /// Opaque session identifier (conversation id, process id, ...).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<String>,
}

/// Flavor of an introspection event.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IntrospectionKind {
    Thought,
    Observation,
    SelfAssessment,
}

impl IntrospectionKind {
    pub fn label(&self) -> &'static str {
        match self {
            IntrospectionKind::Thought => "thought",
            IntrospectionKind::Observation => "observation",
            IntrospectionKind::SelfAssessment => "self-assessment",
        }
    }
}

/// Task lifecycle state.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Blocked,
    Done,
    Failed,
}

impl TaskStatus {
    pub fn label(&self) -> &'static str {
        match self {
            TaskStatus::Pending => "pending",
            TaskStatus::Running => "running",
            TaskStatus::Blocked => "blocked",
            TaskStatus::Done => "done",
            TaskStatus::Failed => "failed",
        }
    }
}

/// The single event envelope shared by all four transports.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    /// Announce an agent (idempotent upsert).
    Register { agent: AgentRegistration },
    /// A thought, observation or self-assessment.
    Introspection {
        agent: String,
        kind: IntrospectionKind,
        content: String,
    },
    /// Confidence levels, strategy changes and reflections.
    Metacognition {
        agent: String,
        /// 0.0 ..= 1.0
        #[serde(default, skip_serializing_if = "Option::is_none")]
        confidence: Option<f64>,
        /// Present when the agent switches strategy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        strategy: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reflection: Option<String>,
    },
    /// Task creation / progress update. Tasks form trees via `parent_id`;
    /// tasks reported by several agents become shared meta-tasks.
    Task {
        agent: String,
        task_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        parent_id: Option<String>,
        title: String,
        status: TaskStatus,
        /// 0 ..= 100
        #[serde(default)]
        percent: u8,
        /// Explicitly mark a task as a cross-agent meta-task.
        #[serde(default)]
        meta: bool,
    },
    /// A lessons-learned entry, queryable by topic.
    Learning {
        agent: String,
        topic: String,
        lesson: String,
    },
    /// Keep-alive ping used for the liveness display.
    Heartbeat { agent: String },
}

impl Event {
    /// Name of the agent the event refers to, if any.
    pub fn agent_name(&self) -> Option<&str> {
        match self {
            Event::Register { agent } => Some(&agent.name),
            Event::Introspection { agent, .. }
            | Event::Metacognition { agent, .. }
            | Event::Task { agent, .. }
            | Event::Learning { agent, .. }
            | Event::Heartbeat { agent } => Some(agent),
        }
    }

    pub fn kind_label(&self) -> &'static str {
        match self {
            Event::Register { .. } => "register",
            Event::Introspection { .. } => "introspection",
            Event::Metacognition { .. } => "metacognition",
            Event::Task { .. } => "task",
            Event::Learning { .. } => "learning",
            Event::Heartbeat { .. } => "heartbeat",
        }
    }
}

/// Transport an event arrived over.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    Http,
    Websocket,
    Tcp,
    Udp,
    Internal,
}

impl Transport {
    pub fn label(&self) -> &'static str {
        match self {
            Transport::Http => "http",
            Transport::Websocket => "websocket",
            Transport::Tcp => "tcp",
            Transport::Udp => "udp",
            Transport::Internal => "internal",
        }
    }
}

/// An event as recorded by the server.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StoredEvent {
    /// Globally monotonically increasing sequence number.
    pub seq: u64,
    /// Server receive time, ms since epoch.
    pub received_at_ms: u64,
    pub transport: Transport,
    #[serde(flatten)]
    pub event: Event,
}

/// Acknowledgement returned by HTTP / WebSocket / TCP ingestion.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Ack {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl Ack {
    pub fn ok(seq: u64) -> Self {
        Ack {
            ok: true,
            seq: Some(seq),
            error: None,
        }
    }

    pub fn err(msg: impl Into<String>) -> Self {
        Ack {
            ok: false,
            seq: None,
            error: Some(msg.into()),
        }
    }
}

/// Read model: one agent as shown on the dashboard.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AgentSnapshot {
    pub name: String,
    pub provider: Provider,
    /// Most recent session id first.
    pub sessions: Vec<String>,
    pub first_seen_ms: u64,
    pub last_seen_ms: u64,
    /// Total events ever ingested for this agent (not bounded by the LRU).
    pub events_ingested: u64,
    /// Latest reported confidence, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_confidence: Option<f64>,
    /// Latest reported strategy, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_strategy: Option<String>,
}

/// Read model: one task node.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TaskSnapshot {
    pub task_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    pub title: String,
    pub status: TaskStatus,
    pub percent: u8,
    /// True if flagged `meta` or reported by more than one agent.
    pub meta: bool,
    /// Every agent that has reported on this task.
    pub agents: Vec<String>,
    pub updated_at_ms: u64,
}

/// Read model: one lessons-learned entry.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Lesson {
    pub seq: u64,
    pub agent: String,
    pub topic: String,
    pub lesson: String,
    pub recorded_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_json_round_trip() {
        let json = r#"{"type":"metacognition","agent":"claude-1","confidence":0.72,"strategy":"breadth-first"}"#;
        let ev: Event = serde_json::from_str(json).expect("parse");
        match &ev {
            Event::Metacognition {
                agent,
                confidence,
                strategy,
                reflection,
            } => {
                assert_eq!(agent, "claude-1");
                assert_eq!(*confidence, Some(0.72));
                assert_eq!(strategy.as_deref(), Some("breadth-first"));
                assert!(reflection.is_none());
            }
            other => panic!("wrong variant: {other:?}"),
        }
        let back = serde_json::to_string(&ev).expect("serialize");
        let again: Event = serde_json::from_str(&back).expect("reparse");
        assert_eq!(again.agent_name(), Some("claude-1"));
    }

    #[test]
    fn register_defaults() {
        let json = r#"{"type":"register","agent":{"name":"g"}}"#;
        let ev: Event = serde_json::from_str(json).expect("parse");
        match ev {
            Event::Register { agent } => {
                assert_eq!(agent.provider, Provider::Other);
                assert!(agent.session.is_none());
            }
            other => panic!("wrong variant: {other:?}"),
        }
    }

    #[test]
    fn stored_event_flattens_event() {
        let stored = StoredEvent {
            seq: 7,
            received_at_ms: 123,
            transport: Transport::Udp,
            event: Event::Heartbeat {
                agent: "a".to_string(),
            },
        };
        let v = serde_json::to_value(&stored).expect("serialize");
        assert_eq!(v["type"], "heartbeat");
        assert_eq!(v["seq"], 7);
        assert_eq!(v["transport"], "udp");
    }
}
