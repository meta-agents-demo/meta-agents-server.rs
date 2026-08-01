//! In-memory storage built on bounded LRU caches (the `lru` crate), behind a
//! small trait so a real database can slot in later without touching the
//! transports or the UI.

use crate::model::{now_ms, AgentSnapshot, Event, Lesson, StoredEvent, TaskSnapshot, Transport};
use lru::LruCache;
use std::collections::BTreeSet;
use std::num::NonZeroUsize;
use std::sync::Mutex;

/// Capacity knobs for the LRU-backed store.
#[derive(Clone, Copy, Debug)]
pub struct StorageConfig {
    /// Max distinct agents kept.
    pub max_agents: usize,
    /// Per-agent event ring size.
    pub events_per_agent: usize,
    /// Global "recent events" ring size.
    pub recent_events: usize,
    /// Sessions remembered per agent.
    pub sessions_per_agent: usize,
    /// Max task nodes kept.
    pub max_tasks: usize,
    /// Max lessons kept.
    pub max_lessons: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            max_agents: 256,
            events_per_agent: 512,
            recent_events: 1024,
            sessions_per_agent: 16,
            max_tasks: 512,
            max_lessons: 512,
        }
    }
}

/// Abstract store. Everything the transports write and the UI reads goes
/// through this trait; `LruStorage` is the in-memory implementation.
pub trait Storage: Send + Sync {
    /// Record one event (auto-registering unknown agents) and return the
    /// stored form. Fails when the event is structurally invalid.
    fn ingest(&self, transport: Transport, event: Event) -> Result<StoredEvent, String>;
    /// All known agents, most recently seen first.
    fn agents(&self) -> Vec<AgentSnapshot>;
    /// One agent by name.
    fn agent(&self, name: &str) -> Option<AgentSnapshot>;
    /// Events for one agent, oldest first, capped at `limit`.
    fn events_for(&self, agent: &str, limit: usize) -> Vec<StoredEvent>;
    /// Most recent events across all agents, oldest first, capped at `limit`.
    fn recent_events(&self, limit: usize) -> Vec<StoredEvent>;
    /// All task nodes, most recently updated first.
    fn tasks(&self) -> Vec<TaskSnapshot>;
    /// Lessons learned, newest first, optionally filtered by (substring,
    /// case-insensitive) topic.
    fn lessons(&self, topic: Option<&str>) -> Vec<Lesson>;
}

struct AgentRecord {
    snapshot: AgentSnapshot,
    /// Bounded per-agent event ring keyed by sequence number.
    events: LruCache<u64, StoredEvent>,
    /// Bounded session cache: session id -> last seen ms.
    sessions: LruCache<String, u64>,
}

struct TaskRecord {
    snapshot: TaskSnapshot,
    agents: BTreeSet<String>,
}

struct Inner {
    seq: u64,
    agents: LruCache<String, AgentRecord>,
    recent: LruCache<u64, StoredEvent>,
    tasks: LruCache<String, TaskRecord>,
    lessons: LruCache<u64, Lesson>,
}

/// The in-memory, LRU-bounded implementation of [`Storage`].
pub struct LruStorage {
    config: StorageConfig,
    inner: Mutex<Inner>,
}

fn cap(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n.max(1)).expect("max(1) is non-zero")
}

impl LruStorage {
    pub fn new(config: StorageConfig) -> Self {
        LruStorage {
            inner: Mutex::new(Inner {
                seq: 0,
                agents: LruCache::new(cap(config.max_agents)),
                recent: LruCache::new(cap(config.recent_events)),
                tasks: LruCache::new(cap(config.max_tasks)),
                lessons: LruCache::new(cap(config.max_lessons)),
            }),
            config,
        }
    }
}

impl Default for LruStorage {
    fn default() -> Self {
        LruStorage::new(StorageConfig::default())
    }
}

impl Inner {
    fn upsert_agent(&mut self, config: &StorageConfig, name: &str, now: u64) -> &mut AgentRecord {
        if !self.agents.contains(name) {
            self.agents.put(
                name.to_string(),
                AgentRecord {
                    snapshot: AgentSnapshot {
                        name: name.to_string(),
                        provider: Default::default(),
                        sessions: Vec::new(),
                        first_seen_ms: now,
                        last_seen_ms: now,
                        events_ingested: 0,
                        last_confidence: None,
                        current_strategy: None,
                    },
                    events: LruCache::new(cap(config.events_per_agent)),
                    sessions: LruCache::new(cap(config.sessions_per_agent)),
                },
            );
        }
        self.agents.get_mut(name).expect("agent was just inserted")
    }
}

fn validated(event: &Event) -> Result<(), String> {
    let name = event.agent_name().unwrap_or("");
    if name.trim().is_empty() {
        return Err("agent name must be non-empty".to_string());
    }
    match event {
        Event::Metacognition {
            confidence,
            strategy,
            reflection,
            ..
        } => {
            if let Some(c) = confidence {
                if !(0.0..=1.0).contains(c) {
                    return Err(format!("confidence {c} outside 0.0..=1.0"));
                }
            }
            if confidence.is_none() && strategy.is_none() && reflection.is_none() {
                return Err(
                    "metacognition event needs confidence, strategy or reflection".to_string(),
                );
            }
            Ok(())
        }
        Event::Task {
            task_id, percent, ..
        } => {
            if task_id.trim().is_empty() {
                return Err("task_id must be non-empty".to_string());
            }
            if *percent > 100 {
                return Err(format!("percent {percent} outside 0..=100"));
            }
            Ok(())
        }
        Event::Learning { topic, .. } => {
            if topic.trim().is_empty() {
                return Err("topic must be non-empty".to_string());
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

impl Storage for LruStorage {
    fn ingest(&self, transport: Transport, event: Event) -> Result<StoredEvent, String> {
        validated(&event)?;
        let now = now_ms();
        let mut inner = self.inner.lock().expect("storage mutex poisoned");
        inner.seq += 1;
        let seq = inner.seq;
        let stored = StoredEvent {
            seq,
            received_at_ms: now,
            transport,
            event: event.clone(),
        };

        // Agent bookkeeping (every event names an agent).
        let name = event
            .agent_name()
            .expect("validated events name an agent")
            .to_string();
        {
            let record = inner.upsert_agent(&self.config, &name, now);
            record.snapshot.last_seen_ms = now;
            record.snapshot.events_ingested += 1;
            match &event {
                Event::Register { agent } => {
                    record.snapshot.provider = agent.provider;
                    if let Some(session) = &agent.session {
                        record.sessions.put(session.clone(), now);
                    }
                }
                Event::Metacognition {
                    confidence,
                    strategy,
                    ..
                } => {
                    if confidence.is_some() {
                        record.snapshot.last_confidence = *confidence;
                    }
                    if strategy.is_some() {
                        record.snapshot.current_strategy = strategy.clone();
                    }
                }
                _ => {}
            }
            record.events.put(seq, stored.clone());
        }

        // Task tree bookkeeping.
        if let Event::Task {
            task_id,
            parent_id,
            title,
            status,
            percent,
            meta,
            ..
        } = &event
        {
            if let Some(existing) = inner.tasks.get_mut(task_id) {
                existing.agents.insert(name.clone());
                existing.snapshot.parent_id = parent_id.clone();
                existing.snapshot.title = title.clone();
                existing.snapshot.status = *status;
                existing.snapshot.percent = *percent;
                existing.snapshot.meta =
                    existing.snapshot.meta || *meta || existing.agents.len() > 1;
                existing.snapshot.updated_at_ms = now;
            } else {
                let mut agents = BTreeSet::new();
                agents.insert(name.clone());
                inner.tasks.put(
                    task_id.clone(),
                    TaskRecord {
                        snapshot: TaskSnapshot {
                            task_id: task_id.clone(),
                            parent_id: parent_id.clone(),
                            title: title.clone(),
                            status: *status,
                            percent: *percent,
                            meta: *meta,
                            agents: Vec::new(),
                            updated_at_ms: now,
                        },
                        agents,
                    },
                );
            }
        }

        // Lessons-learned bookkeeping.
        if let Event::Learning { topic, lesson, .. } = &event {
            inner.lessons.put(
                seq,
                Lesson {
                    seq,
                    agent: name.clone(),
                    topic: topic.clone(),
                    lesson: lesson.clone(),
                    recorded_at_ms: now,
                },
            );
        }

        inner.recent.put(seq, stored.clone());
        Ok(stored)
    }

    fn agents(&self) -> Vec<AgentSnapshot> {
        let inner = self.inner.lock().expect("storage mutex poisoned");
        let mut out: Vec<AgentSnapshot> = inner
            .agents
            .iter()
            .map(|(_, record)| {
                let mut snap = record.snapshot.clone();
                let mut sessions: Vec<(String, u64)> = record
                    .sessions
                    .iter()
                    .map(|(s, at)| (s.clone(), *at))
                    .collect();
                sessions.sort_by_key(|(_, at)| std::cmp::Reverse(*at));
                snap.sessions = sessions.into_iter().map(|(s, _)| s).collect();
                snap
            })
            .collect();
        out.sort_by_key(|a| std::cmp::Reverse(a.last_seen_ms));
        out
    }

    fn agent(&self, name: &str) -> Option<AgentSnapshot> {
        self.agents().into_iter().find(|a| a.name == name)
    }

    fn events_for(&self, agent: &str, limit: usize) -> Vec<StoredEvent> {
        let inner = self.inner.lock().expect("storage mutex poisoned");
        let Some(record) = inner.agents.peek(agent) else {
            return Vec::new();
        };
        let mut events: Vec<StoredEvent> = record.events.iter().map(|(_, e)| e.clone()).collect();
        events.sort_by_key(|e| e.seq);
        if events.len() > limit {
            events.drain(..events.len() - limit);
        }
        events
    }

    fn recent_events(&self, limit: usize) -> Vec<StoredEvent> {
        let inner = self.inner.lock().expect("storage mutex poisoned");
        let mut events: Vec<StoredEvent> = inner.recent.iter().map(|(_, e)| e.clone()).collect();
        events.sort_by_key(|e| e.seq);
        if events.len() > limit {
            events.drain(..events.len() - limit);
        }
        events
    }

    fn tasks(&self) -> Vec<TaskSnapshot> {
        let inner = self.inner.lock().expect("storage mutex poisoned");
        let mut out: Vec<TaskSnapshot> = inner
            .tasks
            .iter()
            .map(|(_, record)| {
                let mut snap = record.snapshot.clone();
                snap.agents = record.agents.iter().cloned().collect();
                snap.meta = snap.meta || snap.agents.len() > 1;
                snap
            })
            .collect();
        out.sort_by_key(|t| std::cmp::Reverse(t.updated_at_ms));
        out
    }

    fn lessons(&self, topic: Option<&str>) -> Vec<Lesson> {
        let inner = self.inner.lock().expect("storage mutex poisoned");
        let needle = topic.map(|t| t.to_lowercase());
        let mut out: Vec<Lesson> = inner
            .lessons
            .iter()
            .map(|(_, l)| l.clone())
            .filter(|l| match &needle {
                Some(n) => l.topic.to_lowercase().contains(n),
                None => true,
            })
            .collect();
        out.sort_by_key(|l| std::cmp::Reverse(l.seq));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AgentRegistration, IntrospectionKind, Provider, TaskStatus};

    fn store() -> LruStorage {
        LruStorage::new(StorageConfig {
            max_agents: 4,
            events_per_agent: 4,
            recent_events: 8,
            sessions_per_agent: 2,
            max_tasks: 8,
            max_lessons: 4,
        })
    }

    fn register(s: &LruStorage, name: &str, provider: Provider) {
        s.ingest(
            Transport::Internal,
            Event::Register {
                agent: AgentRegistration {
                    name: name.to_string(),
                    provider,
                    session: Some(format!("{name}-session-1")),
                },
            },
        )
        .expect("register");
    }

    #[test]
    fn registration_and_snapshot() {
        let s = store();
        register(&s, "claude-1", Provider::Claude);
        let agents = s.agents();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].provider, Provider::Claude);
        assert_eq!(agents[0].sessions, vec!["claude-1-session-1".to_string()]);
        assert_eq!(agents[0].events_ingested, 1);
    }

    #[test]
    fn auto_registers_unknown_agents() {
        let s = store();
        s.ingest(
            Transport::Udp,
            Event::Heartbeat {
                agent: "ghost".to_string(),
            },
        )
        .expect("ingest");
        assert!(s.agent("ghost").is_some());
    }

    #[test]
    fn per_agent_event_ring_is_bounded() {
        let s = store();
        for i in 0..10 {
            s.ingest(
                Transport::Tcp,
                Event::Introspection {
                    agent: "a".to_string(),
                    kind: IntrospectionKind::Thought,
                    content: format!("thought {i}"),
                },
            )
            .expect("ingest");
        }
        let events = s.events_for("a", 100);
        assert_eq!(events.len(), 4, "ring capacity is 4");
        // Oldest entries were evicted; the newest survive in order.
        assert!(events.windows(2).all(|w| w[0].seq < w[1].seq));
        assert_eq!(events.last().expect("non-empty").seq, 10);
        // The total counter is not bounded by the ring.
        assert_eq!(s.agent("a").expect("agent").events_ingested, 10);
    }

    #[test]
    fn rejects_invalid_events() {
        let s = store();
        assert!(s
            .ingest(
                Transport::Http,
                Event::Heartbeat {
                    agent: "  ".to_string()
                }
            )
            .is_err());
        assert!(s
            .ingest(
                Transport::Http,
                Event::Metacognition {
                    agent: "a".to_string(),
                    confidence: Some(1.5),
                    strategy: None,
                    reflection: None,
                }
            )
            .is_err());
        assert!(s
            .ingest(
                Transport::Http,
                Event::Task {
                    agent: "a".to_string(),
                    task_id: "t".to_string(),
                    parent_id: None,
                    title: "x".to_string(),
                    status: TaskStatus::Running,
                    percent: 101,
                    meta: false,
                }
            )
            .is_err());
    }

    #[test]
    fn task_reported_by_two_agents_becomes_meta() {
        let s = store();
        for agent in ["claude-1", "gpt-1"] {
            s.ingest(
                Transport::Http,
                Event::Task {
                    agent: agent.to_string(),
                    task_id: "shared-1".to_string(),
                    parent_id: None,
                    title: "Cross-agent research".to_string(),
                    status: TaskStatus::Running,
                    percent: 30,
                    meta: false,
                },
            )
            .expect("ingest");
        }
        let tasks = s.tasks();
        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].meta);
        assert_eq!(tasks[0].agents.len(), 2);
    }

    #[test]
    fn lessons_filter_by_topic_substring() {
        let s = store();
        for (topic, lesson) in [
            ("rust-async", "spawn_blocking for CPU work"),
            ("prompting", "state the output format first"),
            ("rust-borrowck", "clone at the boundary"),
        ] {
            s.ingest(
                Transport::Websocket,
                Event::Learning {
                    agent: "a".to_string(),
                    topic: topic.to_string(),
                    lesson: lesson.to_string(),
                },
            )
            .expect("ingest");
        }
        assert_eq!(s.lessons(None).len(), 3);
        assert_eq!(s.lessons(Some("RUST")).len(), 2);
        assert_eq!(s.lessons(Some("prompt")).len(), 1);
        assert_eq!(s.lessons(Some("nope")).len(), 0);
    }
}
