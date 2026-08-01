//! HTTP REST API: registration, event ingestion and read-side queries.

use crate::model::{now_ms, Ack, AgentRegistration, Event, Transport};
use crate::state::{AppState, LIVENESS_WINDOW_MS};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;

pub fn api_router() -> Router<AppState> {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/agents", get(list_agents).post(register_agent))
        .route("/api/agents/:name", get(get_agent))
        .route("/api/agents/:name/events", get(agent_events))
        .route("/api/events", post(post_events))
        .route("/api/events/recent", get(recent_events))
        .route("/api/tasks", get(list_tasks))
        .route("/api/lessons", get(list_lessons))
}

async fn health(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!({
        "ok": true,
        "uptime_ms": now_ms().saturating_sub(state.started_at_ms),
        "agents": state.storage.agents().len(),
    }))
}

#[derive(Serialize)]
struct AgentListEntry {
    #[serde(flatten)]
    snapshot: crate::model::AgentSnapshot,
    live: bool,
}

fn with_liveness(snapshot: crate::model::AgentSnapshot) -> AgentListEntry {
    let live = now_ms().saturating_sub(snapshot.last_seen_ms) <= LIVENESS_WINDOW_MS;
    AgentListEntry { snapshot, live }
}

async fn list_agents(State(state): State<AppState>) -> Json<Vec<AgentListEntry>> {
    Json(
        state
            .storage
            .agents()
            .into_iter()
            .map(with_liveness)
            .collect(),
    )
}

async fn get_agent(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    match state.storage.agent(&name) {
        Some(snapshot) => Json(with_liveness(snapshot)).into_response(),
        None => (StatusCode::NOT_FOUND, Json(Ack::err("unknown agent"))).into_response(),
    }
}

async fn register_agent(
    State(state): State<AppState>,
    Json(registration): Json<AgentRegistration>,
) -> Response {
    match state.ingest(
        Transport::Http,
        Event::Register {
            agent: registration,
        },
    ) {
        Ok(stored) => (StatusCode::CREATED, Json(Ack::ok(stored.seq))).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(Ack::err(e))).into_response(),
    }
}

/// Accepts a single event object or an array of events.
async fn post_events(State(state): State<AppState>, body: String) -> Response {
    match state.ingest_json(Transport::Http, &body) {
        Ok(stored) => {
            let acks: Vec<Ack> = stored.iter().map(|s| Ack::ok(s.seq)).collect();
            (StatusCode::ACCEPTED, Json(acks)).into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, Json(Ack::err(e))).into_response(),
    }
}

#[derive(Deserialize)]
struct LimitQuery {
    #[serde(default = "default_limit")]
    limit: usize,
}

fn default_limit() -> usize {
    100
}

async fn agent_events(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(q): Query<LimitQuery>,
) -> Json<Vec<crate::model::StoredEvent>> {
    Json(state.storage.events_for(&name, q.limit))
}

async fn recent_events(
    State(state): State<AppState>,
    Query(q): Query<LimitQuery>,
) -> Json<Vec<crate::model::StoredEvent>> {
    Json(state.storage.recent_events(q.limit))
}

async fn list_tasks(State(state): State<AppState>) -> Json<Vec<crate::model::TaskSnapshot>> {
    Json(state.storage.tasks())
}

#[derive(Deserialize)]
struct LessonsQuery {
    topic: Option<String>,
}

async fn list_lessons(
    State(state): State<AppState>,
    Query(q): Query<LessonsQuery>,
) -> Json<Vec<crate::model::Lesson>> {
    Json(state.storage.lessons(q.topic.as_deref()))
}
