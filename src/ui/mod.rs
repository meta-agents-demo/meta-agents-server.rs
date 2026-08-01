//! Server-rendered web UI built with Leptos (SSR mode, rendered to strings via
//! `leptos::ssr::render_to_string` inside axum handlers). A small dependency-
//! free inline script drives the live feed over `/ws/feed`.

mod chart;

use crate::model::{now_ms, Event, Provider, StoredEvent, TaskSnapshot, TaskStatus};
use crate::state::{AppState, LIVENESS_WINDOW_MS};
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use leptos::{view, CollectView, IntoView};
use serde::Deserialize;
use std::collections::BTreeMap;

const STYLE_CSS: &str = include_str!("style.css");
const LIVE_JS: &str = include_str!("live.js");

pub fn ui_router() -> Router<AppState> {
    Router::new()
        .route("/", get(dashboard))
        .route("/agents/:name", get(agent_page))
        .route("/tasks", get(tasks_page))
        .route("/lessons", get(lessons_page))
        .route("/assets/style.css", get(style_css))
        .route("/assets/live.js", get(live_js))
}

async fn style_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STYLE_CSS,
    )
}

async fn live_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        LIVE_JS,
    )
}

/// Render a page body inside the shared shell and produce a full document.
fn render_page(
    title: String,
    active: &'static str,
    with_live_js: bool,
    body: impl FnOnce() -> leptos::View + 'static,
) -> Html<String> {
    let rendered = leptos::ssr::render_to_string(move || {
        let nav = [
            ("/", "Dashboard", "dashboard"),
            ("/tasks", "Tasks", "tasks"),
            ("/lessons", "Lessons", "lessons"),
        ];
        view! {
            <html lang="en">
                <head>
                    <meta charset="utf-8"/>
                    <meta name="viewport" content="width=device-width, initial-scale=1"/>
                    <title>{title.clone()}</title>
                    <link rel="stylesheet" href="/assets/style.css"/>
                </head>
                <body>
                    <header class="topbar">
                        <span class="brand">"meta-agents"</span>
                        <nav>
                            {nav
                                .iter()
                                .map(|(href, label, key)| {
                                    let class = if *key == active { "nav-link active" } else { "nav-link" };
                                    view! { <a class=class href=*href>{*label}</a> }
                                })
                                .collect_view()}
                        </nav>
                    </header>
                    <main>{body()}</main>
                    {with_live_js.then(|| view! { <script src="/assets/live.js"></script> })}
                </body>
            </html>
        }
    });
    Html(format!("<!DOCTYPE html>{rendered}"))
}

fn ago(now: u64, then: u64) -> String {
    let d = now.saturating_sub(then);
    if d < 2_000 {
        "just now".to_string()
    } else if d < 120_000 {
        format!("{}s ago", d / 1_000)
    } else if d < 7_200_000 {
        format!("{}m ago", d / 60_000)
    } else {
        format!("{}h ago", d / 3_600_000)
    }
}

fn provider_class(p: Provider) -> &'static str {
    match p {
        Provider::Chatgpt => "chip provider-chatgpt",
        Provider::Claude => "chip provider-claude",
        Provider::Gemini => "chip provider-gemini",
        Provider::Other => "chip provider-other",
    }
}

fn event_summary(ev: &Event) -> String {
    match ev {
        Event::Register { agent } => format!(
            "registered ({}{})",
            agent.provider.label(),
            agent
                .session
                .as_ref()
                .map(|s| format!(", session {s}"))
                .unwrap_or_default()
        ),
        Event::Introspection { kind, content, .. } => {
            format!("[{}] {content}", kind.label())
        }
        Event::Metacognition {
            confidence,
            strategy,
            reflection,
            ..
        } => {
            let mut parts = Vec::new();
            if let Some(c) = confidence {
                parts.push(format!("confidence {:.0}%", c * 100.0));
            }
            if let Some(s) = strategy {
                parts.push(format!("strategy -> {s}"));
            }
            if let Some(r) = reflection {
                parts.push(r.clone());
            }
            parts.join(" | ")
        }
        Event::Task {
            task_id,
            title,
            status,
            percent,
            ..
        } => format!("{title} ({task_id}): {} {percent}%", status.label()),
        Event::Learning { topic, lesson, .. } => format!("[{topic}] {lesson}"),
        Event::Heartbeat { .. } => "heartbeat".to_string(),
    }
}

fn feed_table(events: &[StoredEvent], now: u64) -> leptos::View {
    let rows = events
        .iter()
        .rev()
        .map(|e| {
            let agent = e.event.agent_name().unwrap_or("-").to_string();
            let href = format!("/agents/{agent}");
            view! {
                <tr>
                    <td class="num">{e.seq}</td>
                    <td class="muted">{ago(now, e.received_at_ms)}</td>
                    <td><a href=href>{agent}</a></td>
                    <td><span class="chip kind">{e.event.kind_label()}</span></td>
                    <td class="muted">{e.transport.label()}</td>
                    <td class="summary">{event_summary(&e.event)}</td>
                </tr>
            }
        })
        .collect_view();
    view! {
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>"#"</th>
                        <th>"When"</th>
                        <th>"Agent"</th>
                        <th>"Event"</th>
                        <th>"Via"</th>
                        <th>"Summary"</th>
                    </tr>
                </thead>
                <tbody id="live-feed">{rows}</tbody>
            </table>
        </div>
    }
    .into_view()
}

async fn dashboard(State(state): State<AppState>) -> Html<String> {
    let now = now_ms();
    let agents = state.storage.agents();
    let recent = state.storage.recent_events(30);
    render_page(
        "meta-agents — dashboard".to_string(),
        "dashboard",
        true,
        move || {
            let cards = if agents.is_empty() {
                view! {
                <p class="empty">
                    "No agents yet. Register one: "
                    <code>r#"curl -X POST localhost:7700/api/agents -d '{"name":"claude-1","provider":"claude"}'"#</code>
                </p>
            }
            .into_view()
            } else {
                agents
                .iter()
                .map(|a| {
                    let live = now.saturating_sub(a.last_seen_ms) <= LIVENESS_WINDOW_MS;
                    let (dot, live_label) = if live {
                        ("dot live", "live")
                    } else {
                        ("dot offline", "offline")
                    };
                    let href = format!("/agents/{}", a.name);
                    view! {
                        <a class="card agent-card" href=href>
                            <div class="card-head">
                                <span class="agent-name">{a.name.clone()}</span>
                                <span class="liveness"><span class=dot></span>{live_label}</span>
                            </div>
                            <div class="card-row">
                                <span class=provider_class(a.provider)>{a.provider.label()}</span>
                                <span class="muted">{format!("{} events", a.events_ingested)}</span>
                                <span class="muted">{ago(now, a.last_seen_ms)}</span>
                            </div>
                            <div class="card-row">
                                {a.last_confidence
                                    .map(|c| view! { <span>{format!("confidence {:.0}%", c * 100.0)}</span> })}
                                {a.current_strategy
                                    .clone()
                                    .map(|s| view! { <span class="muted">{format!("strategy: {s}")}</span> })}
                            </div>
                            {(!a.sessions.is_empty()).then(|| view! {
                                <div class="card-row muted small">
                                    {format!("sessions: {}", a.sessions.join(", "))}
                                </div>
                            })}
                        </a>
                    }
                })
                .collect_view()
            };
            view! {
            <h1>"Agents"</h1>
            <div class="cards">{cards}</div>
            <h2>"Live event feed"</h2>
            <p class="muted small">"Streams over the browser WebSocket at /ws/feed; newest first."</p>
            {feed_table(&recent, now)}
        }
        .into_view()
        },
    )
}

async fn agent_page(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let now = now_ms();
    let Some(agent) = state.storage.agent(&name) else {
        return (
            StatusCode::NOT_FOUND,
            Html("<h1>Unknown agent</h1>".to_string()),
        )
            .into_response();
    };
    let events = state.storage.events_for(&name, 500);

    // Metacognition timeline inputs.
    let mut confidence_points: Vec<(u64, f64)> = Vec::new();
    let mut strategy_shifts: Vec<(u64, String)> = Vec::new();
    let mut reflections: Vec<(u64, String)> = Vec::new();
    let mut introspections: Vec<(u64, String, String)> = Vec::new();
    for e in &events {
        match &e.event {
            Event::Metacognition {
                confidence,
                strategy,
                reflection,
                ..
            } => {
                if let Some(c) = confidence {
                    confidence_points.push((e.received_at_ms, *c));
                }
                if let Some(s) = strategy {
                    strategy_shifts.push((e.received_at_ms, s.clone()));
                }
                if let Some(r) = reflection {
                    reflections.push((e.received_at_ms, r.clone()));
                }
            }
            Event::Introspection { kind, content, .. } => {
                introspections.push((e.received_at_ms, kind.label().to_string(), content.clone()));
            }
            _ => {}
        }
    }
    let svg = chart::confidence_svg(&confidence_points, &strategy_shifts, now);
    let live = now.saturating_sub(agent.last_seen_ms) <= LIVENESS_WINDOW_MS;

    render_page(format!("meta-agents — {name}"), "dashboard", false, move || {
        let (dot, live_label) = if live { ("dot live", "live") } else { ("dot offline", "offline") };
        let shifts = strategy_shifts
            .iter()
            .rev()
            .map(|(at, s)| {
                view! {
                    <li>
                        <span class="muted">{ago(now, *at)}</span>
                        " switched strategy to "
                        <strong>{s.clone()}</strong>
                    </li>
                }
            })
            .collect_view();
        let refl = reflections
            .iter()
            .rev()
            .map(|(at, r)| {
                view! { <li><span class="muted">{ago(now, *at)}</span>" "{r.clone()}</li> }
            })
            .collect_view();
        let intro = introspections
            .iter()
            .rev()
            .map(|(at, kind, content)| {
                view! {
                    <li>
                        <span class="muted">{ago(now, *at)}</span>
                        <span class="chip kind">{kind.clone()}</span>
                        " "
                        {content.clone()}
                    </li>
                }
            })
            .collect_view();
        view! {
            <p><a href="/">"← all agents"</a></p>
            <h1>
                {agent.name.clone()}
                " "
                <span class=provider_class(agent.provider)>{agent.provider.label()}</span>
                " "
                <span class="liveness"><span class=dot></span>{live_label}</span>
            </h1>
            <p class="muted">
                {format!(
                    "first seen {}, last seen {}, {} events ingested",
                    ago(now, agent.first_seen_ms),
                    ago(now, agent.last_seen_ms),
                    agent.events_ingested
                )}
            </p>
            <h2>"Confidence over time"</h2>
            {if confidence_points.is_empty() {
                view! { <p class="empty">"No confidence reports yet."</p> }.into_view()
            } else {
                view! { <figure class="chart" inner_html=svg.clone()></figure> }.into_view()
            }}
            <div class="columns">
                <section>
                    <h2>"Strategy shifts"</h2>
                    {if strategy_shifts.is_empty() {
                        view! { <p class="empty">"None reported."</p> }.into_view()
                    } else {
                        view! { <ul class="timeline">{shifts}</ul> }.into_view()
                    }}
                    <h2>"Reflections"</h2>
                    {if reflections.is_empty() {
                        view! { <p class="empty">"None reported."</p> }.into_view()
                    } else {
                        view! { <ul class="timeline">{refl}</ul> }.into_view()
                    }}
                </section>
                <section>
                    <h2>"Introspection"</h2>
                    {if introspections.is_empty() {
                        view! { <p class="empty">"No thoughts or observations yet."</p> }.into_view()
                    } else {
                        view! { <ul class="timeline">{intro}</ul> }.into_view()
                    }}
                </section>
            </div>
        }
        .into_view()
    })
    .into_response()
}

fn status_chip(status: TaskStatus) -> leptos::View {
    // Status colors never carry meaning alone: icon + text label always ride along.
    let (class, icon) = match status {
        TaskStatus::Pending => ("chip status-pending", "◌"),
        TaskStatus::Running => ("chip status-running", "▶"),
        TaskStatus::Blocked => ("chip status-blocked", "⚠"),
        TaskStatus::Done => ("chip status-done", "✓"),
        TaskStatus::Failed => ("chip status-failed", "✕"),
    };
    view! { <span class=class>{icon}" "{status.label()}</span> }.into_view()
}

fn task_node(task: &TaskSnapshot, children: &BTreeMap<String, Vec<TaskSnapshot>>) -> leptos::View {
    let kids = children
        .get(&task.task_id)
        .map(|ts| {
            ts.iter()
                .map(|t| task_node(t, children))
                .collect_view()
                .into_view()
        })
        .unwrap_or_else(|| ().into_view());
    let percent = task.percent.min(100);
    let bar_class = match task.status {
        TaskStatus::Done => "bar done",
        TaskStatus::Failed => "bar failed",
        _ => "bar",
    };
    view! {
        <li class="task">
            <div class="task-row">
                <span class="task-title">{task.title.clone()}</span>
                {status_chip(task.status)}
                {task.meta.then(|| view! { <span class="chip meta">"meta-task"</span> })}
                <span class="muted small">{task.agents.join(", ")}</span>
            </div>
            <div class="progress" role="img" aria-label=format!("{percent}% complete")>
                <div class=bar_class style=format!("width:{percent}%")></div>
                <span class="progress-label">{format!("{percent}%")}</span>
            </div>
            <ul class="task-children">{kids}</ul>
        </li>
    }
    .into_view()
}

async fn tasks_page(State(state): State<AppState>) -> Html<String> {
    let tasks = state.storage.tasks();
    render_page(
        "meta-agents — tasks".to_string(),
        "tasks",
        false,
        move || {
            let known: std::collections::HashSet<&str> =
                tasks.iter().map(|t| t.task_id.as_str()).collect();
            let mut children: BTreeMap<String, Vec<TaskSnapshot>> = BTreeMap::new();
            let mut roots: Vec<TaskSnapshot> = Vec::new();
            for t in &tasks {
                match t.parent_id.as_deref().filter(|p| known.contains(p)) {
                    Some(parent) => children
                        .entry(parent.to_string())
                        .or_default()
                        .push(t.clone()),
                    None => roots.push(t.clone()),
                }
            }
            let tree = roots.iter().map(|t| task_node(t, &children)).collect_view();
            view! {
                <h1>"Task trees"</h1>
                <p class="muted small">
                    "Tasks reported by more than one agent are flagged as meta-tasks."
                </p>
                {if tasks.is_empty() {
                    view! { <p class="empty">"No tasks reported yet."</p> }.into_view()
                } else {
                    view! { <ul class="task-tree">{tree}</ul> }.into_view()
                }}
            }
            .into_view()
        },
    )
}

#[derive(Deserialize)]
struct LessonsPageQuery {
    topic: Option<String>,
}

async fn lessons_page(
    State(state): State<AppState>,
    Query(q): Query<LessonsPageQuery>,
) -> Html<String> {
    let now = now_ms();
    let topic = q.topic.clone().filter(|t| !t.trim().is_empty());
    let lessons = state.storage.lessons(topic.as_deref());
    render_page(
        "meta-agents — lessons".to_string(),
        "lessons",
        false,
        move || {
            let filter_value = topic.clone().unwrap_or_default();
            let items = lessons
                .iter()
                .map(|l| {
                    let href = format!("/agents/{}", l.agent);
                    view! {
                        <li class="lesson">
                            <div class="lesson-head">
                                <span class="chip kind">{l.topic.clone()}</span>
                                <a href=href>{l.agent.clone()}</a>
                                <span class="muted small">{ago(now, l.recorded_at_ms)}</span>
                            </div>
                            <p>{l.lesson.clone()}</p>
                        </li>
                    }
                })
                .collect_view();
            view! {
            <h1>"Lessons learned"</h1>
            <form class="filter" method="get" action="/lessons">
                <input type="text" name="topic" placeholder="filter by topic…" value=filter_value/>
                <button type="submit">"Filter"</button>
                <a class="muted small" href="/lessons">"clear"</a>
            </form>
            {if lessons.is_empty() {
                view! { <p class="empty">"No lessons recorded for this filter."</p> }.into_view()
            } else {
                view! { <ul class="lessons">{items}</ul> }.into_view()
            }}
        }
        .into_view()
        },
    )
}
