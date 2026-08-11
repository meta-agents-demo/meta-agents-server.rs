# meta-agents-server.rs

> [!CAUTION]
> **Legacy/reference implementation — do not deploy this repository in
> production.** It is preserved for historical context, protocol comparison,
> and local development. This server has no authentication or authorization,
> stores all state only in memory, loses that state on restart, and does not
> prevent unsafe non-loopback bindings. Its deployment files and support for
> `0.0.0.0` are legacy compatibility surfaces, not production guidance.
>
> The canonical production candidate is
> [`meta-agents-demo/meta-agent-control-plane.rs`](https://github.com/meta-agents-demo/meta-agent-control-plane.rs).
> Follow its container and production-readiness work in
> [PR #40](https://github.com/meta-agents-demo/meta-agent-control-plane.rs/pull/40),
> with project context in
> [Linear DEN-1057](https://linear.app/issue/DEN-1057) and
> [DEN-3496](https://linear.app/issue/DEN-3496).

One Rust binary for AI-agent **introspection, metacognition and meta-task
tracking**: agent processes (ChatGPT, Claude, Gemini, anything else) connect
over HTTP, WebSocket, raw TCP or UDP and report what they are thinking, how
confident they are, which strategy they're on, how far their tasks have
progressed and what they've learned — and a human gets a server-rendered web UI
that visualizes all of it live.

```
                       ┌────────────────────────────────────────────────┐
  ChatGPT agent ──┐    │              meta-agents-server                │
                  │    │  ┌──────────────────┐   ┌────────────────────┐ │
  Claude agent ───┼──▶ │  │    transports    │   │  LRU storage       │ │
                  │    │  │ HTTP  :7700 REST │   │  (lru crate,       │ │
  Gemini agent ───┘    │  │ WS    :7700 /ws  │──▶│   behind Storage   │ │
                       │  │ TCP   :7701 NDJSON│  │   trait — swap in  │ │
   one shared JSON     │  │ UDP   :7702 dgram│   │   a real DB later) │ │
   event schema        │  └──────────────────┘   └─────────┬──────────┘ │
                       │            │                      │            │
                       │            ▼                      ▼            │
                       │  ┌──────────────────┐   ┌────────────────────┐ │
   browser ◀───────────┼──│ /ws/feed (live)  │   │ Leptos SSR web UI  │ │
                       │  └──────────────────┘   │ + REST query API   │ │
                       │                         └────────────────────┘ │
                       └────────────────────────────────────────────────┘
```

## Framework choice: Leptos (SSR via the axum integration), no hydration

**Leptos** over Dioxus: its SSR story composes directly with axum in a single
process (`leptos::ssr::render_to_string` inside ordinary axum handlers), which
keeps this a true one-binary server that also owns raw TCP/UDP sockets — no
separate renderer process, no `dioxus-cli` build pipeline.

**Honest caveat:** the UI is **pure SSR, not hydrated**. Full hydration needs
the `cargo-leptos` + wasm32 toolchain and a dual-artifact (`ssr`/`hydrate`
feature) build, which wasn't practical to verify end-to-end in this
environment. Instead, every view is server-rendered by Leptos and the one
genuinely-live piece — the dashboard event feed — is driven by ~90 lines of
dependency-free vanilla JS ([`src/ui/live.js`](src/ui/live.js)) subscribed to
the `/ws/feed` WebSocket. No CDNs, no external assets; CSS and JS are compiled
into the binary. All visualization requirements (dashboard + liveness, live
feed, confidence timeline with strategy shifts, task trees, lessons browser)
are met; re-introducing hydration later is an additive change behind the same
routes.

## Layout

```
src/
  main.rs         CLI (clap: flags + env vars), starts everything
  lib.rs          ServerConfig / start() — also used by the integration tests
  model.rs        the shared event schema (serde JSON) + read models
  state.rs        AppState: storage handle + broadcast channel for /ws/feed
  storage.rs      Storage trait + LruStorage (bounded `lru` caches)
  ingest/
    http.rs       REST endpoints (register, post events, query state)
    ws.rs         /ws/ingest (bidirectional) + /ws/feed (browser live feed)
    tcp.rs        newline-delimited JSON listener
    udp.rs        JSON datagram listener
  ui/             Leptos SSR pages, SVG chart builder, style.css, live.js
examples/         Rust, Python and curl/websocat/nc connectors
deploy/           sample systemd unit + launchd plist
tests/            end-to-end test across all four transports
```

## Event schema

Every transport accepts the same serde-JSON envelope, discriminated by
`"type"`. A payload may be a single object or an array of objects.

```jsonc
// agent registration (idempotent upsert; also auto-created on first event)
{"type": "register", "agent": {"name": "claude-1", "provider": "claude", "session": "sess-42"}}
//                     provider ∈ chatgpt | claude | gemini | other

// introspection: thoughts, observations, self-assessment
{"type": "introspection", "agent": "claude-1", "kind": "thought", "content": "…"}
//                                     kind ∈ thought | observation | self_assessment

// metacognition: confidence (0..1), strategy changes, reflections (≥1 required)
{"type": "metacognition", "agent": "claude-1", "confidence": 0.72,
 "strategy": "tree-of-thought", "reflection": "sampling first paid off"}

// task / progress: trees via parent_id; meta-tasks span agents
{"type": "task", "agent": "claude-1", "task_id": "root-1", "parent_id": null,
 "title": "Meta research plan", "status": "running", "percent": 40, "meta": true}
//        status ∈ pending | running | blocked | done | failed
// A task reported by more than one agent is flagged as a meta-task automatically.

// lessons learned, queryable by topic
{"type": "learning", "agent": "claude-1", "topic": "rust-async", "lesson": "…"}

// liveness ping (agents count as "live" for 30 s after any event)
{"type": "heartbeat", "agent": "claude-1"}
```

Stored events gain `seq` (global monotonic), `received_at_ms` and `transport`;
that enriched form is what queries return and what `/ws/feed` streams.
HTTP/WS/TCP reply per message with `{"ok":true,"seq":N}` or
`{"ok":false,"error":"…"}`; UDP sends the same ack datagram back best-effort.

## Ports & endpoints

| Port (default) | Transport | What |
|---|---|---|
| `127.0.0.1:7700` | HTTP | web UI: `/`, `/agents/:name`, `/tasks`, `/lessons` |
| | HTTP REST | `POST /api/agents` (register), `POST /api/events` (object or array) |
| | HTTP REST | `GET /api/health`, `/api/agents`, `/api/agents/:name`, `/api/agents/:name/events?limit=`, `/api/events/recent?limit=`, `/api/tasks`, `/api/lessons?topic=` |
| | WebSocket | `/ws/ingest` — bidirectional: send event JSON, receive acks |
| | WebSocket | `/ws/feed` — read-only live stream of every stored event |
| `127.0.0.1:7701` | raw TCP | one JSON event per line ⇄ one ack line |
| `127.0.0.1:7702` | UDP | one JSON event per datagram; best-effort ack back |

## Storage

All in-memory, bounded by LRU caches from the [`lru`](https://crates.io/crates/lru)
crate, behind the `Storage` trait (`src/storage.rs`) so a real database can
slot in later: an agent cache (with a per-agent event ring and session cache),
a global recent-events ring, a task-node cache and a lessons cache. Capacities
are CLI/env tunable (`--max-agents`, `--events-per-agent`, `--recent-events`,
`--max-tasks`, `--max-lessons`). Restarting the server clears state.

## Run it

```sh
cargo run --release                      # localhost defaults: 7700/7701/7702
open http://127.0.0.1:7700
```

Keep all three listeners on their default loopback addresses. Although the CLI
accepts non-loopback addresses, binding this unauthenticated server to
`0.0.0.0` (or another remotely reachable interface) can expose agent events,
tasks, lessons, and the operator UI to any network peer. A reverse proxy does
not make the raw TCP and UDP listeners safe, and this server does not provide
TLS, durable storage, access control, or production hardening. Use the
[canonical production candidate](https://github.com/meta-agents-demo/meta-agent-control-plane.rs)
for containerized or remote deployment work.

The server runs in the foreground (log to stdout, `RUST_LOG=debug` for more).
The following init-system files are preserved as historical packaging examples
for loopback-only local evaluation; they are not supported production units:

- **systemd (Linux):** [`deploy/meta-agents-server.service`](deploy/meta-agents-server.service)
- **launchd (macOS):** [`deploy/com.meta-agents.server.plist`](deploy/com.meta-agents.server.plist)

Do not expose this server to a shared or untrusted network, even behind only a
reverse proxy or firewall.

## Connector guide

An "agent connector" is just your agent process emitting schema JSON at the
right socket — pick whichever transport fits the moment: REST for occasional
reports, WebSocket for a session-long stream with acks, TCP for a simple
long-lived pipe, UDP for high-volume fire-and-forget telemetry (heartbeats,
progress ticks).

- **Rust** — [`examples/rust_connector.rs`](examples/rust_connector.rs), all four
  transports: `cargo run --example rust_connector`
- **Python (stdlib only)** — [`examples/python_connector.py`](examples/python_connector.py):
  `python3 examples/python_connector.py my-agent chatgpt`
- **Shell** — [`examples/curl_websocat.sh`](examples/curl_websocat.sh): curl for
  REST, websocat for WS, `nc` for TCP/UDP.

Per provider, only `provider` and where you hook in differ:

| Provider | `provider` value | Typical hook |
|---|---|---|
| ChatGPT | `chatgpt` | tool/function-call handler or orchestrator loop posts events; `session` = conversation id |
| Claude | `claude` | Claude Code hook / MCP tool / SDK wrapper posts after each turn; `session` = session id |
| Gemini | `gemini` | function-calling wrapper or agent framework callback; `session` = chat id |
| anything else | `other` | wherever your loop can emit JSON |

The minimal viable connector is two lines of shell:

```sh
curl -sX POST localhost:7700/api/agents -d '{"name":"claude-1","provider":"claude"}'
curl -sX POST localhost:7700/api/events -d '{"type":"metacognition","agent":"claude-1","confidence":0.8}'
```

## Web UI

- **Dashboard `/`** — agent cards (provider, liveness dot within a 30 s window,
  last confidence/strategy, sessions) + the live event feed streaming over the
  browser WebSocket.
- **Agent view `/agents/:name`** — metacognition timeline: confidence-over-time
  SVG chart (rendered server-side, strategy shifts as dashed markers with hover
  titles), strategy-shift log, reflections, introspection stream.
- **Tasks `/tasks`** — task trees with status chips, progress bars and
  meta-task badges listing every participating agent.
- **Lessons `/lessons`** — lessons-learned browser filterable by topic.

Styling is hand-written CSS compiled into the binary (light + dark via
`prefers-color-scheme`); zero external requests.

## Development

```sh
cargo check
cargo test                                   # unit + end-to-end over all 4 transports
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

All four are clean at HEAD; the integration test (`tests/transports.rs`) boots
the real server on ephemeral ports, fires events over HTTP, WebSocket, TCP and
UDP, and asserts they come back out of the query API, the task tree, the
lessons store and the live feed.
