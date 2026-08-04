# Meta Agents process bootstrap

`meta-agents-server` has one executable and one reusable library.

## Process boundary

- `src/main.rs` is the Tokio adapter only.
- `src/bootstrap.rs` owns Clap/environment parsing, logging initialization,
  construction of `ServerConfig`, the fixed per-agent session capacity, and
  waiting for the running server.
- `src/lib.rs` owns socket binding, transport tasks, router composition, shared
  state, and the `ServerHandle` lifecycle.
- `src/ingest/` owns HTTP, WebSocket, TCP, and UDP protocol behavior.
- `src/storage.rs` owns bounded LRU retention policy.
- `src/ui.rs` owns the server-rendered operator surface.

The executable bootstrap must not parse events, mutate storage, define routes,
or own transport loops.

## Configuration contract

The CLI and environment variables retain the existing defaults:

- HTTP/WebSocket/UI: `127.0.0.1:7700`;
- TCP NDJSON: `127.0.0.1:7701`;
- UDP JSON: `127.0.0.1:7702`;
- agents: 256;
- events per agent: 512;
- global recent events: 1024;
- sessions per agent: 16;
- tasks: 512;
- lessons: 512.

Clap validates all socket addresses and numeric capacities before any listener
or transport task starts. Mapping tests keep every option bound to the correct
`ServerConfig` field.

## Regression gate

Permanent CI requires workflow lint, formatting, locked all-target/all-feature
check, warnings-denied Clippy, every unit and real-transport test, Rustdoc with
warnings denied, a release build, and source ratchets preventing CLI, telemetry,
configuration construction, or server waiting from returning to `main.rs`.
