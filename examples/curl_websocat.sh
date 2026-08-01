#!/usr/bin/env bash
# Shell connector snippets for meta-agents-server: curl (HTTP), websocat
# (WebSocket), nc (TCP + UDP). Adjust the addresses or export the
# META_AGENTS_*_ADDR variables the server also honors.
set -euo pipefail

HTTP="${META_AGENTS_HTTP_ADDR:-127.0.0.1:7700}"
TCP="${META_AGENTS_TCP_ADDR:-127.0.0.1:7701}"
UDP="${META_AGENTS_UDP_ADDR:-127.0.0.1:7702}"
AGENT="gemini-shell-example"

## --- HTTP REST -------------------------------------------------------------
# Register the agent (idempotent upsert):
curl -sS -X POST "http://$HTTP/api/agents" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$AGENT\",\"provider\":\"gemini\",\"session\":\"shell-$$\"}"
echo

# Introspection + metacognition:
curl -sS -X POST "http://$HTTP/api/events" \
  -H 'Content-Type: application/json' \
  -d "[
    {\"type\":\"introspection\",\"agent\":\"$AGENT\",\"kind\":\"self_assessment\",
     \"content\":\"My retrieval step is the weak link\"},
    {\"type\":\"metacognition\",\"agent\":\"$AGENT\",\"confidence\":0.45,
     \"strategy\":\"retrieval-augmented\"}
  ]"
echo

# Query things back:
curl -sS "http://$HTTP/api/agents" | head -c 400; echo
curl -sS "http://$HTTP/api/lessons?topic=retrieval" | head -c 400; echo

## --- WebSocket (requires websocat) -----------------------------------------
if command -v websocat >/dev/null 2>&1; then
  printf '%s\n' \
    "{\"type\":\"metacognition\",\"agent\":\"$AGENT\",\"confidence\":0.7,\"reflection\":\"grounding fixed it\"}" |
    websocat -n1 "ws://$HTTP/ws/ingest"
else
  echo "websocat not installed — skipping WebSocket demo" >&2
fi

## --- Raw TCP: newline-delimited JSON (one ack line per event line) ---------
printf '%s\n' \
  "{\"type\":\"task\",\"agent\":\"$AGENT\",\"task_id\":\"shell-1\",\"title\":\"Shell demo\",\"status\":\"running\",\"percent\":50}" |
  nc -w 2 "${TCP%:*}" "${TCP##*:}"

## --- UDP: one JSON datagram per event (fire-and-forget) --------------------
printf '%s' \
  "{\"type\":\"learning\",\"agent\":\"$AGENT\",\"topic\":\"shell\",\"lesson\":\"nc -u works fine for telemetry\"}" |
  nc -u -w 1 "${UDP%:*}" "${UDP##*:}" || true

echo "done — open http://$HTTP/agents/$AGENT"
