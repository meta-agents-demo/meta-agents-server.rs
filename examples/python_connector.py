#!/usr/bin/env python3
"""Minimal Python connector for meta-agents-server — stdlib only.

Shows how a ChatGPT/Claude/Gemini agent process registers and streams
introspection, metacognition, task-progress and learning events over
HTTP REST, raw TCP (newline-delimited JSON) and UDP (JSON datagrams).
For WebSocket, `pip install websockets` and see ws_example() below.

Usage: python3 examples/python_connector.py [agent-name] [provider]
"""

import json
import os
import socket
import sys
import urllib.request

HTTP = os.environ.get("META_AGENTS_HTTP_ADDR", "127.0.0.1:7700")
TCP = os.environ.get("META_AGENTS_TCP_ADDR", "127.0.0.1:7701")
UDP = os.environ.get("META_AGENTS_UDP_ADDR", "127.0.0.1:7702")

AGENT = sys.argv[1] if len(sys.argv) > 1 else "gpt-python-example"
PROVIDER = sys.argv[2] if len(sys.argv) > 2 else "chatgpt"  # chatgpt|claude|gemini|other


def http_post(path: str, payload: dict) -> dict:
    req = urllib.request.Request(
        f"http://{HTTP}{path}",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as res:
        return json.loads(res.read())


def main() -> None:
    # 1. Register over HTTP REST.
    print("register:", http_post("/api/agents", {
        "name": AGENT, "provider": PROVIDER, "session": f"pid-{os.getpid()}",
    }))

    # 2. Introspection + metacognition over HTTP REST (arrays work too).
    print("http:", http_post("/api/events", [
        {"type": "introspection", "agent": AGENT, "kind": "observation",
         "content": "The corpus is smaller than expected"},
        {"type": "metacognition", "agent": AGENT, "confidence": 0.55,
         "strategy": "map-reduce summarization",
         "reflection": "Sampling first paid off"},
    ]))

    # 3. Task progress over raw TCP — one JSON object per line, one ack per line.
    tcp_host, tcp_port = TCP.rsplit(":", 1)
    with socket.create_connection((tcp_host, int(tcp_port))) as s:
        f = s.makefile("rw", encoding="utf-8", newline="\n")
        for percent, status in [(10, "running"), (60, "running"), (100, "done")]:
            f.write(json.dumps({
                "type": "task", "agent": AGENT, "task_id": f"{AGENT}-summarize",
                "title": "Summarize corpus", "status": status, "percent": percent,
            }) + "\n")
            f.flush()
            print("tcp ack:", f.readline().strip())

    # 4. Lessons + heartbeats over UDP — fire-and-forget datagrams.
    u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    host, port = UDP.rsplit(":", 1)
    for event in [
        {"type": "learning", "agent": AGENT, "topic": "summarization",
         "lesson": "Chunk on section boundaries, not fixed sizes"},
        {"type": "heartbeat", "agent": AGENT},
    ]:
        u.sendto(json.dumps(event).encode(), (host, int(port)))
    u.settimeout(2.0)
    try:
        ack, _ = u.recvfrom(2048)  # acks are best-effort
        print("udp ack:", ack.decode())
    except socket.timeout:
        pass

    print(f"done — open http://{HTTP}/agents/{AGENT}")


def ws_example() -> None:
    """Bidirectional WebSocket variant (requires: pip install websockets)."""
    import asyncio
    import websockets  # type: ignore

    async def run() -> None:
        async with websockets.connect(f"ws://{HTTP}/ws/ingest") as ws:
            await ws.send(json.dumps({
                "type": "metacognition", "agent": AGENT, "confidence": 0.8,
            }))
            print("ws ack:", await ws.recv())

    asyncio.run(run())


if __name__ == "__main__":
    main()
