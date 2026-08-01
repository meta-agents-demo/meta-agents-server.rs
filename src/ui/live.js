// Live event feed: subscribes to the server's /ws/feed WebSocket and prepends
// rows to the dashboard feed table. Dependency-free, reconnects with backoff.
(function () {
  "use strict";

  var MAX_ROWS = 50;
  var backoff = 1000;

  function ago() {
    return "just now";
  }

  function summarize(e) {
    switch (e.type) {
      case "register":
        return "registered (" + ((e.agent && e.agent.provider) || "other") + ")";
      case "introspection":
        return "[" + e.kind + "] " + e.content;
      case "metacognition": {
        var parts = [];
        if (typeof e.confidence === "number") {
          parts.push("confidence " + Math.round(e.confidence * 100) + "%");
        }
        if (e.strategy) parts.push("strategy -> " + e.strategy);
        if (e.reflection) parts.push(e.reflection);
        return parts.join(" | ");
      }
      case "task":
        return e.title + " (" + e.task_id + "): " + e.status + " " + e.percent + "%";
      case "learning":
        return "[" + e.topic + "] " + e.lesson;
      case "heartbeat":
        return "heartbeat";
      default:
        return "";
    }
  }

  function agentName(e) {
    if (e.type === "register") return (e.agent && e.agent.name) || "-";
    return e.agent || "-";
  }

  function cell(tr, text, cls) {
    var td = document.createElement("td");
    if (cls) td.className = cls;
    td.textContent = text;
    tr.appendChild(td);
    return td;
  }

  function addRow(e) {
    var feed = document.getElementById("live-feed");
    if (!feed) return;
    var tr = document.createElement("tr");
    tr.className = "flash";
    cell(tr, String(e.seq), "num");
    cell(tr, ago(), "muted");
    var agent = agentName(e);
    var td = document.createElement("td");
    var a = document.createElement("a");
    a.href = "/agents/" + encodeURIComponent(agent);
    a.textContent = agent;
    td.appendChild(a);
    tr.appendChild(td);
    var kindTd = document.createElement("td");
    var chip = document.createElement("span");
    chip.className = "chip kind";
    chip.textContent = e.type;
    kindTd.appendChild(chip);
    tr.appendChild(kindTd);
    cell(tr, e.transport || "", "muted");
    cell(tr, summarize(e), "summary");
    feed.insertBefore(tr, feed.firstChild);
    while (feed.children.length > MAX_ROWS) {
      feed.removeChild(feed.lastChild);
    }
  }

  function connect() {
    var proto = window.location.protocol === "https:" ? "wss:" : "ws:";
    var ws = new WebSocket(proto + "//" + window.location.host + "/ws/feed");
    ws.onopen = function () {
      backoff = 1000;
    };
    ws.onmessage = function (msg) {
      try {
        addRow(JSON.parse(msg.data));
      } catch (err) {
        /* malformed frame: ignore */
      }
    };
    ws.onclose = function () {
      setTimeout(connect, backoff);
      backoff = Math.min(backoff * 2, 15000);
    };
  }

  connect();
})();
