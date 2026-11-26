#!/usr/bin/env python3
"""End-to-end smoke test for a running Agent Bridge. Stdlib only.

Usage: python3 smoke_test.py [base_url]
"""
import json
import sys
import time
import urllib.request
from uuid import uuid4

BASE = (sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8081").rstrip("/")
results = []


def check(name, ok, detail=""):
    results.append((name, bool(ok), str(detail)[:160]))
    print(("  PASS  " if ok else "  FAIL  ") + name + ((" :: " + str(detail)[:160]) if detail else ""))


def get(path):
    with urllib.request.urlopen(BASE + path, timeout=20) as r:
        return json.load(r)


def post(path, body):
    req = urllib.request.Request(
        BASE + path, data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


try:
    health = get("/api/health")
    check("health", health.get("status") == "ok", health.get("status"))

    version = get("/api/version")
    check("version", version.get("name") == "agent-bridge", version.get("version"))

    integrity = get("/api/db/integrity")
    check("sqlite integrity", integrity.get("ok") is True, integrity.get("integrity"))

    session = "smoke-" + uuid4().hex[:8]
    ack = post("/api/chat/jobs", {
        "session_id": session,
        "prompt": "In one sentence, what is Tailscale?",
        "tool_mode": "direct",
    })
    rid = ack.get("request_id")
    check("chat job accepted", bool(rid), rid)

    terminal = {"completed", "delegated", "failed", "cancelled", "interrupted"}
    status = {}
    for _ in range(90):
        status = get("/api/chat/jobs/" + rid)
        if status.get("status") in terminal:
            break
        time.sleep(1)
    check("chat job completed", status.get("status") == "completed", status.get("status"))
    response = status.get("response") or {}
    answer = str(response.get("assistant_text") or "")
    check("answer not empty", len(answer.strip()) > 10, answer[:80])
    check("no raw tool dump in answer", "Remote Execution Summary" not in answer, "")

    events = get("/api/chat/jobs/%s/events?after_id=0&limit=200" % rid)
    kinds = {e.get("event_type") for e in events.get("events", [])}
    check("token events", "token" in kinds, ",".join(sorted(kinds)))
    # A fast chat answer can finish before the first 15s heartbeat and needs no tools.
    check("event stream healthy", {"accepted", "started", "token", "completed"}.issubset(kinds),
          ",".join(sorted(kinds)))

    state = get("/api/session-state?session_id=" + session)
    check("session state endpoint", isinstance(state, dict), "")

    plan = post("/api/plan", {"prompt": "Add a footer to the site", "session_id": session})
    check("plan endpoint", bool(str(plan.get("plan", "")).strip()), plan.get("plan", "")[:60])
except Exception as exc:
    check("smoke run", False, "%s: %s" % (type(exc).__name__, exc))

failed = [n for n, ok, _ in results if not ok]
print("\n%d checks, %d failed" % (len(results), len(failed)))
sys.exit(1 if failed else 0)
