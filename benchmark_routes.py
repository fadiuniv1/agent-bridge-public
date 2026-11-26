"""Measure model routing and latency on a fixed prompt set.

Usage: ./venv/bin/python benchmark_routes.py
Runs chat / file-write / website-build jobs sequentially and prints a table.
"""
import json
import os
import time
import urllib.request
from uuid import uuid4

BASE = os.getenv("BRIDGE_BASE_URL", "http://localhost:8081").rstrip("/")
TERMINAL = {"completed", "delegated", "failed", "cancelled", "interrupted"}
PROMPTS = [
    ("chat", "In one sentence, what is Tailscale?"),
    ("file-write", "Create /home/snow/Desktop/benchmark-note.txt containing exactly: ok"),
    ("website", "create a website with css and a readme in a folder called benchmark-site on desktop"),
]


def request(method, path, body=None, headers=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json", **(headers or {})}
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.load(resp)


results = []
for name, prompt in PROMPTS:
    key = "bench-" + uuid4().hex[:10]
    started = time.monotonic()
    ack = request("POST", "/api/chat/jobs",
                  {"session_id": key, "prompt": prompt, "tool_mode": "direct"},
                  {"Idempotency-Key": key})
    rid = ack["request_id"]
    while time.monotonic() - started < 3600:
        job = request("GET", "/api/chat/jobs/" + rid)
        if job["status"] in TERMINAL:
            break
        time.sleep(3)
    resp = job.get("response") or {}
    ollama = resp.get("ollama") or {}
    results.append({
        "case": name,
        "route": resp.get("model_route"),
        "model": resp.get("model_used"),
        "execution": resp.get("execution_status"),
        "wall_seconds": round(time.monotonic() - started, 1),
        "tokens": ollama.get("eval_count"),
        "generation_seconds": round(ollama.get("total_duration", 0) / 1e9, 1),
        "tools": len(resp.get("tool_results", [])),
    })

print("%-12s %-8s %-26s %-14s %8s %8s %7s %6s" % (
    "case", "route", "model", "execution", "wall_s", "gen_s", "tokens", "tools"))
for r in results:
    print("%-12s %-8s %-26s %-14s %8s %8s %7s %6s" % (
        r["case"], r["route"], r["model"], r["execution"],
        r["wall_seconds"], r["generation_seconds"], r["tokens"], r["tools"]))
