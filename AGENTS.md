# Agent Bridge — Project Instructions

Guidance for agents (Claude Code via Ollama, Codex, OpenCode) that work in this repository.

## Test and build
- Run the test suite: `./venv/bin/python -m unittest discover -s tests -v` (uses isolated temp databases; every test must pass).
- Type check: `./venv/bin/python -m py_compile server.py watchdog.py`.
- HTTP API integration: the suite uses `httpx.ASGITransport` against `server.app`.

## Layout
- `server.py` — FastAPI backend + agent orchestration (chat jobs, software tasks, tools).
- `watchdog.py` — service watchdog with bounded restarts (3 per 15 min + 60 s cooldown).
- `tests/test_reliability.py` — reliability tests (must all pass).
- `roadmap.md` — development roadmap and completed-work notes.
- `sessions.db` — production SQLite DB (WAL mode); schema auto-migrates at startup.
- `venv/` — Python virtual environment for the backend.

## Deploy / restart (Ubuntu host, requires sudo)
- Restart the service: `sudo systemctl restart agent-bridge`.
- Verify health: `curl -s http://<tailscale-ip>:8081/api/health`.
- The service runs as user `snow`; Ollama runs at `http://<tailscale-ip>:11434`.
- The services are deliberately bound to the Tailscale IP (<tailscale-ip>) — do not change the bind address to 0.0.0.0.

## Local agent usage (free, via local Ollama)
- Models installed: `qwen3.5:4b` (server default), `llama3.2:3b`, `qwen2.5-coder:1.5b`.
- Claude Code: run `./claude-local` (sets Anthropic env vars to the local Ollama endpoint).
- Codex: `codex --oss -m qwen3.5:4b`.
- Expect slower generations on this host (minutes per response); prefer focused tasks and verify results.

## Rules
- Never modify the Android application (any Android source).
- Run the test suite and type checks before declaring work done.
- Keep security hardening intact: privileged confirmations, sudo allowlist, transfer no-overwrite, etc.
- Update `roadmap.md` when behavior changes.
- Project backups exist: `server.py.bak-20261120-reliability-overhaul` and other `*.bak-20261120-*` files; also `/home/snow/backups/agent-bridge` (restic).

## Monitoring and infra
- System check every 5 minutes: `~/.cache/agent-bridge/system_check.json`; alerts via journal (`journalctl -t agent-bridge-check`).
- Weekly checks log: `~/.cache/agent-bridge/weekly_checks.log`.
- Prometheus targets: `http://<tailscale-ip>:9090/api/v1/targets`.
