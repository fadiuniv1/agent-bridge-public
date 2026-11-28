# Agent Bridge (Ubuntu Host)

![Python](https://img.shields.io/badge/Python-3670A0?style=flat-square&logo=python&logoColor=ffdd54)
![FastAPI](https://img.shields.io/badge/FastAPI-005571?style=flat-square&logo=fastapi)
![SQLite](https://img.shields.io/badge/SQLite-07405E?style=flat-square&logo=sqlite&logoColor=white)
![Linux](https://img.shields.io/badge/Linux-FCC624?style=flat-square&logo=linux&logoColor=black)
![Tailscale](https://img.shields.io/badge/Tailscale-242424?style=flat-square&logo=tailscale&logoColor=white)

FastAPI service that gives the Android app a durable agent runtime on this Ubuntu host over Tailscale.

```text
Android Client ──(Tailscale)──▶ Agent Bridge (:8081) ──▶ Ollama (:11434)
                                      │
                                      ├─▶ sessions.db (SQLite)
                                      └─▶ systemd services & timers
```

## Quick start (fresh Ubuntu)

```bash
git clone https://github.com/fadiuniv1/agent-bridge.git
cd agent-bridge
chmod +x install.sh
./install.sh
```

The installer installs Python deps, Ollama (optional), the restricted sudoers rule,
writes `/etc/systemd/system/agent-bridge.service`, enables it at boot, runs all tests
and prints health URLs. Override with `BIND_IP`, `PORT`, `BRIDGE_API_TOKEN`,
`CLOUD_*`, `INSTALL_OLLAMA=0`, `PULL_MODEL=...`, `SERVICE_NAME=...`.
Verifying from a clean machine is documented in `INTEGRATIONS.md`.

## Host & Services

- **Base URL:** `http://<tailscale-ip>:8081` — bound to the Tailscale IP (not `0.0.0.0`).
- **Ollama:** `http://<tailscale-ip>:11434` — running `qwen3.5:4b` with `16384` context window (`OLLAMA_CONTEXT_LENGTH=16384`), `q8_0` KV cache, and `1` parallel generation.
- **systemd units:** `agent-bridge.service`, `ollama.service` (hardened with `PrivateTmp`, `PrivateDevices`, `RestrictAddressFamilies`, `MemoryDenyWriteExecute`, and `Restart=on-failure`).
- **systemd drop-ins:**
  - `/etc/systemd/system/ollama.service.d/10-agent-bridge.conf` (`OLLAMA_CONTEXT_LENGTH=16384` for optimal RAM vs context balance).
  - `/etc/systemd/system/agent-bridge.service.d/10-session-env.conf` (`DBUS_SESSION_BUS_ADDRESS`, `XDG_RUNTIME_DIR`, `WAYLAND_DISPLAY`, required for `/api/schedules` to list timers).
- **systemd timers:**
  - `agent-bridge-check-system.timer` — every `5` minutes.
  - `agent-bridge-backup.timer` — daily at `03:00` via `restic`.
  - `agent-bridge-weekly-checks.timer` — weekly maintenance checks.
- **Session-bus environment:** Stored in `/etc/systemd/system/agent-bridge.service.d/10-session-env.conf` (`DBUS_SESSION_BUS_ADDRESS`, `XDG_RUNTIME_DIR`, `WAYLAND_DISPLAY`). Required so `/api/schedules` can query timers.

## Layout

```text
/home/snow/agent-bridge/
  server.py            FastAPI app (chat jobs, tasks, files, commands, health)
  watchdog.py          helper
  sessions.db          SQLite: messages, chat_requests, chat_request_events, software_tasks, jobs
  venv/                Python environment
  tests/               unittest suite (90 tests)
  captures/            screenshots
  benchmark_routes.py  route/latency benchmark script
  *.bak-*              timestamped backups of server.py / roadmap.md / README.md
```

## Key API Groups

- **Legacy sync:** `POST /api/chat` (compatibility/debug fallback only).
- **Async chat jobs (default path):** `POST /api/chat/jobs` (optional `Idempotency-Key`), `GET /api/chat/jobs/{id}`, `GET /events?after_id&limit`, `GET /events/stream` (SSE), `DELETE /api/chat/jobs/{id}`.
  - **`tool_mode` values:**
    - `direct` (default everywhere): short prompt, small curated inline tool set (4–9 tools: read/write/run/research/status), never starts background tasks/commands. Completes work inline in the chat conversation with natural language summaries.
    - `full`: durable task delegation mode (opt-in via app Settings "Delegate long work (experimental)"). This mode is still experimental and unreliable on this host: durable software tasks failed in live testing. Never enable it by default; keep `direct` for the normal phone-driven agent flow.
    - `compact`: keyword-based tool selection subset.
  - **Smart tool budgets:** In `direct` mode, non-action prompts receive minimal/zero tool schemas (reducing prompt evaluation from ~100s to ~10s). Action prompts receive a tightly focused tool set. "Run it" requests are prompted to launch the server detached with `nohup … &` and verify it with `curl`, so the chat command does not block on a foreground server. Server-start prompts also forbid broad `pkill`/`killall`/`fuser -k` patterns and exit-1 probes such as `lsof` or `netstat | grep`: if a port is busy the model should pick a different port instead of killing processes that may include the bridge itself.
  - **Streaming token events:** Streams Ollama with `stream: true` and emits batched `token` events on the chat-job event stream. The client appends tokens live and finalizes on `completed`. An early `token` event (`Starting on your request — I’ll do the work on the PC and report back.`) is emitted immediately when the job starts, so the phone shows live text instead of only a heartbeat while Ollama evaluates the prompt and buffers tool calls; the final answer still replaces the bubble exactly once.
  - **Inline tool events:** Emits `tool_started` and `tool_result` events keyed by the chat job UUID (not internal 24-hex IDs) so the UI displays inline progress ("Preparing write_file", "Finished write_file").
  - **Heartbeats:** Emits `model_step` heartbeat events every ~15s while the model computes (`Working (15s)`, `Working (30s)`), keeping connection state fresh.
- **Health & diagnostics:** `GET /api/health`, `/api/version`, `/api/stats` (chat metrics), `/api/system-stats` (uptime/memory/disk), `/api/diagnostics`, `/api/logs`, `/api/tags`.
- **Session memory & control:** `GET /api/session-state?session_id=...` (durable per-tab goal, workspace, files, last success), `POST /api/plan` (read-only plan preview), `POST /api/review` (small-model review sub-agent over touched files).
- **Files:** `GET /api/files?path=&limit=&as_root=`, `/api/files/content`, `PUT /api/file`, `DELETE /api/file`, `POST /api/files/transfer` (409 if destination exists), `/api/upload`, `/api/download`.
- **Commands/jobs:** `POST /api/command`, `POST /api/jobs`, `GET /api/jobs`.
- **Software tasks:** `POST /api/tasks`, `GET /api/tasks/{id}`, `/events`, `/pause`, `/resume`, `/diff`, `/worktree`, `DELETE`.
- **Schedules:** `POST /api/schedules {"action":"list"}` — lists system timers (system scope).

## Reliability & Runtime Behavior

- **`think: false` on every Ollama chat call:** Disables qwen3.5 reasoning tokens to prevent long thinking loops and empty content responses.
- **16k Context (`OLLAMA_CONTEXT_LENGTH=16384`):** Halved from 32k, freeing available system RAM from ~490 MB to ~4.3 GB while providing ample window for multi-turn inline tool execution.
- **D2 Safe Tag Truncation (`_safe_tool_excerpt`):** Truncates tool output at 6000 chars without cutting HTML/XML tags mid-tag, avoiding Ollama `500 XML syntax error: unexpected EOF` during chatml parsing.
- **Post-tool forced text summary turn:** After tool execution, a forced text turn ensures the model generates a natural-language answer explaining what was done, keeping `assistant_text` clean without raw tool/JSON dumps.
- **Safe restart replay:** On service startup, a chat job left `running` is replayed with its idempotency key only if no mutating tool (`tool_started`) was executed; otherwise marked `interrupted` for reconciliation.
- **Auto-resume paused tasks:** Sweeper monitors available memory; when memory recovers past the 256 MB threshold, paused tasks are automatically resumed (adhering to concurrency limits). Stale workers without heartbeats are safely marked `interrupted`.
- **Ollama ProtocolError backoff retry:** `_post_ollama_with_retry()` automatically retries transient connection drops and server disconnects with bounded backoff.
- **Stream tool-call object retention:** Preserves tool argument dicts without stringification in `_post_ollama_stream()`, preventing HTTP 400 errors across follow-up turns.
- **Graceful runner shutdown:** Worker threads cleanly acknowledge cancellation and stop processing when tasks/jobs are deleted.
- **No auto-replay of uncertain sends:** a timed-out send or a create whose outcome is unknown is never automatically resent (side effects may already have happened). The same idempotency key is reused only for a proven safe retry; after a terminal failure the client rotates to a new key. Jobs left `running` with a recorded mutating tool are marked `interrupted` for manual reconciliation.
- **Chat stays the task surface:** the app never auto-opens the Jobs or Software tasks screens. Delegation and task views are manual-only, and delegated work is clearly marked experimental.
- **GNOME Screenshot limitation:** GNOME 50 denies `org.gnome.Shell.Screenshot` to non-graphical-session callers; resolving this requires desktop session permissions.


## Phase 2 Enhancements

- **Model routing (C3):** Short non-action prompts go to `qwen2.5-coder:1.5b` (fast path); action prompts and prompts with images go to `qwen3.5:4b`. Job responses include `model_route` (`fast`/`strong`) and `model_used`. `GET /api/stats` reports route counts, execution statuses, and generation latency (avg/p50/p95, overall and per route).
- **Action routing default:** Tool-using requests (including simple website builds) go straight to `qwen3.5:4b`; the 1.5b fast model is used for chat only. Set `OLLAMA_FAST_ACTION_ROUTING=1` to opt into fast action routing again.
- **Clean final text:** The server no longer prepends a raw `Remote Execution Summary` to `assistant_text`; the model's answer stays clean and execution details live in tool cards and the verified-change block.
- **Verified-change block:** Completed jobs append a compact `**Changes verified**` section listing touched files and syntax/structure check results to the final answer.
- **Simple-site fast path:** Small static HTML/CSS website requests route to `qwen2.5-coder:1.5b`; complex sites (React/backend/database/auth/API) stay on `qwen3.5:4b`. A fast model that returns an empty response or prints a JSON tool call as prose automatically falls back to `qwen3.5:4b` on the recovery turn.
- **Action generation budget:** Chat turns use `OLLAMA_NUM_PREDICT=512` and 6 turns; action jobs use `OLLAMA_NUM_PREDICT_ACTION=1536` and 10 turns; explicit website builds use 16 turns plus a prompt that names `index.html`, `styles.css`, `README.md`. This fixes the previous failure where a 512-token cap truncated file writes after the first file.
- **Action classification:** Common edit verbs (`change`, `update`, `replace`, `add`, `generate`, …) and short follow-ups ("write the others", "create them", "continue") inherit the prior action intent, so they reach the strong model with write tools instead of silently becoming chat. Responses that merely narrate file creation without a change tool are downgraded to `execution_status=not_executed` instead of being reported as success.
- **Executive context (C4):** `session_summaries` stores a bounded rolling summary of constraints, touched files, actions and last outcome. It is injected as a system message only when history is actually compacted (more than 12 messages), keeping short chats cheap.
- **Compact host context:** A ~30-token factual snippet (Ubuntu/GNOME, user, shell, apt, venv path) is prepended to chat system prompts.
- **Self-verification:** After `write_file` of a `.py` file, a bounded `py_compile` check runs and is attached as an inline `verify_python` tool result/event.
- **Web self-verification:** Written `.js` files get `node --check`; written `.html` files get a basic structure check. Both appear as inline `verify_web` tool results. If `node --check` crashes or exits by signal on this host, the result is recorded as `Syntax check skipped` (exit 0) instead of a fake tool failure, and diagnostics are reduced to one meaningful line so no V8 crash dump leaks into the answer.
- **Inline diffs:** `write_file` overwrites of text files under 200 KB produce a bounded unified diff in the tool result, rendered as a collapsible card in the app.
- **Website scaffold fallback:** An explicit website-build request that still creates no files gets a small original starter site (`index.html`, `styles.css`, `README.md`) in a new timestamped `~/Desktop/website-*` folder, with an honest note in the final answer.
- **Partial-build completion:** After any explicit website request, missing starter files are added in the folder the model used (never overwriting existing files), so a partial run still leaves a complete runnable site.
- **Open on PC:** `POST /api/apps/launch` accepts an absolute file path and opens it with `xdg-open` (used by the HTML file chip's "Open in browser" action).
- **Graceful shutdown:** systemd drop-in `20-graceful-shutdown.conf` adds `--timeout-graceful-shutdown 10` so a stuck SSE stream cannot block restarts for 180s.
- **Safe imports:** Restart recovery SQL was moved from `init_db()` into the startup handler, so importing `server` for diagnostics/tests cannot mark running jobs interrupted.

## Cloud models & CLI interop

- **Optional cloud provider** (no keys in source): set `CLOUD_BASE_URL`, `CLOUD_API_KEY`,
  `CLOUD_FAST_MODEL`, `CLOUD_STRONG_MODEL` in the service environment, then restart.
  Recommended free/fast choice: Groq's OpenAI-compatible endpoint
  (`https://api.groq.com/openai/v1`) with `openai/gpt-oss-20b` as fast and
  `openai/gpt-oss-120b` as strong. Gemini's OpenAI-compatible endpoint or
  OpenRouter `:free` models also work. With no env set, everything stays local.
- Models appear in `GET /api/tags` as `cloud:fast` / `cloud:strong` and can be selected
  in the Android app or CLI. Tool schemas are forwarded in OpenAI format; tool-call
  arguments are normalized back to objects for the local turn loop.
- **OpenAI-compatible endpoint:** `POST /v1/chat/completions` lets OpenCode CLI (MIT)
  or Codex CLI (Apache-2.0) use this bridge as a custom OpenAI-compatible provider.
  Live tokens remain on the native chat-job SSE stream.
- **Terminal client:** `cli/agent_cli.py` in the Android repo is original MIT code
  (no code copied from Claude Code/OpenCode/Codex) with chat streaming, plan, review,
  file listing/reading and command execution.

## Session Memory v2

- `session_state` stores a durable goal, workspace, files, decisions, constraints and last successful turn; no-op turns cannot erase successful work.
- `session_turns` stores compact turn records; chat history is built from those instead of raw 16k-character tool dumps.
- `GET /api/session-state?session_id=...` exposes this to the Android app, which shows the task in the header/drawer and offers 'Continue last task'.
- Referential follow-ups ('enhance it', 'the website') inherit the last successful goal and workspace so the agent edits existing files instead of creating a second project.
- Identical repeated prompts after a no-op turn get a 'change approach' directive.

## Agent Control Additions

- `POST /api/plan` previews a numbered plan (small model, no PC changes) with the session workspace/files; the app shows it for approval before sending.
- `POST /api/review` runs a sequential small-model review sub-agent over files touched in the session and returns concise findings.
- `write_file` tool results include bounded `previous_content`, enabling an Undo chip in the app that restores the prior file via `PUT /api/file`.
- `ChatReq.allowed_tools` lets the app disable individual tools per user preference.
- Measured benchmark (this host): chat/fast 24s, single file-write/strong 159s, website-build/strong 968s; generation itself is only 22–50s, so most wall time is multi-turn orchestration.

## Benchmarking & App Notes

- `smoke_test.py` runs 11 end-to-end checks (health, version, SQLite integrity,
  chat job completion, token/tool events, clean answer, session state, plan endpoint):
  `./venv/bin/python smoke_test.py http://<tailscale-ip>:8081`.
- `benchmark_routes.py` runs a fixed chat / file-write / website prompt set and prints route, model, wall time, generation time, token count and tool count. Run with `./venv/bin/python benchmark_routes.py` (takes several minutes).
- Android app: the top bar shows `⏳ N` when prompts are queued; tapping it opens a panel to reorder (↑/↓) or cancel queued prompts. Failed/no-op answers offer a `Retry on 4B` chip.

## Common Commands

```bash
# Service status & restart
systemctl status agent-bridge ollama
systemctl restart agent-bridge

# Health check
curl -s http://<tailscale-ip>:8081/api/health

# Watch a job's events, including live tokens and heartbeats:
curl -s 'http://<tailscale-ip>:8081/api/chat/jobs/<id>/events?after_id=0&limit=500'
cd ~/agent-bridge && ./venv/bin/python -m unittest discover -s tests

# Logs & timers
journalctl -u agent-bridge -n 100
systemctl list-timers --no-pager | grep agent-bridge

# Inspect chat request statuses in SQLite
sqlite3 ~/agent-bridge/sessions.db "select status,count(*) from chat_requests group by status;"
```

## Cloud provider notes (Groq verified)

- `cloud:fast` (Groq `openai/gpt-oss-20b`) and `cloud:strong` (`openai/gpt-oss-120b`)
  are verified live: short chat answers in ~1–3 s vs ~100 s on local Ollama.
- Strict provider validation required three fixes: tool schemas normalized to OpenAI
  format; assistant tool-call arguments serialized as JSON strings; tool roles carry
  `tool_call_id`; historical tool calls/narration stripped; tool-free cloud chats send
  only system context + current prompt so small models cannot hallucinate stale tools.
- Free-tier limits (Groq): 30 req/min, 1k req/day, 8k tokens/min — use cloud for chat
  and short tasks; keep local Ollama as the default for long multi-file work.

## Internationalization / RTL

- `ChatReq.language` (e.g. `he`, `ar`, `fr`, `es`, `de`, `tr`, `ru`) adds a reply-language
  instruction to the system prompt. The Android app sends the selected locale.
- If a small model answers in English anyway, the bridge applies a deterministic
  localized prefix (`תשובה:` / `الإجابة:` …) to the final answer so RTL users still
  receive their language marker. File paths and tool names stay verbatim.
- The Android app ships complete Hebrew and Arabic `strings.xml` translations, a
  language selector in Settings, and RTL layout via `supportsRtl` + `attachBaseContext`.

## Production hardening

- Optional shared-secret auth: set `BRIDGE_API_TOKEN` (`X-Bridge-Token` header on `/api/*`
  and `/v1/*`; health and integrity checks stay exempt), or `AGENT_BRIDGE_API_TOKEN`
  (`Authorization: Bearer`, enforced on every path for non-loopback clients). See `.env.example`.
  The Android app reads `BRIDGE_TOKEN` from `.env` and sends `X-Bridge-Token`.
- Data durability: `session_turns_archive` retains pruned turns instead of deleting
  them; `GET /api/session/export?session_id=...` exports state, turns, messages and
  chat requests; `GET /api/db/integrity` reports `PRAGMA integrity_check`.
- `RUNBOOK.md` covers health checks, restic restore drills, crash recovery and safe
  edit/restart procedure. GitHub Actions runs `py_compile` + the full test suite.

## Verified status (2026-11-27)

- `py_compile server.py` clean; `python -m unittest discover -s tests` → **Ran 81 tests / OK**; `smoke_test.py http://100.67.146.29:8081` → **11 checks, 0 failed**.
- Live direct-mode chat job `07b1e0b0-83d3-4013-87e7-f6692f6697d9` for `create a small website and run it`: the first `token` event arrived in <1 s (early progress line), followed by 23 streamed token events, 2 `tool_started`, 5 `tool_result` and 20 `model_step` heartbeats. It ended `completed` with `execution_status=tools_invoked`, **zero failed tools**, and a clean answer with no raw tool dumps.
- Real side effects: `~/Desktop/index.html` and `~/Desktop/style.css` were written, and `python3 -m http.server 8000 --directory /home/snow/Desktop` was running and returned HTTP 200 with the expected `<title>`.
- A repeat POST with the same `Idempotency-Key` while the job was running returned `reused: true` and the same request ID; every event carried the chat-job UUID rather than the internal 24-hex request ID.
- Host/config: bridge bound only to `100.67.146.29:8081`, `think:false` in request construction, `num_ctx=16384`, and 4.1–5.2 GiB RAM available at idle after unloading the model. The action prompt above routed to `qwen3.5:4b` (`action_required=True`) in this run; the 1.5b fast model remains chat-only by default.

## Publishing checklist

This repository was private during development. Before making it public:

1. Scan the full git history (`gitleaks detect` / `trufflehog git file://.`) — deleting a
   file from HEAD does not remove it from old commits.
2. Old commits contain `sessions.db`, `agent_bridge.log` and `*.bak-*` snapshots. In a
   fresh clone, either run `git filter-repo --sensitive-data-removal --invert-paths
   --path sessions.db --path agent_bridge.log --path agent_bridge.log.1` and force-push,
   or create a fresh repository from the current clean tree and archive the old one.
3. Enable GitHub secret scanning and push protection after publication.
4. Keep `.env` and all API keys out of the repository; configure them in the service
   environment instead.

## Backups & Retention

- **Restic repository:** `/home/snow/backups/agent-bridge` (daily 03:00 timer). Keep it intact.
- **Local backups:** Keep only the newest `*.bak-*` per file family; older ones are cleaned periodically. Restic remains the authoritative backup.
