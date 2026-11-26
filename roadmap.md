# Agent Bridge and Android Studio Handoff

This file is the human-readable companion to `GET /api/agent-bridge/help`. The live endpoint is authoritative for routes and registered model tools. Add cross-PC implementation findings with `POST /api/agent-bridge/notes`; retrieve them with `GET /api/agent-bridge/notes?after_id=<last-seen-id>`.

## Current server capabilities

- Local Ollama chat with server-side conversation history, full or compact tool-schema modes, structured tool results, actions, Ollama timing fields, and explicit error output.
- Chat accepts up to four base64 images (16 MB encoded total) for vision-capable Ollama models. The Android client must download/capture the screenshot, encode it, and include it in `images`.
- `/api/tags` caches the model catalog and serializes refreshes. Even `?refresh=true` can make at most one Ollama request per 60 seconds by default (`TAGS_MIN_REFRESH_SECONDS` has a 60-second minimum); callers receive cached or fallback data during the throttle window. Android should avoid polling more often than once per minute.
- Chat and direct command responses include `request_id`, also written to logs, to connect Android errors with server-side records.
- Chat action requests get one recovery turn if the model only narrates or performs read-only inspection. Responses include `execution_status` and `tool_retry_used`; the client must inspect tool results and never treat a tool call alone as proof of success.
- `GET /api/health` reports bridge uptime, job counts/capacity, cleanup policy, host RAM/swap readings from `/proc/meminfo` (plus a `resources` block with the same memory/CPU/disk/temperature readings), and Ollama concurrency, keep-alive, and context settings.
- Jobs are bounded (2 active and 6 queued by default), batch output writes, cap output at 200,000 characters per stream, and prune terminal records older than 14 days at startup or job submission.
- Persistent software tasks store their objective, acceptance criteria, plan, phase, step count, events, verification results, and linked command job in SQLite. They use at most one active model task by default, 24 model steps, and 6 hours total; server environment variables can lower or raise those caps within configured hard bounds.
- The chat model can start the same durable queue with `start_software_task`; `/api/chat` returns a top-level `software_tasks` list with status, events, and diff URLs. This prevents long software work from being limited to a single chat response.
- Task creation validates the requested task model against the local Ollama catalog. Chat-created tasks fall back to the Android chat model when installed, then to an installed default/catalog model; direct `POST /api/tasks` requests with an unavailable model return HTTP 400 and available tags. When Ollama's catalog is unreachable and there is no stale catalog, `/api/tags` returns an empty model list with an error rather than advertising fictional fallback models. Ollama task errors now include the bounded response body in the failure detail.
- Chat action requests get one recovery turn if the model only narrates or performs read-only inspection. Responses include `execution_status`: `not_executed`, `task_queued`, `task_failed`, `tools_invoked`, or `not_needed`. A queued task is not complete; Android must poll task status/events and show verification and diff review.
- Explicit website-build requests that still have no durable task after recovery now use a deterministic server fallback to queue an educational full-stack demo in `~/Desktop/website`, even if the model made partial setup changes. It reuses an existing Express scaffold when present and otherwise uses Python's standard library, with stack-appropriate verification. This closes the failure shown in the November 19 logs, where the model repeated setup commands without completing a project.
- Follow-up commands such as “start building it” are classified using the preceding actionable user request, so compact tool selection and the no-action guard retain project context. Empty model replies receive one recovery turn and are surfaced explicitly rather than being persisted as blank assistant messages.
- Software task planning is bounded: after two read-only inspection calls, the server saves a small generated plan and begins implementation; missing or malformed plan JSON also falls back to that plan instead of failing the task. This avoids spending additional model calls on planner formatting.
- Chat-started tasks can create a missing workspace only when it is under `~/Desktop`. If a later task-create attempt succeeds, an earlier failed attempt no longer makes the overall chat response report `task_failed`.
- Direct commands and background jobs use Bash consistently. This supports common Ubuntu shell syntax such as brace expansion; the prior `/bin/sh` behavior created a literal `{backend,frontend,database}` directory in the website workspace.
- Chat `Idempotency-Key` currently deduplicates task creation, not the entire `/api/chat` request or direct command tools. Android should keep one request in flight and must not automatically replay a timed-out chat send, since that can repeat command side effects.
- Software tasks require explicit acceptance criteria and verification commands. They are marked complete only when the model reports completion and all supplied checks plus `git diff --check` pass for Git workspaces.
- Software task creation accepts an `Idempotency-Key`; repeated matching creates return the original task. Task events support cursor polling, tasks can be cancelled or resumed explicitly, and ambiguous interrupted tool calls require a client reconciliation note before resuming.
- Git projects use a detached worktree under `~/.cache/agent-bridge/worktrees/<task_id>` by default, retaining the result for review. Non-Git workspaces use direct mode and report the limitation. `GET /api/tasks/{task_id}/diff` returns a review diff capped at 1 MiB.
- `DELETE /api/tasks/{task_id}/worktree?confirm=true` removes only the selected Agent Bridge worktree after explicit client confirmation; Android should offer this after diff export/review to control disk use.
- Ollama HTTP connections are reused for app lifetime with a small connection pool. Software tasks send a reduced set of tool schemas and compact recent event context to reduce prompt size and credit use.
- Task workers and their RAM/concurrency limits are process-local; use a single Agent Bridge worker process until shared coordination is implemented.
- File writes use a same-directory temporary file and atomic replacement to avoid leaving partially written target files.
- Graceful server shutdown cancels managed jobs and terminates their isolated process groups. Direct command timeouts also terminate their process groups.
- Generated screenshots are cleaned at startup and before new captures (72-hour age, 50-file cap by default). Other files in the capture directory are left alone.
- Uploads stream in 1 MiB chunks to a same-directory partial file and atomically replace the destination on success; uploads over 100 MiB are rejected by default and failed partial files are removed.
- Direct commands with structured stdout, stderr, exit status, elapsed time, optional noninteractive sudo, and background jobs that can be inspected or cancelled.
- File listing, reading, writing, upload, download, and deletion.
- DuckDuckGo/Bing web search, direct URL fetching, and a direct search API.
- System status, process listing, service and apt management, user timers, installed-app discovery and launching.
- Desktop screenshot and X11/XWayland input actions, with desktop diagnostics and screenshot download links. On Wayland, capture calls GNOME Shell's Screenshot D-Bus interface directly and may try grim; it skips XWayland root capture, which cannot capture the composited desktop. X11 sessions use available native capture backends. Diagnostic errors are bounded.
- `GET /api/agent-bridge/help` publishes live API routes, tool schemas, request/response guidance, known prerequisites, and Android integration suggestions.
- `GET /api/agent-bridge/help?include_runtime=true` also returns the installed model list and current desktop-session diagnostics.
- `GET/POST /api/agent-bridge/notes` is a shared text-only handoff board for separate agents connected through Tailscale.

## Reliability Overhaul (November 20, 2026)

### Completed by the previous agent

- Persistent asynchronous chat jobs: `POST /api/chat/jobs` persists the request (`chat_requests` + `chat_request_events`), returns `202 {request_id, status, reused, status_url, events_url}`, deduplicates by `Idempotency-Key` with 409 on payload mismatch, and coalesces identical in-flight sends per session. Status (`GET /api/chat/jobs/{id}`), events with cursor polling (`GET /api/chat/jobs/{id}/events?after_id=0&limit=100`), and cancellation (`DELETE /api/chat/jobs/{id}`) endpoints exist; `recover_persisted_work` requeues `queued` chats on startup; `init_db` marks running chats `interrupted` on restart.
- Software task reliability: checkpoints persisted on `software_tasks.checkpoint_json`, heartbeats on `heartbeat_at` (per step and during long command waits), step limits, time budget enforcement, pause/resume/cancel, restart recovery (safe read-only retries; destructive operations require reconciliation), final reports, and repeated-identical-tool detection (`read_cache` in the persisted checkpoint; repeated read listings suppress and pause the task as `stalled`).
- Chat capacity is bounded: `MAX_PENDING_CHAT_REQUESTS` (default 4) returns 429 when queued+running chats are at the limit.

### Completed by the current session (verified)

- SQLite: every database operation uses a new `_db()` context manager (short-lived connection, busy timeout 10 s, guaranteed close). `init_db()` keeps WAL mode, sets `busy_timeout`, and closes on success or failure; migration now covers the full column set (`phase`, `steps_used`, `active_job_id`, `error`, `final_report`, `checkpoint_json`, `heartbeat_at`, `agent_role`, `parent_task_id`, `priority`, `use_worktree`, `base_commit`, `plan`). All chat job, task, job, health, stats, and notes paths use `_db()`; no raw `sqlite3.connect` remains outside `init_db`/`_db` itself.
- Stale-worker sweeper: `STALE_WORKER_MINUTES` (default 30, env-configurable) — every 15 s the server marks records with no progress as `interrupted`: `chat_requests` queued/running without a live runner, `software_tasks` queued/running/verifying without a live runner or stale heartbeat, and `jobs` running without a live runner. The sweeper runs as a background asyncio task on the app lifespan; shutdown cancels it. This guarantees no request or task remains in a running state indefinitely when its worker dies.
- Transfer refusal: `POST /api/files/transfer` now returns 409 when the destination already exists (`os.path.lexists`), matching the documented "no overwrite" behavior; the privileged (sudo) path already refused via `_ROOT_TRANSFER_SCRIPT`.
- `GET /api/health` now includes a `resources` block (memory + CPU + disk + temperatures from `/proc`), as previously requested by the reliability tests.
- Watchdog refactor: `watchdog.py` is now importable and testable (`RestartBudget` class, `check_once()`, `main()` guard). Restart budget: 3 restarts per 15-minute window plus a 60-second cooldown between attempts; after budget exhaustion it logs an error for manual diagnosis and never enters a loop. Memory warning uses `/proc/meminfo` (the `psutil` dependency was missing from the venv, so the dependency was removed).
- Android compatibility: `/api/chat` (synchronous), `/api/command`, `/api/jobs`, file endpoints, and all existing routes are unchanged. New chat-job responses are additive (`status_url`, `events_url`).

### New endpoints (chat jobs, for Android)

- `POST /api/chat/jobs` — body as for `/api/chat`; returns `202 {request_id, status: "queued", reused: false, status_url, events_url}`. Send a stable `Idempotency-Key` (≤160 chars, alphanumeric/`._:-`) for retries; identical replays return the same `request_id` with `reused: true`; different payload with the same key returns 409. Without a key, an identical in-flight send from the same session is coalesced.
- `GET /api/chat/jobs/{request_id}` — `{request_id, status, response, error, task_id, ...}`; status is one of `queued`, `running`, `completed`, `delegated`, `failed`, `cancelled`, `interrupted`.
- `GET /api/chat/jobs/{request_id}/events?after_id=0&limit=100` — `{request_id, events: [{event_id, event_type, message, data, created_at}], next_after_id}`; poll with the last `next_after_id`.
- `DELETE /api/chat/jobs/{request_id}` — cancel; queued jobs are marked `cancelled`, running jobs are stopped with status `interrupted` (side effects are never replayed automatically).
- `POST /api/tasks/{task_id}/pause` / `POST /api/tasks/{task_id}/resume` (with `message`, `additional_steps`, `additional_seconds`) — already present; Android already uses them.

Android guidance: poll `status_url` until terminal, then read `response` (same shape as `/api/chat`). Keep one request in flight per conversation, use the same `Idempotency-Key` only when retrying the same logical send, and keep using the legacy `/api/chat` for compatibility-critical flows.

### Known limitations

- Chat-job events are visible while a request runs; there is no SSE/WebSocket live stream yet (SSE was listed as a suggested improvement).
- Restart recovery for raw command `jobs` marks them `interrupted` without automatic resume; software tasks are the only automatically resumable workers.
- The persistent production database (`sessions.db`) was not modified; schema migrations run automatically at startup on any database it opens.
- Some errors in `agent_bridge.log` predate this session (November 19 website-build demo); new runs write test logs to isolated temp files.

### Recovery behavior

- Server restart: `init_db` marks running chats/jobs interrupted and paused tasks (`recovered_after_restart`); `recover_persisted_work` requeues safe queued chats (capped by the chat capacity) and requeues safe paused tasks.
- Worker crash without restart: the stale-worker sweeper marks records `interrupted` after `STALE_WORKER_MINUTES` without progress; clients see the terminal status and can retry or resume with a reconciliation note.
- Ollama restart: software tasks pause with `ollama_unavailable` until models are available again; chat calls surface errors and the task is retried on the next resume.

### Security controls (verified, not weakened)

- Direct `sudo` in shell commands is blocked; privileged shell commands are allowlisted (`apt-get update/upgrade/install`, read-only systemctl/journalctl); arbitrary interpreters are disabled.
- Privileged operations log a digest; `manage_packages` removal and `manage_service` start/stop/restart require `confirm=true`; `manage_process` refuses PID 1 and the service/parent PIDs, allows only TERM/INT/HUP, and only signals same-user processes.
- File writes/uploads use atomic replace; deletes and privileged operations require confirmation; path handling uses `expand_path` with absolute-path policy; `transfer_file` now refuses overwrite (409).

### Testing results (executed 2026-11-20)

- `./venv/bin/python -m py_compile server.py watchdog.py` — OK.
- `./venv/bin/python -m unittest discover -s tests -v` — 37 tests, OK.
- `./venv/bin/python -m unittest discover -s tests` (full suite) — 37 tests, OK (the pre-existing 2 failures on the previous revision — transfer overwrite and `resources` key — are fixed).
- Each of the 37 tests was also run individually with a 20-second per-test timeout (hang detection) — all OK, none hung.
- `./venv/bin/python -m unittest discover -s tests -v` was run repeatedly (5 runs) — consistently OK.
- FastAPI route listing verified: 49 routes including `/api/chat` (legacy synchronous), `/api/chat/jobs` + status/events/cancel, `/api/tasks`, `/api/tasks/{id}` + events/pause/resume/diff, `/api/jobs` + status/cancel, `/api/command`, and file endpoints.

### Migration notes

- `sessions.db` schema migration runs automatically at startup; a backup is at `sessions.db.bak-20261120-reliability-overhaul` (taken before any edits).
- No production data was modified by this session; tests run against isolated temp databases.

## Infrastructure and Reliability Extras (November 20, 2026, part 2)

Completed by the current session:

- **Systemd hardening.** `agent-bridge.service` now sets `PrivateTmp`, `PrivateDevices`, `RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6`, `SystemCallArchitectures=native`, `MemoryDenyWriteExecute`, `ProtectKernelTunables`, `ProtectKernelLogs`, `RestrictRealtime`, `StartLimitIntervalSec`/`Burst`, and `Restart=on-failure` (was plain `Restart=always`). It binds to the Tailscale IP (`<tailscale-ip>:8081`) instead of all interfaces; `ollama.service` is similarly hardened and bound to `<tailscale-ip>:11434`. Verified with `systemd-analyze security` and `systemd-analyze verify`; both services restarted and healthy.
- **Memory tuning.** `vm.swappiness=10` (persisted in `/etc/sysctl.d/99-agent-bridge.conf`); Ollama unit env: `OLLAMA_KV_CACHE_TYPE=q8_0`, `OLLAMA_MAX_LOADED_MODELS=2`, `OLLAMA_NUM_PARALLEL=1` (see Ollama FAQ: the q8_0 KV cache roughly halves KV-cache memory at a small quality cost). This host has only ~705 Mi free RAM — keep watching the monitoring alerts below.
- **Monitoring + alerts.** Installed `prometheus` + `prometheus-node-exporter` (Ubuntu 26.04 repos); node-exporter bound to `<tailscale-ip>:9100`. `/etc/prometheus/prometheus.yml` scrapes the node target; `/etc/prometheus/alerts.yml` defines: `HighMemoryAvailable` (< 2 GiB for 10 min), `HighLoad` (> 8 for 5 min), `DiskAlmostFull` (< 10% free for 10 min), `NodeExporterDown`. `/opt/agent-bridge/check_system.py` (runs as user `snow`) executes every 5 minutes via `agent-bridge-check-system.timer` — checks the Ollama API version, the Agent Bridge `/api/health` (uptime, memory availability, disk free %, 1-min load average), and stuck records in the DB (`chat_requests` running 15+ min without update; software tasks with stale heartbeats). Results are in `~/.cache/agent-bridge/system_check.json`; alerts go to the journal (`logger -t agent-bridge-check`) and, if `ALERT_WEBHOOK_URL` is set, to a webhook.
- **Weekly freshness checks.** `agent-bridge-weekly-checks.timer` (Mondays 01:00) runs `/opt/agent-bridge/weekly_checks.sh` as root:
  1. **Ollama version check**: compares `ollama --version` with the latest GitHub release tag (`https://api.github.com/repos/ollama/ollama/releases/latest`); on mismatch, logs an alert with the exact upgrade procedure (verified current: installed 0.40.16 == latest).
  2. **Model inventory**: logs `ollama ls` output.
  3. **Model license/metadata watch**: snapshots `/api/show` `license` per model into `~/.cache/agent-bridge/model_licenses.json` and alerts on new/removed/changed licenses.
  4. **apt security updates**: `apt-get update -qq` + `apt-get -s upgrade | grep -c '^Inst'` (reports pending updates).
  5. **Python package audit**: `pip_audit` on the venv (initial run found advisories on the old pip; venv pip upgraded to 26.3 — re-run shows clean).
  6. **needrestart** check (no services/kernels need restart).
- **Backups (Restic).** `restic` installed; repository initialized at `/home/snow/backups/agent-bridge` (password in `/etc/restic/agent-bridge-password`, mode 0600). `/opt/agent-bridge/backup_agent_bridge.sh` runs daily via `agent-bridge-backup.timer` (03:00): creates a consistent `sessions-*.db` snapshot via `VACUUM INTO` (SQLite's atomic online backup), backs it up together with `agent_bridge.log` + `system_check.json`, then `restic forget --keep-last 10 --keep-daily 7 --keep-weekly 4 --keep-monthly 3 --prune` and `restic check`. A restore drill was completed successfully (restored a snapshot to a temp dir; 10 files/dirs).
- **Server extras.** `server.py`: new `research` tool (web search with per-provider retry/backoff, PyPI JSON lookup for latest version/license/metadata, optional README/changelog fetch; registered in `TOOLS` and `SOFTWARE_TASK_TOOLS`); web search now retries with backoff; new SSE endpoint `GET /api/chat/jobs/{request_id}/events/stream` (one `event:` per new record, then a final `event: status`); `GET /api/chat/jobs/{request_id}` responses now include `elapsed_seconds` and `stream_url` ticker fields.

## Current state (November 25, 2026)

The reliability overhaul above is historical. The bridge now also provides:

- **Persistent async chat jobs** with idempotent creation, SSE streaming with `token` events, tool events keyed by the job UUID, 15 s heartbeats, and graceful cancellation.
- **Session Memory v2**: `session_state` and `session_turns` tables; compact turn records replace raw tool dumps in context; referential follow-ups inherit the last successful goal/workspace; no-op turns cannot erase successful work. `GET /api/session-state` exposes this to Android.
- **Agent control**: `POST /api/plan` (read-only plan preview), `POST /api/review` (sequential small-model review sub-agent), `ChatReq.allowed_tools`, and bounded `previous_content` on `write_file` results for app-side Undo.
- **Routing & telemetry**: non-action chat uses the 1.5 b fast model; action jobs use the 4 b model (opt-in env `OLLAMA_FAST_ACTION_ROUTING=1`); `/api/stats` reports avg/p50/p95 latency by route.
- **Self-verification**: `py_compile` for Python, `node --check` for JS, structure check for HTML; inline unified diffs for overwrites; deterministic website scaffold/partial-build completion.
- **Measured latency on this host** (`benchmark_routes.py`): chat/fast 24 s, single file-write 159 s, website build 968 s. Most wall time is multi-turn orchestration, not raw generation.
- **Validation:** 73 unit tests pass; `py_compile` OK; health OK; plan/review endpoints live-tested.

Remaining environment limitation: GNOME 50 denies screenshot D-Bus to the service caller; desktop permission change required, not a code fix.

## Android client priorities

1. Fetch `/api/agent-bridge/help?include_runtime=true` at connect time and use its base URL, routes, schemas, installed models, desktop diagnostics, and chat contract.
2. Give each user-initiated chat send a stable `Idempotency-Key` and reuse it only for retrying that same action; this key also protects software-task creation from duplicate requests.
3. Render `message.content`; use `tool_results.data` for cards and preserve stdout, stderr, exit code, errors, download links, and request IDs.
4. Render screenshots from `download_url`. For screenshot question-answering, send the downloaded image as base64 in the chat `images` array and select a vision-capable model.
5. Use `/api/tasks` or detect `start_software_task` in chat and poll the returned `software_tasks` URLs for multi-step software changes; use `/api/jobs` for standalone long commands. Show task plan, phase, steps, elapsed time, recent events, verification results, resume/cancel controls, worktree path, and diff review. Populate the task model from `GET /api/tags`; never forward a cloud/app-only model tag as the local task model. Treat `task_queued` as queued work, not completion, and keep the same `session_id` for the conversation so the server retains context.
6. Use the server's `/api/tags` cache; poll no more often than every 60 seconds. `?refresh=true` is rate limited too, so it cannot trigger repeated Ollama requests.
7. Add upload/download progress and retry handling, and keep stable per-conversation session IDs.
8. If the app's own network environment cannot reach the Tailscale address, run the HTTP calls on a Tailscale-connected machine or resolve its network/permission policy; a failed socket connection is not evidence of an API error.
9. Show `/api/health` capacity and uptime in a diagnostics panel; handle job submission HTTP 429 as a temporary queue limit and upload HTTP 413 as the configured size cap.

## Suggested server improvements

- Add authenticated pairing or a Tailscale identity check before enabling the bridge beyond a single-user trusted tailnet.
- Replace polling for long jobs with Server-Sent Events or WebSockets once the Android client needs live streaming.
- Add explicit confirmation policy for destructive operations, package removal, service shutdown, and file deletion.
- Add structured audit events with request IDs and elapsed time, while redacting command secrets and chat content by default.
- Add tool-call limits and per-request deadlines so one slow model response cannot starve other clients.
- Add SSE chat events for assistant text and tool progress while retaining the current JSON endpoint for compatibility.
- Raw command jobs still stop on server shutdown and are marked interrupted. Software tasks persist and pause on restart, but a command interrupted mid-operation needs reconciliation before the task resumes.
- Add a user-visible cleanup action for old task worktrees after the user exports or accepts their diffs; worktrees are currently retained for manual review.
- Keep model generations serialized by default on RAM-constrained machines; `MAX_OLLAMA_PARALLEL` controls the capped generation limit, while `OLLAMA_KEEP_ALIVE` controls how long the model remains resident. Check `/api/health` before tuning these values.
- Lower `OLLAMA_NUM_CTX` from 4096 to 2048 or 3072 only if measured available RAM/swap pressure requires it; this reduces context capacity and can truncate longer tool workflows.
- Android should avoid automatic re-submission after long chat waits, use request IDs to identify duplicates, and display `ollama_queue_wait_ms` so model latency is distinguishable from queue delay.
- Add a Wayland-capable desktop control backend and screenshot resizing before vision requests.
- Compare candidate local models using the same harmless PC-control tasks and measure tool-call accuracy, response latency, memory use, and refusal rate before changing defaults.
- For more steerable behavior, compare a function-calling model such as Dolphin 3 against the installed Qwen model; use the RAM budget and measured tool-call reliability to choose. Keep a model selector rather than forcing one model.

## Runtime prerequisites and caveats

- The API port and Tailscale hostname/IP come from the host service configuration; use that configured base URL from Android Studio's PC.
- Admin commands only work if the service account has noninteractive sudo permission.
- GNOME Screenshot must be installed and the service must access the logged-in desktop session. `xdotool` input requires X11/XWayland and may not control native Wayland windows.
- After changing `server.py`, restart the Agent Bridge service before expecting the running process to expose the new code.
- Task completion means all configured checks passed; it cannot prove every behavioral requirement. Choose verification commands that exercise the actual acceptance criteria, and review the returned diff before adopting it.
- Never expose this powerful local-control API via public port forwarding.

## Whole-PC file access update (November 19, 2026)

- Android can browse one directory at a time with `GET /api/files?path=/&limit=200&as_root=false`. The response includes `path`, `parent_path`, `entries` (`name`, `path`, `kind`, `is_symlink`, `size`, `modified`), `truncated`, and `elevated`. `limit` is 1–500. Permission failures or inaccessible metadata retry the whole listing through noninteractive sudo.
- Read a bounded text preview with `GET /api/files/content?path=<absolute-path>&max_chars=40000&as_root=false` (maximum 40,000 characters). Download arbitrary files with `GET /api/download?path=<absolute-path>&as_root=false`.
- Save text with `PUT /api/file` and JSON `{ "path": "/absolute/path", "content": "...", "as_root": false }` (content maximum 2,000,000 characters). Upload multipart field `file` with `POST /api/upload?destination=<directory>&as_root=false`; the configured upload cap applies. Delete a single file with `DELETE /api/file?path=<absolute-path>&as_root=false`; directories are not deleted by this route.
- File endpoints accept `as_root=true` to force noninteractive sudo; otherwise they retry with sudo when the normal service account receives a permission error. Elevated responses identify this with `elevated: true` where applicable. Recursive model-tool `list_files` is capped at 250 files and skips large/generated directories; the Android browser is the complete one-directory-at-a-time view.
- The service runs as `snow`, not root, to retain desktop-session access. Admin commands and elevated file operations need the host's `/etc/sudoers.d/90-agent-bridge` rule. Install it from a normal Ubuntu terminal with `sudo /home/snow/agent-bridge/enable_server_sudo.sh snow`, then restart `agent-bridge.service` for the source edits to load. This restricted coding environment could not install that host-level rule or restart the service.
- Android client guidance: fetch `/api/agent-bridge/help?include_runtime=true` on connect, use `/api/files` to navigate, use `/api/files/content` for bounded preview, and show HTTP 403 as a missing host sudo prerequisite with the returned setup detail. Do not send directory deletion requests; the API only deletes files.
