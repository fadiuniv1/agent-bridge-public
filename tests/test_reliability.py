"""Comprehensive reliability tests.

Every test uses a temporary SQLite database (``server.DB_FILE`` is patched per
test and initialized with ``server.init_db``) and isolated directories, so no
test touches the production database or real system state.
"""
import asyncio
import os
import sqlite3
import sys
import tempfile
import time
import unittest
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

_runtime = tempfile.TemporaryDirectory(prefix="agent-bridge-tests-")
os.environ["AGENT_BRIDGE_DB_FILE"] = str(Path(_runtime.name) / "unused-at-import.db")
os.environ["AGENT_BRIDGE_LOG_FILE"] = str(Path(_runtime.name) / "test.log")

import server  # noqa: E402
import watchdog  # noqa: E402

TEST_HTTP_TIMEOUT = 20.0
TERMINAL_CHAT_STATUSES = {"completed", "delegated", "failed", "cancelled", "interrupted"}
TERMINAL_TASK_STATUSES = {"completed", "failed", "cancelled", "interrupted", "needs_input"}


class _FakeOllamaResponse:
    """Mimics the httpx.Response surface used by _run_software_task."""

    def __init__(self, tool_calls=None, content="", status=200):
        self.status_code = status
        self.is_error = status >= 400
        self.text = ""
        message = {}
        if tool_calls:
            message["tool_calls"] = tool_calls
        if content:
            message["content"] = content
        self._payload = {"message": message}

    def json(self):
        return self._payload


class IsolatedDatabaseTestCase(unittest.IsolatedAsyncioTestCase):
    """Per-test isolated database, patching server.DB_FILE and running init_db."""

    def setUp(self):
        self._temp = tempfile.TemporaryDirectory(prefix="agent-bridge-test-")
        self.addCleanup(self._temp.cleanup)
        self.db_path = Path(self._temp.name) / "test.db"
        self.workspace = Path(self._temp.name) / "workspace"
        self.workspace.mkdir(exist_ok=True)
        self.home_root = Path(self._temp.name) / "home"
        self.home_root.mkdir(exist_ok=True)
        db_patch = patch.object(server, "DB_FILE", str(self.db_path))
        db_patch.start()
        self.addCleanup(db_patch.stop)
        server.init_db()

    @contextmanager
    def _db(self):
        conn = sqlite3.connect(str(self.db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _now(self):
        return datetime.now(timezone.utc).isoformat()

    def _old(self, minutes):
        return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


class ReliabilityAPITests(IsolatedDatabaseTestCase):
    async def asyncSetUp(self):
        transport = httpx.ASGITransport(app=server.app)
        self.client = httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1", timeout=httpx.Timeout(TEST_HTTP_TIMEOUT))
        self.addAsyncCleanup(self.client.aclose)

    async def call(self, method, url, **kwargs):
        return await asyncio.wait_for(self.client.request(method, url, **kwargs), timeout=TEST_HTTP_TIMEOUT)

    async def get(self, url, **kwargs):
        return await self.call("GET", url, **kwargs)

    async def post(self, url, **kwargs):
        return await self.call("POST", url, **kwargs)

    async def delete(self, url, **kwargs):
        return await self.call("DELETE", url, **kwargs)

    async def put(self, url, **kwargs):
        return await self.call("PUT", url, **kwargs)

    async def _wait_for_status(self, url, statuses, timeout=TEST_HTTP_TIMEOUT):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            response = await self.get(url)
            self.assertEqual(response.status_code, 200, response.text[:500])
            last = response.json()
            if last.get("status") in statuses:
                return last
            await asyncio.sleep(0.05)
        self.fail(f"{url} never reached {sorted(statuses)}; last status={last and last.get('status')}")

    async def _wait_for_task_status(self, task_id, statuses, timeout=TEST_HTTP_TIMEOUT):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            task = server._task_row(task_id)
            if task and task.get("status") in statuses:
                return task
            await asyncio.sleep(0.05)
        task = server._task_row(task_id)
        self.fail(f"task {task_id} never reached {sorted(statuses)}; status={task and task.get('status')}")

    # --- Original regression tests ---

    async def test_health_and_diagnostics_are_readable(self):
        health = await self.get("/api/health")
        self.assertEqual(health.status_code, 200)
        self.assertIn("ollama", health.json())
        self.assertIn("resources", health.json())
        self.assertIn("memory", health.json())
        diagnostics = await self.get("/api/diagnostics")
        self.assertEqual(diagnostics.status_code, 200)
        network = await self.get("/api/network/diagnostics")
        self.assertIn(network.status_code, (200, 503))

    async def test_chat_job_persists_and_deduplicates_before_model_work(self):
        response = {"session_id": "test", "message": {"role": "assistant", "content": "ok"}, "software_tasks": []}
        with patch.object(server, "agent_chat", new=AsyncMock(return_value=response)) as model:
            body = {"session_id": "test", "prompt": "Please explain the service status."}
            first = await self.post("/api/chat/jobs", json=body, headers={"Idempotency-Key": "chat-test-1"})
            second = await self.post("/api/chat/jobs", json=body, headers={"Idempotency-Key": "chat-test-1"})
            mismatch = await self.post("/api/chat/jobs", json={**body, "prompt": "different request"}, headers={"Idempotency-Key": "chat-test-1"})
            self.assertEqual(first.status_code, 202)
            self.assertEqual(second.json()["request_id"], first.json()["request_id"])
            self.assertTrue(second.json()["reused"])
            self.assertEqual(mismatch.status_code, 409)
            status = await self._wait_for_status(first.json()["status_url"], TERMINAL_CHAT_STATUSES)
            self.assertEqual(status["status"], "completed")
            events = await self.get(first.json()["events_url"])
            self.assertIn("next_after_id", events.json())
            self.assertTrue(events.json()["events"])
            self.assertLessEqual(model.await_count, 1)

    async def test_task_is_acknowledged_and_idempotency_payload_checked(self):
        body = {
            "objective": "Create a tiny test artifact and verify it exists.",
            "workspace": str(self.workspace),
            "acceptance_criteria": ["The test artifact exists."],
            "verify_commands": ["test -f artifact.txt"],
            "model": "unavailable-is-checked-by-worker",
            "use_worktree": False,
        }
        with patch.object(server, "_run_software_task", new=AsyncMock()):
            first = await self.post("/api/tasks", json=body, headers={"Idempotency-Key": "task-test-1"})
            second = await self.post("/api/tasks", json=body, headers={"Idempotency-Key": "task-test-1"})
            mismatch = await self.post("/api/tasks", json={**body, "objective": "A different task objective."}, headers={"Idempotency-Key": "task-test-1"})
        self.assertEqual(first.status_code, 202)
        self.assertEqual(second.json()["task"]["task_id"], first.json()["task"]["task_id"])
        self.assertTrue(second.json()["reused"])
        self.assertEqual(mismatch.status_code, 409)

    async def test_file_copy_is_safe_and_refuses_overwrite(self):
        source = self.workspace / "source.txt"
        destination = self.workspace / "destination.txt"
        source.write_text("safe copy", encoding="utf-8")
        body = {"source": str(source), "destination": str(destination), "operation": "copy"}
        copied = await self.post("/api/files/transfer", json=body)
        conflict = await self.post("/api/files/transfer", json=body)
        self.assertEqual(copied.status_code, 200)
        self.assertEqual(destination.read_text(encoding="utf-8"), "safe copy")
        self.assertEqual(conflict.status_code, 409)

    async def test_privileged_command_audit_path_does_not_reference_uninitialized_hash(self):
        with patch.object(server, "_run_job", new=AsyncMock()):
            response = await self.post("/api/jobs", json={"command": "apt-get update", "as_root": True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["job"]["status"], "queued")

    async def test_confirmation_and_privileged_shell_guards(self):
        package = await self.post("/api/packages", json={"action": "remove", "package": "example"})
        service = await self.post("/api/services", json={"name": "example.service", "action": "restart"})
        self.assertEqual(package.status_code, 409)
        self.assertEqual(service.status_code, 409)
        self.assertIsNotNone(server._validate_privileged_shell("bash -c 'id'"))
        self.assertIsNotNone(server._validate_privileged_shell("apt-get install -o APT::Get::AllowUnauthenticated=true example"))
        self.assertIsNone(server._validate_privileged_shell("apt-get update"))

    # --- Database initialization and migrations ---

    def test_init_db_creates_all_tables_and_is_idempotent(self):
        expected = {"messages", "jobs", "agent_notes", "software_tasks", "software_task_events", "chat_requests", "chat_request_events"}
        with self._db() as conn:
            tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue(expected <= tables, tables)
        with self._db() as conn:
            self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        server.init_db()  # second run must not fail or lose data
        with self._db() as conn:
            still_there = conn.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='chat_requests'").fetchone()[0]
        self.assertEqual(still_there, 1)

    def test_init_db_migrates_legacy_schema(self):
        legacy_path = Path(self._temp.name) / "legacy.db"
        conn = sqlite3.connect(str(legacy_path))
        conn.executescript(
            """
            CREATE TABLE jobs (
                job_id TEXT PRIMARY KEY, kind TEXT NOT NULL, status TEXT NOT NULL, command TEXT NOT NULL,
                cwd TEXT NOT NULL, timeout_seconds INTEGER NOT NULL, stdout TEXT NOT NULL DEFAULT '',
                stderr TEXT NOT NULL DEFAULT '', exit_code INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                request_hash TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE software_tasks (
                task_id TEXT PRIMARY KEY, objective TEXT NOT NULL, workspace TEXT NOT NULL,
                status TEXT NOT NULL, model TEXT NOT NULL, acceptance_json TEXT NOT NULL, verify_json TEXT NOT NULL,
                max_steps INTEGER NOT NULL, max_seconds INTEGER NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE messages (session_id TEXT, role TEXT, content TEXT);
            """
        )
        conn.commit()
        conn.close()
        with patch.object(server, "DB_FILE", str(legacy_path)):
            server.init_db()
            with sqlite3.connect(str(legacy_path)) as migrated:
                job_columns = {row[1] for row in migrated.execute("PRAGMA table_info(jobs)")}
                task_columns = {row[1] for row in migrated.execute("PRAGMA table_info(software_tasks)")}
                chat_tables = {row[0] for row in migrated.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'chat%'")}
                events_tables = {row[0] for row in migrated.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'software_task%'")}
        self.assertIn("idempotency_key", job_columns)
        self.assertIn("checkpoint_json", task_columns)
        self.assertIn("heartbeat_at", task_columns)
        self.assertIn("parent_task_id", task_columns)
        self.assertEqual(chat_tables, {"chat_requests", "chat_request_events"})
        self.assertEqual(events_tables, {"software_tasks", "software_task_events"})

    # --- Persistent chat jobs ---

    async def test_chat_job_ticker_fields(self):
        payload = {"session_id": "s-ticker", "message": {"role": "assistant", "content": "ok"}, "software_tasks": []}
        with patch.object(server, "agent_chat", new=AsyncMock(return_value=payload)):
            response = await self.post("/api/chat/jobs", json={"session_id": "s-ticker", "prompt": "Ticker fields please."})
            self.assertEqual(response.status_code, 202)
            status = await self._wait_for_status(response.json()["status_url"], TERMINAL_CHAT_STATUSES)
            self.assertIn("elapsed_seconds", status)
            self.assertGreaterEqual(status["elapsed_seconds"], 0.0)
            self.assertIn("stream_url", status)
            self.assertTrue(status["stream_url"].endswith("/events/stream"))

    async def test_ollama_retry_covers_protocol_errors(self):
        response = _FakeOllamaResponse(content="ok")
        client = MagicMock()
        client.post = AsyncMock(side_effect=[httpx.RemoteProtocolError("Server disconnected without sending a response"), response])
        result = await server._post_ollama_with_retry(client, "http://127.0.0.1:11434/api/chat", {"model": "m"}, retries=2)
        self.assertEqual(client.post.await_count, 2)
        self.assertIs(result, response)
        exhausted = MagicMock()
        exhausted.post = AsyncMock(side_effect=httpx.RemoteProtocolError("Server disconnected"))
        with self.assertRaises(httpx.RemoteProtocolError):
            await server._post_ollama_with_retry(exhausted, "http://127.0.0.1:11434/api/chat", {"model": "m"}, retries=1)

    async def test_chat_job_events_stream(self):
        payload = {"session_id": "s-stream", "message": {"role": "assistant", "content": "ok"}, "software_tasks": []}

        async def read_stream():
            collected = []
            async with self.client.stream("GET", f"{response.json()['status_url']}/events/stream") as stream:
                async for line in stream.aiter_lines():
                    collected.append(line)
            return "\n".join(collected)

        with patch.object(server, "agent_chat", new=AsyncMock(return_value=payload)):
            response = await self.post("/api/chat/jobs", json={"session_id": "s-stream", "prompt": "Stream this chat."})
            self.assertEqual(response.status_code, 202)
            text = await asyncio.wait_for(read_stream(), timeout=TEST_HTTP_TIMEOUT)
        self.assertIn("event: status", text)
        self.assertIn("event: completed", text)
        self.assertIn("event: accepted", text)

    async def test_chat_job_status_and_event_endpoints(self):
        payload = {"session_id": "s-status", "message": {"role": "assistant", "content": "ok"}, "software_tasks": []}
        with patch.object(server, "agent_chat", new=AsyncMock(return_value=payload)):
            response = await self.post("/api/chat/jobs", json={"session_id": "s-status", "prompt": "Explain the status of this service."})
            self.assertEqual(response.status_code, 202)
            body = response.json()
            # The request is persisted before the 202 acknowledgment.
            with self._db() as conn:
                row = conn.execute("SELECT request_id,status FROM chat_requests WHERE request_id=?", (body["request_id"],)).fetchone()
            self.assertIsNotNone(row)
            self.assertIn(row["status"], {"queued", "running", "completed"})
            status = await self._wait_for_status(body["status_url"], TERMINAL_CHAT_STATUSES)
            self.assertEqual(status["status"], "completed")
            self.assertEqual(status["request_id"], body["request_id"])
            events = await self.get(body["events_url"])
            self.assertEqual(events.status_code, 200)
            first_page = events.json()
            self.assertTrue(first_page["events"])
            self.assertIn("next_after_id", first_page)
            tail = await self.get(f"{body['events_url']}?after_id={first_page['next_after_id']}")
            self.assertEqual(tail.json()["events"], [])
            event_types = {event["event_type"] for event in first_page["events"]}
            self.assertTrue({"accepted", "started", "completed"} <= event_types, event_types)

    async def test_chat_job_cancellation(self):
        async def slow_model(*args, **kwargs):
            await asyncio.sleep(5)
            return {"session_id": "s-cancel", "message": {"role": "assistant", "content": "ok"}, "software_tasks": []}

        with patch.object(server, "agent_chat", new=slow_model):
            response = await self.post("/api/chat/jobs", json={"session_id": "s-cancel", "prompt": "Start a slow job."})
            self.assertEqual(response.status_code, 202)
            cancel = await self.delete(response.json()["status_url"])
            self.assertEqual(cancel.status_code, 200)
            status = await self._wait_for_status(response.json()["status_url"], TERMINAL_CHAT_STATUSES)
            self.assertIn(status["status"], {"cancelled", "interrupted"})
            with self._db() as conn:
                row = conn.execute("SELECT status FROM chat_requests WHERE request_id=?", (response.json()["request_id"],)).fetchone()
            self.assertIn(row["status"], {"cancelled", "interrupted"})

    async def test_chat_job_capacity_returns_429_when_queue_full(self):
        with self._db() as conn:
            for index in range(server.MAX_PENDING_CHAT_REQUESTS):
                conn.execute(
                    "INSERT INTO chat_requests(request_id,idempotency_key,session_id,payload_hash,payload_json,status,created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?)",
                    (f"capacity-{index}", f"capacity-key-{index}", "s", "hash", "{}", self._now(), self._now()),
                )
            conn.commit()
        response = await self.post("/api/chat/jobs", json={"session_id": "s", "prompt": "over the limit"})
        self.assertEqual(response.status_code, 429)
        with self._db() as conn:
            conn.execute("UPDATE chat_requests SET status='completed'")
            conn.commit()
        accepted = await self.post("/api/chat/jobs", json={"session_id": "s", "prompt": "now there is room"})
        self.assertEqual(accepted.status_code, 202)

    async def test_chat_job_id_collision_with_live_duplicate_is_reused(self):
        payload = {"session_id": "s-reuse", "message": {"role": "assistant", "content": "ok"}, "software_tasks": []}
        async def slow_model(*args, **kwargs):
            await asyncio.sleep(2)
            return payload
        with patch.object(server, "agent_chat", new=slow_model):
            first = await self.post("/api/chat/jobs", json={"session_id": "s-reuse", "prompt": "Repeated send."})
            second = await self.post("/api/chat/jobs", json={"session_id": "s-reuse", "prompt": "Repeated send."})
            self.assertEqual(first.status_code, 202)
            self.assertEqual(second.status_code, 202)
            self.assertEqual(second.json()["request_id"], first.json()["request_id"])
            self.assertTrue(second.json()["reused"])

    # --- Restart recovery ---

    def test_startup_recovery_marks_interrupted_on_restart(self):
        now = self._now()
        with self._db() as conn:
            conn.execute(
                "INSERT INTO chat_requests(request_id,session_id,payload_hash,payload_json,status,created_at,updated_at) VALUES('r-chat','s','h','{}','running',?,?)",
                (now, now))
            conn.execute(
                "INSERT INTO jobs(job_id,kind,status,command,cwd,timeout_seconds,created_at,updated_at) VALUES('r-job','command','running','true','',5,?,?)",
                (now, now))
            conn.execute(
                "INSERT INTO software_tasks(task_id,objective,workspace,model,acceptance_json,verify_json,status,max_steps,max_seconds,created_at,updated_at) VALUES('r-task','obj',?,?,?,?,'running',4,300,?,?)",
                (str(self.workspace), "qwen3.5:4b", "[]", "[]", now, now))
            conn.commit()
        server.recover_records_after_restart()
        with self._db() as conn:
            chat = conn.execute("SELECT status FROM chat_requests WHERE request_id='r-chat'").fetchone()
            job = conn.execute("SELECT status FROM jobs WHERE job_id='r-job'").fetchone()
            task = conn.execute("SELECT status,phase FROM software_tasks WHERE task_id='r-task'").fetchone()
        self.assertEqual(chat["status"], "interrupted")
        self.assertEqual(job["status"], "interrupted")
        self.assertEqual(task["status"], "paused")
        self.assertEqual(task["phase"], "recovered_after_restart")

    async def test_recover_persisted_work_requeues_queued_chats(self):
        now = self._now()
        with self._db() as conn:
            conn.execute(
                "INSERT INTO chat_requests(request_id,idempotency_key,session_id,payload_hash,payload_json,status,created_at,updated_at) VALUES('recover-1','rec-key','s','h','{}','queued',?,?)",
                (now, now))
            conn.commit()
        payload = {"session_id": "s", "message": {"role": "assistant", "content": "ok"}, "software_tasks": []}
        with patch.object(server, "agent_chat", new=AsyncMock(return_value=payload)):
            await server.recover_persisted_work()
            status = await self._wait_for_status("/api/chat/jobs/recover-1", TERMINAL_CHAT_STATUSES)
        self.assertEqual(status["status"], "completed")

    # --- Stale-record sweeper ---

    def test_sweeper_marks_stale_running_chat_interrupted(self):
        old = self._old(server.STALE_WORKER_MINUTES + 10)
        with self._db() as conn:
            conn.execute(
                "INSERT INTO chat_requests(request_id,session_id,payload_hash,payload_json,status,created_at,updated_at) VALUES('stale-1','s','h','{}','running',?,?)",
                (old, old))
            conn.commit()
        server._sweep_stale_records()
        with self._db() as conn:
            row = conn.execute("SELECT status,error FROM chat_requests WHERE request_id='stale-1'").fetchone()
        self.assertEqual(row["status"], "interrupted")
        self.assertIn("inspect", row["error"])

    def test_sweeper_keeps_recently_updated_records(self):
        now = self._now()
        with self._db() as conn:
            conn.execute(
                "INSERT INTO chat_requests(request_id,session_id,payload_hash,payload_json,status,created_at,updated_at) VALUES('fresh-1','s','h','{}','running',?,?)",
                (now, now))
            conn.commit()
        server._sweep_stale_records()
        with self._db() as conn:
            row = conn.execute("SELECT status FROM chat_requests WHERE request_id='fresh-1'").fetchone()
        self.assertEqual(row["status"], "running")

    def test_sweeper_skips_chat_with_live_runner(self):
        old = self._old(server.STALE_WORKER_MINUTES + 10)
        with self._db() as conn:
            conn.execute(
                "INSERT INTO chat_requests(request_id,session_id,payload_hash,payload_json,status,created_at,updated_at) VALUES('live-1','s','h','{}','running',?,?)",
                (old, old))
            conn.commit()
        server.CHAT_REQUEST_RUNNERS["live-1"] = None
        try:
            server._sweep_stale_records()
        finally:
            server.CHAT_REQUEST_RUNNERS.pop("live-1", None)
        with self._db() as conn:
            row = conn.execute("SELECT status FROM chat_requests WHERE request_id='live-1'").fetchone()
        self.assertEqual(row["status"], "running")

    def test_sweeper_marks_stale_running_job_interrupted(self):
        old = self._old(server.STALE_WORKER_MINUTES + 10)
        with self._db() as conn:
            conn.execute(
                "INSERT INTO jobs(job_id,kind,status,command,cwd,timeout_seconds,created_at,updated_at) VALUES('stale-job','command','running','true','',5,?,?)",
                (old, old))
            conn.commit()
        server._sweep_stale_records()
        with self._db() as conn:
            row = conn.execute("SELECT status FROM jobs WHERE job_id='stale-job'").fetchone()
        self.assertEqual(row["status"], "interrupted")

    def test_sweeper_marks_stale_software_task_interrupted(self):
        old = self._old(server.STALE_WORKER_MINUTES + 10)
        with self._db() as conn:
            conn.execute(
                "INSERT INTO software_tasks(task_id,objective,workspace,model,acceptance_json,verify_json,status,phase,heartbeat_at,max_steps,max_seconds,created_at,updated_at) VALUES('stale-task-1','obj',?,?,?,?,'running','running',?,?,?,?,?)",
                (str(self.workspace), "qwen3.5:4b", "[]", "[]", old, 4, 300, old, old))
            conn.commit()
        server._sweep_stale_records()
        with self._db() as conn:
            row = conn.execute("SELECT status,phase,error FROM software_tasks WHERE task_id='stale-task-1'").fetchone()
        self.assertEqual(row["status"], "interrupted")
        self.assertIn("heartbeat", row["error"])

    def test_sweeper_skips_software_task_with_live_runner(self):
        old = self._old(server.STALE_WORKER_MINUTES + 10)
        with self._db() as conn:
            conn.execute(
                "INSERT INTO software_tasks(task_id,objective,workspace,model,acceptance_json,verify_json,status,phase,heartbeat_at,max_steps,max_seconds,created_at,updated_at) VALUES('live-task-1','obj',?,?,?,?,'running','running',?,?,?,?,?)",
                (str(self.workspace), "qwen3.5:4b", "[]", "[]", old, 4, 300, old, old))
            conn.commit()
        server.SOFTWARE_TASK_RUNNERS["live-task-1"] = None
        try:
            server._sweep_stale_records()
        finally:
            server.SOFTWARE_TASK_RUNNERS.pop("live-task-1", None)
        with self._db() as conn:
            row = conn.execute("SELECT status FROM software_tasks WHERE task_id='live-task-1'").fetchone()
        self.assertEqual(row["status"], "running")

    # --- Software task endpoints ---

    def _task_body(self, workspace):
        return {
            "objective": "Test task objective with a stable description.",
            "workspace": str(workspace),
            "acceptance_criteria": ["The workspace is inspected."],
            "verify_commands": ["true"],
            "model": "qwen3.5:4b",
            "use_worktree": False,
        }

    async def test_task_endpoints_status_events_and_cancel(self):
        with patch.object(server, "_run_software_task", new=AsyncMock()):
            first = await self.post("/api/tasks", json=self._task_body(self.workspace), headers={"Idempotency-Key": "task-api-1"})
            self.assertEqual(first.status_code, 202)
            task_id = first.json()["task"]["task_id"]
            self.assertFalse(first.json()["reused"])
            detail = await self.get(f"/api/tasks/{task_id}")
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(detail.json()["task"]["task_id"], task_id)
            events = await self.get(f"/api/tasks/{task_id}/events")
            self.assertEqual(events.status_code, 200)
            self.assertIn("events", events.json())
            cancel = await self.delete(f"/api/tasks/{task_id}")
            self.assertTrue(cancel.json()["cancelled"])
            listed = await self.get("/api/tasks")
            self.assertIn(task_id, {task["task_id"] for task in listed.json()["tasks"]})

    async def test_task_pause_and_resume(self):
        with patch.object(server, "_run_software_task", new=AsyncMock()):
            first = await self.post("/api/tasks", json=self._task_body(self.workspace), headers={"Idempotency-Key": "task-pause-1"})
            task_id = first.json()["task"]["task_id"]
            paused = await self.post(f"/api/tasks/{task_id}/pause")
            self.assertEqual(paused.status_code, 200)
            self.assertTrue(paused.json()["task"]["status"] in {"queued", "running", "paused"})
            resumed = await self.post(f"/api/tasks/{task_id}/resume", json={"message": ""})
            self.assertEqual(resumed.status_code, 200)

    async def test_software_task_checkpoint_and_heartbeat_persist(self):
        def read_file_response():
            return _FakeOllamaResponse(tool_calls=[{"function": {"name": "read_file", "arguments": {"path": "notes.txt"}}}])
        def complete_response():
            return _FakeOllamaResponse(content="TASK_STATUS: COMPLETE\nDone.")
        with patch.object(server, "_post_ollama_with_retry", new=AsyncMock(side_effect=[read_file_response(), complete_response(), complete_response()])), \
             patch.object(server, "_execute_task_tool", new=AsyncMock(return_value={"status": "completed", "exit_code": 0, "stdout": "ok", "stderr": ""})), \
             patch.object(server, "_prepare_task_worktree", new=MagicMock(return_value={"path": str(self.workspace), "mode": "direct", "base_commit": ""})), \
             patch.object(server, "_verify_software_task", new=AsyncMock(return_value={"passed": True, "checks": []})), \
             patch.object(server, "_available_ollama_model_names", new=MagicMock(return_value=["qwen3.5:4b"])):
            response = await self.post("/api/tasks", json=self._task_body(self.workspace), headers={"Idempotency-Key": "task-ckpt-1"})
            task_id = response.json()["task"]["task_id"]
            task = await self._wait_for_task_status(task_id, TERMINAL_TASK_STATUSES)
            self.assertEqual(task["status"], "completed")
            self.assertTrue(task["final_report"])
            self.assertIn("read_file", task["checkpoint"].get("last_tool", {}).get("name", ""))
            self.assertTrue(task["heartbeat_at"])

    async def test_repeated_read_file_calls_pause_task_as_stalled(self):
        def read_file_response():
            return _FakeOllamaResponse(tool_calls=[{"function": {"name": "read_file", "arguments": {"path": "notes.txt"}}}])
        task_id = str(uuid.uuid4())
        with self._db() as conn:
            conn.execute(
                "INSERT INTO software_tasks(task_id,objective,workspace,model,acceptance_json,verify_json,status,phase,max_steps,max_seconds,created_at,updated_at) VALUES(?,?,?,?,?,?,'queued','queued',8,300,?,?)",
                (task_id, "Test task objective.", str(self.workspace), "qwen3.5:4b", "[]", "[]", self._now(), self._now()))
            conn.commit()
        with patch.object(server, "_post_ollama_with_retry", new=AsyncMock(side_effect=[read_file_response() for _ in range(6)])), \
             patch.object(server, "_execute_task_tool", new=AsyncMock(return_value={"status": "completed", "exit_code": 0, "stdout": "ok", "stderr": ""})), \
             patch.object(server, "_prepare_task_worktree", new=MagicMock(return_value={"path": str(self.workspace), "mode": "direct", "base_commit": ""})), \
             patch.object(server, "_available_ollama_model_names", new=MagicMock(return_value=["qwen3.5:4b"])):
            await asyncio.wait_for(server._run_software_task(task_id), timeout=TEST_HTTP_TIMEOUT)
        task = server._task_row(task_id)
        self.assertEqual(task["status"], "paused")
        self.assertEqual(task["phase"], "stalled")
        self.assertIn("read_file", task["checkpoint"].get("last_tool", {}).get("name", ""))

    async def test_safe_task_resumes_after_restart(self):
        def complete_response():
            return _FakeOllamaResponse(content="TASK_STATUS: COMPLETE\nAll done.")
        now = self._now()
        task_id = "resume-task-1"
        with self._db() as conn:
            conn.execute(
                "INSERT INTO software_tasks(task_id,objective,workspace,model,acceptance_json,verify_json,status,phase,max_steps,max_seconds,created_at,updated_at) VALUES(?,?,?,?,?,?,'paused','recovered_after_restart',8,300,?,?)",
                (task_id, "Test task objective.", str(self.workspace), "qwen3.5:4b", "[]", "[]", now, now))
            conn.commit()
        with patch.object(server, "_post_ollama_with_retry", new=AsyncMock(side_effect=[complete_response(), complete_response()])), \
             patch.object(server, "_prepare_task_worktree", new=MagicMock(return_value={"path": str(self.workspace), "mode": "direct", "base_commit": ""})), \
             patch.object(server, "_verify_software_task", new=AsyncMock(return_value={"passed": True, "checks": []})), \
             patch.object(server, "_available_ollama_model_names", new=MagicMock(return_value=["qwen3.5:4b"])):
            await asyncio.wait_for(server._run_software_task(task_id), timeout=TEST_HTTP_TIMEOUT)
        task = server._task_row(task_id)
        self.assertEqual(task["status"], "completed")

    async def test_software_task_pauses_when_ollama_unavailable(self):
        with patch.object(server, "_available_ollama_model_names", new=MagicMock(return_value=[])), \
             patch.object(server, "_prepare_task_worktree", new=MagicMock(return_value={"path": str(self.workspace), "mode": "direct", "base_commit": ""})):
            response = await self.post("/api/tasks", json=self._task_body(self.workspace), headers={"Idempotency-Key": "ollama-down-1"})
            task_id = response.json()["task"]["task_id"]
            task = await self._wait_for_task_status(task_id, {"paused"})
            self.assertEqual(task["phase"], "ollama_unavailable")

    # --- Jobs API ---

    async def test_command_job_lifecycle_endpoints(self):
        with patch.object(server, "_run_job", new=AsyncMock()):
            response = await self.post("/api/jobs", json={"command": "echo reliability", "cwd": "", "timeout_seconds": 30})
            self.assertEqual(response.status_code, 200)
            job_id = response.json()["job"]["job_id"]
            self.assertEqual(response.json()["job"]["status"], "queued")
            detail = await self.get(f"/api/jobs/{job_id}")
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(detail.json()["job"]["job_id"], job_id)
            listed = await self.get("/api/jobs")
            self.assertIn(job_id, {job["job_id"] for job in listed.json()["jobs"]})

    # --- Security ---

    async def test_privileged_confirmation_required_across_surfaces(self):
        service = await self.post("/api/services", json={"name": "example.service", "action": "restart"})
        package = await self.post("/api/packages", json={"action": "remove", "package": "example"})
        write = await self.put("/api/file", json={"path": str(self.workspace / "elevated.txt"), "content": "x", "as_root": True})
        transfer = await self.post("/api/files/transfer", json={"source": str(self.workspace), "destination": str(self.home_root / "copied"), "operation": "copy", "as_root": True})
        self.assertEqual(service.status_code, 409)
        self.assertEqual(package.status_code, 409)
        self.assertEqual(write.status_code, 409)
        self.assertEqual(transfer.status_code, 409)

    async def test_path_validation_and_traversal(self):
        source = self.workspace / "src.txt"
        source.write_text("data", encoding="utf-8")
        existing = self.workspace / "dest.txt"
        existing.write_text("old", encoding="utf-8")
        conflict = await self.post("/api/files/transfer", json={"source": str(source), "destination": str(existing), "operation": "copy"})
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(existing.read_text(encoding="utf-8"), "old")
        with patch.object(server, "HOME_DIR", str(self.home_root)):
            expanded = server.expand_path("relative/file.txt")
            self.assertEqual(Path(expanded), self.home_root / "relative" / "file.txt")
        result = await asyncio.to_thread(server.run_command, "sudo ls")
        self.assertIsNone(result["exit_code"])
        self.assertIn("blocked", result["stderr"])
        ok = await asyncio.to_thread(server.run_command, "true")
        self.assertEqual(ok["exit_code"], 0)

    async def test_absolute_sudo_path_is_blocked(self):
        result = await asyncio.to_thread(server.run_command, "/usr/bin/sudo -n /bin/true")
        self.assertIsNone(result["exit_code"])
        self.assertIn("blocked", result["stderr"])
        self.assertIsNotNone(server._validate_privileged_shell("/usr/bin/sudo -n id"))
        self.assertTrue(server._has_sudo("/usr/bin/sudo -n id"))
        self.assertFalse(server._has_sudo("echo sudoers"))

    async def test_logs_line_count_is_bounded(self):
        self.assertEqual((await self.get("/api/logs?lines=0")).status_code, 422)
        self.assertEqual((await self.get("/api/logs?lines=-1")).status_code, 422)
        self.assertEqual((await self.get("/api/logs?lines=501")).status_code, 422)

    async def test_system_stats_route_is_distinct(self):
        paths = [route.path for route in server.app.routes
                 if getattr(route, "path", None) == "/api/stats"]
        self.assertEqual(len(paths), 1)
        response = await self.get("/api/system-stats")
        self.assertEqual(response.status_code, 200)

    def test_venv_interpreter_write_is_refused(self):
        with patch.object(server, "HOME_DIR", str(self.home_root)):
            target = self.home_root / "venv" / "bin" / "python"
            result = server.write_file(str(target), "malicious")
        self.assertIn("refusing", result)
        self.assertFalse(target.exists())

    def test_privileged_shell_allowlist(self):
        self.assertIsNone(server._validate_privileged_shell("apt-get update"))
        self.assertIsNotNone(server._validate_privileged_shell("apt-get install -o APT::Get::AllowUnauthenticated=true example"))
        self.assertIsNotNone(server._validate_privileged_shell("sudo apt-get update"))
        self.assertIsNotNone(server._validate_privileged_shell("rm -rf /"))
        self.assertIsNotNone(server._validate_privileged_shell("apt-get install --evil x"))

    def test_manage_process_guards(self):
        listed = server.manage_process("list")
        self.assertIn("processes", listed)
        without_confirm = server.manage_process("terminate", pid=os.getpid() + 1, confirm=False)
        self.assertIn("confirmation_required", without_confirm["error"])
        refusing = server.manage_process("terminate", pid=os.getpid(), confirm=True)
        self.assertIn("Refusing", refusing["error"])
        bad_signal = server.manage_process("terminate", pid=2, signal_name="KILL", confirm=True)
        self.assertIn("Allowed signals", bad_signal["error"])

    def test_research_tool_returns_sources(self):
        fake_results = {"query": "q", "results": [{"title": "T", "url": "https://example.com"}], "error": None, "provider": "DuckDuckGo"}
        with patch.object(server, "search_web_results", return_value=fake_results):
            result = server.research({"query": "new backend patterns"})
            self.assertEqual(result["query"], "new backend patterns")
            self.assertIn("web_search", result)
            self.assertIsNone(result["web_search"]["error"])
            self.assertTrue(result["web_search"]["results"])
        empty = server.research({"query": "   "})
        self.assertIn("error", empty)

    def test_research_tool_registered(self):
        names = [tool["function"]["name"] for tool in server.TOOLS]
        self.assertIn("research", names)
        self.assertIn("research", {tool["function"]["name"] for tool in server.SOFTWARE_TASK_TOOLS})

    async def test_transfer_endpoint_refuses_existing_destination(self):
        # The endpoint itself refuses to overwrite an existing destination.
        source = self.workspace / "s.txt"
        source.write_text("x", encoding="utf-8")
        destination = self.workspace / "d.txt"
        destination.write_text("y", encoding="utf-8")
        request = server.FileTransferReq(source=str(source), destination=str(destination), operation="copy")
        from starlette.exceptions import HTTPException
        with self.assertRaises(HTTPException) as ctx:
            await server.transfer_file_endpoint(request)
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(destination.read_text(encoding="utf-8"), "y")


class TimestampParsingTests(unittest.TestCase):
    def test_parse_timestamp_mixed_formats(self):
        expected = datetime(2026, 11, 20, 19, 0, 0, tzinfo=timezone.utc).timestamp()
        sqlite_style = server._parse_timestamp("2026-11-20 19:00:00")
        iso_style = server._parse_timestamp("2026-11-20T19:00:00+00:00")
        zulu_style = server._parse_timestamp("2026-11-20T19:00:00Z")
        for parsed in (sqlite_style, iso_style, zulu_style):
            self.assertIsNotNone(parsed)
            self.assertAlmostEqual(parsed, expected, delta=5)
        self.assertIsNone(server._parse_timestamp(None))
        self.assertIsNone(server._parse_timestamp(""))
        self.assertIsNone(server._parse_timestamp("not-a-date"))


class WatchdogTests(unittest.TestCase):
    def test_restart_budget_is_bounded(self):
        clock = [1000.0]
        budget = watchdog.RestartBudget(now=lambda: clock[0])
        restarts = []
        # Three restarts are allowed inside one window; a fourth is blocked by
        # the cooldown until the window rolls over.
        for _ in range(4):
            allowed = budget.can_restart()
            if allowed:
                budget.record_restart()
                restarts.append(clock[0])
                clock[0] += watchdog.RESTART_COOLDOWN_SECONDS + 10
            else:
                break
        self.assertEqual(len(restarts), 3)
        self.assertFalse(budget.can_restart())
        clock[0] += watchdog.RESTART_WINDOW_SECONDS + 1
        self.assertTrue(budget.can_restart())

    def test_system_memory_percent_is_readable(self):
        percent = watchdog._system_memory_percent()
        self.assertIsInstance(percent, float)
        self.assertGreaterEqual(percent, 0.0)
        self.assertLessEqual(percent, 100.0)

    def test_check_once_bounded_and_reports_exhaustion(self):
        clock = [1000.0]
        budget = watchdog.RestartBudget(now=lambda: clock[0])
        restarts = []
        with patch.object(watchdog.subprocess, "run", MagicMock(return_value=MagicMock(returncode=1, stderr=b"", stdout=b""))):
            for _ in range(12):
                watchdog.check_once(budget, restart_service=lambda: restarts.append(clock[0]))
                clock[0] += 30
        self.assertEqual(len(restarts), watchdog.RESTARTS_PER_WINDOW)
        self.assertTrue(budget.exhausted_reported)

    def test_check_once_active_service_clears_budget(self):
        clock = [1000.0]
        budget = watchdog.RestartBudget(now=lambda: clock[0])
        budget.record_restart()
        with patch.object(watchdog.subprocess, "run", MagicMock(return_value=MagicMock(returncode=0, stderr=b"", stdout=b""))):
            self.assertFalse(watchdog.check_once(budget))
        self.assertEqual(budget.attempts, 0)


    def test_safe_tool_excerpt_preserves_short_text(self):
        text = "Hello world <div class=\'test\'>content</div>"
        self.assertEqual(server._safe_tool_excerpt(text, limit=100), text)
        self.assertEqual(server._safe_tool_excerpt("", limit=10), "")
        self.assertEqual(server._safe_tool_excerpt(None, limit=10), "")

    def test_safe_tool_excerpt_avoids_slicing_xml_tags(self):
        text = "Start " + ("x" * 20) + " <div class=\'broken\' extra=\'data\'>" + ("y" * 50)
        cut = server._safe_tool_excerpt(text, limit=35)
        self.assertNotIn("<div", cut)
        self.assertTrue(cut.endswith("\n[output truncated]"))

        closed_text = "<b>bold</b> rest of text here"
        cut_closed = server._safe_tool_excerpt(closed_text, limit=15)
        self.assertTrue(cut_closed.startswith("<b>bold</b>"))
        self.assertTrue(cut_closed.endswith("\n[output truncated]"))

    def test_select_tools_direct_mode_budgets(self):
        chat_tools = server.select_tools("What is the meaning of life?", mode="direct")
        self.assertEqual(len(chat_tools), 0)

        action_tools = server.select_tools("Create /tmp/hello.txt containing hi", mode="direct")
        action_names = {t["function"]["name"] for t in action_tools}
        self.assertIn("write_file", action_names)
        self.assertNotIn("start_software_task", action_names)
        self.assertNotIn("start_background_command", action_names)


    def test_select_chat_model_fast_path_for_short_queries(self):
        available = ["qwen2.5-coder:1.5b", "qwen3.5:4b", "llama3.2:3b"]
        
        # Short conversational query routes to fast path
        model, fast = server.select_chat_model("qwen3.5:4b", "Hello, how are you?", action_required=False, images=[], available=available)
        self.assertEqual(model, "qwen2.5-coder:1.5b")
        self.assertTrue(fast)

        # Action query routes to 4B
        model, fast = server.select_chat_model("qwen3.5:4b", "Create /home/snow/Desktop/test.txt", action_required=True, images=[], available=available)
        self.assertEqual(model, "qwen3.5:4b")
        self.assertFalse(fast)

        # Query with images routes to 4B
        model, fast = server.select_chat_model("qwen3.5:4b", "Look at this", action_required=False, images=["img1"], available=available)
        self.assertEqual(model, "qwen3.5:4b")
        self.assertFalse(fast)

        # Explicit custom model is honored
        model, fast = server.select_chat_model("llama3.2:3b", "Hello", action_required=False, images=[], available=available)
        self.assertEqual(model, "llama3.2:3b")
        self.assertFalse(fast)

    def test_get_session_history_auto_compaction(self):
        sid = "test-compaction-session"
        with server._db() as conn:
            # Insert 15 conversation turns
            for i in range(15):
                conn.execute("INSERT INTO messages (session_id, role, content) VALUES (?, 'user', ?)", (sid, f"Step {i}"))
                conn.execute("INSERT INTO messages (session_id, role, content) VALUES (?, 'assistant', ?)", (sid, f"Wrote /home/snow/Desktop/project/file{i}.py"))
                conn.commit()
        
        history = server.get_session_history(sid, limit=8)
        self.assertTrue(len(history) > 0)
        # Regression: history must be chronological with user/assistant pairs matched.
        # (A subquery that drops ROWID reverses rows and mismatches pairs.)
        self.assertEqual(history[1]["content"], "Step 11")
        for idx in range(1, len(history), 2):
            self.assertEqual(history[idx]["role"], "user")
            self.assertEqual(history[idx + 1]["role"], "assistant")
            self.assertIn(history[idx]["content"].split()[-1], history[idx + 1]["content"])
        # Check that compaction note is prepended
        first_msg = history[0]
        self.assertEqual(first_msg["role"], "system")
        self.assertIn("Earlier in this session", first_msg["content"])
        self.assertIn("file", first_msg["content"])



class ActionClassificationTests(unittest.TestCase):
    """Short file-edit prompts must be classified as actions (C3 routing)."""

    def test_edit_verbs_with_paths_are_actions(self):
        for prompt in (
            "Change /home/snow/Desktop/a.txt to contain: updated",
            "Update /home/snow/a.txt with the new version",
            "Replace the content of /home/snow/index.html",
            "Add a contact form to /home/snow/site/index.html",
            "Generate a script at /home/snow/clean.py",
        ):
            with self.subTest(prompt=prompt):
                self.assertTrue(server._requires_pc_action(prompt))

    def test_questions_and_chat_are_not_actions(self):
        for prompt in (
            "What is Tailscale?",
            "How do I build a website?",
            "Explain the change to the login flow",
            "Tailscale is a mesh VPN.",
        ):
            with self.subTest(prompt=prompt):
                self.assertFalse(server._requires_pc_action(prompt))

    def test_edit_prompt_routes_strong_model(self):
        prompt = "Change /home/snow/a.txt to contain: updated"
        model, fast = server.select_chat_model(
            "qwen3.5:4b", prompt, server._requires_pc_action(prompt), [],
            ["qwen2.5-coder:1.5b", "qwen3.5:4b"],
        )
        self.assertEqual(model, "qwen3.5:4b")
        self.assertFalse(fast)


class SelfVerificationTests(unittest.IsolatedAsyncioTestCase):
    """Track 4.2: written Python files get a real syntax check."""

    async def test_valid_and_invalid_python_are_reported(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            good = Path(tmp) / "good.py"
            bad = Path(tmp) / "bad.py"
            good.write_text("x = 1\n")
            bad.write_text("def broken(:\n")
            results = [
                {"tool": "write_file", "arguments": {"path": str(good)}},
                {"tool": "write_file", "arguments": {"path": str(bad)}},
            ]
            checks = await server._verify_written_python_files(results)
        self.assertEqual(len(checks), 2)
        self.assertEqual(checks[0]["exit_code"], 0)
        self.assertNotEqual(checks[1]["exit_code"], 0)
        self.assertIn("Syntax", checks[0]["output"])


class ContinuationAndSuccessTests(unittest.TestCase):
    """Short follow-ups inherit action intent; narrated success is not a completion."""

    def test_followups_are_actions(self):
        for prompt in ("write the others", "create them", "write the rest",
                       "continue", "go on", "add the remaining files", "what about the others"):
            with self.subTest(prompt=prompt):
                self.assertTrue(server._is_action_continuation(prompt))

    def test_chat_is_not_a_followup(self):
        for prompt in ("What is Tailscale?", "Explain the change", "Hello there"):
            with self.subTest(prompt=prompt):
                self.assertFalse(server._is_action_continuation(prompt))

    def test_hallucinated_success_detected_without_tools(self):
        text = "**Remote Execution Summary:**\nThe file `/home/snow/site/README.md` has been successfully created."
        self.assertTrue(server._claims_work_without_tools(text, []))
        self.assertFalse(server._claims_work_without_tools(text, [{"tool": "write_file"}]))

    def test_plain_answer_is_not_false_success(self):
        self.assertFalse(server._claims_work_without_tools("Tailscale is a mesh VPN.", []))


class ExecutiveContextTests(unittest.TestCase):
    """Rolling C4 summary is bounded and only injected for compacted sessions."""

    def test_summary_updates_and_is_bounded(self):
        sid = "exec-summary-test"
        with server._db() as conn:
            conn.execute("DELETE FROM session_summaries WHERE session_id=?", (sid,))
        server._update_session_summary(sid, "Keep the original colors. Use CSS only.", {
            "tool_results": [{"tool": "write_file", "arguments": {"path": "/home/snow/site/index.html"}}],
            "actions": ["write_file(/home/snow/site/index.html)"],
            "execution_status": "tools_invoked",
        })
        with server._db() as conn:
            row = conn.execute("SELECT summary FROM session_summaries WHERE session_id=?", (sid,)).fetchone()
        self.assertIsNotNone(row)
        self.assertIn("Constraints:", row[0])
        self.assertIn("index.html", row[0])
        self.assertLessEqual(len(row[0]), 1400)
        with server._db() as conn:
            conn.execute("DELETE FROM session_summaries WHERE session_id=?", (sid,))


class WebsiteScaffoldTests(unittest.TestCase):
    """Explicit website builds get real files even when the model emits none."""

    def test_scaffold_writes_three_verified_files(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            records = server._scaffold_website(tmp)
        self.assertEqual(len(records), 3)
        self.assertEqual([r["arguments"]["path"].rsplit("/", 1)[-1] for r in records],
                         ["index.html", "styles.css", "README.md"])
        self.assertTrue(all(r["data"]["fallback_scaffold"] for r in records))


class LaunchApplicationTests(unittest.TestCase):
    """Open-on-PC uses xdg-open for real absolute file paths."""

    def test_absolute_file_path_uses_xdg_open(self):
        import tempfile
        from unittest.mock import MagicMock, patch
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "index.html"
            target.write_text("<html></html>")
            fake_proc = MagicMock()
            fake_proc.poll.return_value = None
            fake_proc.pid = 4242
            with patch.object(server.shutil, "which",
                              side_effect=lambda name: "/usr/bin/xdg-open" if name == "xdg-open" else None),                  patch.object(server.subprocess, "Popen", return_value=fake_proc) as popen:
                result = server.launch_application(str(target))
        self.assertEqual(result.get("status"), "launched")
        self.assertEqual(popen.call_args[0][0], ["/usr/bin/xdg-open", str(target)])


class InitDbSideEffectTests(unittest.TestCase):
    """init_db (and therefore importing server) must not interrupt live jobs."""

    def test_init_db_preserves_running_chat_requests(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            with patch.object(server, "DB_FILE", str(Path(tmp) / "side-effect.db")):
                server.init_db()
                with server._db() as conn:
                    conn.execute(
                        "INSERT INTO chat_requests(request_id,session_id,payload_hash,payload_json,status,created_at,updated_at) "
                        "VALUES('r1','s1','h','{}','running','now','now')"
                    )
                    conn.commit()
                server.init_db()
                with server._db() as conn:
                    row = conn.execute("SELECT status FROM chat_requests WHERE request_id='r1'").fetchone()
        self.assertEqual(row[0], "running")


class WebVerificationTests(unittest.IsolatedAsyncioTestCase):
    """Written HTML/JS files get inline structure/syntax checks."""

    async def test_html_and_js_checks(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            html = Path(tmp) / "index.html"
            js = Path(tmp) / "app.js"
            html.write_text("<html><body><h1>Hello</h1></body></html>")
            js.write_text("const x = 1;\n")
            checks = await server._verify_written_web_files([
                {"tool": "write_file", "arguments": {"path": str(html)}},
                {"tool": "write_file", "arguments": {"path": str(js)}},
            ])
        self.assertEqual(len(checks), 2)
        self.assertTrue(all(c["exit_code"] == 0 for c in checks))


class WebsiteCompletionTests(unittest.TestCase):
    """Partial website work is completed without overwriting existing files."""

    def test_missing_files_added_existing_preserved(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            existing = Path(tmp) / "index.html"
            existing.write_text("<html><body>custom</body></html>")
            added = server._complete_website_files(tmp)
            self.assertEqual(len(added), 2)
            self.assertEqual([a["arguments"]["path"].rsplit("/", 1)[-1] for a in added],
                             ["styles.css", "README.md"])
            self.assertEqual(existing.read_text(), "<html><body>custom</body></html>")


class ActionBudgetTests(unittest.TestCase):
    """Action jobs get a larger generation budget and more turns than chat."""

    def test_action_budget_is_larger_than_chat(self):
        self.assertGreater(server.OLLAMA_NUM_PREDICT_ACTION, server.OLLAMA_NUM_PREDICT)

    def test_turn_budget_helper(self):
        self.assertEqual(server._chat_turn_budget("create a website with css"), 16)
        self.assertEqual(server._chat_turn_budget("fix the file at /home/snow/a.py"), 10)
        self.assertEqual(server._chat_turn_budget("What is Tailscale?"), 6)


class FastWebsiteRoutingTests(unittest.TestCase):
    """Simple HTML/CSS sites use the fast model; complex sites stay on strong."""

    def test_simple_site_routes_fast(self):
        available = ["qwen2.5-coder:1.5b", "qwen3.5:4b"]
        prompt = "create a website with css and a readme on desktop"
        self.assertTrue(server._is_simple_website_request(prompt))
        model, fast = server.select_chat_model("qwen3.5:4b", prompt, True, [], available,
                                               simple_code=server._is_simple_website_request(prompt))
        self.assertEqual(model, "qwen3.5:4b")
        self.assertFalse(fast)
        with patch.object(server, "FAST_ACTION_ROUTING", True):
            model, fast = server.select_chat_model("qwen3.5:4b", prompt, True, [], available,
                                                   simple_code=True)
        self.assertEqual(model, "qwen2.5-coder:1.5b")
        self.assertTrue(fast)

    def test_complex_site_stays_strong(self):
        for prompt in ("build a react website with a backend and database",
                       "create a full-stack site with auth"):
            self.assertFalse(server._is_simple_website_request(prompt))
            model, fast = server.select_chat_model("qwen3.5:4b", prompt, True, [],
                                                   ["qwen2.5-coder:1.5b", "qwen3.5:4b"],
                                                   simple_code=server._is_simple_website_request(prompt))
            self.assertEqual(model, "qwen3.5:4b")
            self.assertFalse(fast)

    def test_json_tool_prose_detected(self):
        self.assertTrue(server._looks_like_json_tool_call(
            '```json\n{"name": "write_file", "arguments": {"path": "/tmp/a"}}\n```'))
        self.assertFalse(server._looks_like_json_tool_call("Tailscale is a VPN."))


class MissedActionTests(unittest.TestCase):
    """Website follow-ups with edit verbs are actions, not chat."""

    def test_website_followups_are_actions(self):
        for prompt in (
            "include a login page with routing",
            "upgrades the website that you already made",
            "make a json and more buttons, login page with routing to it",
        ):
            with self.subTest(prompt=prompt):
                self.assertTrue(server._requires_pc_action(prompt))
                self.assertEqual(
                    server.select_chat_model("qwen3.5:4b", prompt, True, [],
                                             ["qwen2.5-coder:1.5b", "qwen3.5:4b"])[0],
                    "qwen3.5:4b")

    def test_fake_summary_detected_without_tools(self):
        fake = "**Remote Execution Summary:**\n- `write_file(/home/snow/site/index.html)`\nDone."
        self.assertTrue(server._claims_work_without_tools(fake, []))
        self.assertFalse(server._claims_work_without_tools(fake, [{"tool": "write_file"}]))


class NextBatchTests(unittest.TestCase):
    """Readme edits are actions; final answers carry a compact verified-change block."""

    def test_readme_edit_is_action(self):
        for prompt in ("update the readme accordingly to what has done",
                       "update the documentation", "write a README.md"):
            with self.subTest(prompt=prompt):
                self.assertTrue(server._requires_pc_action(prompt))

    def test_changes_summary_is_clean(self):
        summary = server._build_changes_summary([
            {"tool": "write_file", "arguments": {"path": "/home/snow/site/index.html"}},
            {"tool": "verify_web", "output": "HTML structure OK: /home/snow/site/index.html", "exit_code": 0},
            {"tool": "read_file", "output": "body"},
        ])
        self.assertIn("**Changes verified**", summary)
        self.assertIn("index.html", summary)
        self.assertIn("HTML structure OK", summary)
        self.assertNotIn("## Tool output", summary)
        self.assertEqual(server._build_changes_summary([]), "")


class NoOpAnswerTests(unittest.TestCase):
    """No-op answers never contain narrated work that did not happen."""

    def test_narrated_changes_removed(self):
        fake = "Enhanced the website by updating styles.css with dark mode and new buttons."
        self.assertEqual(server._clean_noop_answer(fake), "")

    def test_short_clarifying_question_kept(self):
        question = "Should I add a dark mode toggle, or a green theme?"
        self.assertEqual(server._clean_noop_answer(question), question)


class SessionMemoryV2Tests(unittest.TestCase):
    """Successful work is retained; no-op turns cannot erase it."""

    def test_state_survives_noop_turn(self):
        sid = "mem-v2-test"
        with server._db() as conn:
            conn.execute("DELETE FROM session_state WHERE session_id=?", (sid,))
            conn.execute("DELETE FROM session_turns WHERE session_id=?", (sid,))
        server._update_session_state(sid, "build a site", {
            "tool_results": [{"tool": "write_file", "arguments": {"path": "/home/snow/site/index.html"}}],
            "actions": ["write_file(/home/snow/site/index.html)"],
            "execution_status": "tools_invoked", "assistant_text": "Created index.html",
        }, request_id="r1")
        server._update_session_state(sid, "enhance it", {
            "tool_results": [{"tool": "read_file", "arguments": {"path": "/home/snow/site/index.html"}}],
            "actions": ["read_file(/home/snow/site/index.html)"],
            "execution_status": "not_executed", "assistant_text": "No changes",
        }, request_id="r2")
        state = server.get_session_state(sid)
        self.assertIn("/home/snow/site/index.html", state["files"])
        self.assertIsNotNone(state["last_success"])
        history = server.get_session_history(sid, limit=12)
        self.assertTrue(any(m["role"] == "system" and "Session state" in str(m["content"]) for m in history))
        self.assertTrue(any("build a site" in str(m.get("content")) for m in history))
        with server._db() as conn:
            conn.execute("DELETE FROM session_state WHERE session_id=?", (sid,))
            conn.execute("DELETE FROM session_turns WHERE session_id=?", (sid,))


class CloudAdapterTests(unittest.TestCase):
    """Cloud model names and catalog injection are opt-in and env-driven."""

    def test_model_name_detection(self):
        with patch.object(server, "CLOUD_ENABLED", True):
            self.assertTrue(server._is_cloud_model("cloud:fast"))
            self.assertTrue(server._is_cloud_model("cloud:strong"))
            self.assertFalse(server._is_cloud_model("qwen3.5:4b"))

    def test_catalog_injection(self):
        with patch.object(server, "CLOUD_ENABLED", True),              patch.object(server, "CLOUD_FAST_MODEL", "test-fast"),              patch.object(server, "CLOUD_STRONG_MODEL", "test-strong"):
            catalog = server._with_cloud_models({"models": []})
        names = [m.get("name") for m in catalog["models"]]
        self.assertIn("cloud:fast", names)
        self.assertIn("cloud:strong", names)


class VersionEndpointTests(unittest.TestCase):
    """Release metadata endpoint is stable and safe."""

    def test_version_shape(self):
        result = server.api_version()
        self.assertEqual(result["name"], "agent-bridge")
        self.assertEqual(result["version"], "1.0.0")
        self.assertIsInstance(result["cloud_enabled"], bool)
        self.assertIsInstance(result["auth_enabled"], bool)


class ReplyLanguageTests(unittest.TestCase):
    """Supported reply languages map to human-readable names."""

    def test_language_map(self):
        names = {
            "he": "Hebrew", "iw": "Hebrew", "ar": "Arabic", "fr": "French",
            "es": "Spanish", "de": "German", "tr": "Turkish", "ru": "Russian",
        }
        self.assertEqual(names.get("he"), "Hebrew")
        self.assertEqual(names.get("ar"), "Arabic")
        self.assertIsNone(names.get("xx"))


class LocalizedAnswerTests(unittest.TestCase):
    """RTL users get a localized marker even if the small model answers in English."""

    def test_hebrew_and_arabic_markers(self):
        self.assertTrue(server._localize_final_answer("Hello", "Hebrew").startswith("תשובה"))
        self.assertTrue(server._localize_final_answer("Hello", "Arabic").startswith("الإجابة"))

    def test_noop_path_is_localized(self):
        result = server._localize_final_answer(
            "No PC changes were made: the model did not invoke a change tool.", "Hebrew")
        self.assertIn("תשובה", result)


class ReplyLanguageNameTests(unittest.TestCase):
    def test_maps_rtl_codes(self):
        self.assertEqual(server._reply_language_name("he"), "Hebrew")
        self.assertEqual(server._reply_language_name("iw"), "Hebrew")
        self.assertEqual(server._reply_language_name("ar"), "Arabic")
        self.assertEqual(server._reply_language_name("xx"), "")


class RTLModelRoutingTests(unittest.TestCase):
    """Hebrew/Arabic chat uses the multilingual local model when available."""

    def setUp(self):
        self.available = ["qwen3.5:4b", "qwen2.5-coder:1.5b", "gemma3:4b"]

    def test_hebrew_and_arabic_route_to_gemma(self):
        for code in ("he", "iw", "ar"):
            model, fast = server.select_chat_model(
                "auto", "Explain VPN", False, [], self.available, language=code)
            self.assertEqual(model, "gemma3:4b")
            self.assertFalse(fast)

    def test_action_job_keeps_strong_model(self):
        model, fast = server.select_chat_model(
            "auto", "write a file", True, [], self.available, language="he")
        self.assertEqual(model, "qwen3.5:4b")
        self.assertFalse(fast)

    def test_image_job_keeps_strong_model(self):
        model, fast = server.select_chat_model(
            "auto", "Look at this", False, ["img"], self.available, language="ar")
        self.assertEqual(model, "qwen3.5:4b")
        self.assertFalse(fast)

    def test_fallback_when_gemma_not_installed(self):
        model, fast = server.select_chat_model(
            "auto", "Explain VPN", False, [], ["qwen3.5:4b", "qwen2.5-coder:1.5b"],
            language="he")
        self.assertEqual(model, "qwen2.5-coder:1.5b")
        self.assertTrue(fast)

    def test_english_and_explicit_model_unchanged(self):
        model, fast = server.select_chat_model(
            "auto", "Hello", False, [], self.available, language="en")
        self.assertEqual(model, "qwen2.5-coder:1.5b")
        self.assertTrue(fast)
        model, _ = server.select_chat_model(
            "llama3.2:3b", "Explain VPN", False, [], self.available, language="he")
        self.assertEqual(model, "llama3.2:3b")


class CloudNormalizerTests(unittest.TestCase):
    """Cloud payloads must satisfy strict OpenAI Chat Completions validation."""

    def test_tools_normalized(self):
        tools = server._cloud_tools([
            {"type": "function", "function": {"name": "write_file", "description": "write",
                                              "parameters": {"type": "object", "properties": {}}}},
            {"type": "function", "function": {}},
        ])
        self.assertEqual(len(tools), 1)
        self.assertEqual(tools[0]["function"]["name"], "write_file")
        self.assertEqual(tools[0]["type"], "function")

    def test_messages_normalized(self):
        messages = server._cloud_messages([
            {"role": "user", "content": "hi"},
            {"role": "assistant", "tool_calls": [
                {"id": "call_1", "function": {"name": "write_file", "arguments": {"path": "/tmp/a"}}}
            ]},
            {"role": "tool", "tool_call_id": "call_1", "content": "ok"},
        ], {"write_file"})
        self.assertEqual(messages[1]["tool_calls"][0]["function"]["arguments"], '{"path": "/tmp/a"}')
        self.assertEqual(messages[2]["tool_call_id"], "call_1")
        # Stale tool calls are dropped when the current request has no such tool.
        stale = server._cloud_messages([
            {"role": "assistant", "content": "Earlier answer", "tool_calls": [
                {"id": "call_1", "function": {"name": "write_file", "arguments": {}}}
            ]},
            {"role": "tool", "tool_call_id": "call_1", "content": "ok"},
        ], set())
        self.assertEqual(stale, [{"role": "assistant", "content": "Earlier answer"}])

if __name__ == "__main__":
    unittest.main()
