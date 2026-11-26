# Production runbook

## Health & integrity
```bash
curl -s http://<tailscale-ip>:8081/api/health
curl -s http://<tailscale-ip>:8081/api/db/integrity
systemctl status agent-bridge ollama
journalctl -u agent-bridge -n 100
```

## Backup (daily 03:00 timer, restic)
```bash
systemctl status agent-bridge-backup.timer
sudo journalctl -u agent-bridge-backup -n 50
RESTIC_REPOSITORY=/home/snow/backups/agent-bridge \
RESTIC_PASSWORD_FILE=/etc/restic/agent-bridge-password restic snapshots
```
Restore drill (into a temp directory, never over the live DB):
```bash
RESTIC_REPOSITORY=/home/snow/backups/agent-bridge \
RESTIC_PASSWORD_FILE=/etc/restic/agent-bridge-password \
restic restore latest --target /tmp/restore-test
sqlite3 /tmp/restore-test/.../sessions-*.db 'PRAGMA integrity_check;'
```

## Recovery after crash
- Running chats/jobs are marked `interrupted` at startup; queued chats are requeued.
- Chat jobs persisted with an Idempotency-Key; the Android app restores in-flight jobs
  from DataStore and reattaches on launch, reconnect and session open.
- Never auto-replay a timed-out send: side effects may already have occurred. Use the
  app's "Retry on 4B" chip, which creates a fresh key.
- Session memory (`session_state`, `session_turns`, `session_turns_archive`) survives
  no-op turns; export a tab with `GET /api/session/export?session_id=...`.

## Safe changes
1. Back up `server.py` with a timestamped `*.bak-*` copy (gitignored).
2. Edit, `py_compile`, run the full unittest suite.
3. Restart: `sudo systemctl restart agent-bridge`; verify health + integrity.

## Security
- Bind only to the Tailscale IP. Optional `BRIDGE_API_TOKEN` enables `X-Bridge-Token`
  auth on all API routes except health/integrity; the app reads it from `.env`.
- Cloud keys live only in the service environment; never in the repository.
