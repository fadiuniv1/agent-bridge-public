# Full security / production audit — both public repos (2026-11-27)

## Current verified state

| Item | Status |
| :--- | :--- |
| `MY_local_AI_project_public` visibility | **public** (checked via GitHub API) |
| `agent-bridge-public` visibility | **public** (checked via GitHub API) |
| Old repos (`MY_local_AI_project`, `agent-bridge`) | still **private**, **not archived** |
| Fresh repo history | single initial commit + small doc/CI commits — no DB/logs/IPs |
| Secrets in published files | none: `.env.example` uses `<tailscale-ip>` placeholders; no API keys |
| Tags | `v1.0.0` on both |
| Branches | `main` only |
| GitHub releases | **none yet** |
| CI | **green** on both public repos (app `verify`, server `server tests`) |

## Full checklist (how to verify a public repo)

### 1. Visibility & repo hygiene
```bash
gh repo view fadiuniv1/MY_local_AI_project_public --json visibility,isPrivate,isArchived,defaultBranchRef
gh repo view fadiuniv1/agent-bridge-public --json visibility,isPrivate,isArchived,defaultBranchRef
```
Expected: `visibility=PUBLIC`, `isArchived=false`, branch `main`.

### 2. Clone the public tree fresh and scan
```bash
git clone https://github.com/fadiuniv1/MY_local_AI_project_public.git /tmp/app
git clone https://github.com/fadiuniv1/agent-bridge-public.git /tmp/srv

# No secrets / internal endpoints in tracked files
git -C /tmp/app grep -n -I -E '100\.67\.146\.29|gsk_|password|api[_-]?key|BEGIN (RSA|OPENSSH|PRIVATE)' -- .
git -C /tmp/srv grep -n -I -E '100\.67\.146\.29|gsk_|password|api[_-]?key|BEGIN (RSA|OPENSSH|PRIVATE)' -- .

# No generated/secret files tracked
git -C /tmp/app ls-files | grep -E '(\.env$|sessions\.db|\.log$|\.bak-|__pycache__)'
git -C /tmp/srv ls-files | grep -E '(\.env$|sessions\.db|\.log$|\.bak-|__pycache__)'
```
Both greps must return **nothing**.

### 3. Build & test from the public clone
```bash
# app
cd /tmp/app && ./gradlew :app:testDebugUnitTest --no-daemon --max-workers=1
# server
cd /tmp/srv && python -m venv venv && venv/bin/pip install -r requirements.txt
venv/bin/python -m py_compile server.py watchdog.py
venv/bin/python -m unittest discover -s tests
```
Expected: 34/34 app tests, 81/81 server tests.

### 4. GitHub settings (web UI, cannot be automated without a token)
- Secret scanning + push protection: **ON**
- Branch protection on `main`: require CI checks, no force push
- Two-factor auth on the publishing account
- Archived old repos remain private; do not delete until new repos verified

### 5. Release asset
Create release `v1.0.0` on `agent-bridge-public` and attach `agent-bridge-standalone.sh`:
```bash
gh release create v1.0.0 agent-bridge-standalone.sh --title "Agent Bridge 1.0.0"
```
No release exists yet.

---

## TODO lists

### Do now (blocking public-readiness)
- [x] Confirm both CI badges green after workflow fixes
- [ ] Create GitHub release + attach standalone installer
- [ ] Enable secret scanning / push protection
- [ ] Enable branch protection on `main` (both repos)
- [ ] Archive the two old private repos
- [ ] Confirm public clone builds/tests pass (commands above)

### Do soon (nice to have)
- [ ] Rename fresh repos to original names (after archiving old ones)
- [ ] Add screenshots to `docs/screenshots/` and embed in README
- [ ] Add `CONTRIBUTING` PR template and issue templates
- [ ] Add a `SECURITY.md` reporting email instead of generic wording

### Do NOT do now (explicitly unnecessary)
- [ ] **No Google Play / release signing** — debug APK from Android Studio is enough
- [ ] **No history rewrite on old repos** — keep them private/archived instead
- [ ] **No cloud key rotation** — key lives in a root-only systemd drop-in, never in git
- [ ] **No dependency upgrades** — current versions are tested and working
- [ ] **No new features** — product is feature-complete and validated

## Files confirmed unnecessary and already removed
- `r.md` (stale plain-text README copy) — deleted from both old and fresh repos
- `.idea/` and `.kotlin/` metadata — untracked/ignored
- `sessions.db`, logs, backups, `__pycache__` — untracked/ignored in server repo

## Files safe to keep (do not delete)
`README.md`, `LICENSE`, `SECURITY.md`, `CONTRIBUTING.md`, `CHANGELOG.md`, `NEEDS.md`,
`PUBLISHING.md`, `CLEANUP.md`, `PUBLIC_RELEASE.md`, `SECURITY_AUDIT.md`, CI workflows,
all source/tests/resources, `cli/`, `docs/`, `install.sh`, `requirements.txt`,
`agent-bridge-standalone.sh`, `RUNBOOK.md`, `RELEASE.md`, `INTEGRATIONS.md`.
