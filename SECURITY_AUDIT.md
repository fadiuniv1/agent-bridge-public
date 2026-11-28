# Security / production audit — both public repos (final, 2026-11-27)

## Verified final state

| Item | Status |
| :--- | :--- |
| `MY_local_AI_project_public` visibility | **public** (GitHub API) |
| `agent-bridge-public` visibility | **public** (GitHub API) |
| Old repos (`MY_local_AI_project`, `agent-bridge`) | **private and archived** (GitHub API) |
| Fresh repo history | clean single-commit seed + small doc/CI/template commits; no DB, logs or internal IPs |
| Secrets in tracked files | none — `.env.example` uses placeholders; keys live only in a root-only systemd drop-in |
| Tags / releases | `v1.0.0` on both, with release notes and `SHA256SUMS.txt` |
| App release asset | `MY-local-AI-1.0.0-signed.apk` (self-signed local key; signer SHA-256 `c0e3a678…d83ca`) |
| Server release asset | `agent-bridge-standalone.sh` (single-file installer) |
| Secret scanning + push protection | **enabled** on both (GitHub API) |
| Branch protection on `main` | **enabled** on both (GitHub API) |
| Private vulnerability reporting | **enabled** on both (GitHub API) |
| Issue/PR templates + `SECURITY.md` reporting link | **added** on both |
| CI | green — app `verify`, server `server tests` |
| Tests | app 34/34; server 91/91; smoke 11/11 |
| Local Android signing | optional gitignored `keystore.properties`; public clones build unsigned |
| Signing keystore | outside git (`Temp\opencode\my-local-ai.jks`) — back it up if long-term continuity matters |

## Security checklist (all done)

- [x] Confirm CI badges green on both public repos
- [x] Create `v1.0.0` releases with full notes and checksum files
- [x] Enable secret scanning and push protection (GitHub API)
- [x] Enable branch protection on `main` (GitHub API)
- [x] Enable private vulnerability reporting (GitHub API)
- [x] Archive the two old private repos (GitHub API confirmed `archived: true`)
- [x] Add issue templates, PR template and a real reporting channel
- [x] Remove credentials, real Tailscale addresses, DB/logs/backups from tracked files

## Still open (tracked as GitHub issues)

- App #1 — screenshots in `docs/screenshots/` and README embeds (needs user-provided files)
- App #2 — clean-clone build/test verification of the public app tree
- App #4 — fully fluent Hebrew/Arabic without a cloud key
- Server #1 — clean-clone install/test/smoke verification of the public server tree
- Server #3 — GNOME 50 screenshot D-Bus permission (environment, not code)
- Server #4 — website-generation latency (~968 s on this host)
- Server #5 — full/durable delegation remains experimental

## Optional / explicitly not doing

- Rename the public repos to the original names (old names are held by archived repos)
- No Google Play signing or release keystore in git
- No history rewrite on the archived old repos
- No cloud key rotation — the key is in a root-only systemd drop-in, never in git
- No dependency upgrades — current versions are tested and working
- Confirm 2FA on the publishing GitHub account in the web UI (account-level, not repo-level)

## Re-verification commands

```bash
# visibility and security settings
gh api repos/fadiuniv1/MY_local_AI_project_public --jq '{visibility,archived,security_and_analysis}'
gh api repos/fadiuniv1/agent-bridge-public --jq '{visibility,archived,security_and_analysis}'

# clean trees
git -C /tmp/app grep -n -I -E '100\.67\.146\.29|gsk_|password|api[_-]?key|BEGIN (RSA|OPENSSH|PRIVATE)' -- .
git -C /tmp/srv grep -n -I -E '100\.67\.146\.29|gsk_|password|api[_-]?key|BEGIN (RSA|OPENSSH|PRIVATE)' -- .

# checksums
curl -L -O https://github.com/fadiuniv1/MY_local_AI_project_public/releases/download/v1.0.0/SHA256SUMS.txt
curl -L -O https://github.com/fadiuniv1/agent-bridge-public/releases/download/v1.0.0/SHA256SUMS.txt
sha256sum -c SHA256SUMS.txt
```

Never expose the bridge on the public internet — bind it to a Tailscale IP only.
