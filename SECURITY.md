# Security policy

## Reporting
**Please do not open a public issue for a vulnerability.**

Use GitHub private vulnerability reporting instead:
https://github.com/fadiuniv1/agent-bridge-public/security/advisories/new

Include reproduction steps, affected versions and the impact. You will receive a
private channel to discuss the report and coordinate a fix before disclosure.

## Threat model
This bridge can read/write files, run shell commands and control a desktop on the
host. Bind it only to a trusted Tailscale IP — never `0.0.0.0` — and never
port-forward it to the public internet. Privileged file writes/deletes, package
removal and service control require `confirm=true`.

Optional shared-secret auth is available via `BRIDGE_API_TOKEN`: clients must send
`X-Bridge-Token`. Health and integrity endpoints remain unauthenticated for monitoring.

## Secrets and generated files
Secrets are read only from environment variables (`CLOUD_API_KEY`, `BRIDGE_API_TOKEN`,
etc.). `.env`, generated databases, logs, backups and bytecode are gitignored, and no
credentials are stored in this repository. Cloud keys belong in a root-only systemd
drop-in (`sudo systemctl edit agent-bridge`), never in tracked files.

## Scope
The standalone installer and this runtime are original MIT-licensed work. OpenCode
(MIT), Codex CLI (Apache-2.0) and Claude Code (proprietary) are separate products;
this project only interoperates through documented public interfaces.
