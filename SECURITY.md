# Security policy

Report vulnerabilities privately to the maintainer; do not post exploits publicly.

This bridge can read/write files, run shell commands and control a desktop. Bind it
only to a trusted Tailscale IP — never `0.0.0.0` — and never port-forward it publicly.
Privileged file writes/deletes, package removal and service control require `confirm=true`.

Secrets are read only from environment variables (`CLOUD_API_KEY` etc.); `.env` is
gitignored. No credentials are stored in this repository. Generated databases, logs,
backups and bytecode are also gitignored.

Before flipping this repository to public, clean the old git history (it contains a
database snapshot and logs) with `git filter-repo` in a fresh clone, or start a fresh
repository from the current tree. See README → Publishing checklist.
