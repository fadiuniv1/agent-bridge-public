# Releases (server)

## Standalone installer
`agent-bridge-standalone.sh` in this repo is a **single self-extracting file** (~162 KB):
it embeds `server.py`, `tests/`, `requirements.txt`, `install.sh`, docs and helpers.
On a fresh Ubuntu machine with sudo:

```bash
curl -L -o agent-bridge-install.sh <RELEASE_ASSET_URL>
chmod +x agent-bridge-install.sh
./agent-bridge-install.sh
```

Optional env overrides: `BIND_IP`, `PORT`, `BRIDGE_API_TOKEN`, `CLOUD_BASE_URL`,
`CLOUD_API_KEY`, `CLOUD_FAST_MODEL`, `CLOUD_STRONG_MODEL`, `INSTALL_OLLAMA=0`,
`PULL_MODEL`, `SERVICE_NAME`.

It installs packages, venv, Ollama (optional), the restricted sudoers rule, writes and
enables the systemd service at boot, runs all tests, then prints health URLs.

## Publishing a release (needs a GitHub token locally)
This environment has no GitHub API token, so the release itself could not be created
automatically. Create it manually from a machine with `gh` installed:

```bash
cd agent-bridge
git tag -f v1.0.0
git push origin v1.0.0
gh release create v1.0.0 agent-bridge-standalone.sh \
  --title "Agent Bridge 1.0.0 — standalone installer" \
  --notes "Single-file installer: extracts and installs the bridge, enables it at boot."
```

Or in the GitHub web UI: Releases → Draft a new release → tag `v1.0.0` → attach
`agent-bridge-standalone.sh`.

## Before publishing
Follow `CLEANUP.md` (in the Android repo) for history scrubbing. Current HEAD is clean;
old commits still contain a DB snapshot/log files.
