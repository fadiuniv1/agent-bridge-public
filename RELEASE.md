# Releases (server) — v1.0.0 published

Release page: https://github.com/fadiuniv1/agent-bridge-public/releases/tag/v1.0.0
Asset: **agent-bridge-standalone.sh** (single self-extracting installer) and
**SHA256SUMS.txt** for verification.

## Standalone installer
`agent-bridge-standalone.sh` (~162 KB) embeds `server.py`, `tests/`, `requirements.txt`,
`install.sh`, docs and helpers. On a fresh Ubuntu machine with sudo:

```bash
curl -L -o agent-bridge-install.sh \
  https://github.com/fadiuniv1/agent-bridge-public/releases/download/v1.0.0/agent-bridge-standalone.sh
chmod +x agent-bridge-install.sh
./agent-bridge-install.sh
```

Verify with `sha256sum -c SHA256SUMS.txt`.

Optional env overrides: `BIND_IP`, `PORT`, `BRIDGE_API_TOKEN`, `CLOUD_BASE_URL`,
`CLOUD_API_KEY`, `CLOUD_FAST_MODEL`, `CLOUD_STRONG_MODEL`, `INSTALL_OLLAMA=0`,
`PULL_MODEL`, `SERVICE_NAME`.

It installs packages, venv, Ollama (optional), the restricted sudoers rule, writes and
enables the systemd service at boot, runs all 81 tests, then prints health URLs.

## Optional cloud fast path
Set `CLOUD_BASE_URL=https://api.groq.com/openai/v1`, a free `CLOUD_API_KEY`,
`CLOUD_FAST_MODEL=openai/gpt-oss-20b` and `CLOUD_STRONG_MODEL=openai/gpt-oss-120b`
to expose `cloud:fast` / `cloud:strong` in the model list. Without them the bridge is
fully local via Ollama.

## Verification
- `GET /api/health`, `/api/version`, `/api/db/integrity`, `/api/cloud/status`
- `./venv/bin/python -m unittest discover -s tests` → 81 tests
- `./venv/bin/python smoke_test.py http://<tailscale-ip>:8081` → 11 checks
