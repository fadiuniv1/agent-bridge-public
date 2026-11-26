#!/usr/bin/env bash
# Agent Bridge one-shot installer for Ubuntu.
# Run from a clone of this repository as a normal user with sudo rights:
#   chmod +x install.sh && ./install.sh
#
# Optional environment overrides:
#   BIND_IP=...        bind address (default: Tailscale IP if found, else first LAN IP, else 127.0.0.1)
#   PORT=8081          service port
#   BRIDGE_API_TOKEN=... optional shared secret required via X-Bridge-Token
#   CLOUD_BASE_URL / CLOUD_API_KEY / CLOUD_FAST_MODEL / CLOUD_STRONG_MODEL
#   INSTALL_OLLAMA=1   install Ollama if missing (default 1)
#   PULL_MODEL=...     optional model to pull after install (default: none)
set -euo pipefail

INSTALL_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT="${PORT:-8081}"
BIND_IP="${BIND_IP:-}"
INSTALL_OLLAMA="${INSTALL_OLLAMA:-1}"
PULL_MODEL="${PULL_MODEL:-}"
SERVICE_NAME="${SERVICE_NAME:-agent-bridge}"
INSTALL_SUDOERS="${INSTALL_SUDOERS:-1}"
USER_NAME="${USER:-snow}"

say() { printf '\n\033[36m[*]\033[0m %s\n' "$*"; }
warn() { printf '\n\033[33m[!]\033[0m %s\n' "$*"; }
fail() { printf '\n\033[31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

[[ "$(uname -s)" == "Linux" ]] || fail "This installer targets Ubuntu/Linux."
command -v sudo >/dev/null || fail "sudo is required."
[[ -f "$INSTALL_DIR/server.py" ]] || fail "server.py not found next to install.sh."

# 1. Locate bind address
if [[ -z "$BIND_IP" ]] && command -v tailscale >/dev/null 2>&1; then
  BIND_IP="$(tailscale ip -4 2>/dev/null | head -1 || true)"
fi
if [[ -z "$BIND_IP" ]]; then
  BIND_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
fi
BIND_IP="${BIND_IP:-127.0.0.1}"
say "Bridge will bind to ${BIND_IP}:${PORT}"

# 2. System packages
say "Installing system packages"
sudo apt-get update -qq
sudo apt-get install -y -qq python3 python3-venv python3-pip sqlite3 curl ca-certificates >/dev/null

# 3. Python environment
say "Creating Python virtual environment"
python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install -q --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install -q -r "$INSTALL_DIR/requirements.txt"

# 4. Environment file
ENV_FILE="$INSTALL_DIR/.env"
if [[ ! -f "$ENV_FILE" ]]; then
  cp "$INSTALL_DIR/.env.example" "$ENV_FILE"
  {
    echo "BIND_IP=$BIND_IP"
    echo "PORT=$PORT"
    [[ -n "${BRIDGE_API_TOKEN:-}" ]] && echo "BRIDGE_API_TOKEN=$BRIDGE_API_TOKEN"
    [[ -n "${CLOUD_BASE_URL:-}" ]] && echo "CLOUD_BASE_URL=$CLOUD_BASE_URL"
    [[ -n "${CLOUD_API_KEY:-}" ]] && echo "CLOUD_API_KEY=$CLOUD_API_KEY"
    [[ -n "${CLOUD_FAST_MODEL:-}" ]] && echo "CLOUD_FAST_MODEL=$CLOUD_FAST_MODEL"
    [[ -n "${CLOUD_STRONG_MODEL:-}" ]] && echo "CLOUD_STRONG_MODEL=$CLOUD_STRONG_MODEL"
  } >> "$ENV_FILE"
  say "Created $ENV_FILE (gitignored). Edit it to configure cloud models/token."
fi

# 5. Ollama (optional)
if [[ "$INSTALL_OLLAMA" == "1" ]] && ! command -v ollama >/dev/null 2>&1; then
  say "Installing Ollama"
  curl -fsSL https://ollama.com/install.sh | sh
else
  warn "Ollama not installed (set INSTALL_OLLAMA=1) or already present."
fi
if [[ -n "$PULL_MODEL" ]] && command -v ollama >/dev/null 2>&1; then
  say "Pulling model $PULL_MODEL (this can take several minutes)"
  ollama pull "$PULL_MODEL" || warn "Model pull failed; run 'ollama pull $PULL_MODEL' later."
fi

# 6. Sudoers helper for bounded privileged operations
if [[ "$INSTALL_SUDOERS" == "1" && -f "$INSTALL_DIR/enable_server_sudo.sh" ]]; then
  say "Installing restricted sudoers rule for $USER_NAME"
  sudo "$INSTALL_DIR/enable_server_sudo.sh" "$USER_NAME" || warn "Sudoers rule not installed; privileged tools will require confirmation."
fi

# 7. systemd service
say "Installing systemd service"
sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" >/dev/null <<EOF
[Unit]
Description=Remote AI Agent Bridge FastAPI Service ($SERVICE_NAME)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$INSTALL_DIR
EnvironmentFile=-$ENV_FILE
ExecStart=$INSTALL_DIR/venv/bin/uvicorn server:app --host $BIND_IP --port $PORT --timeout-graceful-shutdown 10
Restart=on-failure
RestartSec=5
TimeoutStopSec=180

[Install]
WantedBy=multi-user.target
EOF

# Ollama context tuning if ollama service exists
if [[ -d /etc/systemd/system/ollama.service.d ]]; then
  sudo mkdir -p /etc/systemd/system/ollama.service.d
  sudo tee /etc/systemd/system/ollama.service.d/10-agent-bridge.conf >/dev/null <<'EOF'
[Service]
Environment=OLLAMA_CONTEXT_LENGTH=16384
Environment=OLLAMA_KV_CACHE_TYPE=q8_0
Environment=OLLAMA_NUM_PARALLEL=1
EOF
fi

sudo systemctl daemon-reload
sudo systemctl enable --now "${SERVICE_NAME}.service"

# 8. Tests + health
say "Running unit tests"
"$INSTALL_DIR/venv/bin/python" -m py_compile server.py watchdog.py
"$INSTALL_DIR/venv/bin/python" -m unittest discover -s tests || fail "Unit tests failed; inspect before using the service."

say "Waiting for health"
for i in $(seq 1 30); do
  if curl -fsS --max-time 3 "http://${BIND_IP}:${PORT}/api/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS "http://${BIND_IP}:${PORT}/api/version" || fail "Service did not become healthy; check: sudo journalctl -u $SERVICE_NAME -n 100"

printf '\n\033[32m[ok]\033[0m Agent Bridge installed and enabled at boot.\n'
echo "  Health:   http://${BIND_IP}:${PORT}/api/health"
echo "  Version:  http://${BIND_IP}:${PORT}/api/version"
echo "  Config:   $ENV_FILE"
echo "  Terminal: python3 $INSTALL_DIR/../cli/agent_cli.py  (from the Android repo)"
echo "  Logs:     sudo journalctl -u $SERVICE_NAME -f"
warn "Never expose this service on the public internet. Use Tailscale and optionally BRIDGE_API_TOKEN."
