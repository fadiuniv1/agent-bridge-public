#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run with sudo: sudo $0 <bridge-service-user>" >&2
  exit 1
fi

bridge_user="${1:-}"
if [[ -z "$bridge_user" ]] || ! id "$bridge_user" >/dev/null 2>&1; then
  echo "Usage: sudo $0 <existing-bridge-service-user>" >&2
  exit 1
fi

if ! command -v visudo >/dev/null 2>&1; then
  echo "visudo is unavailable; install the sudo package first." >&2
  exit 1
fi

target="/etc/sudoers.d/90-agent-bridge"
if [[ -e "$target" ]]; then
  echo "$target already exists; inspect it and merge changes manually." >&2
  exit 1
fi

temporary="$(mktemp /etc/sudoers.d/.agent-bridge.XXXXXX)"
trap 'rm -f "$temporary"' EXIT
printf '%s ALL=(ALL:ALL) NOPASSWD: ALL\n' "$bridge_user" > "$temporary"
chown root:root "$temporary"
chmod 0440 "$temporary"

if ! visudo -cf "$temporary"; then
  echo "Sudoers validation failed; nothing was installed." >&2
  exit 1
fi

mv "$temporary" "$target"
if ! visudo -c; then
  rm -f "$target"
  echo "Global sudoers validation failed; the new rule was removed." >&2
  exit 1
fi

echo "Installed unrestricted passwordless sudo for user: $bridge_user"
echo "Verify with: sudo -u $bridge_user sudo -n -l"
echo "Revoke with: sudo rm $target && sudo visudo -c"
