#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROFILE_SRC="$ROOT_DIR/integration/bench/arancini-linux-sysctl.conf"
PROFILE_DEST="${ARANCINI_SYSCTL_DEST:-/etc/sysctl.d/99-arancini.conf}"
REPORT_PATH="${1:-$ROOT_DIR/openspec/changes/refactor-arancini-shared-nothing/validation/2.6-linux-tuning-report.md}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "This script must run on Linux."
  exit 1
fi

if [[ ! -f "$PROFILE_SRC" ]]; then
  echo "Missing profile source: $PROFILE_SRC"
  exit 1
fi

if [[ "${EUID:-$(id -u)}" -eq 0 ]]; then
  SUDO=""
elif command -v sudo >/dev/null 2>&1; then
  SUDO="sudo"
else
  echo "Root privileges are required (run as root or install sudo)."
  exit 1
fi

echo "Installing sysctl profile to $PROFILE_DEST"
$SUDO install -D -m 0644 "$PROFILE_SRC" "$PROFILE_DEST"

echo "Applying kernel tuning from $PROFILE_DEST"
$SUDO sysctl --load "$PROFILE_DEST" >/dev/null

echo
echo "Current tuning values:"
for key in \
  fs.file-max \
  net.core.somaxconn \
  net.ipv4.tcp_max_syn_backlog \
  net.core.rmem_max \
  net.core.wmem_max \
  net.ipv4.tcp_rmem \
  net.ipv4.tcp_wmem \
  net.ipv4.tcp_fin_timeout \
  net.core.netdev_max_backlog
do
  printf "  %s = %s\n" "$key" "$(sysctl -n "$key" 2>/dev/null || echo unavailable)"
done

echo
echo "Running 2.6 validation report generator..."
bash "$ROOT_DIR/integration/bench/run_2_6_linux_tuning_validation.sh" "$REPORT_PATH"

echo
echo "2.6 report updated at: $REPORT_PATH"
echo "Note: file descriptor limits are process/service-level; ensure LimitNOFILE or PAM limits are set to >= 100000."
echo "Note: RSS/IRQ affinity remains host/NIC-specific and may still show WARN if queue/channel topology is constrained."
