#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_PATH="${1:-$ROOT_DIR/openspec/changes/refactor-arancini-shared-nothing/validation/2.6-linux-tuning-report.md}"

mkdir -p "$(dirname "$REPORT_PATH")"
TMP_OUT="$(mktemp)"
trap 'rm -f "$TMP_OUT"' EXIT

SOURCE_STATUS="PASS"
HOST_STATUS="PASS"

check_source_pattern() {
  local file="$1"
  local pattern="$2"
  local desc="$3"
  if grep -nE "$pattern" "$file" > /dev/null; then
    echo "- PASS: $desc"
  else
    echo "- FAIL: $desc"
    SOURCE_STATUS="FAIL"
  fi
}

read_sysctl() {
  local key="$1"
  if command -v sysctl >/dev/null 2>&1; then
    sysctl -n "$key" 2>/dev/null && return 0
  fi
  local path="/proc/sys/${key//./\/}"
  if [[ -r "$path" ]]; then
    cat "$path"
    return 0
  fi
  return 1
}

check_min() {
  local key="$1"
  local min="$2"
  local value
  if ! value="$(read_sysctl "$key")"; then
    echo "- WARN: $key unavailable"
    HOST_STATUS="WARN"
    return
  fi
  if [[ "$value" =~ ^[0-9]+$ ]] && (( value >= min )); then
    echo "- PASS: $key=$value (>= $min)"
  else
    echo "- WARN: $key=$value (< $min)"
    HOST_STATUS="WARN"
  fi
}

check_max() {
  local key="$1"
  local max="$2"
  local value
  if ! value="$(read_sysctl "$key")"; then
    echo "- WARN: $key unavailable"
    HOST_STATUS="WARN"
    return
  fi
  if [[ "$value" =~ ^[0-9]+$ ]] && (( value <= max )); then
    echo "- PASS: $key=$value (<= $max)"
  else
    echo "- WARN: $key=$value (> $max)"
    HOST_STATUS="WARN"
  fi
}

check_triplet_third_min() {
  local key="$1"
  local min="$2"
  local value a b c
  if ! value="$(read_sysctl "$key")"; then
    echo "- WARN: $key unavailable"
    HOST_STATUS="WARN"
    return
  fi
  read -r a b c <<<"$value"
  if [[ "${c:-}" =~ ^[0-9]+$ ]] && (( c >= min )); then
    echo "- PASS: $key='$value' (third value >= $min)"
  else
    echo "- WARN: $key='$value' (third value < $min)"
    HOST_STATUS="WARN"
  fi
}

{
  echo "# 2.6 Linux Tuning and Socket Enforcement Validation"
  echo
  echo "- Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Host: $(uname -a)"
  echo
  echo "## Runtime Socket Enforcement (Source Validation)"
  echo
  check_source_pattern "$ROOT_DIR/risotto/src/main.rs" "set_reuse_port\\(true\\)" "Tokio BMP listener enables SO_REUSEPORT"
  check_source_pattern "$ROOT_DIR/risotto/src/main.rs" "listen\\(cfg\\.listener_backlog\\)" "Tokio BMP listener applies runtime-configurable backlog"
  check_source_pattern "$ROOT_DIR/risotto/src/main.rs" "set_recv_buffer_size\\(recv_buf_size\\)" "Tokio BMP path applies configurable SO_RCVBUF"
  check_source_pattern "$ROOT_DIR/risotto/src/main.rs" "set_nodelay\\(true\\)" "Tokio accepted sessions enable TCP_NODELAY"
  check_source_pattern "$ROOT_DIR/risotto/src/arancini.rs" "reuse_port\\(true\\)" "Arancini listener enables SO_REUSEPORT"
  check_source_pattern "$ROOT_DIR/risotto/src/arancini.rs" "backlog\\(cfg\\.listener_backlog\\)" "Arancini listener applies runtime-configurable backlog"
  check_source_pattern "$ROOT_DIR/risotto/src/arancini.rs" "recv_buf_size\\(recv_buf_size\\)" "Arancini listener applies configurable SO_RCVBUF"
  check_source_pattern "$ROOT_DIR/risotto/src/arancini.rs" "set_nodelay\\(true\\)" "Arancini accepted sessions enable TCP_NODELAY"
  echo
  echo "- Source enforcement status: **$SOURCE_STATUS**"
  echo
  echo "## Linux Host Tuning Checks"
  echo

  if [[ "$(uname -s)" != "Linux" ]]; then
    HOST_STATUS="SKIP"
    echo "- SKIP: Linux sysctl/fd/RSS/IRQ checks require a Linux host."
  else
    check_min "fs.file-max" 2097152
    check_min "net.core.somaxconn" 1024
    check_min "net.ipv4.tcp_max_syn_backlog" 1024
    check_min "net.core.rmem_max" 33554432
    check_min "net.core.wmem_max" 33554432
    check_triplet_third_min "net.ipv4.tcp_rmem" 33554432
    check_triplet_third_min "net.ipv4.tcp_wmem" 33554432
    check_max "net.ipv4.tcp_fin_timeout" 15
    check_min "net.core.netdev_max_backlog" 10000

    fd_soft="$(ulimit -n || true)"
    if [[ "$fd_soft" =~ ^[0-9]+$ ]] && (( fd_soft >= 100000 )); then
      echo "- PASS: ulimit -n=$fd_soft (>= 100000 recommended)"
    else
      echo "- WARN: ulimit -n=$fd_soft (< 100000 recommended for large router fan-in)"
      HOST_STATUS="WARN"
    fi

    if command -v ethtool >/dev/null 2>&1; then
      iface="$(ls /sys/class/net | grep -vE '^(lo|docker|veth|br-)' | head -n1 || true)"
      if [[ -n "$iface" ]]; then
        combined="$(ethtool -l "$iface" 2>/dev/null | awk '/Current hardware settings:/ {p=1; next} p && /Combined:/ {print $2; exit}')"
        if [[ "$combined" =~ ^[0-9]+$ ]] && (( combined > 1 )); then
          echo "- PASS: ethtool -l $iface reports Combined channels=$combined (>1)"
        else
          echo "- WARN: RSS channel count for $iface appears low or unavailable"
          HOST_STATUS="WARN"
        fi
      else
        echo "- WARN: no physical NIC detected for RSS validation"
        HOST_STATUS="WARN"
      fi
    else
      echo "- WARN: ethtool not installed; RSS/IRQ guidance check is manual"
      HOST_STATUS="WARN"
    fi
  fi

  echo
  echo "- Linux host tuning status: **$HOST_STATUS**"
  echo
  echo "## Summary"
  echo
  echo "- Socket option enforcement status: **$SOURCE_STATUS**"
  echo "- Linux tuning validation status: **$HOST_STATUS**"
  echo "- Guidance source: \`openspec/changes/refactor-arancini-shared-nothing/design.md\`"
} > "$TMP_OUT"

cp "$TMP_OUT" "$REPORT_PATH"
echo "Report written to: $REPORT_PATH"

if [[ "$SOURCE_STATUS" == "FAIL" ]]; then
  exit 1
fi
