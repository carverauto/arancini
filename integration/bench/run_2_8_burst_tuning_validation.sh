#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_PATH="${1:-$ROOT_DIR/openspec/changes/refactor-arancini-shared-nothing/validation/2.8-burst-socket-tuning-report.md}"

COMPOSE_BASE=(docker compose -f "$ROOT_DIR/integration/compose.yml" -p arancini28)
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
METRICS_URL="${ARANCINI_2_8_METRICS_URL:-http://127.0.0.1:18080/metrics}"
export RISOTTO_METRICS_PORT="${ARANCINI_2_8_METRICS_PORT:-18080}"

mkdir -p "$(dirname "$REPORT_PATH")"

declare -A DELTA_BMP
declare -A DELTA_RX_UPDATES
declare -A ACCEPT_ERRORS
declare -A BACKLOG
declare -A RCVBUF

log() {
  printf '[%s] %s\n' "$(date -u +"%H:%M:%S")" "$*"
}

write_skip_report() {
  local reason="$1"
  {
    echo "# 2.8 Burst Backlog/SO_RCVBUF Validation Report"
    echo
    echo "- Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "- Host: $(uname -a)"
    echo
    echo "## Summary"
    echo
    echo "- 2.8 backlog/SO_RCVBUF burst validation: **SKIP**"
    echo "- Reason: $reason"
    echo "- Validation script: \`integration/bench/run_2_8_burst_tuning_validation.sh\`"
  } >"$REPORT_PATH"
  echo "Report written to: $REPORT_PATH"
  echo "Overall 2.8 status: SKIP"
}

wait_for_metrics() {
  local tries=0
  while (( tries < 90 )); do
    if curl -fsS "$METRICS_URL" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    tries=$((tries + 1))
  done
  return 1
}

wait_for_router() {
  local service="$1"
  local tries=0
  while (( tries < 60 )); do
    if "${COMPOSE_BASE[@]}" exec -T "$service" gobgp neighbor >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    tries=$((tries + 1))
  done
  return 1
}

bmp_metric_sum() {
  curl -fsS "$METRICS_URL" \
    | awk '/^risotto_bmp_messages_total/ {sum+=$2} END {printf "%.0f\n", sum+0}'
}

rx_updates_metric_sum() {
  curl -fsS "$METRICS_URL" \
    | awk '/^risotto_rx_updates_total/ {sum+=$2} END {printf "%.0f\n", sum+0}'
}

load_routes() {
  local service="$1"
  local prefix_b="$2"
  local count="$3"
  local i a b prefix
  for ((i=0; i<count; i++)); do
    a=$((i / 256))
    b=$((i % 256))
    prefix="10.${prefix_b}.${a}.${b}/32"
    "${COMPOSE_BASE[@]}" exec -T "$service" gobgp global rib add "$prefix" -a ipv4 >/dev/null 2>&1 || true
  done
}

write_override_file() {
  local path="$1"
  local backlog="$2"
  local recvbuf="$3"
cat >"$path" <<EOF
services:
  risotto:
    command: -v --kafka-brokers=10.0.0.100:9092 --curation-state-path=/app/state.bin --bmp-listener-backlog=$backlog --bmp-socket-recv-buffer-bytes=$recvbuf
EOF
}

run_trial() {
  local name="$1"
  local backlog="$2"
  local recvbuf="$3"
  local route_count="$4"
  local prefix_a="$5"
  local prefix_b="$6"
  local override_file="$WORK_DIR/$name.override.yml"

  BACKLOG["$name"]="$backlog"
  RCVBUF["$name"]="$recvbuf"

  write_override_file "$override_file" "$backlog" "$recvbuf"
  local compose=("${COMPOSE_BASE[@]}" -f "$override_file")

  echo "[$name] starting integration stack..."
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  log "$name: docker compose up (this may build the risotto image on first run)..."
  "${compose[@]}" up -d --build --force-recreate --renew-anon-volumes

  echo "[$name] waiting for metrics and GoBGP readiness..."
  wait_for_metrics
  wait_for_router gobgp10
  wait_for_router gobgp20

  echo "[$name] preloading routes: $route_count per GoBGP instance..."
  load_routes gobgp10 "$prefix_a" "$route_count" &
  p1=$!
  load_routes gobgp20 "$prefix_b" "$route_count" &
  p2=$!
  wait "$p1" "$p2"

  sleep 3
  local before_bmp before_rx
  before_bmp="$(bmp_metric_sum)"
  before_rx="$(rx_updates_metric_sum)"

  echo "[$name] forcing reconnect burst to trigger table-dump replay..."
  for ((cycle=1; cycle<=RECONNECT_CYCLES; cycle++)); do
    echo "[$name] reconnect cycle $cycle/$RECONNECT_CYCLES..."
    "${compose[@]}" stop gobgp10 gobgp20 >/dev/null
    "${compose[@]}" up -d --no-deps gobgp10 gobgp20 >/dev/null
    sleep "$RECONNECT_SLEEP_SECONDS"
  done

  sleep 20
  local after_bmp after_rx
  after_bmp="$(bmp_metric_sum)"
  after_rx="$(rx_updates_metric_sum)"
  local delta_bmp delta_rx
  delta_bmp=$((after_bmp - before_bmp))
  delta_rx=$((after_rx - before_rx))

  local err_count
  err_count="$("${compose[@]}" logs --no-color risotto 2>&1 \
    | grep -E -c 'failed to accept BMP connection|Error handling BMP connection|failed to set SO_RCVBUF' || true)"

  DELTA_BMP["$name"]="$delta_bmp"
  DELTA_RX_UPDATES["$name"]="$delta_rx"
  ACCEPT_ERRORS["$name"]="$err_count"

  echo "[$name] result: delta_bmp_messages=$delta_bmp delta_rx_updates=$delta_rx accept_or_socket_errors=$err_count"
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}

if ! command -v docker >/dev/null 2>&1; then
  write_skip_report "docker CLI is not installed on host."
  exit 0
fi
if ! docker compose version >/dev/null 2>&1; then
  write_skip_report "docker compose is not installed on host."
  exit 0
fi
if ! command -v curl >/dev/null 2>&1; then
  write_skip_report "curl is not installed on host."
  exit 0
fi
if ! docker info >/dev/null 2>&1; then
  write_skip_report "docker daemon is unreachable (is Docker running and is your user allowed on /var/run/docker.sock?)."
  exit 0
fi

TRIAL_LOW="baseline-low"
TRIAL_HIGH="tuned-high"
ROUTE_COUNT="${ARANCINI_2_8_ROUTE_COUNT:-600}"
RECONNECT_CYCLES="${ARANCINI_2_8_RECONNECT_CYCLES:-6}"
RECONNECT_SLEEP_SECONDS="${ARANCINI_2_8_RECONNECT_SLEEP_SECONDS:-4}"
MIN_BURST_SIGNAL="${ARANCINI_2_8_MIN_BURST_SIGNAL:-20}"

run_trial "$TRIAL_LOW" 64 262144 "$ROUTE_COUNT" 110 120
run_trial "$TRIAL_HIGH" 4096 33554432 "$ROUTE_COUNT" 130 140

LOW_DELTA="${DELTA_BMP[$TRIAL_LOW]}"
HIGH_DELTA="${DELTA_BMP[$TRIAL_HIGH]}"
LOW_RX_DELTA="${DELTA_RX_UPDATES[$TRIAL_LOW]}"
HIGH_RX_DELTA="${DELTA_RX_UPDATES[$TRIAL_HIGH]}"
LOW_ERR="${ACCEPT_ERRORS[$TRIAL_LOW]}"
HIGH_ERR="${ACCEPT_ERRORS[$TRIAL_HIGH]}"

SCALE_STATUS="PASS"
ERR_STATUS="PASS"
SIGNAL_STATUS="PASS"
OVERALL_STATUS="PASS"

SIGNAL_METRIC="BMP message delta"
LOW_SIGNAL="$LOW_DELTA"
HIGH_SIGNAL="$HIGH_DELTA"
if (( LOW_RX_DELTA > 0 || HIGH_RX_DELTA > 0 )); then
  SIGNAL_METRIC="rx update delta"
  LOW_SIGNAL="$LOW_RX_DELTA"
  HIGH_SIGNAL="$HIGH_RX_DELTA"
fi

if (( LOW_SIGNAL <= 0 || HIGH_SIGNAL <= 0 )); then
  SCALE_STATUS="WARN"
fi
if (( HIGH_SIGNAL * 100 < LOW_SIGNAL * 95 )); then
  SCALE_STATUS="WARN"
fi
if (( HIGH_ERR > LOW_ERR )); then
  ERR_STATUS="WARN"
fi
if (( LOW_SIGNAL < MIN_BURST_SIGNAL || HIGH_SIGNAL < MIN_BURST_SIGNAL )); then
  SIGNAL_STATUS="WARN"
fi
if [[ "$SCALE_STATUS" != "PASS" || "$ERR_STATUS" != "PASS" || "$SIGNAL_STATUS" != "PASS" ]]; then
  OVERALL_STATUS="WARN"
fi

{
  echo "# 2.8 Burst Backlog/SO_RCVBUF Validation Report"
  echo
  echo "- Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Host: $(uname -a)"
  echo "- Workload: GoBGP route preload ($ROUTE_COUNT per router), then $RECONNECT_CYCLES reconnect cycles (sleep ${RECONNECT_SLEEP_SECONDS}s) to force replay bursts."
  echo
  echo "## Trial Results"
  echo
  echo "| Trial | backlog | SO_RCVBUF bytes | BMP message delta | rx update delta | accept/socket error log count |"
  echo "|---|---:|---:|---:|---:|---:|"
  echo "| $TRIAL_LOW | ${BACKLOG[$TRIAL_LOW]} | ${RCVBUF[$TRIAL_LOW]} | $LOW_DELTA | $LOW_RX_DELTA | $LOW_ERR |"
  echo "| $TRIAL_HIGH | ${BACKLOG[$TRIAL_HIGH]} | ${RCVBUF[$TRIAL_HIGH]} | $HIGH_DELTA | $HIGH_RX_DELTA | $HIGH_ERR |"
  echo
  echo "## Checks"
  echo
  echo "- Burst ingest throughput check: **$SCALE_STATUS** (using $SIGNAL_METRIC; tuned delta should be >= 95% of baseline)"
  echo "- Burst signal strength check: **$SIGNAL_STATUS** (both trial deltas should be >= $MIN_BURST_SIGNAL)"
  echo "- Accept/socket error trend check: **$ERR_STATUS** (tuned error count should be <= baseline)"
  echo
  echo "## Summary"
  echo
  echo "- 2.8 backlog/SO_RCVBUF burst validation: **$OVERALL_STATUS**"
  echo "- Validation script: \`integration/bench/run_2_8_burst_tuning_validation.sh\`"
} >"$REPORT_PATH"

echo "Report written to: $REPORT_PATH"
echo "Overall 2.8 status: $OVERALL_STATUS"
