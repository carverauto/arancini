#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OLD_ROOT="${ROOT_DIR}/old-risotto/risotto"
REPORT_PATH="${1:-$ROOT_DIR/integration/bench/old-vs-new-benchmark-report.md}"

ROUTE_COUNT="${ROUTE_COUNT:-600}"
RECONNECT_CYCLES="${RECONNECT_CYCLES:-6}"
RECONNECT_SLEEP_SECONDS="${RECONNECT_SLEEP_SECONDS:-4}"
POST_BURST_WAIT_SECONDS="${POST_BURST_WAIT_SECONDS:-15}"

mkdir -p "$(dirname "$REPORT_PATH")"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

if [[ ! -d "$OLD_ROOT" ]]; then
  echo "missing old repo at $OLD_ROOT" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required" >&2
  exit 1
fi
if ! command -v curl >/dev/null 2>&1; then
  echo "curl is required" >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is unreachable" >&2
  exit 1
fi

declare -A RX_DELTA
declare -A BMP_DELTA
declare -A ERR_COUNT
declare -A WINDOW_SECONDS
declare -A SIGNAL_METRIC
declare -A SIGNAL_DELTA
declare -A SIGNAL_RATE

wait_for_metrics() {
  local metrics_url="$1"
  local tries=0
  while (( tries < 120 )); do
    if curl -fsS "$metrics_url" >/dev/null 2>&1; then
      return 0
    fi
    tries=$((tries + 1))
    sleep 1
  done
  return 1
}

wait_for_router() {
  local service="$1"
  shift
  local -a compose=("$@")
  local tries=0
  while (( tries < 90 )); do
    if "${compose[@]}" exec -T "$service" gobgp neighbor >/dev/null 2>&1; then
      return 0
    fi
    tries=$((tries + 1))
    sleep 1
  done
  return 1
}

metric_sum() {
  local metrics_url="$1"
  local metric="$2"
  curl -fsS "$metrics_url" | awk -v m="$metric" '$1 ~ ("^" m) {sum+=$2} END {printf "%.0f\n", sum+0}'
}

load_routes() {
  local service="$1"
  local prefix_b="$2"
  local count="$3"
  shift 3
  local -a compose=("$@")
  local i a b prefix
  for ((i=0; i<count; i++)); do
    a=$((i / 256))
    b=$((i % 256))
    prefix="10.${prefix_b}.${a}.${b}/32"
    "${compose[@]}" exec -T "$service" gobgp global rib add "$prefix" -a ipv4 >/dev/null 2>&1 || true
  done
}

write_override() {
  local file="$1"
  local command="$2"
  cat >"$file" <<EOF
services:
  risotto:
    command: $command
EOF
}

run_profile() {
  local name="$1"
  local repo_root="$2"
  local compose_file="$3"
  local metrics_port="$4"
  local command_override="$5"
  local route_prefix_a="$6"
  local route_prefix_b="$7"

  local project="ovn-${name}"
  local metrics_url="http://127.0.0.1:${metrics_port}/metrics"
  local override_file="$WORK_DIR/${name}.override.yml"

  write_override "$override_file" "$command_override"
  local -a compose=(docker compose -f "$compose_file" -f "$override_file" -p "$project")

  echo "[$name] bringing stack up..."
  "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  (
    cd "$repo_root"
    RISOTTO_METRICS_PORT="$metrics_port" "${compose[@]}" up -d --build --force-recreate --renew-anon-volumes
  )

  echo "[$name] waiting for services..."
  wait_for_metrics "$metrics_url"
  wait_for_router gobgp10 "${compose[@]}"
  wait_for_router gobgp20 "${compose[@]}"

  echo "[$name] preloading routes..."
  (
    cd "$repo_root"
    load_routes gobgp10 "$route_prefix_a" "$ROUTE_COUNT" "${compose[@]}"
  ) &
  local p1=$!
  (
    cd "$repo_root"
    load_routes gobgp20 "$route_prefix_b" "$ROUTE_COUNT" "${compose[@]}"
  ) &
  local p2=$!
  wait "$p1" "$p2"
  sleep 3

  local before_bmp before_rx
  before_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  before_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"

  local start_ts end_ts
  start_ts="$(date +%s)"
  echo "[$name] running reconnect burst cycles..."
  for ((cycle=1; cycle<=RECONNECT_CYCLES; cycle++)); do
    echo "[$name] reconnect cycle $cycle/$RECONNECT_CYCLES..."
    (
      cd "$repo_root"
      "${compose[@]}" stop gobgp10 gobgp20 >/dev/null
      RISOTTO_METRICS_PORT="$metrics_port" "${compose[@]}" up -d --no-deps gobgp10 gobgp20 >/dev/null
    )
    sleep "$RECONNECT_SLEEP_SECONDS"
  done
  sleep "$POST_BURST_WAIT_SECONDS"
  end_ts="$(date +%s)"

  local after_bmp after_rx
  after_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  after_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"

  local delta_bmp delta_rx window
  delta_bmp=$((after_bmp - before_bmp))
  delta_rx=$((after_rx - before_rx))
  window=$((end_ts - start_ts))
  if (( window <= 0 )); then
    window=1
  fi

  local signal_name="bmp_messages"
  local signal_delta="$delta_bmp"
  if (( delta_rx > 0 )); then
    signal_name="rx_updates"
    signal_delta="$delta_rx"
  fi
  local signal_rate
  signal_rate="$(awk "BEGIN{printf \"%.2f\", ${signal_delta}/${window}}")"

  local errors
  (
    cd "$repo_root"
    errors="$("${compose[@]}" logs --no-color risotto 2>&1 | grep -E -c 'failed to accept BMP connection|Error handling BMP connection|panic|thread.*panicked' || true)"
    echo "[$name] done: signal=${signal_name} delta=${signal_delta} rate=${signal_rate}/s errors=${errors}"
  )
  errors="$(
    cd "$repo_root"
    "${compose[@]}" logs --no-color risotto 2>&1 | grep -E -c 'failed to accept BMP connection|Error handling BMP connection|panic|thread.*panicked' || true
  )"

  RX_DELTA["$name"]="$delta_rx"
  BMP_DELTA["$name"]="$delta_bmp"
  ERR_COUNT["$name"]="$errors"
  WINDOW_SECONDS["$name"]="$window"
  SIGNAL_METRIC["$name"]="$signal_name"
  SIGNAL_DELTA["$name"]="$signal_delta"
  SIGNAL_RATE["$name"]="$signal_rate"

  (
    cd "$repo_root"
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  )
}

run_profile \
  "old-risotto" \
  "$OLD_ROOT" \
  "$OLD_ROOT/integration/compose.yml" \
  "18081" \
  "-v --kafka-brokers=10.0.0.100:9092 --curation-state-path=/app/state.bin" \
  "110" \
  "120"

run_profile \
  "new-risotto" \
  "$ROOT_DIR" \
  "$ROOT_DIR/integration/compose.yml" \
  "18082" \
  "-v --runtime-mode=risotto --kafka-brokers=10.0.0.100:9092 --curation-state-path=/app/state.bin" \
  "130" \
  "140"

run_profile \
  "new-arancini" \
  "$ROOT_DIR" \
  "$ROOT_DIR/integration/compose.yml" \
  "18083" \
  "-v --runtime-mode=arancini --arancini-workers=4 --kafka-brokers=10.0.0.100:9092 --curation-state-path=/app/state.bin" \
  "150" \
  "160"

old_rate="${SIGNAL_RATE[old-risotto]}"
new_risotto_rate="${SIGNAL_RATE[new-risotto]}"
new_arancini_rate="${SIGNAL_RATE[new-arancini]}"

speedup_new_risotto="$(awk "BEGIN{if (${old_rate} > 0) printf \"%.2fx\", ${new_risotto_rate}/${old_rate}; else print \"n/a\"}")"
speedup_new_arancini="$(awk "BEGIN{if (${old_rate} > 0) printf \"%.2fx\", ${new_arancini_rate}/${old_rate}; else print \"n/a\"}")"

{
  echo "# Old vs New Benchmark Report"
  echo
  echo "- Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Host: $(uname -a)"
  echo "- Workload: $ROUTE_COUNT routes per router preload + $RECONNECT_CYCLES reconnect cycles (${RECONNECT_SLEEP_SECONDS}s sleep) + ${POST_BURST_WAIT_SECONDS}s settle."
  echo
  echo "## Results"
  echo
  echo "| Profile | signal metric | signal delta | signal rate (/s) | rx_update delta | bmp_message delta | error count |"
  echo "|---|---|---:|---:|---:|---:|---:|"
  echo "| old-risotto | ${SIGNAL_METRIC[old-risotto]} | ${SIGNAL_DELTA[old-risotto]} | ${SIGNAL_RATE[old-risotto]} | ${RX_DELTA[old-risotto]} | ${BMP_DELTA[old-risotto]} | ${ERR_COUNT[old-risotto]} |"
  echo "| new-risotto | ${SIGNAL_METRIC[new-risotto]} | ${SIGNAL_DELTA[new-risotto]} | ${SIGNAL_RATE[new-risotto]} | ${RX_DELTA[new-risotto]} | ${BMP_DELTA[new-risotto]} | ${ERR_COUNT[new-risotto]} |"
  echo "| new-arancini | ${SIGNAL_METRIC[new-arancini]} | ${SIGNAL_DELTA[new-arancini]} | ${SIGNAL_RATE[new-arancini]} | ${RX_DELTA[new-arancini]} | ${BMP_DELTA[new-arancini]} | ${ERR_COUNT[new-arancini]} |"
  echo
  echo "## Relative Throughput"
  echo
  echo "- new-risotto vs old-risotto: **$speedup_new_risotto**"
  echo "- new-arancini vs old-risotto: **$speedup_new_arancini**"
} >"$REPORT_PATH"

echo "Report written to: $REPORT_PATH"
