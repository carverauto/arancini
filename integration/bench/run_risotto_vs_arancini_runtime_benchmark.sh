#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_PATH="${1:-$ROOT_DIR/integration/bench/risotto-vs-arancini-runtime-report.md}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

ROUTE_COUNT="${ROUTE_COUNT:-600}"
RECONNECT_CYCLES="${RECONNECT_CYCLES:-6}"
BURST_ROUTES_PER_CYCLE="${BURST_ROUTES_PER_CYCLE:-100}"
RECONNECT_SLEEP_SECONDS="${RECONNECT_SLEEP_SECONDS:-4}"
POST_BURST_WAIT_SECONDS="${POST_BURST_WAIT_SECONDS:-15}"

mkdir -p "$(dirname "$REPORT_PATH")"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose is required" >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is unreachable" >&2
  exit 1
fi

declare -A BMP_DELTA
declare -A BMP_RATE
declare -A RX_DELTA
declare -A TX_DELTA
declare -A CPU_DELTA
declare -A CPU_PER_1K_BMP
declare -A ERR_COUNT

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
  curl -fsS "$metrics_url" | awk -v m="$metric" '$1 ~ ("^" m) {sum+=$2} END {printf "%.6f\n", sum+0}'
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

run_profile() {
  local name="$1"
  local workers="$2"
  local metrics_port="$3"

  local compose_file="$ROOT_DIR/integration/compose.yml"
  local project="ovn-bench-${name}"
  local metrics_url="http://127.0.0.1:${metrics_port}/metrics"
  local override_file="$WORK_DIR/${name}.override.yml"

  cat >"$override_file" <<EOFYML
services:
  risotto:
    command: -v --arancini-workers=${workers} --kafka-disable --curation-disable --curation-state-path=/app/state.bin
EOFYML

  local -a compose=(docker compose -f "$compose_file" -f "$override_file" -p "$project")
  echo "[$name] bringing stack up..."
  (
    cd "$ROOT_DIR"
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    RISOTTO_METRICS_PORT="$metrics_port" "${compose[@]}" up -d --build --force-recreate --renew-anon-volumes
  )

  wait_for_metrics "$metrics_url"
  wait_for_router gobgp10 "${compose[@]}"
  wait_for_router gobgp20 "${compose[@]}"

  echo "[$name] preloading routes..."
  (
    cd "$ROOT_DIR"
    load_routes gobgp10 110 "$ROUTE_COUNT" "${compose[@]}"
  ) &
  local p1=$!
  (
    cd "$ROOT_DIR"
    load_routes gobgp20 120 "$ROUTE_COUNT" "${compose[@]}"
  ) &
  local p2=$!
  wait "$p1" "$p2"
  sleep 3

  local before_bmp before_rx before_tx before_cpu
  before_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  before_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
  before_tx="$(metric_sum "$metrics_url" "risotto_tx_updates_total")"
  before_cpu="$(metric_sum "$metrics_url" "process_cpu_seconds_total")"

  local start_ts end_ts
  start_ts="$(date +%s)"
  for ((cycle=1; cycle<=RECONNECT_CYCLES; cycle++)); do
    echo "[$name] reconnect cycle $cycle/$RECONNECT_CYCLES..."
    (
      cd "$ROOT_DIR"
      "${compose[@]}" stop gobgp10 gobgp20 >/dev/null
      if ! "${compose[@]}" start gobgp10 gobgp20 >/dev/null 2>&1; then
        RISOTTO_METRICS_PORT="$metrics_port" "${compose[@]}" up -d --no-deps gobgp10 gobgp20 >/dev/null
      fi
    )
    wait_for_router gobgp10 "${compose[@]}"
    wait_for_router gobgp20 "${compose[@]}"
    (
      cd "$ROOT_DIR"
      load_routes gobgp10 $(((130 + cycle) % 256)) "$BURST_ROUTES_PER_CYCLE" "${compose[@]}"
    ) &
    local bp1=$!
    (
      cd "$ROOT_DIR"
      load_routes gobgp20 $(((140 + cycle) % 256)) "$BURST_ROUTES_PER_CYCLE" "${compose[@]}"
    ) &
    local bp2=$!
    wait "$bp1" "$bp2"
    sleep "$RECONNECT_SLEEP_SECONDS"
  done
  sleep "$POST_BURST_WAIT_SECONDS"
  end_ts="$(date +%s)"

  local after_bmp after_rx after_tx after_cpu
  after_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  after_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
  after_tx="$(metric_sum "$metrics_url" "risotto_tx_updates_total")"
  after_cpu="$(metric_sum "$metrics_url" "process_cpu_seconds_total")"

  local bmp_delta rx_delta tx_delta cpu_delta
  bmp_delta="$(awk "BEGIN{printf \"%.0f\", ${after_bmp}-${before_bmp}}")"
  rx_delta="$(awk "BEGIN{printf \"%.0f\", ${after_rx}-${before_rx}}")"
  tx_delta="$(awk "BEGIN{printf \"%.0f\", ${after_tx}-${before_tx}}")"
  cpu_delta="$(awk "BEGIN{printf \"%.6f\", ${after_cpu}-${before_cpu}}")"

  BMP_DELTA["$name"]="$bmp_delta"
  RX_DELTA["$name"]="$rx_delta"
  TX_DELTA["$name"]="$tx_delta"
  CPU_DELTA["$name"]="$cpu_delta"

  local window
  window=$((end_ts - start_ts))
  if (( window <= 0 )); then
    window=1
  fi
  BMP_RATE["$name"]="$(awk "BEGIN{printf \"%.2f\", ${bmp_delta}/${window}}")"

  if awk "BEGIN{exit !(${bmp_delta} > 0)}"; then
    CPU_PER_1K_BMP["$name"]="$(awk "BEGIN{printf \"%.4f\", (${cpu_delta}/${bmp_delta})*1000}")"
  else
    CPU_PER_1K_BMP["$name"]="n/a"
  fi

  ERR_COUNT["$name"]="$(
    cd "$ROOT_DIR"
    "${compose[@]}" logs --no-color risotto 2>&1 | grep -E -c 'failed to accept BMP connection|Error handling BMP connection|panic|thread.*panicked' || true
  )"

  (
    cd "$ROOT_DIR"
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  )
}

ARANCINI_WORKERS="${ARANCINI_WORKERS:-4}"
run_profile "risotto" "$ARANCINI_WORKERS" "18083"
run_profile "arancini" "$ARANCINI_WORKERS" "18084"

if [[ "${RX_DELTA[risotto]}" == "0" || "${RX_DELTA[arancini]}" == "0" ]]; then
  echo "invalid benchmark: rx_update delta is zero for at least one runtime profile" >&2
  echo "this workload is not exercising the update-processing fast path; adjust traffic generation" >&2
  exit 2
fi

bmp_speedup="n/a"
if awk "BEGIN{exit !(${BMP_RATE[risotto]} > 0)}"; then
  bmp_speedup="$(awk "BEGIN{printf \"%.2fx\", ${BMP_RATE[arancini]}/${BMP_RATE[risotto]}}")"
fi

cpu_eff="n/a"
if awk "BEGIN{exit !(${CPU_PER_1K_BMP[risotto]} > 0)}" && awk "BEGIN{exit !(${CPU_PER_1K_BMP[arancini]} > 0)}"; then
  cpu_eff="$(awk "BEGIN{printf \"%.2fx\", ${CPU_PER_1K_BMP[risotto]}/${CPU_PER_1K_BMP[arancini]}}")"
fi

{
  echo "# Risotto vs Arancini Runtime Benchmark Report"
  echo
  echo "- Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Host: $(uname -a)"
  echo "- Workload: $ROUTE_COUNT routes/router preload + $RECONNECT_CYCLES reconnect cycles with $BURST_ROUTES_PER_CYCLE fresh routes/router/cycle (${RECONNECT_SLEEP_SECONDS}s sleep) + ${POST_BURST_WAIT_SECONDS}s settle."
  echo "- Producer paths: disabled (--kafka-disable, NATS disabled)"
  echo "- Curation: disabled (--curation-disable)"
  echo
  echo "## Results"
  echo
  echo "| Runtime | bmp_message delta | bmp_message rate (/s) | rx_update delta | tx_update delta | process_cpu_seconds delta | cpu sec / 1k bmp | error count |"
  echo "|---|---:|---:|---:|---:|---:|---:|---:|"
  echo "| risotto | ${BMP_DELTA[risotto]} | ${BMP_RATE[risotto]} | ${RX_DELTA[risotto]} | ${TX_DELTA[risotto]} | ${CPU_DELTA[risotto]} | ${CPU_PER_1K_BMP[risotto]} | ${ERR_COUNT[risotto]} |"
  echo "| arancini | ${BMP_DELTA[arancini]} | ${BMP_RATE[arancini]} | ${RX_DELTA[arancini]} | ${TX_DELTA[arancini]} | ${CPU_DELTA[arancini]} | ${CPU_PER_1K_BMP[arancini]} | ${ERR_COUNT[arancini]} |"
  echo
  echo "## Relative"
  echo
  echo "- Arancini vs Risotto BMP throughput: **$bmp_speedup**"
  echo "- Arancini vs Risotto CPU efficiency (lower is better): **$cpu_eff**"
} >"$REPORT_PATH"

echo "Report written to: $REPORT_PATH"
