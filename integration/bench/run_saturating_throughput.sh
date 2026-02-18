#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_PATH="${1:-$ROOT_DIR/integration/bench/risotto-vs-arancini-saturating-report.md}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

ROUTE_COUNT="${ROUTE_COUNT:-20000}"
ARANCINI_WORKERS="${ARANCINI_WORKERS:-4}"
TARGET_RX_DELTA="${TARGET_RX_DELTA:-$ROUTE_COUNT}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-300}"

mkdir -p "$(dirname "$REPORT_PATH")"

declare -A RX_DELTA
declare -A BMP_DELTA
declare -A CPU_DELTA
declare -A ELAPSED
declare -A THROUGHPUT
declare -A ERR_COUNT

metric_sum() {
  local metrics_url="$1"
  local metric="$2"
  curl -fsS "$metrics_url" | awk -v m="$metric" '$1 ~ ("^" m) {sum+=$2} END {printf "%.6f\n", sum+0}'
}

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

wait_for_bgp_established() {
  local service="$1"
  local neighbor="$2"
  shift 2
  local -a compose=("$@")
  local tries=0
  while (( tries < 120 )); do
    if "${compose[@]}" exec -T "$service" gobgp neighbor "$neighbor" 2>/dev/null | rg -qi 'SESSION_STATE_ESTABLISHED|BGP state = established|BGP_FSM_ESTABLISHED'; then
      return 0
    fi
    tries=$((tries + 1))
    sleep 1
  done
  return 1
}

inject_routes_from_peer() {
  local count="$1"
  local octet="$2"
  shift 2
  local -a compose=("$@")
  local i a b prefix
  for ((i=0; i<count; i++)); do
    a=$((i / 256))
    b=$((i % 256))
    prefix="198.${octet}.${a}.${b}/32"
    "${compose[@]}" exec -T gobgp50 gobgp global rib add "$prefix" -a ipv4 >/dev/null 2>&1 || true
  done
}

run_trial() {
  local name="$1"
  local runtime_mode="$2"
  local metrics_port="$3"

  local project="saturn-${name}"
  local metrics_url="http://127.0.0.1:${metrics_port}/metrics"
  local override_file="$WORK_DIR/${name}.override.yml"

  cat >"$override_file" <<EOFYML
services:
  gobgp10:
    volumes:
      - ./config/gobgp/bench-gobgp10.toml:/etc/gobgp/gobgp.toml:ro
  gobgp20:
    volumes:
      - ./config/gobgp/bench-gobgp20.toml:/etc/gobgp/gobgp.toml:ro
  gobgp50:
    image: jauderho/gobgp:latest
    command: gobgpd --config-file /etc/gobgp/gobgp.toml
    platform: linux/amd64
    volumes:
      - ./config/gobgp/bench-gobgp50.toml:/etc/gobgp/gobgp.toml:ro
    networks:
      integration:
        ipv4_address: 10.0.0.50
    depends_on:
      - risotto
  risotto:
    command: -v --runtime-mode=${runtime_mode} --arancini-workers=${ARANCINI_WORKERS} --kafka-disable --curation-disable --curation-state-path=/app/state.bin
EOFYML

  local -a compose=(docker compose -f "$ROOT_DIR/integration/compose.yml" -f "$override_file" -p "$project")

  echo "[$name] starting stack..."
  (
    cd "$ROOT_DIR"
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    "${compose[@]}" build risotto >/dev/null
    RISOTTO_METRICS_PORT="$metrics_port" "${compose[@]}" up -d --no-build --remove-orphans --scale bird30=0 --scale bird40=0
  )

  wait_for_metrics "$metrics_url"
  wait_for_bgp_established gobgp50 10.0.0.10 "${compose[@]}"
  wait_for_bgp_established gobgp50 10.0.0.20 "${compose[@]}"

  local before_rx before_bmp before_cpu
  before_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
  before_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  before_cpu="$(metric_sum "$metrics_url" "process_cpu_seconds_total")"

  local start_ts end_ts current_rx
  start_ts="$(date +%s.%N)"

  echo "[$name] injecting ${ROUTE_COUNT} peer routes..."
  inject_routes_from_peer "$ROUTE_COUNT" $((100 + (RANDOM % 100))) "${compose[@]}" &
  local inject_pid=$!

  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while true; do
    current_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
    local delta
    delta="$(awk "BEGIN{printf \"%.0f\", ${current_rx}-${before_rx}}")"
    if (( delta >= TARGET_RX_DELTA )); then
      break
    fi
    if (( $(date +%s) >= deadline )); then
      echo "[$name] timeout waiting for rx delta >= ${TARGET_RX_DELTA}; got ${delta}" >&2
      break
    fi
    sleep 0.2
  done

  wait "$inject_pid" || true
  end_ts="$(date +%s.%N)"

  local after_rx after_bmp after_cpu
  after_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
  after_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  after_cpu="$(metric_sum "$metrics_url" "process_cpu_seconds_total")"

  RX_DELTA["$name"]="$(awk "BEGIN{printf \"%.0f\", ${after_rx}-${before_rx}}")"
  BMP_DELTA["$name"]="$(awk "BEGIN{printf \"%.0f\", ${after_bmp}-${before_bmp}}")"
  CPU_DELTA["$name"]="$(awk "BEGIN{printf \"%.6f\", ${after_cpu}-${before_cpu}}")"
  ELAPSED["$name"]="$(awk "BEGIN{printf \"%.3f\", ${end_ts}-${start_ts}}")"

  if awk "BEGIN{exit !(${ELAPSED[$name]} > 0)}"; then
    THROUGHPUT["$name"]="$(awk "BEGIN{printf \"%.2f\", ${RX_DELTA[$name]}/${ELAPSED[$name]}}")"
  else
    THROUGHPUT["$name"]="0.00"
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

run_trial "risotto" "risotto" "18091"
run_trial "arancini" "arancini" "18092"

if [[ "${RX_DELTA[risotto]}" == "0" || "${RX_DELTA[arancini]}" == "0" ]]; then
  echo "invalid benchmark: rx deltas were zero (risotto=${RX_DELTA[risotto]}, arancini=${RX_DELTA[arancini]})" >&2
  exit 2
fi

speedup="n/a"
if awk "BEGIN{exit !(${THROUGHPUT[risotto]} > 0)}"; then
  speedup="$(awk "BEGIN{printf \"%.2fx\", ${THROUGHPUT[arancini]}/${THROUGHPUT[risotto]}}")"
fi

{
  echo "# Risotto vs Arancini Saturating Throughput Report"
  echo
  echo "- Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Host: $(uname -a)"
  echo "- Workload: peer-pressure inject ${ROUTE_COUNT} routes from gobgp50, target rx delta ${TARGET_RX_DELTA}, timeout ${TIMEOUT_SECONDS}s"
  echo "- Producer paths: disabled (--kafka-disable, NATS disabled)"
  echo "- Curation: disabled (--curation-disable)"
  echo
  echo "## Results"
  echo
  echo "| Runtime | rx_update delta | elapsed sec | rx throughput (/s) | bmp_message delta | process_cpu_seconds delta | error count |"
  echo "|---|---:|---:|---:|---:|---:|---:|"
  echo "| risotto | ${RX_DELTA[risotto]} | ${ELAPSED[risotto]} | ${THROUGHPUT[risotto]} | ${BMP_DELTA[risotto]} | ${CPU_DELTA[risotto]} | ${ERR_COUNT[risotto]} |"
  echo "| arancini | ${RX_DELTA[arancini]} | ${ELAPSED[arancini]} | ${THROUGHPUT[arancini]} | ${BMP_DELTA[arancini]} | ${CPU_DELTA[arancini]} | ${ERR_COUNT[arancini]} |"
  echo
  echo "## Relative"
  echo
  echo "- Arancini vs Risotto rx throughput: **${speedup}**"
} >"$REPORT_PATH"

echo "Report written to: $REPORT_PATH"
