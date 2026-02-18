#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_PATH="${1:-$ROOT_DIR/integration/bench/risotto-vs-arancini-bird-saturating-report.md}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

ROUTE_COUNT_PER_BIRD="${ROUTE_COUNT_PER_BIRD:-5000}"
TARGET_RX_DELTA="${TARGET_RX_DELTA:-2000}"
TIMEOUT_SECONDS="${TIMEOUT_SECONDS:-240}"
ARANCINI_WORKERS="${ARANCINI_WORKERS:-4}"

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

render_bird_conf() {
  local path="$1"
  local router_id="$2"
  local local_ip="$3"
  local local_as="$4"
  local neighbor_ip="$5"
  local neighbor_as="$6"
  local octet="$7"
  local count="$8"

  {
    echo "router id ${router_id};"
    echo
    echo "log stderr all;"
    echo
    echo "protocol device { scan time 5; }"
    echo "protocol direct { ipv4; }"
    echo
    echo "protocol bmp {"
    echo "    station address ip 10.0.0.99 port 4000;"
    echo "    monitoring rib in pre_policy;"
    echo "    monitoring rib in post_policy;"
    echo "    monitoring rib out post_policy;"
    echo "}"
    echo
    echo "protocol static BulkV4 {"
    echo "    ipv4;"
    local i a b
    for ((i=0; i<count; i++)); do
      a=$((i / 256))
      b=$((i % 256))
      echo "    route 172.${octet}.${a}.${b}/32 reject;"
    done
    echo "}"
    echo
    echo "protocol bgp Uplink {"
    echo "    local ${local_ip} as ${local_as};"
    echo "    neighbor ${neighbor_ip} as ${neighbor_as};"
    echo "    ipv4 {"
    echo "        import all;"
    echo "        import table on;"
    echo "        export filter {"
    echo "            if (net ~ [ 172.16.0.0/12+ ]) then accept;"
    echo "            else reject;"
    echo "        };"
    echo "    };"
    echo "}"
  } >"$path"
}

run_profile() {
  local name="$1"
  local runtime_mode="$2"
  local metrics_port="$3"

  local bird30_conf="$WORK_DIR/${name}-bird30.conf"
  local bird40_conf="$WORK_DIR/${name}-bird40.conf"
  render_bird_conf "$bird30_conf" "10.0.0.30" "10.0.0.30" "65030" "10.0.0.40" "65040" 30 "$ROUTE_COUNT_PER_BIRD"
  render_bird_conf "$bird40_conf" "10.0.0.40" "10.0.0.40" "65040" "10.0.0.30" "65030" 40 "$ROUTE_COUNT_PER_BIRD"

  local override_file="$WORK_DIR/${name}.override.yml"
  cat >"$override_file" <<EOFYML
services:
  bird30:
    volumes:
      - ${bird30_conf}:/etc/bird/bird.conf:ro
  bird40:
    volumes:
      - ${bird40_conf}:/etc/bird/bird.conf:ro
  risotto:
    command: -v --runtime-mode=${runtime_mode} --arancini-workers=${ARANCINI_WORKERS} --kafka-disable --curation-disable --curation-state-path=/app/state.bin
EOFYML

  local -a compose=(docker compose -f "$ROOT_DIR/integration/compose.yml" -f "$override_file" -p "birdbench")
  local metrics_url="http://127.0.0.1:${metrics_port}/metrics"

  echo "[$name] starting stack..."
  (
    cd "$ROOT_DIR"
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    "${compose[@]}" build risotto >/dev/null
    RISOTTO_METRICS_PORT="$metrics_port" "${compose[@]}" up -d --no-build --remove-orphans --scale gobgp10=0 --scale gobgp20=0
  )

  wait_for_metrics "$metrics_url"
  sleep 8

  local before_rx before_bmp before_cpu
  before_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
  before_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  before_cpu="$(metric_sum "$metrics_url" "process_cpu_seconds_total")"

  echo "[$name] forcing BIRD replay burst (restart)..."
  (
    cd "$ROOT_DIR"
    "${compose[@]}" restart bird30 bird40 >/dev/null
  )

  local start_ts end_ts
  start_ts="$(date +%s.%N)"
  local deadline=$(( $(date +%s) + TIMEOUT_SECONDS ))
  while true; do
    local current_rx delta
    current_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
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

run_profile "risotto" "risotto" "18093"
run_profile "arancini" "arancini" "18094"

if [[ "${RX_DELTA[risotto]}" == "0" || "${RX_DELTA[arancini]}" == "0" ]]; then
  echo "invalid benchmark: rx deltas were zero (risotto=${RX_DELTA[risotto]}, arancini=${RX_DELTA[arancini]})" >&2
  exit 2
fi

speedup="n/a"
if awk "BEGIN{exit !(${THROUGHPUT[risotto]} > 0)}"; then
  speedup="$(awk "BEGIN{printf \"%.2fx\", ${THROUGHPUT[arancini]}/${THROUGHPUT[risotto]}}")"
fi

{
  echo "# Risotto vs Arancini BIRD Saturating Runtime Report"
  echo
  echo "- Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Host: $(uname -a)"
  echo "- Workload: ${ROUTE_COUNT_PER_BIRD} static routes per BIRD peer, target rx delta ${TARGET_RX_DELTA}, timeout ${TIMEOUT_SECONDS}s"
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
