#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OLD_ROOT="${ROOT_DIR}/old-risotto/risotto"
REPORT_PATH="${1:-$ROOT_DIR/integration/bench/old-kafka-vs-new-nats-report.md}"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

ROUTE_COUNT="${ROUTE_COUNT:-600}"
RECONNECT_CYCLES="${RECONNECT_CYCLES:-6}"
RECONNECT_SLEEP_SECONDS="${RECONNECT_SLEEP_SECONDS:-4}"
POST_BURST_WAIT_SECONDS="${POST_BURST_WAIT_SECONDS:-15}"
BURST_ROUTES_PER_CYCLE="${BURST_ROUTES_PER_CYCLE:-100}"

mkdir -p "$(dirname "$REPORT_PATH")"

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
if ! docker info >/dev/null 2>&1; then
  echo "docker daemon is unreachable" >&2
  exit 1
fi

declare -A SIGNAL_METRIC
declare -A SIGNAL_DELTA
declare -A SIGNAL_RATE
declare -A BMP_RATE
declare -A RX_DELTA
declare -A BMP_DELTA
declare -A KAFKA_MSG_DELTA
declare -A NATS_ACK_DELTA
declare -A NATS_ENQ_DELTA
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

run_old_kafka() {
  local name="old-kafka"
  local compose_file="$OLD_ROOT/integration/compose.yml"
  local project="ovn-old-kafka"
  local metrics_port="18081"
  local metrics_url="http://127.0.0.1:${metrics_port}/metrics"
  local override_file="$WORK_DIR/old-kafka.override.yml"

  cat >"$override_file" <<'EOF'
services:
  risotto:
    command: -v --kafka-brokers=10.0.0.100:9092 --curation-state-path=/app/state.bin
EOF

  local -a compose=(docker compose -f "$compose_file" -f "$override_file" -p "$project")
  echo "[$name] bringing stack up..."
  (
    cd "$OLD_ROOT"
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
    RISOTTO_METRICS_PORT="$metrics_port" "${compose[@]}" up -d --build --force-recreate --renew-anon-volumes
  )

  wait_for_metrics "$metrics_url"
  wait_for_router gobgp10 "${compose[@]}"
  wait_for_router gobgp20 "${compose[@]}"

  echo "[$name] preloading routes..."
  (
    cd "$OLD_ROOT"
    load_routes gobgp10 110 "$ROUTE_COUNT" "${compose[@]}"
  ) &
  local p1=$!
  (
    cd "$OLD_ROOT"
    load_routes gobgp20 120 "$ROUTE_COUNT" "${compose[@]}"
  ) &
  local p2=$!
  wait "$p1" "$p2"
  sleep 3

  local before_bmp before_rx before_kafka
  before_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  before_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
  before_kafka="$(metric_sum "$metrics_url" "risotto_kafka_messages_total")"

  local start_ts end_ts
  start_ts="$(date +%s)"
  for ((cycle=1; cycle<=RECONNECT_CYCLES; cycle++)); do
    echo "[$name] reconnect cycle $cycle/$RECONNECT_CYCLES..."
    (
      cd "$OLD_ROOT"
      "${compose[@]}" stop gobgp10 gobgp20 >/dev/null
      if ! "${compose[@]}" start gobgp10 gobgp20 >/dev/null 2>&1; then
        RISOTTO_METRICS_PORT="$metrics_port" "${compose[@]}" up -d --no-deps gobgp10 gobgp20 >/dev/null
      fi
    )
    wait_for_router gobgp10 "${compose[@]}"
    wait_for_router gobgp20 "${compose[@]}"
    (
      cd "$OLD_ROOT"
      load_routes gobgp10 $(((150 + cycle) % 256)) "$BURST_ROUTES_PER_CYCLE" "${compose[@]}"
    ) &
    local bp1=$!
    (
      cd "$OLD_ROOT"
      load_routes gobgp20 $(((160 + cycle) % 256)) "$BURST_ROUTES_PER_CYCLE" "${compose[@]}"
    ) &
    local bp2=$!
    wait "$bp1" "$bp2"
    sleep "$RECONNECT_SLEEP_SECONDS"
  done
  sleep "$POST_BURST_WAIT_SECONDS"
  end_ts="$(date +%s)"

  local after_bmp after_rx after_kafka
  after_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  after_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
  after_kafka="$(metric_sum "$metrics_url" "risotto_kafka_messages_total")"

  BMP_DELTA["$name"]=$((after_bmp - before_bmp))
  RX_DELTA["$name"]=$((after_rx - before_rx))
  KAFKA_MSG_DELTA["$name"]=$((after_kafka - before_kafka))
  NATS_ACK_DELTA["$name"]=0
  NATS_ENQ_DELTA["$name"]=0

  local window
  window=$((end_ts - start_ts))
  if (( window <= 0 )); then
    window=1
  fi
  SIGNAL_METRIC["$name"]="kafka_messages"
  SIGNAL_DELTA["$name"]="${KAFKA_MSG_DELTA[$name]}"
  SIGNAL_RATE["$name"]="$(awk "BEGIN{printf \"%.2f\", ${SIGNAL_DELTA[$name]}/${window}}")"
  BMP_RATE["$name"]="$(awk "BEGIN{printf \"%.2f\", ${BMP_DELTA[$name]}/${window}}")"

  ERR_COUNT["$name"]="$(
    cd "$OLD_ROOT"
    "${compose[@]}" logs --no-color risotto 2>&1 | grep -E -c 'failed to accept BMP connection|Error handling BMP connection|panic|thread.*panicked' || true
  )"

  (
    cd "$OLD_ROOT"
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  )
}

run_new_nats() {
  local name="new-nats"
  local compose_file="$ROOT_DIR/integration/compose.yml"
  local project="ovn-new-nats"
  local metrics_port="18082"
  local nats_port="14223"
  local metrics_url="http://127.0.0.1:${metrics_port}/metrics"
  local override_file="$WORK_DIR/new-nats.override.yml"

  cat >"$override_file" <<EOF
services:
  nats:
    image: nats:latest
    command: ["-js", "-m", "8222"]
    ports:
      - "${nats_port}:4222"
    networks:
      integration:
        ipv4_address: 10.0.0.110
  risotto:
    command: -v --runtime-mode=arancini --arancini-workers=4 --kafka-disable --nats-enable --nats-server=nats://10.0.0.110:4222 --nats-subject=arancini.updates --curation-state-path=/app/state.bin
EOF

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

  echo "[$name] provisioning JetStream stream..."
  (
    cd "$ROOT_DIR"
    cargo run --release --manifest-path integration/bench/jetstream_admin/Cargo.toml -- \
      --server "nats://127.0.0.1:${nats_port}" \
      --stream "ARANCINI_UPDATES" \
      --subject "arancini.updates.>"
  )

  echo "[$name] preloading routes..."
  (
    cd "$ROOT_DIR"
    load_routes gobgp10 130 "$ROUTE_COUNT" "${compose[@]}"
  ) &
  local p1=$!
  (
    cd "$ROOT_DIR"
    load_routes gobgp20 140 "$ROUTE_COUNT" "${compose[@]}"
  ) &
  local p2=$!
  wait "$p1" "$p2"
  sleep 3

  local before_bmp before_rx before_ack before_enq
  before_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  before_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
  before_ack="$(metric_sum "$metrics_url" "risotto_arancini_nats_ack_success_total")"
  before_enq="$(metric_sum "$metrics_url" "risotto_arancini_nats_publish_enqueued_total")"

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
      load_routes gobgp10 $(((170 + cycle) % 256)) "$BURST_ROUTES_PER_CYCLE" "${compose[@]}"
    ) &
    local bp1=$!
    (
      cd "$ROOT_DIR"
      load_routes gobgp20 $(((180 + cycle) % 256)) "$BURST_ROUTES_PER_CYCLE" "${compose[@]}"
    ) &
    local bp2=$!
    wait "$bp1" "$bp2"
    sleep "$RECONNECT_SLEEP_SECONDS"
  done
  sleep "$POST_BURST_WAIT_SECONDS"
  end_ts="$(date +%s)"

  local after_bmp after_rx after_ack after_enq
  after_bmp="$(metric_sum "$metrics_url" "risotto_bmp_messages_total")"
  after_rx="$(metric_sum "$metrics_url" "risotto_rx_updates_total")"
  after_ack="$(metric_sum "$metrics_url" "risotto_arancini_nats_ack_success_total")"
  after_enq="$(metric_sum "$metrics_url" "risotto_arancini_nats_publish_enqueued_total")"

  BMP_DELTA["$name"]=$((after_bmp - before_bmp))
  RX_DELTA["$name"]=$((after_rx - before_rx))
  KAFKA_MSG_DELTA["$name"]=0
  NATS_ACK_DELTA["$name"]=$((after_ack - before_ack))
  NATS_ENQ_DELTA["$name"]=$((after_enq - before_enq))

  local window
  window=$((end_ts - start_ts))
  if (( window <= 0 )); then
    window=1
  fi
  SIGNAL_METRIC["$name"]="nats_publish_enqueued"
  SIGNAL_DELTA["$name"]="${NATS_ENQ_DELTA[$name]}"
  SIGNAL_RATE["$name"]="$(awk "BEGIN{printf \"%.2f\", ${SIGNAL_DELTA[$name]}/${window}}")"
  BMP_RATE["$name"]="$(awk "BEGIN{printf \"%.2f\", ${BMP_DELTA[$name]}/${window}}")"

  ERR_COUNT["$name"]="$(
    cd "$ROOT_DIR"
    "${compose[@]}" logs --no-color risotto 2>&1 | grep -E -c 'failed to accept BMP connection|Error handling BMP connection|JetStream publish enqueue failed|JetStream ACK failed|panic|thread.*panicked' || true
  )"

  (
    cd "$ROOT_DIR"
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  )
}

run_old_kafka
run_new_nats

old_rate="${SIGNAL_RATE[old-kafka]}"
new_rate="${SIGNAL_RATE[new-nats]}"
speedup="n/a"
if awk "BEGIN{exit !(${old_rate} > 0)}"; then
  speedup="$(awk "BEGIN{printf \"%.2fx\", ${new_rate}/${old_rate}}")"
fi
bmp_old_rate="${BMP_RATE[old-kafka]}"
bmp_new_rate="${BMP_RATE[new-nats]}"
bmp_speedup="n/a"
if awk "BEGIN{exit !(${bmp_old_rate} > 0)}"; then
  bmp_speedup="$(awk "BEGIN{printf \"%.2fx\", ${bmp_new_rate}/${bmp_old_rate}}")"
fi

effective_metric="signal-rate"
effective_speedup="$speedup"
if [[ "$speedup" == "n/a" ]]; then
  effective_metric="bmp-rate-fallback"
  effective_speedup="$bmp_speedup"
fi

{
  echo "# Old Kafka vs New NATS/JetStream Report"
  echo
  echo "- Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Host: $(uname -a)"
  echo "- Workload: $ROUTE_COUNT routes per router preload + $RECONNECT_CYCLES reconnect cycles with $BURST_ROUTES_PER_CYCLE fresh routes per router/cycle (${RECONNECT_SLEEP_SECONDS}s sleep) + ${POST_BURST_WAIT_SECONDS}s settle."
  echo
  echo "## Results"
  echo
  echo "| Profile | signal metric | signal delta | signal rate (/s) | rx_update delta | bmp_message delta | kafka_messages delta | nats_publish_enqueued delta | nats_ack_success delta | error count |"
  echo "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"
  echo "| old-kafka | ${SIGNAL_METRIC[old-kafka]} | ${SIGNAL_DELTA[old-kafka]} | ${SIGNAL_RATE[old-kafka]} | ${RX_DELTA[old-kafka]} | ${BMP_DELTA[old-kafka]} | ${KAFKA_MSG_DELTA[old-kafka]} | ${NATS_ENQ_DELTA[old-kafka]} | ${NATS_ACK_DELTA[old-kafka]} | ${ERR_COUNT[old-kafka]} |"
  echo "| new-nats | ${SIGNAL_METRIC[new-nats]} | ${SIGNAL_DELTA[new-nats]} | ${SIGNAL_RATE[new-nats]} | ${RX_DELTA[new-nats]} | ${BMP_DELTA[new-nats]} | ${KAFKA_MSG_DELTA[new-nats]} | ${NATS_ENQ_DELTA[new-nats]} | ${NATS_ACK_DELTA[new-nats]} | ${ERR_COUNT[new-nats]} |"
  echo
  echo "## Shared Comparator"
  echo
  echo "| Profile | bmp_message rate (/s) |"
  echo "|---|---:|"
  echo "| old-kafka | ${BMP_RATE[old-kafka]} |"
  echo "| new-nats | ${BMP_RATE[new-nats]} |"
  echo
  echo "## Relative Throughput"
  echo
  echo "- signal-rate (transport specific): **$speedup**"
  echo "- bmp-rate (shared fallback): **$bmp_speedup**"
  echo "- effective comparator used: **$effective_metric** => **$effective_speedup**"
} >"$REPORT_PATH"

echo "Report written to: $REPORT_PATH"
