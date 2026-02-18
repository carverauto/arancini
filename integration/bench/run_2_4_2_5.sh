#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_PATH="${1:-$ROOT_DIR/openspec/changes/refactor-arancini-shared-nothing/validation/2.4-2.5-benchmark-report.md}"

mkdir -p "$(dirname "$REPORT_PATH")"

THROUGHPUT_OUT="$(mktemp)"
ALLOC_OUT="$(mktemp)"
trap 'rm -f "$THROUGHPUT_OUT" "$ALLOC_OUT"' EXIT

cd "$ROOT_DIR"

echo "Running throughput benchmark (2.4)..."
cargo run --release -p risotto --bin arancini_bench -- throughput >"$THROUGHPUT_OUT"

echo "Running allocation benchmark (2.5)..."
cargo run --release -p risotto --bin arancini_bench -- alloc >"$ALLOC_OUT"

THROUGHPUT_CHECK=$(
  awk -F',' '
    BEGIN { ok=1; baseline=0 }
    /^workers,total_updates/ { next }
    /^[0-9]+,/ {
      workers=$1+0
      ups=$4+0
      if (workers==1) baseline=ups
      if (workers>1 && baseline>0) {
        eff=ups/(baseline*workers)
        if (eff < 0.60) ok=0
      }
    }
    END {
      if (ok==1) print "PASS"; else print "WARN"
    }
  ' "$THROUGHPUT_OUT"
)

SER_ALLOC_RATIO=$(
  awk -F',' '
    /^serialize_update_alloc_per_call,/ { old=$5+0 }
    /^serialize_update_into_reused_buffer,/ { neu=$5+0 }
    END {
      if (old>0) printf "%.6f", neu/old; else printf "1.000000"
    }
  ' "$ALLOC_OUT"
)

FRAME_ALLOC_RATIO=$(
  awk -F',' '
    /^legacy_peek_and_alloc_per_frame,/ { old=$5+0 }
    /^framed_reusable_buffer_split_to,/ { neu=$5+0 }
    END {
      if (old>0) printf "%.6f", neu/old; else printf "1.000000"
    }
  ' "$ALLOC_OUT"
)

SER_CHECK="WARN"
FRAME_CHECK="WARN"
SER_CHECK="$(awk "BEGIN{ if ($SER_ALLOC_RATIO < 0.90) print \"PASS\"; else print \"WARN\" }")"
FRAME_CHECK="$(awk "BEGIN{ if ($FRAME_ALLOC_RATIO < 0.20) print \"PASS\"; else print \"WARN\" }")"

{
  echo "# 2.4/2.5 Benchmark Report"
  echo
  echo "- Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "- Host: $(uname -a)"
  echo "- Throughput scaling check: **$THROUGHPUT_CHECK** (target efficiency >= 0.60 vs single worker)"
  echo "- Serialization allocation reduction check: **$SER_CHECK** (target new/old alloc-call ratio < 0.90, measured=$SER_ALLOC_RATIO)"
  echo "- Framing allocation reduction check: **$FRAME_CHECK** (new/old alloc-call ratio=$FRAME_ALLOC_RATIO)"
  echo
  echo "## Throughput Raw Output"
  echo
  echo '```text'
  cat "$THROUGHPUT_OUT"
  echo '```'
  echo
  echo "## Allocation Raw Output"
  echo
  echo '```text'
  cat "$ALLOC_OUT"
  echo '```'
} >"$REPORT_PATH"

echo "Report written to: $REPORT_PATH"
