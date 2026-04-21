#!/usr/bin/env bash
# Drives the full 2x2 perf benchmark matrix for the Calcite rewrite.
#   A. HSQLDB   + legacy Mondrian SQL
#   B. Postgres + legacy Mondrian SQL
#   C. HSQLDB   + Calcite-generated SQL
#   D. Postgres + Calcite-generated SQL
#
# Each cell writes target/perf-bench-<backend>-<sqlemitter>.json. The
# Python renderer at the end consumes all four and produces
# docs/reports/perf-benchmark-2x2.md.
#
# Postgres side requires `foodmart_calcite` to be populated — see
# scripts/postgres/load-foodmart.sh. HSQLDB side is self-contained via
# the FoodMartHsqldbBootstrap extractor.
#
# Budget: full run is ~20-40 minutes wall time. Skip cells by setting
#   SKIP_A / SKIP_B / SKIP_C / SKIP_D to 1.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

run_cell() {
  local label="$1"
  local backend_env="$2"
  local emitter="$3"

  echo ""
  echo "=================================================================="
  echo "==> Cell $label: backend=${backend_env:-hsqldb} emitter=$emitter"
  echo "=================================================================="

  if [ -n "$backend_env" ]; then
    export CALCITE_HARNESS_BACKEND="$backend_env"
  else
    unset CALCITE_HARNESS_BACKEND || true
  fi

  mvn -Pcalcite-harness \
      -Dharness.runPerfBench=true \
      -Dmondrian.backend="$emitter" \
      -Dtest=PerfBenchmarkTest test
}

if [ "${SKIP_A:-0}" != "1" ]; then
  run_cell "A (HSQLDB + legacy)" "" legacy
fi
if [ "${SKIP_C:-0}" != "1" ]; then
  run_cell "C (HSQLDB + calcite)" "" calcite
fi
if [ "${SKIP_B:-0}" != "1" ]; then
  run_cell "B (Postgres + legacy)" POSTGRES legacy
fi
if [ "${SKIP_D:-0}" != "1" ]; then
  run_cell "D (Postgres + calcite)" POSTGRES calcite
fi

echo ""
echo "=================================================================="
echo "==> Rendering report"
echo "=================================================================="
python3 "$SCRIPT_DIR/render-bench-report.py"
echo ""
echo "==> Done. See docs/reports/perf-benchmark-2x2.md"
