#!/bin/bash
#
# Build and run Chapter 4 experiment: write/read parallel throughput.
#
# Usage:
#   ./run.sh [threads]
#
#   threads: on | off | both (default: both)
#
#     off  -> C1: serial baseline   (ENABLE_THREADS=OFF)
#     on   -> C4: column-parallel   (ENABLE_THREADS=ON)
#     both -> run C1 then C4
#
# Environment variables:
#   TOTAL_ROWS  rows per run (default: 5000000)
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="$SCRIPT_DIR/E4_12_throughput"

THREADS_MODE=${1:-both}
TOTAL_ROWS=${TOTAL_ROWS:-5000000}

CMAKE=$(command -v cmake 2>/dev/null || echo "")
if [ -z "$CMAKE" ] && [ -x "/Applications/CMake.app/Contents/bin/cmake" ]; then
    CMAKE="/Applications/CMake.app/Contents/bin/cmake"
fi
[ -z "$CMAKE" ] && { echo "ERROR: cmake not found"; exit 1; }

NCPU=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)

build_and_run() {
    local threads=$1  # ON or OFF
    local label=$2    # C1 or C4

    local BUILD_DIR="$PROJECT_DIR/cmake-build-release-${label}"
    echo "  [build] throughput_bench (THREADS=$threads) -> $BUILD_DIR"

    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    "$CMAKE" -DCMAKE_BUILD_TYPE=Release \
             -DENABLE_THREADS=$threads \
             -DENABLE_SIMD=OFF \
             -DBUILD_TEST=OFF \
             "$PROJECT_DIR" > /dev/null 2>&1
    "$CMAKE" --build . --target throughput_bench -j"$NCPU" 2>&1 | tail -3
    cd "$SCRIPT_DIR"

    echo "  [run] throughput_bench ($label, rows=$TOTAL_ROWS)"
    "$BUILD_DIR/experiment/chap04/throughput_bench" "$TOTAL_ROWS" "$OUT_DIR"
    echo ""
}

echo "========================================"
echo " Chapter 4: Parallel Throughput (E4-12)"
echo "========================================"
echo "  threads_mode : $THREADS_MODE"
echo "  total_rows   : $TOTAL_ROWS"
echo "  output       : $OUT_DIR"
echo ""

case "$THREADS_MODE" in
    off)  build_and_run OFF C1 ;;
    on)   build_and_run ON  C4 ;;
    both) build_and_run OFF C1; build_and_run ON C4 ;;
    *)    echo "Usage: $0 [on|off|both]"; exit 1 ;;
esac

echo "========================================"
echo " Done! CSVs in $OUT_DIR/"
echo "  write_results_OFF.csv  (C1 serial)"
echo "  write_results_ON.csv   (C4 parallel)"
echo "  read_results_OFF.csv"
echo "  read_results_ON.csv"
echo "========================================"
