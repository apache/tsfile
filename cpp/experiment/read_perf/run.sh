#!/bin/bash
# Read Performance Benchmark Runner
# Builds multiple configurations and runs experiments for each.
#
# Configurations:
#   1) baseline:  SIMD=OFF, THREADS=OFF
#   2) simd:      SIMD=ON,  THREADS=OFF
#   3) threads:   SIMD=OFF, THREADS=ON
#   4) full:      SIMD=ON,  THREADS=ON  (current default)
#
# Usage: ./run.sh [row_count]
#   Default row_count = 10000000 (10M)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ROW_COUNT="${1:-10000000}"
RESULTS_DIR="$SCRIPT_DIR/results"

CMAKE_CMD="${CMAKE_CMD:-cmake}"
NINJA_CMD="${NINJA_CMD:-ninja}"

# Auto-detect cmake on macOS
if ! command -v "$CMAKE_CMD" &>/dev/null; then
    if [ -x "/Applications/CMake.app/Contents/bin/cmake" ]; then
        CMAKE_CMD="/Applications/CMake.app/Contents/bin/cmake"
    fi
fi

mkdir -p "$RESULTS_DIR"

# Build configurations: name, ENABLE_SIMD, ENABLE_THREADS
declare -a CONFIGS=(
    "baseline:OFF:OFF"
    "simd:ON:OFF"
    "threads:OFF:ON"
    "full:ON:ON"
)

build_and_run() {
    local name="$1"
    local simd="$2"
    local threads="$3"
    local build_dir="$PROJECT_ROOT/cmake-build-bench-${name}"
    local csv_out="$RESULTS_DIR/${name}.csv"

    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "  Configuration: ${name} (SIMD=${simd}, THREADS=${threads})"
    echo "════════════════════════════════════════════════════════════"

    # Configure
    "$CMAKE_CMD" -S "$PROJECT_ROOT" -B "$build_dir" \
        -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DENABLE_SIMD="$simd" \
        -DENABLE_THREADS="$threads" \
        -DBUILD_TEST=OFF \
        2>&1 | tail -5

    # Build just the benchmark target (and its dependency tsfile)
    "$CMAKE_CMD" --build "$build_dir" --target read_benchmark -- -j"$(sysctl -n hw.ncpu 2>/dev/null || nproc)" 2>&1 | tail -3

    # Run
    cd "$RESULTS_DIR"
    "$build_dir/experiment/read_perf/read_benchmark" "$ROW_COUNT" "$csv_out"
    cd -
}

echo "TsFile C++ Read Performance Benchmark"
echo "row_count=${ROW_COUNT}"
echo "results_dir=${RESULTS_DIR}"

for config in "${CONFIGS[@]}"; do
    IFS=':' read -r name simd threads <<< "$config"
    build_and_run "$name" "$simd" "$threads"
done

echo ""
echo "All done. CSV results in: $RESULTS_DIR/"
ls -la "$RESULTS_DIR/"*.csv

# Merge all CSVs
echo ""
echo "Merging results..."
MERGED="$RESULTS_DIR/merged.csv"
head -1 "$RESULTS_DIR/baseline.csv" | sed 's/^/config,/' > "$MERGED"
for config in "${CONFIGS[@]}"; do
    IFS=':' read -r name simd threads <<< "$config"
    tail -n +2 "$RESULTS_DIR/${name}.csv" | sed "s/^/${name},/" >> "$MERGED"
done
echo "Merged CSV: $MERGED"
