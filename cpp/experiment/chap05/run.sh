#!/bin/bash
#
# Build and run Chapter 5 experiments.
#
# Usage:
#   ./run.sh [experiment] [simd]
#
#   experiment: e5_1 | e5_2 | e5_4 | all (default: all)
#   simd:       on | off | both (default: both)
#
# Examples:
#   ./run.sh all both     # run all experiments with SIMD ON and OFF
#   ./run.sh e5_1 on      # run E5-1 only with SIMD ON
#   ./run.sh e5_4          # run E5-4 (SIMD irrelevant for this one)
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

EXPERIMENT=${1:-all}
SIMD_MODE=${2:-both}

# E5-1 / E5-2 row counts (reduce for quick testing)
CODEC_ROWS=${CODEC_ROWS:-20000000}       # 20M per dtype
FILTER_ROWS=${FILTER_ROWS:-200000000}    # 200M

# Find cmake
CMAKE=$(command -v cmake 2>/dev/null || echo "")
if [ -z "$CMAKE" ] && [ -x "/Applications/CMake.app/Contents/bin/cmake" ]; then
    CMAKE="/Applications/CMake.app/Contents/bin/cmake"
fi
if [ -z "$CMAKE" ]; then
    echo "ERROR: cmake not found"
    exit 1
fi

NCPU=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)

# ─── Build helper ────────────────────────────────────────────────────────────

build_target() {
    local target=$1
    local simd=$2    # ON or OFF
    local label=$3   # C1 or C3

    local BUILD_DIR="$PROJECT_DIR/cmake-build-release-${label}"
    echo "  [build] $target (SIMD=$simd) -> $BUILD_DIR"

    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"
    "$CMAKE" -DCMAKE_BUILD_TYPE=Release \
             -DENABLE_SIMD=$simd \
             -DENABLE_THREADS=OFF \
             -DBUILD_TEST=OFF \
             "$PROJECT_DIR" > /dev/null 2>&1
    "$CMAKE" --build . --target "$target" -j"$NCPU" 2>&1 | tail -3
    cd "$SCRIPT_DIR"
}

# ─── E5-1: Codec Throughput ─────────────────────────────────────────────────

run_e5_1() {
    local simd=$1  # ON or OFF
    local label=$2 # C1 or C3
    local BUILD_DIR="$PROJECT_DIR/cmake-build-release-${label}"
    local OUT_DIR="$SCRIPT_DIR/E5_1_codec"

    build_target codec_bench "$simd" "$label"

    echo "  [run] codec_bench (${label}, ${CODEC_ROWS} rows)"
    "$BUILD_DIR/experiment/chap05/codec_bench" "$CODEC_ROWS" "$OUT_DIR"
    echo ""
}

# ─── E5-2: Filter + Late Materialization ────────────────────────────────────

run_e5_2() {
    local simd=$1
    local label=$2
    local BUILD_DIR="$PROJECT_DIR/cmake-build-release-${label}"
    local OUT_DIR="$SCRIPT_DIR/E5_2_filter_latmat"

    build_target filter_bench "$simd" "$label"

    echo "  [run] filter_bench (${label}, ${FILTER_ROWS} rows)"
    "$BUILD_DIR/experiment/chap05/filter_bench" all "$FILTER_ROWS" "$OUT_DIR"
    echo ""
}

# ─── E5-4: Block Filter Precision ──────────────────────────────────────────

run_e5_4() {
    # E5-4 is SIMD-independent (pure decoder analysis), build with default
    local BUILD_DIR="$PROJECT_DIR/cmake-build-release-C1"
    local OUT_DIR="$SCRIPT_DIR/E5_4_block_filter"

    build_target block_filter_bench OFF C1

    echo "  [run] block_filter_bench"
    "$BUILD_DIR/experiment/chap05/block_filter_bench" "$OUT_DIR"
    echo ""
}

# ─── Main ────────────────────────────────────────────────────────────────────

echo "========================================"
echo " Chapter 5: SIMD & Filter Experiments"
echo "========================================"
echo "  experiment: $EXPERIMENT"
echo "  simd_mode:  $SIMD_MODE"
echo "  codec_rows: $CODEC_ROWS"
echo "  filter_rows: $FILTER_ROWS"
echo ""

run_with_simd() {
    local exp=$1

    if [ "$SIMD_MODE" = "off" ] || [ "$SIMD_MODE" = "both" ]; then
        echo "━━━ ${exp} (C1: SIMD OFF) ━━━"
        "run_${exp}" OFF C1
    fi
    if [ "$SIMD_MODE" = "on" ] || [ "$SIMD_MODE" = "both" ]; then
        echo "━━━ ${exp} (C3: SIMD ON) ━━━"
        "run_${exp}" ON C3
    fi
}

case "$EXPERIMENT" in
    e5_1)
        run_with_simd e5_1
        ;;
    e5_2)
        run_with_simd e5_2
        ;;
    e5_4)
        run_e5_4
        ;;
    all)
        run_e5_4
        run_with_simd e5_1
        run_with_simd e5_2
        ;;
    *)
        echo "Unknown experiment: $EXPERIMENT"
        echo "Usage: $0 [e5_1|e5_2|e5_4|all] [on|off|both]"
        exit 1
        ;;
esac

echo "========================================"
echo " Done! Results in:"
echo "  E5-1: $SCRIPT_DIR/E5_1_codec/"
echo "  E5-2: $SCRIPT_DIR/E5_2_filter_latmat/"
echo "  E5-4: $SCRIPT_DIR/E5_4_block_filter/"
echo "========================================"
