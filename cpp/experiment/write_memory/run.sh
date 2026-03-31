#!/bin/bash
#
# Build, run write_memory experiment, and plot results.
#
# Usage:
#   ./run.sh [total_rows] [batch_size] [print_interval] [mem_threshold_mb] [write_mode]
#
# Defaults: 200M rows, 64K batch, 500K print interval, 128 MB threshold, mode 0
# write_mode: 0=sequential (per-device), 1=interleaved (mixed devices per tablet)
#
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
BUILD_DIR="$PROJECT_DIR/cmake-build-release"

TOTAL_ROWS=${1:-200000000}
BATCH_SIZE=${2:-65536}
PRINT_INTERVAL=${3:-500000}
MEM_THRESHOLD_MB=${4:-128}
WRITE_MODE=${5:-0}

if [ "$WRITE_MODE" = "0" ]; then
    MODE_STR="sequential"
else
    MODE_STR="interleaved"
fi

CSV_FILE="$SCRIPT_DIR/write_memory_stats_${MODE_STR}.csv"
PNG_FILE="$SCRIPT_DIR/write_memory_chart_${MODE_STR}.png"

# Find cmake
CMAKE=$(command -v cmake 2>/dev/null || echo "")
if [ -z "$CMAKE" ] && [ -x "/Applications/CMake.app/Contents/bin/cmake" ]; then
    CMAKE="/Applications/CMake.app/Contents/bin/cmake"
fi
if [ -z "$CMAKE" ]; then
    echo "ERROR: cmake not found"
    exit 1
fi

echo "========================================"
echo " TsFile Write Memory Experiment"
echo "========================================"
echo "  total_rows:     $TOTAL_ROWS"
echo "  batch_size:     $BATCH_SIZE"
echo "  print_interval: $PRINT_INTERVAL"
echo "  mem_threshold:  ${MEM_THRESHOLD_MB} MB"
echo "  write_mode:     ${MODE_STR} (${WRITE_MODE})"
echo "  build_dir:      $BUILD_DIR"
echo ""

# --- Step 1: Build ---
echo "[1/3] Building with ENABLE_MEM_STAT=ON ..."
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
"$CMAKE" -DCMAKE_BUILD_TYPE=Release \
         -DENABLE_MEM_STAT=ON \
         -DBUILD_TEST=OFF \
         "$PROJECT_DIR" > /dev/null 2>&1
"$CMAKE" --build . --target write_memory -j$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4) 2>&1 | tail -3
echo "  Build OK"
echo ""

# --- Step 2: Run ---
echo "[2/3] Running write_memory ($((TOTAL_ROWS / 1000000))M rows) ..."
cd "$SCRIPT_DIR"
"$BUILD_DIR/experiment/write_memory/write_memory" \
    "$TOTAL_ROWS" "$BATCH_SIZE" "$PRINT_INTERVAL" "$CSV_FILE" "$MEM_THRESHOLD_MB" "$WRITE_MODE"
echo ""

# --- Step 3: Plot ---
echo "[3/3] Plotting results ..."
PYTHON=$(command -v python3 2>/dev/null || command -v python 2>/dev/null || echo "")
if [ -z "$PYTHON" ]; then
    echo "WARNING: python3 not found, skipping plot"
    echo "  CSV data is at: $CSV_FILE"
    echo "  Run manually: python3 plot_memory.py $CSV_FILE $PNG_FILE"
    exit 0
fi

# Check matplotlib
if ! "$PYTHON" -c "import matplotlib" 2>/dev/null; then
    echo "  Installing matplotlib ..."
    "$PYTHON" -m pip install matplotlib -q
fi

"$PYTHON" "$SCRIPT_DIR/plot_memory.py" "$CSV_FILE" "$PNG_FILE"

echo ""
echo "========================================"
echo " Done!"
echo "  CSV:   $CSV_FILE"
echo "  Chart: $PNG_FILE"
echo "========================================"
