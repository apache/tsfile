#!/bin/bash
# Master script: prepare all 4 datasets for Chapter 6 experiments.
#
# Usage:
#   bash prepare_all.sh [--data-root <path>]
#
# Expected raw data layout under <data-root>/raw/:
#   raw/redd/house_1/channel_*.dat ...
#   raw/geolife/Data/000/Trajectory/*.plt ...
#   raw/tdrive/1.txt ...
#   (TSBS is auto-generated, no raw data needed)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_ROOT="${1:-$SCRIPT_DIR}"

RAW_DIR="$DATA_ROOT/raw"
PREP_DIR="$DATA_ROOT/prepared"

echo "============================================"
echo "  Chapter 6 Dataset Preparation"
echo "  raw:      $RAW_DIR"
echo "  prepared: $PREP_DIR"
echo "============================================"

# ─── REDD ───────────────────────────────────────────────────────────────────
if [ -d "$RAW_DIR/redd" ]; then
    echo -e "\n>>> REDD"
    python3 "$SCRIPT_DIR/prepare_redd.py" \
        --raw-dir "$RAW_DIR/redd" --out-dir "$PREP_DIR/redd"
else
    echo -e "\n>>> REDD: skipped (no raw/redd/ directory)"
fi

# ─── GeoLife ────────────────────────────────────────────────────────────────
if [ -d "$RAW_DIR/geolife" ]; then
    echo -e "\n>>> GeoLife"
    python3 "$SCRIPT_DIR/prepare_geolife.py" \
        --raw-dir "$RAW_DIR/geolife" --out-dir "$PREP_DIR/geolife"
else
    echo -e "\n>>> GeoLife: skipped (no raw/geolife/ directory)"
fi

# ─── TDrive ─────────────────────────────────────────────────────────────────
if [ -d "$RAW_DIR/tdrive" ]; then
    echo -e "\n>>> TDrive"
    python3 "$SCRIPT_DIR/prepare_tdrive.py" \
        --raw-dir "$RAW_DIR/tdrive" --out-dir "$PREP_DIR/tdrive"
else
    echo -e "\n>>> TDrive: skipped (no raw/tdrive/ directory)"
fi

# ─── TSBS ───────────────────────────────────────────────────────────────────
echo -e "\n>>> TSBS (generated)"
# Use small scale by default for quick test; set TSBS_SCALE=4000 for paper config
TSBS_SCALE="${TSBS_SCALE:-100}" \
    bash "$SCRIPT_DIR/prepare_tsbs.sh" --out-dir "$PREP_DIR/tsbs"

echo -e "\n============================================"
echo "  All datasets prepared in: $PREP_DIR"
echo "============================================"
ls -lh "$PREP_DIR"/*/data_sorted.csv 2>/dev/null || true
