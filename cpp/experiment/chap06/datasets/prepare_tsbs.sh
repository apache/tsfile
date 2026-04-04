#!/bin/bash
# Generate TSBS (Time Series Benchmark Suite) IoT dataset.
#
# TSBS IoT use case: trucks with GPS + velocity sensors.
# Schema: TAG(name, fleet, driver), FIELD(latitude, longitude, elevation, velocity)
#
# Paper config: 4000 devices (trucks), 496M points
# Default here: 4000 trucks, ~500M points (adjustable via env vars)
#
# Prerequisites:
#   - Go toolchain (go 1.19+)
#   - tsbs_generate_data binary (built from github.com/timescale/tsbs)
#
# Usage:
#   bash prepare_tsbs.sh --out-dir ./prepared/tsbs
#   TSBS_SCALE=100 bash prepare_tsbs.sh --out-dir ./prepared/tsbs  # smaller

set -euo pipefail

# ─── Defaults ───────────────────────────────────────────────────────────────
TSBS_SCALE="${TSBS_SCALE:-4000}"        # number of trucks (4000 = paper config)
TSBS_SEED="${TSBS_SEED:-42}"
TSBS_TS_START="${TSBS_TS_START:-2022-01-01T00:00:00Z}"
TSBS_TS_END="${TSBS_TS_END:-2022-01-15T00:00:00Z}"  # 2 weeks ≈ 496M points at 4000 scale
TSBS_REPO="github.com/timescale/tsbs"
TSBS_BIN=""
OUT_DIR=""

# ─── Parse args ─────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --out-dir) OUT_DIR="$2"; shift 2 ;;
        --scale)   TSBS_SCALE="$2"; shift 2 ;;
        --seed)    TSBS_SEED="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if [ -z "$OUT_DIR" ]; then
    echo "Usage: $0 --out-dir <path> [--scale N] [--seed N]"
    exit 1
fi
mkdir -p "$OUT_DIR"

# ─── Ensure tsbs_generate_data is available ─────────────────────────────────
find_or_build_tsbs() {
    # Check if already in PATH
    if command -v tsbs_generate_data &>/dev/null; then
        TSBS_BIN="tsbs_generate_data"
        return
    fi

    # Check common Go bin locations
    local gobin="${GOPATH:-$HOME/go}/bin"
    if [ -x "$gobin/tsbs_generate_data" ]; then
        TSBS_BIN="$gobin/tsbs_generate_data"
        return
    fi

    # Build from source
    echo "tsbs_generate_data not found. Installing from $TSBS_REPO..."
    if ! command -v go &>/dev/null; then
        echo "Error: Go toolchain required. Install from https://go.dev/dl/"
        exit 1
    fi

    go install "${TSBS_REPO}/cmd/tsbs_generate_data@latest"
    TSBS_BIN="$gobin/tsbs_generate_data"

    if [ ! -x "$TSBS_BIN" ]; then
        echo "Error: failed to build tsbs_generate_data"
        exit 1
    fi
}

find_or_build_tsbs
echo "Using: $TSBS_BIN"
echo "Config: scale=$TSBS_SCALE, seed=$TSBS_SEED"
echo "  time range: $TSBS_TS_START .. $TSBS_TS_END"

# ─── Generate data ──────────────────────────────────────────────────────────
RAW_FILE="$OUT_DIR/tsbs_raw.csv"
echo "Generating TSBS IoT data..."
"$TSBS_BIN" \
    --use-case="iot" \
    --seed="$TSBS_SEED" \
    --scale="$TSBS_SCALE" \
    --timestamp-start="$TSBS_TS_START" \
    --timestamp-end="$TSBS_TS_END" \
    --format="csv" \
    > "$RAW_FILE"

echo "Raw data: $RAW_FILE ($(wc -l < "$RAW_FILE") lines)"

# ─── Convert to sorted CSV ──────────────────────────────────────────────────
# TSBS CSV format varies by use-case. For IoT, the format is:
# tags: name, fleet, driver
# fields: latitude, longitude, elevation, velocity
#
# The raw output may have headers and metadata lines.
# We use Python to parse and sort by device_id.

python3 - "$RAW_FILE" "$OUT_DIR" <<'PYEOF'
import csv
import json
import sys
from pathlib import Path

raw_file = sys.argv[1]
out_dir = Path(sys.argv[2])

rows = []
devices = set()
header_found = False

with open(raw_file, "r") as f:
    reader = csv.reader(f)
    for line in reader:
        # Skip metadata/header lines
        if not line or line[0].startswith("#") or line[0].startswith("tags"):
            continue
        # TSBS IoT CSV: typically
        #   name,fleet,driver,timestamp,latitude,longitude,elevation,velocity
        # But format may vary; detect from first data row
        if len(line) >= 8:
            try:
                name = line[0]
                fleet = line[1]
                driver = line[2]
                ts_str = line[3]
                lat = float(line[4])
                lon = float(line[5])
                ele = float(line[6])
                vel = float(line[7])
                # Parse timestamp (ISO or epoch)
                if "T" in ts_str or "-" in ts_str:
                    from datetime import datetime
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    ts = int(dt.timestamp())
                else:
                    ts = int(ts_str)
                device_id = f"{name}.{fleet}.{driver}"
                devices.add(device_id)
                rows.append((ts, name, fleet, driver, lat, lon, ele, vel))
            except (ValueError, IndexError):
                continue

# Sort by device (name, fleet, driver), then timestamp
rows.sort(key=lambda r: (r[1], r[2], r[3], r[0]))

csv_path = out_dir / "data_sorted.csv"
print(f"Writing {len(rows)} rows to {csv_path}...")
with open(csv_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "name", "fleet", "driver",
                     "latitude", "longitude", "elevation", "velocity"])
    writer.writerows(rows)

meta = {
    "dataset": "tsbs",
    "table_name": "tsbs",
    "total_points": len(rows),
    "num_devices": len(devices),
    "num_series": len(devices) * 4,
    "tags": [
        {"name": "name", "type": "STRING"},
        {"name": "fleet", "type": "STRING"},
        {"name": "driver", "type": "STRING"},
    ],
    "fields": [
        {"name": "latitude", "type": "DOUBLE"},
        {"name": "longitude", "type": "DOUBLE"},
        {"name": "elevation", "type": "DOUBLE"},
        {"name": "velocity", "type": "DOUBLE"},
    ],
}
meta_path = out_dir / "meta.json"
with open(meta_path, "w") as f:
    json.dump(meta, f, indent=2)

print(f"Done: {meta['total_points']} points, "
      f"{meta['num_devices']} devices, {meta['num_series']} series")
PYEOF

# Clean up raw file to save space
rm -f "$RAW_FILE"
echo "TSBS preparation complete."
