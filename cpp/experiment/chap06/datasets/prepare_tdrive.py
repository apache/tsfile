#!/usr/bin/env python3
"""Prepare TDrive dataset for TsFile vs Parquet benchmark.

TDrive taxi trajectories:
  - ~10,000 taxis in Beijing over 1 week
  - Schema: TAG(taxi_id), FIELD(longitude, latitude DOUBLE)
  - 2 series per device (17778 series / 8889 devices in paper)

Download: https://www.microsoft.com/en-us/research/publication/
          t-drive-driving-directions-based-on-taxi-trajectories/
Extract to --raw-dir.

Usage:
    python3 prepare_tdrive.py --raw-dir ./raw/tdrive --out-dir ./prepared/tdrive
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path


def parse_taxi_file(filepath, taxi_id):
    """Parse a TDrive taxi trajectory file.

    Format: taxi_id, datetime_string, longitude, latitude
    """
    rows = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) < 4:
                continue
            try:
                dt_str = parts[1].strip()
                lon = float(parts[2].strip())
                lat = float(parts[3].strip())
                # Skip invalid coordinates
                if lon < 1 or lat < 1:
                    continue
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                ts = int(dt.timestamp())
                rows.append((ts, taxi_id, lon, lat))
            except (ValueError, IndexError):
                continue
    return rows


def main():
    parser = argparse.ArgumentParser(description="Prepare TDrive dataset")
    parser.add_argument("--raw-dir", required=True,
                        help="Path to extracted TDrive taxi_log data")
    parser.add_argument("--out-dir", required=True,
                        help="Output directory for prepared CSV")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Find taxi files: {id}.txt (may be in a subdirectory like taxi_log_2008_by_id/)
    taxi_files = sorted(raw_dir.glob("*.txt"),
                        key=lambda p: int(p.stem) if p.stem.isdigit() else 0)
    if not taxi_files:
        # Try subdirectories
        taxi_files = sorted(raw_dir.rglob("*.txt"),
                            key=lambda p: int(p.stem) if p.stem.isdigit() else 0)
    # Filter to only numeric-named files (skip Thumbs.db etc)
    taxi_files = [f for f in taxi_files if f.stem.isdigit()]
    if not taxi_files:
        print(f"Error: no taxi .txt files found in {raw_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(taxi_files)} taxi files")

    all_rows = []
    devices = set()

    for tf in taxi_files:
        taxi_id = f"taxi_{tf.stem}"
        rows = parse_taxi_file(tf, taxi_id)
        if rows:
            devices.add(taxi_id)
            all_rows.extend(rows)
        if len(devices) % 1000 == 0 and len(devices) > 0:
            print(f"  processed {len(devices)} taxis, "
                  f"{len(all_rows)} points so far...")

    # Sort by taxi_id, then timestamp
    all_rows.sort(key=lambda r: (r[1], r[0]))

    # Deduplicate: TsFile requires unique timestamps per device.
    before = len(all_rows)
    deduped = []
    for i, row in enumerate(all_rows):
        if i + 1 < len(all_rows) and row[0] == all_rows[i + 1][0] \
                and row[1] == all_rows[i + 1][1]:
            continue
        deduped.append(row)
    all_rows = deduped
    print(f"Deduplicated: {before} -> {len(all_rows)} "
          f"(removed {before - len(all_rows)} duplicate timestamps)")

    # Write CSV
    csv_path = out_dir / "data_sorted.csv"
    print(f"\nWriting {len(all_rows)} rows to {csv_path}...")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "taxi_id", "longitude", "latitude"])
        writer.writerows(all_rows)

    # Write metadata
    meta = {
        "dataset": "tdrive",
        "table_name": "tdrive",
        "total_points": len(all_rows),
        "num_devices": len(devices),
        "num_series": len(devices) * 2,  # lon, lat per device
        "tags": [
            {"name": "taxi_id", "type": "STRING"},
        ],
        "fields": [
            {"name": "longitude", "type": "DOUBLE"},
            {"name": "latitude", "type": "DOUBLE"},
        ],
    }
    meta_path = out_dir / "meta.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"Done: {meta['total_points']} points, "
          f"{meta['num_devices']} devices, "
          f"{meta['num_series']} series")


if __name__ == "__main__":
    main()
