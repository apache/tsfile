#!/usr/bin/env python3
"""Prepare GeoLife dataset for TsFile vs Parquet benchmark.

GeoLife GPS Trajectories:
  - 182 users, GPS tracks of walking/driving/cycling
  - Schema: TAG(user_id), FIELD(latitude, longitude, altitude DOUBLE)
  - 3 series per device (543 series / 181 devices in paper)

Download: https://download.microsoft.com/download/F/4/8/
          F4894AA5-FDBC-481E-9285-D5F8C4C4F039/
          Geolife%20Trajectories%201.3.zip
Extract to --raw-dir (should contain Data/ subdirectory).

Usage:
    python3 prepare_geolife.py --raw-dir ./raw/geolife --out-dir ./prepared/geolife
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path


def parse_plt_file(filepath):
    """Parse a GeoLife .plt trajectory file.

    First 6 lines are header. Data lines:
      latitude, longitude, 0, altitude, days_since_1899, date, time
    """
    rows = []
    with open(filepath, "r") as f:
        # Skip 6 header lines
        for _ in range(6):
            f.readline()
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) < 7:
                continue
            try:
                lat = float(parts[0])
                lon = float(parts[1])
                alt = float(parts[3])
                # Parse date + time -> epoch seconds
                dt_str = parts[5] + " " + parts[6]
                dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
                ts = int(dt.timestamp())
                rows.append((ts, lat, lon, alt))
            except (ValueError, IndexError):
                continue
    return rows


def main():
    parser = argparse.ArgumentParser(description="Prepare GeoLife dataset")
    parser.add_argument("--raw-dir", required=True,
                        help="Path to extracted GeoLife data (contains Data/)")
    parser.add_argument("--out-dir", required=True,
                        help="Output directory for prepared CSV")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Find Data directory
    data_dir = raw_dir / "Data"
    if not data_dir.exists():
        data_dir = raw_dir  # Maybe extracted directly

    user_dirs = sorted([d for d in data_dir.iterdir()
                        if d.is_dir() and d.name.isdigit()])
    if not user_dirs:
        print(f"Error: no user directories found in {data_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(user_dirs)} users")

    all_rows = []
    devices = set()

    for user_dir in user_dirs:
        user_id = user_dir.name  # e.g., "000", "001"
        traj_dir = user_dir / "Trajectory"
        if not traj_dir.exists():
            continue

        devices.add(user_id)
        plt_files = sorted(traj_dir.glob("*.plt"))
        user_points = 0

        for plt_file in plt_files:
            rows = parse_plt_file(plt_file)
            for ts, lat, lon, alt in rows:
                all_rows.append((ts, user_id, lat, lon, alt))
            user_points += len(rows)

        print(f"  user {user_id}: {user_points} points, "
              f"{len(plt_files)} trajectories")

    # Sort by user_id, then timestamp
    all_rows.sort(key=lambda r: (r[1], r[0]))

    # Deduplicate: TsFile requires unique timestamps per device.
    # For duplicate timestamps within the same device, keep last value.
    before = len(all_rows)
    deduped = []
    for i, row in enumerate(all_rows):
        if i + 1 < len(all_rows) and row[0] == all_rows[i + 1][0] \
                and row[1] == all_rows[i + 1][1]:
            continue  # skip duplicate, keep later one
        deduped.append(row)
    all_rows = deduped
    print(f"Deduplicated: {before} -> {len(all_rows)} "
          f"(removed {before - len(all_rows)} duplicate timestamps)")

    # Write CSV
    csv_path = out_dir / "data_sorted.csv"
    print(f"\nWriting {len(all_rows)} rows to {csv_path}...")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "user_id", "latitude", "longitude",
                         "altitude"])
        writer.writerows(all_rows)

    # Write metadata
    meta = {
        "dataset": "geolife",
        "table_name": "geolife",
        "total_points": len(all_rows),
        "num_devices": len(devices),
        "num_series": len(devices) * 3,  # lat, lon, alt per device
        "tags": [
            {"name": "user_id", "type": "STRING"},
        ],
        "fields": [
            {"name": "latitude", "type": "DOUBLE"},
            {"name": "longitude", "type": "DOUBLE"},
            {"name": "altitude", "type": "DOUBLE"},
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
