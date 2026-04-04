#!/usr/bin/env python3
"""Prepare REDD dataset for TsFile vs Parquet benchmark.

REDD (Reference Energy Disaggregation Data Set):
  - Household electricity usage from 6 buildings
  - Each channel = 1 meter = 1 device
  - Schema: TAG(building, meter), FIELD(power DOUBLE)

Download: http://redd.csail.mit.edu/
Extract low_freq data to --raw-dir.

Usage:
    python3 prepare_redd.py --raw-dir ./raw/redd --out-dir ./prepared/redd
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path


def parse_channel_file(filepath, building, channel):
    """Parse a REDD channel .dat file. Format: timestamp power_value"""
    rows = []
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                ts = int(float(parts[0]))
                power = float(parts[1])
                rows.append((ts, building, channel, power))
            except ValueError:
                continue
    return rows


def main():
    parser = argparse.ArgumentParser(description="Prepare REDD dataset")
    parser.add_argument("--raw-dir", required=True,
                        help="Path to extracted REDD low_freq data")
    parser.add_argument("--out-dir", required=True,
                        help="Output directory for prepared CSV")
    args = parser.parse_args()

    raw_dir = Path(args.raw_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not raw_dir.exists():
        print(f"Error: raw directory {raw_dir} does not exist", file=sys.stderr)
        print("Download from http://redd.csail.mit.edu/ and extract low_freq data",
              file=sys.stderr)
        sys.exit(1)

    all_rows = []
    devices = set()
    house_dirs = sorted(raw_dir.glob("house_*"))

    if not house_dirs:
        print(f"Error: no house_* directories found in {raw_dir}", file=sys.stderr)
        sys.exit(1)

    for house_dir in house_dirs:
        building = house_dir.name  # e.g., "house_1"
        channel_files = sorted(house_dir.glob("channel_*.dat"))
        print(f"  {building}: {len(channel_files)} channels")

        for cf in channel_files:
            channel = cf.stem  # e.g., "channel_1"
            device_id = f"{building}_{channel}"
            devices.add(device_id)
            rows = parse_channel_file(cf, building, channel)
            all_rows.extend(rows)
            print(f"    {channel}: {len(rows)} points")

    # Sort by device (building+channel), then timestamp
    all_rows.sort(key=lambda r: (r[1], r[2], r[0]))

    # Write CSV
    csv_path = out_dir / "data_sorted.csv"
    print(f"\nWriting {len(all_rows)} rows to {csv_path}...")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "building", "meter", "power"])
        writer.writerows(all_rows)

    # Write metadata
    meta = {
        "dataset": "redd",
        "table_name": "redd",
        "total_points": len(all_rows),
        "num_devices": len(devices),
        "num_series": len(devices),  # 1 series per device
        "tags": [
            {"name": "building", "type": "STRING"},
            {"name": "meter", "type": "STRING"},
        ],
        "fields": [
            {"name": "power", "type": "DOUBLE"},
        ],
    }
    meta_path = out_dir / "meta.json"
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    print(f"Done: {meta['total_points']} points, "
          f"{meta['num_devices']} devices, "
          f"{meta['num_series']} series")
    print(f"  CSV:  {csv_path}")
    print(f"  Meta: {meta_path}")


if __name__ == "__main__":
    main()
