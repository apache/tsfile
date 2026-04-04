#!/usr/bin/env python3
"""Generate TSBS-compatible IoT dataset (Python, no Go dependency).

Replicates the TSBS IoT use case: trucks with GPS + velocity sensors.
Schema: TAG(name, fleet, driver), FIELD(latitude, longitude, elevation, velocity)

Usage:
    python3 prepare_tsbs_py.py --out-dir ./prepared/tsbs --scale 100
    python3 prepare_tsbs_py.py --out-dir ./prepared/tsbs --scale 4000  # paper config
"""

import argparse
import csv
import json
import math
import os
import random
import sys
from pathlib import Path


# TSBS IoT constants (matching Go implementation)
FLEETS = ["East", "West", "South", "North"]
EPOCH_START = 1640995200  # 2022-01-01T00:00:00Z
SAMPLE_INTERVAL = 10     # seconds between samples


def generate_device_ids(scale):
    """Generate truck device identifiers: (name, fleet, driver)."""
    devices = []
    for i in range(scale):
        fleet = FLEETS[i % len(FLEETS)]
        name = f"truck_{i}"
        driver = f"driver_{i}"
        devices.append((name, fleet, driver))
    return devices


def generate_truck_data(name, fleet, driver, num_points, seed_offset):
    """Generate time series data for a single truck."""
    rng = random.Random(hash((name, fleet, driver)) + seed_offset)

    # Starting position (random in continental US-ish range)
    lat = rng.uniform(25.0, 48.0)
    lon = rng.uniform(-125.0, -70.0)
    ele = rng.uniform(0.0, 2000.0)
    vel = rng.uniform(0.0, 80.0)

    rows = []
    ts = EPOCH_START

    for _ in range(num_points):
        # Random walk for GPS + velocity
        lat += rng.gauss(0, 0.001)
        lon += rng.gauss(0, 0.001)
        ele += rng.gauss(0, 1.0)
        ele = max(0.0, ele)
        vel += rng.gauss(0, 2.0)
        vel = max(0.0, min(vel, 120.0))

        rows.append((ts, name, fleet, driver,
                      round(lat, 6), round(lon, 6),
                      round(ele, 2), round(vel, 2)))
        ts += SAMPLE_INTERVAL

    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Generate TSBS IoT dataset (Python)")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--scale", type=int, default=100,
                        help="Number of trucks (4000 = paper config)")
    parser.add_argument("--points-per-device", type=int, default=0,
                        help="Points per device (0 = auto ~124K for paper)")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    scale = args.scale
    # Paper: 496M points / 4000 devices = 124,000 points/device
    # For smaller scales, keep same density
    ppd = args.points_per_device if args.points_per_device > 0 else 124000

    # Limit to avoid filling disk (17GB free)
    max_total = 50_000_000  # 50M points max
    total_est = scale * ppd
    if total_est > max_total:
        ppd = max_total // scale
        print(f"Warning: limiting to {ppd} points/device "
              f"({scale * ppd / 1e6:.1f}M total) to save disk space")

    devices = generate_device_ids(scale)
    print(f"Generating TSBS IoT data: {scale} trucks, "
          f"{ppd} points/device, ~{scale * ppd / 1e6:.1f}M total")

    csv_path = out_dir / "data_sorted.csv"
    total_points = 0

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "name", "fleet", "driver",
                          "latitude", "longitude", "elevation", "velocity"])

        for i, (name, fleet, driver) in enumerate(devices):
            rows = generate_truck_data(name, fleet, driver, ppd, args.seed)
            writer.writerows(rows)
            total_points += len(rows)
            if (i + 1) % 100 == 0 or i == len(devices) - 1:
                print(f"  {i + 1}/{scale} trucks, "
                      f"{total_points / 1e6:.1f}M points")

    meta = {
        "dataset": "tsbs",
        "table_name": "tsbs",
        "total_points": total_points,
        "num_devices": scale,
        "num_series": scale * 4,
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

    csv_size = os.path.getsize(csv_path) / 1024 / 1024
    print(f"\nDone: {total_points} points, {scale} devices, "
          f"{scale * 4} series")
    print(f"  CSV:  {csv_path} ({csv_size:.1f} MB)")
    print(f"  Meta: {meta_path}")


if __name__ == "__main__":
    main()
