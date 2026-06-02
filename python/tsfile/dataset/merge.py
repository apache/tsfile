# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed with this work under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Merge helpers for dataset reads.

The dataset package enforces a strict cross-shard merge policy:
- only numeric-compatible field columns are exposed,
- null numeric values are represented as ``NaN``,
- duplicate timestamps for the same logical series across shards are rejected.
"""

import heapq
from typing import Dict, List, Tuple

import numpy as np


def merge_timestamp_parts(
    time_parts: List[np.ndarray],
    *,
    deduplicate: bool = False,
    validate_unique: bool = False,
) -> np.ndarray:
    """Merge sorted timestamp parts with optional deduplication or validation."""
    parts = [ts_part for ts_part in time_parts if len(ts_part) > 0]
    if not parts:
        return np.array([], dtype=np.int64)
    if len(parts) == 1:
        return parts[0]

    parts.sort(key=lambda ts_part: int(ts_part[0]))
    if all(
        int(parts[idx - 1][-1]) < int(parts[idx][0]) for idx in range(1, len(parts))
    ):
        return np.concatenate(parts)

    total_length = sum(len(ts_part) for ts_part in parts)
    merged = np.empty(total_length, dtype=np.int64)

    heap = [(int(ts_part[0]), part_idx, 0) for part_idx, ts_part in enumerate(parts)]
    heapq.heapify(heap)

    out_idx = 0
    last_ts = None
    while heap:
        ts, part_idx, offset = heapq.heappop(heap)

        if last_ts is not None and ts == last_ts:
            if validate_unique:
                raise ValueError(f"Duplicate timestamp {ts} found across shards.")
            if not deduplicate:
                merged[out_idx] = ts
                out_idx += 1
        else:
            merged[out_idx] = ts
            out_idx += 1
            last_ts = ts

        next_offset = offset + 1
        if next_offset < len(parts[part_idx]):
            heapq.heappush(
                heap, (int(parts[part_idx][next_offset]), part_idx, next_offset)
            )

    if validate_unique:
        return merged[:out_idx]
    if deduplicate:
        return merged[:out_idx]
    return merged[:out_idx]


def merge_time_value_parts(
    time_parts: List[np.ndarray],
    value_parts: List[np.ndarray],
) -> Tuple[np.ndarray, np.ndarray]:
    """Merge sorted time/value parts for one logical series.

    Duplicate timestamps are validated during metadata loading, so the query
    path can assume each part is already sorted and conflict-free.

    Fast path: if shard ranges do not overlap in time, concatenate in shard
    order after sorting parts by their first timestamp.
    Fallback: use a k-way merge for overlapping-but-disjoint ranges.
    """
    parts = [
        (ts_part, val_part)
        for ts_part, val_part in zip(time_parts, value_parts)
        if len(ts_part) > 0
    ]
    if not parts:
        return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
    if len(parts) == 1:
        return parts[0]

    parts.sort(key=lambda item: int(item[0][0]))
    time_parts = [ts_part for ts_part, _ in parts]
    value_parts = [val_part for _, val_part in parts]

    if all(
        int(time_parts[idx - 1][-1]) < int(time_parts[idx][0])
        for idx in range(1, len(time_parts))
    ):
        return np.concatenate(time_parts), np.concatenate(value_parts)

    total_length = sum(len(ts_part) for ts_part in time_parts)
    merged_ts = np.empty(total_length, dtype=np.int64)
    merged_vals = np.empty(total_length, dtype=np.float64)

    heap = [
        (int(ts_part[0]), part_idx, 0) for part_idx, ts_part in enumerate(time_parts)
    ]
    heapq.heapify(heap)

    out_idx = 0
    while heap:
        _, part_idx, offset = heapq.heappop(heap)
        merged_ts[out_idx] = time_parts[part_idx][offset]
        merged_vals[out_idx] = value_parts[part_idx][offset]
        out_idx += 1

        next_offset = offset + 1
        if next_offset < len(time_parts[part_idx]):
            heapq.heappush(
                heap, (int(time_parts[part_idx][next_offset]), part_idx, next_offset)
            )

    return merged_ts, merged_vals


def build_aligned_matrix(
    series_names: List[str], series_data: Dict[str, Tuple[np.ndarray, np.ndarray]]
) -> Tuple[np.ndarray, np.ndarray]:
    """Build a timestamp union and aligned value matrix for multiple series.

    Each input series is assumed to already satisfy the dataset merge policy,
    meaning its timestamp array is unique within that logical series.
    """
    all_ts_arrays = [ts for ts, _ in series_data.values() if len(ts) > 0]
    if not all_ts_arrays:
        return np.array([], dtype=np.int64), np.empty((0, len(series_names)))

    timestamps = merge_timestamp_parts(all_ts_arrays, deduplicate=True)
    values = np.full((len(timestamps), len(series_names)), np.nan)

    for col_idx, name in enumerate(series_names):
        if name not in series_data:
            continue
        ts_arr, val_arr = series_data[name]
        if len(ts_arr) == 0:
            continue
        indices = np.searchsorted(timestamps, ts_arr)
        values[indices, col_idx] = val_arr

    return timestamps, values
