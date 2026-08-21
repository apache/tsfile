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

from typing import Dict, List, Tuple

import numpy as np

from ._merge import (
    merge_time_value_parts_overlap,
    merge_timestamp_parts_overlap,
    scatter_timeline_columns,
)


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

    return merge_timestamp_parts_overlap(parts, deduplicate, validate_unique)


def merge_time_value_parts(
    time_parts: List[np.ndarray],
    value_parts: List[np.ndarray],
) -> Tuple[np.ndarray, np.ndarray]:
    """Merge sorted time/value parts for one logical series.

    Duplicate timestamps are validated during metadata loading and again by
    the overlapping query merge as a defense against stale or external indexes.

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

    return merge_time_value_parts_overlap(time_parts, value_parts)


def build_aligned_matrix(
    series_names: List[str], series_data: Dict[str, Tuple[np.ndarray, np.ndarray]]
) -> Tuple[np.ndarray, np.ndarray]:
    """Build a timestamp union and aligned value matrix for multiple series.

    Each input series is assumed to already satisfy the dataset merge policy,
    meaning its timestamp array is unique within that logical series.
    """
    # Aligned measurements from one device intentionally share the same
    # timestamp ndarray.  Collapse those identities first, then also collapse
    # exact-equal timelines from different devices.  Otherwise the generic
    # implementation feeds the same 2,880 timestamps into the union once per
    # selected measurement and repeats np.searchsorted for every column.
    timeline_groups = []
    groups_by_identity = {}
    for col_idx, name in enumerate(series_names):
        item = series_data.get(name)
        if item is None or len(item[0]) == 0:
            continue
        ts_arr, val_arr = item
        group = groups_by_identity.get(id(ts_arr))
        if group is None:
            group = [ts_arr, []]
            groups_by_identity[id(ts_arr)] = group
            timeline_groups.append(group)
        group[1].append((col_idx, val_arr))

    if not timeline_groups:
        return np.array([], dtype=np.int64), np.empty((0, len(series_names)))

    distinct_timelines = []
    equivalence_buckets = {}
    for ts_arr, columns in timeline_groups:
        last_index = len(ts_arr) - 1
        sample = tuple(
            int(ts_arr[last_index * sample_index // 7]) for sample_index in range(1, 7)
        )
        equivalence_key = (len(ts_arr), int(ts_arr[0]), int(ts_arr[-1]), sample)
        equivalent = None
        bucket = equivalence_buckets.setdefault(equivalence_key, [])
        for current in bucket:
            current_ts = current[0]
            if np.array_equal(current_ts, ts_arr):
                equivalent = current
                break
        if equivalent is None:
            equivalent = [ts_arr, columns]
            distinct_timelines.append(equivalent)
            bucket.append(equivalent)
        else:
            equivalent[1].extend(columns)

    if len(distinct_timelines) == 1:
        timestamps = distinct_timelines[0][0]
        values = np.full((len(timestamps), len(series_names)), np.nan)
        columns = distinct_timelines[0][1]
        scatter_timeline_columns(
            timestamps,
            timestamps,
            [val_arr for _, val_arr in columns],
            [col_idx for col_idx, _ in columns],
            values,
        )
        return timestamps, values

    timestamps = merge_timestamp_parts(
        [group[0] for group in distinct_timelines], deduplicate=True
    )
    values = np.full((len(timestamps), len(series_names)), np.nan)

    for ts_arr, columns in distinct_timelines:
        scatter_timeline_columns(
            timestamps,
            ts_arr,
            [val_arr for _, val_arr in columns],
            [col_idx for col_idx, _ in columns],
            values,
        )

    return timestamps, values
