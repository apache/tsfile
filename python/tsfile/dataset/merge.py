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

"""Merge helpers for series parts and aligned query matrices."""

from typing import Dict, List, Tuple

import numpy as np


def merge_time_value_parts(
    time_parts: List[np.ndarray], value_parts: List[np.ndarray]
) -> Tuple[np.ndarray, np.ndarray]:
    """Merge sorted time/value parts and deduplicate timestamps."""
    if not time_parts:
        return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
    if len(time_parts) == 1:
        return time_parts[0], value_parts[0]

    merged_ts = np.concatenate(time_parts)
    merged_vals = np.concatenate(value_parts)
    order = np.argsort(merged_ts, kind="mergesort")
    merged_ts = merged_ts[order]
    merged_vals = merged_vals[order]
    _, unique_idx = np.unique(merged_ts, return_index=True)
    return merged_ts[unique_idx], merged_vals[unique_idx]


def build_aligned_matrix(
    series_names: List[str], series_data: Dict[str, Tuple[np.ndarray, np.ndarray]]
) -> Tuple[np.ndarray, np.ndarray]:
    """Build a timestamp union and aligned value matrix for multiple series."""
    all_ts_arrays = [ts for ts, _ in series_data.values() if len(ts) > 0]
    if not all_ts_arrays:
        return np.array([], dtype=np.int64), np.empty((0, len(series_names)))

    timestamps = np.unique(np.concatenate(all_ts_arrays))
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
