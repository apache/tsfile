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
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Timeseries handles returned by the dataset package."""

from typing import List, Optional, Tuple

import numpy as np

from .merge import merge_time_value_parts
from .formatting import format_aligned_timeseries, format_timestamp


class AlignedTimeseries:
    """Time-aligned multi-series query result with timestamps."""

    def __init__(self, timestamps: np.ndarray, values: np.ndarray, series_names: List[str]):
        self.timestamps = timestamps
        self.values = values
        self.series_names = series_names

    @property
    def shape(self):
        return self.values.shape

    def __len__(self):
        return len(self.timestamps)

    def __getitem__(self, key):
        return self.values[key]

    def __repr__(self):
        return format_aligned_timeseries(self.timestamps, self.values, self.series_names, max_rows=20)

    def show(self, max_rows: Optional[int] = None):
        print(format_aligned_timeseries(self.timestamps, self.values, self.series_names, max_rows=max_rows))


class Timeseries:
    """Single logical series with transparent cross-file merging."""

    def __init__(self, name: str, readers_and_infos: list, merged_timestamps: np.ndarray):
        self._name = name
        self._readers_and_infos = readers_and_infos
        self._timestamps = merged_timestamps

    @property
    def name(self) -> str:
        return self._name

    @property
    def timestamps(self) -> np.ndarray:
        return self._timestamps

    @property
    def stats(self) -> dict:
        count = len(self._timestamps)
        if count == 0:
            return {"start_time": None, "end_time": None, "count": 0}
        return {
            "start_time": int(self._timestamps[0]),
            "end_time": int(self._timestamps[-1]),
            "count": count,
        }

    def __len__(self) -> int:
        return len(self._timestamps)

    def __getitem__(self, key):
        length = len(self._timestamps)

        if isinstance(key, int):
            if key < 0:
                key += length
            if key < 0 or key >= length:
                raise IndexError(f"Index {key} out of range [0, {length})")
            ts = int(self._timestamps[key])
            _, values = self._query_time_range(ts, ts)
            return float(values[0]) if len(values) > 0 else None

        if isinstance(key, slice):
            start, stop, step = key.indices(length)
            if start >= stop:
                return np.array([], dtype=np.float64)

            requested_ts = self._timestamps[start:stop]
            if len(requested_ts) == 0:
                return np.array([], dtype=np.float64)

            ts_arr, values = self._query_time_range(int(requested_ts[0]), int(requested_ts[-1]))
            result = np.full(len(requested_ts), np.nan)
            if len(ts_arr) > 0:
                indices = np.searchsorted(ts_arr, requested_ts)
                valid = (indices < len(ts_arr)) & (
                    ts_arr[np.minimum(indices, len(ts_arr) - 1)] == requested_ts
                )
                result[valid] = values[indices[valid]]
            return result[::step] if step != 1 else result

        raise TypeError(f"Unsupported key type: {type(key)}")

    def _query_time_range(self, start_time: int, end_time: int) -> Tuple[np.ndarray, np.ndarray]:
        time_parts = []
        value_parts = []
        for reader, info in self._readers_and_infos:
            if info["max_time"] < start_time or info["min_time"] > end_time:
                continue
            ts_arr, val_arr = reader.read_series_by_time_range(self._name, start_time, end_time)
            if len(ts_arr) > 0:
                time_parts.append(ts_arr)
                value_parts.append(val_arr)
        return merge_time_value_parts(time_parts, value_parts)

    def __repr__(self):
        stats = self.stats
        if stats["count"] == 0:
            return f"Timeseries('{self._name}', count=0)"
        return (
            f"Timeseries('{self._name}', count={stats['count']}, "
            f"start={format_timestamp(stats['start_time'])}, "
            f"end={format_timestamp(stats['end_time'])})"
        )
