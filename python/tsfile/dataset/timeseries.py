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

from typing import Callable, List, Optional, Tuple

import numpy as np

from .merge import merge_time_value_parts
from .formatting import format_aligned_timeseries, format_timestamp


class AlignedTimeseries:
    """Time-aligned multi-series query result with timestamps.

    Returned by ``TsFileDataFrame.loc[...]``. The values matrix is aligned on
    the union of timestamps from the selected logical series.
    """

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
    """Single logical numeric series with transparent cross-file merging.

    Cross-shard reads follow the dataset merge policy defined in
    :mod:`tsfile.dataset.merge`: duplicate timestamps across shards are treated
    as an error rather than being merged implicitly.
    """

    def __init__(
        self,
        name: str,
        series_refs: list,
        stats: dict,
        ensure_open: Callable[[], None],
        load_timestamps: Callable[[], np.ndarray],
    ):
        self._name = name
        self._series_refs = series_refs
        self._stats = dict(stats)
        self._ensure_open = ensure_open
        self._load_timestamps = load_timestamps
        self._timestamps = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def timestamps(self) -> np.ndarray:
        self._ensure_open()
        if self._timestamps is None:
            self._timestamps = self._load_timestamps()
        return self._timestamps

    @property
    def stats(self) -> dict:
        return {
            "start_time": self._stats.get("min_time"),
            "end_time": self._stats.get("max_time"),
            "count": self._stats["count"],
        }

    def __len__(self) -> int:
        return self._stats["count"]

    def __getitem__(self, key):
        timestamps = self.timestamps
        length = len(timestamps)

        if isinstance(key, int):
            if key < 0:
                key += length
            if key < 0 or key >= length:
                raise IndexError(f"Index {key} out of range [0, {length})")
            ts = int(timestamps[key])
            _, values = self._query_time_range(ts, ts)
            return float(values[0]) if len(values) > 0 else None

        if isinstance(key, slice):
            requested_ts = timestamps[key]
            if len(requested_ts) == 0:
                return np.array([], dtype=np.float64)

            ts_arr, values = self._query_time_range(int(np.min(requested_ts)), int(np.max(requested_ts)))
            result = np.full(len(requested_ts), np.nan)
            if len(ts_arr) > 0:
                indices = np.searchsorted(ts_arr, requested_ts)
                valid = (indices < len(ts_arr)) & (
                    ts_arr[np.minimum(indices, len(ts_arr) - 1)] == requested_ts
                )
                result[valid] = values[indices[valid]]
            return result

        raise TypeError(f"Unsupported key type: {type(key)}")

    def _query_time_range(self, start_time: int, end_time: int) -> Tuple[np.ndarray, np.ndarray]:
        self._ensure_open()
        time_parts = []
        value_parts = []
        for reader, device_id, field_idx in self._series_refs:
            device_timestamps = reader.get_device_timestamps(device_id)
            if device_timestamps[-1] < start_time or device_timestamps[0] > end_time:
                continue
            ts_arr, val_arr = reader.read_series_by_ref(device_id, field_idx, start_time, end_time)
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
