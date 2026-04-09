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

    def __init__(
        self, timestamps: np.ndarray, values: np.ndarray, series_names: List[str]
    ):
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
        return format_aligned_timeseries(
            self.timestamps, self.values, self.series_names, max_rows=20
        )

    def show(self, max_rows: Optional[int] = None):
        print(
            format_aligned_timeseries(
                self.timestamps, self.values, self.series_names, max_rows=max_rows
            )
        )


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
        read_by_position: Callable[[int, int], Tuple[np.ndarray, np.ndarray]],
    ):
        self._name = name
        self._series_refs = series_refs
        self._stats = dict(stats)
        self._ensure_open = ensure_open
        self._load_timestamps = load_timestamps
        self._read_by_position = read_by_position
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
        self._ensure_open()
        length = len(self)

        if isinstance(key, int):
            if key < 0:
                key += length
            if key < 0 or key >= length:
                raise IndexError(f"Index {key} out of range [0, {length})")
            _, values = self._read_by_position(key, 1)
            return float(values[0]) if len(values) > 0 else None

        if isinstance(key, slice):
            start, stop, step = key.indices(length)
            if (step > 0 and start >= stop) or (step < 0 and start <= stop):
                return np.array([], dtype=np.float64)

            # The common case is a forward contiguous slice like [:], [a:b], or
            # [a:b:1]. Avoid materializing the full position list for large
            # series; read the window directly.
            if step == 1:
                _, values = self._read_by_position(start, stop - start)
                return values

            positions = np.arange(start, stop, step, dtype=np.int64)
            min_pos = int(positions.min())
            max_pos = int(positions.max())
            _, values = self._read_by_position(min_pos, max_pos - min_pos + 1)
            if len(values) == 0:
                return np.array([], dtype=np.float64)
            relative = positions - min_pos
            return values[relative]

        raise TypeError(f"Unsupported key type: {type(key)}")

    def _query_time_range(
        self, start_time: int, end_time: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        self._ensure_open()
        time_parts = []
        value_parts = []
        for reader, device_id, field_idx in self._series_refs:
            device_info = reader.get_device_info(device_id)
            if (
                device_info["max_time"] < start_time
                or device_info["min_time"] > end_time
            ):
                continue
            ts_arr, val_arr = reader.read_series_by_ref(
                device_id, field_idx, start_time, end_time
            )
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
