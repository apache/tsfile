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

"""
High-performance TsFile Series Reader

Optimized for time series data reading using the Arrow columnar API.
Wraps TsFileReaderPy (Cython) and provides series-level metadata
discovery, timestamp caching, and batch reads with TAG filtering.
"""

import os
import sys
from typing import List, Dict, Optional, Tuple

import numpy as np
import pyarrow.compute as pc

from .constants import ColumnCategory
from .tsfile_reader import TsFileReaderPy


class TsFileSeriesReader:
    """
    TsFile Series Reader

    Wrapper around the Cython TsFileReaderPy for reading TsFile data
    at the series level. Supports TAG columns: a time series is uniquely
    identified by Table + Tag values + Field column, producing series
    paths like "weather.beijing.humidity".
    """

    def __init__(self, file_path: str, show_progress: bool = True):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"TsFile not found: {file_path}")

        self.file_path = file_path
        self.show_progress = show_progress

        try:
            self._reader = TsFileReaderPy(file_path)
        except Exception as e:
            raise ValueError(f"Failed to open TsFile: {e}")

        self.series_paths: List[str] = []
        self.series_info: Dict[str, dict] = {}
        self._timestamps_cache: Dict[str, np.ndarray] = {}
        self._series_data_cache: Dict[str, np.ndarray] = {}

        self._cache_metadata()

    def __del__(self):
        self.close()

    def close(self):
        """Close the underlying Cython reader."""
        if hasattr(self, '_reader'):
            try:
                self._reader.close()
            except Exception:
                pass

    def _cache_metadata(self):
        """Cache metadata from the TsFile."""
        try:
            self._cache_metadata_table_model()
        except Exception as e:
            raise ValueError(
                f"Failed to read TsFile metadata. "
                f"Please ensure the TsFile is valid and readable. Error: {e}"
            )

    def _cache_metadata_table_model(self):
        """
        Cache metadata using table model query via Arrow batch API.

        Unified logic for tables with or without TAG columns.
        """
        table_schemas = self._reader.get_all_table_schemas()
        if not table_schemas:
            raise ValueError("No tables found in TsFile")

        self.series_paths = []
        table_names = list(table_schemas.keys())

        # Progress tracking
        total_rows = 0

        for ti, table_name in enumerate(table_names):
            table_schema = self._reader.get_table_schema(table_name)

            tag_columns = []
            field_columns = []
            for col_schema in table_schema.get_columns():
                col_name = col_schema.get_column_name()
                col_category = col_schema.get_category()
                if col_name.lower() == 'time':
                    continue
                if col_category == ColumnCategory.TAG:
                    tag_columns.append(col_name)
                elif col_category == ColumnCategory.FIELD:
                    field_columns.append(col_name)

            if not field_columns:
                continue

            # Query TAG columns + first FIELD column to discover groups and timestamps
            query_cols = tag_columns + [field_columns[0]]

            time_arrays = []
            tag_arrays = {tc: [] for tc in tag_columns}

            with self._reader.query_table_batch(
                table_name, query_cols, batch_size=65536
            ) as rs:
                while True:
                    arrow_table = rs.read_arrow_batch()
                    if arrow_table is None:
                        break
                    batch_rows = arrow_table.num_rows
                    total_rows += batch_rows
                    time_arrays.append(arrow_table.column('time').to_numpy())
                    for tc in tag_columns:
                        tag_arrays[tc].append(arrow_table.column(tc).to_numpy())

                    if self.show_progress:
                        sys.stderr.write(
                            f"\rReading TsFile metadata: "
                            f"table {ti + 1}/{len(table_names)} "
                            f"[{table_name}] "
                            f"({total_rows:,} rows)"
                        )
                        sys.stderr.flush()

            if not time_arrays:
                continue

            all_times = np.concatenate(time_arrays).astype(np.int64)

            if tag_columns:
                # Merge tag columns and group by unique tag combinations
                all_tags = {tc: np.concatenate(tag_arrays[tc]) for tc in tag_columns}

                # Build a composite key for grouping
                if len(tag_columns) == 1:
                    tag_key = all_tags[tag_columns[0]]
                    unique_keys = np.unique(tag_key)
                    for uk in unique_keys:
                        mask = tag_key == uk
                        tag_values = (uk,) if not isinstance(uk, tuple) else uk
                        self._register_tag_group(
                            table_name, tag_columns, tag_values,
                            field_columns, all_times[mask]
                        )
                else:
                    # Multiple tag columns: use structured approach
                    # Convert to list of tuples for grouping
                    n = len(all_times)
                    tag_tuples = [
                        tuple(all_tags[tc][i] for tc in tag_columns)
                        for i in range(n)
                    ]
                    unique_tuples = list(dict.fromkeys(tag_tuples))
                    for ut in unique_tuples:
                        mask = np.array([t == ut for t in tag_tuples], dtype=bool)
                        self._register_tag_group(
                            table_name, tag_columns, ut,
                            field_columns, all_times[mask]
                        )
            else:
                # No TAG columns: single group
                self._register_tag_group(
                    table_name, tag_columns, (),
                    field_columns, all_times
                )

        if self.show_progress and total_rows > 0:
            sys.stderr.write(
                f"\rReading TsFile metadata: "
                f"{len(table_names)} table(s), "
                f"{total_rows:,} rows, "
                f"{len(self.series_paths)} series "
                f"... done\n"
            )
            sys.stderr.flush()

        if not self.series_paths:
            raise ValueError("No valid numeric series found in TsFile")

    def _register_tag_group(
        self, table_name: str, tag_columns: List[str],
        tag_values: tuple, field_columns: List[str], timestamps: np.ndarray
    ):
        """Register all field series for a given table + tag group."""
        timestamps = np.sort(timestamps)

        if len(timestamps) == 0:
            return

        if tag_columns:
            tag_part = ".".join(str(v) for v in tag_values)
        else:
            tag_part = ""

        tag_values_dict = dict(zip(tag_columns, tag_values)) if tag_columns else {}

        for field_col in field_columns:
            if tag_part:
                series_path = f"{table_name}.{tag_part}.{field_col}"
            else:
                series_path = f"{table_name}.{field_col}"

            self.series_paths.append(series_path)
            self._timestamps_cache[series_path] = timestamps
            self.series_info[series_path] = {
                'length': len(timestamps),
                'min_time': int(timestamps[0]),
                'max_time': int(timestamps[-1]),
                'table_name': table_name,
                'column_name': field_col,
                'tag_columns': tag_columns,
                'tag_values': tag_values_dict,
            }

    def get_all_series(self) -> List[str]:
        """Return a list of all discovered series paths."""
        return self.series_paths.copy()

    def get_series_length(self, series_path: str) -> int:
        """Return the number of data points for a series."""
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")
        return self.series_info[series_path]['length']

    def read_series(self, series_path: str) -> List[float]:
        """Read all data points for a series.

        Args:
            series_path: Time series path.

        Returns:
            List of data points.
        """
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")
        if series_path in self._series_data_cache:
            return self._series_data_cache[series_path].tolist()
        length = self.series_info[series_path]['length']
        return self.read_series_range(series_path, 0, length)

    def read_series_range(self, series_path: str, start: int, end: int) -> List[float]:
        """Read specified range of time series by row index.

        Args:
            series_path: Time series path.
            start: Start index (inclusive).
            end: End index (exclusive).

        Returns:
            List of data points.
        """
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")

        if series_path in self._series_data_cache:
            return self._series_data_cache[series_path][start:end].tolist()

        info = self.series_info[series_path]
        timestamps = self._timestamps_cache[series_path]

        start_time = int(timestamps[start])
        end_time = int(timestamps[end - 1])

        _, vals = self._read_arrow(
            info['table_name'],
            [info['column_name']],
            info['tag_columns'],
            info['tag_values'],
            start_time, end_time,
        )
        return vals[info['column_name']].tolist()

    def read_series_by_time_range(
        self, series_path: str, start_time: int, end_time: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Read data by time range directly (for loc-style queries).

        Args:
            series_path: Time series path.
            start_time: Start timestamp (inclusive, ms).
            end_time: End timestamp (inclusive, ms).

        Returns:
            Tuple of (timestamps_array, values_array).
        """
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")

        info = self.series_info[series_path]
        ts_arr, field_vals = self._read_arrow(
            info['table_name'],
            [info['column_name']],
            info['tag_columns'],
            info['tag_values'],
            start_time, end_time,
        )
        if len(ts_arr) > 0:
            return ts_arr, field_vals[info['column_name']]
        return np.array([], dtype=np.int64), np.array([], dtype=np.float64)

    def read_multi_series_by_time_range(
        self,
        table_name: str,
        field_columns: List[str],
        tag_columns: List[str],
        tag_values: Dict[str, str],
        start_time: int,
        end_time: int,
    ) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
        """Read multiple field columns from the same table+tag group in one query.

        Args:
            table_name: TsFile table name.
            field_columns: List of field column names to read.
            tag_columns: List of tag column names.
            tag_values: Dict of tag column name to tag value.
            start_time: Start timestamp (inclusive, ms).
            end_time: End timestamp (inclusive, ms).

        Returns:
            (timestamps_array, {field_name: values_array}).
        """
        return self._read_arrow(
            table_name, field_columns, tag_columns, tag_values,
            start_time, end_time,
        )

    def _read_arrow(
        self,
        table_name: str,
        field_columns: List[str],
        tag_columns: List[str],
        tag_values: Dict[str, str],
        start_time: int,
        end_time: int,
    ) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
        """Core Arrow batch reader.

        Read one or more field columns from a single table+tag group
        via query_table_batch + read_arrow_batch.

        Args:
            table_name: TsFile table name.
            field_columns: Field columns to read.
            tag_columns: Tag column names (for query).
            tag_values: Tag filter values.
            start_time: Start timestamp (inclusive, ms).
            end_time: End timestamp (inclusive, ms).

        Returns:
            (timestamps_array, {field_name: values_array}).
        """
        if tag_columns:
            query_cols = tag_columns + field_columns
        else:
            query_cols = list(field_columns)

        ts_list = []
        field_lists = {fc: [] for fc in field_columns}

        with self._reader.query_table_batch(
            table_name, query_cols,
            start_time=start_time, end_time=end_time, batch_size=65536
        ) as rs:
            while True:
                arrow_table = rs.read_arrow_batch()
                if arrow_table is None:
                    break

                if tag_values:
                    mask = None
                    for tag_col, tag_val in tag_values.items():
                        col_mask = pc.equal(arrow_table.column(tag_col), tag_val)
                        mask = col_mask if mask is None else pc.and_(mask, col_mask)
                    arrow_table = arrow_table.filter(mask)

                if arrow_table.num_rows > 0:
                    ts_list.append(arrow_table.column('time').to_numpy())
                    for fc in field_columns:
                        field_lists[fc].append(
                            arrow_table.column(fc).to_numpy().astype(np.float64)
                        )

        if ts_list:
            return (
                np.concatenate(ts_list).astype(np.int64),
                {fc: np.concatenate(field_lists[fc]) for fc in field_columns},
            )
        return (
            np.array([], dtype=np.int64),
            {fc: np.array([], dtype=np.float64) for fc in field_columns},
        )

    def cache_series_data(self, series_path: str):
        """Pre-load series data into memory cache.

        Args:
            series_path: Time series path.
        """
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")
        if series_path not in self._series_data_cache:
            data = self.read_series(series_path)
            self._series_data_cache[series_path] = np.array(data, dtype=np.float32)

    def is_series_cached(self, series_path: str) -> bool:
        """Check if a series has its data pre-loaded in cache.

        Args:
            series_path: Time series path.

        Returns:
            True if the series data is cached.
        """
        return series_path in self._series_data_cache
