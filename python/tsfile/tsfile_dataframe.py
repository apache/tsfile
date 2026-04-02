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
TsFileDataFrame - Lazy-loaded unified view over multiple TsFile files.

Provides array-like and DataFrame-like access to time series data
stored in TsFile format. Supports:
1. Pre-training: random index access with array-like slicing
2. Post-training: time-aligned multi-series queries via .loc
"""

import os
import sys
from collections import defaultdict
from typing import List, Dict, Union, Optional, Tuple
from datetime import datetime

import numpy as np


def _format_timestamp(ts_ms: int) -> str:
    """Convert millisecond timestamp to human-readable string."""
    try:
        return datetime.fromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
    except (OSError, ValueError):
        return str(ts_ms)


class AlignedTimeseries:
    """
    Time-aligned multi-series query result with timestamps.

    Returned by .loc[...] and df[slice/list]. Supports:
    - result.timestamps -> np.ndarray of ms timestamps
    - result.values -> np.ndarray of shape (rows, cols)
    - result.series_names -> list of series name strings
    - result[i] / result[i, j] -> index into values
    - print(result) -> truncated table (20 rows)
    - result.show() -> full table (no truncation)
    - result.show(50) -> show up to 50 rows
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

    def _build_display(self):
        """Pre-compute string representations for display."""
        n_rows, n_cols = self.values.shape
        ts_strs = [_format_timestamp(int(t)) for t in self.timestamps]
        ts_width = max((len(s) for s in ts_strs), default=0)
        ts_width = max(ts_width, len('timestamp'))

        col_widths = []
        val_strs = []
        for col_idx in range(n_cols):
            col_name = self.series_names[col_idx] if col_idx < len(self.series_names) else f'col_{col_idx}'
            w = len(col_name)
            col_vals = []
            for row_idx in range(n_rows):
                v = self.values[row_idx, col_idx]
                s = 'NaN' if np.isnan(v) else f'{v:.2f}'
                col_vals.append(s)
                w = max(w, len(s))
            val_strs.append(col_vals)
            col_widths.append(w)

        return ts_strs, ts_width, col_widths, val_strs

    def _format_rows(self, ts_strs, ts_width, col_widths, val_strs, max_rows):
        """Format rows with optional truncation."""
        n_rows = len(ts_strs)
        n_cols = len(col_widths)

        header_parts = ['timestamp'.rjust(ts_width)]
        for col_idx in range(n_cols):
            col_name = self.series_names[col_idx] if col_idx < len(self.series_names) else f'col_{col_idx}'
            header_parts.append(col_name.rjust(col_widths[col_idx]))
        lines = ['  '.join(header_parts)]

        if max_rows is None or n_rows <= max_rows:
            show_rows = list(range(n_rows))
        else:
            show_rows = list(range(max_rows))

        for row_idx in show_rows:
            parts = [ts_strs[row_idx].rjust(ts_width)]
            for col_idx in range(n_cols):
                parts.append(val_strs[col_idx][row_idx].rjust(col_widths[col_idx]))
            lines.append('  '.join(parts))

        return f"AlignedTimeseries({n_rows} rows, {n_cols} series)\n" + '\n'.join(lines)

    def __repr__(self):
        n_rows, n_cols = self.values.shape
        if n_rows == 0:
            return f"AlignedTimeseries(0 rows, {n_cols} series)"
        ts_strs, ts_width, col_widths, val_strs = self._build_display()
        return self._format_rows(ts_strs, ts_width, col_widths, val_strs, max_rows=20)

    def show(self, max_rows: Optional[int] = None):
        """Print formatted table with configurable row limit.

        Args:
            max_rows: Maximum rows to display. None for all rows.
        """
        n_rows, n_cols = self.values.shape
        if n_rows == 0:
            print(f"AlignedTimeseries(0 rows, {n_cols} series)")
            return
        ts_strs, ts_width, col_widths, val_strs = self._build_display()
        print(self._format_rows(ts_strs, ts_width, col_widths, val_strs, max_rows))


class Timeseries:
    """
    Single time series abstraction.

    Supports row-based slicing (converting to time-range queries internally),
    stats access, and length queries.

    When a series spans multiple files, data is merged transparently.
    """

    def __init__(self, name: str, readers_and_infos: list, merged_timestamps: np.ndarray):
        """
        Args:
            name: Full series path (e.g., "weather.Beijing.humidity").
            readers_and_infos: List of (reader, series_info) tuples for this series.
            merged_timestamps: Sorted, deduplicated timestamp array across all files.
        """
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
            return {'start_time': None, 'end_time': None, 'count': 0}
        return {
            'start_time': int(self._timestamps[0]),
            'end_time': int(self._timestamps[-1]),
            'count': count,
        }

    def __len__(self) -> int:
        return len(self._timestamps)

    def __getitem__(self, key):
        """Row-based access.

        - series[20] -> single float value
        - series[20:100] -> np.ndarray of values
        """
        length = len(self._timestamps)

        if isinstance(key, int):
            if key < 0:
                key = length + key
            if key < 0 or key >= length:
                raise IndexError(f"Index {key} out of range [0, {length})")
            ts = int(self._timestamps[key])
            _, vals = self._query_time_range(ts, ts)
            return float(vals[0]) if len(vals) > 0 else None

        elif isinstance(key, slice):
            start, stop, step = key.indices(length)
            if start >= stop:
                return np.array([], dtype=np.float64)

            # Get exact timestamps for the requested rows
            requested_ts = self._timestamps[start:stop]
            if len(requested_ts) == 0:
                return np.array([], dtype=np.float64)

            # Query by time range, then filter to exact timestamps
            start_ts = int(requested_ts[0])
            end_ts = int(requested_ts[-1])
            ts_arr, vals = self._query_time_range(start_ts, end_ts)

            # Vectorized alignment: both ts_arr and requested_ts are sorted
            result = np.full(len(requested_ts), np.nan)
            if len(ts_arr) > 0:
                indices = np.searchsorted(ts_arr, requested_ts)
                valid = (indices < len(ts_arr)) & (ts_arr[np.minimum(indices, len(ts_arr) - 1)] == requested_ts)
                result[valid] = vals[indices[valid]]
            vals = result

            if step != 1:
                vals = vals[::step]
            return vals

        else:
            raise TypeError(f"Unsupported key type: {type(key)}")

    def _query_time_range(self, start_time: int, end_time: int) -> Tuple[np.ndarray, np.ndarray]:
        """Query all readers for this series in the given time range, merge results."""
        all_ts = []
        all_vals = []
        for reader, info in self._readers_and_infos:
            # Skip reader if its data doesn't overlap
            if info['max_time'] < start_time or info['min_time'] > end_time:
                continue
            ts_arr, val_arr = reader.read_series_by_time_range(
                self._name, start_time, end_time
            )
            if len(ts_arr) > 0:
                all_ts.append(ts_arr)
                all_vals.append(val_arr)

        if not all_ts:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)

        if len(all_ts) == 1:
            return all_ts[0], all_vals[0]

        # Merge from multiple readers, sort by timestamp, deduplicate
        merged_ts = np.concatenate(all_ts)
        merged_vals = np.concatenate(all_vals)
        sort_idx = np.argsort(merged_ts, kind='mergesort')
        merged_ts = merged_ts[sort_idx]
        merged_vals = merged_vals[sort_idx]

        # Deduplicate by timestamp (keep first occurrence)
        _, unique_idx = np.unique(merged_ts, return_index=True)
        return merged_ts[unique_idx], merged_vals[unique_idx]

    def __repr__(self):
        stats = self.stats
        if stats['count'] == 0:
            return f"Timeseries('{self._name}', count=0)"
        return (
            f"Timeseries('{self._name}', count={stats['count']}, "
            f"start={_format_timestamp(stats['start_time'])}, "
            f"end={_format_timestamp(stats['end_time'])})"
        )


class _LocIndexer:
    """
    Implements .loc[start_time:end_time, series_list] for time-aligned queries.

    Returns AlignedTimeseries with timestamps, values, and series names.
    """

    def __init__(self, dataframe: 'TsFileDataFrame'):
        self._df = dataframe

    def _parse_key(self, key):
        """Parse key into (start_time, end_time, series_names)."""
        if not isinstance(key, tuple) or len(key) != 2:
            raise ValueError(
                "loc requires exactly 2 arguments: "
                "tsdf.loc[start_time:end_time, series_list]"
            )

        time_slice, series_spec = key

        # Parse time range
        if isinstance(time_slice, slice):
            start_time = time_slice.start
            end_time = time_slice.stop
            if start_time is None:
                start_time = np.iinfo(np.int64).min
            if end_time is None:
                end_time = np.iinfo(np.int64).max
        elif isinstance(time_slice, (int, np.integer)):
            start_time = end_time = int(time_slice)
        else:
            raise TypeError(f"Time index must be slice or int, got {type(time_slice)}")

        # Parse series names
        if isinstance(series_spec, (str, int)):
            series_spec = [series_spec]

        series_names = []
        for s in series_spec:
            if isinstance(s, (int, np.integer)):
                idx = int(s)
                if idx < 0 or idx >= len(self._df._series_list):
                    raise IndexError(f"Series index {idx} out of range")
                series_names.append(self._df._series_list[idx])
            elif isinstance(s, str):
                if s not in self._df._series_map:
                    raise KeyError(f"Series not found: {s}")
                series_names.append(s)
            else:
                raise TypeError(f"Series specifier must be int or str, got {type(s)}")

        return start_time, end_time, series_names

    def _query_aligned(self, start_time: int, end_time: int, series_names: List[str]):
        """Query and align multiple series using batched Arrow reads."""
        # Group series by (reader_id, table_name, tag_tuple) for batch queries.
        # Each group can be fetched with a single query_table_batch call.
        groups = defaultdict(list)  # key -> [(col_idx, field_name, series_name, reader, info)]

        for col_idx, name in enumerate(series_names):
            entries = self._df._series_map[name]
            for reader, info in entries:
                if info['max_time'] < start_time or info['min_time'] > end_time:
                    continue
                key = (id(reader), info['table_name'],
                       tuple(sorted(info['tag_values'].items())))
                groups[key].append((col_idx, info['column_name'], name, reader, info))

        # Fetch data: one query per group
        series_data = {}  # series_name -> (ts_arr, val_arr)

        for key, entries in groups.items():
            reader = entries[0][3]
            info = entries[0][4]
            field_columns = list(dict.fromkeys(e[1] for e in entries))  # dedupe, keep order

            ts_arr, field_vals = reader.read_multi_series_by_time_range(
                info['table_name'], field_columns,
                info['tag_columns'], info['tag_values'],
                start_time, end_time,
            )

            for _col_idx, field_name, name, _, _ in entries:
                if name in series_data:
                    # Series spans multiple readers: merge
                    prev_ts, prev_val = series_data[name]
                    merged_ts = np.concatenate([prev_ts, ts_arr])
                    merged_val = np.concatenate([prev_val, field_vals[field_name]])
                    sort_idx = np.argsort(merged_ts, kind='mergesort')
                    merged_ts = merged_ts[sort_idx]
                    merged_val = merged_val[sort_idx]
                    _, unique_idx = np.unique(merged_ts, return_index=True)
                    series_data[name] = (merged_ts[unique_idx], merged_val[unique_idx])
                else:
                    series_data[name] = (ts_arr, field_vals[field_name])

        # Collect unique timestamps using numpy
        all_ts_arrays = [data[0] for data in series_data.values() if len(data[0]) > 0]
        if not all_ts_arrays:
            return (np.array([], dtype=np.int64),
                    np.array([]).reshape(0, len(series_names)))

        sorted_timestamps = np.unique(np.concatenate(all_ts_arrays))

        # Build result matrix using np.searchsorted for vectorized alignment
        result = np.full((len(sorted_timestamps), len(series_names)), np.nan)

        for col_idx, name in enumerate(series_names):
            if name not in series_data:
                continue
            ts_arr, val_arr = series_data[name]
            if len(ts_arr) == 0:
                continue
            indices = np.searchsorted(sorted_timestamps, ts_arr)
            result[indices, col_idx] = val_arr

        return sorted_timestamps, result

    def __getitem__(self, key) -> 'AlignedTimeseries':
        start_time, end_time, series_names = self._parse_key(key)
        timestamps, values = self._query_aligned(start_time, end_time, series_names)
        return AlignedTimeseries(timestamps, values, series_names)


class TsFileDataFrame:
    """
    Lazy-loaded unified view over multiple TsFile files.

    Each dimension is a time series identified by Table + Tags + Field.
    Supports cross-file merging of same-named series.
    """

    def __init__(self, paths: Union[str, List[str]], show_progress: bool = True):
        if isinstance(paths, str):
            paths = [paths]

        # Expand directories: collect all .tsfile files within each directory
        expanded = []
        for p in paths:
            if os.path.isdir(p):
                tsfiles = sorted(
                    os.path.join(root, f)
                    for root, _, files in os.walk(p)
                    for f in files
                    if f.endswith('.tsfile')
                )
                if not tsfiles:
                    raise FileNotFoundError(
                        f"No .tsfile files found in directory: {p}"
                    )
                expanded.extend(tsfiles)
            else:
                expanded.append(p)

        self._paths = []
        for p in expanded:
            if not os.path.exists(p):
                raise FileNotFoundError(f"TsFile not found: {p}")
            self._paths.append(os.path.abspath(p))

        self._show_progress = show_progress
        self._readers = {}
        self._series_list: List[str] = []
        self._series_map: Dict[str, list] = {}  # series_path -> [(reader, info), ...]
        self._merged_timestamps: Dict[str, np.ndarray] = {}
        self._merged_info: Dict[str, dict] = {}
        self._name_to_index: Dict[str, int] = {}

        self._is_view = False
        self._root = None

        self._load_metadata()

    @classmethod
    def _from_subset(cls, parent: 'TsFileDataFrame', series_names: List[str]) -> 'TsFileDataFrame':
        """Create a lightweight view over a subset of series.

        Shares readers, series_map, merged_timestamps, and merged_info
        with the root. Does NOT own any readers and will not close them.
        """
        obj = object.__new__(cls)
        obj._root = parent._root if parent._is_view else parent
        obj._is_view = True
        obj._paths = parent._paths
        obj._readers = parent._readers
        obj._series_map = parent._series_map
        obj._merged_timestamps = parent._merged_timestamps
        obj._merged_info = parent._merged_info
        obj._show_progress = parent._show_progress
        obj._series_list = list(series_names)
        obj._name_to_index = {name: i for i, name in enumerate(series_names)}
        return obj

    def _load_metadata(self):
        """Load metadata from all TsFile files."""
        from .tsfile_series_reader import TsFileSeriesReader

        if len(self._paths) >= 2:
            self._load_metadata_parallel(TsFileSeriesReader)
        else:
            self._load_metadata_serial(TsFileSeriesReader)

        # Build ordered series list and merged metadata
        seen = set()
        for file_path in self._paths:
            reader = self._readers[file_path]
            for series_path in reader.series_paths:
                if series_path not in seen:
                    seen.add(series_path)
                    self._series_list.append(series_path)
                    self._name_to_index[series_path] = len(self._series_list) - 1
                    self._build_merged_info(series_path)

        if not self._series_list:
            raise ValueError("No valid time series found in the provided TsFile files")

    def _load_metadata_serial(self, ReaderClass):
        """Load metadata from files sequentially."""
        for file_path in self._paths:
            reader = ReaderClass(file_path, show_progress=self._show_progress)
            self._register_reader(file_path, reader)

    def _load_metadata_parallel(self, ReaderClass):
        """Load metadata from files in parallel using threads."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def _open_file(file_path):
            return file_path, ReaderClass(file_path, show_progress=False)

        num_workers = min(len(self._paths), os.cpu_count() or 4)
        total = len(self._paths)
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(_open_file, p): p for p in self._paths}
            results = {}
            done_count = 0
            for future in as_completed(futures):
                file_path, reader = future.result()
                results[file_path] = reader
                done_count += 1
                if self._show_progress:
                    sys.stderr.write(
                        f"\rLoading TsFile shards: "
                        f"{done_count}/{total}"
                    )
                    sys.stderr.flush()

        if self._show_progress and total > 0:
            total_series = sum(len(r.series_paths) for r in results.values())
            sys.stderr.write(
                f"\rLoading TsFile shards: "
                f"{total}/{total} "
                f"({total_series} series) "
                f"... done\n"
            )
            sys.stderr.flush()

        # Register in original path order to keep series ordering deterministic
        for file_path in self._paths:
            self._register_reader(file_path, results[file_path])

    def _register_reader(self, file_path: str, reader):
        """Register a reader and index its series."""
        self._readers[file_path] = reader
        for series_path in reader.series_paths:
            if series_path not in self._series_map:
                self._series_map[series_path] = []
            self._series_map[series_path].append(
                (reader, reader.series_info[series_path])
            )

    def _build_merged_info(self, series_path: str):
        """Merge timestamps and metadata from all readers for a given series."""
        entries = self._series_map[series_path]
        # Use first entry for structural metadata (table/tag/field are identical across files)
        _, first_info = entries[0]

        if len(entries) == 1:
            reader, info = entries[0]
            self._merged_timestamps[series_path] = reader._timestamps_cache[series_path]
            self._merged_info[series_path] = {
                'table_name': info['table_name'],
                'tag_columns': info['tag_columns'],
                'tag_values': info['tag_values'],
                'field': info['column_name'],
                'min_time': info['min_time'],
                'max_time': info['max_time'],
                'count': info['length'],
            }
        else:
            all_ts = []
            for reader, info in entries:
                all_ts.append(reader._timestamps_cache[series_path])
            merged = np.unique(np.concatenate(all_ts))
            merged.sort()
            self._merged_timestamps[series_path] = merged
            self._merged_info[series_path] = {
                'table_name': first_info['table_name'],
                'tag_columns': first_info['tag_columns'],
                'tag_values': first_info['tag_values'],
                'field': first_info['column_name'],
                'min_time': int(merged[0]),
                'max_time': int(merged[-1]),
                'count': len(merged),
            }

    def __len__(self) -> int:
        return len(self._series_list)

    @property
    def timeseries(self) -> List[dict]:
        """Return metadata for each series as a list of dicts."""
        result = []
        for name in self._series_list:
            info = self._merged_info[name]
            entry = {
                'table': info['table_name'],
                'field': info['field'],
                'start_time': _format_timestamp(info['min_time']),
                'end_time': _format_timestamp(info['max_time']),
                'count': info['count'],
            }
            for col in info['tag_columns']:
                entry[col] = info['tag_values'].get(col, '')
            result.append(entry)
        return result

    def list_timeseries(self, path_prefix: str = "") -> List[str]:
        """List series paths, optionally filtered by prefix.

        Args:
            path_prefix: If given, only series whose path starts with this prefix.

        Returns:
            List of series path strings.
        """
        if not path_prefix:
            return list(self._series_list)
        prefix = path_prefix if path_prefix.endswith('.') else path_prefix + '.'
        return [name for name in self._series_list
                if name.startswith(prefix) or name == path_prefix]

    def metadata(self):
        """Return metadata for all time series as a pandas DataFrame.

        Each row represents one time series. Columns are:
          - series_path : full dotted path (table[.tag_values...].field)
          - table       : TsFile table name
          - <tag_col>   : one column per tag column, filled with the tag value
                          (empty string if this series has no such tag)
          - field       : measurement / field column name
          - start_time  : earliest timestamp (ms, int)
          - end_time    : latest  timestamp (ms, int)
          - count       : number of data points

        Returns:
            pandas.DataFrame
        """
        import pandas as pd

        tag_cols = self._collect_tag_columns()
        fixed_cols = ['series_path', 'table'] + tag_cols + ['field', 'start_time', 'end_time', 'count']

        rows = []
        for name in self._series_list:
            info = self._merged_info[name]
            row = {
                'series_path': name,
                'table': info['table_name'],
                'field': info['field'],
                'start_time': info['min_time'],
                'end_time': info['max_time'],
                'count': info['count'],
            }
            for tc in tag_cols:
                row[tc] = info['tag_values'].get(tc, '')
            rows.append(row)

        return pd.DataFrame(rows, columns=fixed_cols)

    def _get_timeseries(self, name: str) -> 'Timeseries':
        """Internal helper to create a Timeseries object."""
        return Timeseries(name, self._series_map[name], self._merged_timestamps[name])

    def __getitem__(self, key):
        """Access time series by index, name, slice, or list.

        - tsdf[0] -> Timeseries
        - tsdf['weather.Beijing.humidity'] -> Timeseries
        - tsdf[1:3] -> TsFileDataFrame (subset view)
        - tsdf[[0, 1, 2]] -> TsFileDataFrame (subset view)
        - tsdf[tsdf['start_time'] > xxx] -> TsFileDataFrame (subset view)
        """
        # pandas boolean Series from metadata() filtering
        try:
            import pandas as pd
            if isinstance(key, pd.Series) and key.dtype == bool:
                selected_names = [self._series_list[i] for i in key.index[key]]
                return TsFileDataFrame._from_subset(self, selected_names)
        except ImportError:
            pass

        if isinstance(key, (int, np.integer)):
            key = int(key)
            if key < 0:
                key = len(self._series_list) + key
            if key < 0 or key >= len(self._series_list):
                raise IndexError(f"Index {key} out of range [0, {len(self._series_list)})")
            name = self._series_list[key]
            return self._get_timeseries(name)

        elif isinstance(key, str):
            # If the key matches a series name in this dataframe's subset, return Timeseries.
            # Otherwise treat it as a metadata column name and return a pandas Series,
            # mirroring the behaviour of metadata()[key] so that:
            #   tsdf['ps_id'] == '10'  ->  boolean pd.Series
            #   tsdf[tsdf['ps_id'] == '10']  ->  TsFileDataFrame (subset)
            if key in self._name_to_index:
                return self._get_timeseries(key)
            # Check if it is a valid metadata column
            valid_cols = {'table', 'field', 'start_time', 'end_time', 'count'}
            valid_cols.update(self._collect_tag_columns())
            if key in valid_cols:
                import pandas as pd
                if key == 'table':
                    values = [self._merged_info[n]['table_name'] for n in self._series_list]
                elif key == 'field':
                    values = [self._merged_info[n]['field'] for n in self._series_list]
                elif key == 'start_time':
                    values = [self._merged_info[n]['min_time'] for n in self._series_list]
                elif key == 'end_time':
                    values = [self._merged_info[n]['max_time'] for n in self._series_list]
                elif key == 'count':
                    values = [self._merged_info[n]['count'] for n in self._series_list]
                else:
                    values = [self._merged_info[n]['tag_values'].get(key, '')
                              for n in self._series_list]
                return pd.Series(values, name=key)
            raise KeyError(f"Series not found: '{key}'. "
                           f"Use df.metadata() to see available metadata columns.")

        elif isinstance(key, slice):
            indices = list(range(*key.indices(len(self._series_list))))
            selected_names = [self._series_list[i] for i in indices]
            return TsFileDataFrame._from_subset(self, selected_names)

        elif isinstance(key, list):
            selected_names = []
            for k in key:
                if not isinstance(k, (int, np.integer)):
                    raise TypeError(f"List index must contain integers, got {type(k)}")
                idx = int(k)
                if idx < 0:
                    idx = len(self._series_list) + idx
                if idx < 0 or idx >= len(self._series_list):
                    raise IndexError(f"Index {k} out of range [0, {len(self._series_list)})")
                selected_names.append(self._series_list[idx])
            return TsFileDataFrame._from_subset(self, selected_names)

        else:
            raise TypeError(f"Unsupported key type: {type(key)}")

    @property
    def loc(self):
        """Attribute-style access to the loc indexer."""
        return _LocIndexer(self)

    def _collect_tag_columns(self) -> List[str]:
        """Return ordered list of all unique tag column names across all series."""
        seen = {}
        for name in self._series_list:
            for col in self._merged_info[name]['tag_columns']:
                if col not in seen:
                    seen[col] = True
        return list(seen.keys())

    def _format_table(self, indices=None, max_rows=20) -> str:
        """Format series metadata as an aligned text table."""
        if indices is None:
            indices = range(len(self._series_list))

        indices = list(indices)
        total = len(indices)

        if total > max_rows:
            show_indices = list(indices[:max_rows // 2]) + list(indices[-max_rows // 2:])
            truncated = True
        else:
            show_indices = indices
            truncated = False

        # Determine which tag columns exist across shown rows
        tag_cols = self._collect_tag_columns()

        rows = []
        for idx in show_indices:
            name = self._series_list[idx]
            info = self._merged_info[name]
            row = {
                'index': idx,
                'table': info['table_name'],
                'field': info['field'],
                'start_time': _format_timestamp(info['min_time']),
                'end_time': _format_timestamp(info['max_time']),
                'count': info['count'],
            }
            for tc in tag_cols:
                row[tc] = info['tag_values'].get(tc, '')
            rows.append(row)

        if not rows:
            return "Empty TsFileDataFrame"

        # Build ordered headers: index, table, tag_cols..., field, start_time, end_time, count
        fixed_headers = ['', 'table'] + tag_cols + ['field', 'start_time', 'end_time', 'count']
        col_widths = {h: len(h) for h in fixed_headers}
        col_widths[''] = max(len(str(r['index'])) for r in rows)

        for r in rows:
            col_widths[''] = max(col_widths[''], len(str(r['index'])))
            col_widths['table'] = max(col_widths['table'], len(r['table']))
            col_widths['field'] = max(col_widths['field'], len(r['field']))
            col_widths['start_time'] = max(col_widths['start_time'], len(r['start_time']))
            col_widths['end_time'] = max(col_widths['end_time'], len(r['end_time']))
            col_widths['count'] = max(col_widths['count'], len(str(r['count'])))
            for tc in tag_cols:
                col_widths[tc] = max(col_widths[tc], len(str(r[tc])))

        header_line = "  ".join(h.rjust(col_widths[h]) for h in fixed_headers)
        lines = [header_line]

        half = len(rows) // 2 if truncated else len(rows)
        for i, r in enumerate(rows):
            if truncated and i == half:
                lines.append("...")
            parts = [
                str(r['index']).rjust(col_widths['']),
                r['table'].rjust(col_widths['table']),
            ]
            for tc in tag_cols:
                parts.append(str(r[tc]).rjust(col_widths[tc]))
            parts += [
                r['field'].rjust(col_widths['field']),
                r['start_time'].rjust(col_widths['start_time']),
                r['end_time'].rjust(col_widths['end_time']),
                str(r['count']).rjust(col_widths['count']),
            ]
            lines.append("  ".join(parts))

        return "\n".join(lines)

    def __repr__(self):
        total = len(self._series_list)
        if self._is_view:
            root_total = len(self._root._series_list)
            header = f"TsFileDataFrame({total} time series, subset of {root_total})\n"
        else:
            header = f"TsFileDataFrame({total} time series, {len(self._paths)} files)\n"
        return header + self._format_table()

    def __str__(self):
        return self.__repr__()

    def close(self):
        """Close all underlying readers.

        No-op for subset views (they don't own readers).
        """
        if self._is_view:
            return
        for reader in self._readers.values():
            reader.close()
        self._readers.clear()

    def __del__(self):
        try:
            if not getattr(self, '_is_view', False):
                self.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
