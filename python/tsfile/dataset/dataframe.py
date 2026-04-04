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

"""TsFileDataFrame and related top-level dataframe operations."""

from collections import defaultdict
import os
import sys
from typing import Dict, List, Union

import numpy as np

from .formatting import format_dataframe_table, format_timestamp
from .metadata import build_merged_entry, collect_tag_columns, register_reader
from .merge import build_aligned_matrix, merge_time_value_parts
from .timeseries import AlignedTimeseries, Timeseries


def _expand_paths(paths: Union[str, List[str]]) -> List[str]:
    if isinstance(paths, str):
        paths = [paths]

    expanded = []
    for path in paths:
        if os.path.isdir(path):
            tsfiles = sorted(
                os.path.join(root, name)
                for root, _, files in os.walk(path)
                for name in files
                if name.endswith(".tsfile")
            )
            if not tsfiles:
                raise FileNotFoundError(f"No .tsfile files found in directory: {path}")
            expanded.extend(tsfiles)
        else:
            expanded.append(path)

    resolved = []
    for path in expanded:
        if not os.path.exists(path):
            raise FileNotFoundError(f"TsFile not found: {path}")
        resolved.append(os.path.abspath(path))
    return resolved


class _LocIndexer:
    """Implement .loc[start_time:end_time, series_list]."""

    def __init__(self, dataframe: "TsFileDataFrame"):
        self._df = dataframe

    def _parse_key(self, key):
        if not isinstance(key, tuple) or len(key) != 2:
            raise ValueError("loc requires exactly 2 arguments: tsdf.loc[start_time:end_time, series_list]")

        time_slice, series_spec = key
        if isinstance(time_slice, slice):
            start_time = np.iinfo(np.int64).min if time_slice.start is None else time_slice.start
            end_time = np.iinfo(np.int64).max if time_slice.stop is None else time_slice.stop
        elif isinstance(time_slice, (int, np.integer)):
            start_time = end_time = int(time_slice)
        else:
            raise TypeError(f"Time index must be slice or int, got {type(time_slice)}")

        if isinstance(series_spec, (str, int, np.integer)):
            series_spec = [series_spec]

        series_names = []
        for item in series_spec:
            if isinstance(item, (int, np.integer)):
                idx = int(item)
                if idx < 0 or idx >= len(self._df._series_list):
                    raise IndexError(f"Series index {idx} out of range")
                series_names.append(self._df._series_list[idx])
            elif isinstance(item, str):
                if item not in self._df._series_map:
                    raise KeyError(f"Series not found: {item}")
                series_names.append(item)
            else:
                raise TypeError(f"Series specifier must be int or str, got {type(item)}")

        return start_time, end_time, series_names

    def _query_aligned(self, start_time: int, end_time: int, series_names: List[str]):
        groups = defaultdict(list)
        for col_idx, name in enumerate(series_names):
            for reader, info in self._df._series_map[name]:
                if info["max_time"] < start_time or info["min_time"] > end_time:
                    continue
                key = (id(reader), info["table_name"], tuple(sorted(info["tag_values"].items())))
                groups[key].append((col_idx, info["column_name"], name, reader, info))

        series_data = {}
        for entries in groups.values():
            reader = entries[0][3]
            info = entries[0][4]
            field_columns = list(dict.fromkeys(entry[1] for entry in entries))
            ts_arr, field_vals = reader.read_multi_series_by_time_range(
                info["table_name"],
                field_columns,
                info["tag_columns"],
                info["tag_values"],
                start_time,
                end_time,
            )

            for _, field_name, name, _, _ in entries:
                if name in series_data:
                    prev_ts, prev_vals = series_data[name]
                    series_data[name] = merge_time_value_parts(
                        [prev_ts, ts_arr], [prev_vals, field_vals[field_name]]
                    )
                else:
                    series_data[name] = (ts_arr, field_vals[field_name])

        return build_aligned_matrix(series_names, series_data)

    def __getitem__(self, key) -> AlignedTimeseries:
        start_time, end_time, series_names = self._parse_key(key)
        timestamps, values = self._query_aligned(start_time, end_time, series_names)
        return AlignedTimeseries(timestamps, values, series_names)


class TsFileDataFrame:
    """Lazy-loaded unified view over multiple TsFile files."""

    def __init__(self, paths: Union[str, List[str]], show_progress: bool = True):
        self._paths = _expand_paths(paths)
        self._show_progress = show_progress
        self._readers: Dict[str, object] = {}
        self._series_list: List[str] = []
        self._series_map: Dict[str, list] = {}
        self._merged_timestamps: Dict[str, np.ndarray] = {}
        self._merged_info: Dict[str, dict] = {}
        self._name_to_index: Dict[str, int] = {}
        self._is_view = False
        self._root = None
        self._load_metadata()

    @classmethod
    def _from_subset(cls, parent: "TsFileDataFrame", series_names: List[str]) -> "TsFileDataFrame":
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
        obj._name_to_index = {name: idx for idx, name in enumerate(series_names)}
        return obj

    def _load_metadata(self):
        from .reader import TsFileSeriesReader

        if len(self._paths) >= 2:
            self._load_metadata_parallel(TsFileSeriesReader)
        else:
            self._load_metadata_serial(TsFileSeriesReader)

        seen = set()
        for file_path in self._paths:
            reader = self._readers[file_path]
            for series_path in reader.series_paths:
                if series_path in seen:
                    continue
                seen.add(series_path)
                self._series_list.append(series_path)
                self._name_to_index[series_path] = len(self._series_list) - 1
                merged_timestamps, merged_info = build_merged_entry(series_path, self._series_map[series_path])
                self._merged_timestamps[series_path] = merged_timestamps
                self._merged_info[series_path] = merged_info

        if not self._series_list:
            raise ValueError("No valid time series found in the provided TsFile files")

    def _load_metadata_serial(self, reader_class):
        for file_path in self._paths:
            register_reader(
                self._readers,
                self._series_map,
                file_path,
                reader_class(file_path, show_progress=self._show_progress),
            )

    def _load_metadata_parallel(self, reader_class):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def open_file(file_path):
            return file_path, reader_class(file_path, show_progress=False)

        total = len(self._paths)
        with ThreadPoolExecutor(max_workers=min(total, os.cpu_count() or 4)) as executor:
            futures = {executor.submit(open_file, path): path for path in self._paths}
            results = {}
            done = 0
            for future in as_completed(futures):
                file_path, reader = future.result()
                results[file_path] = reader
                done += 1
                if self._show_progress:
                    sys.stderr.write(f"\rLoading TsFile shards: {done}/{total}")
                    sys.stderr.flush()

        if self._show_progress and total > 0:
            total_series = sum(len(reader.series_paths) for reader in results.values())
            sys.stderr.write(f"\rLoading TsFile shards: {total}/{total} ({total_series} series) ... done\n")
            sys.stderr.flush()

        for file_path in self._paths:
            register_reader(self._readers, self._series_map, file_path, results[file_path])

    def __len__(self) -> int:
        return len(self._series_list)

    @property
    def timeseries(self) -> List[dict]:
        rows = []
        for name in self._series_list:
            info = self._merged_info[name]
            row = {
                "table": info["table_name"],
                "field": info["field"],
                "start_time": format_timestamp(info["min_time"]),
                "end_time": format_timestamp(info["max_time"]),
                "count": info["count"],
            }
            for column in info["tag_columns"]:
                row[column] = info["tag_values"].get(column, "")
            rows.append(row)
        return rows

    def list_timeseries(self, path_prefix: str = "") -> List[str]:
        if not path_prefix:
            return list(self._series_list)
        prefix = path_prefix if path_prefix.endswith(".") else path_prefix + "."
        return [name for name in self._series_list if name.startswith(prefix) or name == path_prefix]

    def metadata(self):
        import pandas as pd

        tag_columns = self._collect_tag_columns()
        columns = ["series_path", "table"] + tag_columns + ["field", "start_time", "end_time", "count"]
        rows = []
        for name in self._series_list:
            info = self._merged_info[name]
            row = {
                "series_path": name,
                "table": info["table_name"],
                "field": info["field"],
                "start_time": info["min_time"],
                "end_time": info["max_time"],
                "count": info["count"],
            }
            for tag_column in tag_columns:
                row[tag_column] = info["tag_values"].get(tag_column, "")
            rows.append(row)
        return pd.DataFrame(rows, columns=columns)

    def _get_timeseries(self, name: str) -> Timeseries:
        return Timeseries(name, self._series_map[name], self._merged_timestamps[name])

    def __getitem__(self, key):
        try:
            import pandas as pd

            if isinstance(key, pd.Series) and key.dtype == bool:
                selected = [self._series_list[idx] for idx in key.index[key]]
                return TsFileDataFrame._from_subset(self, selected)
        except ImportError:
            pass

        if isinstance(key, (int, np.integer)):
            idx = int(key)
            if idx < 0:
                idx += len(self._series_list)
            if idx < 0 or idx >= len(self._series_list):
                raise IndexError(f"Index {idx} out of range [0, {len(self._series_list)})")
            return self._get_timeseries(self._series_list[idx])

        if isinstance(key, str):
            if key in self._name_to_index:
                return self._get_timeseries(key)

            valid_columns = {"table", "field", "start_time", "end_time", "count"}
            valid_columns.update(self._collect_tag_columns())
            if key not in valid_columns:
                raise KeyError(f"Series not found: '{key}'. Use df.metadata() to see available metadata columns.")

            import pandas as pd

            if key == "table":
                values = [self._merged_info[name]["table_name"] for name in self._series_list]
            elif key == "field":
                values = [self._merged_info[name]["field"] for name in self._series_list]
            elif key == "start_time":
                values = [self._merged_info[name]["min_time"] for name in self._series_list]
            elif key == "end_time":
                values = [self._merged_info[name]["max_time"] for name in self._series_list]
            elif key == "count":
                values = [self._merged_info[name]["count"] for name in self._series_list]
            else:
                values = [self._merged_info[name]["tag_values"].get(key, "") for name in self._series_list]
            return pd.Series(values, name=key)

        if isinstance(key, slice):
            return TsFileDataFrame._from_subset(
                self, [self._series_list[idx] for idx in range(*key.indices(len(self._series_list)))]
            )

        if isinstance(key, list):
            selected = []
            for item in key:
                if not isinstance(item, (int, np.integer)):
                    raise TypeError(f"List index must contain integers, got {type(item)}")
                idx = int(item)
                if idx < 0:
                    idx += len(self._series_list)
                if idx < 0 or idx >= len(self._series_list):
                    raise IndexError(f"Index {item} out of range [0, {len(self._series_list)})")
                selected.append(self._series_list[idx])
            return TsFileDataFrame._from_subset(self, selected)

        raise TypeError(f"Unsupported key type: {type(key)}")

    @property
    def loc(self):
        return _LocIndexer(self)

    def _collect_tag_columns(self) -> List[str]:
        return collect_tag_columns(self._series_list, self._merged_info)

    def _format_table(self, indices=None, max_rows: int = 20) -> str:
        return format_dataframe_table(
            self._series_list,
            self._merged_info,
            self._collect_tag_columns(),
            indices=indices,
            max_rows=max_rows,
        )

    def __repr__(self):
        total = len(self._series_list)
        if self._is_view:
            header = f"TsFileDataFrame({total} time series, subset of {len(self._root._series_list)})\n"
        else:
            header = f"TsFileDataFrame({total} time series, {len(self._paths)} files)\n"
        return header + self._format_table()

    def __str__(self):
        return self.__repr__()

    def close(self):
        if self._is_view:
            return
        for reader in self._readers.values():
            reader.close()
        self._readers.clear()

    def __del__(self):
        try:
            if not getattr(self, "_is_view", False):
                self.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
