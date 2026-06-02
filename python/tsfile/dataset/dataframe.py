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

"""Top-level dataset accessors for TsFile shards."""

from collections import defaultdict
from dataclasses import dataclass, field
import heapq
import os
import sys
from typing import Dict, List, Set, Tuple, Union
import warnings

import numpy as np

from .formatting import format_dataframe_table
from .metadata import (
    TableEntry,
    build_logical_series_components,
    build_logical_series_path,
    split_logical_series_path,
)
from .merge import build_aligned_matrix, merge_time_value_parts, merge_timestamp_parts
from .timeseries import AlignedTimeseries, Timeseries

DeviceKey = Tuple[str, tuple]
SeriesRefKey = Tuple[int, int]
SeriesRef = Tuple[object, int, int]
DeviceRef = Tuple[object, int]

_QUERY_START = np.iinfo(np.int64).min
_QUERY_END = np.iinfo(np.int64).max
_DATACLASS_SLOTS = {"slots": True} if sys.version_info >= (3, 10) else {}
# Overlap position reads use chunked k-way merge. Keep the default chunk small
# enough to avoid large read amplification for `series[i]` / short slices, but
# large enough to avoid excessive query_by_row round-trips when overlap spans
# multiple shards.
_OVERLAP_ROW_CHUNK_SIZE = 256


@dataclass(**_DATACLASS_SLOTS)
class _LogicalIndex:
    """Cross-reader logical mapping for devices and series."""

    # Shared table schema references keyed by table name.
    table_entries: Dict[str, TableEntry] = field(default_factory=dict)

    # Stable logical device order, each item is (table_name, tag_values).
    device_order: List[DeviceKey] = field(default_factory=list)
    # Map one logical device key to its dataframe-local device index.
    device_index_by_key: Dict[DeviceKey, int] = field(default_factory=dict)
    # Tables that need sparse compressed-path lookup because some devices
    # contain non-trailing missing tag values.
    tables_with_sparse_tag_values: Set[str] = field(default_factory=set)
    # Map one compressed tree-style device path to sparse logical devices only.
    sparse_device_indices_by_compressed_path: Dict[
        Tuple[str, Tuple[str, ...]], List[int]
    ] = field(default_factory=dict)
    # For each logical device, keep the contributing reader-local device refs.
    device_refs: List[List[DeviceRef]] = field(default_factory=list)

    # Stable logical series order, each item is (device_idx, field_idx).
    series_refs_ordered: List[SeriesRefKey] = field(default_factory=list)
    # Map one logical series ref to the contributing reader-local series refs.
    series_ref_map: Dict[SeriesRefKey, List[SeriesRef]] = field(default_factory=dict)
    # Fast membership check for resolved series refs.
    series_ref_set: Set[SeriesRefKey] = field(default_factory=set)


@dataclass(**_DATACLASS_SLOTS)
class _DerivedCache:
    """Merged metadata derived from the logical index."""

    devices: List[dict] = field(default_factory=list)
    field_stats: Dict[SeriesRefKey, dict] = field(default_factory=dict)


def _expand_paths(paths: Union[str, List[str]]) -> List[str]:
    """Normalize file/directory inputs into a validated list of absolute TsFile paths."""
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


def _series_lookup_hint(name: str) -> str:
    return f"Series not found: '{name}'. Use df.list_timeseries() to inspect available series."


def _validate_table_schema(
    existing: TableEntry, incoming: TableEntry, file_path: str
) -> None:
    """Reject same-name tables whose tag/field layout differs across shards."""
    if (
        existing.tag_columns == incoming.tag_columns
        and existing.tag_types == incoming.tag_types
        and existing.field_columns == incoming.field_columns
    ):
        return

    raise ValueError(
        f"Incompatible schema for table '{incoming.table_name}' in '{file_path}'. "
        f"Expected tags={list(existing.tag_columns)}, tag_types={list(existing.tag_types)}, "
        f"fields={list(existing.field_columns)} but found "
        f"tags={list(incoming.tag_columns)}, tag_types={list(incoming.tag_types)}, "
        f"fields={list(incoming.field_columns)}."
    )


def _register_reader(
    readers: Dict[str, object],
    index: _LogicalIndex,
    file_path: str,
    reader,
) -> None:
    """Merge one reader's catalog into the dataframe-wide logical index."""
    readers[file_path] = reader
    catalog = reader.catalog

    for table_entry in catalog.table_entries:
        existing_entry = index.table_entries.get(table_entry.table_name)
        if existing_entry is None:
            index.table_entries[table_entry.table_name] = table_entry
        else:
            _validate_table_schema(existing_entry, table_entry, file_path)

    for device_id, device_entry in enumerate(catalog.device_entries):
        table_entry = catalog.table_entries[device_entry.table_id]
        device_key = (table_entry.table_name, tuple(device_entry.tag_values))
        device_idx = index.device_index_by_key.get(device_key)
        if device_idx is None:
            device_idx = len(index.device_order)
            index.device_index_by_key[device_key] = device_idx
            index.device_order.append(device_key)
            index.device_refs.append([])
            if any(value is None for value in device_entry.tag_values):
                index.tables_with_sparse_tag_values.add(table_entry.table_name)
                compressed_components = tuple(
                    build_logical_series_components(
                        table_entry.table_name,
                        device_entry.tag_values,
                        "",
                        table_entry.tag_columns,
                    )[1:-1]
                )
                compressed_key = (table_entry.table_name, compressed_components)
                index.sparse_device_indices_by_compressed_path.setdefault(
                    compressed_key, []
                ).append(device_idx)
        index.device_refs[device_idx].append((reader, device_id))

        for field_idx in range(len(table_entry.field_columns)):
            series_ref = (device_idx, field_idx)
            if series_ref not in index.series_ref_map:
                index.series_refs_ordered.append(series_ref)
                index.series_ref_map[series_ref] = []
            index.series_ref_map[series_ref].append((reader, device_id, field_idx))


def _build_device_entry(refs: List[DeviceRef]) -> dict:
    """Compute per-device time bounds from cheap metadata only.

    We intentionally do not validate duplicates at the device level because
    table-model fields do not necessarily share one complete timestamp axis.
    Duplicate detection stays on the logical-series paths that materialize or
    merge one field's timestamps.
    """
    infos = [reader.get_device_info(device_id) for reader, device_id in refs]
    min_time = min(info["min_time"] for info in infos)
    max_time = max(info["max_time"] for info in infos)

    return {
        "min_time": min_time,
        "max_time": max_time,
    }


def _build_runtime_series_stats(refs: List[SeriesRef]) -> dict:
    """Build shared-timeline series stats from native timeline metadata."""
    min_time = None
    max_time = None
    count = 0

    for reader, device_id, field_idx in refs:
        info = reader.get_series_info_by_ref(device_id, field_idx)
        shard_min = info["timeline_min_time"]
        shard_max = info["timeline_max_time"]
        shard_count = info["timeline_length"]

        if shard_count == 0:
            continue

        count += shard_count
        min_time = shard_min if min_time is None else min(min_time, shard_min)
        max_time = shard_max if max_time is None else max(max_time, shard_max)

    return {
        "min_time": min_time,
        "max_time": max_time,
        "count": count,
    }


def _merge_field_timestamps(series_name: str, refs: List[SeriesRef]) -> np.ndarray:
    """Load and merge the full timestamp axis for one logical series on demand."""
    # This is intentionally lazy because it is one of the most expensive dataset
    # paths: it reads the full timestamp axis for the logical series across all
    # shards. Today this happens only when callers explicitly ask for
    # `Timeseries.timestamps`.
    time_parts = []
    for reader, device_id, field_idx in refs:
        ts_arr, _ = reader.read_series_by_ref(
            device_id, field_idx, _QUERY_START, _QUERY_END
        )
        if len(ts_arr) > 0:
            time_parts.append(ts_arr)

    if not time_parts:
        merged_timestamps = np.array([], dtype=np.int64)
    elif len(time_parts) == 1:
        merged_timestamps = time_parts[0]
    else:
        try:
            merged_timestamps = merge_timestamp_parts(time_parts, validate_unique=True)
        except ValueError as e:
            message = str(e)
            duplicate_suffix = message.removeprefix("Duplicate timestamp ")
            duplicate_suffix = duplicate_suffix.removesuffix(" found across shards.")
            raise ValueError(
                f"Duplicate timestamp {duplicate_suffix} found for series '{series_name}' across shards. "
                f"Cross-shard duplicate timestamps are not supported."
            ) from e

    return merged_timestamps


def _read_field_by_position(
    series_name: str,
    refs: List[SeriesRef],
    offset: int,
    limit: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """Read one logical series by global position without materializing timestamps for non-overlapping shards."""
    if limit <= 0:
        return np.array([], dtype=np.int64), np.array([], dtype=np.float64)

    infos = []
    for reader, device_id, field_idx in refs:
        series_info = reader.get_series_info_by_ref(device_id, field_idx)
        infos.append(
            {
                "length": series_info["timeline_length"],
                "min_time": series_info["timeline_min_time"],
                "max_time": series_info["timeline_max_time"],
                "table_name": series_info["table_name"],
                "column_name": series_info["column_name"],
                "device_id": series_info["device_id"],
                "field_idx": series_info["field_idx"],
                "tag_columns": series_info["tag_columns"],
                "tag_values": series_info["tag_values"],
            }
        )
    ordered = sorted(
        zip(refs, infos), key=lambda item: (item[1]["min_time"], item[1]["max_time"])
    )
    if _has_time_range_overlap([info for _, info in ordered]):
        return _read_field_by_position_overlap(series_name, ordered, offset, limit)

    remaining_offset = offset
    remaining_limit = limit
    time_parts = []
    value_parts = []
    for (reader, device_id, field_idx), info in ordered:
        shard_count = info["length"]
        if remaining_offset >= shard_count:
            remaining_offset -= shard_count
            continue
        local_limit = min(remaining_limit, shard_count - remaining_offset)
        ts_arr, values = reader.read_series_by_row(
            device_id, field_idx, remaining_offset, local_limit
        )
        if len(ts_arr) > 0:
            time_parts.append(ts_arr)
            value_parts.append(values)
        remaining_limit -= local_limit
        remaining_offset = 0
        if remaining_limit <= 0:
            break

    if not time_parts:
        return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
    return np.concatenate(time_parts), np.concatenate(value_parts)


def _has_time_range_overlap(infos: List[dict]) -> bool:
    previous_max = None
    for info in infos:
        if info["min_time"] is None or info["max_time"] is None:
            continue
        if previous_max is not None and info["min_time"] <= previous_max:
            return True
        previous_max = (
            info["max_time"]
            if previous_max is None
            else max(previous_max, info["max_time"])
        )
    return False


def _read_field_by_position_overlap(
    series_name: str,
    ordered: List[Tuple[SeriesRef, dict]],
    offset: int,
    limit: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """Merge overlapping shard streams lazily until the requested global window is covered."""
    total_count = sum(info["length"] for _, info in ordered)
    if offset >= total_count:
        return np.array([], dtype=np.int64), np.array([], dtype=np.float64)

    chunk_size = max(_OVERLAP_ROW_CHUNK_SIZE, limit)
    states = []
    heap = []

    def fill_state(state_idx: int) -> bool:
        state = states[state_idx]
        while state["buffer_index"] >= len(state["timestamps"]):
            remaining = state["length"] - state["next_offset"]
            if remaining <= 0:
                state["exhausted"] = True
                return False

            local_limit = min(chunk_size, remaining)
            reader, device_id, field_idx = state["ref"]
            ts_arr, val_arr = reader.read_series_by_row(
                device_id, field_idx, state["next_offset"], local_limit
            )
            state["next_offset"] += len(ts_arr)
            state["timestamps"] = ts_arr
            state["values"] = val_arr
            state["buffer_index"] = 0
            if len(ts_arr) > 0:
                return True

            state["exhausted"] = True
            return False
        return True

    for ref, info in ordered:
        state_idx = len(states)
        states.append(
            {
                "ref": ref,
                "length": info["length"],
                "next_offset": 0,
                "timestamps": np.array([], dtype=np.int64),
                "values": np.array([], dtype=np.float64),
                "buffer_index": 0,
                "exhausted": False,
            }
        )
        if fill_state(state_idx):
            heapq.heappush(heap, (int(states[state_idx]["timestamps"][0]), state_idx))

    skipped = 0
    output_timestamps = []
    output_values = []
    last_timestamp = None

    while heap and len(output_timestamps) < limit:
        current_ts, state_idx = heapq.heappop(heap)
        if last_timestamp is not None and current_ts == last_timestamp:
            raise ValueError(
                f"Duplicate timestamp {current_ts} found for series '{series_name}' across shards. "
                f"Cross-shard duplicate timestamps are not supported."
            )

        state = states[state_idx]
        buffer_index = state["buffer_index"]
        current_value = float(state["values"][buffer_index])
        state["buffer_index"] += 1
        if fill_state(state_idx):
            next_ts = int(state["timestamps"][state["buffer_index"]])
            heapq.heappush(heap, (next_ts, state_idx))

        last_timestamp = current_ts
        if skipped < offset:
            skipped += 1
            continue

        output_timestamps.append(current_ts)
        output_values.append(current_value)

    return np.asarray(output_timestamps, dtype=np.int64), np.asarray(
        output_values, dtype=np.float64
    )


def _build_field_stats(refs: List[SeriesRef]) -> dict:
    """Aggregate per-series timeline statistics for dataframe display."""
    min_time = None
    max_time = None
    count = 0

    for reader, device_id, field_idx in refs:
        info = reader.get_series_info_by_ref(device_id, field_idx)
        shard_min = info["timeline_min_time"]
        shard_max = info["timeline_max_time"]
        shard_count = info["timeline_length"]

        if shard_count == 0:
            continue

        count += shard_count
        min_time = shard_min if min_time is None else min(min_time, shard_min)
        max_time = shard_max if max_time is None else max(max_time, shard_max)

    return {
        "min_time": min_time,
        "max_time": max_time,
        "count": count,
    }


class _LocIndexer:
    """Implement ``.loc[start_time:end_time, series_list]`` for aligned reads."""

    def __init__(self, dataframe: "TsFileDataFrame"):
        self._df = dataframe

    def _parse_key(self, key):
        if not isinstance(key, tuple) or len(key) != 2:
            raise ValueError(
                "loc requires exactly 2 arguments: tsdf.loc[start_time:end_time, series_list]"
            )

        time_slice, series_spec = key
        if isinstance(time_slice, slice):
            start_time = _QUERY_START if time_slice.start is None else time_slice.start
            end_time = _QUERY_END if time_slice.stop is None else time_slice.stop
        elif isinstance(time_slice, (int, np.integer)):
            start_time = end_time = int(time_slice)
        else:
            raise TypeError(f"Time index must be slice or int, got {type(time_slice)}")

        if isinstance(series_spec, (str, int, np.integer)):
            series_spec = [series_spec]

        series_refs = []
        series_names = []
        for item in series_spec:
            if isinstance(item, (int, np.integer)):
                idx = int(item)
                if idx < 0:
                    idx += len(self._df._index.series_refs_ordered)
                if idx < 0 or idx >= len(self._df._index.series_refs_ordered):
                    raise IndexError(f"Series index {item} out of range")
                series_ref = self._df._index.series_refs_ordered[idx]
            elif isinstance(item, str):
                series_ref = self._df._resolve_series_name(item)
            else:
                raise TypeError(
                    f"Series specifier must be int or str, got {type(item)}"
                )
            series_refs.append(series_ref)
            series_names.append(self._df._build_series_name(series_ref))

        return start_time, end_time, series_refs, series_names

    def _query_aligned(
        self,
        start_time: int,
        end_time: int,
        series_refs: List[SeriesRefKey],
        series_names: List[str],
    ):
        """Batch aligned reads by reader/device, then merge per-series fragments."""
        self._df._assert_open()
        groups = defaultdict(list)
        for col_idx, series_ref in enumerate(series_refs):
            device_idx, field_idx = series_ref
            device_info = self._df._cache.devices[device_idx]
            if (
                device_info["max_time"] is None
                or device_info["max_time"] < start_time
                or device_info["min_time"] > end_time
            ):
                continue

            _, table_entry, _ = self._df._get_series_components(series_ref)
            field_name = table_entry.field_columns[field_idx]
            for reader, device_id, reader_field_idx in self._df._index.series_ref_map[
                series_ref
            ]:
                groups[(id(reader), device_id)].append(
                    (
                        col_idx,
                        reader_field_idx,
                        field_name,
                        series_names[col_idx],
                        reader,
                        device_id,
                    )
                )

        series_time_parts = defaultdict(list)
        series_value_parts = defaultdict(list)
        for entries in groups.values():
            reader = entries[0][4]
            device_id = entries[0][5]
            field_indices = list(dict.fromkeys(entry[1] for entry in entries))
            ts_arr, field_vals = reader.read_device_fields_by_time_range(
                device_id, field_indices, start_time, end_time
            )
            for _, _, field_name, series_name, _, _ in entries:
                if len(ts_arr) > 0:
                    series_time_parts[series_name].append(ts_arr)
                    series_value_parts[series_name].append(field_vals[field_name])

        series_data = {}
        for name in series_names:
            series_data[name] = merge_time_value_parts(
                series_time_parts[name], series_value_parts[name]
            )

        return build_aligned_matrix(series_names, series_data)

    def __getitem__(self, key) -> AlignedTimeseries:
        start_time, end_time, series_refs, series_names = self._parse_key(key)
        timestamps, values = self._query_aligned(
            start_time, end_time, series_refs, series_names
        )
        return AlignedTimeseries(timestamps, values, series_names)


class TsFileDataFrame:
    """Lazy-loaded unified numeric dataset view over multiple TsFile shards."""

    def __init__(self, paths: Union[str, List[str]], show_progress: bool = True):
        self._paths = _expand_paths(paths)
        self._show_progress = show_progress
        self._readers: Dict[str, object] = {}
        self._index = _LogicalIndex()
        self._cache = _DerivedCache()
        self._is_view = False
        self._root = None
        self._closed = False
        self._load_metadata()

    @classmethod
    def _from_subset(
        cls, parent: "TsFileDataFrame", series_refs: List[SeriesRefKey]
    ) -> "TsFileDataFrame":
        """Create a lightweight view that reuses the parent's readers and caches."""
        obj = object.__new__(cls)
        obj._root = parent._root if parent._is_view else parent
        obj._is_view = True
        obj._paths = parent._paths
        obj._show_progress = parent._show_progress
        obj._readers = parent._readers
        obj._index = _LogicalIndex(
            table_entries=parent._index.table_entries,
            device_order=parent._index.device_order,
            device_index_by_key=parent._index.device_index_by_key,
            device_refs=parent._index.device_refs,
            series_refs_ordered=list(series_refs),
            series_ref_map=parent._index.series_ref_map,
            series_ref_set=set(series_refs),
        )
        obj._cache = _DerivedCache(
            devices=parent._cache.devices, field_stats=parent._cache.field_stats
        )
        obj._closed = False
        return obj

    def _owner(self) -> "TsFileDataFrame":
        return self._root if self._is_view else self

    def _assert_open(self):
        if self._owner()._closed:
            raise RuntimeError("Current TsFileDataFrame is closed.")

    def _load_metadata(self):
        """Build the logical cross-file index and the derived per-series caches."""
        from .reader import TsFileSeriesReader

        if len(self._paths) >= 2:
            self._load_metadata_parallel(TsFileSeriesReader)
        else:
            self._load_metadata_serial(TsFileSeriesReader)

        self._cache.devices = [
            _build_device_entry(refs) for refs in self._index.device_refs
        ]
        for series_ref in self._index.series_refs_ordered:
            self._cache.field_stats[series_ref] = _build_field_stats(
                self._index.series_ref_map[series_ref]
            )

        self._index.series_ref_set = set(self._index.series_refs_ordered)
        if not self._index.series_refs_ordered:
            raise ValueError("No valid time series found in the provided TsFile files")

    def _show_loading_progress(self, done: int, total: int, total_series: int = None):
        if not self._show_progress or total <= 0:
            return

        if total_series is None:
            sys.stderr.write(f"\rLoading TsFile shards: {done}/{total}")
        else:
            sys.stderr.write(
                f"\rLoading TsFile shards: {done}/{total} ({total_series} series) ... done\n"
            )
        sys.stderr.flush()

    def _load_metadata_serial(self, reader_class):
        total = len(self._paths)
        self._show_loading_progress(0, total)

        for index, file_path in enumerate(self._paths, start=1):
            _register_reader(
                self._readers,
                self._index,
                file_path,
                reader_class(
                    file_path, show_progress=self._show_progress and total == 1
                ),
            )
            if total > 1:
                self._show_loading_progress(index, total)

        self._show_loading_progress(
            total, total, sum(reader.series_count for reader in self._readers.values())
        )

    def _load_metadata_parallel(self, reader_class):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def open_file(file_path):
            return file_path, reader_class(file_path, show_progress=False)

        total = len(self._paths)
        self._show_loading_progress(0, total)
        with ThreadPoolExecutor(
            max_workers=min(total, os.cpu_count() or 4)
        ) as executor:
            futures = {executor.submit(open_file, path): path for path in self._paths}
            results = {}
            done = 0
            for future in as_completed(futures):
                file_path, reader = future.result()
                results[file_path] = reader
                done += 1
                self._show_loading_progress(done, total)

        self._show_loading_progress(
            total, total, sum(reader.series_count for reader in results.values())
        )

        for file_path in self._paths:
            _register_reader(
                self._readers,
                self._index,
                file_path,
                results[file_path],
            )

    def _get_series_components(
        self, series_ref: SeriesRefKey
    ) -> Tuple[DeviceKey, TableEntry, int]:
        device_idx, field_idx = series_ref
        device_key = self._index.device_order[device_idx]
        return device_key, self._index.table_entries[device_key[0]], field_idx

    def _build_series_name(self, series_ref: SeriesRefKey) -> str:
        device_key, table_entry, field_idx = self._get_series_components(series_ref)
        table_name, tag_values = device_key
        field_name = table_entry.field_columns[field_idx]
        return build_logical_series_path(
            table_name, tag_values, field_name, table_entry.tag_columns
        )

    def _resolve_series_name(self, series_name: str) -> SeriesRefKey:
        try:
            parts = split_logical_series_path(series_name)
        except ValueError as exc:
            raise KeyError(_series_lookup_hint(series_name)) from exc
        if len(parts) < 2:
            raise KeyError(_series_lookup_hint(series_name))

        table_name = parts[0]
        if table_name not in self._index.table_entries:
            raise KeyError(_series_lookup_hint(series_name))

        table_entry = self._index.table_entries[table_name]
        field_name = parts[-1]
        try:
            field_idx = table_entry.get_field_index(field_name)
        except ValueError as exc:
            raise KeyError(_series_lookup_hint(series_name)) from exc

        tag_parts = parts[1:-1]
        direct_device_idx = self._index.device_index_by_key.get(
            (table_name, tuple(tag_parts))
        )

        if table_name not in self._index.tables_with_sparse_tag_values:
            if direct_device_idx is None:
                raise KeyError(_series_lookup_hint(series_name))
            device_idx = direct_device_idx
        else:
            compressed_key = (table_name, tuple(tag_parts))
            sparse_device_indices = (
                self._index.sparse_device_indices_by_compressed_path.get(
                    compressed_key, []
                )
            )
            candidate_indices = []
            if direct_device_idx is not None:
                candidate_indices.append(direct_device_idx)
            for device_idx in sparse_device_indices:
                if device_idx not in candidate_indices:
                    candidate_indices.append(device_idx)
            if not candidate_indices:
                raise KeyError(_series_lookup_hint(series_name))
            if len(candidate_indices) > 1:
                raise KeyError(f"Ambiguous series path: '{series_name}'.")
            device_idx = candidate_indices[0]

        series_ref = (device_idx, field_idx)
        if series_ref not in self._index.series_ref_set:
            raise KeyError(_series_lookup_hint(series_name))
        return series_ref

    def _build_series_info(self, series_ref: SeriesRefKey) -> dict:
        device_idx, field_idx = series_ref
        device_key, table_entry, _ = self._get_series_components(series_ref)
        field_stats = self._cache.field_stats[series_ref]
        return {
            "table_name": table_entry.table_name,
            "field": table_entry.field_columns[field_idx],
            "tag_columns": table_entry.tag_columns,
            "tag_values": dict(zip(table_entry.tag_columns, device_key[1])),
            "min_time": field_stats["min_time"],
            "max_time": field_stats["max_time"],
            "count": field_stats["count"],
        }

    def __len__(self) -> int:
        return len(self._index.series_refs_ordered)

    def list_timeseries(self, path_prefix: str = "") -> List[str]:
        if not path_prefix:
            return [
                self._build_series_name(series_ref)
                for series_ref in self._index.series_refs_ordered
            ]

        try:
            prefix_parts = split_logical_series_path(path_prefix)
        except ValueError:
            return []

        matched = []
        for series_ref in self._index.series_refs_ordered:
            device_key, table_entry, field_idx = self._get_series_components(series_ref)
            components = build_logical_series_components(
                table_entry.table_name,
                device_key[1],
                table_entry.field_columns[field_idx],
                table_entry.tag_columns,
            )
            if prefix_parts == components[: len(prefix_parts)]:
                matched.append(self._build_series_name(series_ref))
        return matched

    def _get_timeseries(self, series_ref: SeriesRefKey) -> Timeseries:
        self._assert_open()
        series_name = self._build_series_name(series_ref)
        return Timeseries(
            series_name,
            self._index.series_ref_map[series_ref],
            _build_runtime_series_stats(self._index.series_ref_map[series_ref]),
            self._assert_open,
            lambda: _merge_field_timestamps(
                series_name, self._index.series_ref_map[series_ref]
            ),
            lambda offset, limit: _read_field_by_position(
                series_name, self._index.series_ref_map[series_ref], offset, limit
            ),
        )

    def __getitem__(self, key):
        try:
            import pandas as pd

            if isinstance(key, pd.Series) and key.dtype == bool:
                selected = [
                    self._index.series_refs_ordered[idx] for idx in key.index[key]
                ]
                return TsFileDataFrame._from_subset(self, selected)
        except ImportError:
            pass

        if isinstance(key, (int, np.integer)):
            idx = int(key)
            if idx < 0:
                idx += len(self._index.series_refs_ordered)
            if idx < 0 or idx >= len(self._index.series_refs_ordered):
                raise IndexError(
                    f"Index {idx} out of range [0, {len(self._index.series_refs_ordered)})"
                )
            return self._get_timeseries(self._index.series_refs_ordered[idx])

        if isinstance(key, str):
            try:
                return self._get_timeseries(self._resolve_series_name(key))
            except KeyError:
                pass

            valid_columns = {"table", "field", "start_time", "end_time", "count"}
            valid_columns.update(self._collect_tag_columns())
            if key not in valid_columns:
                raise KeyError(_series_lookup_hint(key))

            import pandas as pd

            values = []
            for series_ref in self._index.series_refs_ordered:
                info = self._build_series_info(series_ref)
                if key == "table":
                    values.append(info["table_name"])
                elif key == "field":
                    values.append(info["field"])
                elif key == "start_time":
                    values.append(info["min_time"])
                elif key == "end_time":
                    values.append(info["max_time"])
                elif key == "count":
                    values.append(info["count"])
                else:
                    values.append(info["tag_values"].get(key, ""))
            return pd.Series(values, name=key)

        if isinstance(key, slice):
            return TsFileDataFrame._from_subset(
                self,
                [
                    self._index.series_refs_ordered[idx]
                    for idx in range(*key.indices(len(self._index.series_refs_ordered)))
                ],
            )

        if isinstance(key, list):
            selected = []
            for item in key:
                if not isinstance(item, (int, np.integer)):
                    raise TypeError(
                        f"List index must contain integers, got {type(item)}"
                    )
                idx = int(item)
                if idx < 0:
                    idx += len(self._index.series_refs_ordered)
                if idx < 0 or idx >= len(self._index.series_refs_ordered):
                    raise IndexError(
                        f"Index {item} out of range [0, {len(self._index.series_refs_ordered)})"
                    )
                selected.append(self._index.series_refs_ordered[idx])
            return TsFileDataFrame._from_subset(self, selected)

        raise TypeError(f"Unsupported key type: {type(key)}")

    @property
    def loc(self):
        return _LocIndexer(self)

    def _collect_tag_columns(self) -> List[str]:
        seen = {}
        for table_name, _ in self._index.device_order:
            for column in self._index.table_entries[table_name].tag_columns:
                seen.setdefault(column, True)
        return list(seen.keys())

    @staticmethod
    def _preview_indices(
        indices: List[int], max_rows: int
    ) -> Tuple[List[int], bool, int]:
        total = len(indices)
        if total <= max_rows:
            return indices, False, total

        head = max_rows // 2
        tail = max_rows - head
        return list(indices[:head]) + list(indices[-tail:]), True, head

    def _format_table(self, indices=None, max_rows: int = 20) -> str:
        if indices is None:
            indices = list(range(len(self._index.series_refs_ordered)))
        else:
            indices = list(indices)

        preview_indices, truncated, split_index = self._preview_indices(
            indices, max_rows
        )
        rows = []
        for idx in preview_indices:
            series_ref = self._index.series_refs_ordered[idx]
            info = self._build_series_info(series_ref)
            row = {
                "index": idx,
                "table": info["table_name"],
                "field": info["field"],
                "start_time": info["min_time"],
                "end_time": info["max_time"],
                "count": info["count"],
            }
            row.update(info["tag_values"])
            rows.append(row)

        return format_dataframe_table(
            rows,
            self._collect_tag_columns(),
            total_count=len(indices),
            truncated=truncated,
            split_index=split_index,
        )

    def _repr_header(self) -> str:
        total = len(self._index.series_refs_ordered)
        if self._is_view:
            return f"TsFileDataFrame({total} time series, subset of {len(self._root._index.series_refs_ordered)})\n"
        return f"TsFileDataFrame({total} time series, {len(self._paths)} files)\n"

    def __repr__(self):
        return self._repr_header() + self._format_table()

    def __str__(self):
        return self.__repr__()

    def show(self, max_rows: int = 20):
        print(self._repr_header() + self._format_table(max_rows=max_rows))

    def close(self):
        if self._is_view:
            warnings.warn(
                "close() on a subset TsFileDataFrame is a no-op; only the root dataframe owns the readers.",
                RuntimeWarning,
                stacklevel=2,
            )
            return
        if self._closed:
            return
        for reader in self._readers.values():
            reader.close()
        self._readers.clear()
        self._closed = True

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
