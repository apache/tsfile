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
import contextlib
from dataclasses import dataclass, field
import heapq
import os
import sys
from types import SimpleNamespace
from typing import Dict, List, Optional, Tuple, Union

import numpy as np

from .formatting import format_dataframe_table
from .metadata import (
    MODEL_TABLE,
    MODEL_TREE,
    SeriesPath,
    TableEntry,
    _normalize_tag_values,
    build_logical_series_components,
    build_logical_series_path,
    split_logical_series_path,
)
from .merge import build_aligned_matrix, merge_time_value_parts, merge_timestamp_parts
from .timeseries import AlignedTimeseries, Timeseries

DeviceKey = Tuple[str, tuple]
SeriesRefKey = Tuple[int, int]
SeriesRef = Tuple[object, int, int]

_QUERY_START = np.iinfo(np.int64).min
_QUERY_END = np.iinfo(np.int64).max
_DATACLASS_SLOTS = {"slots": True} if sys.version_info >= (3, 10) else {}
# Overlap position reads use chunked k-way merge. Keep the default chunk small
# enough to avoid large read amplification for `series[i]` / short slices, but
# large enough to avoid excessive query_by_row round-trips when overlap spans
# multiple shards.
_OVERLAP_ROW_CHUNK_SIZE = 256


@dataclass(**_DATACLASS_SLOTS)
class _DataFrameCatalog:
    """TsFileDataFrame's cross-file unified catalog: merges each tsfile's
    ``MetadataCatalog`` into one user-facing global view.
    """

    # Model kind for the entire load set: "table" or "tree". A single
    # TsFileDataFrame is not allowed to mix table-model and tree-model files.
    model: Optional[str] = None

    # Shared table schema references keyed by table name.
    table_entries: Dict[str, TableEntry] = field(default_factory=dict)

    # Stable logical device order, each item is (table_name, tag_values).
    devices: List[DeviceKey] = field(default_factory=list)
    # Map one logical device key to its dataframe-local device index. The key's
    # tag tuple keeps interior nulls (None) and drops trailing ones, so every
    # device -- including null-tagged ones -- resolves by a single direct lookup.
    device_index: Dict[DeviceKey, int] = field(default_factory=dict)
    # Aggregated (min_time, max_time) per logical device, computed once at
    # registration so query-time lookups are O(1).
    device_time_bounds: List[Tuple[Optional[int], Optional[int]]] = field(
        default_factory=list
    )

    # Stable logical series order, each item is (device_idx, field_idx).
    series: List[SeriesRefKey] = field(default_factory=list)
    # For each logical series: which shards hold it, plus its (device_id,
    # field_idx) coordinates inside each shard.
    series_shards: Dict[SeriesRefKey, List[SeriesRef]] = field(default_factory=dict)


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
    """Reject same-name tables whose complete ordered schema differs."""
    if (
        existing.schema_columns
        and incoming.schema_columns
        and existing.schema_columns == incoming.schema_columns
    ):
        return
    if (
        not existing.schema_columns
        and not incoming.schema_columns
        and existing.tag_columns == incoming.tag_columns
        and existing.tag_types == incoming.tag_types
        and existing.field_columns == incoming.field_columns
        and existing.field_types == incoming.field_types
    ):
        return

    raise ValueError(
        f"Incompatible schema for table '{incoming.table_name}' in '{file_path}'. "
        f"Expected columns={list(existing.schema_columns)} but found "
        f"columns={list(incoming.schema_columns)}."
    )


def _merge_tree_table_entries(existing: TableEntry, incoming: TableEntry) -> TableEntry:
    """Widen two per-file synthetic tree tables into one global table.

    Tree TsFiles carry no authored schema, so each file synthesizes its own
    table (root name, ``_col_i`` tag columns sized to that file's deepest
    device, fields = the measurements present in that file). When several tree
    files are loaded together we widen to their union: field columns are
    unioned (existing order first, then new measurements) and the tag
    columns/types grow to the deepest file's depth so shallower devices pad
    with nulls. Per-(device, field) ownership stays exact because each shard is
    keyed by the global field index resolved by measurement name.
    """
    if len(incoming.tag_columns) > len(existing.tag_columns):
        tag_columns, tag_types = incoming.tag_columns, incoming.tag_types
    else:
        tag_columns, tag_types = existing.tag_columns, existing.tag_types

    field_columns = list(existing.field_columns)
    seen = set(field_columns)
    for name in incoming.field_columns:
        if name not in seen:
            seen.add(name)
            field_columns.append(name)

    return TableEntry(
        table_name=existing.table_name,
        tag_columns=tag_columns,
        tag_types=tag_types,
        field_columns=tuple(field_columns),
        field_types=(),
        schema_columns=(),
    )


def _register_reader(
    readers: Dict[str, object],
    index: _DataFrameCatalog,
    file_path: str,
    reader,
) -> None:
    """Merge one reader's catalog into the dataframe-wide logical index."""
    cur_tsfile_model = reader.model_kind
    if index.model is None:
        index.model = cur_tsfile_model
    elif index.model != cur_tsfile_model:
        raise ValueError(
            f"Mixed table-model and tree-model TsFiles detected. The first "
            f"loaded file is {index.model!r} but '{file_path}' is "
            f"{cur_tsfile_model!r}. A single TsFileDataFrame load set must be "
            f"entirely table-model or entirely tree-model."
        )

    readers[file_path] = reader
    catalog = reader.catalog

    for table_entry in catalog.table_entries:
        existing_entry = index.table_entries.get(table_entry.table_name)
        if existing_entry is None:
            index.table_entries[table_entry.table_name] = table_entry
        elif index.model == MODEL_TREE:
            # Tree shards carry no authored schema; widen the synthetic table to
            # the union of fields and the deepest tag layout across files
            # instead of rejecting differing subsets/depths.
            index.table_entries[table_entry.table_name] = _merge_tree_table_entries(
                existing_entry, table_entry
            )
        else:
            _validate_table_schema(existing_entry, table_entry, file_path)

    for device_id, device_entry in enumerate(catalog.device_entries):
        table_entry = catalog.table_entries[device_entry.table_id]
        device_key = (table_entry.table_name, tuple(device_entry.tag_values))
        device_idx = index.device_index.get(device_key)
        if device_idx is None:
            device_idx = len(index.devices)
            index.device_index[device_key] = device_idx
            index.devices.append(device_key)
            index.device_time_bounds.append(
                (device_entry.min_time, device_entry.max_time)
            )
        else:
            cur_min, cur_max = index.device_time_bounds[device_idx]
            new_min = (
                device_entry.min_time
                if cur_min is None
                else min(cur_min, device_entry.min_time)
            )
            new_max = (
                device_entry.max_time
                if cur_max is None
                else max(cur_max, device_entry.max_time)
            )
            index.device_time_bounds[device_idx] = (new_min, new_max)

    # Register every (device, field) pair the reader physically holds. The
    # logical series is keyed by the GLOBAL field index (resolved by measurement
    # name against the merged table), while the per-shard tuple keeps the
    # reader-local field index for the read path. This lets tree shards with
    # different field subsets/orders merge without mis-mapping measurements
    # (for table model the global and local indices always coincide).
    for device_id, field_idx in catalog.series_stats_by_ref:
        device_entry = catalog.device_entries[device_id]
        reader_table_entry = catalog.table_entries[device_entry.table_id]
        field_name = reader_table_entry.field_columns[field_idx]
        device_key = (reader_table_entry.table_name, tuple(device_entry.tag_values))
        device_idx = index.device_index[device_key]
        global_field_idx = index.table_entries[
            reader_table_entry.table_name
        ].get_field_index(field_name)
        series_ref = (device_idx, global_field_idx)
        if series_ref not in index.series_shards:
            index.series.append(series_ref)
            index.series_shards[series_ref] = []
        index.series_shards[series_ref].append((reader, device_id, field_idx))


def _validate_unique_shard_timestamps(index: _DataFrameCatalog) -> None:
    """Reject overlapping shards that contain the same logical timestamp."""
    validated_timeline_pairs = set()
    for series_ref in index.series:
        fragments = []
        for reader, device_id, field_idx in index.series_shards[series_ref]:
            stats = reader.catalog.series_stats_by_ref[(device_id, field_idx)]
            timeline_identity = (
                reader.file_path,
                device_id,
                (
                    stats.time_metadata_offset
                    if stats.layout
                    else stats.value_metadata_offset
                ),
                (
                    stats.time_metadata_length
                    if stats.layout
                    else stats.value_metadata_length
                ),
            )
            fragments.append(
                (
                    stats.timeline_min_time,
                    stats.timeline_max_time,
                    timeline_identity,
                    reader,
                    device_id,
                    field_idx,
                )
            )
        fragments.sort(key=lambda item: (item[0], item[1], item[2]))
        for right_index, right in enumerate(fragments):
            for left in fragments[:right_index]:
                if left[1] < right[0]:
                    continue
                overlap_start = max(left[0], right[0])
                overlap_end = min(left[1], right[1])
                if overlap_start > overlap_end:
                    continue
                pair = tuple(sorted((left[2], right[2])))
                if pair in validated_timeline_pairs:
                    continue
                left_times, _ = left[3].read_series_by_ref(
                    left[4], left[5], overlap_start, overlap_end
                )
                right_times, _ = right[3].read_series_by_ref(
                    right[4], right[5], overlap_start, overlap_end
                )
                duplicate = np.intersect1d(left_times, right_times, assume_unique=True)
                if len(duplicate):
                    raise ValueError(
                        f"Duplicate timestamp {int(duplicate[0])} found across "
                        "TsFile shards while building the Dataset Index."
                    )
                validated_timeline_pairs.add(pair)


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
    cached_infos=None,
    cached_locator_ids=None,
) -> Tuple[np.ndarray, np.ndarray]:
    """Read one logical series by global position without materializing timestamps for non-overlapping shards."""
    if limit <= 0:
        return np.array([], dtype=np.int64), np.array([], dtype=np.float64)

    if cached_infos is None:
        infos = []
        for reader, device_id, field_idx in refs:
            series_info = reader.get_series_info_by_ref(device_id, field_idx)
            infos.append(
                {
                    "length": series_info["timeline_length"],
                    "min_time": series_info["timeline_min_time"],
                    "max_time": series_info["timeline_max_time"],
                }
            )
    else:
        infos = cached_infos
    if cached_locator_ids is None:
        locator_ids = [None] * len(refs)
    else:
        if len(cached_locator_ids) != len(refs):
            raise ValueError("cached locator count does not match series refs")
        locator_ids = cached_locator_ids
    ordered = sorted(
        zip(refs, infos, locator_ids),
        key=lambda item: (item[1]["min_time"], item[1]["max_time"]),
    )
    if _has_time_range_overlap([info for _, info, _ in ordered]):
        return _read_field_by_position_overlap(series_name, ordered, offset, limit)

    remaining_offset = offset
    remaining_limit = limit
    time_parts = []
    value_parts = []
    for (reader, device_id, field_idx), info, locator_id in ordered:
        shard_count = info["length"]
        if remaining_offset >= shard_count:
            remaining_offset -= shard_count
            continue
        local_limit = min(remaining_limit, shard_count - remaining_offset)
        ts_arr, values = _read_shard_by_position(
            reader,
            device_id,
            field_idx,
            locator_id,
            remaining_offset,
            local_limit,
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


def _read_shard_by_position(
    reader, device_id, field_idx, locator_id, offset, limit
) -> Tuple[np.ndarray, np.ndarray]:
    if locator_id is not None:
        read_at_locator = getattr(reader, "read_series_by_row_at_locator", None)
        if read_at_locator is not None:
            return read_at_locator(locator_id, offset, limit)
    return reader.read_series_by_row(device_id, field_idx, offset, limit)


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
    ordered: List[Tuple[SeriesRef, dict, Optional[int]]],
    offset: int,
    limit: int,
) -> Tuple[np.ndarray, np.ndarray]:
    """Merge overlapping shard streams lazily until the requested global window is covered."""
    total_count = sum(info["length"] for _, info, _ in ordered)
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
            ts_arr, val_arr = _read_shard_by_position(
                reader,
                device_id,
                field_idx,
                state["locator_id"],
                state["next_offset"],
                local_limit,
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

    for ref, info, locator_id in ordered:
        state_idx = len(states)
        states.append(
            {
                "ref": ref,
                "locator_id": locator_id,
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
                    idx += len(self._df._index.series)
                if idx < 0 or idx >= len(self._df._index.series):
                    raise IndexError(f"Series index {item} out of range")
                series_ref = self._df._index.series[idx]
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
            min_time_dev, max_time_dev = self._df._index.device_time_bounds[device_idx]
            if (
                max_time_dev is None
                or max_time_dev < start_time
                or (min_time_dev is not None and min_time_dev > end_time)
            ):
                continue

            _, table_entry, _ = self._df._get_series_components(series_ref)
            field_name = table_entry.field_columns[field_idx]
            descriptor = self._df._index.series_shards.describe(series_ref)
            for shard in descriptor.shards:
                overlap_start = max(start_time, shard.min_time)
                overlap_end = min(end_time, shard.max_time)
                if shard.timeline_length <= 0 or overlap_start > overlap_end:
                    continue
                if overlap_start <= shard.min_time and overlap_end >= shard.max_time:
                    estimated_rows = shard.timeline_length
                elif shard.min_time == shard.max_time:
                    estimated_rows = 1
                else:
                    estimated_rows = max(
                        1,
                        min(
                            shard.timeline_length,
                            int(
                                shard.timeline_length
                                * (overlap_end - overlap_start + 1)
                                / (shard.max_time - shard.min_time + 1)
                            ),
                        ),
                    )
                reader = shard.reader
                device_id = shard.device_id
                reader_field_idx = shard.column_id
                groups[(id(reader), device_id)].append(
                    (
                        col_idx,
                        reader_field_idx,
                        field_name,
                        series_names[col_idx],
                        reader,
                        device_id,
                        estimated_rows,
                    )
                )

        def query_group(entries):
            reader = entries[0][4]
            device_id = entries[0][5]
            field_indices = list(dict.fromkeys(entry[1] for entry in entries))
            ts_arr, field_vals = reader.read_device_fields_by_time_range(
                device_id, field_indices, start_time, end_time
            )
            return entries, ts_arr, field_vals

        group_entries = list(groups.values())
        group_results = self._df._runtime.map_query_groups(
            query_group,
            group_entries,
            estimated_rows=[
                max(entry[6] for entry in entries) for entries in group_entries
            ],
        )

        series_time_parts = defaultdict(list)
        series_value_parts = defaultdict(list)
        for entries, ts_arr, field_vals in group_results:
            if len(ts_arr) == 0:
                continue
            appended_series = set()
            for _, _, field_name, series_name, _, _, _ in entries:
                if series_name in appended_series:
                    continue
                appended_series.add(series_name)
                series_time_parts[series_name].append(ts_arr)
                series_value_parts[series_name].append(field_vals[field_name])

        series_data = {}
        for name in series_names:
            series_data[name] = merge_time_value_parts(
                series_time_parts[name], series_value_parts[name]
            )

        return build_aligned_matrix(series_names, series_data)

    def __getitem__(self, key) -> AlignedTimeseries:
        with self._df._query_guard():
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
        self._index = _DataFrameCatalog()
        self._is_view = False
        self._root = None
        self._closed = False
        self._runtime = None
        self._runtime_lease = None
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
        subset_refs = list(series_refs)
        obj._index = SimpleNamespace(
            model=parent._index.model,
            table_entries=parent._index.table_entries,
            devices=parent._index.devices,
            device_index=parent._index.device_index,
            device_time_bounds=parent._index.device_time_bounds,
            series=subset_refs,
            series_shards=parent._index.series_shards,
        )
        obj._runtime = parent._runtime
        obj._runtime_lease = (
            parent._runtime_lease.clone() if parent._runtime_lease is not None else None
        )
        obj._readers = parent._readers
        obj._closed = False
        return obj

    def _owner(self) -> "TsFileDataFrame":
        return self

    def _assert_open(self):
        if self._closed:
            raise RuntimeError("Current TsFileDataFrame is closed.")

    @contextlib.contextmanager
    def _query_guard(self):
        self._assert_open()
        if self._runtime_lease is None:
            yield
        else:
            with self._runtime_lease.query_lease():
                yield

    def _load_metadata(self):
        """Map a valid persistent index, or build it once under a file lock."""
        from .reader import TsFileSeriesReader
        from .index import (
            build_index_from_dataframe,
            index_matches_paths,
            index_path_for,
        )
        from .runtime import DatasetRuntime

        index_path = index_path_for(self._paths)
        if not index_matches_paths(index_path, self._paths):
            lock_path = index_path + ".lock"
            os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
            with open(lock_path, "a+b") as lock_file:
                try:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                except ImportError:
                    pass
                if not index_matches_paths(index_path, self._paths):
                    if len(self._paths) >= 2:
                        self._load_metadata_parallel(TsFileSeriesReader)
                    else:
                        self._load_metadata_serial(TsFileSeriesReader)

                    if not self._index.series:
                        raise ValueError(
                            "No valid time series found in the provided TsFile files"
                        )
                    try:
                        _validate_unique_shard_timestamps(self._index)
                        build_index_from_dataframe(self, index_path)
                    finally:
                        for reader in self._readers.values():
                            reader.close()
                        self._readers.clear()

        self._runtime = DatasetRuntime(index_path)
        self._runtime_lease = self._runtime.lease()
        self._index = self._runtime.catalog
        if len(self._index.series) == 0:
            self._runtime_lease.close()
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
        device_key = self._index.devices[device_idx]
        return device_key, self._index.table_entries[device_key[0]], field_idx

    def _build_series_name(
        self, series_ref: SeriesRefKey, series_id: Optional[int] = None
    ) -> SeriesPath:
        device_key, table_entry, field_idx = self._get_series_components(series_ref)
        table_name, tag_values = device_key
        field_name = table_entry.field_columns[field_idx]
        return build_logical_series_path(
            table_name,
            tag_values,
            field_name,
            table_entry.tag_columns,
            index_identity=getattr(self._index, "index_identity", None),
            series_id=series_id,
        )

    def _descriptor_from_series_path(self, series_path: SeriesPath):
        identity = getattr(series_path, "_index_identity", None)
        series_id = getattr(series_path, "_series_id", None)
        resolver = getattr(self._index, "resolve_series_descriptor_by_id", None)
        if (
            identity is None
            or series_id is None
            or resolver is None
            or identity != getattr(self._index, "index_identity", None)
        ):
            return None
        try:
            return resolver(series_id, series_path.table, series_path.field)
        except (IndexError, KeyError, ValueError):
            return None

    def _resolve_series_name(self, series_name) -> SeriesRefKey:
        """Resolve a ``SeriesPath`` or path string (``\\N`` = null tag) to a ref.

        Every device has a unique position-preserving key, so this is a single
        direct lookup -- no sparse/compressed fallback and no ambiguity.
        """
        if isinstance(series_name, SeriesPath):
            descriptor = self._descriptor_from_series_path(series_name)
            if descriptor is not None:
                return descriptor.ref
            table_name, tag_parts, field_name = (
                series_name.table,
                list(series_name.tags),
                series_name.field,
            )
        else:
            try:
                parts = split_logical_series_path(series_name)
            except ValueError as exc:
                raise KeyError(_series_lookup_hint(series_name)) from exc
            if len(parts) < 2:
                raise KeyError(_series_lookup_hint(series_name))
            table_name, field_name, tag_parts = parts[0], parts[-1], parts[1:-1]

        if table_name not in self._index.table_entries:
            raise KeyError(_series_lookup_hint(series_name))
        table_entry = self._index.table_entries[table_name]
        try:
            field_idx = table_entry.get_field_index(field_name)
        except ValueError as exc:
            raise KeyError(_series_lookup_hint(series_name)) from exc

        normalized_tags = _normalize_tag_values(tag_parts)
        resolver = getattr(self._index, "resolve_series_descriptor", None)
        if resolver is not None:
            try:
                return resolver(table_name, normalized_tags, field_name).ref
            except (KeyError, ValueError) as exc:
                raise KeyError(_series_lookup_hint(series_name)) from exc

        device_key = (table_name, normalized_tags)
        device_idx = self._index.device_index.get(device_key)
        if device_idx is None:
            raise KeyError(_series_lookup_hint(series_name))

        series_ref = (device_idx, field_idx)
        if series_ref not in self._index.series_shards:
            raise KeyError(_series_lookup_hint(series_name))
        return series_ref

    def _build_series_info(self, series_ref: SeriesRefKey) -> dict:
        device_idx, field_idx = series_ref
        device_key, table_entry, _ = self._get_series_components(series_ref)
        # Aggregate per-shard timeline stats lazily on demand for this series.
        field_stats = _build_field_stats(self._index.series_shards[series_ref])
        # Pad short tag tuples (tree-model devices whose path is shorter than
        # the synthetic table's max depth) with None so positional access by
        # `_col_i` index always lands on a defined cell.
        tag_values_ordered = list(device_key[1])
        if len(tag_values_ordered) < len(table_entry.tag_columns):
            tag_values_ordered.extend(
                [None] * (len(table_entry.tag_columns) - len(tag_values_ordered))
            )
        return {
            "table_name": table_entry.table_name,
            "field": table_entry.field_columns[field_idx],
            "tag_columns": table_entry.tag_columns,
            "tag_values": dict(zip(table_entry.tag_columns, tag_values_ordered)),
            "tag_values_ordered": tag_values_ordered,
            "min_time": field_stats["min_time"],
            "max_time": field_stats["max_time"],
            "count": field_stats["count"],
        }

    def __len__(self) -> int:
        return len(self._index.series)

    @property
    def model(self) -> str:
        return self._index.model

    def list_timeseries(self, path_prefix: str = "") -> List[SeriesPath]:
        if not path_prefix:
            series = self._index.series
            series_id_at = getattr(series, "series_id", None)
            return [
                self._build_series_name(
                    series[position],
                    None if series_id_at is None else series_id_at(position),
                )
                for position in range(len(series))
            ]

        try:
            prefix_parts = split_logical_series_path(path_prefix)
        except ValueError:
            return []

        matched = []
        series = self._index.series
        series_id_at = getattr(series, "series_id", None)
        for position, series_ref in enumerate(series):
            device_key, table_entry, field_idx = self._get_series_components(series_ref)
            components = build_logical_series_components(
                table_entry.table_name,
                device_key[1],
                table_entry.field_columns[field_idx],
                table_entry.tag_columns,
            )
            if prefix_parts == components[: len(prefix_parts)]:
                matched.append(
                    self._build_series_name(
                        series_ref,
                        None if series_id_at is None else series_id_at(position),
                    )
                )
        return matched

    def list_timeseries_metadata(self, path_prefix: str = ""):
        """Return a pandas DataFrame of per-series metadata.

        The returned frame is indexed by the logical series name and includes
        per-series ``field``, time-bound (start/end) statistics, observation
        ``count``, and the per-device tag values (named ``_col_1``, ``_col_2``,
        ... in tree mode, or by their declared tag-column names in table
        mode). Time bounds are exposed as ``pandas.Timestamp`` for ergonomic
        comparison; ``count`` is an integer.

        ``path_prefix`` filters by the same logical-path prefix semantics as
        ``list_timeseries`` (no prefix returns the full catalog).
        """
        import pandas as pd

        # Reuse list_timeseries to apply prefix filtering, then map names back
        # to the underlying series_ref (this respects view subsetting too).
        names = self.list_timeseries(path_prefix)

        rows = []
        for series_name in names:
            series_ref = self._resolve_series_name(series_name)
            info = self._build_series_info(series_ref)
            row = {
                "field": info["field"],
                "start_time": pd.to_datetime(info["min_time"], unit="ms"),
                "end_time": pd.to_datetime(info["max_time"], unit="ms"),
                "count": int(info["count"]),
            }
            if self._index.model != MODEL_TREE:
                row["table"] = info["table_name"]
            tag_columns = info["tag_columns"]
            tag_values_ordered = info["tag_values_ordered"]
            for column, value in zip(tag_columns, tag_values_ordered):
                row[column] = value
            rows.append((series_name, row))

        if not rows:
            columns = ["field", "start_time", "end_time", "count"]
            if self._index.model != MODEL_TREE:
                columns.insert(0, "table")
            columns.extend(self._collect_tag_columns())
            return pd.DataFrame(columns=columns)

        index = [name for name, _ in rows]
        data = [row for _, row in rows]
        df = pd.DataFrame(data, index=index)

        # Stable, predictable column order: leading bookkeeping, then tags.
        leading = ["field", "start_time", "end_time", "count"]
        if self._index.model != MODEL_TREE:
            leading.insert(0, "table")
        tag_order = list(self._collect_tag_columns())
        ordered_columns = leading + [c for c in tag_order if c in df.columns]
        # Preserve any extra columns at the end (defensive against schema drift).
        for extra in df.columns:
            if extra not in ordered_columns:
                ordered_columns.append(extra)
        return df.reindex(columns=ordered_columns)

    def _get_timeseries(
        self,
        series_ref: SeriesRefKey,
        descriptor=None,
        series_name: Optional[SeriesPath] = None,
    ) -> Timeseries:
        self._assert_open()
        if series_name is None:
            series_name = self._build_series_name(series_ref)
        if descriptor is None:
            describe = getattr(self._index.series_shards, "describe", None)
            if describe is not None:
                descriptor = describe(series_ref)
        if descriptor is None:
            refs = self._index.series_shards[series_ref]
            stats = _build_runtime_series_stats(refs)
            cached_infos = None
            cached_locator_ids = None
        else:
            refs = list(descriptor.refs)
            stats = {
                "min_time": descriptor.min_time,
                "max_time": descriptor.max_time,
                "count": descriptor.count,
            }
            cached_infos = tuple(
                {
                    "length": shard.timeline_length,
                    "min_time": shard.min_time,
                    "max_time": shard.max_time,
                }
                for shard in descriptor.shards
            )
            cached_locator_ids = tuple(shard.locator_id for shard in descriptor.shards)
        runtime_lease = (
            self._runtime_lease.clone() if self._runtime_lease is not None else None
        )
        return Timeseries(
            series_name,
            refs,
            stats,
            self._assert_open if runtime_lease is None else None,
            lambda: _merge_field_timestamps(series_name, refs),
            lambda offset, limit: _read_field_by_position(
                series_name,
                refs,
                offset,
                limit,
                cached_infos,
                cached_locator_ids,
            ),
            runtime_lease=runtime_lease,
        )

    def __getitem__(self, key):
        try:
            import pandas as pd

            if isinstance(key, pd.Series) and key.dtype == bool:
                selected = [self._index.series[idx] for idx in key.index[key]]
                return TsFileDataFrame._from_subset(self, selected)
        except ImportError:
            pass

        if isinstance(key, (int, np.integer)):
            idx = int(key)
            if idx < 0:
                idx += len(self._index.series)
            if idx < 0 or idx >= len(self._index.series):
                raise IndexError(
                    f"Index {idx} out of range [0, {len(self._index.series)})"
                )
            series_ref = self._index.series[idx]
            descriptor = None
            series_id_at = getattr(self._index.series, "series_id", None)
            describe = getattr(self._index.series_shards, "describe", None)
            if series_id_at is not None and describe is not None:
                descriptor = describe(series_ref, series_id=series_id_at(idx))
            return self._get_timeseries(series_ref, descriptor)

        if isinstance(key, SeriesPath):
            descriptor = self._descriptor_from_series_path(key)
            if descriptor is not None:
                return self._get_timeseries(descriptor.ref, descriptor, key)

        if isinstance(key, str):
            try:
                resolver = getattr(self._index, "resolve_series_descriptor", None)
                if resolver is None:
                    return self._get_timeseries(self._resolve_series_name(key))

                parts = split_logical_series_path(key)
                if len(parts) < 2:
                    raise KeyError(key)
                table_name, field_name = parts[0], parts[-1]
                descriptor = resolver(
                    table_name,
                    _normalize_tag_values(parts[1:-1]),
                    field_name,
                )
                return self._get_timeseries(descriptor.ref, descriptor)
            except (KeyError, ValueError):
                pass

            valid_columns = {"field", "start_time", "end_time", "count"}
            if self._index.model != MODEL_TREE:
                valid_columns.add("table")
            valid_columns.update(self._collect_tag_columns())
            if key not in valid_columns:
                raise KeyError(_series_lookup_hint(key))

            import pandas as pd

            values = []
            for series_ref in self._index.series:
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
                    values.append(info["tag_values"].get(key))
            return pd.Series(values, name=key)

        if isinstance(key, slice):
            return TsFileDataFrame._from_subset(
                self,
                [
                    self._index.series[idx]
                    for idx in range(*key.indices(len(self._index.series)))
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
                    idx += len(self._index.series)
                if idx < 0 or idx >= len(self._index.series):
                    raise IndexError(
                        f"Index {item} out of range [0, {len(self._index.series)})"
                    )
                selected.append(self._index.series[idx])
            return TsFileDataFrame._from_subset(self, selected)

        raise TypeError(f"Unsupported key type: {type(key)}")

    @property
    def loc(self):
        return _LocIndexer(self)

    def _collect_tag_columns(self) -> List[str]:
        seen = {}
        for table_name, _ in self._index.devices:
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
            indices = list(range(len(self._index.series)))
        else:
            indices = list(indices)

        preview_indices, truncated, split_index = self._preview_indices(
            indices, max_rows
        )
        is_tree = self._index.model == MODEL_TREE
        rows = []
        for idx in preview_indices:
            series_ref = self._index.series[idx]
            info = self._build_series_info(series_ref)
            row = {
                "index": idx,
                "field": info["field"],
                "start_time": info["min_time"],
                "end_time": info["max_time"],
                "count": info["count"],
            }
            if not is_tree:
                row["table"] = info["table_name"]
            row.update(info["tag_values"])
            rows.append(row)

        return format_dataframe_table(
            rows,
            self._collect_tag_columns(),
            total_count=len(indices),
            truncated=truncated,
            split_index=split_index,
            is_table_model=not is_tree,
        )

    def _repr_header(self) -> str:
        total = len(self._index.series)
        model_marker = self._index.model
        if self._is_view:
            return (
                f"TsFileDataFrame({model_marker} model, {total} time series, "
                f"subset of {len(self._root._index.series)})\n"
            )
        return (
            f"TsFileDataFrame({model_marker} model, {total} time series, "
            f"{len(self._paths)} files)\n"
        )

    def __repr__(self):
        return self._repr_header() + self._format_table()

    def __str__(self):
        return self.__repr__()

    def show(self, max_rows: int = 20):
        print(self._repr_header() + self._format_table(max_rows=max_rows))

    def close(self):
        if self._closed:
            return
        self._closed = True
        if self._runtime_lease is not None:
            self._runtime_lease.close()
        else:
            for reader in self._readers.values():
                reader.close()
            self._readers.clear()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
