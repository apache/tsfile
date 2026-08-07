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
import copy
from dataclasses import dataclass, field
import heapq
import json
import os
import sys
import tempfile
from typing import Any, Dict, List, Optional, Tuple, Union
import warnings

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

_COVARIATE_ROLE = "C"
_TARGET_ROLE = "T"
_DESCRIPTION_ROLES = {_COVARIATE_ROLE, _TARGET_ROLE}


def _load_dataframe_description(description_path: Optional[str]) -> dict:
    """Load a dataframe description JSON object.

    A missing optional description file is treated as an empty description so
    callers can create it later through :meth:`TsFileDataFrame.set`.
    """
    if description_path is None:
        return {}

    try:
        with open(description_path, "r", encoding="utf-8") as description_file:
            description = json.load(description_file)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid dataframe description JSON in '{description_path}': {exc.msg}"
        ) from exc

    if not isinstance(description, dict):
        raise ValueError(
            f"Dataframe description in '{description_path}' must be a JSON object"
        )
    return description


def _write_dataframe_description(description_path: str, description: dict) -> None:
    """Atomically write a dataframe description JSON object to disk."""
    directory = os.path.dirname(description_path) or "."
    os.makedirs(directory, exist_ok=True)
    fd, temporary_path = tempfile.mkstemp(
        prefix=".tsfile-description-", suffix=".tmp", dir=directory
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as description_file:
            json.dump(description, description_file, ensure_ascii=False, indent=2)
            description_file.write("\n")
        os.replace(temporary_path, description_path)
    except Exception:
        try:
            os.unlink(temporary_path)
        except FileNotFoundError:
            pass
        raise


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
            for reader, device_id, reader_field_idx in self._df._index.series_shards[
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
            if len(ts_arr) == 0:
                continue
            appended_series = set()
            for _, _, field_name, series_name, _, _ in entries:
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
        start_time, end_time, series_refs, series_names = self._parse_key(key)
        timestamps, values = self._query_aligned(
            start_time, end_time, series_refs, series_names
        )
        return AlignedTimeseries(timestamps, values, series_names)


class TsFileDataFrame:
    """Lazy-loaded unified numeric dataset view over multiple TsFile shards.

    ``description_path`` optionally points to a JSON object describing the
    columns in each table.  A description uses the following shape::

        {
          "weather": {
            "temperature": "C",
            "humidity": "T"
          }
        }

    ``C`` marks a covariate column and ``T`` marks a target column.  The
    description is independent of the TsFile data and is shared by dataframe
    subsets created through slicing or boolean selection.
    """

    def __init__(
        self,
        paths: Union[str, List[str]],
        show_progress: bool = True,
        description_path: Optional[Union[str, os.PathLike]] = None,
    ):
        self._paths = _expand_paths(paths)
        self._show_progress = show_progress
        self._description_path = (
            os.path.abspath(os.fspath(description_path))
            if description_path is not None
            else None
        )
        self._description = _load_dataframe_description(self._description_path)
        self._readers: Dict[str, object] = {}
        self._index = _DataFrameCatalog()
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
        obj._description_path = parent._description_path
        obj._description = parent._description
        obj._readers = parent._readers
        # Reuse the parent's full mapping but restrict the membership scope to
        # the requested subset.
        subset_refs = list(series_refs)
        parent_shards = parent._index.series_shards
        subset_shards = {ref: parent_shards[ref] for ref in subset_refs}
        obj._index = _DataFrameCatalog(
            model=parent._index.model,
            table_entries=parent._index.table_entries,
            devices=parent._index.devices,
            device_index=parent._index.device_index,
            device_time_bounds=parent._index.device_time_bounds,
            series=subset_refs,
            series_shards=subset_shards,
        )
        obj._closed = False
        return obj

    def _owner(self) -> "TsFileDataFrame":
        return self._root if self._is_view else self

    def get(self, key: str, default: Any = None) -> Any:
        """Return a top-level value from the JSON description.

        The returned value is copied, so mutating a dictionary or list from a
        ``get`` call cannot change the in-memory description without going
        through :meth:`set`.
        """
        if not isinstance(key, str):
            raise TypeError(f"Description key must be a string, got {type(key)}")
        owner = self._owner()
        return copy.deepcopy(owner._description.get(key, default))

    def set(self, key: str, value: Any) -> None:
        """Set a top-level JSON description value and persist it if configured.

        ``value`` must be JSON serializable.  If no ``description_path`` was
        supplied, the update is kept in memory for the lifetime of the root
        dataframe and can still be read through :meth:`get` or the role APIs.
        """
        if not isinstance(key, str):
            raise TypeError(f"Description key must be a string, got {type(key)}")

        owner = self._owner()
        updated_description = copy.deepcopy(owner._description)
        updated_description[key] = copy.deepcopy(value)

        try:
            json.dumps(updated_description, ensure_ascii=False)
        except (TypeError, ValueError) as exc:
            raise TypeError("Description value must be JSON serializable") from exc

        if owner._description_path is not None:
            _write_dataframe_description(owner._description_path, updated_description)
        owner._description = updated_description

    def _get_role_columns(self, table_name: str, role: str) -> List[str]:
        if not isinstance(table_name, str):
            raise TypeError(
                f"Description table name must be a string, got {type(table_name)}"
            )

        table_description = self._owner()._description.get(table_name)
        if table_description is None:
            return []
        if not isinstance(table_description, dict):
            raise ValueError(
                f"Description for table '{table_name}' must be a JSON object"
            )

        invalid_roles = {
            column: column_role
            for column, column_role in table_description.items()
            if not isinstance(column_role, str) or column_role not in _DESCRIPTION_ROLES
        }
        if invalid_roles:
            column, column_role = next(iter(invalid_roles.items()))
            raise ValueError(
                f"Invalid role {column_role!r} for column '{column}' in table "
                f"'{table_name}'; expected 'C' or 'T'"
            )
        return [
            column
            for column, column_role in table_description.items()
            if column_role == role
        ]

    def get_covariate_columns(self, table_name: str) -> List[str]:
        """Return the columns marked ``C`` for ``table_name``."""
        return self._get_role_columns(table_name, _COVARIATE_ROLE)

    def get_target_columns(self, table_name: str) -> List[str]:
        """Return the columns marked ``T`` for ``table_name``."""
        return self._get_role_columns(table_name, _TARGET_ROLE)

    def get_covariate_column_count(self, table_name: str) -> int:
        """Return the number of columns marked ``C`` for ``table_name``."""
        return len(self.get_covariate_columns(table_name))

    def get_target_column_count(self, table_name: str) -> int:
        """Return the number of columns marked ``T`` for ``table_name``."""
        return len(self.get_target_columns(table_name))

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

        if not self._index.series:
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

    def _build_series_name(self, series_ref: SeriesRefKey) -> SeriesPath:
        device_key, table_entry, field_idx = self._get_series_components(series_ref)
        table_name, tag_values = device_key
        field_name = table_entry.field_columns[field_idx]
        return build_logical_series_path(
            table_name, tag_values, field_name, table_entry.tag_columns
        )

    def _resolve_series_name(self, series_name) -> SeriesRefKey:
        """Resolve a ``SeriesPath`` or path string (``\\N`` = null tag) to a ref.

        Every device has a unique position-preserving key, so this is a single
        direct lookup -- no sparse/compressed fallback and no ambiguity.
        """
        if isinstance(series_name, SeriesPath):
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

        device_key = (table_name, _normalize_tag_values(tag_parts))
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

    def _get_device_node_counts_by_index(self, table_name: str) -> Dict[int, int]:
        """Count unique physical series per device in the current dataframe view."""
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name)}")

        node_indices_by_device: Dict[int, set] = {}
        seen_series = set()
        for device_idx, field_idx in self._index.series:
            device_table_name, _ = self._index.devices[device_idx]
            if device_table_name != table_name:
                continue

            series_ref = (device_idx, field_idx)
            if series_ref in seen_series:
                continue
            seen_series.add(series_ref)
            node_indices_by_device.setdefault(device_idx, set()).add(field_idx)

        return {
            device_idx: len(node_indices)
            for device_idx, node_indices in node_indices_by_device.items()
        }

    def _get_device_statistics_by_index(self, table_name: str) -> Dict[int, dict]:
        """Aggregate physical-series statistics per device in the current view."""
        node_counts_by_device = self._get_device_node_counts_by_index(table_name)
        point_counts_by_device: Dict[int, int] = {}
        value_counts_by_device: Dict[int, int] = {}
        seen_series = set()
        for device_idx, field_idx in self._index.series:
            if device_idx not in node_counts_by_device:
                continue

            series_ref = (device_idx, field_idx)
            if series_ref in seen_series:
                continue
            seen_series.add(series_ref)

            for reader, reader_device_id, reader_field_idx in self._index.series_shards[
                series_ref
            ]:
                series_info = reader.get_series_info_by_ref(
                    reader_device_id, reader_field_idx
                )
                point_counts_by_device[device_idx] = point_counts_by_device.get(
                    device_idx, 0
                ) + int(series_info["timeline_length"])
                value_counts_by_device[device_idx] = value_counts_by_device.get(
                    device_idx, 0
                ) + int(series_info["length"])

        statistics_by_device = {}
        for device_idx, node_count in node_counts_by_device.items():
            if device_idx < len(self._index.device_time_bounds):
                min_time, max_time = self._index.device_time_bounds[device_idx]
            else:
                min_time, max_time = None, None
            statistics_by_device[device_idx] = {
                "node_count": node_count,
                # point_count follows Timeseries.stats['count']: every row on
                # every physically present node, including NaN rows.
                "point_count": point_counts_by_device.get(device_idx, 0),
                # value_count counts only non-null values from native stats.
                "value_count": value_counts_by_device.get(device_idx, 0),
                "start_time": min_time,
                "end_time": max_time,
            }
        return statistics_by_device

    def get_device_count(self, table_name: str) -> int:
        """Return the number of devices represented under ``table_name``.

        A subset dataframe only counts devices that own at least one series in
        that subset. A table that is not present returns ``0``.
        """
        return len(self._get_device_node_counts_by_index(table_name))

    def get_device_node_counts(self, table_name: str) -> Dict[Tuple[Any, ...], int]:
        """Return ``{device_tag_values: node_count}`` for ``table_name``.

        Each key is the device's ordered tag-value tuple. For tree-model files,
        it is the tuple of device path segments after the root. ``node_count``
        is the number of unique, physically present numeric time series for the
        device in the current dataframe view.
        """
        counts_by_index = self._get_device_node_counts_by_index(table_name)
        return {
            self._index.devices[device_idx][1]: counts_by_index[device_idx]
            for device_idx in range(len(self._index.devices))
            if device_idx in counts_by_index
        }

    def get_device_statistics(self, table_name: str) -> Dict[Tuple[Any, ...], dict]:
        """Return per-device counts and time bounds for ``table_name``.

        Each value contains ``node_count``, ``point_count`` (the sum of the
        per-node timeline counts), ``value_count`` (the sum of non-null values),
        ``start_time``, and ``end_time``. The key is the device's ordered
        tag-value tuple.
        """
        statistics_by_index = self._get_device_statistics_by_index(table_name)
        return {
            self._index.devices[device_idx][1]: stats
            for device_idx, stats in statistics_by_index.items()
        }

    def get_device_point_counts(self, table_name: str) -> Dict[Tuple[Any, ...], int]:
        """Return ``{device_tag_values: point_count}`` for ``table_name``."""
        return {
            tag_values: stats["point_count"]
            for tag_values, stats in self.get_device_statistics(table_name).items()
        }

    def _resolve_device_index(self, table_name: str, device) -> int:
        """Resolve a public device selector to a dataframe-local device index."""
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be a string, got {type(table_name)}")
        table_entry = self._index.table_entries.get(table_name)
        if table_entry is None:
            raise KeyError(f"Table not found: '{table_name}'")

        if isinstance(device, SeriesPath):
            if device.table != table_name:
                raise KeyError(
                    f"Device {device!r} does not belong to table '{table_name}'"
                )
            tag_values = device.tags
        elif isinstance(device, dict):
            tag_values = tuple(device.get(column) for column in table_entry.tag_columns)
        elif isinstance(device, (tuple, list)):
            tag_values = tuple(device)
        elif len(table_entry.tag_columns) == 1:
            tag_values = (device,)
        else:
            raise TypeError(
                "A device with multiple tag columns must be identified by an "
                "ordered tuple/list or a tag-value dictionary"
            )

        device_idx = self._index.device_index.get(
            (table_name, _normalize_tag_values(tag_values))
        )
        if device_idx is None:
            raise KeyError(f"Device not found in table '{table_name}': {device!r}")
        if device_idx not in self._get_device_node_counts_by_index(table_name):
            raise KeyError(
                f"Device is not represented in the current dataframe view: {device!r}"
            )
        return device_idx

    def get_device_stats(self, table_name: str, device) -> dict:
        """Return statistics for one device selected by tags or ``SeriesPath``."""
        device_idx = self._resolve_device_index(table_name, device)
        return self._get_device_statistics_by_index(table_name)[device_idx].copy()

    def get_device_point_count(self, table_name: str, device) -> int:
        """Return the timeline point count for one selected device."""
        return self.get_device_stats(table_name, device)["point_count"]

    def list_device_metadata(self, table_name: str):
        """Return a pandas DataFrame with one row and statistics per device.

        Declared tag columns are expanded into named columns. Short tag tuples
        are padded with ``None`` so nullable table tags and tree devices of
        different depths remain position preserving.
        """
        import pandas as pd

        statistics_by_index = self._get_device_statistics_by_index(table_name)
        table_entry = self._index.table_entries.get(table_name)
        tag_columns = list(table_entry.tag_columns) if table_entry else []
        leading_columns = [
            "node_count",
            "point_count",
            "value_count",
            "start_time",
            "end_time",
        ]
        if self._index.model != MODEL_TREE:
            leading_columns.insert(0, "table")
        columns = leading_columns + tag_columns

        rows = []
        for device_idx in range(len(self._index.devices)):
            if device_idx not in statistics_by_index:
                continue
            _, tag_values = self._index.devices[device_idx]
            ordered_tag_values = list(tag_values)
            if len(ordered_tag_values) < len(tag_columns):
                ordered_tag_values.extend(
                    [None] * (len(tag_columns) - len(ordered_tag_values))
                )

            row = statistics_by_index[device_idx].copy()
            if self._index.model != MODEL_TREE:
                row["table"] = table_name
            row.update(zip(tag_columns, ordered_tag_values))
            rows.append(row)

        return pd.DataFrame(rows, columns=columns)

    def list_timeseries(self, path_prefix: str = "") -> List[SeriesPath]:
        if not path_prefix:
            return [
                self._build_series_name(series_ref) for series_ref in self._index.series
            ]

        try:
            prefix_parts = split_logical_series_path(path_prefix)
        except ValueError:
            return []

        matched = []
        for series_ref in self._index.series:
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

    def _get_timeseries(self, series_ref: SeriesRefKey) -> Timeseries:
        self._assert_open()
        series_name = self._build_series_name(series_ref)
        return Timeseries(
            series_name,
            self._index.series_shards[series_ref],
            _build_runtime_series_stats(self._index.series_shards[series_ref]),
            self._assert_open,
            lambda: _merge_field_timestamps(
                series_name, self._index.series_shards[series_ref]
            ),
            lambda offset, limit: _read_field_by_position(
                series_name, self._index.series_shards[series_ref], offset, limit
            ),
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
            return self._get_timeseries(self._index.series[idx])

        if isinstance(key, str):
            try:
                return self._get_timeseries(self._resolve_series_name(key))
            except KeyError:
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
