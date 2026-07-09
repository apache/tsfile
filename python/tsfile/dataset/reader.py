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

"""Single-file reader backend used by TsFileDataFrame."""

import os
import sys
from typing import Dict, Iterator, List, Tuple

import numpy as np

from ..constants import ColumnCategory, TSDataType
from ..tag_filter import tag_eq, tag_is_null
from ..tsfile_reader import TsFileReaderPy
from .metadata import (
    MetadataCatalog,
    MODEL_TABLE,
    MODEL_TREE,
    SeriesStats,
    build_series_path,
    resolve_series_path,
)

_NUMERIC_FIELD_TYPES = {
    TSDataType.BOOLEAN,
    TSDataType.INT32,
    TSDataType.INT64,
    TSDataType.FLOAT,
    TSDataType.DOUBLE,
    TSDataType.TIMESTAMP,
}


def _to_python_scalar(value):
    return value.item() if hasattr(value, "item") else value


def _build_exact_tag_filter(tag_values: Dict[str, object]):
    """Build a conjunctive filter that isolates exactly one device.

    A ``None`` tag value matches the device's null/missing tag via IS NULL so
    that devices sharing the same non-null tags (for example a trailing-null
    device versus a fully specified one) are not conflated.
    """
    tag_filter = None
    for tag_column, tag_value in tag_values.items():
        if tag_value is None:
            expr = tag_is_null(tag_column)
        else:
            expr = tag_eq(tag_column, str(tag_value))
        tag_filter = expr if tag_filter is None else tag_filter & expr
    return tag_filter


def _device_exact_tag_values(table_entry, device_entry) -> Dict[str, object]:
    """Map every declared tag column to this device's value (None when null/missing).

    ``device_entry.tag_values`` drops trailing null tags, so columns beyond its
    length are treated as null rather than omitted from the exact-match filter.
    """
    device_tag_values = device_entry.tag_values
    return {
        column: device_tag_values[idx] if idx < len(device_tag_values) else None
        for idx, column in enumerate(table_entry.tag_columns)
    }


def _expand_tree_device_segments(segments: Tuple[object, ...]) -> Tuple[object, ...]:
    """Expand the native tree DeviceID prefix into logical path segments."""
    if not segments or segments[0] is None:
        return tuple(segments)
    return tuple(str(segments[0]).split(".")) + tuple(segments[1:])


class TsFileSeriesReader:
    """Wrap ``TsFileReaderPy`` with numeric dataset discovery and batch reads."""

    def __init__(self, file_path: str, show_progress: bool = True):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"TsFile not found: {file_path}")

        self.file_path = file_path
        self.show_progress = show_progress

        try:
            self._reader = TsFileReaderPy(file_path)
        except Exception as e:
            raise ValueError(f"Failed to open TsFile: {e}") from e

        # Probe the file model: an empty table-schema map signals tree model
        self._table_schemas = self._reader.get_all_table_schemas()
        self._model_kind: str = MODEL_TREE if not self._table_schemas else MODEL_TABLE

        self._catalog = MetadataCatalog()
        self._cache_metadata()

    def __del__(self):
        self.close()

    @property
    def catalog(self) -> MetadataCatalog:
        return self._catalog

    @property
    def model_kind(self) -> str:
        return self._model_kind

    @property
    def series_paths(self) -> List[str]:
        return list(self.iter_series_paths())

    @property
    def series_count(self) -> int:
        return self._catalog.series_count

    def iter_series_paths(self) -> Iterator[str]:
        for device_id, field_idx in self._catalog.series_stats_by_ref:
            yield build_series_path(self._catalog, device_id, field_idx)

    def iter_series_refs(self) -> Iterator[Tuple[str, int, int]]:
        for device_id, field_idx in self._catalog.series_stats_by_ref:
            yield build_series_path(
                self._catalog, device_id, field_idx
            ), device_id, field_idx

    def close(self):
        if hasattr(self, "_reader"):
            try:
                self._reader.close()
            except Exception:
                pass

    def _cache_metadata(self):
        """Wrap metadata discovery so reader construction surfaces one stable error shape."""
        try:
            if self._model_kind == MODEL_TABLE:
                self._cache_metadata_table_model()
            else:
                self._cache_metadata_tree_model()
        except Exception as e:
            raise ValueError(
                f"Failed to read TsFile metadata. Please ensure the TsFile is valid and readable. Error: {e}"
            ) from e

    def _cache_metadata_table_model(self):
        """Build the in-memory catalog from table schemas and native metadata."""
        table_schemas = self._table_schemas
        if not table_schemas:
            raise ValueError("No tables found in TsFile")

        self._catalog = MetadataCatalog()
        table_names = list(table_schemas.keys())
        metadata_groups = self._reader.get_timeseries_metadata(None)
        if self.show_progress:
            sys.stderr.write(f"\rReading TsFile metadata: 0/{len(table_names)}")
            sys.stderr.flush()

        for table_index, table_name in enumerate(table_names):
            table_schema = table_schemas[table_name]

            tag_columns = []
            tag_types = []
            field_columns = []
            for column_schema in table_schema.get_columns():
                column_name = column_schema.get_column_name()
                column_category = column_schema.get_category()
                if column_category == ColumnCategory.TIME:
                    continue
                if column_category == ColumnCategory.TAG:
                    tag_columns.append(column_name)
                    tag_types.append(column_schema.get_data_type())

                # ignore fields which is not numeric, we won't use them currently.
                elif (
                    column_category == ColumnCategory.FIELD
                    and column_schema.get_data_type() in _NUMERIC_FIELD_TYPES
                ):
                    field_columns.append(column_name)

            if not field_columns:
                continue

            table_id = self._catalog.add_table(
                table_name, tag_columns, tag_types, field_columns
            )
            table_groups = [
                group
                for group in metadata_groups.values()
                if (group.table_name or "").lower() == table_name.lower()
            ]
            table_groups.sort(
                key=lambda group: tuple(
                    "" if value is None else str(value) for value in group.segments
                )
            )

            for group in table_groups:
                stats = self._metadata_device_stats(group)
                if stats is None:
                    continue
                tag_values = self._metadata_tag_values(group, len(tag_columns))
                device_id = self._add_device(
                    table_id, tag_values, stats["min_time"], stats["max_time"]
                )

                stats_by_field = self._metadata_field_stats(group)
                table_entry = self._catalog.table_entries[table_id]
                for field_idx, field_name in enumerate(table_entry.field_columns):
                    field_stats = stats_by_field.get(field_name)
                    if field_stats is None:
                        # Schema declares this field but the device never
                        # wrote it (or wrote it entirely as NaN). Skip --
                        # the dataset surface only carries real series.
                        continue
                    self._catalog.series_stats_by_ref[(device_id, field_idx)] = (
                        field_stats
                    )

            if self.show_progress:
                sys.stderr.write(
                    f"\rReading TsFile metadata: table {table_index + 1}/{len(table_names)} "
                    f"[{table_name}]"
                )
                sys.stderr.flush()

        if self.show_progress:
            sys.stderr.write(
                f"\rReading TsFile metadata: {len(table_names)} table(s), {self.series_count} series ... done\n"
            )
            sys.stderr.flush()

    def _cache_metadata_tree_model(self):
        """Build the in-memory catalog by synthesizing one virtual table.

        Tree TsFiles have no schema, so we materialize a single
        ``TableEntry``: table name = the shared root segment, tag columns
        = ``_col_1..._col_{N_max}`` (one per remaining path segment),
        fields = union of measurements across devices. Per-device
        ownership is preserved by registering only the
        ``(device_id, field_idx)`` pairs actually written on disk in
        ``series_stats_by_ref``.
        """
        metadata_groups = self._reader.get_timeseries_metadata(None)
        if not metadata_groups:
            raise ValueError("No devices found in tree-model TsFile")

        # 1) Walk every device once to collect: root-segment, max depth, and
        #    the union of measurements that pass the numeric filter.
        root_name = None
        max_depth = 0  # segments after the root (i.e. virtual tag depth)
        device_specs = []  # list of (tail_segments, group, stats_by_field)
        union_fields = []  # ordered union of measurement names
        seen_field_names = set()

        for group in metadata_groups.values():
            full_segments = _expand_tree_device_segments(tuple(group.segments))
            if not full_segments:
                continue
            current_root = full_segments[0]
            if root_name is None:
                root_name = current_root
            elif current_root != root_name:
                raise ValueError(
                    f"Tree-model TsFile contains multiple root segments: "
                    f"{root_name!r} vs {current_root!r}. A single load set "
                    f"must share one tree root."
                )

            tail = full_segments[1:]
            depth = len(tail)
            if depth > max_depth:
                max_depth = depth

            stats_by_field = self._metadata_field_stats(group)
            if not stats_by_field:
                continue
            for measurement in stats_by_field.keys():
                if measurement not in seen_field_names:
                    seen_field_names.add(measurement)
                    union_fields.append(measurement)
            device_specs.append((tail, group, stats_by_field))

        if root_name is None:
            raise ValueError("No devices found in tree-model TsFile")
        if not union_fields:
            raise ValueError("No numeric measurements found in tree-model TsFile")

        # 2) Materialize the synthetic table entry. Tag columns are 1-based
        #    so the rendered headers match the requirement ("_col_1").
        tag_columns = tuple(f"_col_{i + 1}" for i in range(max_depth))
        tag_types = (TSDataType.STRING,) * max_depth

        self._catalog = MetadataCatalog()
        table_id = self._catalog.add_table(
            root_name, tag_columns, tag_types, union_fields
        )
        table_entry = self._catalog.table_entries[table_id]

        # 3) Stable device order: keep input iteration order so the dataset
        #    layer's row index stays deterministic across reloads.
        total = len(device_specs)
        if self.show_progress:
            sys.stderr.write(f"\rReading TsFile metadata: 0/{total} devices")
            sys.stderr.flush()

        for idx, (tail, group, stats_by_field) in enumerate(device_specs, start=1):
            device_stats = self._metadata_device_stats(group)
            if device_stats is None:
                continue
            # Pad shorter devices with None to length max_depth; the catalog
            # normalizer strips trailing Nones so this never enters the
            # sparse-tag bookkeeping.
            padded = tuple(list(tail) + [None] * (max_depth - len(tail)))
            device_id = self._add_device(
                table_id, padded, device_stats["min_time"], device_stats["max_time"]
            )
            for measurement, field_stats in stats_by_field.items():
                field_idx = table_entry.get_field_index(measurement)
                self._catalog.series_stats_by_ref[(device_id, field_idx)] = field_stats
            if self.show_progress and (idx % 64 == 0 or idx == total):
                sys.stderr.write(f"\rReading TsFile metadata: {idx}/{total} devices")
                sys.stderr.flush()

        if self.show_progress:
            sys.stderr.write(
                f"\rReading TsFile metadata (tree): {total} device(s), "
                f"{self.series_count} series ... done\n"
            )
            sys.stderr.flush()

    @staticmethod
    def _metadata_device_stats(group) -> dict:
        """Derive cheap device-level metadata hints from native field statistics.

        Callers must treat them as pruning/display hints rather than exact
        logical-series timeline semantics.
        """
        statistics = [
            timeseries.timeline_statistic
            for timeseries in group.timeseries
            if timeseries.timeline_statistic.has_statistic
            and timeseries.timeline_statistic.row_count > 0
        ]
        if not statistics:
            return None

        return {
            "min_time": min(int(statistic.start_time) for statistic in statistics),
            "max_time": max(int(statistic.end_time) for statistic in statistics),
        }

    @staticmethod
    def _metadata_tag_values(group, tag_count: int) -> tuple:
        """Extract ordered table tag values from IDeviceID segments.

        A table-model DeviceID may only materialize a prefix of the declared
        tag columns. Preserve the available prefix rather than requiring a
        full-length tag tuple here. Some backends may still materialize
        trailing missing tags as explicit ``None`` values; normalize those
        back to the same prefix representation.
        """
        if tag_count == 0:
            return ()
        values = list(group.segments[1 : min(len(group.segments), 1 + tag_count)])
        while values and values[-1] is None:
            values.pop()
        return tuple(values)

    @staticmethod
    def _metadata_field_stats(group) -> Dict[str, SeriesStats]:
        """Collect per-measurement stats for numeric cells that have real values.

        A measurement appears iff it is numeric AND its native ``statistic``
        block is populated with a positive ``row_count``. Non-numeric
        measurements (STRING/TEXT) are dropped because the dataset surface reads
        values as ``float64`` -- the same reason table mode filters non-numeric
        fields out of its schema. Columns that the device never wrote (Tablet
        skip / all-NaN pandas column) carry no real values and are intentionally
        absent -- the dataset layer surfaces only series that physically exist.
        """
        stats: Dict[str, SeriesStats] = {}
        for timeseries in group.timeseries:
            # Drop non-numeric measurements (see docstring): tree mode relies on
            # this to avoid surfacing a string/text series that crashes on read.
            if timeseries.data_type not in _NUMERIC_FIELD_TYPES:
                continue
            statistic = timeseries.statistic
            # Gate on the value statistic's non-null row_count, not the
            # timeline. A skipped/all-NaN field still carries a value-stat
            # block (has_statistic=True) and shares the device's timeline
            # rows, so a timeline gate would surface a phantom series whose
            # value row_count is 0. See
            # test_dataset_omits_table_model_phantom_series_for_skipped_cells.
            if not statistic.has_statistic or statistic.row_count <= 0:
                continue
            # timeline_statistic is always a TimeseriesStatistic (never None;
            # the native binding fills row_count/start/end even when
            # has_statistic is False), so the reads below cannot raise.
            timeline_statistic = timeseries.timeline_statistic
            stats[timeseries.measurement_name] = SeriesStats(
                length=int(statistic.row_count),
                min_time=int(statistic.start_time),
                max_time=int(statistic.end_time),
                timeline_length=int(timeline_statistic.row_count),
                timeline_min_time=int(timeline_statistic.start_time),
                timeline_max_time=int(timeline_statistic.end_time),
            )
        return stats

    def _add_device(
        self,
        table_id: int,
        tag_values: tuple,
        min_time: int,
        max_time: int,
    ):
        """Add one device to the catalog."""
        return self._catalog.add_device(table_id, tag_values, min_time, max_time)

    def _resolve_series_path(self, series_path: str) -> Tuple[int, int, int]:
        return resolve_series_path(self._catalog, series_path)

    def _resolve_series_ref(self, device_id: int, field_idx: int):
        """Resolve a reader-local ref into the table/device metadata needed by read paths."""
        device_entry = self._catalog.device_entries[device_id]
        table_entry = self._catalog.table_entries[device_entry.table_id]
        field_name = table_entry.field_columns[field_idx]
        return table_entry, device_entry, field_name

    def get_device_info(self, device_id: int) -> dict:
        device_entry = self._catalog.device_entries[device_id]
        table_entry = self._catalog.table_entries[device_entry.table_id]
        return {
            "table_name": table_entry.table_name,
            "tag_columns": table_entry.tag_columns,
            "tag_values": dict(zip(table_entry.tag_columns, device_entry.tag_values)),
            "min_time": device_entry.min_time,
            "max_time": device_entry.max_time,
        }

    def get_series_info_by_ref(self, device_id: int, field_idx: int) -> dict:
        table_entry, device_entry, field_name = self._resolve_series_ref(
            device_id, field_idx
        )
        field_stats = self._catalog.series_stats_by_ref[(device_id, field_idx)]
        return {
            "length": field_stats.length,
            "min_time": field_stats.min_time,
            "max_time": field_stats.max_time,
            "timeline_length": field_stats.timeline_length,
            "timeline_min_time": field_stats.timeline_min_time,
            "timeline_max_time": field_stats.timeline_max_time,
            "table_name": table_entry.table_name,
            "column_name": field_name,
            "device_id": device_id,
            "field_idx": field_idx,
            "tag_columns": table_entry.tag_columns,
            "tag_values": dict(zip(table_entry.tag_columns, device_entry.tag_values)),
        }

    def get_series_info(self, series_path: str) -> dict:
        device_id, field_idx = self._resolve_series_path(series_path)[1:]
        return self.get_series_info_by_ref(device_id, field_idx)

    def read_series_by_ref(
        self, device_id: int, field_idx: int, start_time: int, end_time: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        table_entry, _, field_name = self._resolve_series_ref(device_id, field_idx)
        timestamps, field_values = self.read_device_fields_by_time_range(
            device_id, [field_idx], start_time, end_time
        )
        if len(timestamps) == 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        return timestamps, field_values[field_name]

    def read_series_by_time_range(
        self, series_path: str, start_time: int, end_time: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        _, device_id, field_idx = self._resolve_series_path(series_path)
        return self.read_series_by_ref(device_id, field_idx, start_time, end_time)

    def read_series_by_row(
        self, device_id: int, field_idx: int, offset: int, limit: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Read one logical series by device-local row offset/limit."""
        if limit <= 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)

        table_entry, device_entry, field_name = self._resolve_series_ref(
            device_id, field_idx
        )

        if self._model_kind == MODEL_TREE:
            device_path = self._build_tree_device_path(table_entry, device_entry)
            return self._read_series_by_row_tree(device_path, field_name, offset, limit)

        tag_values = _device_exact_tag_values(table_entry, device_entry)
        tag_filter = _build_exact_tag_filter(tag_values) if tag_values else None

        # Pull whole TsBlocks via the Arrow C-Data interface instead of
        # iterating row-by-row in Python. Each result_set.next() +
        # get_value_by_name() pair would be a Python<->C round-trip per row
        # and dominates wall time on long slices; read_arrow_batch() returns
        # a column-oriented batch in one call and lands directly in numpy.
        timestamp_parts = []
        value_parts = []
        remaining = limit
        next_offset = offset

        while remaining > 0:
            produced_this_call = 0
            with self._reader.query_table_by_row(
                table_entry.table_name,
                [field_name],
                offset=next_offset,
                limit=remaining,
                tag_filter=tag_filter,
                batch_size=65536,
            ) as result_set:
                while True:
                    arrow_table = result_set.read_arrow_batch()
                    if arrow_table is None:
                        break
                    if arrow_table.num_rows == 0:
                        continue
                    timestamp_parts.append(arrow_table.column("time").to_numpy())
                    raw_values = arrow_table.column(field_name).to_numpy(
                        zero_copy_only=False
                    )
                    value_parts.append(np.asarray(raw_values, dtype=np.float64))
                    produced_this_call += arrow_table.num_rows

            if produced_this_call == 0:
                break

            next_offset += produced_this_call
            remaining -= produced_this_call

        if not timestamp_parts:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        if len(timestamp_parts) == 1:
            return timestamp_parts[0], value_parts[0]
        return np.concatenate(timestamp_parts), np.concatenate(value_parts)

    def _read_series_by_row_tree(
        self, device_path: str, field_name: str, offset: int, limit: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Tree-model row read: scan on-tree result, filter device, apply offset/limit."""
        target_path_segments = device_path.split(".")
        # +1 because cwrapper prepends the root as an extra col_i cell.
        expected_path_len = (
            max(len(t.tag_columns) for t in self._catalog.table_entries) + 1
        )
        timestamps = []
        values = []
        skipped = 0
        # PERF: O(total_rows). query_table_on_tree scans every device's rows
        # for this field and we filter down to one device client-side, so an
        # aligned read over N devices is O(N * total_rows). Per-device tree
        # pushdown (query_tree_by_row) isn't reliable in the cwrapper yet
        # (stale col_i path columns leak across queries on a reused reader);
        # see PR #816. Hot path for profilers.
        # The native tree query normalizes column names and path segments to
        # lower case (table model is case-insensitive), so match the field name
        # and device path case-insensitively to preserve the original casing.
        target_path_lower = [seg.lower() for seg in target_path_segments]
        with self._reader.query_table_on_tree([field_name]) as result_set:
            md = result_set.get_metadata()
            num_cols = md.get_column_num()
            col_names = [md.get_column_name(i + 1) for i in range(num_cols)]
            lower_col_names = [name.lower() for name in col_names]
            try:
                field_idx = lower_col_names.index(field_name.lower()) + 1
            except ValueError:
                return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
            all_col_indices = [
                idx + 1
                for idx, name in enumerate(lower_col_names)
                if name.startswith("col_")
            ]
            # Only the trailing expected_path_len col_i cells are genuine; the
            # leading duplicates are stale from prior queries on this reader.
            col_indices = all_col_indices[-expected_path_len:]
            # Fail fast if the cwrapper col_ leak pattern changes: the trailing
            # slice assumes at least expected_path_len col_ cells. A count
            # mismatch means the heuristic is stale and could silently pick the
            # wrong path columns (returning wrong-device data). Guards the count
            # only -- it can't catch a leak where the genuine columns are no
            # longer the trailing ones.
            assert len(col_indices) == expected_path_len, (
                f"tree path col_ columns: expected {expected_path_len}, got "
                f"{len(col_indices)} of {len(all_col_indices)}; cwrapper col_ "
                f"leak pattern may have changed"
            )
            while result_set.next():
                row_path_segments = [
                    result_set.get_value_by_index(ci) for ci in col_indices
                ]
                # Trim trailing Nones for the (possibly-shorter) device path.
                while row_path_segments and row_path_segments[-1] is None:
                    row_path_segments.pop()
                row_path_lower = [
                    seg.lower() if isinstance(seg, str) else seg
                    for seg in row_path_segments
                ]
                if row_path_lower != target_path_lower:
                    continue
                if skipped < offset:
                    skipped += 1
                    continue
                if len(timestamps) >= limit:
                    break
                ts = result_set.get_value_by_index(1)
                raw = result_set.get_value_by_index(field_idx)
                timestamps.append(int(ts))
                values.append(np.nan if raw is None else float(raw))

        if not timestamps:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        return (
            np.asarray(timestamps, dtype=np.int64),
            np.asarray(values, dtype=np.float64),
        )

    def read_device_fields_by_time_range(
        self, device_id: int, field_indices: List[int], start_time: int, end_time: int
    ) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
        """Read one device slice and return the requested field columns keyed by field name."""
        device_entry = self._catalog.device_entries[device_id]
        table_entry = self._catalog.table_entries[device_entry.table_id]
        requested_field_columns = [
            table_entry.field_columns[field_idx] for field_idx in field_indices
        ]
        if self._model_kind == MODEL_TREE:
            device_path = self._build_tree_device_path(table_entry, device_entry)
            return self._read_arrow_tree(
                device_path, requested_field_columns, start_time, end_time
            )
        return self._read_arrow_table(
            table_entry.table_name,
            requested_field_columns,
            table_entry.tag_columns,
            _device_exact_tag_values(table_entry, device_entry),
            start_time,
            end_time,
        )

    @staticmethod
    def _build_tree_device_path(table_entry, device_entry) -> str:
        """Reassemble the cwrapper-facing tree device path from catalog state.

        The native ``query_timeseries`` / ``query_tree_by_row`` APIs split the
        device path on ``.`` internally, so segments themselves must not
        contain ``.``. Tree-model writers enforce this convention; we surface
        an explicit error if a future writer ever violates it.
        """
        components = [str(table_entry.table_name)]
        for value in device_entry.tag_values:
            if value is None:
                # Should not happen: trailing-None devices are normalized to a
                # shorter tag tuple. An interior None signals a sparse-tag
                # device, which is not part of the tree-model contract.
                raise ValueError(
                    f"Tree device path cannot include a null segment: "
                    f"{device_entry.tag_values!r}"
                )
            text = str(value)
            if "." in text:
                raise NotImplementedError(
                    f"Tree device segment with '.' is not supported by the "
                    f"underlying cwrapper path API: {text!r}"
                )
            components.append(text)
        return ".".join(components)

    def _read_arrow_table(
        self,
        table_name: str,
        field_columns: List[str],
        tag_columns: Tuple[str, ...],
        tag_values: Dict[str, object],
        start_time: int,
        end_time: int,
    ) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
        """Execute the underlying table query with exact tag filter pushdown."""
        tag_columns = list(tag_columns)
        field_columns = list(field_columns)
        query_columns = list(field_columns)
        timestamp_parts = []
        field_parts = {field_column: [] for field_column in field_columns}
        tag_filter = _build_exact_tag_filter(tag_values) if tag_values else None

        with self._reader.query_table(
            table_name,
            query_columns,
            start_time=start_time,
            end_time=end_time,
            tag_filter=tag_filter,
            batch_size=65536,
        ) as result_set:
            while True:
                arrow_table = result_set.read_arrow_batch()
                if arrow_table is None:
                    break

                if arrow_table.num_rows == 0:
                    continue

                timestamp_parts.append(arrow_table.column("time").to_numpy())
                for field_column in field_columns:
                    raw_values = arrow_table.column(field_column).to_numpy()
                    try:
                        field_parts[field_column].append(
                            np.asarray(raw_values, dtype=np.float64)
                        )
                    except (TypeError, ValueError) as e:
                        raise TypeError(
                            f"Field column '{field_column}' in table '{table_name}' is not numeric-compatible."
                        ) from e

        if not timestamp_parts:
            return (
                np.array([], dtype=np.int64),
                {
                    field_column: np.array([], dtype=np.float64)
                    for field_column in field_columns
                },
            )

        timestamps = np.concatenate(timestamp_parts).astype(np.int64)
        field_values = {
            field_column: np.concatenate(field_parts[field_column])
            for field_column in field_columns
        }

        # Keep the dataset layer strict about the requested time window even if
        # the underlying query path returns boundary-adjacent null rows.
        mask = (timestamps >= start_time) & (timestamps <= end_time)
        timestamps = timestamps[mask]
        field_values = {
            field_column: values[mask] for field_column, values in field_values.items()
        }

        return timestamps, field_values

    def _read_arrow_tree(
        self,
        device_path: str,
        field_columns: List[str],
        start_time: int,
        end_time: int,
    ) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
        """Tree-model time-range read for one device (multi-field)."""
        field_columns = list(field_columns)
        if not field_columns:
            return (
                np.array([], dtype=np.int64),
                {},
            )

        target_path_segments = device_path.split(".")
        target_path_lower = [seg.lower() for seg in target_path_segments]
        expected_path_len = (
            max(len(t.tag_columns) for t in self._catalog.table_entries) + 1
        )
        timestamps = []
        value_buckets = {col: [] for col in field_columns}

        # PERF: O(total_rows) full tree scan filtered to one device
        # client-side; aligned reads over N devices are O(N * total_rows).
        # See _read_series_by_row_tree / PR #816 for the cwrapper limitation
        # that blocks per-device pushdown (query_timeseries). Hot path.
        # The native tree query lower-cases column names and path segments
        # (table model is case-insensitive), so match both case-insensitively.
        with self._reader.query_table_on_tree(
            field_columns, start_time, end_time
        ) as result_set:
            md = result_set.get_metadata()
            num_cols = md.get_column_num()
            col_names = [md.get_column_name(i + 1) for i in range(num_cols)]
            lower_col_names = [name.lower() for name in col_names]
            value_indices = {}
            for col in field_columns:
                try:
                    value_indices[col] = lower_col_names.index(col.lower()) + 1
                except ValueError:
                    # Column missing (no device in file owns it). Yield empty.
                    return (
                        np.array([], dtype=np.int64),
                        {
                            col2: np.array([], dtype=np.float64)
                            for col2 in field_columns
                        },
                    )
            all_col_indices = [
                idx + 1
                for idx, name in enumerate(lower_col_names)
                if name.startswith("col_")
            ]
            col_indices = all_col_indices[-expected_path_len:]
            # Fail fast if the cwrapper col_ leak pattern changes: the trailing
            # slice assumes at least expected_path_len col_ cells. A count
            # mismatch means the heuristic is stale and could silently pick the
            # wrong path columns (returning wrong-device data). Guards the count
            # only -- it can't catch a leak where the genuine columns are no
            # longer the trailing ones.
            assert len(col_indices) == expected_path_len, (
                f"tree path col_ columns: expected {expected_path_len}, got "
                f"{len(col_indices)} of {len(all_col_indices)}; cwrapper col_ "
                f"leak pattern may have changed"
            )
            while result_set.next():
                row_path_segments = [
                    result_set.get_value_by_index(ci) for ci in col_indices
                ]
                while row_path_segments and row_path_segments[-1] is None:
                    row_path_segments.pop()
                row_path_lower = [
                    seg.lower() if isinstance(seg, str) else seg
                    for seg in row_path_segments
                ]
                if row_path_lower != target_path_lower:
                    continue
                ts = int(result_set.get_value_by_index(1))
                # The on-tree scan already honors start/end_time at the
                # cwrapper level, but defensively re-clip on the boundary.
                if ts < start_time or ts > end_time:
                    continue
                timestamps.append(ts)
                for col, vidx in value_indices.items():
                    raw = result_set.get_value_by_index(vidx)
                    value_buckets[col].append(np.nan if raw is None else float(raw))

        if not timestamps:
            return (
                np.array([], dtype=np.int64),
                {col: np.array([], dtype=np.float64) for col in field_columns},
            )

        timestamps_arr = np.asarray(timestamps, dtype=np.int64)
        field_values = {
            col: np.asarray(value_buckets[col], dtype=np.float64)
            for col in field_columns
        }
        return timestamps_arr, field_values
