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
from ..tag_filter import tag_eq
from ..tsfile_reader import TsFileReaderPy
from .metadata import MetadataCatalog, build_series_path, iter_series_refs, resolve_series_path


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


def _ensure_supported_exact_tag_values(tag_values: Dict[str, object]) -> None:
    if any(tag_value is None for tag_value in tag_values.values()):
        raise NotImplementedError(
            "Exact tag matching with None tag values is not supported yet. "
            "Native tag filter support for IS NULL / IS NOT NULL is required."
        )


def _build_exact_tag_filter(tag_values: Dict[str, object]):
    _ensure_supported_exact_tag_values(tag_values)
    tag_filter = None
    for tag_column, tag_value in tag_values.items():
        expr = tag_eq(tag_column, str(tag_value))
        tag_filter = expr if tag_filter is None else tag_filter & expr
    return tag_filter


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

        self._catalog = MetadataCatalog()
        self._cache_metadata()

    def __del__(self):
        self.close()

    @property
    def catalog(self) -> MetadataCatalog:
        return self._catalog

    @property
    def series_paths(self) -> List[str]:
        return list(self.iter_series_paths())

    @property
    def series_count(self) -> int:
        return self._catalog.series_count

    def iter_series_paths(self) -> Iterator[str]:
        for device_id, field_idx in iter_series_refs(self._catalog):
            yield build_series_path(self._catalog, device_id, field_idx)

    def iter_series_refs(self) -> Iterator[Tuple[str, int, int]]:
        for device_id, field_idx in iter_series_refs(self._catalog):
            yield build_series_path(self._catalog, device_id, field_idx), device_id, field_idx

    def close(self):
        if hasattr(self, "_reader"):
            try:
                self._reader.close()
            except Exception:
                pass

    def _cache_metadata(self):
        """Wrap metadata discovery so reader construction surfaces one stable error shape."""
        try:
            self._cache_metadata_table_model()
            # todo: we should support tree model
        except Exception as e:
            raise ValueError(
                f"Failed to read TsFile metadata. Please ensure the TsFile is valid and readable. Error: {e}"
            ) from e

    def _cache_metadata_table_model(self):
        """Build the in-memory catalog from table schemas and native metadata."""
        table_schemas = self._reader.get_all_table_schemas()
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

            table_id = self._catalog.add_table(table_name, tag_columns, tag_types, field_columns)
            table_groups = [
                group
                for group in metadata_groups.values()
                if (group.table_name or "").lower() == table_name.lower()
            ]
            table_groups.sort(key=lambda group: tuple("" if value is None else str(value) for value in group.segments))

            for group in table_groups:
                stats = self._metadata_device_stats(group)
                if stats is None:
                    continue
                tag_values = self._metadata_tag_values(group, len(tag_columns))
                device_id = self._add_device(table_id, tag_values, stats["min_time"], stats["max_time"])

                stats_by_field = self._metadata_field_stats(group)
                table_entry = self._catalog.table_entries[table_id]
                for field_idx, field_name in enumerate(table_entry.field_columns):
                    field_stats = stats_by_field.get(field_name)
                    if field_stats is None:
                        self._catalog.series_stats_by_ref[(device_id, field_idx)] = {
                            "length": 0,
                            "min_time": None,
                            "max_time": None,
                            "timeline_length": 0,
                            "timeline_min_time": None,
                            "timeline_max_time": None,
                        }
                    else:
                        self._catalog.series_stats_by_ref[(device_id, field_idx)] = field_stats

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

    @staticmethod
    def _metadata_device_stats(group) -> dict:
        """Derive cheap device-level metadata hints from native field statistics.

        Callers must treat them as pruning/display hints rather than exact
        logical-series timeline semantics.
        """
        statistics = [
            timeseries.timeline_statistic
            for timeseries in group.timeseries
            if timeseries.timeline_statistic.has_statistic and timeseries.timeline_statistic.row_count > 0
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
    def _metadata_field_stats(group) -> Dict[str, dict]:
        stats = {}
        for timeseries in group.timeseries:
            statistic = timeseries.statistic
            timeline_statistic = timeseries.timeline_statistic
            if not timeline_statistic.has_statistic or timeline_statistic.row_count <= 0:
                continue
            stats[timeseries.measurement_name] = {
                "length": int(statistic.row_count) if statistic.has_statistic else 0,
                "min_time": int(statistic.start_time) if statistic.has_statistic else None,
                "max_time": int(statistic.end_time) if statistic.has_statistic else None,
                "timeline_length": int(timeline_statistic.row_count),
                "timeline_min_time": int(timeline_statistic.start_time),
                "timeline_max_time": int(timeline_statistic.end_time),
            }
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
        table_entry, device_entry, field_name = self._resolve_series_ref(device_id, field_idx)
        field_stats = self._catalog.series_stats_by_ref[(device_id, field_idx)]
        return {
            "length": field_stats["length"],
            "min_time": field_stats["min_time"],
            "max_time": field_stats["max_time"],
            "timeline_length": field_stats["timeline_length"],
            "timeline_min_time": field_stats["timeline_min_time"],
            "timeline_max_time": field_stats["timeline_max_time"],
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

    def read_series_by_ref(self, device_id: int, field_idx: int, start_time: int, end_time: int) -> Tuple[np.ndarray, np.ndarray]:
        table_entry, _, field_name = self._resolve_series_ref(device_id, field_idx)
        timestamps, field_values = self.read_device_fields_by_time_range(device_id, [field_idx], start_time, end_time)
        if len(timestamps) == 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        return timestamps, field_values[field_name]

    def read_series_by_time_range(self, series_path: str, start_time: int, end_time: int) -> Tuple[np.ndarray, np.ndarray]:
        _, device_id, field_idx = self._resolve_series_path(series_path)
        return self.read_series_by_ref(device_id, field_idx, start_time, end_time)

    def read_series_by_row(self, device_id: int, field_idx: int, offset: int, limit: int) -> Tuple[np.ndarray, np.ndarray]:
        """Read one logical series by device-local row offset/limit."""
        if limit <= 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)

        table_entry, device_entry, field_name = self._resolve_series_ref(device_id, field_idx)
        tag_values = dict(zip(table_entry.tag_columns, device_entry.tag_values))
        tag_filter = _build_exact_tag_filter(tag_values) if tag_values else None

        # Some native row-query paths stop at an internal block boundary even
        # when the requested window extends further. Re-issue from the advanced
        # offset until we fill the caller's logical row window or reach EOF.
        timestamp_parts = []
        value_parts = []
        remaining = limit
        next_offset = offset

        while remaining > 0:
            batch_timestamps = []
            batch_values = []
            with self._reader.query_table_by_row(
                table_entry.table_name,
                [field_name],
                offset=next_offset,
                limit=remaining,
                tag_filter=tag_filter,
            ) as result_set:
                while result_set.next():
                    batch_timestamps.append(result_set.get_value_by_name("time"))
                    value = result_set.get_value_by_name(field_name)
                    batch_values.append(np.nan if value is None else float(value))

            if not batch_timestamps:
                break

            timestamp_parts.append(np.asarray(batch_timestamps, dtype=np.int64))
            value_parts.append(np.asarray(batch_values, dtype=np.float64))
            read_count = len(batch_timestamps)
            next_offset += read_count
            remaining -= read_count

        if not timestamp_parts:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        if len(timestamp_parts) == 1:
            return timestamp_parts[0], value_parts[0]
        return np.concatenate(timestamp_parts), np.concatenate(value_parts)

    def read_device_fields_by_time_range(
        self, device_id: int, field_indices: List[int], start_time: int, end_time: int
    ) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
        """Read one device slice and return the requested field columns keyed by field name."""
        device_entry = self._catalog.device_entries[device_id]
        table_entry = self._catalog.table_entries[device_entry.table_id]
        requested_field_columns = [table_entry.field_columns[field_idx] for field_idx in field_indices]
        timestamps, field_values = self._read_arrow(
            table_entry.table_name,
            requested_field_columns,
            table_entry.tag_columns,
            dict(zip(table_entry.tag_columns, device_entry.tag_values)),
            start_time,
            end_time,
        )
        return timestamps, field_values

    def _read_arrow(
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
                        field_parts[field_column].append(np.asarray(raw_values, dtype=np.float64))
                    except (TypeError, ValueError) as e:
                        raise TypeError(
                            f"Field column '{field_column}' in table '{table_name}' is not numeric-compatible."
                        ) from e

        if not timestamp_parts:
            return (
                np.array([], dtype=np.int64),
                {field_column: np.array([], dtype=np.float64) for field_column in field_columns},
            )

        timestamps = np.concatenate(timestamp_parts).astype(np.int64)
        field_values = {field_column: np.concatenate(field_parts[field_column]) for field_column in field_columns}

        # Keep the dataset layer strict about the requested time window even if
        # the underlying query path returns boundary-adjacent null rows.
        mask = (timestamps >= start_time) & (timestamps <= end_time)
        timestamps = timestamps[mask]
        field_values = {field_column: values[mask] for field_column, values in field_values.items()}

        return timestamps, field_values
