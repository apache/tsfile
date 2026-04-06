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
import pyarrow.compute as pc

from ..constants import ColumnCategory, TSDataType
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
        """Build the in-memory catalog by scanning table batches from the file."""
        table_schemas = self._reader.get_all_table_schemas()
        if not table_schemas:
            raise ValueError("No tables found in TsFile")

        self._catalog = MetadataCatalog()
        total_rows = 0
        table_names = list(table_schemas.keys())

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
            time_arrays = []
            tag_arrays = {tag_column: [] for tag_column in tag_columns}

            # [Temporary] It will be replaced by new tsfile api, we won't query all the data later.
            query_columns = tag_columns + field_columns

            with self._reader.query_table_batch(table_name, query_columns, batch_size=65536) as result_set:
                while True:
                    arrow_table = result_set.read_arrow_batch()
                    if arrow_table is None:
                        break
                    batch_rows = arrow_table.num_rows
                    total_rows += batch_rows
                    time_arrays.append(arrow_table.column("time").to_numpy())
                    for tag_column in tag_columns:
                        tag_arrays[tag_column].append(arrow_table.column(tag_column).to_numpy())

                    if self.show_progress:
                        sys.stderr.write(
                            f"\rReading TsFile metadata: table {table_index + 1}/{len(table_names)} "
                            f"[{table_name}] ({total_rows:,} rows)"
                        )
                        sys.stderr.flush()

            if not time_arrays:
                continue

            timestamps = np.concatenate(time_arrays).astype(np.int64)
            if not tag_columns:
                self._add_device(table_id, (), timestamps)
                continue

            for tag_values, device_timestamps in self._iter_device_groups(tag_columns, timestamps, tag_arrays):
                self._add_device(table_id, tag_values, device_timestamps)

        if self.show_progress and total_rows > 0:
            sys.stderr.write(
                f"\rReading TsFile metadata: {len(table_names)} table(s), {total_rows:,} rows, "
                f"{self.series_count} series ... done\n"
            )
            sys.stderr.flush()

        if self.series_count == 0:
            raise ValueError("No valid numeric series found in TsFile")

    def _iter_device_groups(
        self,
        tag_columns: List[str],
        timestamps: np.ndarray,
        tag_arrays: Dict[str, list],
    ) -> Iterator[Tuple[tuple, np.ndarray]]:
        """Group one table's rows by tag tuple while preserving original row membership."""
        tag_values_by_column = {column: np.concatenate(tag_arrays[column]) for column in tag_columns}

        n = len(timestamps)
        arrays = [tag_values_by_column[col] for col in tag_columns]
        dtype = np.dtype([(col, arrays[i].dtype) for i, col in enumerate(tag_columns)])
        composite = np.empty(n, dtype=dtype)
        for i, col in enumerate(tag_columns):
            composite[col] = arrays[i]

        _, inverse, counts = np.unique(composite, return_inverse=True, return_counts=True)
        ordered_indices = np.argsort(inverse, kind="stable")
        group_bounds = np.cumsum(counts)[:-1]
        for group_indices in np.split(ordered_indices, group_bounds):
            first = int(group_indices[0])
            tag_tuple = tuple(_to_python_scalar(composite[col][first]) for col in tag_columns)
            yield tag_tuple, timestamps[group_indices]

    def _add_device(
        self,
        table_id: int,
        tag_values: tuple,
        timestamps: np.ndarray,
    ):
        """Add one device to the catalog."""
        if len(timestamps) == 0:
            return

        self._catalog.add_device(table_id, tag_values, timestamps)

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
            "length": device_entry.length,
            "min_time": device_entry.min_time,
            "max_time": device_entry.max_time,
        }

    def get_device_timestamps(self, device_id: int) -> np.ndarray:
        return self._catalog.device_entries[device_id].timestamps

    def get_series_info_by_ref(self, device_id: int, field_idx: int) -> dict:
        table_entry, device_entry, field_name = self._resolve_series_ref(device_id, field_idx)
        return {
            "length": device_entry.length,
            "min_time": device_entry.min_time,
            "max_time": device_entry.max_time,
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

    def get_series_timestamps(self, series_path: str) -> np.ndarray:
        device_id = self._resolve_series_path(series_path)[1]
        return self.get_device_timestamps(device_id)

    def read_series_by_ref(self, device_id: int, field_idx: int, start_time: int, end_time: int) -> Tuple[np.ndarray, np.ndarray]:
        table_entry, _, field_name = self._resolve_series_ref(device_id, field_idx)
        timestamps, field_values = self.read_device_fields_by_time_range(device_id, [field_idx], start_time, end_time)
        if len(timestamps) == 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        return timestamps, field_values[field_name]

    def read_series_by_time_range(self, series_path: str, start_time: int, end_time: int) -> Tuple[np.ndarray, np.ndarray]:
        _, device_id, field_idx = self._resolve_series_path(series_path)
        return self.read_series_by_ref(device_id, field_idx, start_time, end_time)

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
        """Execute the underlying table query, then apply tag filtering client-side."""
        tag_columns = list(tag_columns)
        field_columns = list(field_columns)
        query_columns = tag_columns + field_columns if tag_columns else list(field_columns)
        timestamp_parts = []
        field_parts = {field_column: [] for field_column in field_columns}

        with self._reader.query_table_batch(
            table_name,
            query_columns,
            start_time=start_time,
            end_time=end_time,
            batch_size=65536,
        ) as result_set:
            while True:
                arrow_table = result_set.read_arrow_batch()
                if arrow_table is None:
                    break

                if tag_values:
                    mask = None
                    for tag_column, tag_value in tag_values.items():
                        column_mask = pc.equal(arrow_table.column(tag_column), tag_value)
                        mask = column_mask if mask is None else pc.and_(mask, column_mask)
                    arrow_table = arrow_table.filter(mask)

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

        return (
            np.concatenate(timestamp_parts).astype(np.int64),
            {field_column: np.concatenate(field_parts[field_column]) for field_column in field_columns},
        )
