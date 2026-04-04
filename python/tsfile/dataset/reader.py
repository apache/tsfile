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
from typing import Dict, List, Tuple

import numpy as np
import pyarrow.compute as pc

from ..constants import ColumnCategory
from ..tsfile_reader import TsFileReaderPy


class TsFileSeriesReader:
    """Wrap TsFileReaderPy with series-level metadata and batch reads."""

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

        self._cache_metadata()

    def __del__(self):
        self.close()

    def close(self):
        if hasattr(self, "_reader"):
            try:
                self._reader.close()
            except Exception:
                pass

    def _cache_metadata(self):
        try:
            self._cache_metadata_table_model()
        except Exception as e:
            raise ValueError(
                f"Failed to read TsFile metadata. Please ensure the TsFile is valid and readable. Error: {e}"
            )

    def _cache_metadata_table_model(self):
        table_schemas = self._reader.get_all_table_schemas()
        if not table_schemas:
            raise ValueError("No tables found in TsFile")

        self.series_paths = []
        total_rows = 0
        table_names = list(table_schemas.keys())

        for table_index, table_name in enumerate(table_names):
            table_schema = self._reader.get_table_schema(table_name)

            tag_columns = []
            field_columns = []
            for column_schema in table_schema.get_columns():
                column_name = column_schema.get_column_name()
                column_category = column_schema.get_category()
                if column_name.lower() == "time":
                    continue
                if column_category == ColumnCategory.TAG:
                    tag_columns.append(column_name)
                elif column_category == ColumnCategory.FIELD:
                    field_columns.append(column_name)

            if not field_columns:
                continue

            time_arrays = []
            tag_arrays = {tag_column: [] for tag_column in tag_columns}
            query_columns = tag_columns + [field_columns[0]]

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
            if tag_columns:
                self._register_tagged_groups(table_name, tag_columns, field_columns, timestamps, tag_arrays)
            else:
                self._register_tag_group(table_name, tag_columns, (), field_columns, timestamps)

        if self.show_progress and total_rows > 0:
            sys.stderr.write(
                f"\rReading TsFile metadata: {len(table_names)} table(s), {total_rows:,} rows, "
                f"{len(self.series_paths)} series ... done\n"
            )
            sys.stderr.flush()

        if not self.series_paths:
            raise ValueError("No valid numeric series found in TsFile")

    def _register_tagged_groups(
        self,
        table_name: str,
        tag_columns: List[str],
        field_columns: List[str],
        timestamps: np.ndarray,
        tag_arrays: Dict[str, list],
    ):
        tag_values_by_column = {column: np.concatenate(tag_arrays[column]) for column in tag_columns}
        if len(tag_columns) == 1:
            tag_key = tag_values_by_column[tag_columns[0]]
            for tag_value in np.unique(tag_key):
                self._register_tag_group(
                    table_name,
                    tag_columns,
                    (tag_value,),
                    field_columns,
                    timestamps[tag_key == tag_value],
                )
            return

        tag_tuples = [
            tuple(tag_values_by_column[column][row_index] for column in tag_columns)
            for row_index in range(len(timestamps))
        ]
        for tag_tuple in dict.fromkeys(tag_tuples):
            mask = np.array([value == tag_tuple for value in tag_tuples], dtype=bool)
            self._register_tag_group(table_name, tag_columns, tag_tuple, field_columns, timestamps[mask])

    def _register_tag_group(
        self,
        table_name: str,
        tag_columns: List[str],
        tag_values: tuple,
        field_columns: List[str],
        timestamps: np.ndarray,
    ):
        timestamps = np.sort(timestamps)
        if len(timestamps) == 0:
            return

        tag_values_dict = dict(zip(tag_columns, tag_values)) if tag_columns else {}
        tag_part = ".".join(str(value) for value in tag_values) if tag_columns else ""

        for field_column in field_columns:
            series_path = f"{table_name}.{tag_part}.{field_column}" if tag_part else f"{table_name}.{field_column}"
            self.series_paths.append(series_path)
            self._timestamps_cache[series_path] = timestamps
            self.series_info[series_path] = {
                "length": len(timestamps),
                "min_time": int(timestamps[0]),
                "max_time": int(timestamps[-1]),
                "table_name": table_name,
                "column_name": field_column,
                "tag_columns": tag_columns,
                "tag_values": tag_values_dict,
            }

    def read_series_by_time_range(self, series_path: str, start_time: int, end_time: int) -> Tuple[np.ndarray, np.ndarray]:
        if series_path not in self.series_info:
            raise ValueError(f"Series not found: {series_path}")

        info = self.series_info[series_path]
        timestamps, field_values = self._read_arrow(
            info["table_name"],
            [info["column_name"]],
            info["tag_columns"],
            info["tag_values"],
            start_time,
            end_time,
        )
        if len(timestamps) == 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        return timestamps, field_values[info["column_name"]]

    def read_multi_series_by_time_range(
        self,
        table_name: str,
        field_columns: List[str],
        tag_columns: List[str],
        tag_values: Dict[str, str],
        start_time: int,
        end_time: int,
    ) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
        return self._read_arrow(table_name, field_columns, tag_columns, tag_values, start_time, end_time)

    def _read_arrow(
        self,
        table_name: str,
        field_columns: List[str],
        tag_columns: List[str],
        tag_values: Dict[str, str],
        start_time: int,
        end_time: int,
    ) -> Tuple[np.ndarray, Dict[str, np.ndarray]]:
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
                    field_parts[field_column].append(arrow_table.column(field_column).to_numpy().astype(np.float64))

        if not timestamp_parts:
            return (
                np.array([], dtype=np.int64),
                {field_column: np.array([], dtype=np.float64) for field_column in field_columns},
            )

        return (
            np.concatenate(timestamp_parts).astype(np.int64),
            {field_column: np.concatenate(field_parts[field_column]) for field_column in field_columns},
        )
