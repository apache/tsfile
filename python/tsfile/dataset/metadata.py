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

"""Shared metadata models for dataset readers and views."""

from dataclasses import dataclass, field
import sys
from typing import Any, Dict, Iterable, Iterator, List, Tuple

from ..constants import TSDataType


_PATH_SEPARATOR = "."
_PATH_ESCAPE = "\\"
_DATACLASS_SLOTS = {"slots": True} if sys.version_info >= (3, 10) else {}

@dataclass(**_DATACLASS_SLOTS)
class TableEntry:
    """Schema-level metadata shared by every device in one table."""

    table_name: str
    tag_columns: Tuple[str, ...]
    tag_types: Tuple[TSDataType, ...]
    field_columns: Tuple[str, ...]
    _field_index_by_name: Dict[str, int] = field(init=False, repr=False)

    def __post_init__(self):
        self._field_index_by_name = {column: idx for idx, column in enumerate(self.field_columns)}

    def get_field_index(self, field_name: str) -> int:
        if field_name not in self._field_index_by_name:
            raise ValueError(f"Field not found in table '{self.table_name}': {field_name}")
        return self._field_index_by_name[field_name]


@dataclass(**_DATACLASS_SLOTS)
class DeviceEntry:
    """One logical device identified by table_id + ordered tag values.

    The table_id refers to MetadataCatalog.table_entries[table_id].
    """

    table_id: int
    tag_values: Tuple[Any, ...]
    min_time: int
    max_time: int


@dataclass(**_DATACLASS_SLOTS)
class MetadataCatalog:
    """Canonical metadata store shared by dataset readers and dataframes."""

    table_entries: List[TableEntry] = field(default_factory=list)
    device_entries: List[DeviceEntry] = field(default_factory=list)
    table_id_by_name: Dict[str, int] = field(default_factory=dict)
    device_id_by_key: Dict[Tuple[int, tuple], int] = field(default_factory=dict)
    series_stats_by_ref: Dict[Tuple[int, int], Dict[str, int]] = field(default_factory=dict)

    def add_table(
        self,
        table_name: str,
        tag_columns: Iterable[str],
        tag_types: Iterable[TSDataType],
        field_columns: Iterable[str],
    ) -> int:
        table_id = len(self.table_entries)
        self.table_entries.append(
            TableEntry(
                table_name=table_name,
                tag_columns=tuple(tag_columns),
                tag_types=tuple(tag_types),
                field_columns=tuple(field_columns),
            )
        )
        self.table_id_by_name[table_name] = table_id
        return table_id

    def add_device(
        self,
        table_id: int,
        tag_values: tuple,
        min_time: int,
        max_time: int,
    ) -> int:
        key = (table_id, tuple(tag_values))
        if key in self.device_id_by_key:
            return self.device_id_by_key[key]

        device_id = len(self.device_entries)
        self.device_entries.append(
            DeviceEntry(
                table_id=table_id,
                tag_values=tuple(tag_values),
                min_time=min_time,
                max_time=max_time,
            )
        )
        self.device_id_by_key[key] = device_id
        return device_id

    @property
    def series_count(self) -> int:
        return sum(len(self.table_entries[device.table_id].field_columns) for device in self.device_entries)


def _escape_path_component(value: Any) -> str:
    return str(value).replace(_PATH_ESCAPE, _PATH_ESCAPE * 2).replace(_PATH_SEPARATOR, _PATH_ESCAPE + _PATH_SEPARATOR)


def split_logical_series_path(series_path: str) -> List[str]:
    parts = []
    current = []
    escaping = False

    for char in series_path:
        if escaping:
            current.append(char)
            escaping = False
            continue
        if char == _PATH_ESCAPE:
            escaping = True
            continue
        if char == _PATH_SEPARATOR:
            parts.append("".join(current))
            current = []
            continue
        current.append(char)

    if escaping:
        raise ValueError(f"Invalid series path: {series_path}")

    parts.append("".join(current))
    return parts


def build_logical_series_path(table_name: str, tag_values: Iterable[Any], field_name: str) -> str:
    components = [table_name, *tag_values, field_name]
    return _PATH_SEPARATOR.join(_escape_path_component(component) for component in components)


def build_series_path(catalog: MetadataCatalog, device_id: int, field_idx: int) -> str:
    """Return the external logical series name for one device field."""
    device_entry = catalog.device_entries[device_id]
    table_entry = catalog.table_entries[device_entry.table_id]
    field_name = table_entry.field_columns[field_idx]
    return build_logical_series_path(table_entry.table_name, device_entry.tag_values, field_name)


def iter_series_refs(catalog: MetadataCatalog) -> Iterator[Tuple[int, int]]:
    """Yield ``(device_id, field_idx)`` pairs in catalog order."""
    for device_id, device_entry in enumerate(catalog.device_entries):
        table_entry = catalog.table_entries[device_entry.table_id]
        for field_idx in range(len(table_entry.field_columns)):
            yield device_id, field_idx


def iter_series_paths(catalog: MetadataCatalog) -> Iterator[str]:
    """Yield logical series names in catalog order."""
    for device_id, field_idx in iter_series_refs(catalog):
        yield build_series_path(catalog, device_id, field_idx)


def resolve_series_path(catalog: MetadataCatalog, series_path: str) -> Tuple[int, int, int]:
    """Resolve an external path to ``(table_id, device_id, field_idx)``."""
    parts = split_logical_series_path(series_path)
    if len(parts) < 2:
        raise ValueError(f"Invalid series path: {series_path}")

    table_name = parts[0]
    if table_name not in catalog.table_id_by_name:
        raise ValueError(f"Series not found: {series_path}")

    table_id = catalog.table_id_by_name[table_name]
    table_entry = catalog.table_entries[table_id]
    expected_parts = len(table_entry.tag_columns) + 2
    if len(parts) > expected_parts:
        raise ValueError(f"Series not found: {series_path}")

    field_name = parts[-1]
    try:
        field_idx = table_entry.get_field_index(field_name)
    except ValueError as exc:
        raise ValueError(f"Series not found: {series_path}") from exc

    tag_values = tuple(
        _coerce_path_component(raw_value, tag_type)
        for raw_value, tag_type in zip(parts[1:-1], table_entry.tag_types)
    )
    key = (table_id, tag_values)
    if key not in catalog.device_id_by_key:
        raise ValueError(f"Series not found: {series_path}")

    return table_id, catalog.device_id_by_key[key], field_idx


def _coerce_path_component(value: str, data_type: TSDataType) -> Any:
    if data_type in {TSDataType.STRING, TSDataType.TEXT, TSDataType.BLOB}:
        return value
    if data_type == TSDataType.BOOLEAN:
        lowered = value.lower()
        if lowered == "true":
            return True
        if lowered == "false":
            return False
        raise ValueError(f"Invalid boolean tag value: {value}")
    if data_type in {TSDataType.INT32, TSDataType.INT64, TSDataType.TIMESTAMP, TSDataType.DATE}:
        return int(value)
    if data_type in {TSDataType.FLOAT, TSDataType.DOUBLE}:
        return float(value)
    return value
