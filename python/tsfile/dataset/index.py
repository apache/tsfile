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

"""Persistent mmap-backed Dataset Index v1.

The binary layout in this module is intentionally identical to
``cpp/src/dataset/dataset_index.h``.  Python owns cold-build orchestration and
public object construction; hot-path catalog lookup reads packed records
directly from a read-only mmap without recreating the old dictionary graph.
"""

from __future__ import annotations

from collections import defaultdict
import contextlib
import mmap
import os
import struct
from typing import Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple

from ..constants import ColumnCategory
from .metadata import MODEL_TREE, _join_series_path

MAGIC = b"TSIDX\0\0\0"
VERSION_MAJOR = 1
VERSION_MINOR = 0
HEADER_SIZE = 64
DIRECTORY_ENTRY_SIZE = 32
SECTION_COUNT = 13
ALIGNMENT = 64
INDEX_FILE_NAME = ".tsfile_dataframe_index.tsidx"

STRING_OFFSETS = 1
STRING_BYTES = 2
TABLE_NAME_INDEX = 3
TABLE_RECORD = 4
DEVICE_NAME_INDEX = 5
DEVICE_RECORD = 6
COLUMN_NAME_INDEX = 7
COLUMN_SCHEMA = 8
LOGICAL_SERIES = 9
TSFILE_RECORD = 10
DEVICE_FILE_SPAN = 11
SERIES_FILE_SPAN = 12
SERIES_LOCATOR = 13

HEADER = struct.Struct("<8sHHIQIIQI20s")
DIRECTORY = struct.Struct("<IIQQII")
RECORDS: Mapping[int, struct.Struct] = {
    STRING_OFFSETS: struct.Struct("<I"),
    TABLE_NAME_INDEX: struct.Struct("<QII"),
    TABLE_RECORD: struct.Struct("<IIIIIIQ"),
    DEVICE_NAME_INDEX: struct.Struct("<IIQII"),
    DEVICE_RECORD: struct.Struct("<IIIIIIIIqq"),
    COLUMN_NAME_INDEX: struct.Struct("<IIQII"),
    COLUMN_SCHEMA: struct.Struct("<IIIHHHHHHQ"),
    LOGICAL_SERIES: struct.Struct("<IIIIqq"),
    TSFILE_RECORD: struct.Struct("<IIQQQ"),
    DEVICE_FILE_SPAN: struct.Struct("<IIQIHHQ"),
    SERIES_FILE_SPAN: struct.Struct("<IIIIqqQ"),
    SERIES_LOCATOR: struct.Struct("<IHHQII"),
}


def _align64(value: int) -> int:
    return (value + ALIGNMENT - 1) & ~(ALIGNMENT - 1)


def name_hash(value: bytes) -> int:
    """Return the format-v1 FNV-1a hash (matching the C++ implementation)."""
    result = 1469598103934665603
    for byte in value:
        result ^= byte
        result = (result * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return result


_CRC32C_TABLE = []
for _value in range(256):
    _crc = _value
    for _ in range(8):
        _crc = (_crc >> 1) ^ (0x82F63B78 if _crc & 1 else 0)
    _CRC32C_TABLE.append(_crc)


def crc32c(data) -> int:
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc ^ 0xFFFFFFFF


def file_fingerprint(path: str, stat_result=None) -> int:
    """Cheap sealed-dataset generation fingerprint.

    v1 combines size and nanosecond mtime.  ReaderSession rechecks the same
    tuple before interpreting a locator; content replacement therefore cannot
    silently reuse an active generation under the static Dataset contract.
    """
    st = os.stat(path) if stat_result is None else stat_result
    return name_hash(struct.pack("<Qq", st.st_size, st.st_mtime_ns))


class _StringPool:
    def __init__(self):
        self._ids: Dict[str, int] = {}
        self._values: List[bytes] = []

    def intern(self, value: str) -> int:
        result = self._ids.get(value)
        if result is not None:
            return result
        encoded = value.encode("utf-8")
        result = len(self._values)
        self._ids[value] = result
        self._values.append(encoded)
        return result

    def sections(self) -> Tuple[bytes, bytes]:
        offsets = bytearray(RECORDS[STRING_OFFSETS].size * (len(self._values) + 1))
        contents = bytearray()
        for index, value in enumerate(self._values):
            RECORDS[STRING_OFFSETS].pack_into(offsets, index * 4, len(contents))
            contents.extend(value)
        RECORDS[STRING_OFFSETS].pack_into(offsets, len(self._values) * 4, len(contents))
        if len(contents) >= 1 << 32:
            raise OverflowError("Dataset Index v1 string pool exceeds 4 GiB")
        return bytes(offsets), bytes(contents)


def _pack_records(section_type: int, rows: Iterable[tuple]) -> bytes:
    record = RECORDS[section_type]
    rows = list(rows)
    result = bytearray(record.size * len(rows))
    for index, row in enumerate(rows):
        record.pack_into(result, index * record.size, *row)
    return bytes(result)


def write_index_atomic(path: str, section_payloads: Mapping[int, bytes]) -> None:
    """Write, fsync, validate, and atomically publish one format-v1 index."""
    if set(section_payloads) != set(range(1, SECTION_COUNT + 1)):
        raise ValueError("Dataset Index v1 requires exactly 13 sections")
    directory_offset = HEADER_SIZE
    cursor = _align64(HEADER_SIZE + SECTION_COUNT * DIRECTORY_ENTRY_SIZE)
    entries = []
    for section_type in range(1, SECTION_COUNT + 1):
        payload = section_payloads[section_type]
        record_size = 0 if section_type == STRING_BYTES else RECORDS[section_type].size
        if record_size:
            if len(payload) % record_size:
                raise ValueError(f"section {section_type} has a partial record")
            count = len(payload) // record_size
        else:
            count = len(payload)
        if count >= 1 << 32:
            raise OverflowError(f"section {section_type} count exceeds uint32")
        entries.append(
            (section_type, record_size, cursor, len(payload), count, crc32c(payload))
        )
        cursor = _align64(cursor + len(payload))

    file_length = entries[-1][2] + entries[-1][3]
    header_without_crc = HEADER.pack(
        MAGIC,
        VERSION_MAJOR,
        VERSION_MINOR,
        HEADER_SIZE,
        directory_offset,
        SECTION_COUNT,
        DIRECTORY_ENTRY_SIZE,
        file_length,
        0,
        b"\0" * 20,
    )
    header = bytearray(header_without_crc)
    struct.pack_into("<I", header, 40, crc32c(header_without_crc))

    parent = os.path.dirname(path) or "."
    os.makedirs(parent, exist_ok=True)
    temporary = f"{path}.tmp.{os.getpid()}"
    try:
        with open(temporary, "xb") as output:
            output.write(header)
            for entry in entries:
                output.write(DIRECTORY.pack(*entry))
            for entry in entries:
                padding = entry[2] - output.tell()
                if padding < 0:
                    raise AssertionError("section offsets overlap")
                if padding:
                    output.write(b"\0" * padding)
                output.write(section_payloads[entry[0]])
            output.flush()
            os.fsync(output.fileno())
        with MappedDatasetIndex(temporary, verify_sections=True):
            pass
        os.replace(temporary, path)
        if hasattr(os, "O_DIRECTORY"):
            directory_fd = os.open(parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    finally:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(temporary)


class MappedDatasetIndex:
    """Validated read-only mmap and typed zero-copy record access."""

    def __init__(self, path: str, verify_sections: bool = False):
        self.path = path
        self._file = open(path, "rb")
        try:
            stat = os.fstat(self._file.fileno())
            self.identity = (
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
            )
            self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
            self._view = memoryview(self._mmap)
            self._entries = self._validate(verify_sections)
        except Exception:
            if getattr(self, "_view", None) is not None:
                self._view.release()
                self._view = None
            if getattr(self, "_mmap", None) is not None:
                self._mmap.close()
                self._mmap = None
            self._file.close()
            raise

    def _validate(self, verify_sections: bool):
        if len(self._view) < HEADER_SIZE:
            raise ValueError("Dataset Index is shorter than its header")
        header = HEADER.unpack_from(self._view)
        (
            magic,
            major,
            minor,
            header_size,
            directory_offset,
            section_count,
            directory_entry_size,
            file_length,
            expected_crc,
            reserved,
        ) = header
        if magic != MAGIC:
            raise ValueError("bad Dataset Index magic")
        if (major, minor) != (VERSION_MAJOR, VERSION_MINOR):
            raise ValueError(f"unsupported Dataset Index version {major}.{minor}")
        if (
            header_size != HEADER_SIZE
            or directory_entry_size != DIRECTORY_ENTRY_SIZE
            or section_count != SECTION_COUNT
            or file_length != len(self._view)
            or reserved != b"\0" * 20
        ):
            raise ValueError("invalid Dataset Index header shape")
        header_copy = bytearray(self._view[:HEADER_SIZE])
        struct.pack_into("<I", header_copy, 40, 0)
        if crc32c(header_copy) != expected_crc:
            raise ValueError("Dataset Index header checksum mismatch")
        directory_end = directory_offset + section_count * directory_entry_size
        if directory_offset < HEADER_SIZE or directory_end > len(self._view):
            raise ValueError("Dataset Index directory is out of range")

        entries = {}
        previous_end = _align64(directory_end)
        for index in range(section_count):
            entry = DIRECTORY.unpack_from(
                self._view, directory_offset + index * DIRECTORY_ENTRY_SIZE
            )
            section_type, record_size, offset, length, count, checksum = entry
            if section_type != STRING_BYTES and section_type not in RECORDS:
                raise ValueError("invalid Dataset Index section type")
            expected_size = (
                0 if section_type == STRING_BYTES else RECORDS[section_type].size
            )
            if section_type != index + 1 or record_size != expected_size:
                raise ValueError("invalid Dataset Index section directory")
            if (
                offset % ALIGNMENT
                or offset < previous_end
                or offset + length > len(self._view)
                or (record_size and count * record_size > length)
                or (not record_size and count != length)
            ):
                raise ValueError("invalid Dataset Index section range")
            if (
                verify_sections
                and crc32c(self._view[offset : offset + length]) != checksum
            ):
                raise ValueError(
                    f"Dataset Index section {section_type} checksum mismatch"
                )
            entries[section_type] = entry
            previous_end = offset + length

        offsets = entries[STRING_OFFSETS]
        strings = entries[STRING_BYTES]
        if offsets[4] == 0:
            raise ValueError("StringOffsets lacks terminal offset")
        previous = 0
        for index in range(offsets[4]):
            current = RECORDS[STRING_OFFSETS].unpack_from(
                self._view, offsets[2] + index * 4
            )[0]
            if current < previous or current > strings[3]:
                raise ValueError("invalid Dataset Index string offset")
            previous = current
        if previous != strings[3]:
            raise ValueError("Dataset Index terminal string offset mismatch")
        return entries

    def close(self):
        if getattr(self, "_view", None) is not None:
            self._view.release()
            self._view = None
        if getattr(self, "_mmap", None) is not None:
            self._mmap.close()
            self._mmap = None
        if getattr(self, "_file", None) is not None:
            self._file.close()
            self._file = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def count(self, section_type: int) -> int:
        return self._entries[section_type][4]

    def record(self, section_type: int, record_id: int) -> tuple:
        entry = self._entries[section_type]
        if record_id < 0 or record_id >= entry[4]:
            raise IndexError(record_id)
        return RECORDS[section_type].unpack_from(
            self._view, entry[2] + record_id * entry[1]
        )

    def records(self, section_type: int, first: int = 0, count: Optional[int] = None):
        total = self.count(section_type)
        end = total if count is None else first + count
        if first < 0 or end < first or end > total:
            raise IndexError((first, count))
        for record_id in range(first, end):
            yield self.record(section_type, record_id)

    def string_bytes(self, sid: int) -> bytes:
        offsets = self._entries[STRING_OFFSETS]
        strings = self._entries[STRING_BYTES]
        if sid < 0 or sid + 1 >= offsets[4]:
            raise IndexError(sid)
        start = RECORDS[STRING_OFFSETS].unpack_from(self._view, offsets[2] + sid * 4)[0]
        end = RECORDS[STRING_OFFSETS].unpack_from(
            self._view, offsets[2] + (sid + 1) * 4
        )[0]
        return bytes(self._view[strings[2] + start : strings[2] + end])

    def string(self, sid: int) -> str:
        return self.string_bytes(sid).decode("utf-8")

    def _equal_hash_range(self, section_type: int, hash_index: int, value_hash: int):
        low = 0
        high = self.count(section_type)
        while low < high:
            middle = low + (high - low) // 2
            if self.record(section_type, middle)[hash_index] < value_hash:
                low = middle + 1
            else:
                high = middle
        first = low
        while (
            low < self.count(section_type)
            and self.record(section_type, low)[hash_index] == value_hash
        ):
            low += 1
        return first, low

    def find_table_ids(self, name: str) -> List[int]:
        encoded = name.encode("utf-8")
        first, end = self._equal_hash_range(TABLE_NAME_INDEX, 0, name_hash(encoded))
        return [
            table_id
            for _, sid, table_id in self.records(TABLE_NAME_INDEX, first, end - first)
            if self.string_bytes(sid) == encoded
        ]

    def _find_child(
        self, section_type: int, table_id: int, name: str, first: int, count: int
    ):
        encoded = name.encode("utf-8")
        target_hash = name_hash(encoded)
        low, high = first, first + count
        while low < high:
            middle = low + (high - low) // 2
            row = self.record(section_type, middle)
            key = (row[0], row[2], self.string_bytes(row[3]))
            target = (table_id, target_hash, encoded)
            if key < target:
                low = middle + 1
            else:
                high = middle
        if low < first + count:
            row = self.record(section_type, low)
            if (
                row[0] == table_id
                and row[2] == target_hash
                and self.string_bytes(row[3]) == encoded
            ):
                return row[1]
        raise KeyError(name)

    def find_device_id(self, table_id: int, name: str) -> int:
        table = self.record(TABLE_RECORD, table_id)
        return self._find_child(DEVICE_NAME_INDEX, table_id, name, table[2], table[3])

    def find_column_id(self, table_id: int, name: str) -> int:
        table = self.record(TABLE_RECORD, table_id)
        return self._find_child(COLUMN_NAME_INDEX, table_id, name, table[4], table[5])

    def find_series_id(self, device_id: int, column_id: int) -> int:
        device = self.record(DEVICE_RECORD, device_id)
        low, high = device[4], device[4] + device[5]
        while low < high:
            middle = low + (high - low) // 2
            if self.record(LOGICAL_SERIES, middle)[1] < column_id:
                low = middle + 1
            else:
                high = middle
        if (
            low < device[4] + device[5]
            and self.record(LOGICAL_SERIES, low)[1] == column_id
        ):
            return low
        raise KeyError(column_id)


def index_path_for(paths: Sequence[str]) -> str:
    common = os.path.commonpath([os.path.abspath(path) for path in paths])
    if not os.path.isdir(common):
        common = os.path.dirname(common)
    return os.path.join(common, INDEX_FILE_NAME)


def index_matches_paths(path: str, paths: Sequence[str]) -> bool:
    """Return whether an index describes exactly the current sealed files."""
    expected = sorted(os.path.abspath(item) for item in paths)
    try:
        with MappedDatasetIndex(path) as index:
            if index.count(TSFILE_RECORD) != len(expected):
                return False
            actual = []
            for file_id in range(index.count(TSFILE_RECORD)):
                record = index.record(TSFILE_RECORD, file_id)
                file_path = index.string(record[0])
                actual.append(file_path)
                st = os.stat(file_path)
                if (
                    st.st_size != record[2]
                    or file_fingerprint(file_path, st) != record[3]
                ):
                    return False
            return actual == expected
    except (OSError, ValueError, UnicodeError, IndexError):
        return False


def build_sections_from_dataframe(dataframe) -> Mapping[int, bytes]:
    """Convert one fully scanned legacy DataFrame into deterministic v1 sections.

    This is the cold-build bridge.  Once published, subsequent constructions
    use :class:`MappedDatasetIndex` and do not recreate this object graph.
    """
    index = dataframe._index
    pool = _StringPool()
    file_paths = sorted(dataframe._readers)
    file_ids = {path: file_id for file_id, path in enumerate(file_paths)}

    table_names = sorted(index.table_entries)
    table_ids = {name: table_id for table_id, name in enumerate(table_names)}
    field_types: Dict[Tuple[str, str], int] = {}
    for series_ref, shards in index.series_shards.items():
        device_idx, field_idx = series_ref
        table_name, _ = index.devices[device_idx]
        table_entry = index.table_entries[table_name]
        field_name = table_entry.field_columns[field_idx]
        declared_type = (
            int(table_entry.field_types[field_idx])
            if field_idx < len(table_entry.field_types)
            else None
        )
        for reader, local_device, local_field in shards:
            stats = reader.catalog.series_stats_by_ref[(local_device, local_field)]
            if declared_type is not None and declared_type != stats.data_type:
                raise ValueError(
                    f"physical type for {table_name}.{field_name} does not match "
                    "its canonical TableSchema"
                )
            current = field_types.setdefault((table_name, field_name), stats.data_type)
            if current != stats.data_type:
                raise ValueError(
                    f"incompatible physical type for {table_name}.{field_name}"
                )

    columns = []
    columns_by_table = defaultdict(list)
    for table_name in table_names:
        table_id = table_ids[table_name]
        table = index.table_entries[table_name]
        if table.schema_columns:
            definitions = [
                (ordinal, name, data_type, category)
                for ordinal, (name, data_type, category) in enumerate(
                    table.schema_columns
                )
                if category != int(ColumnCategory.TIME)
            ]
        else:
            definitions = [
                (ordinal, name, int(data_type), int(ColumnCategory.TAG))
                for ordinal, (name, data_type) in enumerate(
                    zip(table.tag_columns, table.tag_types)
                )
            ]
            first_field_ordinal = len(definitions)
            definitions.extend(
                [
                    (
                        first_field_ordinal + ordinal,
                        name,
                        field_types.get((table_name, name), -1),
                        int(ColumnCategory.FIELD),
                    )
                    for ordinal, name in enumerate(table.field_columns)
                ]
            )
        for ordinal, name, data_type, role in definitions:
            column_id = len(columns)
            columns.append(
                (
                    table_id,
                    pool.intern(name),
                    ordinal,
                    data_type,
                    data_type,
                    0,
                    0,
                    role,
                    1,
                    0,
                )
            )
            columns_by_table[table_id].append((name, column_id))

    device_specs = []
    old_to_new_device = {}
    for old_device_id, (table_name, tags) in enumerate(index.devices):
        canonical_name = _join_series_path(table_name, tags, "")
        device_specs.append(
            (table_ids[table_name], canonical_name, old_device_id, tags)
        )
    device_specs.sort(key=lambda item: (item[0], item[1].encode("utf-8")))
    for new_device_id, (_, _, old_device_id, _) in enumerate(device_specs):
        old_to_new_device[old_device_id] = new_device_id

    series_specs = []
    for old_device_id, field_idx in index.series:
        table_name, _ = index.devices[old_device_id]
        field_name = index.table_entries[table_name].field_columns[field_idx]
        column_id = dict(columns_by_table[table_ids[table_name]])[field_name]
        series_specs.append(
            (old_to_new_device[old_device_id], column_id, (old_device_id, field_idx))
        )
    series_specs.sort(key=lambda item: (item[0], item[1]))
    old_series_to_new = {old: new for new, (_, _, old) in enumerate(series_specs)}

    locators = []
    series_span_rows = []
    device_span_specs = {}
    series_spans_by_id = defaultdict(list)
    device_spans_by_device = defaultdict(list)

    for _, _, old_series_ref in series_specs:
        series_id = old_series_to_new[old_series_ref]
        old_device_id, _ = old_series_ref
        device_id = old_to_new_device[old_device_id]
        for reader, local_device, local_field in index.series_shards[old_series_ref]:
            file_id = file_ids[reader.file_path]
            stats = reader.catalog.series_stats_by_ref[(local_device, local_field)]
            if stats.value_metadata_length <= 0:
                raise ValueError(
                    f"missing exact TimeseriesMetadata locator in {reader.file_path}"
                )
            if stats.layout and not (stats.locator_flags & 1):
                raise ValueError(
                    f"aligned time/value chunk metadata mismatch in {reader.file_path}"
                )
            span_key = (device_id, file_id)
            candidate = (
                stats.time_metadata_offset if stats.layout else 0,
                stats.time_metadata_length if stats.layout else 0,
                stats.layout,
                1 if stats.layout and stats.locator_flags & 1 else 0,
                stats.timeline_length if stats.layout else 0,
            )
            previous = device_span_specs.get(span_key)
            if previous is None:
                device_span_specs[span_key] = candidate
            else:
                if previous[:4] != candidate[:4]:
                    raise ValueError(
                        "inconsistent aligned time locator for one device/file"
                    )
                if previous[4] != candidate[4]:
                    raise ValueError("inconsistent aligned timeline row count")
            locator_id = len(locators)
            locators.append(
                (
                    span_key,
                    stats.layout,
                    0,
                    stats.value_metadata_offset,
                    stats.value_metadata_length,
                    0,
                )
            )
            series_spans_by_id[series_id].append(
                [
                    series_id,
                    file_id,
                    locator_id,
                    0,
                    stats.timeline_min_time,
                    stats.timeline_max_time,
                    stats.length if not stats.layout else 0,
                ]
            )

    device_span_rows = []
    device_span_ids = {}
    for key, values in sorted(device_span_specs.items()):
        device_id, file_id = key
        device_span_ids[key] = len(device_span_rows)
        device_span_rows.append((device_id, file_id, *values))
        device_spans_by_device[device_id].append(device_span_ids[key])
    locator_rows = [
        (device_span_ids[key], kind, flags, offset, length, padding)
        for key, kind, flags, offset, length, padding in locators
    ]

    logical_series_rows = []
    for series_id, (device_id, column_id, _) in enumerate(series_specs):
        spans = sorted(series_spans_by_id[series_id], key=lambda row: (row[4], row[1]))
        first = len(series_span_rows)
        for row in spans:
            series_span_rows.append(tuple(row))
        logical_series_rows.append(
            (
                device_id,
                column_id,
                first,
                len(spans),
                min(row[4] for row in spans),
                max(row[5] for row in spans),
            )
        )

    device_rows = []
    device_names_by_table = defaultdict(list)
    series_by_device = defaultdict(list)
    for series_id, row in enumerate(logical_series_rows):
        series_by_device[row[0]].append(series_id)
    device_span_flat = []
    # DeviceFileSpan is already globally sorted by device, so each device range
    # is contiguous and its first id can be recorded directly.
    for device_id, (table_id, canonical_name, _, _) in enumerate(device_specs):
        name_sid = pool.intern(canonical_name)
        device_names_by_table[table_id].append((canonical_name, device_id, name_sid))
        series_ids = series_by_device[device_id]
        span_ids = device_spans_by_device[device_id]
        first_series = series_ids[0] if series_ids else 0
        first_span = span_ids[0] if span_ids else 0
        if series_ids:
            minimum = min(logical_series_rows[sid][4] for sid in series_ids)
            maximum = max(logical_series_rows[sid][5] for sid in series_ids)
        else:
            minimum = maximum = 0
        device_rows.append(
            (
                table_id,
                name_sid,
                0,
                0,
                first_series,
                len(series_ids),
                first_span,
                len(span_ids),
                minimum,
                maximum,
            )
        )

    device_name_rows = []
    column_name_rows = []
    table_rows = []
    table_name_rows = []
    for table_name in table_names:
        table_id = table_ids[table_name]
        table = index.table_entries[table_name]
        name_sid = pool.intern(table_name)
        table_name_rows.append(
            (name_hash(table_name.encode("utf-8")), name_sid, table_id)
        )
        first_device = len(device_name_rows)
        names = sorted(
            device_names_by_table[table_id],
            key=lambda item: (
                name_hash(item[0].encode("utf-8")),
                item[0].encode("utf-8"),
            ),
        )
        for name, device_id, sid in names:
            device_name_rows.append(
                (table_id, device_id, name_hash(name.encode("utf-8")), sid, 0)
            )
        first_column = len(column_name_rows)
        column_names = sorted(
            columns_by_table[table_id],
            key=lambda item: (
                name_hash(item[0].encode("utf-8")),
                item[0].encode("utf-8"),
            ),
        )
        for name, column_id in column_names:
            column_name_rows.append(
                (
                    table_id,
                    column_id,
                    name_hash(name.encode("utf-8")),
                    pool.intern(name),
                    0,
                )
            )
        table_rows.append(
            (
                name_sid,
                0,
                first_device,
                len(names),
                first_column,
                len(column_names),
                0,
            )
        )
    table_name_rows.sort(key=lambda row: (row[0], pool._values[row[1]], row[2]))

    file_rows = []
    for file_id, path in enumerate(file_paths):
        st = os.stat(path)
        file_rows.append(
            (
                pool.intern(path),
                0,
                st.st_size,
                file_fingerprint(path, st),
                0,
            )
        )

    string_offsets, string_bytes = pool.sections()
    return {
        STRING_OFFSETS: string_offsets,
        STRING_BYTES: string_bytes,
        TABLE_NAME_INDEX: _pack_records(TABLE_NAME_INDEX, table_name_rows),
        TABLE_RECORD: _pack_records(TABLE_RECORD, table_rows),
        DEVICE_NAME_INDEX: _pack_records(DEVICE_NAME_INDEX, device_name_rows),
        DEVICE_RECORD: _pack_records(DEVICE_RECORD, device_rows),
        COLUMN_NAME_INDEX: _pack_records(COLUMN_NAME_INDEX, column_name_rows),
        COLUMN_SCHEMA: _pack_records(COLUMN_SCHEMA, columns),
        LOGICAL_SERIES: _pack_records(LOGICAL_SERIES, logical_series_rows),
        TSFILE_RECORD: _pack_records(TSFILE_RECORD, file_rows),
        DEVICE_FILE_SPAN: _pack_records(DEVICE_FILE_SPAN, device_span_rows),
        SERIES_FILE_SPAN: _pack_records(SERIES_FILE_SPAN, series_span_rows),
        SERIES_LOCATOR: _pack_records(SERIES_LOCATOR, locator_rows),
    }


def build_index_from_dataframe(dataframe, path: Optional[str] = None) -> str:
    path = index_path_for(dataframe._paths) if path is None else path
    write_index_atomic(path, build_sections_from_dataframe(dataframe))
    return path
