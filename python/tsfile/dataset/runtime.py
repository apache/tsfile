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

"""Process-local Runtime over the persistent Dataset Index."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping, Sequence
import contextlib
import os
import threading
from typing import Dict, Optional

import numpy as np

from ..constants import TSDataType
from ..tag_filter import tag_eq, tag_is_null
from ..tsfile_reader import TsFileReaderPy
from .index import (
    COLUMN_NAME_INDEX,
    COLUMN_SCHEMA,
    DEVICE_FILE_SPAN,
    DEVICE_RECORD,
    LOGICAL_SERIES,
    SERIES_FILE_SPAN,
    SERIES_LOCATOR,
    TABLE_RECORD,
    TSFILE_RECORD,
    MappedDatasetIndex,
    file_fingerprint,
)
from .metadata import MODEL_TABLE, MODEL_TREE, TableEntry, split_logical_series_path


def _exact_tag_filter(tag_columns, tag_values):
    result = None
    for index, name in enumerate(tag_columns):
        value = tag_values[index] if index < len(tag_values) else None
        current = tag_is_null(name) if value is None else tag_eq(name, str(value))
        result = current if result is None else result & current
    return result


class RuntimeLease:
    def __init__(self, runtime: "DatasetRuntime"):
        self._runtime = runtime
        self._closed = False
        runtime._acquire_object()

    def clone(self):
        if self._closed:
            raise RuntimeError("Runtime lease is closed")
        return RuntimeLease(self._runtime)

    def close(self):
        if not self._closed:
            self._closed = True
            self._runtime._release_object()


class _QueryLease:
    def __init__(self, runtime: "DatasetRuntime"):
        self._runtime = runtime

    def __enter__(self):
        self._runtime._acquire_query()
        return self

    def __exit__(self, *_):
        self._runtime._release_query()


class _ReaderSession:
    def __init__(self, file_id: int, path: str, expected_size: int, fingerprint: int):
        st = os.stat(path)
        if st.st_size != expected_size or file_fingerprint(path, st) != fingerprint:
            raise RuntimeError(
                f"TsFile generation changed after Dataset Index publication: {path}"
            )
        self.file_id = file_id
        self.path = path
        self.reader = TsFileReaderPy(path)
        self.active_uses = 0

    def close(self):
        self.reader.close()


class ReaderSessionPool:
    """Per-Runtime LRU pool with a hard cap on simultaneously open Readers."""

    def __init__(self, index: MappedDatasetIndex, max_open_files: int):
        self._index = index
        self.max_open_files = max(1, int(max_open_files))
        self._sessions: "OrderedDict[int, _ReaderSession]" = OrderedDict()
        self._condition = threading.Condition()
        self._closed = False

    def _new_session(self, file_id: int):
        record = self._index.record(TSFILE_RECORD, file_id)
        return _ReaderSession(
            file_id,
            self._index.string(record[0]),
            record[2],
            record[5],
        )

    @contextlib.contextmanager
    def acquire(self, file_id: int):
        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("ReaderSessionPool is closed")
                session = self._sessions.get(file_id)
                if session is not None:
                    self._sessions.move_to_end(file_id)
                    session.active_uses += 1
                    break
                if len(self._sessions) < self.max_open_files:
                    session = self._new_session(file_id)
                    session.active_uses = 1
                    self._sessions[file_id] = session
                    break
                idle_id = next(
                    (
                        key
                        for key, value in self._sessions.items()
                        if value.active_uses == 0
                    ),
                    None,
                )
                if idle_id is None:
                    self._condition.wait()
                    continue
                idle = self._sessions.pop(idle_id)
                idle.close()
                session = self._new_session(file_id)
                session.active_uses = 1
                self._sessions[file_id] = session
                break
        try:
            yield session.reader
        finally:
            with self._condition:
                session.active_uses -= 1
                self._condition.notify_all()

    def close(self):
        with self._condition:
            self._closed = True
            while any(session.active_uses for session in self._sessions.values()):
                self._condition.wait()
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            session.close()

    @property
    def open_count(self):
        with self._condition:
            return len(self._sessions)


class PreparedSeriesCache:
    """Runtime-wide single-flight cache of native exact-locator metadata."""

    def __init__(self, index: MappedDatasetIndex):
        self._index = index
        self._condition = threading.Condition()
        self._entries = {}
        self._loading = set()
        self._closed = False

    def _locator_tuple(self, file_id, locator_id):
        locator = self._index.record(SERIES_LOCATOR, locator_id)
        device_span = self._index.record(DEVICE_FILE_SPAN, locator[0])
        file_record = self._index.record(TSFILE_RECORD, file_id)
        if device_span[1] != file_id:
            raise ValueError("series locator points at another TsFile")
        return (
            id(self._index),
            file_id,
            file_record[2],
            file_record[5],
            locator_id,
            locator[1],
            locator[2],
            locator[3],
            locator[4],
            device_span[2],
            device_span[3],
            locator[5],
        )

    def get(self, file_id, locator_id, reader):
        key = (id(self._index), file_id, locator_id)
        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("PreparedSeriesCache is closed")
                result = self._entries.get(key)
                if result is not None:
                    return result
                if key not in self._loading:
                    self._loading.add(key)
                    break
                self._condition.wait()
        try:
            result = reader.prepare_series(self._locator_tuple(file_id, locator_id))
        except Exception:
            with self._condition:
                self._loading.remove(key)
                self._condition.notify_all()
            raise
        with self._condition:
            if self._closed:
                result.close()
                self._loading.remove(key)
                self._condition.notify_all()
                raise RuntimeError("PreparedSeriesCache is closed")
            self._entries[key] = result
            self._loading.remove(key)
            self._condition.notify_all()
            return result

    def close(self):
        with self._condition:
            self._closed = True
            while self._loading:
                self._condition.wait()
            entries = list(self._entries.values())
            self._entries.clear()
        for prepared in entries:
            prepared.close()

    @property
    def size(self):
        with self._condition:
            return len(self._entries)


class DatasetRuntime:
    def __init__(self, path: str, max_open_files: Optional[int] = None):
        self.index = MappedDatasetIndex(path)
        maximum = (
            int(os.environ.get("TSFILE_DATAFRAME_MAX_OPEN_FILES", "16"))
            if max_open_files is None
            else max_open_files
        )
        self.readers = ReaderSessionPool(self.index, maximum)
        self.prepared = PreparedSeriesCache(self.index)
        self._condition = threading.Condition()
        self._object_leases = 0
        self._query_leases = 0
        self._accepting = True
        self._torn_down = False
        self.catalog = MappedDataFrameCatalog(self)

    def lease(self):
        return RuntimeLease(self)

    def query_lease(self):
        return _QueryLease(self)

    def _acquire_object(self):
        with self._condition:
            if not self._accepting:
                raise RuntimeError("Dataset Runtime is closing")
            self._object_leases += 1

    def _release_object(self):
        teardown = False
        with self._condition:
            self._object_leases -= 1
            if self._object_leases == 0:
                self._accepting = False
                while self._query_leases:
                    self._condition.wait()
                teardown = not self._torn_down
                self._torn_down = True
        if teardown:
            self.prepared.close()
            self.readers.close()
            self.index.close()

    def _acquire_query(self):
        with self._condition:
            if not self._accepting:
                raise RuntimeError("Dataset Runtime is closing")
            self._query_leases += 1

    def _release_query(self):
        with self._condition:
            self._query_leases -= 1
            self._condition.notify_all()


class _TableMapping(Mapping):
    def __init__(self, catalog: "MappedDataFrameCatalog"):
        self._catalog = catalog
        self._names = {}
        self._cache = {}
        for table_id in range(catalog.index.count(TABLE_RECORD)):
            record = catalog.index.record(TABLE_RECORD, table_id)
            name = catalog.index.string(record[0])
            if name in self._names:
                raise ValueError(
                    f"Schema variant for table '{name}' is ambiguous without a fingerprint"
                )
            self._names[name] = table_id

    def __getitem__(self, name):
        result = self._cache.get(name)
        if result is not None:
            return result
        table_id = self._names[name]
        table = self._catalog.index.record(TABLE_RECORD, table_id)
        tags = []
        tag_types = []
        fields = []
        for name_index in self._catalog.index.records(
            COLUMN_NAME_INDEX, table[5], table[6]
        ):
            column = self._catalog.index.record(COLUMN_SCHEMA, name_index[1])
            column_name = self._catalog.index.string(column[1])
            if column[7] == 0:
                tags.append((column[2], column_name, TSDataType(column[3])))
            elif column[7] == 1:
                fields.append((column[2], column_name))
        tags.sort()
        fields.sort()
        result = TableEntry(
            name,
            tuple(item[1] for item in tags),
            tuple(item[2] for item in tags),
            tuple(item[1] for item in fields),
        )
        self._cache[name] = result
        return result

    def __iter__(self):
        return iter(self._names)

    def __len__(self):
        return len(self._names)

    def table_id(self, name):
        return self._names[name]


class _DeviceSequence(Sequence):
    def __init__(self, catalog):
        self._catalog = catalog

    def __len__(self):
        return self._catalog.index.count(DEVICE_RECORD)

    def __getitem__(self, device_id):
        if isinstance(device_id, slice):
            return [self[index] for index in range(*device_id.indices(len(self)))]
        record = self._catalog.index.record(DEVICE_RECORD, device_id)
        table = self._catalog.index.record(TABLE_RECORD, record[0])
        table_name = self._catalog.index.string(table[0])
        components = split_logical_series_path(self._catalog.index.string(record[1]))
        return table_name, tuple(components[1:-1])


class _DeviceIndexMapping(Mapping):
    def __init__(self, catalog):
        self._catalog = catalog

    def get(self, key, default=None):
        table_name, tags = key
        table_ids = self._catalog.index.find_table_ids(table_name)
        if len(table_ids) != 1:
            return default
        from .metadata import _join_series_path

        try:
            return self._catalog.index.find_device_id(
                table_ids[0], _join_series_path(table_name, tags, "")
            )
        except KeyError:
            return default

    def __getitem__(self, key):
        result = self.get(key)
        if result is None:
            raise KeyError(key)
        return result

    def __iter__(self):
        return iter(self._catalog.devices)

    def __len__(self):
        return len(self._catalog.devices)


class _SeriesSequence(Sequence):
    def __init__(self, catalog, ids=None):
        self._catalog = catalog
        self._ids = ids

    def __len__(self):
        return (
            self._catalog.index.count(LOGICAL_SERIES)
            if self._ids is None
            else len(self._ids)
        )

    def series_id(self, position):
        return position if self._ids is None else self._ids[position]

    def __getitem__(self, position):
        if isinstance(position, slice):
            return [self[index] for index in range(*position.indices(len(self)))]
        series_id = self.series_id(position)
        series = self._catalog.index.record(LOGICAL_SERIES, series_id)
        device = self._catalog.index.record(DEVICE_RECORD, series[0])
        table_name = self._catalog.index.string(
            self._catalog.index.record(TABLE_RECORD, device[0])[0]
        )
        table = self._catalog.table_entries[table_name]
        column_name = self._catalog.index.string(
            self._catalog.index.record(COLUMN_SCHEMA, series[1])[1]
        )
        return series[0], table.get_field_index(column_name)

    def __iter__(self):
        for index in range(len(self)):
            yield self[index]


class _RouteMapping(Mapping):
    def __init__(self, catalog):
        self._catalog = catalog

    def _series_id(self, ref):
        device_id, field_idx = ref
        device = self._catalog.index.record(DEVICE_RECORD, device_id)
        table_name = self._catalog.index.string(
            self._catalog.index.record(TABLE_RECORD, device[0])[0]
        )
        field_name = self._catalog.table_entries[table_name].field_columns[field_idx]
        column_id = self._catalog.index.find_column_id(device[0], field_name)
        return self._catalog.index.find_series_id(device_id, column_id)

    def __contains__(self, ref):
        try:
            self._series_id(ref)
            return True
        except KeyError:
            return False

    def __getitem__(self, ref):
        series_id = self._series_id(ref)
        series = self._catalog.index.record(LOGICAL_SERIES, series_id)
        result = []
        for span_id in range(series[2], series[2] + series[3]):
            span = self._catalog.index.record(SERIES_FILE_SPAN, span_id)
            result.append((self._catalog.reader_for(span[1]), series[0], series[1]))
        return result

    def __iter__(self):
        return iter(self._catalog.series)

    def __len__(self):
        return len(self._catalog.series)


class MappedDataFrameCatalog:
    def __init__(self, runtime: DatasetRuntime):
        self.runtime = runtime
        self.index = runtime.index
        self.table_entries = _TableMapping(self)
        self.devices = _DeviceSequence(self)
        self.device_index = _DeviceIndexMapping(self)
        self.device_time_bounds = _DeviceTimeBounds(self)
        self.series = _SeriesSequence(self)
        self.series_shards = _RouteMapping(self)
        self._readers = {}
        self.model = self._infer_model()

    def _infer_model(self):
        if len(self.table_entries) != 1:
            return MODEL_TABLE
        table = next(iter(self.table_entries.values()))
        if table.tag_columns == tuple(
            f"_col_{index + 1}" for index in range(len(table.tag_columns))
        ):
            return MODEL_TREE
        return MODEL_TABLE

    def reader_for(self, file_id):
        reader = self._readers.get(file_id)
        if reader is None:
            reader = RuntimeSeriesReader(self.runtime, file_id)
            self._readers[file_id] = reader
        return reader


class _DeviceTimeBounds(Sequence):
    def __init__(self, catalog):
        self._catalog = catalog

    def __len__(self):
        return len(self._catalog.devices)

    def __getitem__(self, device_id):
        record = self._catalog.index.record(DEVICE_RECORD, device_id)
        return record[8], record[9]


class RuntimeSeriesReader:
    """File-specific facade whose metadata comes from mmap, not a Python catalog."""

    def __init__(self, runtime: DatasetRuntime, file_id: int):
        self.runtime = runtime
        self.file_id = file_id

    def _series(self, device_id, column_id):
        return self.runtime.index.find_series_id(device_id, column_id)

    def _span(self, device_id, column_id):
        series_id = self._series(device_id, column_id)
        series = self.runtime.index.record(LOGICAL_SERIES, series_id)
        for span_id in range(series[2], series[2] + series[3]):
            span = self.runtime.index.record(SERIES_FILE_SPAN, span_id)
            if span[1] == self.file_id:
                return span
        raise KeyError((device_id, column_id, self.file_id))

    def _identity(self, device_id, column_id):
        index = self.runtime.index
        device = index.record(DEVICE_RECORD, device_id)
        table = index.record(TABLE_RECORD, device[0])
        table_name = index.string(table[0])
        components = split_logical_series_path(index.string(device[1]))
        tags = tuple(components[1:-1])
        table_entry = self.runtime.catalog.table_entries[table_name]
        column_name = index.string(index.record(COLUMN_SCHEMA, column_id)[1])
        return table_name, tags, table_entry, column_name

    def get_device_info(self, device_id):
        record = self.runtime.index.record(DEVICE_RECORD, device_id)
        table_name, tags = self.runtime.catalog.devices[device_id]
        table = self.runtime.catalog.table_entries[table_name]
        return {
            "table_name": table_name,
            "tag_columns": table.tag_columns,
            "tag_values": dict(zip(table.tag_columns, tags)),
            "min_time": record[8],
            "max_time": record[9],
        }

    def get_series_info_by_ref(self, device_id, column_id):
        span = self._span(device_id, column_id)
        locator = self.runtime.index.record(SERIES_LOCATOR, span[2])
        device_span = self.runtime.index.record(DEVICE_FILE_SPAN, locator[0])
        table_name, tags, table, column_name = self._identity(device_id, column_id)
        timeline_length = device_span[8] if device_span[4] == 1 else span[7]
        return {
            "length": timeline_length,
            "min_time": span[4],
            "max_time": span[5],
            "timeline_length": timeline_length,
            "timeline_min_time": span[4],
            "timeline_max_time": span[5],
            "table_name": table_name,
            "column_name": column_name,
            "device_id": device_id,
            "field_idx": column_id,
            "tag_columns": table.tag_columns,
            "tag_values": dict(zip(table.tag_columns, tags)),
        }

    @staticmethod
    def _consume(result):
        timestamps = []
        values = []
        with result:
            while result.next():
                timestamp = result.get_value_by_index(1)
                value = result.get_value_by_index(2)
                timestamps.append(int(timestamp))
                values.append(np.nan if value is None else float(value))
        return np.asarray(timestamps, dtype=np.int64), np.asarray(
            values, dtype=np.float64
        )

    def _query(
        self,
        device_id,
        column_id,
        start_time=None,
        end_time=None,
        offset=None,
        limit=None,
    ):
        span = self._span(device_id, column_id)
        with self.runtime.query_lease(), self.runtime.readers.acquire(
            self.file_id
        ) as reader:
            prepared = self.runtime.prepared.get(self.file_id, span[2], reader)
            if offset is None:
                result = reader.query_prepared(
                    prepared, start_time=start_time, end_time=end_time
                )
            else:
                result = reader.query_prepared(prepared, offset=offset, limit=limit)
            return self._consume(result)

    def read_series_by_ref(self, device_id, column_id, start_time, end_time):
        return self._query(device_id, column_id, start_time, end_time)

    def read_series_by_row(self, device_id, column_id, offset, limit):
        if limit <= 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        return self._query(device_id, column_id, offset=offset, limit=limit)

    def read_device_fields_by_time_range(
        self, device_id, column_ids, start_time, end_time
    ):
        parts = [
            self.read_series_by_ref(device_id, column_id, start_time, end_time)
            for column_id in column_ids
        ]
        if not parts:
            return np.array([], dtype=np.int64), {}
        # Existing dataframe merge aligns separate field parts across files;
        # this method is only a compatibility surface for a single file.
        timestamps = parts[0][0]
        values = {}
        for column_id, (current_timestamps, current_values) in zip(column_ids, parts):
            if not np.array_equal(current_timestamps, timestamps):
                raise ValueError("single-file fields do not share one timeline")
            _, _, _, name = self._identity(device_id, column_id)
            values[name] = current_values
        return timestamps, values
