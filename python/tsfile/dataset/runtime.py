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
from concurrent.futures import ThreadPoolExecutor, wait
from concurrent.futures import thread as thread_pool_module
from dataclasses import dataclass
import os
import threading
from typing import Dict, Optional, Tuple

import numpy as np

from ..constants import NUMERIC_DATASET_FIELD_TYPES, TSDataType
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
from .metadata import (
    MODEL_TABLE,
    MODEL_TREE,
    TableEntry,
    _join_series_path,
    split_logical_series_path,
)
from .merge import build_aligned_matrix

_SERIES_DESCRIPTOR_CACHE_SIZE = 4096


def _configured_non_negative_int(name, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _configured_prepared_series_cache_size():
    return _configured_non_negative_int(
        "TSFILE_DATAFRAME_MAX_PREPARED_SERIES",
        _SERIES_DESCRIPTOR_CACHE_SIZE,
    )


def _run_cleanups(cleanups):
    first_error = None
    for cleanup in cleanups:
        try:
            cleanup()
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


def _discard_executor_after_fork(executor):
    """Detach a CPython ThreadPoolExecutor without touching inherited locks."""
    # CPython resets the thread module's global shutdown lock after fork, but
    # not a ThreadPoolExecutor instance's _shutdown_lock.  shutdown()/join()
    # can therefore deadlock in the child.  Disconnect the vanished workers
    # and their queues lock-free; this runtime never uses the executor again.
    executor._shutdown = True
    threads = tuple(getattr(executor, "_threads", ()))
    if hasattr(executor, "_threads"):
        executor._threads = set()
    if hasattr(executor, "_work_queue"):
        executor._work_queue = None
    thread_queues = getattr(thread_pool_module, "_threads_queues", None)
    for thread in threads:
        if thread_queues is not None:
            thread_queues.pop(thread, None)
        if hasattr(thread, "_target"):
            thread._target = None
        if hasattr(thread, "_args"):
            thread._args = ()
        if hasattr(thread, "_kwargs"):
            thread._kwargs = {}


@dataclass(frozen=True)
class RuntimeSeriesShard:
    """One immutable physical fragment already expanded from the mmap route."""

    reader: "RuntimeSeriesReader"
    device_id: int
    column_id: int
    locator_id: int
    timeline_length: int
    min_time: int
    max_time: int


@dataclass(frozen=True)
class RuntimeSeriesDescriptor:
    """Bounded process-local expansion of one logical series route."""

    ref: Tuple[int, int]
    series_id: int
    column_id: int
    shards: Tuple[RuntimeSeriesShard, ...]
    min_time: Optional[int]
    max_time: Optional[int]
    count: int

    @property
    def refs(self):
        return tuple(
            (shard.reader, shard.device_id, shard.column_id) for shard in self.shards
        )


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
        self._creator_pid = os.getpid()
        self._lock = threading.Lock()
        self._closed = False
        runtime._acquire_object()

    def _assert_current_process(self):
        if self._creator_pid != os.getpid():
            raise RuntimeError(
                "Inherited runtime handles cannot be queried after fork; "
                "obtain a new Timeseries from the child's TsFileDataFrame"
            )

    def clone(self):
        self._assert_current_process()
        with self._lock:
            if self._closed:
                raise RuntimeError("Runtime lease is closed")
            return RuntimeLease(self._runtime)

    def query_lease(self):
        self._assert_current_process()
        with self._lock:
            if self._closed:
                raise RuntimeError("Runtime lease is closed")
            self._runtime._acquire_query()
            return _QueryLease(self._runtime, acquired=True)

    def close(self):
        if self._creator_pid != os.getpid():
            if self._closed:
                return
            self._closed = True
            self._runtime.discard_after_fork()
            return
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._runtime._release_object()


class _QueryLease:
    def __init__(self, runtime: "DatasetRuntime", acquired: bool = False):
        self._runtime = runtime
        self._creator_pid = os.getpid()
        self._acquired = acquired

    def __enter__(self):
        if self._creator_pid != os.getpid():
            raise RuntimeError(
                "Inherited runtime handles cannot be queried after fork; "
                "obtain a new Timeseries from the child's TsFileDataFrame"
            )
        if not self._acquired:
            self._runtime._acquire_query()
            self._acquired = True
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        if self._creator_pid != os.getpid():
            if not self._acquired:
                return
            self._acquired = False
            self._runtime.discard_after_fork()
            return
        if self._acquired:
            self._acquired = False
            self._runtime._release_query()


class _ReaderSession:
    def __init__(
        self,
        file_id: int,
        path: str,
        expected_size: int,
        fingerprint: int,
        validate_generation: bool = True,
    ):
        self.file_id = file_id
        self.path = path
        self.expected_size = expected_size
        self.fingerprint = fingerprint
        self._validate_generation_enabled = validate_generation
        if validate_generation:
            self._validate_generation()
        self.reader = TsFileReaderPy(path)
        self.active_uses = 0
        self._closed = False

    def _validate_generation(self):
        st = os.stat(self.path)
        if (
            st.st_size != self.expected_size
            or file_fingerprint(self.path, st) != self.fingerprint
        ):
            raise RuntimeError(
                "TsFile generation changed after Dataset Index publication: "
                f"{self.path}"
            )

    def close(self):
        if self._closed:
            return
        self._closed = True
        self.reader.close()


class ReaderSessionPool:
    """Per-Runtime LRU pool with a hard cap on simultaneously open Readers."""

    def __init__(
        self,
        index: MappedDatasetIndex,
        max_open_files: int,
        validate_generation: bool = True,
    ):
        self._index = index
        self.max_open_files = max(1, int(max_open_files))
        self._validate_generation = bool(validate_generation)
        self._sessions: "OrderedDict[int, _ReaderSession]" = OrderedDict()
        self._condition = threading.Condition()
        self._closed = False

    def _new_session(self, file_id: int):
        record = self._index.record(TSFILE_RECORD, file_id)
        return _ReaderSession(
            file_id,
            self._index.string(record[0]),
            record[2],
            record[3],
            validate_generation=self._validate_generation,
        )

    @contextlib.contextmanager
    def acquire(self, file_id: int):
        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("ReaderSessionPool is closed")
                session = self._sessions.get(file_id)
                if session is not None:
                    if self._validate_generation:
                        session._validate_generation()
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
        _run_cleanups(session.close for session in sessions)

    def discard_after_fork(self):
        """Close only this child copy without acquiring inherited locks."""
        if self._closed and not self._sessions:
            return
        self._closed = True
        sessions = list(self._sessions.values())
        self._sessions = OrderedDict()
        _run_cleanups(session.close for session in sessions)

    @property
    def open_count(self):
        with self._condition:
            return len(self._sessions)


@dataclass
class _PreparedSeriesEntry:
    prepared: object
    active_uses: int = 0
    time_owner: Optional["_PreparedSeriesEntry"] = None
    dependent_uses: int = 0


class PreparedSeriesCache:
    """Runtime-wide single-flight LRU of native exact-locator metadata."""

    def __init__(
        self,
        index: MappedDatasetIndex,
        validate_references: bool = True,
        max_entries: Optional[int] = None,
    ):
        self._index = index
        self._validate_references = bool(validate_references)
        maximum = _SERIES_DESCRIPTOR_CACHE_SIZE if max_entries is None else max_entries
        if int(maximum) < 0:
            raise ValueError("max_entries must be non-negative")
        self.max_entries = int(maximum)
        self._condition = threading.Condition()
        self._entries = OrderedDict()
        self._entries_by_prepared_id = {}
        self._loading = set()
        self._closed = False

    def _locator_tuple(self, file_id, locator_id):
        locator = self._index.record(SERIES_LOCATOR, locator_id)
        device_span = self._index.record(DEVICE_FILE_SPAN, locator[0])
        file_record = self._index.record(TSFILE_RECORD, file_id)
        if self._validate_references and device_span[1] != file_id:
            raise ValueError("series locator points at another TsFile")
        return (
            id(self._index),
            file_id,
            file_record[2],
            file_record[3],
            locator_id,
            locator[1],
            locator[2],
            locator[3],
            locator[4],
            device_span[2],
            device_span[3],
        )

    def _evict_idle_locked(self):
        evicted = []
        while len(self._entries) > self.max_entries:
            idle_key = next(
                (
                    key
                    for key, entry in self._entries.items()
                    if entry.active_uses == 0 and entry.dependent_uses == 0
                ),
                None,
            )
            if idle_key is None:
                break
            evicted.append(self._pop_entry_locked(idle_key))
        return evicted

    def _pop_entry_locked(self, key):
        entry = self._entries.pop(key)
        self._entries_by_prepared_id.pop(id(entry.prepared), None)
        if entry.time_owner is not None:
            entry.time_owner.dependent_uses -= 1
            entry.time_owner = None
        return entry.prepared

    def _drain_entries_locked(self):
        prepared = []
        while self._entries:
            leaf_key = next(
                (
                    key
                    for key, entry in self._entries.items()
                    if entry.dependent_uses == 0
                ),
                None,
            )
            if leaf_key is None:
                raise RuntimeError("PreparedSeries owner graph contains a cycle")
            prepared.append(self._pop_entry_locked(leaf_key))
        return prepared

    @staticmethod
    def _close_entries(entries):
        _run_cleanups(prepared.close for prepared in entries)

    def _release(self, entry):
        with self._condition:
            entry.active_uses -= 1
            evicted = self._evict_idle_locked()
            self._condition.notify_all()
        self._close_entries(evicted)

    @contextlib.contextmanager
    def acquire(self, file_id, locator_id, reader, time_owner=None):
        key = (id(self._index), file_id, locator_id)
        entry = None
        owner_entry = None
        prepared_time_owner = time_owner
        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("PreparedSeriesCache is closed")
                entry = self._entries.get(key)
                if entry is not None:
                    self._entries.move_to_end(key)
                    entry.active_uses += 1
                    break
                if key not in self._loading:
                    if time_owner is not None:
                        requested_owner = self._entries_by_prepared_id.get(
                            id(time_owner)
                        )
                        if (
                            requested_owner is None
                            or requested_owner.prepared is not time_owner
                            or requested_owner.active_uses == 0
                        ):
                            raise RuntimeError(
                                "PreparedSeries time owner requires an active cache lease"
                            )
                        owner_entry = requested_owner.time_owner or requested_owner
                        prepared_time_owner = owner_entry.prepared
                    self._loading.add(key)
                    break
                self._condition.wait()

        evicted = []
        if entry is None:
            try:
                result = reader.prepare_series(
                    self._locator_tuple(file_id, locator_id),
                    time_owner=prepared_time_owner,
                )
            except BaseException:
                with self._condition:
                    self._loading.discard(key)
                    self._condition.notify_all()
                raise

            with self._condition:
                self._loading.discard(key)
                rejected = self._closed
                if not rejected:
                    entry = _PreparedSeriesEntry(
                        result,
                        active_uses=1,
                        time_owner=owner_entry,
                    )
                    if owner_entry is not None:
                        owner_entry.dependent_uses += 1
                    self._entries[key] = entry
                    self._entries_by_prepared_id[id(result)] = entry
                    evicted = self._evict_idle_locked()
                self._condition.notify_all()
            if rejected:
                try:
                    result.close()
                except BaseException as exc:
                    raise RuntimeError("PreparedSeriesCache is closed") from exc
                raise RuntimeError("PreparedSeriesCache is closed")

        try:
            self._close_entries(evicted)
        except BaseException:
            try:
                self._release(entry)
            except BaseException:
                pass
            raise

        try:
            yield entry.prepared
        except BaseException:
            try:
                self._release(entry)
            except BaseException:
                pass
            raise
        else:
            self._release(entry)

    def close(self):
        with self._condition:
            self._closed = True
            while self._loading or any(
                entry.active_uses for entry in self._entries.values()
            ):
                self._condition.wait()
            entries = self._drain_entries_locked()
        self._close_entries(entries)

    def discard_after_fork(self):
        """Close only this child copy without acquiring inherited locks."""
        if self._closed and not self._entries and not self._loading:
            return
        self._closed = True
        entries = self._drain_entries_locked()
        self._entries_by_prepared_id = {}
        self._loading = set()
        self._close_entries(entries)

    @property
    def size(self):
        with self._condition:
            return len(self._entries)


class DatasetRuntime:
    def __init__(
        self,
        path: str,
        max_open_files: Optional[int] = None,
        query_workers: Optional[int] = None,
        query_parallel_min_rows: Optional[int] = None,
        trust_index: bool = False,
    ):
        self.trust_index = bool(trust_index)
        maximum = (
            int(os.environ.get("TSFILE_DATAFRAME_MAX_OPEN_FILES", "16"))
            if max_open_files is None
            else max_open_files
        )
        self.max_open_files = max(1, int(maximum))
        workers = (
            int(
                os.environ.get(
                    "TSFILE_DATAFRAME_QUERY_WORKERS",
                    str(min(4, os.cpu_count() or 1)),
                )
            )
            if query_workers is None
            else query_workers
        )
        self.query_workers = max(1, int(workers))
        minimum_rows = (
            int(os.environ.get("TSFILE_DATAFRAME_QUERY_PARALLEL_MIN_ROWS", "8192"))
            if query_parallel_min_rows is None
            else query_parallel_min_rows
        )
        self.query_parallel_min_rows = max(1, int(minimum_rows))
        prepared_max_entries = _configured_prepared_series_cache_size()
        self.creator_pid = os.getpid()
        self._discarded_pid = None
        self._path = os.fspath(path)
        self.index = None
        self._query_executor = None
        self.readers = None
        self.prepared = None
        self.catalog = None
        self._resources_closed = False
        self._condition = threading.Condition()
        self._object_leases = 0
        self._query_leases = 0
        self._accepting = True
        self._torn_down = False

        try:
            self.index = MappedDatasetIndex(self._path, trust_index=self.trust_index)
            self._query_executor = (
                ThreadPoolExecutor(
                    max_workers=self.query_workers,
                    thread_name_prefix="tsfile-dataframe-query",
                )
                if self.query_workers > 1
                else None
            )
            self.readers = ReaderSessionPool(
                self.index,
                self.max_open_files,
                validate_generation=not self.trust_index,
            )
            self.prepared = PreparedSeriesCache(
                self.index,
                validate_references=not self.trust_index,
                max_entries=prepared_max_entries,
            )
            self.catalog = MappedDataFrameCatalog(self)
        except BaseException:
            try:
                self._close_resources()
            except BaseException:
                pass
            raise

    def _close_resources(self):
        if self._resources_closed:
            return
        self._resources_closed = True
        cleanups = []
        if self._query_executor is not None:
            cleanups.append(
                lambda executor=self._query_executor: executor.shutdown(
                    wait=True, cancel_futures=True
                )
            )
        if self.prepared is not None:
            cleanups.append(self.prepared.close)
        if self.readers is not None:
            cleanups.append(self.readers.close)
        if self.index is not None:
            cleanups.append(self.index.close)
        _run_cleanups(cleanups)

    def fork_replacement(self):
        """Create fresh process-local mutable/native state after ``fork``."""
        return type(self)(
            self._path,
            max_open_files=self.max_open_files,
            query_workers=self.query_workers,
            query_parallel_min_rows=self.query_parallel_min_rows,
            trust_index=self.trust_index,
        )

    def _assert_current_process(self):
        if self.creator_pid != os.getpid():
            raise RuntimeError(
                "Inherited DatasetRuntime cannot be used after fork; "
                "obtain a new Timeseries from the child's TsFileDataFrame"
            )

    def discard_after_fork(self):
        """Deterministically discard this child copy without inherited locks."""
        current_pid = os.getpid()
        if self.creator_pid == current_pid:
            raise RuntimeError("discard_after_fork requires an inherited runtime")
        if self._discarded_pid == current_pid:
            return

        self._discarded_pid = current_pid
        self._resources_closed = True
        self._accepting = False
        self._torn_down = True

        prepared = self.prepared
        readers = self.readers
        index = self.index
        catalog = self.catalog
        executor = self._query_executor
        self._query_executor = None
        self.prepared = None
        self.readers = None
        self.index = None
        self.catalog = None

        if catalog is not None:
            catalog.runtime = None
            catalog.index = None
            catalog_readers = getattr(catalog, "_readers", None)
            if catalog_readers is not None:
                catalog_readers.clear()

        cleanups = []
        if executor is not None:
            cleanups.append(lambda: _discard_executor_after_fork(executor))
        if prepared is not None:
            cleanups.append(prepared.discard_after_fork)
        if readers is not None:
            cleanups.append(readers.discard_after_fork)
        if index is not None:
            cleanups.append(index.close)
        _run_cleanups(cleanups)

    def lease(self):
        self._assert_current_process()
        return RuntimeLease(self)

    def query_lease(self):
        self._assert_current_process()
        return _QueryLease(self)

    def map_query_groups(self, function, groups, estimated_rows=None):
        """Run independent query groups under the caller's query lease."""
        self._assert_current_process()
        groups = list(groups)
        if not groups:
            return []
        estimates = list(estimated_rows) if estimated_rows is not None else []
        large_enough = not estimates or max(estimates, default=0) >= (
            self.query_parallel_min_rows
        )
        if self._query_executor is None or len(groups) == 1 or not large_enough:
            return [function(group) for group in groups]

        futures = [self._query_executor.submit(function, group) for group in groups]
        try:
            # Preserve group order so merge behavior stays deterministic.
            return [future.result() for future in futures]
        except BaseException:
            for future in futures:
                future.cancel()
            wait(futures)
            raise

    def _acquire_object(self):
        self._assert_current_process()
        with self._condition:
            if not self._accepting:
                raise RuntimeError("Dataset Runtime is closing")
            self._object_leases += 1

    def _release_object(self):
        if self.creator_pid != os.getpid():
            self.discard_after_fork()
            return
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
            self._close_resources()

    def _acquire_query(self):
        self._assert_current_process()
        with self._condition:
            if not self._accepting:
                raise RuntimeError("Dataset Runtime is closing")
            self._query_leases += 1

    def _release_query(self):
        if self.creator_pid != os.getpid():
            self.discard_after_fork()
            return
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
                    f"Dataset Index contains duplicate canonical table '{name}'"
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
        field_types = []
        schema_columns = []
        for name_index in self._catalog.index.records(
            COLUMN_NAME_INDEX, table[4], table[5]
        ):
            column = self._catalog.index.record(COLUMN_SCHEMA, name_index[1])
            column_name = self._catalog.index.string(column[1])
            schema_columns.append(
                (column[2], column_name, int(column[3]), int(column[7]))
            )
            if column[7] == 0:
                tags.append((column[2], column_name, TSDataType(column[3])))
            elif column[7] == 1 and column[3] in {
                int(data_type) for data_type in NUMERIC_DATASET_FIELD_TYPES
            }:
                fields.append((column[2], column_name, TSDataType(column[3])))
        tags.sort()
        fields.sort()
        schema_columns.sort()
        result = TableEntry(
            name,
            tuple(item[1] for item in tags),
            tuple(item[2] for item in tags),
            tuple(item[1] for item in fields),
            tuple(item[2] for item in fields),
            tuple((item[1], item[2], item[3]) for item in schema_columns),
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
        try:
            table_id = self._catalog.table_entries.table_id(table_name)
        except KeyError:
            return default

        try:
            return self._catalog.index.find_device_id(
                table_id, _join_series_path(table_name, tags, "")
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
    def __init__(self, catalog, cache_size):
        self._catalog = catalog
        self._cache_size = cache_size
        self._cache = OrderedDict()
        self._cache_lock = threading.Lock()

    def _series_id(self, ref):
        device_id, field_idx = ref
        device = self._catalog.index.record(DEVICE_RECORD, device_id)
        table_name = self._catalog.index.string(
            self._catalog.index.record(TABLE_RECORD, device[0])[0]
        )
        field_name = self._catalog.table_entries[table_name].field_columns[field_idx]
        column_id = self._catalog.index.find_column_id(device[0], field_name)
        return self._catalog.index.find_series_id(device_id, column_id)

    def describe(self, ref, series_id=None, column_id=None):
        with self._cache_lock:
            result = self._cache.get(ref)
            if result is not None:
                self._cache.move_to_end(ref)
                return result

        if series_id is None:
            series_id = self._series_id(ref)
        series = self._catalog.index.record(LOGICAL_SERIES, series_id)
        if series[0] != ref[0]:
            raise KeyError(ref)
        if column_id is None:
            column_id = series[1]
        elif series[1] != column_id:
            raise KeyError(ref)

        shards = []
        count = 0
        for span_id in range(series[2], series[2] + series[3]):
            span = self._catalog.index.record(SERIES_FILE_SPAN, span_id)
            locator = self._catalog.index.record(SERIES_LOCATOR, span[2])
            device_span = self._catalog.index.record(DEVICE_FILE_SPAN, locator[0])
            timeline_length = device_span[6] if device_span[4] == 1 else span[6]
            count += timeline_length
            shards.append(
                RuntimeSeriesShard(
                    self._catalog.reader_for(span[1]),
                    series[0],
                    column_id,
                    span[2],
                    timeline_length,
                    span[4],
                    span[5],
                )
            )

        result = RuntimeSeriesDescriptor(
            ref,
            series_id,
            column_id,
            tuple(shards),
            series[4] if count else None,
            series[5] if count else None,
            count,
        )
        if self._cache_size:
            with self._cache_lock:
                existing = self._cache.get(ref)
                if existing is not None:
                    self._cache.move_to_end(ref)
                    return existing
                self._cache[ref] = result
                while len(self._cache) > self._cache_size:
                    self._cache.popitem(last=False)
        return result

    def __contains__(self, ref):
        try:
            self.describe(ref)
            return True
        except KeyError:
            return False

    def __getitem__(self, ref):
        return list(self.describe(ref).refs)

    def __iter__(self):
        return iter(self._catalog.series)

    def __len__(self):
        return len(self._catalog.series)


class MappedDataFrameCatalog:
    def __init__(self, runtime: DatasetRuntime):
        self.runtime = runtime
        self.index = runtime.index
        self.index_identity = runtime.index.identity
        self._descriptor_cache_size = _SERIES_DESCRIPTOR_CACHE_SIZE
        self._descriptor_cache = OrderedDict()
        self._descriptor_cache_lock = threading.Lock()
        self.table_entries = _TableMapping(self)
        self.devices = _DeviceSequence(self)
        self.device_index = _DeviceIndexMapping(self)
        self.device_time_bounds = _DeviceTimeBounds(self)
        self.series = _SeriesSequence(self)
        self.series_shards = _RouteMapping(self, self._descriptor_cache_size)
        self._readers = {}
        self.model = self._infer_model()

    def resolve_series_descriptor(self, table_name, tags, field_name):
        key = (table_name, tuple(tags), field_name)
        with self._descriptor_cache_lock:
            result = self._descriptor_cache.get(key)
            if result is not None:
                self._descriptor_cache.move_to_end(key)
                return result

        table_id = self.table_entries.table_id(table_name)
        table = self.table_entries[table_name]
        field_idx = table.get_field_index(field_name)
        device_id = self.index.find_device_id(
            table_id, _join_series_path(table_name, tags, "")
        )
        column_id = self.index.find_column_id(table_id, field_name)
        series_id = self.index.find_series_id(device_id, column_id)
        result = self.series_shards.describe(
            (device_id, field_idx), series_id=series_id, column_id=column_id
        )

        if self._descriptor_cache_size:
            with self._descriptor_cache_lock:
                existing = self._descriptor_cache.get(key)
                if existing is not None:
                    self._descriptor_cache.move_to_end(key)
                    return existing
                self._descriptor_cache[key] = result
                while len(self._descriptor_cache) > self._descriptor_cache_size:
                    self._descriptor_cache.popitem(last=False)
        return result

    def resolve_series_descriptor_by_id(self, series_id, table_name, field_name):
        if series_id < 0 or series_id >= self.index.count(LOGICAL_SERIES):
            raise KeyError(series_id)
        series = self.index.record(LOGICAL_SERIES, series_id)
        table = self.table_entries[table_name]
        field_idx = table.get_field_index(field_name)
        return self.series_shards.describe(
            (series[0], field_idx), series_id=series_id, column_id=series[1]
        )

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
        timeline_length = device_span[6] if device_span[4] == 1 else span[6]
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
        timestamp_parts = []
        value_parts = []
        with result:
            read_arrow = getattr(result, "read_arrow_record_batch", None)
            if read_arrow is None:
                read_arrow = result.read_arrow_batch
            while True:
                arrow_batch = read_arrow()
                if arrow_batch is None:
                    break
                if arrow_batch.num_rows == 0:
                    continue
                timestamp_parts.append(
                    np.asarray(
                        arrow_batch.column(0).to_numpy(zero_copy_only=False),
                        dtype=np.int64,
                    )
                )
                value_parts.append(
                    np.asarray(
                        arrow_batch.column(1).to_numpy(zero_copy_only=False),
                        dtype=np.float64,
                    )
                )
        if not timestamp_parts:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        if len(timestamp_parts) == 1:
            return timestamp_parts[0], value_parts[0]
        return np.concatenate(timestamp_parts), np.concatenate(value_parts)

    @staticmethod
    def _consume_multi(result, column_names):
        timestamp_parts = []
        value_parts = {name: [] for name in column_names}
        with result:
            read_arrow = getattr(result, "read_arrow_record_batch", None)
            if read_arrow is None:
                read_arrow = result.read_arrow_batch
            while True:
                arrow_batch = read_arrow()
                if arrow_batch is None:
                    break
                if arrow_batch.num_rows == 0:
                    continue
                timestamp_parts.append(
                    np.asarray(
                        arrow_batch.column(0).to_numpy(zero_copy_only=False),
                        dtype=np.int64,
                    )
                )
                for column_index, name in enumerate(column_names, start=1):
                    value_parts[name].append(
                        np.asarray(
                            arrow_batch.column(column_index).to_numpy(
                                zero_copy_only=False
                            ),
                            dtype=np.float64,
                        )
                    )
        if not timestamp_parts:
            return np.array([], dtype=np.int64), {
                name: np.array([], dtype=np.float64) for name in column_names
            }
        timestamps = (
            timestamp_parts[0]
            if len(timestamp_parts) == 1
            else np.concatenate(timestamp_parts)
        )
        values = {
            name: parts[0] if len(parts) == 1 else np.concatenate(parts)
            for name, parts in value_parts.items()
        }
        return timestamps, values

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
        return self._query_at_locator(
            span[2],
            start_time=start_time,
            end_time=end_time,
            offset=offset,
            limit=limit,
        )

    def _query_at_locator(
        self,
        locator_id,
        start_time=None,
        end_time=None,
        offset=None,
        limit=None,
    ):
        with self.runtime.readers.acquire(self.file_id) as reader:
            with self.runtime.prepared.acquire(
                self.file_id, locator_id, reader
            ) as prepared:
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

    def read_series_by_row_at_locator(self, locator_id, offset, limit):
        """Read by a locator already validated against this Runtime snapshot."""
        if limit <= 0:
            return np.array([], dtype=np.int64), np.array([], dtype=np.float64)
        return self._query_at_locator(locator_id, offset=offset, limit=limit)

    def read_device_fields_by_time_range(
        self, device_id, column_ids, start_time, end_time
    ):
        if not column_ids:
            return np.array([], dtype=np.int64), {}

        spans = [self._span(device_id, column_id) for column_id in column_ids]
        locators = [
            self.runtime.index.record(SERIES_LOCATOR, span[2]) for span in spans
        ]
        device_span_ids = {locator[0] for locator in locators}
        can_read_aligned = len(device_span_ids) == 1
        if can_read_aligned:
            device_span = self.runtime.index.record(
                DEVICE_FILE_SPAN, next(iter(device_span_ids))
            )
            can_read_aligned = (
                device_span[1] == self.file_id
                and device_span[4] == 1
                and all(locator[1] == 1 for locator in locators)
            )

        if can_read_aligned:
            column_names = [
                self._identity(device_id, column_id)[3] for column_id in column_ids
            ]
            with self.runtime.readers.acquire(self.file_id) as reader:
                with contextlib.ExitStack() as stack:
                    prepared = []
                    time_owner = None
                    for span in spans:
                        current = stack.enter_context(
                            self.runtime.prepared.acquire(
                                self.file_id,
                                span[2],
                                reader,
                                time_owner=time_owner,
                            )
                        )
                        prepared.append(current)
                        if time_owner is None:
                            time_owner = current
                    result = reader.query_prepared_multi(
                        prepared, start_time=start_time, end_time=end_time
                    )
                    return self._consume_multi(result, column_names)

        # Non-aligned device (or fields spanning different device spans): each
        # field carries its own timeline, so align them onto a single timestamp
        # union with NaN for missing values. This matches the cross-file merge
        # semantics of build_aligned_matrix rather than requiring identical
        # per-field timelines.
        parts = {}
        for column_id in column_ids:
            _, _, _, name = self._identity(device_id, column_id)
            parts[name] = self.read_series_by_ref(
                device_id, column_id, start_time, end_time
            )
        names = list(parts)
        timestamps, matrix = build_aligned_matrix(names, parts)
        return timestamps, {name: matrix[:, index] for index, name in enumerate(names)}
