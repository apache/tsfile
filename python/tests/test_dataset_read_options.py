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

from concurrent.futures import ThreadPoolExecutor
import threading
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    TSDataType,
    TsFileDataFrame,
    TsFileTableWriter,
)
from tsfile.dataset.runtime import PreparedSeriesCache

_OPTIONS = (
    "max_prepared_series",
    "descriptor_cache_size",
    "max_open_files",
    "query_workers",
    "query_parallel_min_rows",
)


@pytest.fixture
def dataset_file(tmp_path):
    path = tmp_path / "devices.tsfile"
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("other", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(
            pd.DataFrame(
                {
                    "time": [0, 1, 0, 1, 0, 1],
                    "device": ["d0", "d0", "d1", "d1", "d2", "d2"],
                    "value": [0.0, 1.0, 10.0, 11.0, 20.0, 21.0],
                    "other": [100.0, np.nan, 110.0, 111.0, 120.0, 121.0],
                }
            )
        )
    return str(path)


def _effective_options(dataframe):
    runtime = dataframe._runtime
    return (
        runtime.prepared.max_entries,
        runtime.catalog._descriptor_cache_size,
        runtime.readers.max_open_files,
        runtime.query_workers,
        runtime.query_parallel_min_rows,
    )


def test_dataframe_options_override_environment(dataset_file, monkeypatch):
    for option in _OPTIONS:
        monkeypatch.setenv("TSFILE_DATAFRAME_" + option.upper(), "invalid")
    options = dict(zip(_OPTIONS, (2, 3, 4, 1, 5)))
    with TsFileDataFrame(
        dataset_file, show_progress=False, use_index=True, **options
    ) as dataframe:
        assert _effective_options(dataframe) == (2, 3, 4, 1, 5)
        with dataframe[[0, 1]] as subset:
            assert subset._runtime is dataframe._runtime
            assert _effective_options(subset) == _effective_options(dataframe)


def test_dataframe_snapshots_environment_for_each_runtime(dataset_file, monkeypatch):
    for option in _OPTIONS:
        monkeypatch.setenv("TSFILE_DATAFRAME_" + option.upper(), "2")
    with TsFileDataFrame(dataset_file, show_progress=False, use_index=True) as first:
        for option in _OPTIONS:
            monkeypatch.setenv("TSFILE_DATAFRAME_" + option.upper(), "1")
        with TsFileDataFrame(
            dataset_file, show_progress=False, use_index=True
        ) as second:
            assert _effective_options(first) == (2, 2, 2, 2, 2)
            assert _effective_options(second) == (1, 1, 1, 1, 1)


def test_dataframe_read_options_defaults(dataset_file, monkeypatch):
    for option in _OPTIONS:
        monkeypatch.delenv("TSFILE_DATAFRAME_" + option.upper(), raising=False)
    monkeypatch.setattr("os.cpu_count", lambda: 2)
    with TsFileDataFrame(
        dataset_file, show_progress=False, use_index=True
    ) as dataframe:
        assert _effective_options(dataframe) == (4096, 4096, 16, 2, 8192)


@pytest.mark.parametrize("capacity", [0, 1, 2])
@pytest.mark.parametrize("trust_index", [True, False])
def test_small_caches_preserve_single_and_aligned_reads(
    dataset_file, capacity, trust_index
):
    with TsFileDataFrame(
        dataset_file,
        show_progress=False,
        use_index=True,
        trust_index=trust_index,
        max_prepared_series=capacity,
        descriptor_cache_size=capacity,
        query_workers=2,
        query_parallel_min_rows=1,
    ) as dataframe:
        names = [str(name) for name in dataframe.list_timeseries()]
        baseline = dataframe.loc[0:1, names].values.copy()
        expected = []
        for name in names:
            device = int(name.split(".")[1][1:])
            values = np.array([device * 10.0, device * 10.0 + 1])
            if name.endswith(".other"):
                values += 100
                if device == 0:
                    values[1] = np.nan
            expected.append(values)
        np.testing.assert_allclose(baseline, np.column_stack(expected), equal_nan=True)
        for _ in range(2):
            for column, name in enumerate(names):
                with dataframe[name] as series:
                    np.testing.assert_allclose(
                        series[:], baseline[:, column], equal_nan=True
                    )
                assert dataframe._runtime.prepared.size <= capacity
                assert len(dataframe._index._descriptor_cache) <= capacity
                assert len(dataframe._index.series_shards._cache) <= capacity
            np.testing.assert_allclose(
                dataframe.loc[0:1, names].values, baseline, equal_nan=True
            )
            assert dataframe._runtime.prepared.size <= capacity


@pytest.mark.parametrize("option", _OPTIONS)
@pytest.mark.parametrize("value", ["invalid", "-1", "1.5"])
def test_invalid_environment_is_rejected_before_reading_files(
    monkeypatch, option, value
):
    env_name = "TSFILE_DATAFRAME_" + option.upper()
    monkeypatch.setenv(env_name, value)
    with pytest.raises(ValueError, match=env_name):
        TsFileDataFrame("missing.tsfile", use_index=True)


@pytest.mark.parametrize("option", _OPTIONS[2:])
def test_zero_requires_positive_read_limits(option):
    with pytest.raises(ValueError, match=option):
        TsFileDataFrame("missing.tsfile", use_index=True, **{option: 0})


class _Prepared:
    def __init__(self, locator_id, time_owner=None):
        self.locator_id = locator_id
        self.time_owner = time_owner
        self.closed = False

    def close(self):
        self.closed = True


class _Reader:
    def __init__(self):
        self.prepared = []

    def prepare_series(self, locator, time_owner=None, trust_index=False):
        assert time_owner is None or not time_owner.closed
        prepared = _Prepared(locator, time_owner)
        self.prepared.append(prepared)
        return prepared


def _cache(monkeypatch, capacity):
    cache = PreparedSeriesCache(SimpleNamespace(), max_entries=capacity)
    monkeypatch.setattr(cache, "_locator_tuple", lambda _file, locator: locator)
    return cache


def test_prepared_cache_evicts_lru_and_reprepares(monkeypatch):
    cache = _cache(monkeypatch, 2)
    reader = _Reader()
    with cache.acquire(0, 0, reader) as first:
        pass
    with cache.acquire(0, 1, reader) as second:
        pass
    with cache.acquire(0, 0, reader) as reused:
        assert reused is first
    with cache.acquire(0, 2, reader):
        assert second.closed
        assert not first.closed
    assert cache.size == 2
    with cache.acquire(0, 1, reader) as rebuilt:
        assert rebuilt is not second
        assert not rebuilt.closed
    cache.close()
    assert all(item.closed for item in reader.prepared)


def test_prepared_cache_zero_keeps_active_entries_and_shared_owner(monkeypatch):
    cache = _cache(monkeypatch, 0)
    reader = _Reader()
    with cache.acquire(0, 0, reader) as owner:
        with cache.acquire(0, 1, reader, time_owner=owner) as value:
            assert value.time_owner is owner
            assert not owner.closed
            assert not value.closed
            assert cache.size == 2
        assert value.closed
        assert not owner.closed
    assert owner.closed
    assert cache.size == 0
    cache.close()


def test_prepared_cache_keeps_owner_until_dependent_is_evicted(monkeypatch):
    cache = _cache(monkeypatch, 2)
    reader = _Reader()
    with cache.acquire(0, 0, reader) as owner:
        with cache.acquire(0, 1, reader, time_owner=owner) as value:
            pass
    with cache.acquire(0, 2, reader):
        assert value.closed
        assert not owner.closed
    with cache.acquire(0, 3, reader):
        assert owner.closed
    cache.close()


def test_prepared_cache_single_flight_and_active_queries_survive_pressure(monkeypatch):
    cache = _cache(monkeypatch, 1)
    reader = _Reader()
    preparing = threading.Event()
    finish_preparing = threading.Event()
    acquired = threading.Barrier(3, timeout=5)
    release = threading.Event()
    original = reader.prepare_series

    def prepare(locator, time_owner=None, trust_index=False):
        preparing.set()
        assert finish_preparing.wait(timeout=5)
        return original(locator, time_owner, trust_index=trust_index)

    monkeypatch.setattr(reader, "prepare_series", prepare)

    def query():
        with cache.acquire(0, 0, reader) as prepared:
            acquired.wait()
            assert release.wait(timeout=5)
            assert not prepared.closed
            return prepared

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(query)
        assert preparing.wait(timeout=5)
        second = executor.submit(query)
        finish_preparing.set()
        try:
            acquired.wait()
            assert len(reader.prepared) == 1
            with cache.acquire(0, 1, reader) as other:
                assert not reader.prepared[0].closed
                assert not other.closed
            assert other.closed
        finally:
            release.set()
        assert first.result(timeout=5) is second.result(timeout=5)
    cache.close()


def test_prepared_cache_close_waits_for_active_lease(monkeypatch):
    cache = _cache(monkeypatch, 0)
    reader = _Reader()
    closing = threading.Event()
    closed = threading.Event()

    def close():
        closing.set()
        cache.close()
        closed.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        with cache.acquire(0, 0, reader) as prepared:
            future = executor.submit(close)
            assert closing.wait(timeout=5)
            assert not closed.wait(timeout=0.05)
            assert not prepared.closed
        future.result(timeout=5)
    assert prepared.closed
    assert closed.is_set()


def test_failed_preparation_can_be_retried(monkeypatch):
    cache = _cache(monkeypatch, 1)
    reader = _Reader()
    original = reader.prepare_series

    def fail(*_args, **_kwargs):
        raise RuntimeError("prepare failed")

    monkeypatch.setattr(reader, "prepare_series", fail)
    with pytest.raises(RuntimeError, match="prepare failed"):
        with cache.acquire(0, 0, reader):
            pass
    monkeypatch.setattr(reader, "prepare_series", original)
    with cache.acquire(0, 0, reader) as prepared:
        assert not prepared.closed
    cache.close()


def test_cache_close_waits_for_eviction_cleanup(monkeypatch):
    cache = _cache(monkeypatch, 0)
    reader = _Reader()
    freeing = threading.Event()
    finish_freeing = threading.Event()
    closing = threading.Event()
    closed = threading.Event()
    original_prepare = reader.prepare_series

    def prepare(*args, **kwargs):
        prepared = original_prepare(*args, **kwargs)
        original_close = prepared.close

        def free():
            freeing.set()
            assert finish_freeing.wait(timeout=5)
            original_close()

        prepared.close = free
        return prepared

    monkeypatch.setattr(reader, "prepare_series", prepare)

    def query():
        with cache.acquire(0, 0, reader):
            pass

    def close():
        closing.set()
        cache.close()
        closed.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        query_future = executor.submit(query)
        assert freeing.wait(timeout=5)
        close_future = executor.submit(close)
        try:
            assert closing.wait(timeout=5)
            assert not closed.wait(timeout=0.05)
        finally:
            finish_freeing.set()
        query_future.result(timeout=5)
        close_future.result(timeout=5)
    assert all(item.closed for item in reader.prepared)


def test_cache_close_waits_for_preparation_and_releases_rejected_result(monkeypatch):
    cache = _cache(monkeypatch, 1)
    reader = _Reader()
    preparing = threading.Event()
    finish_preparing = threading.Event()
    closing = threading.Event()
    closed = threading.Event()
    original = reader.prepare_series

    def prepare(*args, **kwargs):
        preparing.set()
        assert finish_preparing.wait(timeout=5)
        return original(*args, **kwargs)

    monkeypatch.setattr(reader, "prepare_series", prepare)

    def query():
        with cache.acquire(0, 0, reader):
            pytest.fail("closing cache must reject a newly prepared result")

    def close():
        closing.set()
        cache.close()
        closed.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        query_future = executor.submit(query)
        assert preparing.wait(timeout=5)
        close_future = executor.submit(close)
        try:
            assert closing.wait(timeout=5)
            assert not closed.wait(timeout=0.05)
        finally:
            finish_preparing.set()
        with pytest.raises(RuntimeError, match="closed"):
            query_future.result(timeout=5)
        close_future.result(timeout=5)
    assert all(item.closed for item in reader.prepared)
    assert cache.size == 0


@pytest.mark.parametrize(
    "option",
    [
        "max_prepared_series",
        "descriptor_cache_size",
        "max_open_files",
        "query_workers",
        "query_parallel_min_rows",
    ],
)
@pytest.mark.parametrize("value", [-1, 1.5, True, "32"])
def test_dataframe_rejects_invalid_options_before_reading_files(option, value):
    with pytest.raises((ValueError, TypeError), match=option):
        TsFileDataFrame("missing.tsfile", use_index=True, **{option: value})


def test_dataframe_rejects_ignored_options_without_index():
    with pytest.raises(ValueError, match="use_index=True"):
        TsFileDataFrame("missing.tsfile", max_prepared_series=32)
