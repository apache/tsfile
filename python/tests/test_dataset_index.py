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

from types import SimpleNamespace
import os
import threading

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import tsfile.dataset.dataframe as dataframe_module
import tsfile.dataset.index_identity as identity_module
import tsfile.dataset.index as index_module
import tsfile.dataset.runtime as runtime_module
from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    TsFileDataFrame,
    TsFileTableWriter,
)
from tsfile.constants import TSDataType
from tsfile.dataset.index import (
    COLUMN_SCHEMA,
    DEVICE_FILE_SPAN,
    DIRECTORY,
    HEADER,
    LOGICAL_SERIES,
    MappedDatasetIndex,
    RECORDS,
    SERIES_FILE_SPAN,
    SERIES_LOCATOR,
    TSFILE_RECORD,
    build_sections_from_dataframe,
    crc32c,
    write_index_atomic,
)
from tsfile.dataset.metadata import MetadataCatalog, SeriesStats
from tsfile.dataset.runtime import RuntimeSeriesReader
from tsfile.dataset.runtime import DatasetRuntime
from tsfile.tsfile_reader import TsFileReaderPy


def _synthetic_dataframe(source_path):
    catalog = MetadataCatalog()
    table_id = catalog.add_table("root", (), (), ("s1",))
    device_id = catalog.add_device(table_id, (), 1, 10)
    catalog.series_stats_by_ref[(device_id, 0)] = SeriesStats(
        10,
        1,
        10,
        10,
        1,
        10,
        int(TSDataType.INT64),
        128,
        16,
        64,
        16,
        1,
        1,
        1,
        1,
    )
    reader = SimpleNamespace(file_path=source_path, catalog=catalog)
    index = SimpleNamespace(
        table_entries={"root": catalog.table_entries[0]},
        devices=[("root", ())],
        series=[(0, 0)],
        series_shards={(0, 0): [(reader, 0, 0)]},
    )
    return SimpleNamespace(
        _index=index,
        _readers={source_path: reader},
        _paths=[source_path],
    )


def test_binary_layout_matches_cpp_v1():
    assert HEADER.size == 64
    assert DIRECTORY.size == 32
    assert RECORDS[COLUMN_SCHEMA].size == 32
    assert RECORDS[LOGICAL_SERIES].size == 32
    assert RECORDS[TSFILE_RECORD].size == 32
    assert RECORDS[DEVICE_FILE_SPAN].size == 32
    assert RECORDS[SERIES_FILE_SPAN].size == 40
    assert RECORDS[SERIES_LOCATOR].size == 24
    assert index_module.SECTION_COUNT == 13


def test_crc32c_known_vector():
    assert crc32c(b"123456789") == 0xE3069283


def test_build_publish_map_and_lookup(tmp_path):
    source = tmp_path / "source.tsfile"
    source.write_bytes(b"T" * 4096)
    output = tmp_path / "dataset.tsidx"
    dataframe = _synthetic_dataframe(str(source))
    write_index_atomic(str(output), build_sections_from_dataframe(dataframe))

    with MappedDatasetIndex(str(output), verify_sections=True) as index:
        table_id = index.find_table_ids("root")[0]
        device_id = index.find_device_id(table_id, "root.")
        column_id = index.find_column_id(table_id, "s1")
        assert index.find_series_id(device_id, column_id) == 0
        assert index.count(LOGICAL_SERIES) == 1
        assert index.count(SERIES_LOCATOR) == 1
        file_record = index.record(TSFILE_RECORD, 0)
        assert index.string(file_record[0]) == str(source)


def test_child_lookup_checks_full_bytes_for_hash_collisions(monkeypatch):
    rows = [
        (0, 10, 42, 0, 0),
        (0, 11, 42, 1, 0),
        (0, 12, 42, 2, 0),
    ]
    names = [b"alpha", b"beta", b"gamma"]
    monkeypatch.setattr(index_module, "name_hash", lambda _value: 42)

    class _Index:
        @staticmethod
        def record(_section_type, record_id):
            return rows[record_id]

        @staticmethod
        def string_bytes(sid):
            return names[sid]

    assert MappedDatasetIndex._find_child(_Index(), 0, 0, "beta", 0, 3) == 11
    with pytest.raises(KeyError):
        MappedDatasetIndex._find_child(_Index(), 0, 0, "missing", 0, 3)


def test_rejects_damaged_header_checksum(tmp_path):
    source = tmp_path / "source.tsfile"
    source.write_bytes(b"T" * 4096)
    output = tmp_path / "dataset.tsidx"
    write_index_atomic(
        str(output), build_sections_from_dataframe(_synthetic_dataframe(str(source)))
    )
    with output.open("r+b") as stream:
        stream.seek(32)
        stream.write((1).to_bytes(8, "little"))
    with pytest.raises(ValueError, match="header shape|checksum"):
        MappedDatasetIndex(str(output))


def test_reader_exposes_exact_aligned_locator_ranges(tmp_path):
    source = tmp_path / "aligned.tsfile"
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(source), schema) as writer:
        writer.write_dataframe(
            pd.DataFrame(
                {
                    "time": [0, 1, 2],
                    "device": ["d0", "d0", "d0"],
                    "value": [1.0, 2.0, 3.0],
                }
            )
        )
    reader = TsFileReaderPy(str(source))
    try:
        groups = reader.get_timeseries_metadata()
        metadata = [item for group in groups.values() for item in group.timeseries]
        assert metadata
        assert all(item.value_metadata_length > 0 for item in metadata)
        assert all(
            item.value_metadata_offset + item.value_metadata_length
            <= os.path.getsize(source)
            for item in metadata
        )
        aligned = [item for item in metadata if item.layout == 1]
        assert aligned
        assert all(item.time_metadata_length > 0 for item in aligned)
        assert all(
            item.time_chunk_meta_count == item.chunk_meta_count for item in aligned
        )
    finally:
        reader.close()


def _write_runtime_file(path, start):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(
            pd.DataFrame(
                {
                    "time": [start, start + 1],
                    "device": ["d0", "d0"],
                    "value": [float(start), float(start + 1)],
                }
            )
        )


def _write_runtime_devices_file(path):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(
            pd.DataFrame(
                {
                    "time": [0, 1, 0, 1, 0, 1],
                    "device": ["d0", "d0", "d1", "d1", "d2", "d2"],
                    "value": [0.0, 1.0, 10.0, 11.0, 20.0, 21.0],
                }
            )
        )


def test_hot_construction_maps_index_without_opening_readers(tmp_path, monkeypatch):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as first:
        assert len(first) == 1

    def fail_legacy_scan(*_args, **_kwargs):
        raise AssertionError("hot construction must not build a legacy catalog")

    monkeypatch.setattr("tsfile.dataset.reader.TsFileSeriesReader", fail_legacy_scan)
    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as second:
        assert len(second) == 1
        assert second._runtime.readers.open_count == 0
        series = second[0]
        assert second._runtime.readers.open_count == 0
        assert series[0] == 0.0
        assert second._runtime.readers.open_count == 1
        assert second._runtime.prepared.size == 1
        assert series[1] == 1.0
        assert second._runtime.prepared.size == 1
        series.close()


def test_dataframe_does_not_use_or_create_index_by_default(tmp_path):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    index_path = index_module.index_path_for([str(source)])

    with TsFileDataFrame(str(source), show_progress=False) as dataframe:
        assert dataframe._runtime is None
        assert len(dataframe) == 1
        np.testing.assert_array_equal(dataframe[0][:], np.array([0.0, 1.0]))
        aligned = dataframe.loc[0:1, [0]]
        np.testing.assert_array_equal(aligned.timestamps, np.array([0, 1]))
        np.testing.assert_array_equal(aligned.values, np.array([[0.0], [1.0]]))

    assert not os.path.exists(index_path)


def test_persistent_index_path_is_scoped_to_expanded_file_set(tmp_path, monkeypatch):
    first = tmp_path / "part1.tsfile"
    second = tmp_path / "part2.tsfile"
    third = tmp_path / "part3.tsfile"
    _write_runtime_file(first, 0)
    _write_runtime_file(second, 2)
    _write_runtime_file(third, 10)

    first_set = [str(first), str(second)]
    second_set = [str(first), str(third)]
    first_index = index_module.index_path_for(first_set)
    second_index = index_module.index_path_for(second_set)
    assert first_index != second_index

    with TsFileDataFrame(first_set, show_progress=False, use_index=True) as dataframe:
        assert len(dataframe) == 1
    with TsFileDataFrame(second_set, show_progress=False, use_index=True) as dataframe:
        assert len(dataframe) == 1
    assert os.path.exists(first_index)
    assert os.path.exists(second_index)

    def fail_legacy_scan(*_args, **_kwargs):
        raise AssertionError("matching file-set index should be reused")

    monkeypatch.setattr("tsfile.dataset.reader.TsFileSeriesReader", fail_legacy_scan)
    with TsFileDataFrame(first_set, show_progress=False, use_index=True) as dataframe:
        np.testing.assert_array_equal(dataframe[0][:], np.array([0.0, 1.0, 2.0, 3.0]))
    with TsFileDataFrame(second_set, show_progress=False, use_index=True) as dataframe:
        np.testing.assert_array_equal(dataframe[0][:], np.array([0.0, 1.0, 10.0, 11.0]))


def test_trusted_index_skips_index_and_file_set_validation(tmp_path, monkeypatch):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as first:
        assert len(first) == 1

    def fail_validation(*_args, **_kwargs):
        raise AssertionError("trusted construction must skip validation")

    monkeypatch.setattr(index_module, "index_matches_paths", fail_validation)
    monkeypatch.setattr(index_module.MappedDatasetIndex, "_validate", fail_validation)

    with TsFileDataFrame(
        str(source), show_progress=False, trust_index=True
    ) as dataframe:
        assert dataframe._use_index is True
        assert dataframe._trust_index is True
        assert len(dataframe) == 1


def test_trusted_index_environment_override_enables_index(tmp_path, monkeypatch):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as first:
        assert len(first) == 1

    def fail_validation(*_args, **_kwargs):
        raise AssertionError("trusted construction must skip validation")

    monkeypatch.setenv("TSFILE_DATAFRAME_TRUST_INDEX", "yes")
    monkeypatch.setattr(index_module, "index_matches_paths", fail_validation)
    monkeypatch.setattr(index_module.MappedDatasetIndex, "_validate", fail_validation)

    # The environment-level option also turns on the persistent-index path, so
    # callers do not need to add both use_index=True and trust_index=True.
    with TsFileDataFrame(str(source), show_progress=False) as dataframe:
        assert dataframe._use_index is True
        assert dataframe._trust_index is True
        assert len(dataframe) == 1


def test_explicit_trust_index_false_overrides_environment(tmp_path, monkeypatch):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    monkeypatch.setenv("TSFILE_DATAFRAME_TRUST_INDEX", "1")

    with TsFileDataFrame(
        str(source), show_progress=False, trust_index=False
    ) as dataframe:
        assert dataframe._trust_index is False
        assert dataframe._runtime is None
        assert len(dataframe) == 1


def test_trusted_index_requires_an_existing_index(tmp_path):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)

    with pytest.raises(FileNotFoundError, match="Trusted Dataset Index not found"):
        TsFileDataFrame(str(source), show_progress=False, trust_index=True)


def test_trusted_index_skips_reader_generation_revalidation(tmp_path):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as first:
        assert len(first) == 1

    with TsFileDataFrame(
        str(source), show_progress=False, trust_index=True
    ) as dataframe:
        pool = dataframe._runtime.readers
        with pool.acquire(0):
            pass
        stat = os.stat(source)
        os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
        with pool.acquire(0):
            pass


def test_named_selection_reuses_bounded_runtime_descriptor(tmp_path, monkeypatch):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        name = str(dataframe.list_timeseries()[0])
        find_device_calls = 0
        original_find_device = dataframe._runtime.index.find_device_id

        def count_find_device(*args, **kwargs):
            nonlocal find_device_calls
            find_device_calls += 1
            return original_find_device(*args, **kwargs)

        def fail_series_info(*_args, **_kwargs):
            raise AssertionError(
                "descriptor-backed selection must not rebuild series info"
            )

        monkeypatch.setattr(
            dataframe._runtime.index, "find_device_id", count_find_device
        )
        monkeypatch.setattr(
            RuntimeSeriesReader, "get_series_info_by_ref", fail_series_info
        )

        first = dataframe[name]
        second = dataframe[name]
        assert first.stats == {"start_time": 0, "end_time": 1, "count": 2}
        np.testing.assert_array_equal(first[:], np.array([0.0, 1.0]))
        assert second.stats == first.stats
        assert find_device_calls == 1
        assert len(dataframe._index._descriptor_cache) == 1
        first.close()
        second.close()


def test_listed_series_path_resolves_directly_by_snapshot_series_id(
    tmp_path, monkeypatch
):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        path = dataframe.list_timeseries()[0]
        assert isinstance(path, str)
        assert path.series_id == 0
        assert path._index_identity == dataframe._runtime.index.identity

        def fail_name_lookup(*_args, **_kwargs):
            raise AssertionError("listed SeriesPath must bypass device name lookup")

        def fail_name_rebuild(*_args, **_kwargs):
            raise AssertionError("listed SeriesPath must not be rebuilt from the index")

        def fail_span_lookup(*_args, **_kwargs):
            raise AssertionError("descriptor locator must bypass series span lookup")

        monkeypatch.setattr(
            dataframe._runtime.index, "find_device_id", fail_name_lookup
        )
        monkeypatch.setattr(dataframe, "_build_series_name", fail_name_rebuild)
        monkeypatch.setattr(RuntimeSeriesReader, "_span", fail_span_lookup)
        series = dataframe[path]
        assert series.name is path
        np.testing.assert_array_equal(series[:], np.array([0.0, 1.0]))
        series.close()

        # Converting to a plain str deliberately drops the snapshot-local id.
        assert not hasattr(str(path), "series_id")


def test_series_path_from_another_index_falls_back_to_its_name(tmp_path, monkeypatch):
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    first_source = first_dir / "part.tsfile"
    second_source = second_dir / "part.tsfile"
    _write_runtime_file(first_source, 0)
    _write_runtime_file(second_source, 10)

    with TsFileDataFrame(
        str(first_source), show_progress=False, use_index=True
    ) as first:
        foreign_path = first.list_timeseries()[0]
        with TsFileDataFrame(
            str(second_source), show_progress=False, use_index=True
        ) as second:
            assert foreign_path._index_identity != second._runtime.index.identity
            find_device_calls = 0
            original_find_device = second._runtime.index.find_device_id

            def count_find_device(*args, **kwargs):
                nonlocal find_device_calls
                find_device_calls += 1
                return original_find_device(*args, **kwargs)

            monkeypatch.setattr(
                second._runtime.index, "find_device_id", count_find_device
            )
            series = second[foreign_path]
            np.testing.assert_array_equal(series[:], np.array([10.0, 11.0]))
            assert find_device_calls == 1
            series.close()


def test_runtime_descriptor_cache_evicts_least_recent_name(tmp_path, monkeypatch):
    source = tmp_path / "devices.tsfile"
    _write_runtime_devices_file(source)
    monkeypatch.setattr(runtime_module, "_SERIES_DESCRIPTOR_CACHE_SIZE", 2)

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        names = [str(name) for name in dataframe.list_timeseries()]
        find_device_calls = 0
        original_find_device = dataframe._runtime.index.find_device_id

        def count_find_device(*args, **kwargs):
            nonlocal find_device_calls
            find_device_calls += 1
            return original_find_device(*args, **kwargs)

        monkeypatch.setattr(
            dataframe._runtime.index, "find_device_id", count_find_device
        )
        for name in names:
            dataframe[name].close()

        assert find_device_calls == 3
        assert len(dataframe._index._descriptor_cache) == 2
        assert len(dataframe._index.series_shards._cache) == 2

        # d0 was the least recently used name and must be resolved again.
        dataframe[names[0]].close()
        assert find_device_calls == 4


def test_reader_pool_enforces_open_file_cap(tmp_path, monkeypatch):
    first = tmp_path / "part1.tsfile"
    second = tmp_path / "part2.tsfile"
    _write_runtime_file(first, 0)
    _write_runtime_file(second, 2)
    monkeypatch.setenv("TSFILE_DATAFRAME_MAX_OPEN_FILES", "1")
    with TsFileDataFrame(
        [str(first), str(second)], show_progress=False, use_index=True
    ) as dataframe:
        series = dataframe[0]
        assert list(series[:]) == [0.0, 1.0, 2.0, 3.0]
        assert dataframe._runtime.readers.open_count == 1
        assert dataframe._runtime.prepared.size == 2
        series.close()


def test_runtime_consume_concatenates_arrow_batches_without_scalar_reads():
    class _ArrowResult:
        def __init__(self):
            self._batches = iter(
                [
                    pa.table(
                        {
                            "time": pa.array([], type=pa.int64()),
                            "value": pa.array([], type=pa.float64()),
                        }
                    ),
                    pa.table(
                        {
                            "time": pa.array([1, 2], type=pa.int64()),
                            "value": pa.array([10.0, None], type=pa.float64()),
                        }
                    ),
                    pa.table(
                        {
                            "time": pa.array([3], type=pa.int64()),
                            "value": pa.array([30.0], type=pa.float64()),
                        }
                    ),
                ]
            )
            self.closed = False

        def __enter__(self):
            return self

        def __exit__(self, *_):
            self.closed = True

        def read_arrow_batch(self):
            return next(self._batches, None)

        def next(self):
            raise AssertionError("Runtime must not consume prepared rows one by one")

    result = _ArrowResult()
    timestamps, values = RuntimeSeriesReader._consume(result)

    np.testing.assert_array_equal(timestamps, np.array([1, 2, 3], dtype=np.int64))
    np.testing.assert_allclose(values, np.array([10.0, np.nan, 30.0]), equal_nan=True)
    assert result.closed


def test_prepared_query_reads_nullable_offset_window_in_arrow_batches(tmp_path):
    source = tmp_path / "nullable.tsfile"
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    expected = np.arange(10, dtype=np.float64)
    expected[2] = np.nan
    expected[6] = np.nan
    with TsFileTableWriter(str(source), schema) as writer:
        writer.write_dataframe(
            pd.DataFrame(
                {
                    "time": np.arange(10, dtype=np.int64),
                    "device": ["d0"] * 10,
                    "value": expected,
                }
            )
        )

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        runtime = dataframe._runtime
        series = runtime.index.record(LOGICAL_SERIES, 0)
        span = runtime.index.record(SERIES_FILE_SPAN, series[2])
        with runtime.readers.acquire(0) as reader:
            with runtime.prepared.acquire(0, span[2], reader) as prepared:
                with reader.query_prepared(prepared, offset=1, limit=7) as result:
                    batches = []
                    while True:
                        batch = result.read_arrow_batch()
                        if batch is None:
                            break
                        batches.append(batch)
                with reader.query_prepared(
                    prepared, start_time=100, end_time=200
                ) as empty_result:
                    assert empty_result.read_arrow_batch() is None

    assert batches
    table = pa.concat_tables(batches)
    np.testing.assert_array_equal(
        table.column("time").to_numpy(), np.arange(1, 8, dtype=np.int64)
    )
    np.testing.assert_allclose(
        table.column("value").to_numpy(zero_copy_only=False),
        expected[1:8],
        equal_nan=True,
    )


def test_prepared_locator_rejects_stale_generation_and_bad_range(tmp_path):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        runtime = dataframe._runtime
        series = runtime.index.record(LOGICAL_SERIES, 0)
        span = runtime.index.record(SERIES_FILE_SPAN, series[2])
        locator = list(runtime.prepared._locator_tuple(0, span[2]))
        with runtime.readers.acquire(0) as reader:
            stale = list(locator)
            stale[3] ^= 1
            with pytest.raises(Exception, match="prepare Dataset Index locator"):
                reader.prepare_series(stale)

            out_of_range = list(locator)
            out_of_range[7] = os.path.getsize(source) + 1
            with pytest.raises(Exception, match="prepare Dataset Index locator"):
                reader.prepare_series(out_of_range)


def test_reader_session_revalidates_generation_when_reused(tmp_path):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        pool = dataframe._runtime.readers
        with pool.acquire(0):
            pass
        stat = os.stat(source)
        os.utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
        with pytest.raises(RuntimeError, match="generation changed"):
            with pool.acquire(0):
                pass


def test_runtime_lease_close_waits_for_query_lease(tmp_path):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        runtime = DatasetRuntime(str(dataframe._runtime.index.path), query_workers=1)
        lease = runtime.lease()
        entered = threading.Event()
        release = threading.Event()
        query_done = threading.Event()

        def run_query():
            with lease.query_lease():
                entered.set()
                assert release.wait(timeout=2)
            query_done.set()

        query_thread = threading.Thread(target=run_query)
        query_thread.start()
        assert entered.wait(timeout=2)

        close_done = threading.Event()

        def close_lease():
            lease.close()
            close_done.set()

        close_thread = threading.Thread(target=close_lease)
        close_thread.start()
        assert not close_done.wait(timeout=0.05)

        release.set()
        query_thread.join(timeout=2)
        close_thread.join(timeout=2)
        assert query_done.is_set()
        assert close_done.is_set()


def _write_tiny_mapped_index(path):
    source = path.with_suffix(".tsfile")
    source.write_bytes(b"T" * 4096)
    write_index_atomic(
        str(path), build_sections_from_dataframe(_synthetic_dataframe(str(source)))
    )


def _target_inode(path):
    stat = os.stat(path)
    return stat.st_dev, stat.st_ino


def _target_inode_fd_count(path):
    if not os.path.isdir("/proc/self/fd"):
        pytest.skip("Linux /proc FD attribution is unavailable")
    target = _target_inode(path)
    count = 0
    for fd in os.listdir("/proc/self/fd"):
        try:
            stat = os.stat(f"/proc/self/fd/{fd}")
        except FileNotFoundError:
            continue
        if (stat.st_dev, stat.st_ino) == target:
            count += 1
    return count


def _target_inode_vma_count(path):
    if not os.path.isfile("/proc/self/maps"):
        pytest.skip("Linux /proc VMA attribution is unavailable")
    target_device, target_inode = _target_inode(path)
    target_device = f"{os.major(target_device):02x}:{os.minor(target_device):02x}"
    target_inode = str(target_inode)
    count = 0
    with open("/proc/self/maps", encoding="utf-8") as maps:
        for line in maps:
            fields = line.split(maxsplit=5)
            if (
                len(fields) >= 5
                and fields[3] == target_device
                and fields[4] == target_inode
            ):
                count += 1
    return count


def _assert_index_lookup(index):
    table_id = index.find_table_ids("root")[0]
    device_id = index.find_device_id(table_id, "root.")
    column_id = index.find_column_id(table_id, "s1")
    assert index.find_series_id(device_id, column_id) == 0


def test_mapped_index_releases_original_fd_after_readonly_mmap(tmp_path):
    output = tmp_path / "single.tsidx"
    _write_tiny_mapped_index(output)
    baseline_fds = _target_inode_fd_count(output)
    baseline_vmas = _target_inode_vma_count(output)

    index = MappedDatasetIndex(str(output))
    try:
        _assert_index_lookup(index)
        assert _target_inode_fd_count(output) == baseline_fds + 1
        assert index._file is None
        assert _target_inode_vma_count(output) == baseline_vmas + 1
    finally:
        index.close()

    assert _target_inode_fd_count(output) == baseline_fds
    assert _target_inode_vma_count(output) == baseline_vmas


def test_mapped_indices_keep_one_fd_and_live_vma_per_mapping(tmp_path):
    paths = [tmp_path / f"index-{number}.tsidx" for number in range(16)]
    for path in paths:
        _write_tiny_mapped_index(path)

    baseline_vmas = {path: _target_inode_vma_count(path) for path in paths}
    indices = [MappedDatasetIndex(str(path)) for path in paths]
    try:
        for path, index in zip(paths, indices):
            _assert_index_lookup(index)
            assert _target_inode_fd_count(path) == 1
            assert index._file is None
            assert _target_inode_vma_count(path) == baseline_vmas[path] + 1
    finally:
        for index in indices:
            index.close()
        for index in indices:
            index.close()

    for path in paths:
        assert _target_inode_fd_count(path) == 0
        assert _target_inode_vma_count(path) == baseline_vmas[path]


def test_mapped_index_mmap_failure_releases_target_resources(tmp_path, monkeypatch):
    output = tmp_path / "mmap-failure.tsidx"
    _write_tiny_mapped_index(output)
    baseline_fds = _target_inode_fd_count(output)
    baseline_vmas = _target_inode_vma_count(output)

    def fail_mmap(*_args, **_kwargs):
        raise OSError("injected mmap failure")

    monkeypatch.setattr(index_module.mmap, "mmap", fail_mmap)
    with pytest.raises(OSError, match="injected mmap failure"):
        MappedDatasetIndex(str(output))

    assert _target_inode_fd_count(output) == baseline_fds
    assert _target_inode_vma_count(output) == baseline_vmas


@pytest.mark.parametrize(
    ("trust_index", "method_name"),
    [(False, "_validate"), (True, "_map_entries_without_validation")],
)
def test_mapped_index_post_mmap_failure_releases_target_resources(
    tmp_path, monkeypatch, trust_index, method_name
):
    output = tmp_path / f"post-mmap-{trust_index}.tsidx"
    _write_tiny_mapped_index(output)
    baseline_fds = _target_inode_fd_count(output)
    baseline_vmas = _target_inode_vma_count(output)

    def fail_after_mmap(*_args, **_kwargs):
        raise RuntimeError("injected post-mmap failure")

    monkeypatch.setattr(MappedDatasetIndex, method_name, fail_after_mmap)
    with pytest.raises(RuntimeError, match="injected post-mmap failure"):
        MappedDatasetIndex(str(output), trust_index=trust_index)

    assert _target_inode_fd_count(output) == baseline_fds
    assert _target_inode_vma_count(output) == baseline_vmas


def test_mapped_index_close_is_idempotent_and_closes_later_resources_after_error():
    events = []

    class _View:
        def release(self):
            events.append("view.release")
            raise RuntimeError("view release failed")

    class _Mmap:
        def close(self):
            events.append("mmap.close")

    class _File:
        def close(self):
            events.append("file.close")

    index = MappedDatasetIndex.__new__(MappedDatasetIndex)
    index._view = _View()
    index._mmap = _Mmap()
    index._file = _File()

    with pytest.raises(RuntimeError, match="view release failed"):
        index.close()
    assert events == ["view.release", "mmap.close", "file.close"]
    assert index._view is None
    assert index._mmap is None
    assert index._file is None
    index.close()


def _linux_proc_resource_attribution_available():
    return (
        hasattr(os, "fork")
        and os.path.isdir("/proc/self/fd")
        and os.path.isfile("/proc/self/maps")
    )


def test_linux_proc_resource_attribution_guard_requires_proc(monkeypatch):
    monkeypatch.setattr(os, "fork", lambda: None, raising=False)
    monkeypatch.setattr(os.path, "isdir", lambda path: path != "/proc/self/fd")
    monkeypatch.setattr(os.path, "isfile", lambda path: True)
    assert not _linux_proc_resource_attribution_available()


def test_mapped_index_inherited_mapping_survives_child_double_close(tmp_path):
    if not _linux_proc_resource_attribution_available():
        pytest.skip("Linux fork /proc resource attribution is unavailable")
    output = tmp_path / "fork.tsidx"
    _write_tiny_mapped_index(output)
    index = MappedDatasetIndex(str(output))
    read_fd, write_fd = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        os.close(read_fd)
        try:
            _assert_index_lookup(index)
            assert index._file is None
            assert _target_inode_fd_count(output) == 1
            index.close()
            index.close()
            assert _target_inode_fd_count(output) == 0
            assert _target_inode_vma_count(output) == 0
            os.write(write_fd, b"OK")
            status = 0
        except BaseException as exc:
            os.write(write_fd, f"{type(exc).__name__}: {exc}".encode())
            status = 1
        finally:
            os.close(write_fd)
        os._exit(status)

    os.close(write_fd)
    child_message = os.read(read_fd, 4096)
    os.close(read_fd)
    waited_pid, child_status = os.waitpid(child_pid, 0)
    try:
        assert waited_pid == child_pid
        assert os.WIFEXITED(child_status), child_message.decode()
        assert os.WEXITSTATUS(child_status) == 0, child_message.decode()
        assert child_message == b"OK"
        _assert_index_lookup(index)
        assert _target_inode_fd_count(output) == 1
    finally:
        index.close()
        index.close()

    assert _target_inode_fd_count(output) == 0
    assert _target_inode_vma_count(output) == 0


class _LifecycleAbort(BaseException):
    pass


class _PreparedIndexStub:
    @staticmethod
    def record(section_type, record_id):
        if section_type == SERIES_LOCATOR:
            return (0, 0, record_id * 10, 8, 1)
        if section_type == DEVICE_FILE_SPAN:
            return (0, 0, 100, 16, 1, 0, 2)
        if section_type == TSFILE_RECORD:
            return (0, 0, 4096, 12345)
        raise AssertionError(section_type)


class _PreparedHandleStub:
    def __init__(
        self, locator_id, close_error=None, time_owner=None, close_events=None
    ):
        self.locator_id = locator_id
        self.close_error = close_error
        self.time_owner = time_owner
        self.close_events = close_events
        self.close_calls = 0

    def close(self):
        self.close_calls += 1
        if self.close_events is not None:
            self.close_events.append(f"prepared.close:{self.locator_id}")
        if self.close_error is not None:
            raise self.close_error


class _PreparedReaderStub:
    def __init__(
        self,
        *,
        abort_once=False,
        abort_query=False,
        close_error_ids=(),
        close_events=None,
        query_result=None,
    ):
        self.abort_once = abort_once
        self.abort_query = abort_query
        self.close_error_ids = set(close_error_ids)
        self.close_events = close_events
        self.query_result = query_result
        self.prepared = []

    def prepare_series(self, locator, time_owner=None):
        if self.abort_once:
            self.abort_once = False
            raise _LifecycleAbort("prepare interrupted")
        locator_id = locator[4]
        prepared = _PreparedHandleStub(
            locator_id,
            close_error=(
                RuntimeError(f"close {locator_id}")
                if locator_id in self.close_error_ids
                else None
            ),
            time_owner=time_owner,
            close_events=self.close_events,
        )
        self.prepared.append(prepared)
        return prepared

    def query_prepared(self, _prepared, **_kwargs):
        if self.abort_query:
            raise _LifecycleAbort("query interrupted")
        return self.query_result


def test_prepared_cache_evicts_lru_and_plateaus_at_configured_cap():
    reader = _PreparedReaderStub()
    cache = runtime_module.PreparedSeriesCache(_PreparedIndexStub(), max_entries=2)

    with cache.acquire(0, 0, reader) as first:
        assert first.locator_id == 0
    with cache.acquire(0, 1, reader):
        pass
    with cache.acquire(0, 0, reader) as reused:
        assert reused is first
    with cache.acquire(0, 2, reader):
        pass

    assert cache.size == 2
    assert [entry.close_calls for entry in reader.prepared] == [0, 1, 0]
    cache.close()
    assert [entry.close_calls for entry in reader.prepared] == [1, 1, 1]


def test_prepared_cache_default_is_finite_and_plateaus_without_environment(
    monkeypatch,
):
    monkeypatch.delenv("TSFILE_DATAFRAME_MAX_PREPARED_SERIES", raising=False)
    reader = _PreparedReaderStub()
    cache = runtime_module.PreparedSeriesCache(_PreparedIndexStub())

    for locator_id in range(4097):
        with cache.acquire(0, locator_id, reader):
            pass

    assert cache.max_entries == 4096
    assert cache.size == 4096
    assert reader.prepared[0].close_calls == 1
    assert all(entry.close_calls == 0 for entry in reader.prepared[1:])
    cache.close()
    assert all(entry.close_calls == 1 for entry in reader.prepared)


def test_prepared_cache_keeps_active_lru_until_lease_release():
    reader = _PreparedReaderStub()
    cache = runtime_module.PreparedSeriesCache(_PreparedIndexStub(), max_entries=1)

    with cache.acquire(0, 0, reader) as active:
        with cache.acquire(0, 1, reader):
            assert cache.size == 2
            assert active.close_calls == 0
        assert cache.size == 1
        assert active.close_calls == 0

    with cache.acquire(0, 2, reader):
        pass
    assert active.close_calls == 1
    assert cache.size == 1
    cache.close()


@pytest.mark.parametrize("max_entries", [0, 1, 2])
def test_prepared_cache_owner_graph_plateaus_without_dependency_chains(max_entries):
    reader = _PreparedReaderStub()
    cache = runtime_module.PreparedSeriesCache(
        _PreparedIndexStub(), max_entries=max_entries
    )

    for locator_id in range(1, 33):
        with cache.acquire(0, locator_id - 1, reader) as owner:
            with cache.acquire(0, locator_id, reader, time_owner=owner):
                pass

        reachable = {}
        pending = [entry.prepared for entry in cache._entries.values()]
        while pending:
            prepared = pending.pop()
            if id(prepared) in reachable:
                continue
            reachable[id(prepared)] = prepared
            if prepared.time_owner is not None:
                pending.append(prepared.time_owner)

        assert cache.size <= cache.max_entries
        assert len(reachable) <= cache.max_entries
        assert all(
            prepared.time_owner is None or prepared.time_owner.time_owner is None
            for prepared in reachable.values()
        )

    cache.close()
    assert all(prepared.close_calls == 1 for prepared in reader.prepared)


def test_prepared_cache_zero_retention_closes_each_handle_exactly_once():
    reader = _PreparedReaderStub()
    cache = runtime_module.PreparedSeriesCache(_PreparedIndexStub(), max_entries=0)

    for _ in range(3):
        with cache.acquire(0, 0, reader):
            assert cache.size == 1
        assert cache.size == 0

    assert len(reader.prepared) == 3
    assert [entry.close_calls for entry in reader.prepared] == [1, 1, 1]
    cache.close()
    assert [entry.close_calls for entry in reader.prepared] == [1, 1, 1]


def test_prepared_cache_recovers_single_flight_after_prepare_baseexception():
    reader = _PreparedReaderStub(abort_once=True)
    cache = runtime_module.PreparedSeriesCache(_PreparedIndexStub(), max_entries=0)

    with pytest.raises(_LifecycleAbort, match="prepare interrupted"):
        with cache.acquire(0, 0, reader):
            pass

    assert not cache._loading
    with cache.acquire(0, 0, reader) as prepared:
        assert prepared.locator_id == 0
    assert prepared.close_calls == 1
    cache.close()


def test_prepared_cache_close_is_idempotent_and_closes_past_errors():
    reader = _PreparedReaderStub(close_error_ids={0})
    cache = runtime_module.PreparedSeriesCache(_PreparedIndexStub())
    with cache.acquire(0, 0, reader) as first:
        pass
    with cache.acquire(0, 1, reader) as second:
        pass

    with pytest.raises(RuntimeError, match="close 0"):
        cache.close()
    assert first.close_calls == 1
    assert second.close_calls == 1

    cache.close()
    assert first.close_calls == 1
    assert second.close_calls == 1


def test_runtime_discard_after_fork_is_lock_free_exact_once_and_resilient():
    from concurrent.futures import thread as thread_pool_module
    import queue

    close_calls = []

    class _PoisonCondition:
        def __enter__(self):
            raise AssertionError("inherited condition must not be acquired")

    class _Discardable:
        def __init__(self, name, fail=False):
            self.name = name
            self.fail = fail

        def discard_after_fork(self):
            close_calls.append(self.name)
            if self.fail:
                raise RuntimeError(f"discard {self.name}")

    class _Index:
        def close(self):
            close_calls.append("index")

    class _Executor:
        def __init__(self):
            self._shutdown = False
            self._work_queue = queue.SimpleQueue()
            self._threads = {_InheritedThread()}

        def shutdown(self, **_kwargs):
            raise AssertionError("an inherited executor must not be shut down")

    class _InheritedThread:
        def __init__(self):
            self._target = object()
            self._args = (object(),)
            self._kwargs = {"inherited": True}

        @staticmethod
        def join():
            pass

    runtime = object.__new__(runtime_module.DatasetRuntime)
    runtime.creator_pid = os.getpid() + 1
    runtime._condition = _PoisonCondition()
    executor = _Executor()
    inherited_thread = next(iter(executor._threads))
    thread_pool_module._threads_queues[inherited_thread] = executor._work_queue
    runtime._query_executor = executor
    runtime.prepared = _Discardable("prepared", fail=True)
    runtime.readers = _Discardable("readers")
    runtime.index = _Index()
    catalog = SimpleNamespace(
        runtime=runtime, index=runtime.index, _readers={0: object()}
    )
    runtime.catalog = catalog
    runtime._resources_closed = False
    runtime._discarded_pid = None
    runtime._accepting = True
    runtime._torn_down = False

    with pytest.raises(RuntimeError, match="discard prepared"):
        runtime.discard_after_fork()

    assert close_calls == ["prepared", "readers", "index"]
    assert runtime._resources_closed
    assert runtime._discarded_pid == os.getpid()
    assert runtime._query_executor is None
    assert executor._shutdown
    assert executor._work_queue is None
    assert executor._threads == set()
    assert inherited_thread not in thread_pool_module._threads_queues
    assert inherited_thread._target is None
    assert inherited_thread._args == ()
    assert inherited_thread._kwargs == {}
    assert runtime.prepared is None
    assert runtime.readers is None
    assert runtime.index is None
    assert runtime.catalog is None
    assert catalog.runtime is None
    assert catalog.index is None
    assert catalog._readers == {}

    runtime.discard_after_fork()
    assert close_calls == ["prepared", "readers", "index"]


def test_reader_pool_never_evicts_an_active_reader(monkeypatch):
    pool = runtime_module.ReaderSessionPool(
        SimpleNamespace(), max_open_files=1, validate_generation=False
    )
    sessions = {}

    class _Session:
        def __init__(self, file_id):
            self.reader = file_id
            self.active_uses = 0
            self.close_calls = 0

        def close(self):
            self.close_calls += 1

    def new_session(file_id):
        session = _Session(file_id)
        sessions[file_id] = session
        return session

    monkeypatch.setattr(pool, "_new_session", new_session)
    acquired_second = threading.Event()

    def acquire_second():
        with pool.acquire(1):
            acquired_second.set()

    with pool.acquire(0):
        thread = threading.Thread(target=acquire_second)
        thread.start()
        assert not acquired_second.wait(timeout=0.05)
        assert sessions[0].close_calls == 0

    thread.join(timeout=2)
    assert acquired_second.is_set()
    assert sessions[0].close_calls == 1
    pool.close()
    assert sessions[1].close_calls == 1


def test_runtime_partial_construction_rolls_back_all_resources(monkeypatch):
    events = []

    class _Index:
        def __init__(self, *_args, **_kwargs):
            events.append("index.open")

        def close(self):
            events.append("index.close")

    class _Executor:
        def __init__(self, *_args, **_kwargs):
            events.append("executor.open")

        def shutdown(self, **kwargs):
            events.append(("executor.shutdown", kwargs))

    class _Readers:
        def __init__(self, *_args, **_kwargs):
            events.append("readers.open")

        def close(self):
            events.append("readers.close")

    class _Prepared:
        def __init__(self, *_args, **_kwargs):
            events.append("prepared.open")

        def close(self):
            events.append("prepared.close")
            raise RuntimeError("prepared cleanup failed")

    class _Catalog:
        def __init__(self, _runtime):
            raise _LifecycleAbort("catalog construction interrupted")

    monkeypatch.setattr(runtime_module, "MappedDatasetIndex", _Index)
    monkeypatch.setattr(runtime_module, "ThreadPoolExecutor", _Executor)
    monkeypatch.setattr(runtime_module, "ReaderSessionPool", _Readers)
    monkeypatch.setattr(runtime_module, "PreparedSeriesCache", _Prepared)
    monkeypatch.setattr(runtime_module, "MappedDataFrameCatalog", _Catalog)

    with pytest.raises(_LifecycleAbort, match="catalog construction interrupted"):
        runtime_module.DatasetRuntime("unused.tsidx", query_workers=2)

    assert events == [
        "index.open",
        "executor.open",
        "readers.open",
        "prepared.open",
        ("executor.shutdown", {"wait": True, "cancel_futures": True}),
        "prepared.close",
        "readers.close",
        "index.close",
    ]


@pytest.mark.parametrize("cache_size", [0, 2])
def test_prepared_cache_follows_environment_cap(tmp_path, monkeypatch, cache_size):
    source = tmp_path / "devices.tsfile"
    _write_runtime_devices_file(source)
    monkeypatch.setenv("TSFILE_DATAFRAME_MAX_PREPARED_SERIES", str(cache_size))

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        for index in range(3):
            series = dataframe[index]
            np.testing.assert_array_equal(
                series[:], np.array([index * 10.0, index * 10.0 + 1.0])
            )
            series.close()

        assert dataframe._runtime.prepared.max_entries == cache_size
        assert dataframe._runtime.prepared.size == cache_size


class _ReaderPoolStub:
    def __init__(self, reader, events):
        self.reader = reader
        self.events = events
        self.acquire_calls = 0
        self.release_calls = 0

    def acquire(self, _file_id):
        pool = self

        class _Lease:
            def __enter__(self):
                pool.acquire_calls += 1
                return pool.reader

            def __exit__(self, *_args):
                pool.release_calls += 1
                pool.events.append("reader.release")

        return _Lease()


class _FailingResultStub:
    def __init__(self, events):
        self.events = events
        self.close_calls = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close_calls += 1
        self.events.append("result.close")

    @staticmethod
    def read_arrow_batch():
        raise _LifecycleAbort("decode interrupted")


def test_query_baseexception_releases_prepared_and_reader_exactly_once():
    events = []
    reader = _PreparedReaderStub(
        abort_query=True,
        close_events=events,
    )
    pool = _ReaderPoolStub(reader, events)
    prepared_cache = runtime_module.PreparedSeriesCache(
        _PreparedIndexStub(), max_entries=0
    )
    runtime = SimpleNamespace(
        index=_PreparedIndexStub(), readers=pool, prepared=prepared_cache
    )

    with pytest.raises(_LifecycleAbort, match="query interrupted"):
        RuntimeSeriesReader(runtime, 0)._query_at_locator(0, offset=0, limit=1)

    assert [item.close_calls for item in reader.prepared] == [1]
    assert pool.acquire_calls == 1
    assert pool.release_calls == 1
    assert events == ["prepared.close:0", "reader.release"]
    prepared_cache.close()


def test_decode_baseexception_closes_result_before_ownership_leases():
    events = []
    result = _FailingResultStub(events)
    reader = _PreparedReaderStub(
        close_events=events,
        query_result=result,
    )
    pool = _ReaderPoolStub(reader, events)
    prepared_cache = runtime_module.PreparedSeriesCache(
        _PreparedIndexStub(), max_entries=0
    )
    runtime = SimpleNamespace(
        index=_PreparedIndexStub(), readers=pool, prepared=prepared_cache
    )

    with pytest.raises(_LifecycleAbort, match="decode interrupted"):
        RuntimeSeriesReader(runtime, 0)._query_at_locator(0, offset=0, limit=1)

    assert result.close_calls == 1
    assert [item.close_calls for item in reader.prepared] == [1]
    assert pool.release_calls == 1
    assert events == ["result.close", "prepared.close:0", "reader.release"]
    prepared_cache.close()


def test_reader_pool_close_is_idempotent_and_closes_past_errors(monkeypatch):
    pool = runtime_module.ReaderSessionPool(
        SimpleNamespace(), max_open_files=2, validate_generation=False
    )
    sessions = {}

    class _Session:
        def __init__(self, file_id):
            self.reader = file_id
            self.active_uses = 0
            self.close_calls = 0

        def close(self):
            self.close_calls += 1
            if self.reader == 0:
                raise RuntimeError("reader close 0")

    def new_session(file_id):
        session = _Session(file_id)
        sessions[file_id] = session
        return session

    monkeypatch.setattr(pool, "_new_session", new_session)
    with pool.acquire(0):
        pass
    with pool.acquire(1):
        pass

    with pytest.raises(RuntimeError, match="reader close 0"):
        pool.close()
    assert sessions[0].close_calls == 1
    assert sessions[1].close_calls == 1
    pool.close()
    assert sessions[0].close_calls == 1
    assert sessions[1].close_calls == 1


def test_resultset_survives_zero_retention_prepared_eviction(tmp_path, monkeypatch):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    monkeypatch.setenv("TSFILE_DATAFRAME_MAX_PREPARED_SERIES", "0")

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        runtime = dataframe._runtime
        series = runtime.index.record(LOGICAL_SERIES, 0)
        span = runtime.index.record(SERIES_FILE_SPAN, series[2])
        with runtime.readers.acquire(0) as reader:
            prepared_lease = runtime.prepared.acquire(0, span[2], reader)
            prepared = prepared_lease.__enter__()
            result = reader.query_prepared(prepared, offset=0, limit=2)
            prepared_lease.__exit__(None, None, None)
            assert runtime.prepared.size == 0
            timestamps, values = RuntimeSeriesReader._consume(result)

    np.testing.assert_array_equal(timestamps, np.array([0, 1], dtype=np.int64))
    np.testing.assert_array_equal(values, np.array([0.0, 1.0]))


def _write_runtime_aligned_fields_file(path):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value_a", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("value_b", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(
            pd.DataFrame(
                {
                    "time": [0, 1, 2],
                    "device": ["d0", "d0", "d0"],
                    "value_a": [1.0, 2.0, 3.0],
                    "value_b": [10.0, 20.0, 30.0],
                }
            )
        )


@pytest.mark.parametrize("release_owner_first", [True, False])
def test_aligned_prepared_handles_survive_both_eviction_orders(
    tmp_path, monkeypatch, release_owner_first
):
    source = tmp_path / "aligned.tsfile"
    _write_runtime_aligned_fields_file(source)
    monkeypatch.setenv("TSFILE_DATAFRAME_MAX_PREPARED_SERIES", "0")

    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        runtime = dataframe._runtime
        series_records = [
            runtime.index.record(LOGICAL_SERIES, index) for index in range(2)
        ]
        spans = [
            runtime.index.record(SERIES_FILE_SPAN, series[2])
            for series in series_records
        ]
        with runtime.readers.acquire(0) as reader:
            owner_lease = runtime.prepared.acquire(0, spans[0][2], reader)
            owner = owner_lease.__enter__()
            value_lease = runtime.prepared.acquire(
                0, spans[1][2], reader, time_owner=owner
            )
            value = value_lease.__enter__()

            if release_owner_first:
                owner_lease.__exit__(None, None, None)
                result = reader.query_prepared(value, offset=0, limit=3)
                value_lease.__exit__(None, None, None)
                expected = np.array([10.0, 20.0, 30.0])
            else:
                value_lease.__exit__(None, None, None)
                result = reader.query_prepared(owner, offset=0, limit=3)
                owner_lease.__exit__(None, None, None)
                expected = np.array([1.0, 2.0, 3.0])

            assert runtime.prepared.size == 0
            timestamps, values = RuntimeSeriesReader._consume(result)

    np.testing.assert_array_equal(timestamps, np.array([0, 1, 2], dtype=np.int64))
    np.testing.assert_array_equal(values, expected)


def test_dataframe_reopens_process_local_runtime_after_fork(tmp_path):
    if not _linux_proc_resource_attribution_available():
        pytest.skip("Linux fork lifecycle validation is unavailable")

    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        parent_runtime = dataframe._runtime
        parent_reader_pool = parent_runtime.readers
        parent_prepared_cache = parent_runtime.prepared
        parent_index = parent_runtime.index
        parent_executor = parent_runtime._query_executor
        inherited_executor_threads = ()
        if parent_executor is not None:
            assert (
                parent_executor.submit(lambda: "parent-ready").result()
                == "parent-ready"
            )
            inherited_executor_threads = tuple(parent_executor._threads)
            assert inherited_executor_threads
        inherited_runtime_lease = dataframe._runtime_lease
        series = dataframe[0]
        np.testing.assert_array_equal(series[:], np.array([0.0, 1.0]))
        series.close()
        assert parent_reader_pool.open_count == 1
        assert parent_prepared_cache.size == 1

        read_fd, write_fd = os.pipe()
        child_pid = os.fork()
        if child_pid == 0:
            os.close(read_fd)
            try:
                child_series = dataframe[0]
                child_runtime = dataframe._runtime
                assert child_runtime is not parent_runtime
                assert child_runtime.readers is not parent_reader_pool
                assert child_runtime.prepared is not parent_prepared_cache
                assert child_runtime.creator_pid == os.getpid()
                assert parent_runtime._resources_closed
                assert parent_runtime._discarded_pid == os.getpid()
                assert parent_runtime.readers is None
                assert parent_runtime.prepared is None
                assert parent_runtime.index is None
                assert parent_runtime.catalog is None
                if parent_executor is not None:
                    assert parent_executor._shutdown
                    assert parent_executor._work_queue is None
                    assert parent_executor._threads == set()
                    assert all(
                        thread._target is None
                        and thread._args == ()
                        and thread._kwargs == {}
                        for thread in inherited_executor_threads
                    )
                assert parent_reader_pool._closed
                assert parent_reader_pool._sessions == {}
                assert parent_prepared_cache._closed
                assert parent_prepared_cache._entries == {}
                assert parent_index._view is None
                assert inherited_runtime_lease._closed
                values = child_series[:]
                child_series.close()
                np.testing.assert_array_equal(values, np.array([0.0, 1.0]))
                dataframe.close()
                assert child_runtime._resources_closed
                if os.path.isdir("/proc/self/fd"):
                    open_targets = {
                        os.path.realpath(f"/proc/self/fd/{fd}")
                        for fd in os.listdir("/proc/self/fd")
                        if os.path.exists(f"/proc/self/fd/{fd}")
                    }
                    assert os.path.realpath(source) not in open_targets
                    assert os.path.realpath(parent_index.path) not in open_targets
                os.write(write_fd, b"OK")
                status = 0
            except BaseException as exc:
                os.write(write_fd, f"{type(exc).__name__}: {exc}".encode())
                status = 1
            finally:
                os.close(write_fd)
            os._exit(status)

        os.close(write_fd)
        child_message = os.read(read_fd, 4096)
        os.close(read_fd)
        waited_pid, child_status = os.waitpid(child_pid, 0)

        assert waited_pid == child_pid
        assert os.WIFEXITED(child_status), child_message.decode()
        assert os.WEXITSTATUS(child_status) == 0, child_message.decode()
        assert child_message == b"OK"
        assert dataframe._runtime is parent_runtime
        assert dataframe._runtime.readers is parent_reader_pool
        assert dataframe._runtime.prepared is parent_prepared_cache
        assert parent_reader_pool.open_count == 1
        assert parent_prepared_cache.size == 1
        if parent_executor is not None:
            assert not parent_executor._shutdown
            assert parent_executor._work_queue is not None
            assert parent_executor.submit(lambda: "parent-ok").result() == "parent-ok"
        parent_series = dataframe[0]
        np.testing.assert_array_equal(parent_series[:], np.array([0.0, 1.0]))
        parent_series.close()


def test_prefork_timeseries_fails_fast_in_child_without_harming_parent(tmp_path):
    if not _linux_proc_resource_attribution_available():
        pytest.skip("Linux fork lifecycle validation is unavailable")

    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    with TsFileDataFrame(str(source), show_progress=False, use_index=True) as dataframe:
        parent_runtime = dataframe._runtime
        parent_reader_pool = parent_runtime.readers
        parent_prepared_cache = parent_runtime.prepared
        parent_index = parent_runtime.index
        series = dataframe[0]
        np.testing.assert_array_equal(series[:], np.array([0.0, 1.0]))
        inherited_query_lease = series._runtime_lease.query_lease()

        read_fd, write_fd = os.pipe()
        child_pid = os.fork()
        if child_pid == 0:
            os.close(read_fd)
            try:
                with pytest.raises(
                    RuntimeError,
                    match="obtain a new Timeseries from the child.*TsFileDataFrame",
                ):
                    series[:]
                with pytest.raises(
                    RuntimeError,
                    match="obtain a new Timeseries from the child.*TsFileDataFrame",
                ):
                    series._runtime_lease.clone()
                inherited_query_lease.close()
                assert not inherited_query_lease._acquired
                series.close()
                assert series._closed
                assert parent_runtime._resources_closed
                assert parent_runtime._discarded_pid == os.getpid()
                assert parent_runtime.readers is None
                assert parent_runtime.prepared is None
                assert parent_runtime.index is None
                assert parent_runtime.catalog is None
                assert parent_reader_pool._closed
                assert parent_reader_pool._sessions == {}
                assert parent_prepared_cache._closed
                assert parent_prepared_cache._entries == {}
                assert parent_index._view is None
                os.write(write_fd, b"OK")
                status = 0
            except BaseException as exc:
                os.write(write_fd, f"{type(exc).__name__}: {exc}".encode())
                status = 1
            finally:
                os.close(write_fd)
            os._exit(status)

        os.close(write_fd)
        child_message = os.read(read_fd, 4096)
        os.close(read_fd)
        waited_pid, child_status = os.waitpid(child_pid, 0)

        assert waited_pid == child_pid
        assert os.WIFEXITED(child_status), child_message.decode()
        assert os.WEXITSTATUS(child_status) == 0, child_message.decode()
        assert child_message == b"OK"
        assert inherited_query_lease._acquired
        assert parent_runtime._query_leases == 1
        np.testing.assert_array_equal(series[:], np.array([0.0, 1.0]))
        inherited_query_lease.close()
        assert parent_runtime._query_leases == 0
        series.close()


def _build_runtime_attestations(paths):
    canonical_paths = [os.path.realpath(os.fspath(path)) for path in paths]
    with TsFileDataFrame(
        canonical_paths,
        show_progress=False,
        use_index=True,
    ) as dataframe:
        index_path = dataframe._runtime.index.path

    index_attestation = identity_module.attest_index(index_path)
    with MappedDatasetIndex(index_path, trust_index=True) as mapped_index:
        data_attestations = tuple(
            identity_module.attest_data_file(
                file_id,
                mapped_index.string(record[0]),
                existing_index_fingerprint=f"{record[3]:016x}",
            )
            for file_id in range(mapped_index.count(TSFILE_RECORD))
            for record in (mapped_index.record(TSFILE_RECORD, file_id),)
        )
    return index_path, index_attestation, data_attestations


def test_attested_trusted_dataframe_uses_manifest_without_directory_scan(
    tmp_path, monkeypatch
):
    paths = [tmp_path / "part-0.tsfile", tmp_path / "part-1.tsfile"]
    _write_runtime_file(paths[0], 0)
    _write_runtime_file(paths[1], 10)
    _, index_attestation, data_attestations = _build_runtime_attestations(paths)

    def unexpected_scan(_paths):
        raise AssertionError("attested workers must not expand or scan dataset paths")

    monkeypatch.setattr(dataframe_module, "_expand_paths", unexpected_scan)
    with TsFileDataFrame(
        str(tmp_path),
        show_progress=False,
        trust_index=True,
        index_attestation=index_attestation,
        data_file_attestations=data_attestations,
    ) as dataframe:
        runtime = dataframe._runtime
        assert tuple(dataframe._paths) == tuple(
            item.canonical_path for item in data_attestations
        )
        assert runtime.data_manifest_identity_sha256 == (
            identity_module.data_manifest_identity(
                data_attestations,
                require_sorted=True,
            )
        )
        assert tuple(runtime.file_generation_tokens) == (0, 1)
        assert runtime.prepared._mapped_index_identity == int.from_bytes(
            bytes.fromhex(index_attestation.index_identity_sha256)[:8],
            byteorder="little",
        )
        np.testing.assert_array_equal(
            dataframe[0][:],
            np.array([0.0, 1.0, 10.0, 11.0]),
        )


def test_attested_dataframe_requires_explicit_trust_and_complete_pair(tmp_path):
    path = tmp_path / "part.tsfile"
    _write_runtime_file(path, 0)
    _, index_attestation, data_attestations = _build_runtime_attestations((path,))

    with pytest.raises(ValueError, match="trust_index=True"):
        TsFileDataFrame(
            str(tmp_path),
            show_progress=False,
            use_index=True,
            index_attestation=index_attestation,
            data_file_attestations=data_attestations,
        )
    with pytest.raises(ValueError, match="provided together"):
        TsFileDataFrame(
            str(tmp_path),
            show_progress=False,
            trust_index=True,
            index_attestation=index_attestation,
        )
    with pytest.raises(ValueError, match="provided together"):
        TsFileDataFrame(
            str(tmp_path),
            show_progress=False,
            trust_index=True,
            data_file_attestations=data_attestations,
        )


def test_attested_dataframe_rejects_incomplete_order_and_unbound_fingerprint(tmp_path):
    paths = [tmp_path / "part-0.tsfile", tmp_path / "part-1.tsfile"]
    _write_runtime_file(paths[0], 0)
    _write_runtime_file(paths[1], 10)
    _, index_attestation, data_attestations = _build_runtime_attestations(paths)

    with pytest.raises(ValueError, match="file_id=0..N-1 in order"):
        TsFileDataFrame(
            str(tmp_path),
            show_progress=False,
            trust_index=True,
            index_attestation=index_attestation,
            data_file_attestations=data_attestations[:1],
        )
    with pytest.raises(ValueError, match="file_id=0..N-1 in order"):
        TsFileDataFrame(
            str(tmp_path),
            show_progress=False,
            trust_index=True,
            index_attestation=index_attestation,
            data_file_attestations=tuple(reversed(data_attestations)),
        )

    first = data_attestations[0]
    unbound = identity_module.DataFileAttestation(
        file_id=first.file_id,
        canonical_path=first.canonical_path,
        st_dev=first.st_dev,
        st_ino=first.st_ino,
        st_size=first.st_size,
        st_mtime_ns=first.st_mtime_ns,
    )
    with pytest.raises(
        identity_module.AttestationMismatchError,
        match="fingerprint",
    ):
        TsFileDataFrame(
            str(tmp_path),
            show_progress=False,
            trust_index=True,
            index_attestation=index_attestation,
            data_file_attestations=(unbound, data_attestations[1]),
        )


def test_attested_index_mismatch_fails_before_mapping(tmp_path, monkeypatch):
    path = tmp_path / "part.tsfile"
    _write_runtime_file(path, 0)
    index_path, index_attestation, data_attestations = _build_runtime_attestations(
        (path,)
    )
    with open(index_path, "ab") as stream:
        stream.write(b"x")

    mapped_calls = 0

    def forbidden_mapping(*_args, **_kwargs):
        nonlocal mapped_calls
        mapped_calls += 1
        raise AssertionError("a mismatched index must not be mapped")

    monkeypatch.setattr(runtime_module, "MappedDatasetIndex", forbidden_mapping)
    with pytest.raises(
        identity_module.AttestationMismatchError,
        match="index stat",
    ):
        TsFileDataFrame(
            str(tmp_path),
            show_progress=False,
            trust_index=True,
            index_attestation=index_attestation,
            data_file_attestations=data_attestations,
        )
    assert mapped_calls == 0


def test_attested_data_mismatch_fails_before_native_reader_open(tmp_path, monkeypatch):
    path = tmp_path / "part.tsfile"
    _write_runtime_file(path, 0)
    _, index_attestation, data_attestations = _build_runtime_attestations((path,))

    replacement = tmp_path / "replacement.tsfile"
    replacement.write_bytes(b"X" * path.stat().st_size)
    os.replace(replacement, path)

    native_open_calls = 0

    def forbidden_reader(*_args, **_kwargs):
        nonlocal native_open_calls
        native_open_calls += 1
        raise AssertionError("a mismatched data file must not reach the native reader")

    monkeypatch.setattr(runtime_module, "TsFileReaderPy", forbidden_reader)
    with TsFileDataFrame(
        str(tmp_path),
        show_progress=False,
        trust_index=True,
        index_attestation=index_attestation,
        data_file_attestations=data_attestations,
    ) as dataframe:
        with pytest.raises(
            identity_module.AttestationMismatchError,
            match="data-file stat",
        ):
            dataframe[0][:]
    assert native_open_calls == 0
