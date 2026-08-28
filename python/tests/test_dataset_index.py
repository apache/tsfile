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
            prepared = runtime.prepared.get(0, span[2], reader)
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
