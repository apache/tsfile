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

import pandas as pd
import pytest

from tsfile import ColumnCategory, ColumnSchema, TableSchema, TsFileTableWriter
from tsfile import TsFileDataFrame
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
    assert RECORDS[TSFILE_RECORD].size == 48
    assert RECORDS[DEVICE_FILE_SPAN].size == 48
    assert RECORDS[SERIES_FILE_SPAN].size == 48
    assert RECORDS[SERIES_LOCATOR].size == 24


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


def test_hot_construction_maps_index_without_opening_readers(tmp_path, monkeypatch):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    with TsFileDataFrame(str(source), show_progress=False) as first:
        assert len(first) == 1

    def fail_legacy_scan(*_args, **_kwargs):
        raise AssertionError("hot construction must not build a legacy catalog")

    monkeypatch.setattr("tsfile.dataset.reader.TsFileSeriesReader", fail_legacy_scan)
    with TsFileDataFrame(str(source), show_progress=False) as second:
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


def test_reader_pool_enforces_open_file_cap(tmp_path, monkeypatch):
    first = tmp_path / "part1.tsfile"
    second = tmp_path / "part2.tsfile"
    _write_runtime_file(first, 0)
    _write_runtime_file(second, 2)
    monkeypatch.setenv("TSFILE_DATAFRAME_MAX_OPEN_FILES", "1")
    with TsFileDataFrame([str(first), str(second)], show_progress=False) as dataframe:
        series = dataframe[0]
        assert list(series[:]) == [0.0, 1.0, 2.0, 3.0]
        assert dataframe._runtime.readers.open_count == 1
        assert dataframe._runtime.prepared.size == 2
        series.close()


def test_prepared_locator_rejects_stale_generation_and_bad_range(tmp_path):
    source = tmp_path / "part.tsfile"
    _write_runtime_file(source, 0)
    with TsFileDataFrame(str(source), show_progress=False) as dataframe:
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
