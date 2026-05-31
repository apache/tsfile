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

import json
import os

import pandas as pd
import pytest

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TSDataType,
    TableSchema,
    TsFileDataFrame,
    TsFileTableWriter,
)
from tsfile.dataset import reader as reader_module
from tsfile.dataset.cache import (
    catalog_from_dict,
    catalog_to_dict,
    extract_catalog,
    manifest_path,
    read_manifest,
)


def _write_weather_file(path, start=0):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("humidity", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    df = pd.DataFrame(
        {
            "time": [start, start + 1, start + 2],
            "device": ["device_a", "device_a", "device_a"],
            "temperature": [20.0, 21.5, 23.0],
            "humidity": [50.0, 52.0, 55.0],
        }
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(df)


def test_catalog_round_trip(tmp_path):
    path = tmp_path / "round_trip.tsfile"
    _write_weather_file(path)
    df = TsFileDataFrame(str(path), show_progress=False, cache="off")
    reader = next(iter(df._readers.values()))
    catalog = reader.catalog

    rebuilt = catalog_from_dict(catalog_to_dict(catalog))

    assert [t.table_name for t in rebuilt.table_entries] == [
        t.table_name for t in catalog.table_entries
    ]
    assert [t.field_columns for t in rebuilt.table_entries] == [
        t.field_columns for t in catalog.table_entries
    ]
    assert [t.tag_columns for t in rebuilt.table_entries] == [
        t.tag_columns for t in catalog.table_entries
    ]
    assert [t.tag_types for t in rebuilt.table_entries] == [
        t.tag_types for t in catalog.table_entries
    ]
    assert [
        (d.table_id, d.tag_values, d.min_time, d.max_time)
        for d in rebuilt.device_entries
    ] == [
        (d.table_id, d.tag_values, d.min_time, d.max_time)
        for d in catalog.device_entries
    ]
    assert rebuilt.series_stats_by_ref == catalog.series_stats_by_ref
    df.close()


def test_manifest_path_uses_common_parent(tmp_path):
    paths = [str(tmp_path / f"shard_{i}.tsfile") for i in range(3)]
    assert manifest_path(paths) == str(tmp_path / "tsfile_dataset.metacache.json")


def test_manifest_path_falls_back_for_single_file(tmp_path):
    path = tmp_path / "single.tsfile"
    _write_weather_file(path)
    expected = str(tmp_path / "tsfile_dataset.metacache.json")
    assert manifest_path([str(path)]) == expected


def test_cache_auto_hit_skips_native_metadata(tmp_path, monkeypatch):
    path = tmp_path / "cache_hit.tsfile"
    _write_weather_file(path)

    TsFileDataFrame(str(path), show_progress=False, cache="auto").close()
    assert os.path.exists(manifest_path([str(path)]))

    calls = {"count": 0}
    original = reader_module.TsFileSeriesReader._cache_metadata_table_model

    def spy(self):
        calls["count"] += 1
        return original(self)

    monkeypatch.setattr(
        reader_module.TsFileSeriesReader, "_cache_metadata_table_model", spy
    )

    df_hot = TsFileDataFrame(str(path), show_progress=False, cache="auto")
    try:
        assert calls["count"] == 0
        assert df_hot.list_timeseries() == [
            "weather.device_a.temperature",
            "weather.device_a.humidity",
        ]
    finally:
        df_hot.close()


def test_cache_invalidated_by_mtime_change(tmp_path):
    path = tmp_path / "mtime.tsfile"
    _write_weather_file(path)

    TsFileDataFrame(str(path), show_progress=False, cache="auto").close()
    shards = read_manifest(manifest_path([str(path)]))
    assert extract_catalog(shards, str(path)) is not None

    stat = os.stat(str(path))
    os.utime(str(path), ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
    assert extract_catalog(shards, str(path)) is None


def test_cache_invalidated_by_size_change(tmp_path):
    path = tmp_path / "size.tsfile"
    _write_weather_file(path)

    TsFileDataFrame(str(path), show_progress=False, cache="auto").close()
    mp = manifest_path([str(path)])
    with open(mp, "r") as fh:
        payload = json.load(fh)
    payload["shards"][str(path)]["size"] += 1
    with open(mp, "w") as fh:
        json.dump(payload, fh)

    assert extract_catalog(read_manifest(mp), str(path)) is None


def test_cache_off_does_not_write(tmp_path):
    path = tmp_path / "cache_off.tsfile"
    _write_weather_file(path)
    df = TsFileDataFrame(str(path), show_progress=False, cache="off")
    df.close()
    assert not os.path.exists(manifest_path([str(path)]))


def test_cache_rebuild_overwrites_stale(tmp_path):
    path = tmp_path / "rebuild.tsfile"
    _write_weather_file(path)
    mp = manifest_path([str(path)])
    with open(mp, "w") as fh:
        fh.write("not json")

    df = TsFileDataFrame(str(path), show_progress=False, cache="rebuild")
    try:
        shards = read_manifest(mp)
        assert shards is not None
        assert extract_catalog(shards, str(path)) is not None
        assert df.list_timeseries()
    finally:
        df.close()


def test_invalid_cache_mode_rejected(tmp_path):
    path = tmp_path / "bad_mode.tsfile"
    _write_weather_file(path)
    with pytest.raises(ValueError):
        TsFileDataFrame(str(path), show_progress=False, cache="bogus")


def test_invalid_max_open_files_rejected(tmp_path):
    path = tmp_path / "bad_cap.tsfile"
    _write_weather_file(path)
    with pytest.raises(ValueError):
        TsFileDataFrame(str(path), show_progress=False, max_open_files=0)


def test_single_manifest_for_multiple_shards(tmp_path):
    paths = [tmp_path / f"shard_{i}.tsfile" for i in range(3)]
    for i, p in enumerate(paths):
        _write_weather_file(p, start=i * 100)
    str_paths = [str(p) for p in paths]
    mp = manifest_path(str_paths)

    df = TsFileDataFrame(str_paths, show_progress=False, cache="auto")
    df.close()
    assert os.path.exists(mp)
    shards = read_manifest(mp)
    assert set(shards.keys()) == set(str_paths)
    for p in paths:
        assert not os.path.exists(str(p) + ".metacache.json")


def test_adding_shard_keeps_existing_entries(tmp_path):
    initial = [tmp_path / "shard_0.tsfile", tmp_path / "shard_1.tsfile"]
    for i, p in enumerate(initial):
        _write_weather_file(p, start=i * 100)
    initial_paths = [str(p) for p in initial]
    TsFileDataFrame(initial_paths, show_progress=False, cache="auto").close()

    new_shard = tmp_path / "shard_2.tsfile"
    _write_weather_file(new_shard, start=200)
    expanded_paths = initial_paths + [str(new_shard)]
    TsFileDataFrame(expanded_paths, show_progress=False, cache="auto").close()

    shards = read_manifest(manifest_path(expanded_paths))
    assert set(shards.keys()) == set(expanded_paths)


def test_manifest_skipped_on_full_hit(tmp_path):
    path = tmp_path / "no_rewrite.tsfile"
    _write_weather_file(path)

    TsFileDataFrame(str(path), show_progress=False, cache="auto").close()
    mp = manifest_path([str(path)])
    before = os.stat(mp).st_mtime_ns

    TsFileDataFrame(str(path), show_progress=False, cache="auto").close()
    after = os.stat(mp).st_mtime_ns
    assert before == after


def test_lru_evicts_older_reader_when_over_cap(tmp_path):
    paths = [tmp_path / f"lru_{i}.tsfile" for i in range(3)]
    for i, p in enumerate(paths):
        _write_weather_file(p, start=i * 100)
    str_paths = [str(p) for p in paths]

    df = TsFileDataFrame(str_paths, show_progress=False, cache="auto", max_open_files=2)
    try:
        # After eager init with cap=2, the LRU shard's native handle is closed
        # but its Python wrapper + catalog stay valid.
        first = df._readers[str_paths[0]]
        second = df._readers[str_paths[1]]
        third = df._readers[str_paths[2]]

        assert first._reader is None
        assert second._reader is not None
        assert third._reader is not None
        assert df._fd_pool.capacity == 2
        assert len(df._fd_pool) == 2

        # Touching the evicted reader reopens its native handle and pushes the
        # current LRU (`second`) out.
        first._ensure_open()

        assert first._reader is not None
        assert third._reader is not None
        assert second._reader is None
        assert len(df._fd_pool) == 2
    finally:
        df.close()


def test_lru_disabled_when_cap_exceeds_shard_count(tmp_path):
    paths = [tmp_path / f"all_open_{i}.tsfile" for i in range(3)]
    for i, p in enumerate(paths):
        _write_weather_file(p, start=i * 100)

    df = TsFileDataFrame(
        [str(p) for p in paths],
        show_progress=False,
        cache="auto",
        max_open_files=16,
    )
    try:
        for p in paths:
            assert df._readers[str(p)]._reader is not None
        assert len(df._fd_pool) == 3
    finally:
        df.close()


def test_lru_close_all_drains_pool(tmp_path):
    path = tmp_path / "drain.tsfile"
    _write_weather_file(path)

    df = TsFileDataFrame(str(path), show_progress=False, cache="auto")
    reader = next(iter(df._readers.values()))
    assert reader._reader is not None
    df.close()
    assert reader._reader is None
