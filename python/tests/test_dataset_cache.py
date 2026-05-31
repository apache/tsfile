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
    cache_path,
    catalog_from_dict,
    catalog_to_dict,
    read_sidecar,
    write_sidecar,
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

    data = catalog_to_dict(catalog)
    rebuilt = catalog_from_dict(data)

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


def test_cache_auto_hit_skips_native_metadata(tmp_path, monkeypatch):
    path = tmp_path / "cache_hit.tsfile"
    _write_weather_file(path)

    # First load: cold cache — populates the sidecar.
    df_cold = TsFileDataFrame(str(path), show_progress=False, cache="auto")
    df_cold.close()
    assert os.path.exists(cache_path(str(path)))

    # Spy on the heavy native metadata fetch. A cache hit must not call it.
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
        # The cached dataframe is functionally equivalent.
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
    assert read_sidecar(str(path)) is not None

    # Bump mtime so the cached fingerprint no longer matches.
    stat = os.stat(str(path))
    os.utime(str(path), ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000))
    assert read_sidecar(str(path)) is None


def test_cache_invalidated_by_size_change(tmp_path):
    path = tmp_path / "size.tsfile"
    _write_weather_file(path)

    TsFileDataFrame(str(path), show_progress=False, cache="auto").close()
    assert read_sidecar(str(path)) is not None

    # Pretend the sidecar was written for a different file size, preserving mtime.
    stat_before = os.stat(str(path))
    import json

    cp = cache_path(str(path))
    with open(cp, "r") as fh:
        payload = json.load(fh)
    payload["file"]["size"] = stat_before.st_size + 1
    with open(cp, "w") as fh:
        json.dump(payload, fh)

    assert read_sidecar(str(path)) is None


def test_cache_off_does_not_write(tmp_path):
    path = tmp_path / "cache_off.tsfile"
    _write_weather_file(path)

    df = TsFileDataFrame(str(path), show_progress=False, cache="off")
    df.close()

    assert not os.path.exists(cache_path(str(path)))


def test_cache_rebuild_overwrites_stale(tmp_path):
    path = tmp_path / "rebuild.tsfile"
    _write_weather_file(path)

    # Seed a corrupt sidecar to prove rebuild does not read it.
    with open(cache_path(str(path)), "w") as fh:
        fh.write("not json")

    df = TsFileDataFrame(str(path), show_progress=False, cache="rebuild")
    try:
        # After rebuild the sidecar must be valid again.
        rebuilt = read_sidecar(str(path))
        assert rebuilt is not None
        # And functionally the dataframe works.
        assert df.list_timeseries()
    finally:
        df.close()


def test_invalid_cache_mode_rejected(tmp_path):
    path = tmp_path / "bad_mode.tsfile"
    _write_weather_file(path)
    with pytest.raises(ValueError):
        TsFileDataFrame(str(path), show_progress=False, cache="bogus")


def test_multi_shard_cache(tmp_path):
    paths = [tmp_path / f"shard_{i}.tsfile" for i in range(3)]
    for i, p in enumerate(paths):
        _write_weather_file(p, start=i * 100)

    df = TsFileDataFrame([str(p) for p in paths], show_progress=False, cache="auto")
    df.close()
    for p in paths:
        assert os.path.exists(cache_path(str(p)))

    df_hot = TsFileDataFrame([str(p) for p in paths], show_progress=False, cache="auto")
    try:
        assert len(df_hot.list_timeseries()) == 2
    finally:
        df_hot.close()
