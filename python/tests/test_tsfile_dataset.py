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

import numpy as np
import pandas as pd
import pytest

from tsfile import ColumnCategory, ColumnSchema, TSDataType, TableSchema, TsFileTableWriter
from tsfile import AlignedTimeseries, Timeseries, TsFileDataFrame
from tsfile.dataset.formatting import format_timestamp
from tsfile.dataset.reader import TsFileSeriesReader


def _write_weather_file(path, start):
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


def _write_numeric_and_text_file(path):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("status", TSDataType.STRING, ColumnCategory.FIELD),
        ],
    )
    df = pd.DataFrame(
        {
            "time": [0, 1, 2],
            "device": ["device_a", "device_a", "device_a"],
            "temperature": [20.0, np.nan, 23.5],
            "status": ["ok", "warn", "ok"],
        }
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(df)


def _write_partial_numeric_rows_file(path):
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
            "time": [0, 1],
            "device": ["device_a", "device_a"],
            "temperature": [np.nan, 21.0],
            "humidity": [50.0, 51.0],
        }
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(df)


def _write_weather_with_extra_field_file(path, start):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("humidity", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("pressure", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    df = pd.DataFrame(
        {
            "time": [start, start + 1],
            "device": ["device_a", "device_a"],
            "temperature": [20.0, 21.0],
            "humidity": [50.0, 51.0],
            "pressure": [1000.0, 1001.0],
        }
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(df)


def _write_multi_tag_file(path):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("city", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("humidity", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("status", TSDataType.STRING, ColumnCategory.FIELD),
        ],
    )
    df = pd.DataFrame(
        {
            "time": [0, 1, 0, 1],
            "city": ["beijing", "beijing", "shanghai", "shanghai"],
            "device": ["device_a", "device_a", "device_b", "device_b"],
            "temperature": [20.0, 21.0, 24.0, 25.0],
            "humidity": [50.0, 51.0, 60.0, 61.0],
            "status": ["ok", "ok", "warn", "warn"],
        }
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(df)


def _write_special_tag_file(path):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("city", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    df = pd.DataFrame(
        {
            "time": [0, 1],
            "city": ["bei.jing", "bei.jing"],
            "device": [r"dev\1", r"dev\1"],
            "temperature": [20.0, 21.0],
        }
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(df)


def test_dataset_top_level_imports():
    assert TsFileDataFrame.__module__ == "tsfile.dataset.dataframe"
    assert Timeseries.__module__ == "tsfile.dataset.timeseries"
    assert AlignedTimeseries.__module__ == "tsfile.dataset.timeseries"


def test_format_timestamp_preserves_millisecond_precision():
    assert "." not in format_timestamp(1000)
    assert format_timestamp(1).endswith(".001")


def test_dataset_basic_access_patterns(tmp_path, capsys):
    path1 = tmp_path / "part1.tsfile"
    path2 = tmp_path / "part2.tsfile"
    _write_weather_file(path1, 0)
    _write_weather_file(path2, 3)

    with TsFileDataFrame([str(path1), str(path2)], show_progress=False) as tsdf:
        assert len(tsdf) == 2

        first = tsdf[0]
        assert isinstance(first, Timeseries)
        assert first.name in tsdf.list_timeseries()
        assert len(first) == 6
        assert first[0] == 20.0
        assert first[-1] == 23.0
        assert "Timeseries(" in repr(first)

        by_name = tsdf[first.name]
        assert isinstance(by_name, Timeseries)
        assert by_name.name == first.name

        subset = tsdf[:1]
        assert isinstance(subset, TsFileDataFrame)
        assert len(subset) == 1

        selected = tsdf[[0, 1]]
        assert isinstance(selected, TsFileDataFrame)
        assert len(selected) == 2

        aligned = tsdf.loc[0:5, [0, 1]]
        assert isinstance(aligned, AlignedTimeseries)
        assert aligned.shape == (6, 2)

        aligned_negative = tsdf.loc[0:5, [-1]]
        assert isinstance(aligned_negative, AlignedTimeseries)
        assert aligned_negative.shape == (6, 1)

        assert list(tsdf["field"]) == ["temperature", "humidity"]

        assert "TsFileDataFrame(2 time series, 2 files)" in repr(tsdf)
        aligned.show(2)
        assert "AlignedTimeseries(6 rows, 2 series)" in capsys.readouterr().out


def test_dataset_exposes_only_numeric_fields_and_keeps_nan(tmp_path):
    path = tmp_path / "numeric_and_text.tsfile"
    _write_numeric_and_text_file(path)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        assert tsdf.list_timeseries() == ["weather.device_a.temperature"]

        series = tsdf[0]
        assert series.name == "weather.device_a.temperature"
        assert np.isnan(series[1])
        sliced = series[:3]
        assert sliced.shape == (3,)
        assert np.isnan(sliced[1])
        assert series[1:1].shape == (0,)


def test_dataset_timeseries_supports_negative_step_slices(tmp_path):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        series = tsdf[0]
        np.testing.assert_array_equal(series[::-1], np.array([23.0, 21.5, 20.0]))
        np.testing.assert_array_equal(series[::-2], np.array([23.0, 20.0]))


def test_dataset_metadata_discovery_uses_all_numeric_fields(tmp_path):
    path = tmp_path / "partial_numeric_rows.tsfile"
    _write_partial_numeric_rows_file(path)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        assert tsdf.list_timeseries() == [
            "weather.device_a.temperature",
            "weather.device_a.humidity",
        ]

        assert list(tsdf["count"]) == [2, 2]
        assert list(tsdf["start_time"]) == [0, 0]
        assert list(tsdf["end_time"]) == [1, 1]


def test_dataset_rejects_duplicate_timestamps_across_shards(tmp_path):
    path1 = tmp_path / "part1.tsfile"
    path2 = tmp_path / "part2.tsfile"
    _write_weather_file(path1, 0)
    _write_weather_file(path2, 2)

    with pytest.raises(ValueError, match="Duplicate timestamp"):
        TsFileDataFrame([str(path1), str(path2)], show_progress=False)


def test_dataset_rejects_data_access_after_close(tmp_path):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    tsdf = TsFileDataFrame(str(path), show_progress=False)
    series = tsdf[0]
    tsdf.close()

    with pytest.raises(RuntimeError, match="TsFileDataFrame is closed"):
        _ = tsdf[0]

    with pytest.raises(RuntimeError, match="TsFileDataFrame is closed"):
        _ = series[0]


def test_subset_close_warns_and_does_not_close_root(tmp_path):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        subset = tsdf[:1]
        with pytest.warns(RuntimeWarning, match="no-op"):
            subset.close()

        series = tsdf[0]
        assert series[0] == 20.0


def test_dataset_rejects_incompatible_table_schemas_across_shards(tmp_path):
    path1 = tmp_path / "part1.tsfile"
    path2 = tmp_path / "part2.tsfile"
    _write_weather_file(path1, 0)
    _write_weather_with_extra_field_file(path2, 2)

    with pytest.raises(ValueError, match="Incompatible schema for table 'weather'"):
        TsFileDataFrame([str(path1), str(path2)], show_progress=False)


def test_dataset_multi_tag_metadata_discovery(tmp_path):
    path = tmp_path / "multi_tag.tsfile"
    _write_multi_tag_file(path)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        assert tsdf.list_timeseries() == [
            "weather.beijing.device_a.temperature",
            "weather.beijing.device_a.humidity",
            "weather.shanghai.device_b.temperature",
            "weather.shanghai.device_b.humidity",
        ]

        summary = pd.DataFrame(
            {
                "series_path": tsdf.list_timeseries(),
                "table": tsdf["table"],
                "city": tsdf["city"],
                "device": tsdf["device"],
                "field": tsdf["field"],
                "start_time": tsdf["start_time"],
                "end_time": tsdf["end_time"],
                "count": tsdf["count"],
            }
        ).sort_values(["city", "device", "field"]).reset_index(drop=True)
        assert list(summary.columns) == [
            "series_path",
            "table",
            "city",
            "device",
            "field",
            "start_time",
            "end_time",
            "count",
        ]
        assert list(summary["city"]) == ["beijing", "beijing", "shanghai", "shanghai"]
        assert list(summary["device"]) == ["device_a", "device_a", "device_b", "device_b"]
        assert list(summary["field"]) == ["humidity", "temperature", "humidity", "temperature"]
        assert list(summary["count"]) == [2, 2, 2, 2]


def test_dataset_series_paths_escape_special_tag_values(tmp_path):
    path = tmp_path / "special_tag.tsfile"
    _write_special_tag_file(path)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        expected_path = r"weather.bei\.jing.dev\\1.temperature"
        assert tsdf.list_timeseries() == [expected_path]

        series = tsdf[expected_path]
        assert isinstance(series, Timeseries)
        assert series.name == expected_path
        assert list(tsdf["city"]) == ["bei.jing"]
        assert list(tsdf["device"]) == [r"dev\1"]


def test_reader_series_paths_escape_special_tag_values(tmp_path):
    path = tmp_path / "special_tag.tsfile"
    _write_special_tag_file(path)

    reader = TsFileSeriesReader(str(path), show_progress=False)
    try:
        expected_path = r"weather.bei\.jing.dev\\1.temperature"
        assert reader.series_paths == [expected_path]
        info = reader.get_series_info(expected_path)
        assert info["tag_values"] == {"city": "bei.jing", "device": r"dev\1"}
    finally:
        reader.close()


def test_reader_catalog_shares_device_metadata_and_resolves_paths(tmp_path):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 100)

    reader = TsFileSeriesReader(str(path), show_progress=False)
    try:
        assert reader.series_paths == [
            "weather.device_a.temperature",
            "weather.device_a.humidity",
        ]
        assert len(reader.catalog.table_entries) == 1
        assert len(reader.catalog.device_entries) == 1
        assert reader.catalog.series_count == 2

        by_path = reader.get_series_info("weather.device_a.temperature")
        by_ref = reader.get_series_info_by_ref(0, 0)
        assert by_ref == by_path
        assert by_ref["tag_values"] == {"device": "device_a"}

        ts_by_path = reader.get_series_timestamps("weather.device_a.temperature")
        ts_by_device = reader.get_device_timestamps(0)
        np.testing.assert_array_equal(ts_by_path, ts_by_device)
    finally:
        reader.close()
