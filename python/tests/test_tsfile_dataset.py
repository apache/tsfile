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

from tsfile.dataset import dataframe as dataframe_module
from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TSDataType,
    TableSchema,
    TsFileTableWriter,
)
from tsfile import AlignedTimeseries, Timeseries, TsFileDataFrame
from tsfile.dataset.formatting import format_timestamp
from tsfile.dataset.metadata import (
    MetadataCatalog,
    build_series_path,
    resolve_series_path,
)
from tsfile.dataset.reader import TsFileSeriesReader, _build_exact_tag_filter


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


def _write_weather_rows_file(path, rows):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("humidity", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    df = pd.DataFrame(rows)
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(df)


def _write_empty_weather_file(path):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("humidity", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema):
        pass


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


def test_dataset_loc_aligns_timestamp_union_and_preserves_requested_order(tmp_path):
    path = tmp_path / "weather_sparse.tsfile"
    _write_weather_rows_file(
        path,
        {
            "time": [0, 1, 2],
            "device": ["device_a", "device_a", "device_a"],
            "temperature": [10.0, np.nan, 30.0],
            "humidity": [np.nan, 200.0, 300.0],
        },
    )

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        aligned = tsdf.loc[
            0:2,
            [
                "weather.device_a.humidity",
                "weather.device_a.temperature",
            ],
        ]

        assert isinstance(aligned, AlignedTimeseries)
        assert aligned.series_names == [
            "weather.device_a.humidity",
            "weather.device_a.temperature",
        ]
        np.testing.assert_array_equal(
            aligned.timestamps, np.array([0, 1, 2], dtype=np.int64)
        )
        assert aligned.shape == (3, 2)
        assert np.isnan(aligned.values[0, 0])
        assert aligned.values[0, 1] == 10.0
        assert aligned.values[1, 0] == 200.0
        assert np.isnan(aligned.values[1, 1])
        assert aligned.values[2, 0] == 300.0
        assert aligned.values[2, 1] == 30.0


def test_dataset_loc_supports_single_timestamp_and_mixed_series_specifiers(tmp_path):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        aligned = tsdf.loc[1, [0, "weather.device_a.humidity"]]

        assert isinstance(aligned, AlignedTimeseries)
        assert aligned.series_names == [
            "weather.device_a.temperature",
            "weather.device_a.humidity",
        ]
        np.testing.assert_array_equal(aligned.timestamps, np.array([1], dtype=np.int64))
        np.testing.assert_array_equal(aligned.values, np.array([[21.5, 52.0]]))


def test_dataset_loc_supports_open_ended_ranges_and_negative_series_index(tmp_path):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 100)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        aligned = tsdf.loc[:101, [-1]]

        assert isinstance(aligned, AlignedTimeseries)
        assert aligned.series_names == ["weather.device_a.humidity"]
        np.testing.assert_array_equal(
            aligned.timestamps, np.array([100, 101], dtype=np.int64)
        )
        np.testing.assert_array_equal(aligned.values, np.array([[50.0], [52.0]]))


def test_dataset_loc_with_nulls_does_not_expand_beyond_requested_time_range(tmp_path):
    path = tmp_path / "weather_sparse_range.tsfile"
    _write_weather_rows_file(
        path,
        {
            "time": [0, 1, 2, 3],
            "device": ["device_a", "device_a", "device_a", "device_a"],
            "temperature": [10.0, np.nan, np.nan, 40.0],
            "humidity": [np.nan, 20.0, np.nan, 50.0],
        },
    )

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        aligned = tsdf.loc[
            1:2,
            [
                "weather.device_a.temperature",
                "weather.device_a.humidity",
            ],
        ]

        assert isinstance(aligned, AlignedTimeseries)
        np.testing.assert_array_equal(
            aligned.timestamps, np.array([1, 2], dtype=np.int64)
        )
        assert aligned.shape == (2, 2)
        assert np.isnan(aligned.values[0, 0])
        assert aligned.values[0, 1] == 20.0
        assert np.isnan(aligned.values[1, 0])
        assert np.isnan(aligned.values[1, 1])


def test_dataset_loc_single_timestamp_with_nulls_keeps_exact_time_window(tmp_path):
    path = tmp_path / "weather_sparse_point.tsfile"
    _write_weather_rows_file(
        path,
        {
            "time": [0, 1, 2],
            "device": ["device_a", "device_a", "device_a"],
            "temperature": [10.0, np.nan, 30.0],
            "humidity": [np.nan, 20.0, 40.0],
        },
    )

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        aligned = tsdf.loc[
            1,
            [
                "weather.device_a.temperature",
                "weather.device_a.humidity",
            ],
        ]

        assert isinstance(aligned, AlignedTimeseries)
        np.testing.assert_array_equal(aligned.timestamps, np.array([1], dtype=np.int64))
        assert aligned.shape == (1, 2)
        assert np.isnan(aligned.values[0, 0])
        assert aligned.values[0, 1] == 20.0


def test_dataset_repr_only_builds_preview_rows(tmp_path, monkeypatch):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        tsdf._index.series_refs_ordered = [(0, 0)] * 1000

        built_rows = []

        def fake_build_series_info(series_ref):
            built_rows.append(series_ref)
            return {
                "table_name": "weather",
                "field": "temperature",
                "tag_columns": ("device",),
                "tag_values": {"device": "device_a"},
                "min_time": 0,
                "max_time": 2,
                "count": 3,
            }

        def fail_build_series_name(_series_ref):
            raise AssertionError(
                "__repr__ should not build full series names for preview output"
            )

        monkeypatch.setattr(tsdf, "_build_series_info", fake_build_series_info)
        monkeypatch.setattr(tsdf, "_build_series_name", fail_build_series_name)

        rendered = repr(tsdf)
        assert "TsFileDataFrame(1000 time series, 1 files)" in rendered
        assert "..." in rendered
        assert len(built_rows) == 20


def test_dataset_exposes_only_numeric_fields_and_keeps_nan(tmp_path):
    path = tmp_path / "numeric_and_text.tsfile"
    _write_numeric_and_text_file(path)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        assert tsdf.list_timeseries() == ["weather.device_a.temperature"]

        series = tsdf[0]
        assert series.name == "weather.device_a.temperature"
        assert len(series) == 3
        assert series.stats == {"start_time": 0, "end_time": 2, "count": 3}
        assert np.isnan(series[1])
        np.testing.assert_array_equal(
            series.timestamps, np.array([0, 1, 2], dtype=np.int64)
        )
        sliced = series[:]
        assert sliced.shape == (3,)
        assert np.isnan(sliced[1])
        assert sliced[2] == 23.5
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

    with TsFileDataFrame([str(path1), str(path2)], show_progress=False) as tsdf:
        series = tsdf["weather.device_a.temperature"]
        with pytest.raises(ValueError, match="Duplicate timestamp"):
            _ = series.timestamps


def test_dataset_overlap_position_access_avoids_full_timestamp_materialization(
    tmp_path, monkeypatch
):
    path1 = tmp_path / "part1.tsfile"
    path2 = tmp_path / "part2.tsfile"
    _write_weather_rows_file(
        path1,
        {
            "time": [0, 2, 4],
            "device": ["device_a", "device_a", "device_a"],
            "temperature": [10.0, 30.0, 50.0],
            "humidity": [100.0, 300.0, 500.0],
        },
    )
    _write_weather_rows_file(
        path2,
        {
            "time": [1, 3, 5],
            "device": ["device_a", "device_a", "device_a"],
            "temperature": [20.0, 40.0, 60.0],
            "humidity": [200.0, 400.0, 600.0],
        },
    )

    def fail_merge(*_args, **_kwargs):
        raise AssertionError(
            "full timestamp merge should not run for overlap position reads"
        )

    monkeypatch.setattr(dataframe_module, "_merge_field_timestamps", fail_merge)

    with TsFileDataFrame([str(path1), str(path2)], show_progress=False) as tsdf:
        series = tsdf["weather.device_a.temperature"]
        assert series[0] == 10.0
        assert series[1] == 20.0
        assert series[4] == 50.0
        np.testing.assert_array_equal(series[1:5], np.array([20.0, 30.0, 40.0, 50.0]))


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


def test_dataset_skips_empty_tsfile_shards(tmp_path):
    empty_path = tmp_path / "empty.tsfile"
    data_path = tmp_path / "part.tsfile"
    _write_empty_weather_file(empty_path)
    _write_weather_file(data_path, 0)

    with TsFileDataFrame(
        [str(empty_path), str(data_path)], show_progress=False
    ) as tsdf:
        assert tsdf.list_timeseries() == [
            "weather.device_a.temperature",
            "weather.device_a.humidity",
        ]


def test_reader_allows_empty_tsfile(tmp_path):
    path = tmp_path / "empty.tsfile"
    _write_empty_weather_file(path)

    reader = TsFileSeriesReader(str(path), show_progress=False)
    try:
        assert reader.series_paths == []
        assert reader.catalog.series_count == 0
    finally:
        reader.close()


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

        summary = (
            pd.DataFrame(
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
            )
            .sort_values(["city", "device", "field"])
            .reset_index(drop=True)
        )
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
        assert list(summary["device"]) == [
            "device_a",
            "device_a",
            "device_b",
            "device_b",
        ]
        assert list(summary["field"]) == [
            "humidity",
            "temperature",
            "humidity",
            "temperature",
        ]
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
        ts_arr, values = reader.read_series_by_ref(0, 0, 100, 102)
        np.testing.assert_array_equal(ts_arr, np.array([100, 101, 102]))
        np.testing.assert_array_equal(values, np.array([20.0, 21.5, 23.0]))
    finally:
        reader.close()


def test_reader_read_series_by_row_retries_across_native_row_query_boundaries():
    class _FakeResultSet:
        def __init__(self, rows):
            self._rows = rows
            self._index = -1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def next(self):
            self._index += 1
            return self._index < len(self._rows)

        def get_value_by_name(self, name):
            return self._rows[self._index][name]

    class _FakeNativeReader:
        def __init__(self, timestamps, values, boundary):
            self._timestamps = timestamps
            self._values = values
            self._boundary = boundary

        def query_table_by_row(
            self, table_name, column_names, offset=0, limit=-1, tag_filter=None
        ):
            assert table_name == "pvf"
            assert column_names == ["totalcloudcover"]
            assert tag_filter is None
            if limit < 0:
                stop = len(self._timestamps)
            else:
                stop = min(offset + limit, len(self._timestamps))

            # Simulate the current native bug: one row query cannot cross the
            # next internal boundary, so callers must re-issue from the
            # advanced offset to complete a large logical window.
            chunk_stop = min(stop, ((offset // self._boundary) + 1) * self._boundary)
            rows = [
                {
                    "time": int(self._timestamps[idx]),
                    "totalcloudcover": float(self._values[idx]),
                }
                for idx in range(offset, chunk_stop)
            ]
            return _FakeResultSet(rows)

    reader = object.__new__(TsFileSeriesReader)
    reader._reader = _FakeNativeReader(
        np.arange(30, dtype=np.int64), np.arange(30, dtype=np.float64), boundary=10
    )
    reader._catalog = MetadataCatalog()
    table_id = reader._catalog.add_table("pvf", (), (), ("totalcloudcover",))
    device_id = reader._catalog.add_device(table_id, (), 0, 29)

    ts_arr, values = reader.read_series_by_row(device_id, 0, 5, 12)
    np.testing.assert_array_equal(ts_arr, np.arange(5, 17, dtype=np.int64))
    np.testing.assert_array_equal(values, np.arange(5, 17, dtype=np.float64))


def test_series_path_resolution_allows_prefix_tag_values():
    catalog = MetadataCatalog()
    table_id = catalog.add_table(
        "weather",
        ("site", "device", "region"),
        (TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
        ("temperature",),
    )
    device_id = catalog.add_device(table_id, ("site_a", "device_a"), 0, 1)
    catalog.series_stats_by_ref[(device_id, 0)] = {
        "length": 1,
        "min_time": 0,
        "max_time": 0,
        "timeline_length": 1,
        "timeline_min_time": 0,
        "timeline_max_time": 0,
    }

    series_path = build_series_path(catalog, device_id, 0)
    assert series_path == "weather.site_a.device_a.temperature"
    assert resolve_series_path(catalog, series_path) == (table_id, device_id, 0)


def test_series_path_resolution_allows_missing_trailing_tag_value():
    catalog = MetadataCatalog()
    table_id = catalog.add_table(
        "weather",
        ("device",),
        (TSDataType.STRING,),
        ("temperature",),
    )
    device_id = catalog.add_device(table_id, (), 0, 1)
    catalog.series_stats_by_ref[(device_id, 0)] = {
        "length": 1,
        "min_time": 0,
        "max_time": 0,
        "timeline_length": 1,
        "timeline_min_time": 0,
        "timeline_max_time": 0,
    }

    series_path = build_series_path(catalog, device_id, 0)
    assert series_path == "weather.temperature"
    assert resolve_series_path(catalog, series_path) == (table_id, device_id, 0)


def test_series_path_resolution_uses_named_tags_for_sparse_non_prefix_values():
    catalog = MetadataCatalog()
    table_id = catalog.add_table(
        "weather",
        ("city", "device", "region"),
        (TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
        ("temperature",),
    )
    device_id = catalog.add_device(table_id, (None, "device_a", None), 0, 1)
    catalog.series_stats_by_ref[(device_id, 0)] = {
        "length": 1,
        "min_time": 0,
        "max_time": 0,
        "timeline_length": 1,
        "timeline_min_time": 0,
        "timeline_max_time": 0,
    }

    series_path = build_series_path(catalog, device_id, 0)
    assert series_path == "weather.device_a.temperature"
    assert resolve_series_path(catalog, series_path) == (table_id, device_id, 0)


def test_reader_metadata_tag_values_trim_trailing_none():
    class _Group:
        segments = ("weather", "device_a", None, None)

    assert TsFileSeriesReader._metadata_tag_values(_Group(), 3) == ("device_a",)
    assert TsFileSeriesReader._metadata_tag_values(_Group(), 1) == ("device_a",)


def test_exact_tag_filter_rejects_none_tag_values():
    with pytest.raises(NotImplementedError, match="IS NULL / IS NOT NULL"):
        _build_exact_tag_filter({"device": None})
    with pytest.raises(NotImplementedError, match="IS NULL / IS NOT NULL"):
        _build_exact_tag_filter({"city": "beijing", "device": None})


def test_reader_exact_match_with_none_tag_values_fails_fast():
    class _FakeNativeReader:
        def query_table(self, *args, **kwargs):
            raise AssertionError(
                "query should not be issued when None-tag exact matching is unsupported"
            )

        def query_table_by_row(self, *args, **kwargs):
            raise AssertionError(
                "row query should not be issued when None-tag exact matching is unsupported"
            )

    reader = object.__new__(TsFileSeriesReader)
    reader._reader = _FakeNativeReader()
    reader._catalog = MetadataCatalog()
    table_id = reader._catalog.add_table(
        "weather",
        ("city", "device", "region"),
        (TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
        ("temperature",),
    )
    device_id = reader._catalog.add_device(table_id, (None, "device_a", "north"), 0, 1)
    reader._catalog.series_stats_by_ref[(device_id, 0)] = {
        "length": 2,
        "min_time": 0,
        "max_time": 1,
        "timeline_length": 2,
        "timeline_min_time": 0,
        "timeline_max_time": 1,
    }

    with pytest.raises(NotImplementedError, match="IS NULL / IS NOT NULL"):
        reader.read_series_by_ref(device_id, 0, 0, 1)
    with pytest.raises(NotImplementedError, match="IS NULL / IS NOT NULL"):
        reader.read_series_by_row(device_id, 0, 0, 2)


def test_dataframe_resolves_named_sparse_tag_series_path():
    tsdf = object.__new__(TsFileDataFrame)
    tsdf._index = dataframe_module._LogicalIndex()
    tsdf._index.table_entries["weather"] = dataframe_module.TableEntry(
        table_name="weather",
        tag_columns=("city", "device", "region"),
        tag_types=(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
        field_columns=("temperature",),
    )
    device_key = ("weather", (None, "device_a"))
    tsdf._index.device_order = [device_key]
    tsdf._index.device_index_by_key = {device_key: 0}
    tsdf._index.tables_with_sparse_tag_values = {"weather"}
    tsdf._index.sparse_device_indices_by_compressed_path = {
        ("weather", ("device_a",)): [0]
    }
    tsdf._index.device_refs = [[]]
    tsdf._index.series_refs_ordered = [(0, 0)]
    tsdf._index.series_ref_set = {(0, 0)}
    tsdf._index.series_ref_map = {(0, 0): []}

    assert tsdf.list_timeseries() == ["weather.device_a.temperature"]
    assert tsdf._resolve_series_name("weather.device_a.temperature") == (0, 0)


def test_dataframe_list_timeseries_filters_named_sparse_tag_prefix():
    tsdf = object.__new__(TsFileDataFrame)
    tsdf._index = dataframe_module._LogicalIndex()
    tsdf._index.table_entries["weather"] = dataframe_module.TableEntry(
        table_name="weather",
        tag_columns=("city", "device", "region"),
        tag_types=(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
        field_columns=("temperature",),
    )
    tsdf._index.device_order = [
        ("weather", (None, "device_a")),
        ("weather", ("beijing", "device_b")),
    ]
    tsdf._index.device_index_by_key = {
        ("weather", (None, "device_a")): 0,
        ("weather", ("beijing", "device_b")): 1,
    }
    tsdf._index.tables_with_sparse_tag_values = {"weather"}
    tsdf._index.sparse_device_indices_by_compressed_path = {
        ("weather", ("device_a",)): [0],
        ("weather", ("beijing", "device_b")): [1],
    }
    tsdf._index.device_refs = [[], []]
    tsdf._index.series_refs_ordered = [(0, 0), (1, 0)]
    tsdf._index.series_ref_set = {(0, 0), (1, 0)}
    tsdf._index.series_ref_map = {(0, 0): [], (1, 0): []}

    assert tsdf.list_timeseries("weather.device_a") == ["weather.device_a.temperature"]


def test_dataframe_list_timeseries_prefix_can_skip_full_name_build(
    tmp_path, monkeypatch
):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(str(path), show_progress=False) as tsdf:
        tsdf._index.series_refs_ordered = [(0, 0)] * 1000

        def fail_build_series_name(_series_ref):
            raise AssertionError(
                "list_timeseries(prefix) should not build full names for non-matching series"
            )

        monkeypatch.setattr(tsdf, "_build_series_name", fail_build_series_name)
        assert tsdf.list_timeseries("pvf") == []


def test_series_path_resolution_reports_ambiguous_sparse_path():
    catalog = MetadataCatalog()
    table_id = catalog.add_table(
        "weather",
        ("city", "device"),
        (TSDataType.STRING, TSDataType.STRING),
        ("temperature",),
    )
    first_id = catalog.add_device(table_id, ("beijing", None), 0, 1)
    second_id = catalog.add_device(table_id, (None, "beijing"), 0, 1)
    for device_id in (first_id, second_id):
        catalog.series_stats_by_ref[(device_id, 0)] = {
            "length": 1,
            "min_time": 0,
            "max_time": 0,
            "timeline_length": 1,
            "timeline_min_time": 0,
            "timeline_max_time": 0,
        }

    assert build_series_path(catalog, first_id, 0) == "weather.beijing.temperature"
    assert build_series_path(catalog, second_id, 0) == "weather.beijing.temperature"
    with pytest.raises(ValueError, match="Ambiguous series path"):
        resolve_series_path(catalog, "weather.beijing.temperature")


def test_reader_show_progress_reports_start_immediately(tmp_path, capsys):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    reader = TsFileSeriesReader(str(path), show_progress=True)
    try:
        stderr = capsys.readouterr().err
        assert "Reading TsFile metadata: 0/1" in stderr
        assert "Reading TsFile metadata: 1 table(s), 2 series ... done" in stderr
    finally:
        reader.close()


def test_dataframe_parallel_show_progress_reports_start_immediately(tmp_path, capsys):
    path1 = tmp_path / "part1.tsfile"
    path2 = tmp_path / "part2.tsfile"
    _write_weather_file(path1, 0)
    _write_weather_file(path2, 3)

    with TsFileDataFrame([str(path1), str(path2)], show_progress=True):
        pass

    stderr = capsys.readouterr().err
    assert "Loading TsFile shards: 0/2" in stderr
    assert "Loading TsFile shards: 2/2 (4 series) ... done" in stderr
