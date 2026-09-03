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
import threading

from tsfile.dataset import dataframe as dataframe_module
from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TSDataType,
    TableSchema,
    TsFileTableWriter,
)
from tsfile import AlignedTimeseries, SeriesPath, Timeseries, TsFileDataFrame
from tsfile.dataset.formatting import format_timestamp
from tsfile.dataset.metadata import (
    MetadataCatalog,
    SeriesStats,
    build_series_path,
    resolve_series_path,
)
from tsfile.dataset.reader import (
    MODEL_TABLE,
    TsFileSeriesReader,
    _build_exact_tag_filter,
)
from tsfile.dataset.runtime import RuntimeSeriesReader


@pytest.fixture(params=[False, True], ids=["no-index", "index"])
def dataframe_use_index(request):
    return request.param


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


def _write_weather_int_temperature_file(path, start):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("humidity", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(
            pd.DataFrame(
                {
                    "time": [start, start + 1],
                    "device": ["device_a", "device_a"],
                    "temperature": [20, 21],
                    "humidity": [50.0, 51.0],
                }
            )
        )


def _write_weather_boolean_status_file(path, start):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("status", TSDataType.BOOLEAN, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(
            pd.DataFrame(
                {
                    "time": [start, start + 1],
                    "device": ["device_a", "device_a"],
                    "temperature": [20.0, 21.0],
                    "status": [True, False],
                }
            )
        )


def _write_weather_timestamp_field_file(path, start):
    schema = TableSchema(
        "weather",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("observed_at", TSDataType.TIMESTAMP, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(
            pd.DataFrame(
                {
                    "time": [start, start + 1],
                    "device": ["device_a", "device_a"],
                    "observed_at": [1_700_000_000_000, 1_700_000_000_001],
                }
            )
        )


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


def test_dataset_basic_access_patterns(tmp_path, capsys, dataframe_use_index):
    path1 = tmp_path / "part1.tsfile"
    path2 = tmp_path / "part2.tsfile"
    _write_weather_file(path1, 0)
    _write_weather_file(path2, 3)

    with TsFileDataFrame(
        [str(path1), str(path2)],
        show_progress=False,
        use_index=dataframe_use_index,
    ) as tsdf:
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

        assert "TsFileDataFrame(table model, 2 time series, 2 files)" in repr(tsdf)
        aligned.show(2)
        assert "AlignedTimeseries(6 rows, 2 series)" in capsys.readouterr().out


def test_dataset_loc_aligns_timestamp_union_and_preserves_requested_order(
    tmp_path, dataframe_use_index
):
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

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
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


def test_dataset_loc_batches_aligned_fields_per_device_then_unions_devices(
    tmp_path, dataframe_use_index
):
    path = tmp_path / "weather_multi_device.tsfile"
    _write_weather_rows_file(
        path,
        {
            "time": [0, 1, 1, 2],
            "device": ["device_a", "device_a", "device_b", "device_b"],
            "temperature": [10.0, 11.0, 20.0, 21.0],
            "humidity": [100.0, 101.0, 200.0, 201.0],
        },
    )

    requested = [
        "weather.device_a.temperature",
        "weather.device_a.humidity",
        "weather.device_b.humidity",
    ]
    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        aligned = tsdf.loc[0:2, requested]

        assert aligned.series_names == requested
        np.testing.assert_array_equal(
            aligned.timestamps, np.array([0, 1, 2], dtype=np.int64)
        )
        np.testing.assert_allclose(
            aligned.values,
            np.array(
                [
                    [10.0, 100.0, np.nan],
                    [11.0, 101.0, 200.0],
                    [np.nan, np.nan, 201.0],
                ]
            ),
            equal_nan=True,
        )


def test_dataset_loc_runs_independent_device_groups_concurrently(tmp_path, monkeypatch):
    path = tmp_path / "weather_concurrent_devices.tsfile"
    _write_weather_rows_file(
        path,
        {
            "time": [0, 1, 0, 1],
            "device": ["device_a", "device_a", "device_b", "device_b"],
            "temperature": [10.0, 11.0, 20.0, 21.0],
            "humidity": [100.0, 101.0, 200.0, 201.0],
        },
    )
    monkeypatch.setenv("TSFILE_DATAFRAME_QUERY_WORKERS", "2")
    monkeypatch.setenv("TSFILE_DATAFRAME_QUERY_PARALLEL_MIN_ROWS", "1")

    original = RuntimeSeriesReader.read_device_fields_by_time_range
    rendezvous = threading.Barrier(2)
    worker_ids = set()

    def observed_read(reader, device_id, column_ids, start_time, end_time):
        worker_ids.add(threading.get_ident())
        rendezvous.wait(timeout=2)
        return original(reader, device_id, column_ids, start_time, end_time)

    monkeypatch.setattr(
        RuntimeSeriesReader,
        "read_device_fields_by_time_range",
        observed_read,
    )

    with TsFileDataFrame(str(path), show_progress=False, use_index=True) as tsdf:
        aligned = tsdf.loc[
            0:1,
            [
                "weather.device_a.temperature",
                "weather.device_a.humidity",
                "weather.device_b.humidity",
            ],
        ]

        assert len(worker_ids) == 2
        np.testing.assert_allclose(
            aligned.values,
            np.array([[10.0, 100.0, 200.0], [11.0, 101.0, 201.0]]),
        )


def test_dataset_loc_keeps_small_device_groups_inline(tmp_path, monkeypatch):
    path = tmp_path / "weather_inline_devices.tsfile"
    _write_weather_rows_file(
        path,
        {
            "time": [0, 1, 0, 1],
            "device": ["device_a", "device_a", "device_b", "device_b"],
            "temperature": [10.0, 11.0, 20.0, 21.0],
            "humidity": [100.0, 101.0, 200.0, 201.0],
        },
    )
    monkeypatch.setenv("TSFILE_DATAFRAME_QUERY_WORKERS", "2")
    monkeypatch.setenv("TSFILE_DATAFRAME_QUERY_PARALLEL_MIN_ROWS", "8192")

    original = RuntimeSeriesReader.read_device_fields_by_time_range
    caller_id = threading.get_ident()
    worker_ids = set()

    def observed_read(reader, device_id, column_ids, start_time, end_time):
        worker_ids.add(threading.get_ident())
        return original(reader, device_id, column_ids, start_time, end_time)

    monkeypatch.setattr(
        RuntimeSeriesReader,
        "read_device_fields_by_time_range",
        observed_read,
    )

    with TsFileDataFrame(str(path), show_progress=False, use_index=True) as tsdf:
        tsdf.loc[
            0:1,
            [
                "weather.device_a.temperature",
                "weather.device_b.humidity",
            ],
        ]

    assert worker_ids == {caller_id}


def test_dataset_reads_nullable_tag_devices_in_isolation(tmp_path, dataframe_use_index):
    path = tmp_path / "nullable_tags.tsfile"
    schema = TableSchema(
        "sensors",
        [
            ColumnSchema("region", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    # Non-trailing null: region IS NULL, device='alpha'.
    null_region = pd.DataFrame(
        {
            "time": [0, 1, 2],
            "region": [None, None, None],
            "device": ["alpha", "alpha", "alpha"],
            "temperature": [10.0, 11.0, 12.0],
        }
    )
    # Trailing null: region='north', device IS NULL. Shares the region prefix
    # with the fully specified device below to exercise device isolation.
    null_device = pd.DataFrame(
        {
            "time": [0, 1, 2],
            "region": ["north", "north", "north"],
            "device": [None, None, None],
            "temperature": [20.0, 21.0, 22.0],
        }
    )
    full = pd.DataFrame(
        {
            "time": [0, 1, 2],
            "region": ["north", "north", "north"],
            "device": ["beta", "beta", "beta"],
            "temperature": [30.0, 31.0, 32.0],
        }
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(null_region)
        writer.write_dataframe(null_device)
        writer.write_dataframe(full)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        series = tsdf.list_timeseries()
        # Null tags keep their position via the \N marker; trailing nulls drop.
        assert set(series) == {
            "sensors.\\N.alpha.temperature",
            "sensors.north.temperature",
            "sensors.north.beta.temperature",
        }
        # list_timeseries returns SeriesPath objects carrying structured tags.
        by_tags = {sp.tags: sp for sp in series}
        assert (None, "alpha") in by_tags
        assert ("north",) in by_tags
        assert ("north", "beta") in by_tags

        ordered = sorted(series)
        aligned = tsdf.loc[:, ordered]
        by_name = dict(zip(aligned.series_names, aligned.values.T))

        # Non-trailing null device reads its own data (previously crashed / NaN).
        np.testing.assert_array_equal(
            by_name["sensors.\\N.alpha.temperature"], np.array([10.0, 11.0, 12.0])
        )
        # Trailing-null device must NOT merge with the fully specified north.beta.
        np.testing.assert_array_equal(
            by_name["sensors.north.temperature"], np.array([20.0, 21.0, 22.0])
        )
        np.testing.assert_array_equal(
            by_name["sensors.north.beta.temperature"], np.array([30.0, 31.0, 32.0])
        )


def test_series_path_object_roundtrip_and_escaping():
    from tsfile.dataset.metadata import split_logical_series_path

    sp = SeriesPath("tbl", ("a.b", None, "x"), "f")
    assert isinstance(sp, str)
    assert sp.table == "tbl"
    assert sp.tags == ("a.b", None, "x")
    assert sp.field == "f"
    # A dot in a value is escaped; a null tag uses the collision-proof \N marker.
    assert str(sp) == "tbl.a\\.b.\\N.x.f"
    # Splitting round-trips: the escaped dot stays in the value, \N decodes to None.
    assert split_logical_series_path(str(sp)) == ["tbl", "a.b", None, "x", "f"]
    # Trailing nulls are dropped (mirroring device-id normalization).
    assert SeriesPath("tbl", ("a", None), "f").tags == ("a",)
    assert str(SeriesPath("tbl", ("a", None), "f")) == "tbl.a.f"


def test_series_path_construction_forms_are_equivalent():
    explicit = SeriesPath("tbl", (None, "sensorA"), "temperature")
    flat = SeriesPath(["tbl", None, "sensorA", "temperature"])  # [table, *tags, field]
    from_string = SeriesPath("tbl.\\N.sensorA.temperature")

    for sp in (explicit, flat, from_string):
        assert sp == "tbl.\\N.sensorA.temperature"
        assert sp.table == "tbl"
        assert sp.tags == (None, "sensorA")
        assert sp.field == "temperature"

    # A no-tag table is just [table, field].
    assert SeriesPath(["tbl", "f"]).tags == ()
    with pytest.raises(ValueError):
        SeriesPath(["tbl"])


def test_split_logical_series_path_null_marker_only_whole_component():
    from tsfile.dataset.metadata import split_logical_series_path

    # \N is a null tag only as a complete component.
    assert split_logical_series_path("a.\\N.b.f") == ["a", None, "b", "f"]
    assert split_logical_series_path("a.\\N.\\N.f") == ["a", None, None, "f"]
    # A real value "\N" (doubled backslash) stays a string, never null.
    assert split_logical_series_path("a.\\\\N.b.f") == ["a", "\\N", "b", "f"]

    # \N mixed with other characters is invalid input and fails fast, instead of
    # being silently parsed as a null tag (which could resolve the wrong device).
    for bad in (
        "tbl.a\\N.b.f",  # characters before the marker
        "tbl.\\Nfoo.x.f",  # characters after the marker
        "a.\\N\\N.f",  # two markers in one component
        "a.\\N\\.b.f",  # an escape after the marker
    ):
        with pytest.raises(ValueError, match="Invalid series path"):
            split_logical_series_path(bad)


def test_dataset_null_tag_positions_and_string_null_are_distinct(
    tmp_path, dataframe_use_index
):
    path = tmp_path / "null_positions.tsfile"
    schema = TableSchema(
        "a",
        [
            ColumnSchema("t1", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("t2", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("t3", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("v", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    rows = {
        (None, "b", "c"): 10.0,  # null at position 1
        ("b", None, "c"): 20.0,  # null at position 2 (distinct from the above)
        ("null", "b", "c"): 30.0,  # the literal string "null", not a real null
    }
    with TsFileTableWriter(str(path), schema) as writer:
        for tags, base in rows.items():
            writer.write_dataframe(
                pd.DataFrame(
                    {
                        "time": [0, 1],
                        "t1": [tags[0], tags[0]],
                        "t2": [tags[1], tags[1]],
                        "t3": [tags[2], tags[2]],
                        "v": [base, base + 1],
                    }
                )
            )

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        series = tsdf.list_timeseries()
        # Nothing collapses: three physically distinct devices stay distinct.
        assert len(series) == 3
        by_tags = {sp.tags: sp for sp in series}
        assert (None, "b", "c") in by_tags  # null position 1
        assert ("b", None, "c") in by_tags  # null position 2
        assert ("null", "b", "c") in by_tags  # the string "null"

        # Each device reads its own data via SeriesPath and via the \N string form.
        for tags, base in rows.items():
            sp = by_tags[tags]
            np.testing.assert_array_equal(
                tsdf.loc[:, sp].values.ravel(), np.array([base, base + 1])
            )
            np.testing.assert_array_equal(
                tsdf.loc[:, str(sp)].values.ravel(), np.array([base, base + 1])
            )

        # A hand-built SeriesPath resolves to the same null-tag device.
        hand = SeriesPath("a", (None, "b", "c"), "v")
        np.testing.assert_array_equal(
            tsdf.loc[:, hand].values.ravel(), np.array([10.0, 11.0])
        )


def test_dataset_loc_supports_single_timestamp_and_mixed_series_specifiers(
    tmp_path, dataframe_use_index
):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        aligned = tsdf.loc[1, [0, "weather.device_a.humidity"]]

        assert isinstance(aligned, AlignedTimeseries)
        assert aligned.series_names == [
            "weather.device_a.temperature",
            "weather.device_a.humidity",
        ]
        np.testing.assert_array_equal(aligned.timestamps, np.array([1], dtype=np.int64))
        np.testing.assert_array_equal(aligned.values, np.array([[21.5, 52.0]]))


def test_dataset_loc_dedups_repeated_series_specifiers(tmp_path, dataframe_use_index):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        humidity = "weather.device_a.humidity"
        humidity_idx = tsdf.list_timeseries().index(humidity)

        # 1) name + matching idx pointing at the same series.
        aligned_two_dup = tsdf.loc[0:2, [humidity, humidity_idx]]
        assert aligned_two_dup.shape == (3, 2)
        np.testing.assert_array_equal(
            aligned_two_dup.timestamps, np.array([0, 1, 2], dtype=np.int64)
        )
        np.testing.assert_array_equal(
            aligned_two_dup.values,
            np.array([[50.0, 50.0], [52.0, 52.0], [55.0, 55.0]]),
        )

        # 2) same name twice -- single-group, single-key dedup path.
        aligned_name_twice = tsdf.loc[0:2, [humidity, humidity]]
        assert aligned_name_twice.shape == (3, 2)
        np.testing.assert_array_equal(
            aligned_name_twice.values,
            np.array([[50.0, 50.0], [52.0, 52.0], [55.0, 55.0]]),
        )

        # 3) duplicate among other distinct series must not regress the
        #    historically-passing case either.
        aligned_mixed = tsdf.loc[
            0:2, [humidity, "weather.device_a.temperature", humidity_idx]
        ]
        assert aligned_mixed.shape == (3, 3)
        np.testing.assert_array_equal(
            aligned_mixed.timestamps, np.array([0, 1, 2], dtype=np.int64)
        )
        np.testing.assert_array_equal(
            aligned_mixed.values,
            np.array(
                [
                    [50.0, 20.0, 50.0],
                    [52.0, 21.5, 52.0],
                    [55.0, 23.0, 55.0],
                ]
            ),
        )


def test_dataset_loc_supports_open_ended_ranges_and_negative_series_index(
    tmp_path, dataframe_use_index
):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 100)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        aligned = tsdf.loc[:101, [-1]]

        assert isinstance(aligned, AlignedTimeseries)
        assert aligned.series_names == ["weather.device_a.humidity"]
        np.testing.assert_array_equal(
            aligned.timestamps, np.array([100, 101], dtype=np.int64)
        )
        np.testing.assert_array_equal(aligned.values, np.array([[50.0], [52.0]]))


def test_dataset_loc_with_nulls_does_not_expand_beyond_requested_time_range(
    tmp_path, dataframe_use_index
):
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

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
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


def test_dataset_loc_single_timestamp_with_nulls_keeps_exact_time_window(
    tmp_path, dataframe_use_index
):
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

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
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


def test_dataset_repr_only_builds_preview_rows(
    tmp_path, monkeypatch, dataframe_use_index
):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        tsdf._index.series = [(0, 0)] * 1000

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
        assert "TsFileDataFrame(table model, 1000 time series, 1 files)" in rendered
        assert "..." in rendered
        assert len(built_rows) == 20


def test_dataset_exposes_only_numeric_fields_and_keeps_nan(
    tmp_path, dataframe_use_index
):
    path = tmp_path / "numeric_and_text.tsfile"
    _write_numeric_and_text_file(path)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
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


def test_dataset_omits_table_model_phantom_series_for_skipped_cells(
    tmp_path, dataframe_use_index
):
    """Schema-declared fields that a device never wrote must NOT appear.

    The dataset surface treats a series as "data physically written for one
    (device, field) pair". A Tablet that skips ``add_value_by_name`` for a
    column produces a chunk with ``length=0``; that cell is not a real series
    and must not be exposed via ``list_timeseries`` / ``len(tsdf)`` /
    ``series_shards`` -- table-model and tree-model behave identically here.
    """
    from tsfile import Tablet

    path = tmp_path / "sparse_table.tsfile"
    schema = TableSchema(
        "bench",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("v1", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("v2", TSDataType.DOUBLE, ColumnCategory.FIELD),
            ColumnSchema("v3", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema) as writer:
        # d1: writes only v1 and v2 (skip v3)
        t1 = Tablet(
            ["device", "v1", "v2", "v3"],
            [
                TSDataType.STRING,
                TSDataType.DOUBLE,
                TSDataType.DOUBLE,
                TSDataType.DOUBLE,
            ],
            1,
        )
        t1.add_timestamp(0, 1)
        t1.add_value_by_name("device", 0, "d1")
        t1.add_value_by_name("v1", 0, 100.0)
        t1.add_value_by_name("v2", 0, 200.0)
        writer.write_table(t1)

        # d2: writes only v1 and v3 (skip v2)
        t2 = Tablet(
            ["device", "v1", "v2", "v3"],
            [
                TSDataType.STRING,
                TSDataType.DOUBLE,
                TSDataType.DOUBLE,
                TSDataType.DOUBLE,
            ],
            1,
        )
        t2.add_timestamp(0, 2)
        t2.add_value_by_name("device", 0, "d2")
        t2.add_value_by_name("v1", 0, 110.0)
        t2.add_value_by_name("v3", 0, 330.0)
        writer.write_table(t2)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        # 4 real cells: (d1,v1), (d1,v2), (d2,v1), (d2,v3); NO phantoms.
        assert len(tsdf) == 4
        assert sorted(tsdf.list_timeseries()) == [
            "bench.d1.v1",
            "bench.d1.v2",
            "bench.d2.v1",
            "bench.d2.v3",
        ]

        with pytest.raises(KeyError):
            tsdf["bench.d1.v3"]
        with pytest.raises(KeyError):
            tsdf["bench.d2.v2"]

    # series_count must report the 4 physically-present series, not the 2x3
    # schema cross-product -- regression guard for the sparse-schema case
    # (catalog and reader must agree).
    reader = TsFileSeriesReader(str(path), show_progress=False)
    try:
        assert reader.series_count == 4
        assert reader.catalog.series_count == 4
    finally:
        reader.close()


def test_dataset_timeseries_supports_negative_step_slices(
    tmp_path, dataframe_use_index
):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        series = tsdf[0]
        np.testing.assert_array_equal(series[::-1], np.array([23.0, 21.5, 20.0]))
        np.testing.assert_array_equal(series[::-2], np.array([23.0, 20.0]))


def test_dataset_metadata_discovery_uses_all_numeric_fields(
    tmp_path, dataframe_use_index
):
    path = tmp_path / "partial_numeric_rows.tsfile"
    _write_partial_numeric_rows_file(path)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        assert tsdf.list_timeseries() == [
            "weather.device_a.temperature",
            "weather.device_a.humidity",
        ]

        assert list(tsdf["count"]) == [2, 2]
        assert list(tsdf["start_time"]) == [0, 0]
        assert list(tsdf["end_time"]) == [1, 1]


def test_dataset_rejects_duplicate_timestamps_across_shards(
    tmp_path, dataframe_use_index
):
    path1 = tmp_path / "part1.tsfile"
    path2 = tmp_path / "part2.tsfile"
    _write_weather_file(path1, 0)
    _write_weather_file(path2, 2)

    with pytest.raises(ValueError, match="Duplicate timestamp"):
        TsFileDataFrame(
            [str(path1), str(path2)],
            show_progress=False,
            use_index=dataframe_use_index,
        )


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

    def fail_span_lookup(*_args, **_kwargs):
        raise AssertionError("overlap position reads must reuse descriptor locators")

    monkeypatch.setattr(RuntimeSeriesReader, "_span", fail_span_lookup)

    with TsFileDataFrame([str(path1), str(path2)], show_progress=False) as tsdf:
        series = tsdf["weather.device_a.temperature"]
        assert series[0] == 10.0
        assert series[1] == 20.0
        assert series[4] == 50.0
        np.testing.assert_array_equal(series[1:5], np.array([20.0, 30.0, 40.0, 50.0]))


def test_dataset_close_only_releases_current_handle(tmp_path):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    tsdf = TsFileDataFrame(str(path), show_progress=False, use_index=True)
    series = tsdf[0]
    tsdf.close()

    with pytest.raises(RuntimeError, match="TsFileDataFrame is closed"):
        _ = tsdf[0]

    assert series[0] == 20.0
    series.close()
    with pytest.raises(RuntimeError, match="Timeseries is closed"):
        _ = series[0]


def test_subset_close_releases_only_subset_lease(tmp_path):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(str(path), show_progress=False, use_index=True) as tsdf:
        subset = tsdf[:1]
        subset.close()

        with pytest.raises(RuntimeError, match="TsFileDataFrame is closed"):
            _ = subset[0]

        series = tsdf[0]
        assert series[0] == 20.0


def test_dataset_rejects_incompatible_table_schemas_across_shards(
    tmp_path, dataframe_use_index
):
    path1 = tmp_path / "part1.tsfile"
    path2 = tmp_path / "part2.tsfile"
    _write_weather_file(path1, 0)
    _write_weather_with_extra_field_file(path2, 2)

    with pytest.raises(ValueError, match="Incompatible schema for table 'weather'"):
        TsFileDataFrame(
            [str(path1), str(path2)],
            show_progress=False,
            use_index=dataframe_use_index,
        )


def test_dataset_rejects_same_field_name_with_different_type(
    tmp_path, dataframe_use_index
):
    path1 = tmp_path / "double.tsfile"
    path2 = tmp_path / "int64.tsfile"
    _write_weather_file(path1, 0)
    _write_weather_int_temperature_file(path2, 3)

    with pytest.raises(ValueError, match="Incompatible schema for table 'weather'"):
        TsFileDataFrame(
            [str(path1), str(path2)],
            show_progress=False,
            use_index=dataframe_use_index,
        )


def test_dataset_rejects_nonnumeric_declared_schema_difference(
    tmp_path, dataframe_use_index
):
    path1 = tmp_path / "text-status.tsfile"
    path2 = tmp_path / "boolean-status.tsfile"
    _write_numeric_and_text_file(path1)
    _write_weather_boolean_status_file(path2, 3)

    with pytest.raises(ValueError, match="Incompatible schema for table 'weather'"):
        TsFileDataFrame(
            [str(path1), str(path2)],
            show_progress=False,
            use_index=dataframe_use_index,
        )


def test_dataset_table_model_exposes_boolean_fields_as_float64(
    tmp_path, dataframe_use_index
):
    # The original TsFileDataFrame numeric surface includes BOOLEAN fields and
    # represents them as float64 values (1.0/0.0), just like the other exposed
    # field types. Regression for the reader/runtime field-type whitelist drift.
    path = tmp_path / "weather_boolean.tsfile"
    _write_weather_boolean_status_file(path, 0)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        assert tsdf.model == "table"
        assert tsdf.list_timeseries() == [
            "weather.device_a.temperature",
            "weather.device_a.status",
        ]
        np.testing.assert_array_equal(
            tsdf["weather.device_a.temperature"][:], np.array([20.0, 21.0])
        )
        np.testing.assert_array_equal(
            tsdf["weather.device_a.status"][:], np.array([1.0, 0.0])
        )
        assert tsdf["weather.device_a.status"][0] == 1.0
        assert isinstance(tsdf["weather.device_a.status"][0], float)


def test_dataset_table_model_exposes_timestamp_fields_as_float64(
    tmp_path, dataframe_use_index
):
    path = tmp_path / "weather_timestamp.tsfile"
    _write_weather_timestamp_field_file(path, 0)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        assert tsdf.list_timeseries() == ["weather.device_a.observed_at"]
        values = tsdf["weather.device_a.observed_at"][:]
        assert values.dtype == np.float64
        np.testing.assert_array_equal(
            values, np.array([1_700_000_000_000.0, 1_700_000_000_001.0])
        )


def test_dataset_close_waits_for_an_active_public_query(tmp_path, monkeypatch):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)
    entered = threading.Event()
    release = threading.Event()
    query_error = []
    original = RuntimeSeriesReader.read_device_fields_by_time_range

    def blocked_read(reader, device_id, column_ids, start_time, end_time):
        entered.set()
        assert release.wait(timeout=2)
        return original(reader, device_id, column_ids, start_time, end_time)

    monkeypatch.setattr(
        RuntimeSeriesReader,
        "read_device_fields_by_time_range",
        blocked_read,
    )

    dataframe = TsFileDataFrame(str(path), show_progress=False, use_index=True)
    query_done = threading.Event()

    def run_query():
        try:
            result = dataframe.loc[0:2, [0]]
            assert result.values.shape == (3, 1)
        except BaseException as exc:
            query_error.append(exc)
        finally:
            query_done.set()

    query_thread = threading.Thread(target=run_query)
    query_thread.start()
    assert entered.wait(timeout=2)

    close_done = threading.Event()

    def close_dataframe():
        dataframe.close()
        close_done.set()

    close_thread = threading.Thread(target=close_dataframe)
    close_thread.start()
    assert not close_done.wait(timeout=0.05)

    release.set()
    query_thread.join(timeout=2)
    close_thread.join(timeout=2)
    assert query_done.is_set()
    assert close_done.is_set()
    assert query_error == []


def test_dataset_skips_empty_tsfile_shards(tmp_path, dataframe_use_index):
    empty_path = tmp_path / "empty.tsfile"
    data_path = tmp_path / "part.tsfile"
    _write_empty_weather_file(empty_path)
    _write_weather_file(data_path, 0)

    with TsFileDataFrame(
        [str(empty_path), str(data_path)],
        show_progress=False,
        use_index=dataframe_use_index,
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


def test_dataset_multi_tag_metadata_discovery(tmp_path, dataframe_use_index):
    path = tmp_path / "multi_tag.tsfile"
    _write_multi_tag_file(path)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
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


def test_dataset_series_paths_escape_special_tag_values(tmp_path, dataframe_use_index):
    path = tmp_path / "special_tag.tsfile"
    _write_special_tag_file(path)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
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
    """read_series_by_row pulls TsBlocks via read_arrow_batch and must keep
    re-issuing query_table_by_row when the underlying native call stops at
    an internal block boundary before the caller's window is filled."""

    import pyarrow as pa

    class _FakeResultSet:
        def __init__(self, times, values):
            self._batch = pa.table(
                {
                    "time": pa.array(times, type=pa.int64()),
                    "totalcloudcover": pa.array(values, type=pa.float64()),
                }
            )
            self._delivered = False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            return False

        def read_arrow_batch(self):
            if self._delivered or self._batch.num_rows == 0:
                return None
            self._delivered = True
            return self._batch

    class _FakeNativeReader:
        def __init__(self, timestamps, values, boundary):
            self._timestamps = timestamps
            self._values = values
            self._boundary = boundary

        def query_table_by_row(
            self,
            table_name,
            column_names,
            offset=0,
            limit=-1,
            tag_filter=None,
            batch_size=0,
        ):
            assert table_name == "pvf"
            assert column_names == ["totalcloudcover"]
            assert tag_filter is None
            assert batch_size > 0, "row reads should use batch (Arrow) mode"
            if limit < 0:
                stop = len(self._timestamps)
            else:
                stop = min(offset + limit, len(self._timestamps))

            # Simulate the native quirk where one query stops at the next
            # internal block boundary; callers must re-issue from the
            # advanced offset to complete a large logical window.
            chunk_stop = min(stop, ((offset // self._boundary) + 1) * self._boundary)
            return _FakeResultSet(
                self._timestamps[offset:chunk_stop],
                self._values[offset:chunk_stop],
            )

    reader = object.__new__(TsFileSeriesReader)
    reader._model_kind = MODEL_TABLE
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
    catalog.series_stats_by_ref[(device_id, 0)] = SeriesStats(
        length=1,
        min_time=0,
        max_time=0,
        timeline_length=1,
        timeline_min_time=0,
        timeline_max_time=0,
    )

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
    catalog.series_stats_by_ref[(device_id, 0)] = SeriesStats(
        length=1,
        min_time=0,
        max_time=0,
        timeline_length=1,
        timeline_min_time=0,
        timeline_max_time=0,
    )

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
    catalog.series_stats_by_ref[(device_id, 0)] = SeriesStats(
        length=1,
        min_time=0,
        max_time=0,
        timeline_length=1,
        timeline_min_time=0,
        timeline_max_time=0,
    )

    series_path = build_series_path(catalog, device_id, 0)
    # The leading null tag is preserved at its position via the \N marker.
    assert series_path == "weather.\\N.device_a.temperature"
    assert series_path.tags == (None, "device_a")
    assert resolve_series_path(catalog, series_path) == (table_id, device_id, 0)
    # The plain string form (with \N) round-trips to the same device.
    assert resolve_series_path(catalog, str(series_path)) == (table_id, device_id, 0)


def test_resolve_series_path_rejects_wrong_tag_count():
    catalog = MetadataCatalog()
    table_id = catalog.add_table(
        "weather",
        ("city", "device"),
        (TSDataType.STRING, TSDataType.STRING),
        ("temperature",),
    )
    device_id = catalog.add_device(table_id, ("beijing", "d1"), 0, 1)
    catalog.series_stats_by_ref[(device_id, 0)] = {
        "length": 1,
        "min_time": 0,
        "max_time": 0,
        "timeline_length": 1,
        "timeline_min_time": 0,
        "timeline_max_time": 0,
    }

    assert resolve_series_path(catalog, "weather.beijing.d1.temperature") == (
        table_id,
        device_id,
        0,
    )
    # An extra tag must NOT be silently truncated into a match.
    with pytest.raises(ValueError, match="Series not found"):
        resolve_series_path(catalog, "weather.beijing.d1.extra.temperature")
    # Too few tags has no matching device either.
    with pytest.raises(ValueError, match="Series not found"):
        resolve_series_path(catalog, "weather.beijing.temperature")


def test_reader_metadata_tag_values_trim_trailing_none():
    class _Group:
        segments = ("weather", "device_a", None, None)

    assert TsFileSeriesReader._metadata_tag_values(_Group(), 3) == ("device_a",)
    assert TsFileSeriesReader._metadata_tag_values(_Group(), 1) == ("device_a",)


def test_exact_tag_filter_uses_is_null_for_none_tag_values():
    from tsfile.tag_filter import AndTagFilter, ComparisonTagFilter

    only_null = _build_exact_tag_filter({"device": None})
    assert isinstance(only_null, ComparisonTagFilter)
    assert only_null.op == ComparisonTagFilter.IS_NULL
    assert only_null.column_name == "device"

    mixed = _build_exact_tag_filter({"city": "beijing", "device": None})
    assert isinstance(mixed, AndTagFilter)
    assert isinstance(mixed.left, ComparisonTagFilter)
    assert mixed.left.op == ComparisonTagFilter.EQ
    assert mixed.left.value == "beijing"
    assert isinstance(mixed.right, ComparisonTagFilter)
    assert mixed.right.op == ComparisonTagFilter.IS_NULL
    assert mixed.right.column_name == "device"


def _tag_filter_has_is_null(tag_filter) -> bool:
    from tsfile.tag_filter import ComparisonTagFilter

    if isinstance(tag_filter, ComparisonTagFilter):
        return tag_filter.op == ComparisonTagFilter.IS_NULL
    for attr in ("left", "right", "filter"):
        child = getattr(tag_filter, attr, None)
        if child is not None and _tag_filter_has_is_null(child):
            return True
    return False


def test_reader_exact_match_with_none_tag_values_issues_is_null_query():
    captured = {}

    class _EmptyResultSet:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read_arrow_batch(self):
            return None

        def next(self):
            return False

    class _FakeNativeReader:
        def query_table(self, *args, **kwargs):
            captured["table"] = kwargs.get("tag_filter")
            return _EmptyResultSet()

        def query_table_by_row(self, *args, **kwargs):
            captured["row"] = kwargs.get("tag_filter")
            return _EmptyResultSet()

    reader = object.__new__(TsFileSeriesReader)
    reader._model_kind = MODEL_TABLE
    reader._reader = _FakeNativeReader()
    reader._catalog = MetadataCatalog()
    table_id = reader._catalog.add_table(
        "weather",
        ("city", "device", "region"),
        (TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
        ("temperature",),
    )
    device_id = reader._catalog.add_device(table_id, (None, "device_a", "north"), 0, 1)
    reader._catalog.series_stats_by_ref[(device_id, 0)] = SeriesStats(
        length=2,
        min_time=0,
        max_time=1,
        timeline_length=2,
        timeline_min_time=0,
        timeline_max_time=1,
    )

    # Both read paths now issue a native query that encodes the null tag as
    # IS NULL instead of failing fast.
    reader.read_series_by_ref(device_id, 0, 0, 1)
    reader.read_series_by_row(device_id, 0, 0, 2)

    assert _tag_filter_has_is_null(captured["table"])
    assert _tag_filter_has_is_null(captured["row"])


def test_dataframe_resolves_named_sparse_tag_series_path():
    tsdf = object.__new__(TsFileDataFrame)
    tsdf._index = dataframe_module._DataFrameCatalog()
    tsdf._index.table_entries["weather"] = dataframe_module.TableEntry(
        table_name="weather",
        tag_columns=("city", "device", "region"),
        tag_types=(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
        field_columns=("temperature",),
    )
    device_key = ("weather", (None, "device_a"))
    tsdf._index.devices = [device_key]
    tsdf._index.device_index = {device_key: 0}
    tsdf._index.device_time_bounds = [(0, 1)]
    tsdf._index.series = [(0, 0)]
    tsdf._index.series_shards = {(0, 0): []}

    assert tsdf.list_timeseries() == ["weather.\\N.device_a.temperature"]
    # Resolvable by the \N string form and by the returned SeriesPath itself.
    assert tsdf._resolve_series_name("weather.\\N.device_a.temperature") == (0, 0)
    assert tsdf._resolve_series_name(tsdf.list_timeseries()[0]) == (0, 0)


def test_dataframe_list_timeseries_filters_named_sparse_tag_prefix():
    tsdf = object.__new__(TsFileDataFrame)
    tsdf._index = dataframe_module._DataFrameCatalog()
    tsdf._index.table_entries["weather"] = dataframe_module.TableEntry(
        table_name="weather",
        tag_columns=("city", "device", "region"),
        tag_types=(TSDataType.STRING, TSDataType.STRING, TSDataType.STRING),
        field_columns=("temperature",),
    )
    tsdf._index.devices = [
        ("weather", (None, "device_a")),
        ("weather", ("beijing", "device_b")),
    ]
    tsdf._index.device_index = {
        ("weather", (None, "device_a")): 0,
        ("weather", ("beijing", "device_b")): 1,
    }
    tsdf._index.device_time_bounds = [(0, 1), (0, 1)]
    tsdf._index.series = [(0, 0), (1, 0)]
    tsdf._index.series_shards = {(0, 0): [], (1, 0): []}

    # Prefix matching is position-aware: "weather.\N" selects the null-city
    # device, "weather.beijing" selects the fully specified one.
    assert tsdf.list_timeseries("weather.\\N") == ["weather.\\N.device_a.temperature"]
    assert tsdf.list_timeseries("weather.beijing") == [
        "weather.beijing.device_b.temperature"
    ]


def test_dataframe_list_timeseries_prefix_can_skip_full_name_build(
    tmp_path, monkeypatch, dataframe_use_index
):
    path = tmp_path / "weather.tsfile"
    _write_weather_file(path, 0)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        tsdf._index.series = [(0, 0)] * 1000

        def fail_build_series_name(_series_ref):
            raise AssertionError(
                "list_timeseries(prefix) should not build full names for non-matching series"
            )

        monkeypatch.setattr(tsdf, "_build_series_name", fail_build_series_name)
        assert tsdf.list_timeseries("pvf") == []


def test_series_path_resolution_distinguishes_null_position():
    catalog = MetadataCatalog()
    table_id = catalog.add_table(
        "weather",
        ("city", "device"),
        (TSDataType.STRING, TSDataType.STRING),
        ("temperature",),
    )
    first_id = catalog.add_device(table_id, ("beijing", None), 0, 1)  # device IS NULL
    second_id = catalog.add_device(table_id, (None, "beijing"), 0, 1)  # city IS NULL
    for device_id in (first_id, second_id):
        catalog.series_stats_by_ref[(device_id, 0)] = SeriesStats(
            length=1,
            min_time=0,
            max_time=0,
            timeline_length=1,
            timeline_min_time=0,
            timeline_max_time=0,
        )

    # Null position is preserved, so these two devices get distinct paths
    # (previously both compressed to "weather.beijing.temperature" -> ambiguous).
    first_path = build_series_path(catalog, first_id, 0)
    second_path = build_series_path(catalog, second_id, 0)
    assert first_path == "weather.beijing.temperature"
    assert second_path == "weather.\\N.beijing.temperature"
    assert first_path != second_path

    # Each resolves unambiguously back to its own device.
    assert resolve_series_path(catalog, first_path) == (table_id, first_id, 0)
    assert resolve_series_path(catalog, second_path) == (table_id, second_id, 0)


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


# --- Tree-model tests -------------------------------------------------------


def _write_tree_file(path):
    """Tree-model TsFile with two devices; the second device is shorter and
    only has one of the two declared measurements, exercising None-pad +
    union-field paths in the synthetic table layer.
    """
    from tsfile import (
        Field,
        RowRecord,
        TimeseriesSchema,
        TsFileWriter,
    )

    writer = TsFileWriter(str(path))
    writer.register_timeseries(
        "root.ln.wf01.wt01", TimeseriesSchema("status", TSDataType.INT32)
    )
    writer.register_timeseries(
        "root.ln.wf01.wt01", TimeseriesSchema("temperature", TSDataType.DOUBLE)
    )
    writer.register_timeseries(
        "root.ln.wf02.wt02", TimeseriesSchema("status", TSDataType.INT32)
    )
    for t in range(5):
        writer.write_row_record(
            RowRecord(
                "root.ln.wf01.wt01",
                t,
                [
                    Field("status", t, TSDataType.INT32),
                    Field("temperature", float(t) + 0.5, TSDataType.DOUBLE),
                ],
            )
        )
        writer.write_row_record(
            RowRecord(
                "root.ln.wf02.wt02",
                t,
                [Field("status", t * 2, TSDataType.INT32)],
            )
        )
    writer.close()


def test_dataset_tree_model_metadata_and_repr(tmp_path, dataframe_use_index):
    path = tmp_path / "tree.tsfile"
    _write_tree_file(path)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        assert tsdf.model == "tree"
        assert len(tsdf) == 3
        assert sorted(tsdf.list_timeseries()) == [
            "root.ln.wf01.wt01.status",
            "root.ln.wf01.wt01.temperature",
            "root.ln.wf02.wt02.status",
        ]

        rendered = repr(tsdf)
        # Header carries the model marker; tag headers use _col_i (1-based).
        assert "TsFileDataFrame(tree model, 3 time series, 1 files)" in rendered
        assert "_col_1" in rendered and "_col_2" in rendered and "_col_3" in rendered
        assert "table" not in rendered.splitlines()[1]  # no 'table' header

        # Metadata column projection: _col_i and field; 'table' is rejected.
        assert list(tsdf["_col_1"]) == ["ln", "ln", "ln"]
        assert list(tsdf["_col_3"]) == ["wt01", "wt01", "wt02"]
        assert list(tsdf["field"]) == ["status", "temperature", "status"]
        with pytest.raises(KeyError):
            tsdf["table"]


def test_dataset_tree_model_series_access(tmp_path, dataframe_use_index):
    path = tmp_path / "tree.tsfile"
    _write_tree_file(path)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        ts = tsdf["root.ln.wf01.wt01.temperature"]
        assert isinstance(ts, Timeseries)
        assert ts.name == "root.ln.wf01.wt01.temperature"
        assert len(ts) == 5
        np.testing.assert_array_equal(ts.timestamps, np.arange(5, dtype=np.int64))
        # __getitem__ slice routes through _read_series_by_row_tree.
        first_three = ts[0:3]
        np.testing.assert_array_equal(first_three, np.array([0.5, 1.5, 2.5]))

        # Aligned read across two co-located series.
        aligned = tsdf.loc[
            0:5,
            [
                "root.ln.wf01.wt01.temperature",
                "root.ln.wf01.wt01.status",
            ],
        ]
        assert isinstance(aligned, AlignedTimeseries)
        assert aligned.shape == (5, 2)
        np.testing.assert_array_equal(aligned.timestamps, np.arange(5, dtype=np.int64))


def test_dataset_tree_model_reads_uppercase_measurement_names(
    tmp_path, dataframe_use_index
):
    """Tree-model series with uppercase measurement names must read data.

    Regression: a measurement like ``Temperature``/``STATUS`` must return its
    values, not an empty array, when read back through TsFileDataFrame.
    """
    from tsfile import Field, RowRecord, TimeseriesSchema, TsFileWriter

    path = tmp_path / "tree_upper.tsfile"
    writer = TsFileWriter(str(path))
    writer.register_timeseries(
        "root.ln.wf01.wt01", TimeseriesSchema("Temperature", TSDataType.DOUBLE)
    )
    writer.register_timeseries(
        "root.ln.wf01.wt01", TimeseriesSchema("STATUS", TSDataType.INT32)
    )
    for t in range(5):
        writer.write_row_record(
            RowRecord(
                "root.ln.wf01.wt01",
                t,
                [
                    Field("Temperature", float(t) + 0.5, TSDataType.DOUBLE),
                    Field("STATUS", t * 2, TSDataType.INT32),
                ],
            )
        )
    writer.close()

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        assert sorted(tsdf.list_timeseries()) == [
            "root.ln.wf01.wt01.STATUS",
            "root.ln.wf01.wt01.Temperature",
        ]
        np.testing.assert_array_equal(
            tsdf["root.ln.wf01.wt01.Temperature"][:],
            np.array([0.5, 1.5, 2.5, 3.5, 4.5]),
        )
        np.testing.assert_array_equal(
            tsdf["root.ln.wf01.wt01.STATUS"][:],
            np.array([0.0, 2.0, 4.0, 6.0, 8.0]),
        )


def test_dataset_tree_model_case_distinct_measurements_do_not_collide(
    tmp_path, capsys, dataframe_use_index
):
    """Case-distinct measurements on one device must not be conflated.

    ``temperature`` and ``Temperature`` are two independent series; each must
    return its own values instead of an empty array or the other's data.
    """
    from tsfile import Field, RowRecord, TimeseriesSchema, TsFileWriter

    path = tmp_path / "tree_case.tsfile"

    writer = TsFileWriter(str(path))
    writer.register_timeseries(
        "root.case.d1", TimeseriesSchema("temperature", TSDataType.DOUBLE)
    )
    writer.register_timeseries(
        "root.case.d1", TimeseriesSchema("Temperature", TSDataType.DOUBLE)
    )
    for t in range(3):
        writer.write_row_record(
            RowRecord(
                "root.case.d1",
                t,
                [
                    Field("temperature", 30.0 + t, TSDataType.DOUBLE),
                    Field("Temperature", 40.0 + t, TSDataType.DOUBLE),
                ],
            )
        )
    writer.close()

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        # Two independent tree-model series are discovered (case preserved).
        assert len(tsdf) == 2
        assert sorted(tsdf.list_timeseries()) == [
            "root.case.d1.Temperature",
            "root.case.d1.temperature",
        ]

        # Metadata layer keeps both rows distinct with their original casing.
        meta = tsdf.list_timeseries_metadata()
        assert sorted(meta.index.tolist()) == [
            "root.case.d1.Temperature",
            "root.case.d1.temperature",
        ]
        for name in ("root.case.d1.temperature", "root.case.d1.Temperature"):
            assert meta.loc[name, "_col_1"] == "case"
            assert meta.loc[name, "_col_2"] == "d1"
            assert meta.loc[name, "count"] == 3
        assert set(meta["field"]) == {"temperature", "Temperature"}

        # print(tsdf) renders both case-distinct series verbatim. The timestamp
        # text is derived from format_timestamp so the whole-string match stays
        # stable across timezones.
        t0, t2 = format_timestamp(0), format_timestamp(2)
        expected_repr = "\n".join(
            [
                "TsFileDataFrame(tree model, 2 time series, 1 files)",
                "   _col_1  _col_2        field           start_time                 end_time  count",
                f"0    case      d1  Temperature  {t0}  {t2}      3",
                f"1    case      d1  temperature  {t0}  {t2}      3",
            ]
        )
        assert repr(tsdf) == expected_repr
        print(tsdf)
        assert capsys.readouterr().out == expected_repr + "\n"

        # Each series must read back its OWN values -- not empty, not the
        # other's data. Lower-casing anywhere in the read path breaks this.
        np.testing.assert_array_equal(
            tsdf["root.case.d1.temperature"][:],
            np.array([30.0, 31.0, 32.0]),
        )
        np.testing.assert_array_equal(
            tsdf["root.case.d1.Temperature"][:],
            np.array([40.0, 41.0, 42.0]),
        )

        # Aligned read of both case-distinct series keeps them separated.
        aligned = tsdf.loc[
            0:2,
            ["root.case.d1.temperature", "root.case.d1.Temperature"],
        ]
        assert aligned.series_names == [
            "root.case.d1.temperature",
            "root.case.d1.Temperature",
        ]
        np.testing.assert_array_equal(
            aligned.values,
            np.array([[30.0, 40.0], [31.0, 41.0], [32.0, 42.0]]),
        )


def test_tree_reader_handles_stale_path_columns_after_reused_queries(
    tmp_path, dataframe_use_index
):
    """Reusing a reader must not leak prefix path state across queries.

    Reading one device series then another reuses the cached device id; stale
    prefix segments used to mismatch the device and return empty data.
    """
    path = tmp_path / "tree.tsfile"
    _write_tree_file(path)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        # First read establishes query state on the reader.
        np.testing.assert_array_equal(
            tsdf["root.ln.wf01.wt01.temperature"][:],
            np.array([0.5, 1.5, 2.5, 3.5, 4.5]),
        )
        # Second read reuses the same reader for another device.
        np.testing.assert_array_equal(
            tsdf["root.ln.wf02.wt02.status"][:],
            np.array([0.0, 2.0, 4.0, 6.0, 8.0]),
        )
        # Read back the first series to confirm alternating reads stay stable.
        np.testing.assert_array_equal(
            tsdf["root.ln.wf01.wt01.temperature"][:],
            np.array([0.5, 1.5, 2.5, 3.5, 4.5]),
        )


def test_dataset_tree_model_list_timeseries_metadata(tmp_path, dataframe_use_index):
    path = tmp_path / "tree.tsfile"
    _write_tree_file(path)

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        meta = tsdf.list_timeseries_metadata()

        assert isinstance(meta, pd.DataFrame)
        assert list(meta.columns) == [
            "field",
            "start_time",
            "end_time",
            "count",
            "_col_1",
            "_col_2",
            "_col_3",
        ]
        assert sorted(meta.index.tolist()) == sorted(tsdf.list_timeseries())
        # Time bounds surface as pandas.Timestamp for ergonomic comparison.
        assert pd.api.types.is_datetime64_any_dtype(meta["start_time"])
        assert pd.api.types.is_datetime64_any_dtype(meta["end_time"])
        # Per-series count comes from the catalog, not the synthetic union.
        assert meta.loc["root.ln.wf01.wt01.temperature", "count"] == 5
        assert meta.loc["root.ln.wf02.wt02.status", "count"] == 5


def test_dataset_rejects_mixed_model_load(tmp_path, dataframe_use_index):
    table_path = tmp_path / "weather.tsfile"
    tree_path = tmp_path / "tree.tsfile"
    _write_weather_file(table_path, 0)
    _write_tree_file(tree_path)

    with pytest.raises(ValueError, match="Mixed table-model and tree-model"):
        TsFileDataFrame(
            [str(table_path), str(tree_path)],
            show_progress=False,
            use_index=dataframe_use_index,
        )


def _write_tree_rows(path, device_measurements, t_start=0, t_count=3):
    """Write a tree-model TsFile from a device -> [(measurement, dtype)] map.

    Each device's measurements are written for timestamps
    ``t_start .. t_start + t_count - 1``; numeric values are ``float(t) + 0.5``
    (INT32 measurements use ``t``), so series read back deterministically.
    """
    from tsfile import Field, RowRecord, TimeseriesSchema, TsFileWriter

    writer = TsFileWriter(str(path))
    for device, measurements in device_measurements.items():
        for name, dtype in measurements:
            writer.register_timeseries(device, TimeseriesSchema(name, dtype))
    for t in range(t_start, t_start + t_count):
        for device, measurements in device_measurements.items():
            fields = [
                Field(name, (t if dtype == TSDataType.INT32 else float(t) + 0.5), dtype)
                for name, dtype in measurements
            ]
            writer.write_row_record(RowRecord(device, t, fields))
    writer.close()


def test_dataset_tree_model_merges_identical_structure_across_files(
    tmp_path, dataframe_use_index
):
    # Two tree files, same device/measurement, disjoint time ranges: one logical
    # series whose shards and time bounds merge.
    path1 = tmp_path / "t1.tsfile"
    path2 = tmp_path / "t2.tsfile"
    _write_tree_rows(path1, {"root.a.b": [("m1", TSDataType.DOUBLE)]}, t_start=0)
    _write_tree_rows(path2, {"root.a.b": [("m1", TSDataType.DOUBLE)]}, t_start=10)

    with TsFileDataFrame(
        [str(path1), str(path2)],
        show_progress=False,
        use_index=dataframe_use_index,
    ) as tsdf:
        assert tsdf.model == "tree"
        assert tsdf.list_timeseries() == ["root.a.b.m1"]
        ts = tsdf["root.a.b.m1"]
        assert len(ts) == 6
        np.testing.assert_array_equal(
            ts.timestamps, np.array([0, 1, 2, 10, 11, 12], dtype=np.int64)
        )
        meta = tsdf.list_timeseries_metadata()
        assert meta.loc["root.a.b.m1", "count"] == 6


def test_dataset_tree_model_unions_fields_across_files(tmp_path, dataframe_use_index):
    # Same device, different measurement subsets across files -> union of fields.
    path1 = tmp_path / "t1.tsfile"
    path2 = tmp_path / "t2.tsfile"
    _write_tree_rows(path1, {"root.a.b": [("m1", TSDataType.DOUBLE)]}, t_start=0)
    _write_tree_rows(path2, {"root.a.b": [("m2", TSDataType.DOUBLE)]}, t_start=0)

    with TsFileDataFrame(
        [str(path1), str(path2)],
        show_progress=False,
        use_index=dataframe_use_index,
    ) as tsdf:
        assert tsdf.model == "tree"
        assert sorted(tsdf.list_timeseries()) == ["root.a.b.m1", "root.a.b.m2"]
        # Each field reads its own file's data, not the other's.
        np.testing.assert_array_equal(tsdf["root.a.b.m1"][:], np.array([0.5, 1.5, 2.5]))
        np.testing.assert_array_equal(tsdf["root.a.b.m2"][:], np.array([0.5, 1.5, 2.5]))
        # Aligned read spans the unioned fields on the one device.
        aligned = tsdf.loc[0:2, ["root.a.b.m1", "root.a.b.m2"]]
        assert aligned.shape == (3, 2)
        np.testing.assert_array_equal(
            aligned.timestamps, np.array([0, 1, 2], dtype=np.int64)
        )


def test_dataset_tree_model_unions_different_depths_across_files(
    tmp_path, dataframe_use_index
):
    # Files with different max depth -> global tag layout widens; the shallower
    # device pads its deepest tag column with null.
    path1 = tmp_path / "t1.tsfile"
    path2 = tmp_path / "t2.tsfile"
    _write_tree_rows(path1, {"root.a.b": [("m1", TSDataType.DOUBLE)]}, t_start=0)
    _write_tree_rows(path2, {"root.a.b.c": [("m1", TSDataType.DOUBLE)]}, t_start=0)

    with TsFileDataFrame(
        [str(path1), str(path2)],
        show_progress=False,
        use_index=dataframe_use_index,
    ) as tsdf:
        assert tsdf.model == "tree"
        assert sorted(tsdf.list_timeseries()) == ["root.a.b.c.m1", "root.a.b.m1"]
        meta = tsdf.list_timeseries_metadata()
        # Global depth widened to the deeper file (a, b, c -> _col_3 exists).
        assert "_col_3" in meta.columns
        assert meta.loc["root.a.b.c.m1", "_col_3"] == "c"
        # The shallower device's deepest tag column is null (padded).
        assert pd.isna(meta.loc["root.a.b.m1", "_col_3"])
        # Each device still reads its own data.
        np.testing.assert_array_equal(tsdf["root.a.b.m1"][:], np.array([0.5, 1.5, 2.5]))
        np.testing.assert_array_equal(
            tsdf["root.a.b.c.m1"][:], np.array([0.5, 1.5, 2.5])
        )


def test_dataset_tree_model_omits_non_numeric_measurements(
    tmp_path, dataframe_use_index
):
    # The dataset surface is numeric (float64); a STRING tree measurement must
    # be dropped, not surfaced as a series that crashes on read.
    from tsfile import Field, RowRecord, TimeseriesSchema, TsFileWriter

    path = tmp_path / "tree_mixed.tsfile"
    writer = TsFileWriter(str(path))
    writer.register_timeseries("root.a.b", TimeseriesSchema("temp", TSDataType.DOUBLE))
    writer.register_timeseries(
        "root.a.b", TimeseriesSchema("online", TSDataType.BOOLEAN)
    )
    writer.register_timeseries(
        "root.a.b", TimeseriesSchema("status", TSDataType.STRING)
    )
    for t in range(3):
        writer.write_row_record(
            RowRecord(
                "root.a.b",
                t,
                [
                    Field("temp", float(t) + 0.5, TSDataType.DOUBLE),
                    Field("online", t % 2 == 0, TSDataType.BOOLEAN),
                    Field("status", "ok", TSDataType.STRING),
                ],
            )
        )
    writer.close()

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        # Numeric-compatible fields are exposed as float64; STRING is dropped.
        assert sorted(tsdf.list_timeseries()) == ["root.a.b.online", "root.a.b.temp"]
        with pytest.raises(KeyError):
            tsdf["root.a.b.status"]
        np.testing.assert_array_equal(
            tsdf["root.a.b.temp"][:], np.array([0.5, 1.5, 2.5])
        )
        np.testing.assert_array_equal(
            tsdf["root.a.b.online"][:], np.array([1.0, 0.0, 1.0])
        )


def test_dataset_tree_model_loc_aligns_sparse_non_aligned_fields(
    tmp_path, dataframe_use_index
):
    # Non-aligned (tree) device whose two measurements are sampled at
    # different timestamps must produce a timestamp union with NaN fill,
    # instead of raising when the per-field timelines differ. Regression for
    # read_device_fields_by_time_range on non-aligned devices.
    from tsfile import Field, RowRecord, TimeseriesSchema, TsFileWriter

    path = tmp_path / "sparse_tree.tsfile"
    writer = TsFileWriter(str(path))
    writer.register_timeseries("root.a.b", TimeseriesSchema("m1", TSDataType.DOUBLE))
    writer.register_timeseries("root.a.b", TimeseriesSchema("m2", TSDataType.DOUBLE))
    writer.write_row_record(
        RowRecord("root.a.b", 0, [Field("m1", 0.5, TSDataType.DOUBLE)])
    )
    writer.write_row_record(
        RowRecord("root.a.b", 1, [Field("m2", 10.5, TSDataType.DOUBLE)])
    )
    writer.write_row_record(
        RowRecord("root.a.b", 2, [Field("m1", 2.5, TSDataType.DOUBLE)])
    )
    writer.write_row_record(
        RowRecord("root.a.b", 3, [Field("m2", 30.5, TSDataType.DOUBLE)])
    )
    writer.write_row_record(
        RowRecord("root.a.b", 4, [Field("m1", 4.5, TSDataType.DOUBLE)])
    )
    writer.close()

    with TsFileDataFrame(
        str(path), show_progress=False, use_index=dataframe_use_index
    ) as tsdf:
        assert tsdf.model == "tree"
        aligned = tsdf.loc[0:5, ["root.a.b.m1", "root.a.b.m2"]]
        assert isinstance(aligned, AlignedTimeseries)
        assert aligned.series_names == ["root.a.b.m1", "root.a.b.m2"]
        np.testing.assert_array_equal(
            aligned.timestamps, np.array([0, 1, 2, 3, 4], dtype=np.int64)
        )
        np.testing.assert_allclose(
            aligned.values,
            np.array(
                [
                    [0.5, np.nan],
                    [np.nan, 10.5],
                    [2.5, np.nan],
                    [np.nan, 30.5],
                    [4.5, np.nan],
                ]
            ),
            equal_nan=True,
        )
