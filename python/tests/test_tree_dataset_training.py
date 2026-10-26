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

"""Tree-model sequence reads, statistics, and numeric training windows."""

from pathlib import Path
import shutil

import numpy as np
import pandas as pd
import pytest

from tsfile import (
    Field,
    RowRecord,
    TSDataType,
    TimeseriesSchema,
    TsFileDataFrame,
    TsFileWriter,
)


def _value(dtype, timestamp):
    if dtype == TSDataType.BOOLEAN:
        return timestamp % 2 == 0
    if dtype in {TSDataType.FLOAT, TSDataType.DOUBLE}:
        return timestamp + 0.25
    return timestamp * 10 + 100


def _write_tree(path, specs, start, stop):
    with TsFileWriter(str(path)) as writer:
        for device, field, dtype in specs:
            writer.register_timeseries(device, TimeseriesSchema(field, dtype))
        for timestamp in range(start, stop):
            for device, field, dtype in specs:
                writer.write_row_record(
                    RowRecord(
                        device,
                        timestamp,
                        [Field(field, _value(dtype, timestamp), dtype)],
                    )
                )


@pytest.mark.parametrize("use_index", [False, True], ids=["no-index", "index"])
@pytest.mark.parametrize("shards", [1, 2])
@pytest.mark.parametrize(
    "other_type",
    [
        TSDataType.INT32,
        TSDataType.INT64,
        TSDataType.FLOAT,
        TSDataType.BOOLEAN,
        TSDataType.TIMESTAMP,
    ],
)
def test_tree_independent_measurement_types_read_training_windows(
    tmp_path, monkeypatch, use_index, shards, other_type
):
    specs = [
        ("root.training.d1", "temperature", TSDataType.DOUBLE),
        ("root.training.d2.deep", "temperature", other_type),
    ]
    paths = []
    for shard in range(shards):
        path = tmp_path / f"part{shard}.tsfile"
        _write_tree(path, specs, shard * 6, (shard + 1) * 6)
        paths.append(str(path))
    timestamps = np.arange(shards * 6, dtype=np.int64)
    names = [f"{device}.{field}" for device, field, _ in specs]
    expected = {
        f"{device}.{field}": np.array(
            [_value(dtype, int(t)) for t in timestamps], dtype=np.float64
        )
        for device, field, dtype in specs
    }

    for load in range(2 if use_index else 1):
        if load:

            def fail_scan(*_args, **_kwargs):
                raise AssertionError("hot tree reload must use the persisted index")

            monkeypatch.setattr(
                TsFileDataFrame, "_load_metadata_without_index", fail_scan
            )
        with TsFileDataFrame(paths, show_progress=False, use_index=use_index) as frame:
            assert frame.model == "tree"
            assert sorted(frame.list_timeseries()) == sorted(names)
            metadata = frame.list_timeseries_metadata()
            for name in names:
                with frame[name] as series:
                    np.testing.assert_array_equal(series.timestamps, timestamps)
                    np.testing.assert_array_equal(series[:], expected[name])
                    np.testing.assert_array_equal(series[1:5], expected[name][1:5])
                    np.testing.assert_array_equal(series[::-1], expected[name][::-1])
                    assert series.stats == {
                        "start_time": 0,
                        "end_time": int(timestamps[-1]),
                        "count": len(timestamps),
                    }
                    windows = np.stack(
                        [series[start : start + 3] for start in (0, 2, 3)]
                    )
                    assert windows.shape == (3, 3)
                    assert windows.dtype == np.float64
                    assert np.isfinite(windows).all()
                assert metadata.loc[name, "count"] == len(timestamps)
                assert metadata.loc[name, "start_time"] == pd.Timestamp(0, unit="ms")
                assert metadata.loc[name, "end_time"] == pd.Timestamp(
                    int(timestamps[-1]), unit="ms"
                )
            batch = frame.loc[1:4, names]
            np.testing.assert_array_equal(batch.timestamps, timestamps[1:5])
            np.testing.assert_array_equal(
                batch.values, np.column_stack([expected[name][1:5] for name in names])
            )


@pytest.mark.parametrize("use_index", [False, True], ids=["no-index", "index"])
@pytest.mark.parametrize("field", ["_col_1", "col_1"])
def test_tree_path_column_names_are_valid_measurements(tmp_path, use_index, field):
    path = tmp_path / "tree.tsfile"
    _write_tree(path, [("root.a.b", field, TSDataType.DOUBLE)], 0, 6)
    name = f"root.a.b.{field}"
    expected = np.arange(6, dtype=np.float64) + 0.25
    for _ in range(2 if use_index else 1):
        with TsFileDataFrame(
            str(path), show_progress=False, use_index=use_index
        ) as frame:
            assert frame.list_timeseries() == [name]
            with frame[name] as series:
                np.testing.assert_array_equal(series[:], expected)
                assert series.stats == {"start_time": 0, "end_time": 5, "count": 6}
            assert frame.list_timeseries_metadata().loc[name, "count"] == 6
            np.testing.assert_array_equal(
                frame.loc[1:3, [name]].values[:, 0], expected[1:4]
            )


def test_tree_index_rejects_type_changes_in_the_same_series(tmp_path):
    first, second = tmp_path / "first.tsfile", tmp_path / "second.tsfile"
    _write_tree(first, [("root.a.b", "temperature", TSDataType.DOUBLE)], 0, 3)
    _write_tree(second, [("root.a.b", "temperature", TSDataType.INT32)], 3, 6)
    with pytest.raises(
        ValueError, match=r"incompatible physical type for root\.a\.b\.temperature"
    ):
        TsFileDataFrame([str(first), str(second)], show_progress=False, use_index=True)


@pytest.mark.parametrize("use_index", [False, True], ids=["no-index", "index"])
@pytest.mark.parametrize("time_encoding", ["plain", "ts2diff"])
def test_java_aligned_tree_training_windows_preserve_nulls(
    tmp_path, use_index, time_encoding
):
    filename = (
        "tree_aligned_mixed.tsfile"
        if time_encoding == "plain"
        else "tree_aligned_mixed_ts2diff.tsfile"
    )
    source = Path(__file__).parent / "resources" / filename
    path = tmp_path / "aligned.tsfile"
    shutil.copyfile(source, path)
    expected = {
        "root.training.d1.temperature": np.array(
            [0.25, 1.25, np.nan, 3.25, 4.25, 5.25]
        ),
        "root.training.d1.status": np.arange(6, dtype=np.float64),
        "root.training.d2.deep.temperature": np.array(
            [100, 110, 120, np.nan, 140, 150]
        ),
        "root.training.d2.deep.status": np.array([1, 0, 1, 0, 1, 0], dtype=np.float64),
    }
    for _ in range(2 if use_index else 1):
        with TsFileDataFrame(
            str(path), show_progress=False, use_index=use_index
        ) as frame:
            metadata = frame.list_timeseries_metadata()
            for name, values in expected.items():
                with frame[name] as series:
                    np.testing.assert_allclose(series[:], values, equal_nan=True)
                    np.testing.assert_allclose(series[1:5], values[1:5], equal_nan=True)
                    np.testing.assert_array_equal(
                        series.timestamps, np.arange(6, dtype=np.int64)
                    )
                    assert series.stats == {"start_time": 0, "end_time": 5, "count": 6}
                assert metadata.loc[name, "count"] == 6
            names = list(expected)
            batch = frame.loc[1:4, names]
            np.testing.assert_array_equal(
                batch.timestamps, np.arange(1, 5, dtype=np.int64)
            )
            np.testing.assert_allclose(
                batch.values,
                np.column_stack([expected[name][1:5] for name in names]),
                equal_nan=True,
            )
