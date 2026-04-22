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
import pytest

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    Field,
    RowRecord,
    TableSchema,
    TSDataType,
    TimeseriesSchema,
    TsFileTableWriter,
    TsFileWriter,
    TsFileDataFrame,
)
import pandas as pd


def _write_tree_file(path, devices, num_rows=10):
    """Write a tree-model TsFile.

    devices: dict of device_path -> list of (measurement_name, TSDataType) tuples
    """
    writer = TsFileWriter(str(path))
    for device_path, measurements in devices.items():
        for meas_name, meas_type in measurements:
            writer.register_timeseries(
                device_path, TimeseriesSchema(meas_name, meas_type)
            )

    for t in range(num_rows):
        for device_path, measurements in devices.items():
            fields = []
            for meas_name, meas_type in measurements:
                if meas_type == TSDataType.INT64:
                    fields.append(Field(meas_name, t * 100, TSDataType.INT64))
                elif meas_type == TSDataType.DOUBLE:
                    fields.append(Field(meas_name, t * 1.5, TSDataType.DOUBLE))
                elif meas_type == TSDataType.INT32:
                    fields.append(Field(meas_name, t * 2, TSDataType.INT32))
                elif meas_type == TSDataType.FLOAT:
                    fields.append(Field(meas_name, t * 0.5, TSDataType.FLOAT))
            writer.write_row_record(RowRecord(device_path, t, fields))

    writer.close()


def _write_table_file(path, start=0, num_rows=5):
    """Write a table-model TsFile for mixed-directory tests."""
    schema = TableSchema(
        "sensors",
        [
            ColumnSchema("region", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    df = pd.DataFrame(
        {
            "time": [start + i for i in range(num_rows)],
            "region": ["east"] * num_rows,
            "temperature": [20.0 + i for i in range(num_rows)],
        }
    )
    with TsFileTableWriter(str(path), schema) as writer:
        writer.write_dataframe(df)


class TestTreeModelBasic:
    def test_single_device_list_timeseries(self, tmp_path):
        fpath = tmp_path / "tree_basic.tsfile"
        _write_tree_file(
            fpath,
            {"root.d1": [("s1", TSDataType.INT64), ("s2", TSDataType.DOUBLE)]},
        )

        with TsFileDataFrame(str(fpath), show_progress=False) as df:
            series = df.list_timeseries()
            assert "root.d1.s1" in series
            assert "root.d1.s2" in series
            assert len(df) == 2

    def test_getitem_by_name(self, tmp_path):
        fpath = tmp_path / "tree_getitem.tsfile"
        _write_tree_file(
            fpath,
            {"root.d1": [("s1", TSDataType.INT64)]},
            num_rows=5,
        )

        with TsFileDataFrame(str(fpath), show_progress=False) as df:
            ts = df["root.d1.s1"]
            assert len(ts) == 5
            assert ts[0] == 0.0
            assert ts[4] == 400.0

    def test_getitem_by_index(self, tmp_path):
        fpath = tmp_path / "tree_idx.tsfile"
        _write_tree_file(
            fpath,
            {"root.d1": [("s1", TSDataType.INT64)]},
            num_rows=3,
        )

        with TsFileDataFrame(str(fpath), show_progress=False) as df:
            ts = df[0]
            assert ts.name == "root.d1.s1"
            assert len(ts) == 3

    def test_loc_time_range(self, tmp_path):
        fpath = tmp_path / "tree_loc.tsfile"
        _write_tree_file(
            fpath,
            {"root.d1": [("s1", TSDataType.INT64), ("s2", TSDataType.DOUBLE)]},
            num_rows=10,
        )

        with TsFileDataFrame(str(fpath), show_progress=False) as df:
            result = df.loc[2:5, ["root.d1.s1", "root.d1.s2"]]
            assert len(result) > 0
            assert all(t >= 2 and t <= 5 for t in result.timestamps)

    def test_repr(self, tmp_path):
        fpath = tmp_path / "tree_repr.tsfile"
        _write_tree_file(
            fpath,
            {"root.d1": [("s1", TSDataType.INT64)]},
        )

        with TsFileDataFrame(str(fpath), show_progress=False) as df:
            r = repr(df)
            assert "TsFileDataFrame" in r
            assert "root" in r


class TestTreeModelMultiDevice:
    def test_different_depths(self, tmp_path):
        fpath = tmp_path / "tree_depth.tsfile"
        _write_tree_file(
            fpath,
            {
                "root.d1": [("s1", TSDataType.INT64)],
                "root.sg1.d2": [("s1", TSDataType.INT64)],
            },
        )

        with TsFileDataFrame(str(fpath), show_progress=False) as df:
            series = df.list_timeseries()
            assert "root.d1.s1" in series
            assert "root.sg1.d2.s1" in series

            ts1 = df["root.d1.s1"]
            ts2 = df["root.sg1.d2.s1"]
            assert len(ts1) == 10
            assert len(ts2) == 10

    def test_different_measurements(self, tmp_path):
        fpath = tmp_path / "tree_diff_meas.tsfile"
        _write_tree_file(
            fpath,
            {
                "root.d1": [("s1", TSDataType.INT64), ("s2", TSDataType.DOUBLE)],
                "root.d2": [("s2", TSDataType.DOUBLE), ("s3", TSDataType.INT64)],
            },
        )

        with TsFileDataFrame(str(fpath), show_progress=False) as df:
            series = df.list_timeseries()
            assert "root.d1.s1" in series
            assert "root.d1.s2" in series
            assert "root.d2.s2" in series
            assert "root.d2.s3" in series
            # Union fields: s1, s2, s3 for each device = 6 total series
            # root.d1.s3 (count=0) and root.d2.s1 (count=0) should also exist
            assert "root.d1.s3" in series
            assert "root.d2.s1" in series
            assert len(df) == 6

            ts_empty = df["root.d1.s3"]
            assert len(ts_empty) == 0

            ts_real = df["root.d1.s1"]
            assert len(ts_real) == 10

    def test_prefix_filter(self, tmp_path):
        fpath = tmp_path / "tree_prefix.tsfile"
        _write_tree_file(
            fpath,
            {
                "root.d1": [("s1", TSDataType.INT64)],
                "root.d2": [("s1", TSDataType.INT64)],
            },
        )

        with TsFileDataFrame(str(fpath), show_progress=False) as df:
            d1_series = df.list_timeseries("root.d1")
            assert len(d1_series) == 1
            assert d1_series[0] == "root.d1.s1"


class TestMixedDirectory:
    def test_tree_and_table_files(self, tmp_path):
        tree_path = tmp_path / "tree.tsfile"
        table_path = tmp_path / "table.tsfile"

        _write_tree_file(
            tree_path,
            {"root.d1": [("s1", TSDataType.INT64)]},
            num_rows=5,
        )
        _write_table_file(table_path, start=0, num_rows=5)

        with TsFileDataFrame(str(tmp_path), show_progress=False) as df:
            series = df.list_timeseries()
            has_tree = any("root.d1.s1" in s for s in series)
            has_table = any("sensors" in s for s in series)
            assert has_tree
            assert has_table


class TestTreeModelMultiShard:
    def test_cross_shard_merge(self, tmp_path):
        f1 = tmp_path / "shard1.tsfile"
        f2 = tmp_path / "shard2.tsfile"

        writer1 = TsFileWriter(str(f1))
        writer1.register_timeseries("root.d1", TimeseriesSchema("s1", TSDataType.INT64))
        for t in range(5):
            writer1.write_row_record(
                RowRecord("root.d1", t, [Field("s1", t * 10, TSDataType.INT64)])
            )
        writer1.close()

        writer2 = TsFileWriter(str(f2))
        writer2.register_timeseries("root.d1", TimeseriesSchema("s1", TSDataType.INT64))
        for t in range(5, 10):
            writer2.write_row_record(
                RowRecord("root.d1", t, [Field("s1", t * 10, TSDataType.INT64)])
            )
        writer2.close()

        with TsFileDataFrame(str(tmp_path), show_progress=False) as df:
            ts = df["root.d1.s1"]
            assert len(ts) == 10
            assert ts[0] == 0.0
            assert ts[9] == 90.0

    def test_cross_shard_duplicate_error(self, tmp_path):
        f1 = tmp_path / "dup1.tsfile"
        f2 = tmp_path / "dup2.tsfile"

        writer1 = TsFileWriter(str(f1))
        writer1.register_timeseries("root.d1", TimeseriesSchema("s1", TSDataType.INT64))
        writer1.write_row_record(
            RowRecord("root.d1", 0, [Field("s1", 100, TSDataType.INT64)])
        )
        writer1.close()

        writer2 = TsFileWriter(str(f2))
        writer2.register_timeseries("root.d1", TimeseriesSchema("s1", TSDataType.INT64))
        writer2.write_row_record(
            RowRecord("root.d1", 0, [Field("s1", 200, TSDataType.INT64)])
        )
        writer2.close()

        with TsFileDataFrame(str(tmp_path), show_progress=False) as df:
            ts = df["root.d1.s1"]
            with pytest.raises(ValueError, match="Duplicate timestamp"):
                _ = ts.timestamps


class TestTreeModelNumericFilter:
    def test_text_measurements_filtered(self, tmp_path):
        fpath = tmp_path / "tree_text.tsfile"
        writer = TsFileWriter(str(fpath))
        writer.register_timeseries(
            "root.d1", TimeseriesSchema("numeric_s", TSDataType.INT64)
        )
        writer.register_timeseries(
            "root.d1", TimeseriesSchema("text_s", TSDataType.STRING)
        )

        for t in range(5):
            writer.write_row_record(
                RowRecord(
                    "root.d1",
                    t,
                    [
                        Field("numeric_s", t * 10, TSDataType.INT64),
                        Field("text_s", f"val_{t}", TSDataType.STRING),
                    ],
                )
            )
        writer.close()

        with TsFileDataFrame(str(fpath), show_progress=False) as df:
            series = df.list_timeseries()
            assert "root.d1.numeric_s" in series
            assert "root.d1.text_s" not in series
            assert len(df) == 1
