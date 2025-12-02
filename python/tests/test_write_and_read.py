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
from pandas.core.dtypes.common import is_integer_dtype

from tsfile import ColumnSchema, TableSchema, TSEncoding
from tsfile import Compressor
from tsfile import TSDataType
from tsfile import Tablet, RowRecord, Field
from tsfile import TimeseriesSchema
from tsfile import TsFileTableWriter
from tsfile import TsFileWriter, TsFileReader, ColumnCategory
from tsfile import to_dataframe
from tsfile.exceptions import TableNotExistError, ColumnNotExistError, NotSupportedError


def test_row_record_write_and_read():
    try:
        if os.path.exists("record_write_and_read.tsfile"):
            os.remove("record_write_and_read.tsfile")
        writer = TsFileWriter("record_write_and_read.tsfile")
        writer.register_timeseries("root.device1", TimeseriesSchema("level1", TSDataType.INT64))
        writer.register_timeseries("root.device1", TimeseriesSchema("level2", TSDataType.DOUBLE))
        writer.register_timeseries("root.device1", TimeseriesSchema("level3", TSDataType.INT32))
        writer.register_timeseries("root.device1", TimeseriesSchema("level4", TSDataType.STRING))
        writer.register_timeseries("root.device1", TimeseriesSchema("level5", TSDataType.TEXT))
        writer.register_timeseries("root.device1", TimeseriesSchema("level6", TSDataType.BLOB))
        writer.register_timeseries("root.device1", TimeseriesSchema("level7", TSDataType.DATE))

        max_row_num = 10

        for i in range(max_row_num):
            row = RowRecord("root.device1", i,
                            [Field("level1", i + 1, TSDataType.INT64),
                             Field("level2", i * 1.1, TSDataType.DOUBLE),
                             Field("level3", i * 2, TSDataType.INT32),
                             Field("level4", f"string_value_{i}", TSDataType.STRING),
                             Field("level5", f"text_value_{i}", TSDataType.TEXT),
                             Field("level6", f"blob_data_{i}".encode('utf-8'), TSDataType.BLOB),
                             Field("level7", i, TSDataType.DATE)])
            writer.write_row_record(row)

        writer.close()

        reader = TsFileReader("record_write_and_read.tsfile")
        result = reader.query_timeseries(
            "root.device1",
            ["level1", "level2", "level3", "level4", "level5", "level6", "level7"],
            0,
            100,
        )
        assert len(reader.get_active_query_result()) == 1

        for row_num in range(max_row_num):
            assert result.next()
            assert result.get_value_by_index(1) == row_num
            assert result.get_value_by_index(2) == row_num + 1
            assert result.get_value_by_index(3) == pytest.approx(row_num * 1.1)
            assert result.get_value_by_index(4) == row_num * 2
            assert result.get_value_by_index(5) == f"string_value_{row_num}"
            assert result.get_value_by_index(6) == f"text_value_{row_num}"
            assert result.get_value_by_index(7) == f"blob_data_{row_num}"
            assert result.get_value_by_index(8) == row_num

        assert not result.next()
        assert len(reader.get_active_query_result()) == 1
        result.close()
        print(reader.get_active_query_result())
        assert len(reader.get_active_query_result()) == 0
        reader.close()



    finally:
        if os.path.exists("record_write_and_read.tsfile"):
            os.remove("record_write_and_read.tsfile")

def test_tree_query_to_dataframe_variants():
    file_path = "tree_query_to_dataframe.tsfile"
    device_ids = [
        "root.db1.t1",
        "root.db2.t1",
        "root.db3.t2.t3",
        "root.db3.t3",
        "device",
        "device.ln",
        "device2.ln1.tmp",
        "device3.ln2.tmp.v1.v2",
        "device3.ln2.tmp.v1.v3",
    ]
    device_path_map = [
        "root.db1.t1.null.null",
        "root.db2.t1.null.null",
        "root.db3.t2.t3.null",
        "root.db3.t3.null.null",
        "device.null.null.null.null",
        "device.ln.null.null.null",
        "device2.ln1.tmp.null.null",
        "device3.ln2.tmp.v1.v2",
        "device3.ln2.tmp.v1.v3",
    ]
    measurement_ids1 = ["temperature", "hudi", "level"]
    measurement_ids2 = ["level", "vol"]
    rows_per_device = 2
    expected_values = {}
    all_measurements = set()

    def _is_null(value):
        return value is None or pd.isna(value)

    def _extract_device(row, path_columns):
        parts = []
        for col in path_columns:
            value = row[col]
            if not _is_null(value):
                parts.append(str(value))
            else:
                parts.append("null")
        return ".".join(parts)

    try:
        writer = TsFileWriter(file_path)
        for idx, device_id in enumerate(device_ids):
            measurements = measurement_ids1 if idx % 2 == 0 else measurement_ids2
            all_measurements.update(measurements)
            for measurement in measurements:
                writer.register_timeseries(
                    device_id, TimeseriesSchema(measurement, TSDataType.INT32)
                )
            for ts in range(rows_per_device):
                fields = []
                measurement_snapshot = {}
                for m_idx, measurement in enumerate(measurements):
                    value = idx * 100 + ts * 10 + m_idx
                    fields.append(Field(measurement, value, TSDataType.INT32))
                    measurement_snapshot[measurement] = value
                writer.write_row_record(RowRecord(device_id, ts, fields))
                expected_values[(device_path_map[idx], ts)] = measurement_snapshot
        writer.close()

        df_all = to_dataframe(file_path, start_time=0, end_time=rows_per_device)
        print(df_all)
        total_rows = len(device_ids) * rows_per_device
        assert df_all.shape[0] == total_rows
        for measurement in all_measurements:
            assert measurement in df_all.columns
        assert "time" in df_all.columns
        path_columns = sorted(
            [col for col in df_all.columns if col.startswith("col_")],
            key=lambda name: int(name.split("_")[1]),
        )
        assert len(path_columns) > 0

        for _, row in df_all.iterrows():
            device = _extract_device(row, path_columns)
            timestamp = int(row["time"])
            assert (device, timestamp) in expected_values
            expected_row = expected_values[(device, timestamp)]
            for measurement in all_measurements:
                value = row.get(measurement)
                if measurement in expected_row:
                    assert value == expected_row[measurement]
                else:
                    assert _is_null(value)
            assert device in device_path_map

        requested_columns = ["level", "temperature"]
        df_subset = to_dataframe(
            file_path, column_names=requested_columns, start_time=0, end_time=rows_per_device
        )
        for column in requested_columns:
            assert column in df_subset.columns
        for measurement in all_measurements:
            if measurement not in requested_columns:
                assert measurement not in df_subset.columns
        for _, row in df_subset.iterrows():
            device = _extract_device(row, path_columns)
            timestamp = int(row["time"])
            expected_row = expected_values[(device, timestamp)]
            for measurement in requested_columns:
                value = row.get(measurement)
                if measurement in expected_row:
                    assert value == expected_row[measurement]
                else:
                    assert _is_null(value)
            assert device in device_path_map
        df_limited = to_dataframe(
            file_path, column_names=["level"], max_row_num=5, start_time=0, end_time=rows_per_device
        )
        assert df_limited.shape[0] == 5
        assert "level" in df_limited.columns

        iterator = to_dataframe(
            file_path,
            column_names=["level", "temperature"],
            max_row_num=3,
            start_time=0,
            end_time=rows_per_device,
            as_iterator=True,
        )
        iter_rows = 0
        for batch in iterator:
            assert isinstance(batch, pd.DataFrame)
            assert set(batch.columns).issuperset({"time", "level"})
            iter_rows += len(batch)
            print(batch)
        assert iter_rows == 18

        iterator = to_dataframe(
            file_path,
            column_names=["level", "temperature"],
            max_row_num=3,
            start_time=0,
            end_time=0,
            as_iterator=True,
        )
        iter_rows = 0
        for batch in iterator:
            assert isinstance(batch, pd.DataFrame)
            assert set(batch.columns).issuperset({"time", "level"})
            iter_rows += len(batch)
            print(batch)
        assert iter_rows == 9

        with pytest.raises(ColumnNotExistError):
            to_dataframe(file_path, column_names=["level", "not_exists"])
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

def test_get_all_timeseries_schemas():
    file_path = "get_all_timeseries_schema.tsfile"
    device_ids = [
        "root.db1.t1",
        "root.db2.t1",
        "root.db3.t2.t3",
        "root.db3.t3",
        "device",
        "device.ln",
        "device2.ln1.tmp",
        "device3.ln2.tmp.v1.v2",
        "device3.ln2.tmp.v1.v3",
    ]
    measurement_ids1 = ["temperature", "hudi", "level"]
    measurement_ids2 = ["level", "vol"]
    rows_per_device = 2

    try:
        writer = TsFileWriter(file_path)
        for idx, device_id in enumerate(device_ids):
            measurements = measurement_ids1 if idx % 2 == 0 else measurement_ids2
            for measurement in measurements:
                writer.register_timeseries(
                    device_id, TimeseriesSchema(measurement, TSDataType.INT32)
                )
            for ts in range(rows_per_device):
                fields = []
                for measurement in measurements:
                    fields.append(
                        Field(
                            measurement,
                            idx * 100 + ts * 10 + len(fields),
                            TSDataType.INT32,
                        )
                    )
                writer.write_row_record(RowRecord(device_id, ts, fields))
        writer.close()

        reader = TsFileReader(file_path)
        device_schema_map = reader.get_all_timeseries_schemas()
        expected_devices = {device_id.lower() for device_id in device_ids}
        assert set(device_schema_map.keys()) == expected_devices
        print(device_schema_map)

        for idx, device_id in enumerate(device_ids):
            measurements = measurement_ids1 if idx % 2 == 0 else measurement_ids2
            normalized_device = device_id.lower()
            assert normalized_device in device_schema_map
            device_schema = device_schema_map[normalized_device]
            assert device_schema.get_device_name() == normalized_device
            timeseries_list = device_schema.get_timeseries_list()
            assert len(timeseries_list) == len(measurements)
            actual_measurements = {
                ts_schema.get_timeseries_name() for ts_schema in timeseries_list
            }
            assert actual_measurements == {m.lower() for m in measurements}
            for ts_schema in timeseries_list:
                assert ts_schema.get_data_type() == TSDataType.INT32
        reader.close()
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)

def test_tablet_write_and_read():
    try:
        if os.path.exists("tablet_write_and_read.tsfile"):
            os.remove("tablet_write_and_read.tsfile")
        writer = TsFileWriter("tablet_write_and_read.tsfile")
        measurement_num = 30
        for i in range(measurement_num):
            writer.register_timeseries("root.device1", TimeseriesSchema('level' + str(i), TSDataType.INT64))

        max_row_num = 10000
        tablet_row_num = 1000
        tablet_num = 0
        for i in range(max_row_num // tablet_row_num):
            tablet = Tablet([f'level{j}' for j in range(measurement_num)],
                            [TSDataType.INT64 for _ in range(measurement_num)], tablet_row_num)
            tablet.set_table_name("root.device1")
            for row in range(tablet_row_num):
                tablet.add_timestamp(row, row + tablet_num * tablet_row_num)
                for col in range(measurement_num):
                    tablet.add_value_by_index(col, row, row + tablet_num * tablet_row_num)
            writer.write_tablet(tablet)
            tablet_num += 1

        writer.close()

        reader = TsFileReader("tablet_write_and_read.tsfile")
        result = reader.query_timeseries("root.device1", ["level0"], 0, 1000000)
        row_num = 0
        print(result.get_result_column_info())
        while result.next():
            assert result.is_null_by_index(1) == False
            assert result.get_value_by_index(1) == row_num
            assert result.get_value_by_name("level0") == row_num
            assert result.get_value_by_index(2) == row_num
            row_num = row_num + 1

        assert row_num == max_row_num
        reader.close()
        with pytest.raises(Exception):
            result.next()

    finally:
        if os.path.exists("tablet_write_and_read.tsfile"):
            os.remove("tablet_write_and_read.tsfile")

def test_table_writer_and_reader():
    table = TableSchema("test_table",
                        [ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD)])
    try:
        if os.path.exists("table_write.tsfile"):
            os.remove("table_write.tsfile")
        with TsFileTableWriter("table_write.tsfile", table) as writer:
            tablet = Tablet(["device", "value"],
                            [TSDataType.STRING, TSDataType.DOUBLE], 100)
            for i in range(100):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("device", i, "device" + str(i))
                tablet.add_value_by_index(1, i, i * 100.0)
            writer.write_table(tablet)

        with TsFileReader("table_write.tsfile") as reader:
            with reader.query_table("test_table", ["device", "value"],
                                    0, 10) as result:
                cur_line = 0
                while result.next():
                    cur_time = result.get_value_by_name("time")
                    assert result.get_value_by_name("device") == "device" + str(cur_time)
                    assert result.is_null_by_name("device") == False
                    assert result.is_null_by_name("value") == False
                    assert result.is_null_by_index(1) == False
                    assert result.is_null_by_index(2) == False
                    assert result.is_null_by_index(3) == False
                    assert result.get_value_by_name("value") == cur_time * 100.0
                    cur_line = cur_line + 1
                assert cur_line == 11
            with reader.query_table("test_table", ["device", "value"],
                                    0, 100) as result:
                line_num = 0
                print("dataframe")
                while result.next():
                    data_frame = result.read_data_frame(max_row_num=30)
                    if 100 - line_num >= 30:
                        assert data_frame.shape == (30, 3)
                    else:
                        assert data_frame.shape == (100 - line_num, 3)
                    line_num += len(data_frame)

            schemas = reader.get_all_table_schemas()
            assert len(schemas) == 1
            assert schemas["test_table"] is not None
            tableSchema = schemas["test_table"]
            assert tableSchema.get_table_name() == "test_table"
            print(tableSchema)
            assert tableSchema.__repr__() == ("TableSchema(test_table, [ColumnSchema(device,"
                                              " STRING, TAG), ColumnSchema(value, DOUBLE, FIELD)])")
    finally:
        if os.path.exists("table_write.tsfile"):
            os.remove("table_write.tsfile")

def test_query_result_detach_from_reader():
    try:
        ## Prepare data
        writer = TsFileWriter("query_result_detach_from_reader.tsfile")
        timeseries = TimeseriesSchema("level1", TSDataType.INT64)
        writer.register_timeseries("root.device1", timeseries)
        max_row_num = 1000
        for i in range(max_row_num):
            row = RowRecord("root.device1", i,
                            [Field("level1", i, TSDataType.INT64)])
            writer.write_row_record(row)

        writer.close()

        reader = TsFileReader("query_result_detach_from_reader.tsfile")
        result1 = reader.query_timeseries("root.device1", ["level1"], 0, 100)
        assert 1 == len(reader.get_active_query_result())
        result2 = reader.query_timeseries("root.device1", ["level1"], 20, 100)
        assert 2 == len(reader.get_active_query_result())
        result1.close()
        assert 1 == len(reader.get_active_query_result())
        reader.close()
        with pytest.raises(Exception):
            result1.next()
        with pytest.raises(Exception):
            result2.next()
    finally:
        if os.path.exists("query_result_detach_from_reader.tsfile"):
            os.remove("query_result_detach_from_reader.tsfile")

def test_lower_case_name():
    if os.path.exists("lower_case_name.tsfile"):
        os.remove("lower_case_name.tsfile")
    table = TableSchema("tEst_Table",
                        [ColumnSchema("Device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("vAlue", TSDataType.DOUBLE, ColumnCategory.FIELD)])
    with TsFileTableWriter("lower_case_name.tsfile", table) as writer:
        tablet = Tablet(["device", "VALUE"], [TSDataType.STRING, TSDataType.DOUBLE])
        for i in range(100):
            tablet.add_timestamp(i, i)
            tablet.add_value_by_name("device", i, "device" + str(i))
            tablet.add_value_by_name("valuE", i, i * 1.1)

        writer.write_table(tablet)

    with TsFileReader("lower_case_name.tsfile") as reader:
        result = reader.query_table("test_Table", ["DEvice", "value"], 0, 100)
        while result.next():
            print(result.get_value_by_name("DEVICE"))
            data_frame = result.read_data_frame(max_row_num=130)
            assert data_frame.shape == (100, 3)
            assert data_frame["value"].sum() == 5445.0

def test_tsfile_config():
    from tsfile import get_tsfile_config, set_tsfile_config

    config = get_tsfile_config()

    table = TableSchema("tEst_Table",
                        [ColumnSchema("Device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("vAlue", TSDataType.DOUBLE, ColumnCategory.FIELD)])
    if os.path.exists("test1.tsfile"):
        os.remove("test1.tsfile")
    with TsFileTableWriter("test1.tsfile", table) as writer:
        tablet = Tablet(["device", "VALUE"], [TSDataType.STRING, TSDataType.DOUBLE])
        for i in range(100):
            tablet.add_timestamp(i, i)
            tablet.add_value_by_name("device", i, "device" + str(i))
            tablet.add_value_by_name("valuE", i, i * 1.1)

        writer.write_table(tablet)

    config_normal = get_tsfile_config()
    print(config_normal)
    assert config_normal["chunk_group_size_threshold_"] == 128 * 1024 * 1024

    os.remove("test1.tsfile")
    with TsFileTableWriter("test1.tsfile", table, 100 * 100) as writer:
        tablet = Tablet(["device", "VALUE"], [TSDataType.STRING, TSDataType.DOUBLE])
        for i in range(100):
            tablet.add_timestamp(i, i)
            tablet.add_value_by_name("device", i, "device" + str(i))
            tablet.add_value_by_name("valuE", i, i * 1.1)

        writer.write_table(tablet)
    config_modified = get_tsfile_config()
    assert config_normal != config_modified
    assert config_modified["chunk_group_size_threshold_"] == 100 * 100
    set_tsfile_config({'chunk_group_size_threshold_': 100 * 20})
    assert get_tsfile_config()["chunk_group_size_threshold_"] == 100 * 20
    with pytest.raises(TypeError):
        set_tsfile_config({"time_compress_type_": TSDataType.DOUBLE})
    with pytest.raises(TypeError):
        set_tsfile_config({'chunk_group_size_threshold_': -1 * 100 * 20})

    set_tsfile_config({'float_encoding_type_': TSEncoding.PLAIN})
    assert get_tsfile_config()["float_encoding_type_"] == TSEncoding.PLAIN

    with pytest.raises(TypeError):
        set_tsfile_config({"float_encoding_type_": -1 * 100 * 20})
    with pytest.raises(NotSupportedError):
        set_tsfile_config({"float_encoding_type_": TSEncoding.BITMAP})
    with pytest.raises(NotSupportedError):
        set_tsfile_config({"time_compress_type_": Compressor.PAA})

def test_tsfile_to_df():
    table = TableSchema("test_table",
                        [ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                         ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
                         ColumnSchema("value2", TSDataType.INT64, ColumnCategory.FIELD)])
    try:
        with TsFileTableWriter("table_write_to_df.tsfile", table) as writer:
            tablet = Tablet(["device", "value", "value2"],
                            [TSDataType.STRING, TSDataType.DOUBLE, TSDataType.INT64], 4097)
            for i in range(4097):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("device", i, "device" + str(i))
                tablet.add_value_by_index(1, i, i * 100.0)
                tablet.add_value_by_index(2, i, i * 100)
            writer.write_table(tablet)
        df1 = to_dataframe("table_write_to_df.tsfile")
        assert df1.shape == (4097, 4)
        assert df1["value2"].sum() == 100 * (1 + 4096) / 2 * 4096
        assert is_integer_dtype(df1["time"])
        assert df1["value"].dtype == np.float64
        assert is_integer_dtype(df1["value2"])
        df2 = to_dataframe("table_write_to_df.tsfile", column_names=["device", "value2"])
        assert df2.shape == (4097, 3)
        assert df1["value2"].equals(df2["value2"])
        df3 = to_dataframe("table_write_to_df.tsfile", column_names=["device", "value"], max_row_num=8000)
        assert df3.shape == (4097, 3)
        with pytest.raises(TableNotExistError):
            to_dataframe("table_write_to_df.tsfile", "test_tb")
        with pytest.raises(ColumnNotExistError):
            to_dataframe("table_write_to_df.tsfile", "test_table", ["device1"])
    finally:
        os.remove("table_write_to_df.tsfile")


import os

if __name__ == "__main__":
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    pytest.main([
        "test_write_and_read.py::test_row_record_write_and_read",
        "-s", "-v"
    ])
