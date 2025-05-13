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

import pytest

from tsfile import ColumnSchema, TableSchema, TSEncoding, NotSupportedError
from tsfile import TSDataType
from tsfile import Tablet, RowRecord, Field
from tsfile import TimeseriesSchema
from tsfile import TsFileTableWriter
from tsfile import TsFileWriter, TsFileReader, ColumnCategory
from tsfile import Compressor


def test_row_record_write_and_read():
    try:
        writer = TsFileWriter("record_write_and_read.tsfile")
        timeseries = TimeseriesSchema("level1", TSDataType.INT64)
        writer.register_timeseries("root.device1", timeseries)
        writer.register_timeseries("root.device1", TimeseriesSchema("level2", TSDataType.DOUBLE))
        writer.register_timeseries("root.device1", TimeseriesSchema("level3", TSDataType.INT32))

        max_row_num = 1000
        for i in range(max_row_num):
            row = RowRecord("root.device1", i,
                            [Field("level1", i + 1, TSDataType.INT64),
                             Field("level2", i * 1.1, TSDataType.DOUBLE),
                             Field("level3", i * 2, TSDataType.INT32)])
            writer.write_row_record(row)

        writer.close()

        reader = TsFileReader("record_write_and_read.tsfile")
        result = reader.query_timeseries("root.device1", ["level1", "level2"], 10, 100)
        i = 10
        while result.next():
            print(result.get_value_by_index(1))
        print(reader.get_active_query_result())
        result.close()
        print(reader.get_active_query_result())
        reader.close()
    finally:
        if os.path.exists("record_write_and_read.tsfile"):
            os.remove("record_write_and_read.tsfile")

@pytest.mark.skip(reason="API not match")
def test_tablet_write_and_read():
    try:
        if os.path.exists("record_write_and_read.tsfile"):
            os.remove("record_write_and_read.tsfile")
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
            # Here, the data retrieval uses the table model's API,
            # which might be incompatible. Therefore, it is better to skip it for now.
            assert result.get_value_by_name("level0") == row_num
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
