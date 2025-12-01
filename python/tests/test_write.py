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

from tsfile import TsFileWriter, TimeseriesSchema, DeviceSchema, ColumnCategory
from tsfile import ColumnSchema, TableSchema
from tsfile import Tablet, RowRecord, Field
from tsfile import TSDataType

def test_row_record_write():
    try:
        writer = TsFileWriter("record_write.tsfile")
        timeseries = TimeseriesSchema("level1", TSDataType.INT64)
        writer.register_timeseries("root.device1", timeseries)

        record = RowRecord("root.device1", 10,[Field("level1", 10, TSDataType.INT64)])
        writer.write_row_record(record)
        writer.close()
    finally:
        if os.path.exists("record_write.tsfile"):
            os.remove("record_write.tsfile")

def test_tablet_write():
    try:
        writer = TsFileWriter("tablet_write.tsfile")
        timeseries1 = TimeseriesSchema("level1", TSDataType.INT64)
        timeseries2 = TimeseriesSchema("level2", TSDataType.DOUBLE)
        device = DeviceSchema("root.device1", [timeseries1, timeseries2])
        writer.register_device(device)

        tablet = Tablet(["level1", "level2"], [TSDataType.INT64, TSDataType.DOUBLE], 100)
        tablet.set_table_name("root.device1")
        for i in range(100):
            tablet.add_timestamp(i, i)
            tablet.add_value_by_index(0, i, i + 1)
            tablet.add_value_by_name("level2", i, i * 0.1)

        writer.write_tablet(tablet)
        writer.close()
    finally:
        if os.path.exists("tablet_write.tsfile"):
            os.remove("tablet_write.tsfile")

def test_tablet_write():
    try:
        writer = TsFileWriter("tablet_write.tsfile")
        timeseries1 = TimeseriesSchema("level1", TSDataType.INT64)
        timeseries2 = TimeseriesSchema("level2", TSDataType.DOUBLE)
        device = DeviceSchema("root.device1", [timeseries1, timeseries2])
        writer.register_device(device)

        tablet = Tablet(["level1", "level2"], [TSDataType.INT64, TSDataType.DOUBLE], 100)
        tablet.set_table_name("root.device1")
        for i in range(100):
            tablet.add_timestamp(i, i)
            tablet.add_value_by_index(0, i, i + 1)
            tablet.add_value_by_name("level2", i, i * 0.1)

        writer.write_tablet(tablet)
        writer.close()
    finally:
        if os.path.exists("tablet_write.tsfile"):
            os.remove("tablet_write.tsfile")

def test_table_write():
    try:
        with TsFileWriter("table_write.tsfile") as writer:
            column1 = ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG)
            column2 = ColumnSchema("sensor", TSDataType.STRING, ColumnCategory.TAG)
            column3 = ColumnSchema("value1", TSDataType.DOUBLE)
            column4 = ColumnSchema("value2", TSDataType.INT32, ColumnCategory.FIELD)
            table = TableSchema("test_table", [column1, column2, column3, column4])
            writer.register_table(table)
            row_num = 100

            tablet = Tablet( ["device", "sensor", "value1", "value2"],
                            [TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE, TSDataType.INT32],
                            row_num)
            tablet.set_table_name("test_table")
            for i in range(100):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("device", i, "device" + str(i))
                tablet.add_value_by_name("sensor", i, "sensor" + str(i))
                tablet.add_value_by_name("value1", i, i * 10.1)
                tablet.add_value_by_index(3, i, 1 * 100)

            writer.write_table(tablet)
    finally:
        if os.path.exists("table_write.tsfile"):
            os.remove("table_write.tsfile")

def test_flush():
    file_name = "table_flush.tsfile"
    try:
        with TsFileWriter(file_name) as writer:
            origin_size = os.path.getsize(file_name)
            column1 = ColumnSchema("item_id", TSDataType.STRING, ColumnCategory.TAG)
            column2 = ColumnSchema("value", TSDataType.DOUBLE)
            table = TableSchema("test_flush", [column1, column2])
            writer.register_table(table)
            row_num = 100

            tablet = Tablet(["item_id", "value"],
                            [TSDataType.STRING, TSDataType.DOUBLE],
                            row_num)
            tablet.set_table_name("test_flush")
            for i in range(100):
                tablet.add_timestamp(i, i)
                tablet.add_value_by_name("item_id", i, str(i))
                tablet.add_value_by_name("value", i, i * 10.1)
            writer.write_table(tablet)
            assert os.path.getsize(file_name) == origin_size
            writer.flush()
            assert os.path.getsize(file_name) > origin_size
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)









