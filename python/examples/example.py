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

import os

from tsfile import DeviceSchema, TimeseriesSchema, ColumnSchema, TableSchema, RowRecord, Field
from tsfile import Tablet
from tsfile import TsFileWriter, TsFileReader, TSDataType, TSEncoding, Compressor, ColumnCategory

## tsfile path.
reader_data_dir = os.path.join(os.path.dirname(__file__), "tree_model.tsfile")
if os.path.exists(reader_data_dir):
    os.remove(reader_data_dir)

## Tree Model Write Data

DEVICE_NAME = "root.device"

writer = TsFileWriter(reader_data_dir)

timeseries = TimeseriesSchema("temp1", TSDataType.INT32, TSEncoding.PLAIN, Compressor.UNCOMPRESSED)
timeseries2 = TimeseriesSchema("temp2", TSDataType.INT64)
timeseries3 = TimeseriesSchema("level1", TSDataType.BOOLEAN)

### register timeseries
writer.register_timeseries(DEVICE_NAME, timeseries)

### register device
device = DeviceSchema(DEVICE_NAME, [timeseries2, timeseries3])
writer.register_device(device)

### Write data with row record
row_num = 10
for i in range(row_num):
    row_record = RowRecord(DEVICE_NAME, i + 1,
                           [Field("temp1",i, TSDataType.INT32),
                            Field("temp2", i, TSDataType.INT64)])
    writer.write_row_record(row_record)

### Flush data and close writer.
writer.close()

## Tree Model Read Data

reader = TsFileReader(reader_data_dir)

### Query device with specify time scope
result = reader.query_timeseries(DEVICE_NAME, ["temp1", "temp2"], 0, 100)

### Get result list data types
sensor_info_list = result.get_result_column_info()
print(sensor_info_list)

### Print data
while result.next():
    print(result.get_value_by_name("temp1"))
    print(result.get_value_by_index(1))
result.close()

### Get query result which can free automatically

with reader.query_timeseries(DEVICE_NAME, ["temp1"], 0, 100) as result:
    while result.next():
        print(result.get_value_by_name("temp1"))

reader.close()

## Table Model Write and Read
table_data_dir = os.path.join(os.path.dirname(__file__), "table_model.tsfile")
if os.path.exists(table_data_dir):
    os.remove(table_data_dir)

column1 = ColumnSchema("id", TSDataType.STRING, ColumnCategory.TAG)
column2 = ColumnSchema("id2", TSDataType.STRING, ColumnCategory.TAG)
column3 = ColumnSchema("value", TSDataType.FLOAT, ColumnCategory.FIELD)

### Free resource automatically
with TsFileWriter(table_data_dir) as writer:
    writer.register_table(TableSchema("test_table", [column1, column2, column3]))
    tablet_row_num = 100
    tablet = Tablet("test_table",
                    ["id1", "id2", "value"],
                    [TSDataType.STRING, TSDataType.STRING, TSDataType.FLOAT],
                    [ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD],
                    tablet_row_num)

    for i in range(tablet_row_num):
        tablet.add_timestamp(i, i * 10)
        tablet.add_value_by_name("id1", i, "test1")
        tablet.add_value_by_name("id2", i, "test" + str(i))
        tablet.add_value_by_index(2, i, i * 100.2)

    writer.write_table(tablet)

### Read table data from tsfile reader.
# with TsFileReader(table_data_dir) as reader:
#     with reader.query_table("test_table", ["id2", "value"], 0, 50) as result:
#         while result.next():
#             print(result.get_value_by_name("id2"))
#             print(result.get_value_by_name("value"))
