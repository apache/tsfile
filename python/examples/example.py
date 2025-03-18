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

from tsfile import ColumnSchema, TableSchema
from tsfile import Tablet
from tsfile import TsFileTableWriter, TsFileReader, TSDataType, TSEncoding, Compressor, ColumnCategory

##  Write
table_data_dir = os.path.join(os.path.dirname(__file__), "table_data.tsfile")
if os.path.exists(table_data_dir):
    os.remove(table_data_dir)

column1 = ColumnSchema("id", TSDataType.STRING, ColumnCategory.TAG)
column2 = ColumnSchema("id2", TSDataType.STRING, ColumnCategory.TAG)
column3 = ColumnSchema("value", TSDataType.FLOAT, ColumnCategory.FIELD)
table_schema = TableSchema("test_table", columns=[column1, column2, column3])


### Free resource automatically
with TsFileTableWriter(table_data_dir, table_schema) as writer:
    tablet_row_num = 100
    tablet = Tablet(
                    ["id", "id2", "value"],
                    [TSDataType.STRING, TSDataType.STRING, TSDataType.FLOAT],
                    tablet_row_num)

    for i in range(tablet_row_num):
        tablet.add_timestamp(i, i * 10)
        tablet.add_value_by_name("id", i, "test1")
        tablet.add_value_by_name("id2", i, "test" + str(i))
        tablet.add_value_by_index(2, i, i * 100.2)

    writer.write_table(tablet)

##  Read

### Free resource automatically
with TsFileReader(table_data_dir) as reader:
    with reader.query_table("test_table", ["id2", "value"], 0, 50) as result:
        while result.next():
            print(result.get_value_by_name("id2"))
            print(result.get_value_by_name("value"))
            print(result.read_data_frame())

