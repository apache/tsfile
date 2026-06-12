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

from tsfile import ColumnCategory, ColumnSchema, Tablet
from tsfile import TableSchema, TSDataType, TsFileReader, TsFileTableWriter

from conftest import (
    FLUSH_ROWS,
    MIN_ACCEPTABLE_FILE_SIZE,
    START_TIME,
    TABLET_ROWS,
    TARGET_FILE_SIZE,
    get_file_size,
    random_suffix,
)

TABLE_NAME = "large_table"
DEVICE_TAG = "device"
VALUE_FIELD = "value"


def _fill_table_tablet(tablet: Tablet, total_rows: int) -> None:
    row_range = range(TABLET_ROWS)
    tablet.timestamp_list[:TABLET_ROWS] = [
        START_TIME + (total_rows + row) * 1000 for row in row_range
    ]
    tablet.data_list[0][:TABLET_ROWS] = ["device0"] * TABLET_ROWS
    tablet.data_list[1][:TABLET_ROWS] = [total_rows + row for row in row_range]


def _verify_table_record(reader: TsFileReader, record_index: int) -> bool:
    timestamp = START_TIME + record_index * 1000
    with reader.query_table(
        TABLE_NAME, [VALUE_FIELD], timestamp, timestamp + 1
    ) as result:
        if not result.next():
            return False
        return (
            result.get_value_by_index(1) == timestamp
            and result.get_value_by_index(2) == record_index
        )


def test_large_file_4gb_table_write_and_read():
    file_name = f"large_file_table_test_{random_suffix()}.tsfile"
    if os.path.exists(file_name):
        os.remove(file_name)

    schema = TableSchema(
        TABLE_NAME,
        [
            ColumnSchema(DEVICE_TAG, TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema(VALUE_FIELD, TSDataType.INT64, ColumnCategory.FIELD),
        ],
    )

    try:
        with TsFileTableWriter(file_name, schema) as writer:
            tablet = Tablet(
                [DEVICE_TAG, VALUE_FIELD],
                [TSDataType.STRING, TSDataType.INT64],
                TABLET_ROWS,
            )
            tablet.set_table_name(TABLE_NAME)

            total_rows = 0
            while get_file_size(file_name) < TARGET_FILE_SIZE:
                _fill_table_tablet(tablet, total_rows)
                writer.write_table(tablet)
                total_rows += TABLET_ROWS
                if total_rows % FLUSH_ROWS == 0:
                    writer.flush()

            writer.flush()

        final_size = get_file_size(file_name)
        assert final_size >= MIN_ACCEPTABLE_FILE_SIZE

        reader = TsFileReader(file_name)
        try:
            for index in (0, total_rows // 2, total_rows - 1):
                assert _verify_table_record(reader, index), f"index={index}"
        finally:
            reader.close()
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)
