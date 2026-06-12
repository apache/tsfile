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

from tsfile import Compressor, Tablet, TSEncoding, TSDataType, TimeseriesSchema
from tsfile import TsFileReader, TsFileWriter

from conftest import (
    FLUSH_ROWS,
    MIN_ACCEPTABLE_FILE_SIZE,
    START_TIME,
    TABLET_ROWS,
    TARGET_FILE_SIZE,
    get_file_size,
    random_suffix,
)


def _fill_tree_tablet(tablet: Tablet, total_rows: int) -> None:
    row_range = range(TABLET_ROWS)
    tablet.timestamp_list[:TABLET_ROWS] = [
        START_TIME + (total_rows + row) * 1000 for row in row_range
    ]
    tablet.data_list[0][:TABLET_ROWS] = [total_rows + row for row in row_range]


def _verify_tree_record(reader: TsFileReader, record_index: int) -> bool:
    timestamp = START_TIME + record_index * 1000
    result = reader.query_timeseries(
        "device1", ["temperature"], timestamp, timestamp + 1
    )
    try:
        if not result.next():
            return False
        return (
            result.get_value_by_index(1) == timestamp
            and result.get_value_by_index(2) == record_index
        )
    finally:
        result.close()


def test_large_file_4gb_tree_write_and_read():
    file_name = f"large_file_tree_test_{random_suffix()}.tsfile"
    if os.path.exists(file_name):
        os.remove(file_name)

    try:
        writer = TsFileWriter(file_name)
        writer.register_timeseries(
            "device1",
            TimeseriesSchema(
                "temperature",
                TSDataType.INT64,
                TSEncoding.PLAIN,
                Compressor.UNCOMPRESSED,
            ),
        )

        tablet = Tablet(["temperature"], [TSDataType.INT64], TABLET_ROWS)
        tablet.set_table_name("device1")

        total_rows = 0
        while get_file_size(file_name) < TARGET_FILE_SIZE:
            _fill_tree_tablet(tablet, total_rows)
            writer.write_tablet(tablet)
            total_rows += TABLET_ROWS
            if total_rows % FLUSH_ROWS == 0:
                writer.flush()

        writer.flush()
        writer.close()

        final_size = get_file_size(file_name)
        assert final_size >= MIN_ACCEPTABLE_FILE_SIZE

        reader = TsFileReader(file_name)
        try:
            for index in (0, total_rows // 2, total_rows - 1):
                assert _verify_tree_record(reader, index), f"index={index}"
        finally:
            reader.close()
    finally:
        if os.path.exists(file_name):
            os.remove(file_name)
