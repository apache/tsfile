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

from tsfile import (
    TIME_COLUMN,
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    TSDataType,
    Tablet,
    TsFileTableWriter,
    to_dataframe,
)
from tsfile.exceptions import OutOfOrderError

TABLE_NAME = "dup_table"
FILE_NAME = "allow_non_decreasing_timestamps.tsfile"


def _table_schema():
    return TableSchema(
        TABLE_NAME,
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value", TSDataType.INT64, ColumnCategory.FIELD),
        ],
    )


def _build_tablet(times):
    tablet = Tablet(
        ["device", "value"], [TSDataType.STRING, TSDataType.INT64], len(times)
    )
    tablet.set_table_name(TABLE_NAME)
    for i, timestamp in enumerate(times):
        tablet.add_timestamp(i, timestamp)
        tablet.add_value_by_name("device", i, "d0")
        tablet.add_value_by_name("value", i, i)
    return tablet


def _remove_file():
    if os.path.exists(FILE_NAME):
        os.remove(FILE_NAME)


def test_table_writer_duplicates_allowed():
    try:
        with TsFileTableWriter(
            FILE_NAME, _table_schema(), allow_non_decreasing_timestamps=True
        ) as writer:
            writer.write_table(_build_tablet([10, 11, 11, 12]))
        df = to_dataframe(FILE_NAME, table_name=TABLE_NAME)
        assert df.shape[0] == 4
        assert list(df[TIME_COLUMN]) == [10, 11, 11, 12]
    finally:
        _remove_file()


def test_table_writer_duplicates_rejected_by_default():
    _assert_write_rejected(allow_non_decreasing_timestamps=False, times=[10, 11, 11])


def test_table_writer_strictly_decreasing_rejected():
    _assert_write_rejected(allow_non_decreasing_timestamps=True, times=[10, 11, 10])


def _assert_write_rejected(allow_non_decreasing_timestamps, times):
    try:
        writer = TsFileTableWriter(
            FILE_NAME,
            _table_schema(),
            allow_non_decreasing_timestamps=allow_non_decreasing_timestamps,
        )
        try:
            with pytest.raises(OutOfOrderError):
                writer.write_table(_build_tablet(times))
        finally:
            # A rejected time column leaves the C++ writer unrecoverable, so
            # close() may raise DataInconsistencyError; ignore it here.
            try:
                writer.close()
            except Exception:
                pass
    finally:
        _remove_file()
