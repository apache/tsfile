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
import shutil
import tempfile

import pytest

from tsfile import (
    TIME_COLUMN,
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    TSDataType,
    Tablet,
    TsFileReader,
    TsFileTableWriter,
    to_dataframe,
)
from tsfile.exceptions import OutOfOrderError

TABLE_NAME = "dup_table"


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


def _new_tmpdir():
    return tempfile.mkdtemp(prefix="tsfile_allow_non_decreasing_")


def test_table_writer_duplicates_allowed():
    tmpdir = _new_tmpdir()
    path = os.path.join(tmpdir, "dup.tsfile")
    try:
        with TsFileTableWriter(
            path, _table_schema(), allow_non_decreasing_timestamps=True
        ) as writer:
            writer.write_table(_build_tablet([10, 11, 11, 12]))
        df = to_dataframe(path, table_name=TABLE_NAME)
        assert df.shape[0] == 4
        assert list(df[TIME_COLUMN]) == [10, 11, 11, 12]
        with TsFileReader(path) as reader:
            assert (
                reader.get_tsfile_properties()["allowNonDecreasingTimestamps"]
                == b"true"
            )
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_policy_property_absent_when_flag_off():
    tmpdir = _new_tmpdir()
    path = os.path.join(tmpdir, "strict.tsfile")
    try:
        with TsFileTableWriter(
            path, _table_schema(), allow_non_decreasing_timestamps=False
        ) as writer:
            writer.write_table(_build_tablet([10, 11, 12]))
        with TsFileReader(path) as reader:
            assert "allowNonDecreasingTimestamps" not in reader.get_tsfile_properties()
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def test_table_writer_duplicates_rejected_by_default():
    _assert_write_rejected(allow_non_decreasing_timestamps=False, times=[10, 11, 11])


def test_table_writer_strictly_decreasing_rejected():
    _assert_write_rejected(allow_non_decreasing_timestamps=True, times=[10, 11, 10])


def _assert_write_rejected(allow_non_decreasing_timestamps, times):
    tmpdir = _new_tmpdir()
    path = os.path.join(tmpdir, "dup.tsfile")
    try:
        writer = TsFileTableWriter(
            path,
            _table_schema(),
            allow_non_decreasing_timestamps=allow_non_decreasing_timestamps,
        )
        try:
            with pytest.raises(OutOfOrderError):
                writer.write_table(_build_tablet(times))
        finally:
            # A rejected time column leaves the C++ writer unrecoverable, so
            # close() raises DataInconsistencyError instead of releasing the
            # file; ignore it here. The handle stays open on Windows, so the
            # rmtree below cleans up best-effort.
            try:
                writer.close()
            except Exception:
                pass
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
