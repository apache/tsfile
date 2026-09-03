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

import pytest

from tsfile import Field, RowRecord, Tablet, TimeseriesSchema, TSDataType
from tsfile import TsFileReader, TsFileWriter
from tsfile.exceptions import (
    AlreadyExistsError,
    ErrorCode,
    FileMapError,
    FileOpenError,
    InvalidArgumentError,
    InvalidPathError,
    LibraryError,
    TsFileCorruptedError,
    get_exception,
)


def test_get_exception_preserves_known_and_unknown_codes():
    corrupted = get_exception(ErrorCode.TSFILE_CORRUPTED)
    assert isinstance(corrupted, TsFileCorruptedError)
    assert corrupted.code == ErrorCode.TSFILE_CORRUPTED
    assert corrupted.message == "TsFile is corrupted"

    unknown = get_exception(999, "opening reader")
    assert type(unknown) is LibraryError
    assert unknown.code == 999
    assert unknown.message == "opening reader: Unknown library error"

    retired_codes = {
        *range(9, 20),
        23,
        25,
        39,
        41,
        42,
        46,
        47,
    }
    assert retired_codes.isdisjoint(error.value for error in ErrorCode)
    for code in retired_codes:
        retired = get_exception(code)
        assert type(retired) is LibraryError
        assert retired.code == code
        assert retired.message == "Unknown library error"

    invalid_path = get_exception(37)
    assert isinstance(invalid_path, InvalidPathError)
    assert invalid_path.code == ErrorCode.INVALID_PATH

    map_error = get_exception(ErrorCode.FILE_MAP_ERROR)
    assert isinstance(map_error, FileMapError)
    assert map_error.code == ErrorCode.FILE_MAP_ERROR


def test_invalid_tree_path_propagates_native_error(tmp_path):
    path = tmp_path / "invalid-path.tsfile"
    with TsFileWriter(str(path)) as writer:
        writer.register_timeseries(
            "root.device", TimeseriesSchema("value", TSDataType.INT64)
        )

    with TsFileReader(str(path)) as reader:
        with pytest.raises(InvalidPathError) as exc_info:
            reader.query_timeseries("root.device", ["a*%"], 0, 2)

    assert exc_info.value.code == ErrorCode.INVALID_PATH


def test_writer_constructor_propagates_native_error(tmp_path, capsys):
    path = tmp_path / "already-exists.tsfile"
    path.touch()

    with pytest.raises(AlreadyExistsError) as exc_info:
        TsFileWriter(str(path))

    assert exc_info.value.code == ErrorCode.ALREADY_EXIST
    assert exc_info.value.message == "Resource already exists"
    assert capsys.readouterr().out == ""


def test_reader_constructor_propagates_native_errors(tmp_path):
    missing = tmp_path / "missing.tsfile"
    with pytest.raises(FileOpenError) as exc_info:
        TsFileReader(str(missing))
    assert exc_info.value.code == ErrorCode.FILE_OPEN_ERROR

    corrupted = tmp_path / "corrupted.tsfile"
    corrupted.touch()
    with pytest.raises(TsFileCorruptedError) as exc_info:
        TsFileReader(str(corrupted))
    assert exc_info.value.code == ErrorCode.TSFILE_CORRUPTED


def test_duplicate_tablet_columns_do_not_silently_drop_rows(tmp_path):
    path = tmp_path / "duplicate-columns.tsfile"
    with TsFileWriter(str(path)) as writer:
        writer.register_timeseries(
            "root.device", TimeseriesSchema("value", TSDataType.INT64)
        )
        tablet = Tablet(
            ["value", "VALUE"],
            [TSDataType.INT64, TSDataType.INT64],
            max_row_num=1,
        )
        tablet.set_table_name("root.device")
        tablet.add_timestamp(0, 1)
        tablet.add_value_by_index(0, 0, 10)
        tablet.add_value_by_index(1, 0, 20)

        with pytest.raises(InvalidArgumentError) as exc_info:
            writer.write_tablet(tablet)

        assert exc_info.value.code == ErrorCode.INVALID_ARGUMENT
        assert "unique" in exc_info.value.message


def test_row_record_still_writes_after_error_handling_changes(tmp_path):
    path = tmp_path / "row-record.tsfile"
    with TsFileWriter(str(path)) as writer:
        writer.register_timeseries(
            "root.device", TimeseriesSchema("value", TSDataType.INT64)
        )
        writer.write_row_record(
            RowRecord("root.device", 1, [Field("value", 10, TSDataType.INT64)])
        )

    with TsFileReader(str(path)) as reader:
        with reader.query_timeseries("root.device", ["value"], 0, 2) as result:
            assert result.next()
            assert result.get_value_by_index(2) == 10
