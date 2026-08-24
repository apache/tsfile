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

import pytest

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    FileWriteError,
    TableSchema,
    TSDataType,
    TsFileCorruptedError,
    TsFileReader,
    TsFileTableWriter,
    TsFileWriter,
)


def test_tsfile_writer_properties_round_trip(tmp_path):
    path = os.fspath(tmp_path / "writer-properties.tsfile")
    writer = TsFileWriter(path)
    writer.add_tsfile_property("overwritten", b"first")
    writer.flush()
    writer.add_tsfile_property("overwritten", b"\x00\xff\x80\x00")
    writer.add_tsfile_property("empty", b"")
    writer.add_tsfile_property("embedded\x00key", b"binary-key")

    with pytest.raises(TypeError):
        writer.add_tsfile_property("text", "not-bytes")
    with pytest.raises(TypeError):
        writer.add_tsfile_property("bytearray", bytearray(b"not-bytes"))
    with pytest.raises(TypeError):
        writer.add_tsfile_property("bytes-subclass", type("B", (bytes,), {})(b"value"))

    writer.close()
    with pytest.raises(FileWriteError):
        writer.add_tsfile_property("closed", b"value")

    with TsFileReader(path) as reader:
        properties = reader.get_tsfile_properties()
        assert properties["overwritten"] == b"\x00\xff\x80\x00"
        assert properties["empty"] == b""
        assert properties["embedded\x00key"] == b"binary-key"
        assert properties["encryptLevel"] == b"0"
        assert properties["encryptType"] == b"org.apache.tsfile.encrypt.UNENCRYPTED"
        assert properties["encryptKey"] is None


def test_tsfile_table_writer_property_delegation(tmp_path):
    path = os.fspath(tmp_path / "table-writer-properties.tsfile")
    schema = TableSchema(
        "table",
        [ColumnSchema("value", TSDataType.INT64, ColumnCategory.FIELD)],
    )
    writer = TsFileTableWriter(path, schema)
    writer.add_tsfile_property("table-property", b"before")
    writer.flush()
    writer.add_tsfile_property("table-property", b"after\x00\xff")
    writer.close()
    with pytest.raises(FileWriteError):
        writer.add_tsfile_property("closed", b"value")

    with TsFileReader(path) as reader:
        assert reader.get_tsfile_properties()["table-property"] == b"after\x00\xff"


def test_reader_reports_invalid_utf8_property_key(tmp_path):
    path = tmp_path / "invalid-property-key.tsfile"
    writer = TsFileWriter(os.fspath(path))
    writer.add_tsfile_property("invalid-key", b"value")
    writer.close()

    file_bytes = path.read_bytes()
    assert file_bytes.count(b"invalid-key") == 1
    path.write_bytes(file_bytes.replace(b"invalid-key", b"invalid-\xffey", 1))

    with TsFileReader(os.fspath(path)) as reader:
        with pytest.raises(
            TsFileCorruptedError,
            match="TsFile property key is not valid UTF-8",
        ):
            reader.get_tsfile_properties()
