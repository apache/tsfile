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

import io

import pytest

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    Tablet,
    TSDataType,
    TsFileReader,
    TsFileWriter,
)


def _write_table(writer):
    column1 = ColumnSchema("item_id", TSDataType.STRING, ColumnCategory.TAG)
    column2 = ColumnSchema("value", TSDataType.DOUBLE)
    table = TableSchema("test_table", [column1, column2])
    writer.register_table(table)

    tablet = Tablet(["item_id", "value"], [TSDataType.STRING, TSDataType.DOUBLE], 10)
    tablet.set_table_name("test_table")
    for i in range(10):
        tablet.add_timestamp(i, i)
        tablet.add_value_by_name("item_id", i, f"id{i}")
        tablet.add_value_by_name("value", i, i * 10.0)
    writer.write_table(tablet)


def test_write_to_bytesio_and_read_back():
    buf = io.BytesIO()
    with TsFileWriter(buf) as writer:
        _write_table(writer)

    data = buf.getvalue()
    assert len(data) > 0

    source = io.BytesIO(data)
    with TsFileReader(source) as reader:
        result = reader.query_table("test_table", ["item_id", "value"])
        try:
            seen = []
            while result.next():
                seen.append(result.get_value_by_name("item_id"))
            assert seen == [f"id{i}" for i in range(10)]
        finally:
            result.close()


def test_write_target_must_be_writable():
    class NotWritable:
        pass

    with pytest.raises(TypeError):
        TsFileWriter(NotWritable())


def test_path_and_file_object_produce_same_bytes(tmp_path):
    path = tmp_path / "path_write.tsfile"
    with TsFileWriter(str(path)) as writer:
        _write_table(writer)
    path_bytes = path.read_bytes()

    buf = io.BytesIO()
    with TsFileWriter(buf) as writer:
        _write_table(writer)

    assert buf.getvalue() == path_bytes
