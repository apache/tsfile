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


def test_pathname_keyword_and_bytes_path(tmp_path):
    keyword_path = tmp_path / "keyword.tsfile"
    with TsFileWriter(pathname=str(keyword_path)) as writer:
        _write_table(writer)
    keyword_bytes = keyword_path.read_bytes()
    assert len(keyword_bytes) > 0

    bytes_path = str(tmp_path / "bytes.tsfile").encode()
    with TsFileWriter(bytes_path) as writer:
        _write_table(writer)
    assert (tmp_path / "bytes.tsfile").read_bytes() == keyword_bytes


def test_file_object_with_existing_content_is_reset():
    buf = io.BytesIO(b"stale trailing bytes" * 100)
    buf.seek(1000)
    with TsFileWriter(buf) as writer:
        _write_table(writer)

    data = buf.getvalue()
    with TsFileReader(io.BytesIO(data)) as reader:
        result = reader.query_table("test_table", ["item_id", "value"])
        try:
            seen = []
            while result.next():
                seen.append(result.get_value_by_name("item_id"))
            assert seen == [f"id{i}" for i in range(10)]
        finally:
            result.close()


def test_flush_failure_is_reported():
    from tsfile import FileSyncError

    class FailingFlush(io.BytesIO):
        def flush(self):
            raise OSError("disk full")

    target = FailingFlush()
    with pytest.raises(FileSyncError) as excinfo:
        with TsFileWriter(target) as writer:
            _write_table(writer)
    # The original OSError text is kept, not replaced by a generic message.
    assert "disk full" in str(excinfo.value)


def test_memory_threshold_is_applied():
    from tsfile import get_tsfile_config

    from tsfile import set_tsfile_config

    original = get_tsfile_config()["chunk_group_size_threshold_"]
    try:
        with TsFileWriter(io.BytesIO(), memory_threshold=4096) as writer:
            _write_table(writer)
        assert get_tsfile_config()["chunk_group_size_threshold_"] == 4096
    finally:
        set_tsfile_config({"chunk_group_size_threshold_": original})


def test_position_does_not_depend_on_tell_after_start():
    class FlakyTell(io.BytesIO):
        def __init__(self, *args):
            super().__init__(*args)
            self._calls = 0

        def tell(self):
            self._calls += 1
            if self._calls > 1:
                raise OSError("tell is not supported while writing")
            return super().tell()

    target = FlakyTell()
    with TsFileWriter(target) as writer:
        _write_table(writer)
    with TsFileReader(io.BytesIO(target.getvalue())) as reader:
        result = reader.query_table("test_table", ["item_id", "value"])
        try:
            seen = []
            while result.next():
                seen.append(result.get_value_by_name("item_id"))
            assert seen == [f"id{i}" for i in range(10)]
        finally:
            result.close()


class _ForwardOnlyStream(io.RawIOBase):
    """A write-only sink: seek/truncate exist but raise, like a pipe."""

    def __init__(self):
        self._buffer = bytearray()

    def writable(self):
        return True

    def write(self, data):
        self._buffer += data
        return len(data)

    def tell(self):
        return len(self._buffer)


def test_forward_only_stream_is_accepted():
    target = _ForwardOnlyStream()
    with TsFileWriter(target) as writer:
        _write_table(writer)

    with TsFileReader(io.BytesIO(bytes(target._buffer))) as reader:
        result = reader.query_table("test_table", ["item_id", "value"])
        try:
            seen = []
            while result.next():
                seen.append(result.get_value_by_name("item_id"))
            assert seen == [f"id{i}" for i in range(10)]
        finally:
            result.close()


def test_forward_only_stream_at_nonzero_position_is_rejected():
    target = _ForwardOnlyStream()
    target._buffer += b"prefix"
    with pytest.raises(ValueError):
        TsFileWriter(target)


class _SeekableWithoutTruncate(io.BytesIO):
    def truncate(self, size=None):
        raise io.UnsupportedOperation("truncate")


def test_seekable_target_without_truncate_must_be_empty():
    with pytest.raises(ValueError):
        TsFileWriter(_SeekableWithoutTruncate(b"stale"))

    target = _SeekableWithoutTruncate()
    with TsFileWriter(target) as writer:
        _write_table(writer)
    assert len(target.getvalue()) > 0


def test_tree_model_write_to_file_object():
    from tsfile import Field, RowRecord, TimeseriesSchema

    buf = io.BytesIO()
    with TsFileWriter(buf) as writer:
        writer.register_timeseries(
            "root.device", TimeseriesSchema("temperature", TSDataType.DOUBLE)
        )
        for second in range(5):
            fields = [Field("temperature", second * 0.5, TSDataType.DOUBLE)]
            writer.write_row_record(RowRecord("root.device", second, fields))

    with TsFileReader(io.BytesIO(buf.getvalue())) as reader:
        with reader.query_timeseries("root.device", ["temperature"], 0, 4) as result:
            seen = []
            while result.next():
                seen.append(result.get_value_by_index(2))
            assert seen == [second * 0.5 for second in range(5)]


def test_explicit_flush_keeps_writing():
    buf = io.BytesIO()
    writer = TsFileWriter(buf)
    try:
        _write_table(writer)
        writer.flush()
        assert len(buf.getvalue()) > 0
    finally:
        writer.close()

    with TsFileReader(io.BytesIO(buf.getvalue())) as reader:
        result = reader.query_table("test_table", ["item_id", "value"])
        try:
            assert result.next()
        finally:
            result.close()
