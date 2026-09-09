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

import gc
import io
import os
import subprocess
import sys
import weakref
from pathlib import Path
from typing import Optional

import pytest
import numpy as np

from tsfile import (
    Field,
    RowRecord,
    TSDataType,
    TimeseriesSchema,
    TsFileReader,
    TsFileWriter,
)
from tsfile.exceptions import FileOpenError, FileReadError

RESOURCES = Path(__file__).parent / "resources"


class TrackingBytesIO(io.BytesIO):
    def __init__(self, data: bytes, max_chunk_size: Optional[int] = None):
        super().__init__(data)
        self.max_chunk_size = max_chunk_size
        self.read_sizes = []
        self.close_calls = 0

    def read(self, size=-1):
        if size < 0:
            raise AssertionError("TsFileReader must use explicitly sized reads")
        self.read_sizes.append(size)
        if self.max_chunk_size is not None:
            size = min(size, self.max_chunk_size)
        return super().read(size)

    def close(self):
        self.close_calls += 1
        super().close()


def collect_table_rows(source):
    with TsFileReader(source) as reader:
        result = reader.query_table("test", ["s0", "s2"])
        try:
            rows = []
            while result.next():
                rows.append(tuple(result.get_value_by_index(i) for i in range(1, 4)))
            return rows
        finally:
            result.close()


def collect_tree_rows(source):
    with TsFileReader(source) as reader:
        result = reader.query_timeseries(
            "root.ln.wf01.wt01",
            ["temperature", "status"],
            0,
            (1 << 63) - 1,
        )
        try:
            rows = []
            while result.next():
                rows.append(tuple(result.get_value_by_index(i) for i in range(1, 4)))
            return rows
        finally:
            result.close()


class CustomPath:
    def __init__(self, path):
        self.path = path

    def __fspath__(self):
        return self.path


@pytest.mark.parametrize(
    "make_source",
    [str, Path, os.fsencode, CustomPath, lambda p: CustomPath(os.fsencode(p)), np.str_],
    ids=["str", "pathlib", "bytes", "pathlike-str", "pathlike-bytes", "numpy-str"],
)
def test_path_sources_match_string_path(make_source):
    path = RESOURCES / "simple_table_t1.tsfile"
    expected = collect_table_rows(str(path))
    assert expected
    assert collect_table_rows(make_source(str(path))) == expected


@pytest.mark.skipif(os.name != "posix", reason="POSIX filesystem byte paths")
@pytest.mark.parametrize(
    "make_source",
    [lambda p: p, CustomPath, lambda p: Path(os.fsdecode(p))],
    ids=["bytes", "pathlike-bytes", "pathlib"],
)
def test_non_utf8_missing_path_reaches_native_open(tmp_path, make_source):
    path = os.fsencode(tmp_path) + b"/missing-\xff.tsfile"
    with pytest.raises(FileOpenError):
        TsFileReader(make_source(path))


@pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="Linux filenames permit arbitrary non-NUL bytes; macOS rejects them",
)
def test_non_utf8_filesystem_path_is_preserved(tmp_path):
    fixture = RESOURCES / "simple_table_t1.tsfile"
    path = os.fsencode(tmp_path) + b"/sample-\xff.tsfile"
    with open(path, "wb") as output:
        output.write(fixture.read_bytes())
    assert collect_table_rows(path) == collect_table_rows(str(fixture))


def test_seekable_binary_source_matches_local_table_query_and_preserves_cursor():
    path = RESOURCES / "simple_table_t1.tsfile"
    expected = collect_table_rows(str(path))
    source = TrackingBytesIO(path.read_bytes(), max_chunk_size=7)
    source.seek(11)

    actual = collect_table_rows(source)

    assert actual == expected
    assert source.tell() == 11
    assert source.read_sizes
    assert source.close_calls == 0


def test_file_like_source_initializes_native_runtime_in_fresh_process():
    path = RESOURCES / "simple_table_t1.tsfile"
    script = """
import io
import sys
from pathlib import Path

from tsfile import TsFileReader

source = io.BytesIO(Path(sys.argv[1]).read_bytes())
source.seek(11)
with TsFileReader(source) as reader:
    result = reader.query_table("test", ["s0"])
    try:
        assert result.next()
        assert result.get_value_by_index(1) == 1760106020000
        assert result.get_value_by_index(2) == "a"
    finally:
        result.close()
assert source.tell() == 11
"""

    completed = subprocess.run(
        [sys.executable, "-c", script, str(path)],
        cwd=Path(__file__).parents[1],
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_file_like_multi_field_row_query_does_not_deadlock():
    path = RESOURCES / "simple_table_t1.tsfile"
    script = """
import io
import sys
from pathlib import Path

from tsfile import TsFileReader

source = io.BytesIO(Path(sys.argv[1]).read_bytes())
source.seek(19)
with TsFileReader(source) as reader:
    result = reader.query_table("test", ["s2", "s3"])
    try:
        assert result.next()
        assert result.get_value_by_index(1) == 1760106020000
        assert result.get_value_by_index(2) == 1010
        assert result.get_value_by_index(3) == 2.0
    finally:
        result.close()
assert source.tell() == 19
"""

    try:
        completed = subprocess.run(
            [sys.executable, "-c", script, str(path)],
            cwd=Path(__file__).parents[1],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "multi-field row query deadlocked while native workers waited for the GIL"
        )

    assert completed.returncode == 0, completed.stderr


def test_reader_keeps_source_alive_until_close_without_closing_it():
    path = RESOURCES / "simple_table_t1.tsfile"
    source = TrackingBytesIO(path.read_bytes())
    source_ref = weakref.ref(source)
    reader = TsFileReader(source)

    del source
    gc.collect()
    assert source_ref() is not None

    reader.close()
    gc.collect()
    assert source_ref() is None


def test_seekable_binary_source_supports_tree_queries():
    path = RESOURCES / "simple_tree.tsfile"
    expected = collect_tree_rows(str(path))
    source = TrackingBytesIO(path.read_bytes(), max_chunk_size=5)

    assert collect_tree_rows(source) == expected


def test_non_file_object_is_rejected():
    with pytest.raises(TypeError, match="seekable binary file object"):
        TsFileReader(object())


@pytest.mark.parametrize(
    "failure,fail_restore",
    [
        ("initial_tell", False),
        ("seek_end", False),
        ("size_tell", False),
        ("restore", True),
        ("seek_end", True),
        ("size_tell", True),
    ],
)
def test_size_probe_errors_map_to_file_open_error(failure, fail_restore):
    class FailingProbeSource(TrackingBytesIO):
        tell_calls = 0

        def __init__(self, data):
            super().__init__(data)
            self.seek_calls = []

        def tell(self):
            self.tell_calls += 1
            if (failure == "initial_tell" and self.tell_calls == 1) or (
                failure == "size_tell" and self.tell_calls == 2
            ):
                raise OSError("size probe tell failed")
            return super().tell()

        def seek(self, offset, whence=os.SEEK_SET):
            self.seek_calls.append((offset, whence))
            if whence == os.SEEK_SET and fail_restore:
                raise RuntimeError("cursor restoration failed")
            position = super().seek(offset, whence)
            if whence == os.SEEK_END and failure == "seek_end":
                # A source may move its cursor before reporting failure.
                raise OSError("size probe seek failed")
            return position

    data = (RESOURCES / "simple_table_t1.tsfile").read_bytes()
    source = FailingProbeSource(data)
    io.BytesIO.seek(source, 13)

    with pytest.raises(FileOpenError):
        TsFileReader(source)

    assert not source.closed
    assert source.close_calls == 0
    assert source.read_sizes == []
    if failure == "initial_tell":
        assert source.seek_calls == []
    else:
        assert source.seek_calls == [(0, os.SEEK_END), (13, os.SEEK_SET)]
    assert io.BytesIO.tell(source) == (len(data) if fail_restore else 13)


def test_source_read_error_during_open_preserves_cursor():
    path = RESOURCES / "simple_table_t1.tsfile"

    class FailingBytesIO(TrackingBytesIO):
        fail_reads = False

        def read(self, size=-1):
            if self.fail_reads:
                raise OSError("remote read failed")
            return super().read(size)

    source = FailingBytesIO(path.read_bytes())
    source.seek(13)
    source.fail_reads = True

    with pytest.raises(FileReadError):
        TsFileReader(source)

    assert source.tell() == 13
    assert source.close_calls == 0


def test_source_read_error_during_query_preserves_cursor_and_source():
    path = RESOURCES / "simple_table_t1.tsfile"

    class FailingBytesIO(TrackingBytesIO):
        fail_reads = False

        def read(self, size=-1):
            if self.fail_reads:
                raise OSError("remote read failed")
            return super().read(size)

    source = FailingBytesIO(path.read_bytes())
    source.seek(17)

    with TsFileReader(source) as reader:
        source.fail_reads = True
        with pytest.raises(FileReadError):
            result = reader.query_table("test", ["s0"])
            try:
                result.next()
            finally:
                result.close()

    assert source.tell() == 17
    assert source.close_calls == 0


@pytest.mark.parametrize("measurements", [["temperature"], ["temperature", "status"]])
@pytest.mark.parametrize("stage", ["initial_block", "next_block"])
def test_tree_query_propagates_data_block_read_errors(tmp_path, measurements, stage):
    class FailingBytesIO(TrackingBytesIO):
        read_calls = 0
        fail_at = None
        failed = False

        def read(self, size=-1):
            self.read_calls += 1
            if self.fail_at is not None and self.read_calls >= self.fail_at:
                self.failed = True
                raise OSError("data block read failed")
            return super().read(size)

    path = tmp_path / "multiple_chunks.tsfile"
    device = "root.sg.device"
    with TsFileWriter(str(path)) as writer:
        for name in measurements:
            writer.register_timeseries(device, TimeseriesSchema(name, TSDataType.INT64))
        for chunk in range(2):
            for timestamp in range(chunk * 100, (chunk + 1) * 100):
                writer.write_row_record(
                    RowRecord(
                        device,
                        timestamp,
                        [
                            Field(name, timestamp, TSDataType.INT64)
                            for name in measurements
                        ],
                    )
                )
            writer.flush()
    data = path.read_bytes()

    def query(reader):
        return reader.query_timeseries(device, measurements, 0, (1 << 63) - 1)

    # Locate the initial and subsequent block reads without fixing their call
    # numbers: opening and metadata reads may change as the reader evolves.
    baseline = FailingBytesIO(data)
    with TsFileReader(baseline) as reader:
        with query(reader) as result:
            initial_block_read = baseline.read_calls
            rows = 0
            while result.next():
                rows += 1
    assert rows == 200
    assert baseline.read_calls > initial_block_read

    source = FailingBytesIO(data)
    source.seek(17)
    with TsFileReader(source) as reader:
        if stage == "initial_block":
            source.fail_at = initial_block_read
            with pytest.raises(FileReadError):
                with query(reader):
                    pass
        else:
            with query(reader) as result:
                source.fail_at = source.read_calls + 1
                with pytest.raises(FileReadError):
                    while result.next():
                        pass
                # A failed result must not become a successful EOF on retry.
                source.fail_at = None
                with pytest.raises(FileReadError):
                    result.next()
    assert source.failed
    assert source.tell() == 17
    assert source.close_calls == 0


@pytest.mark.parametrize("method", ["get_all_devices", "get_all_table_schemas"])
def test_metadata_read_failure_is_not_an_empty_result(method):
    class FailingBytesIO(TrackingBytesIO):
        fail_reads = False
        failed = False

        def read(self, size=-1):
            if self.fail_reads:
                self.failed = True
                raise OSError("metadata read failed")
            return super().read(size)

    source = FailingBytesIO((RESOURCES / "simple_table_t1.tsfile").read_bytes())
    source.seek(17)
    with TsFileReader(source) as reader:
        source.fail_reads = True
        with pytest.raises(FileReadError):
            getattr(reader, method)()
    assert source.failed
    assert source.tell() == 17
    assert source.close_calls == 0


def test_device_index_read_failure_does_not_return_partial_devices(tmp_path):
    class FailingBytesIO(TrackingBytesIO):
        fail_index_reads = False
        index_reads = 0
        failed = False

        def read(self, size=-1):
            if self.fail_index_reads:
                self.index_reads += 1
                if self.index_reads == 2:
                    self.failed = True
                    raise OSError("device index read failed")
            return super().read(size)

    # More than two default index nodes (256 children each), so a failure in
    # the second node must not be hidden by a successful read of the third.
    path = tmp_path / "device_index.tsfile"
    with TsFileWriter(str(path)) as writer:
        for index in range(513):
            device = f"root.sg.d{index:04d}"
            writer.register_timeseries(device, TimeseriesSchema("s", TSDataType.INT64))
            writer.write_row_record(
                RowRecord(device, 0, [Field("s", index, TSDataType.INT64)])
            )
    source = FailingBytesIO(path.read_bytes())
    source.seek(17)
    with TsFileReader(source) as reader:
        assert len(reader.get_all_devices()) == 513
        source.fail_index_reads = True
        with pytest.raises(FileReadError):
            reader.get_all_devices()
    assert source.failed
    assert source.tell() == 17
    assert source.close_calls == 0
