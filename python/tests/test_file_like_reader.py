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
import subprocess
import sys
import weakref
from pathlib import Path
from typing import Optional

import pytest

from tsfile import TsFileReader
from tsfile.exceptions import FileReadError

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
