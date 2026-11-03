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
"""Ownership and lifetime scenarios for the sanitizer job.

Each scenario is executed in a fresh interpreter by
``tests/test_sanitizer_ownership.py`` so that LeakSanitizer can run at normal
process exit. Run one manually with::

    python -m tests.sanitizer_scenarios <scenario> <work-dir>

The module must run from the ``python/`` directory with an instrumented build
whose sanitizer runtime was preloaded (see ``python/SANITIZER.md``).
"""

import ctypes
import gc
import io
import sys
import threading
from datetime import date
from pathlib import Path

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    Field,
    RowRecord,
    TableSchema,
    Tablet,
    TSDataType,
    TimeseriesSchema,
    TsFileDataFrame,
    TsFileReader,
    TsFileTableWriter,
    TsFileWriter,
)

ROWS = 1024
BATCH_SIZE = 64
RESOURCES = Path(__file__).resolve().parent / "resources"

TREE_MEASUREMENTS = [
    ("i32", TSDataType.INT32),
    ("i64", TSDataType.INT64),
    ("f", TSDataType.FLOAT),
    ("d", TSDataType.DOUBLE),
    ("b", TSDataType.BOOLEAN),
    ("s", TSDataType.STRING),
    ("t", TSDataType.TEXT),
    ("blob", TSDataType.BLOB),
    ("dt", TSDataType.DATE),
    ("ts", TSDataType.TIMESTAMP),
]
TREE_DEVICES = ["root.ds.d1", "root.ds.d2"]

TABLE_COLUMNS = [
    ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
    ColumnSchema("i32", TSDataType.INT32, ColumnCategory.FIELD),
    ColumnSchema("i64", TSDataType.INT64, ColumnCategory.FIELD),
    ColumnSchema("f", TSDataType.FLOAT, ColumnCategory.FIELD),
    ColumnSchema("d", TSDataType.DOUBLE, ColumnCategory.FIELD),
    ColumnSchema("b", TSDataType.BOOLEAN, ColumnCategory.FIELD),
    ColumnSchema("s", TSDataType.STRING, ColumnCategory.FIELD),
    ColumnSchema("t", TSDataType.TEXT, ColumnCategory.FIELD),
    ColumnSchema("blob", TSDataType.BLOB, ColumnCategory.FIELD),
    ColumnSchema("dt", TSDataType.DATE, ColumnCategory.FIELD),
    ColumnSchema("ts", TSDataType.TIMESTAMP, ColumnCategory.FIELD),
]


def _tree_value(name, data_type, row):
    if data_type == TSDataType.INT32:
        return row * 2
    if data_type == TSDataType.INT64:
        return row * 3
    if data_type == TSDataType.FLOAT:
        return row * 1.5
    if data_type == TSDataType.DOUBLE:
        return row * 2.5
    if data_type == TSDataType.BOOLEAN:
        return row % 2 == 0
    if data_type == TSDataType.STRING:
        return f"str_{row}"
    if data_type == TSDataType.TEXT:
        return f"text_{row}"
    if data_type == TSDataType.BLOB:
        return b"blob\x00" + str(row).encode("ascii")
    if data_type == TSDataType.DATE:
        return date(2025, 1, 1 + row % 28)
    if data_type == TSDataType.TIMESTAMP:
        return row * 1000
    raise AssertionError(f"unexpected type {data_type}")


def _skip_tree_value(name, row):
    if name == "s":
        return row % 7 == 3
    if name == "blob":
        return row % 5 == 2
    return False


def _build_tree(base):
    path = base / "tree_all.tsfile"
    writer = TsFileWriter(str(path))
    for device in TREE_DEVICES:
        for name, data_type in TREE_MEASUREMENTS:
            writer.register_timeseries(device, TimeseriesSchema(name, data_type))
    writer.add_tsfile_property("owner", b"apache\x00tsfile")
    for row in range(ROWS):
        for device in TREE_DEVICES:
            fields = [
                Field(name, _tree_value(name, data_type, row), data_type)
                for name, data_type in TREE_MEASUREMENTS
                if not _skip_tree_value(name, row)
            ]
            writer.write_row_record(RowRecord(device, row, fields))
    writer.close()
    return path


def _build_table(base):
    path = base / "table_all.tsfile"
    schema = TableSchema("measures", TABLE_COLUMNS)
    with TsFileTableWriter(str(path), schema) as writer:
        tablet = Tablet(
            [column.column_name for column in TABLE_COLUMNS],
            [column.data_type for column in TABLE_COLUMNS],
            ROWS,
        )
        for row in range(ROWS):
            tablet.add_timestamp(row, row)
            tablet.add_value_by_name("device", row, "d1")
            tablet.add_value_by_name("i32", row, row * 2)
            tablet.add_value_by_name("i64", row, row * 3)
            tablet.add_value_by_name("f", row, row * 1.5)
            tablet.add_value_by_name("d", row, row * 2.5)
            if row % 3:
                tablet.add_value_by_name("b", row, row % 2 == 0)
            if row % 4 != 1:
                tablet.add_value_by_name("s", row, f"str_{row}")
            tablet.add_value_by_name("t", row, f"text_{row}")
            if row % 5 != 2:
                tablet.add_value_by_name(
                    "blob", row, b"blob\x00" + str(row).encode("ascii")
                )
            tablet.add_value_by_name("dt", row, date(2025, 1, 1 + row % 28))
            tablet.add_value_by_name("ts", row, row * 1000)
        writer.write_table(tablet)
    return path


def _build_tree_numeric(base):
    path = base / "tree_numeric.tsfile"
    writer = TsFileWriter(str(path))
    writer.register_timeseries("root.ds.d1", TimeseriesSchema("s1", TSDataType.INT64))
    writer.register_timeseries("root.ds.d1", TimeseriesSchema("s2", TSDataType.DOUBLE))
    for row in range(ROWS):
        writer.write_row_record(
            RowRecord(
                "root.ds.d1",
                row,
                [
                    Field("s1", row * 2, TSDataType.INT64),
                    Field("s2", row * 0.5, TSDataType.DOUBLE),
                ],
            )
        )
    writer.close()
    return path


def _build_table_numeric(base):
    path = base / "table_numeric.tsfile"
    schema = TableSchema(
        "metrics",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("value1", TSDataType.INT64, ColumnCategory.FIELD),
            ColumnSchema("value2", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ],
    )
    with TsFileTableWriter(str(path), schema) as writer:
        tablet = Tablet(
            ["device", "value1", "value2"],
            [TSDataType.STRING, TSDataType.INT64, TSDataType.DOUBLE],
            ROWS,
        )
        for row in range(ROWS):
            tablet.add_timestamp(row, row)
            tablet.add_value_by_name("device", row, "d1")
            tablet.add_value_by_name("value1", row, row * 2)
            tablet.add_value_by_name("value2", row, row * 0.5)
        writer.write_table(tablet)
    return path


def build_fixtures(base):
    base.mkdir(parents=True, exist_ok=True)
    for name in (
        "tree_all.tsfile",
        "table_all.tsfile",
        "tree_numeric.tsfile",
        "table_numeric.tsfile",
    ):
        (base / name).unlink(missing_ok=True)
    paths = [
        _build_tree(base),
        _build_table(base),
        _build_tree_numeric(base),
        _build_table_numeric(base),
    ]
    for path in paths:
        assert path.is_file() and path.stat().st_size > 0, path


def _fixture(base, name):
    path = base / name
    assert path.is_file(), f"missing fixture {path}; run build_fixtures first"
    return str(path)


def tree_full(base):
    path = _fixture(base, "tree_all.tsfile")
    names = [name for name, _ in TREE_MEASUREMENTS]
    with TsFileReader(path) as reader:
        result = reader.query_timeseries(TREE_DEVICES[0], names, 0, ROWS + 1)
        try:
            row = 0
            while result.next():
                assert result.get_value_by_index(1) == row
                assert result.get_value_by_index(2) == row * 2
                assert result.get_value_by_index(3) == row * 3
                assert result.get_value_by_index(5) == row * 2.5
                assert result.get_value_by_index(6) == (row % 2 == 0)
                if _skip_tree_value("s", row):
                    assert result.is_null_by_index(7)
                else:
                    assert result.get_value_by_index(7) == f"str_{row}"
                assert result.get_value_by_index(8) == f"text_{row}"
                if _skip_tree_value("blob", row):
                    assert result.is_null_by_index(9)
                else:
                    assert result.get_value_by_index(9) == (
                        b"blob\x00" + str(row).encode("ascii")
                    )
                row += 1
            assert row == ROWS
        finally:
            result.close()

        by_row = reader.query_tree_by_row([TREE_DEVICES[0]], names[:3], 8, 16)
        try:
            row = 8
            while by_row.next():
                assert by_row.get_value_by_index(1) == row
                assert by_row.get_value_by_index(2) == row * 2
                row += 1
            assert row == 24
        finally:
            by_row.close()


def table_full(base):
    path = _fixture(base, "table_all.tsfile")
    columns = ["device", "i32", "i64", "d", "b", "s", "blob"]
    with TsFileReader(path) as reader:
        result = reader.query_table("measures", columns, 0, ROWS + 1)
        try:
            row = 0
            while result.next():
                assert result.get_value_by_index(1) == row
                assert result.get_value_by_index(2) == "d1"
                assert result.get_value_by_index(3) == row * 2
                assert result.get_value_by_index(4) == row * 3
                assert result.get_value_by_index(5) == row * 2.5
                if row % 3:
                    assert result.get_value_by_index(6) == (row % 2 == 0)
                else:
                    assert result.is_null_by_index(6)
                if row % 4 != 1:
                    assert result.get_value_by_index(7) == f"str_{row}"
                else:
                    assert result.is_null_by_index(7)
                if row % 5 != 2:
                    assert result.get_value_by_index(8) == (
                        b"blob\x00" + str(row).encode("ascii")
                    )
                else:
                    assert result.is_null_by_index(8)
                row += 1
            assert row == ROWS
        finally:
            result.close()

        by_row = reader.query_table_by_row("measures", columns, 4, 8)
        try:
            row = 4
            while by_row.next():
                assert by_row.get_value_by_index(1) == row
                row += 1
            assert row == 12
        finally:
            by_row.close()

        frame = reader.query_table("measures", columns, 0, ROWS + 1)
        try:
            assert frame.next()
            dataframe = frame.read_data_frame(max_row_num=ROWS)
        finally:
            frame.close()
        assert len(dataframe) == ROWS
        assert dataframe["i64"].iloc[-1] == (ROWS - 1) * 3


def metadata_reads(base):
    tree_path = _fixture(base, "tree_all.tsfile")
    table_path = _fixture(base, "table_all.tsfile")
    for _ in range(3):
        with TsFileReader(table_path) as reader:
            schema = reader.get_table_schema("measures")
            assert schema.get_table_name() == "measures"
            assert reader.get_all_table_schemas()
            assert reader.get_all_devices()
            properties = reader.get_tsfile_properties()
            assert properties["encryptType"] == (
                b"org.apache.tsfile.encrypt.UNENCRYPTED"
            )
            assert "owner" not in properties
        with TsFileReader(tree_path) as reader:
            schemas = reader.get_all_timeseries_schemas()
            assert set(schemas) == set(TREE_DEVICES)
            for device in TREE_DEVICES:
                assert len(schemas[device].get_timeseries_list()) == len(
                    TREE_MEASUREMENTS
                )
            assert len(reader.get_all_devices()) == len(TREE_DEVICES)
            assert reader.get_timeseries_metadata()
            properties = reader.get_tsfile_properties()
            assert properties["owner"] == b"apache\x00tsfile"


def file_object_read(base):
    source = io.BytesIO((RESOURCES / "simple_table_t1.tsfile").read_bytes())
    source.seek(11)
    with TsFileReader(source) as reader:
        result = reader.query_table("test", ["s0", "s1"])
        try:
            assert result.next()
            assert result.get_value_by_index(1) == 1760106020000
            assert result.get_value_by_index(2) == "a"
        finally:
            result.close()
    assert source.tell() == 11


def dataset_read(base):
    tree_path = _fixture(base, "tree_numeric.tsfile")
    table_path = _fixture(base, "table_numeric.tsfile")
    for use_index in (False, True):
        with TsFileDataFrame(
            tree_path, show_progress=False, use_index=use_index
        ) as frame:
            names = frame.list_timeseries()
            assert names
            for name in names:
                values = frame[name][:]
                assert len(values) == ROWS
                assert float(values[0]) == 0.0
        with TsFileDataFrame(
            table_path, show_progress=False, use_index=use_index
        ) as frame:
            names = frame.list_timeseries()
            assert names
            for name in names:
                values = frame[name][:]
                assert len(values) == ROWS
                assert float(values[0]) == 0.0


def arrow_full(base):
    path = _fixture(base, "table_all.tsfile")
    columns = ["device", "i64", "d", "s"]
    with TsFileReader(path) as reader:
        result = reader.query_table(
            "measures", columns, 0, ROWS + 1, batch_size=BATCH_SIZE
        )
        batches = []
        total = 0
        try:
            while True:
                batch = result.read_arrow_record_batch()
                if batch is None:
                    break
                batches.append(batch)
                total += batch.num_rows
            assert total == ROWS
            # Ownership transfers to pyarrow; dropping them must release the
            # native Arrow buffers.
            del batches
            gc.collect()
        finally:
            result.close()

        table_result = reader.query_table(
            "measures", columns, 0, ROWS + 1, batch_size=BATCH_SIZE
        )
        try:
            table = table_result.read_arrow_batch()
            assert table is not None
            assert table.num_rows > 0
            assert table.num_rows <= BATCH_SIZE
            del table
            gc.collect()
        finally:
            table_result.close()


def arrow_early_release(base):
    path = _fixture(base, "table_all.tsfile")
    with TsFileReader(path) as reader:
        result = reader.query_table(
            "measures", ["i64", "d"], 0, ROWS + 1, batch_size=BATCH_SIZE
        )
        try:
            batch = result.read_arrow_record_batch()
            assert batch is not None
            del batch
            gc.collect()
        finally:
            result.close()


def early_termination(base):
    tree_path = _fixture(base, "tree_all.tsfile")
    table_path = _fixture(base, "table_all.tsfile")
    with TsFileReader(tree_path) as reader:
        with reader.query_timeseries(
            TREE_DEVICES[1], ["s", "blob"], 0, ROWS + 1
        ) as result:
            assert result.next()
            assert result.next()
    with TsFileReader(table_path) as reader:
        with reader.query_table(
            "measures", ["s", "d"], 0, ROWS + 1, batch_size=BATCH_SIZE
        ) as result:
            assert result.read_arrow_record_batch() is not None


def exception_cleanup(base):
    path = _fixture(base, "table_all.tsfile")
    for name in ("bad_tree.tsfile", "bad_table.tsfile"):
        (base / name).unlink(missing_ok=True)
    with TsFileReader(path) as reader:
        try:
            with reader.query_table("measures", ["i64", "s"]) as result:
                assert result.next()
                raise RuntimeError("intentional cleanup test")
        except RuntimeError:
            pass

        try:
            result = reader.query_table("missing_table", ["nope"])
            try:
                result.next()
            finally:
                result.close()
        except Exception:
            pass

    bad_tree_path = str(base / "bad_tree.tsfile")
    writer = TsFileWriter(bad_tree_path)
    try:
        writer.register_timeseries("root.bad", TimeseriesSchema("bad", None))
    except ValueError:
        pass
    else:
        raise AssertionError("invalid timeseries schema was accepted")
    writer.close()

    bad_table_path = str(base / "bad_table.tsfile")
    try:
        TsFileTableWriter(
            bad_table_path,
            TableSchema("bad", [ColumnSchema("x", 123)]),
        )
    except ValueError:
        pass
    else:
        raise AssertionError("invalid table schema was accepted")


def repeated_open_close(base):
    tree_path = _fixture(base, "tree_all.tsfile")
    table_path = _fixture(base, "table_all.tsfile")
    for _ in range(40):
        with TsFileReader(tree_path) as reader:
            with reader.query_timeseries(
                TREE_DEVICES[0], ["i64", "s"], 0, ROWS + 1
            ) as result:
                assert result.next()
        with TsFileReader(table_path) as reader:
            with reader.query_table("measures", ["i64"], 0, ROWS + 1) as result:
                assert result.next()


def lifetime_interactions(base):
    path = _fixture(base, "table_all.tsfile")

    reader = TsFileReader(path)
    result = reader.query_table("measures", ["i64", "s"], 0, ROWS + 1)
    assert result.next()
    reader.close()
    try:
        result.next()
    except Exception as exc:
        assert "Invalid result set" in str(exc), exc
    else:
        raise AssertionError("result remained readable after the reader closed")
    result.close()
    result.close()
    reader.close()

    reader = TsFileReader(path)
    result = reader.query_table("measures", ["i64"], 0, ROWS + 1)
    assert result.next()
    result.close()
    result.close()
    reader.close()

    def scope_result_first():
        local_reader = TsFileReader(path)
        local_result = local_reader.query_table("measures", ["i64"], 0, ROWS + 1)
        assert local_result.next()
        local_result.close()
        return local_reader

    scope_result_first()
    gc.collect()

    def scope_reader_first():
        local_reader = TsFileReader(path)
        local_result = local_reader.query_table("measures", ["i64"], 0, ROWS + 1)
        assert local_result.next()
        return local_result

    scope_reader_first()
    gc.collect()


def import_only(base):
    assert sys.modules["tsfile"].__name__ == "tsfile"


def verify_runtime(base):
    maps = Path("/proc/self/maps").read_text()
    assert "libasan" in maps, "ASan runtime is not mapped into the process"
    package = Path(sys.modules["tsfile"].__file__).resolve().parent
    assert any(
        "libtsfile" in line and str(package) in line for line in maps.splitlines()
    ), "libtsfile from the instrumented build is not mapped"


def _leak_beyond_recognition():
    libc = ctypes.CDLL(None)
    libc.malloc.restype = ctypes.c_void_p

    def worker():
        assert libc.malloc(4096), "malloc failed"

    # The pointer lives only on the worker's stack, which is gone after join();
    # no live stack or register can keep the block reachable for LeakSanitizer.
    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()


def positive_control_leak(base):
    _leak_beyond_recognition()
    gc.collect()


def positive_control_uaf(base):
    libc = ctypes.CDLL(None)
    libc.malloc.restype = ctypes.c_void_p
    pointer = libc.malloc(64)
    assert pointer, "malloc failed"
    libc.free(ctypes.c_void_p(pointer))
    # ASan intercepts both calls and reports the write after free.
    libc.memset(ctypes.c_void_p(pointer), 0x41, 8)


SCENARIOS = {
    "build_fixtures": build_fixtures,
    "tree_full": tree_full,
    "table_full": table_full,
    "metadata_reads": metadata_reads,
    "file_object_read": file_object_read,
    "dataset_read": dataset_read,
    "arrow_full": arrow_full,
    "arrow_early_release": arrow_early_release,
    "early_termination": early_termination,
    "exception_cleanup": exception_cleanup,
    "repeated_open_close": repeated_open_close,
    "lifetime_interactions": lifetime_interactions,
    "import_only": import_only,
    "verify_runtime": verify_runtime,
    "positive_control_leak": positive_control_leak,
    "positive_control_uaf": positive_control_uaf,
}


def main(argv):
    if len(argv) != 3:
        print(
            f"usage: {Path(argv[0]).name} <scenario> <work-dir>",
            file=sys.stderr,
        )
        return 2
    scenario = SCENARIOS.get(argv[1])
    if scenario is None:
        print(f"unknown scenario: {argv[1]}", file=sys.stderr)
        return 2
    scenario(Path(argv[2]).resolve())
    print(f"scenario {argv[1]}: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
