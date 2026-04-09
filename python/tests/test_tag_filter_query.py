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

import pyarrow as pa
import pytest

from tsfile import (
    ColumnSchema,
    TableSchema,
    TSDataType,
    ColumnCategory,
    TsFileTableWriter,
    TsFileReader,
    Tablet,
    tag_eq,
    tag_gteq,
    TIME_COLUMN,
)

TSFILE_PATH = "test_tag_filter_query.tsfile"

TABLE_NAME = "sensors"
COLUMNS = [
    ColumnSchema("region", TSDataType.STRING, ColumnCategory.TAG),
    ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
    ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
]
SCHEMA = TableSchema(TABLE_NAME, COLUMNS)

# Test data:
# region  | device   | timestamps | values
# north   | dev_a    | 0..4       | 0.0..4.0
# north   | dev_b    | 5..9       | 5.0..9.0
# south   | dev_c    | 10..14     | 10.0..14.0
# east    | dev_d    | 15..19     | 15.0..19.0
DEVICES = [
    ("north", "dev_a", 0, 5),
    ("north", "dev_b", 5, 10),
    ("south", "dev_c", 10, 15),
    ("east", "dev_d", 15, 20),
]


@pytest.fixture(scope="module", autouse=True)
def create_tsfile():
    if os.path.exists(TSFILE_PATH):
        os.remove(TSFILE_PATH)

    with TsFileTableWriter(TSFILE_PATH, SCHEMA) as writer:
        for region, device, start, end in DEVICES:
            count = end - start
            tablet = Tablet(
                ["region", "device", "value"],
                [TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE],
                count,
            )
            for i in range(count):
                ts = start + i
                tablet.add_timestamp(i, ts)
                tablet.add_value_by_name("region", i, region)
                tablet.add_value_by_name("device", i, device)
                tablet.add_value_by_name("value", i, float(ts))
            writer.write_table(tablet)

    yield

    if os.path.exists(TSFILE_PATH):
        os.remove(TSFILE_PATH)


# ---------------------------------------------------------------------------
# Helper: collect all rows from scalar (row-by-row) iteration
# ---------------------------------------------------------------------------
def _scalar_rows(result):
    rows = []
    while result.next():
        rows.append(
            (
                result.get_value_by_name("region"),
                result.get_value_by_name("device"),
                result.get_value_by_name("value"),
            )
        )
    return rows


# ---------------------------------------------------------------------------
# Helper: collect all rows from arrow batch iteration
# ---------------------------------------------------------------------------
def _arrow_rows(result):
    tables = []
    while True:
        batch = result.read_arrow_batch()
        if batch is None:
            break
        tables.append(batch)
    if not tables:
        return []
    combined = pa.concat_tables(tables)
    rows = []
    for i in range(combined.num_rows):
        rows.append(
            (
                combined.column("region")[i].as_py(),
                combined.column("device")[i].as_py(),
                combined.column("value")[i].as_py(),
            )
        )
    return rows


# ===========================================================================
# query_table with tag_filter — scalar mode
# ===========================================================================
class TestQueryTableTagFilterScalar:

    def test_eq_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "north"),
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 10
                assert all(r[0] == "north" for r in rows)

    def test_and_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            f = tag_eq("region", "north") & tag_eq("device", "dev_a")
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 5
                assert all(r[0] == "north" and r[1] == "dev_a" for r in rows)

    def test_or_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            f = tag_eq("region", "south") | tag_eq("region", "east")
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 10
                assert {r[0] for r in rows} == {"south", "east"}

    def test_with_time_range(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                start_time=0,
                end_time=7,
                tag_filter=tag_eq("region", "north"),
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 8
                assert all(r[0] == "north" for r in rows)

    def test_no_match(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "west"),
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 0


# ===========================================================================
# query_table with tag_filter — arrow batch mode
# ===========================================================================
class TestQueryTableTagFilterArrow:

    def test_eq_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "north"),
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 10
                assert all(r[0] == "north" for r in rows)

    def test_and_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            f = tag_eq("region", "north") & tag_eq("device", "dev_b")
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 5
                assert all(r[0] == "north" and r[1] == "dev_b" for r in rows)

    def test_or_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            f = tag_eq("region", "south") | tag_eq("region", "east")
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 10
                assert {r[0] for r in rows} == {"south", "east"}

    def test_with_time_range(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                start_time=0,
                end_time=7,
                tag_filter=tag_eq("region", "north"),
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 8

    def test_small_batch_size(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "north"),
                batch_size=3,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 10
                assert all(r[0] == "north" for r in rows)

    def test_no_match(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "west"),
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 0


# ===========================================================================
# query_table_by_row with tag_filter — scalar mode
# ===========================================================================
class TestQueryTableByRowTagFilterScalar:

    def test_eq_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "north"),
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 10
                assert all(r[0] == "north" for r in rows)

    def test_and_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            f = tag_eq("region", "north") & tag_eq("device", "dev_a")
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 5
                assert all(r[0] == "north" and r[1] == "dev_a" for r in rows)

    def test_or_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            f = tag_eq("region", "south") | tag_eq("region", "east")
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 10
                assert {r[0] for r in rows} == {"south", "east"}

    def test_with_offset_limit(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                offset=2,
                limit=3,
                tag_filter=tag_eq("region", "north"),
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 3

    def test_gteq_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_gteq("region", "north"),
            ) as result:
                rows = _scalar_rows(result)
                # "north" (10) + "south" (5) = 15
                assert len(rows) == 15
                assert all(r[0] in ("north", "south") for r in rows)

    def test_no_match(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "west"),
            ) as result:
                rows = _scalar_rows(result)
                assert len(rows) == 0


# ===========================================================================
# query_table_by_row with tag_filter — arrow batch mode
# ===========================================================================
class TestQueryTableByRowTagFilterArrow:

    def test_eq_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "north"),
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 10
                assert all(r[0] == "north" for r in rows)

    def test_and_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            f = tag_eq("region", "north") & tag_eq("device", "dev_b")
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 5
                assert all(r[0] == "north" and r[1] == "dev_b" for r in rows)

    def test_or_filter(self):
        with TsFileReader(TSFILE_PATH) as reader:
            f = tag_eq("region", "south") | tag_eq("region", "east")
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 10
                assert {r[0] for r in rows} == {"south", "east"}

    def test_with_offset_limit(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                offset=0,
                limit=6,
                tag_filter=tag_eq("region", "north"),
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 6
                assert all(r[0] == "north" for r in rows)

    def test_small_batch_size(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "south"),
                batch_size=2,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 5
                assert all(r[0] == "south" for r in rows)

    def test_no_match(self):
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=tag_eq("region", "west"),
                batch_size=1024,
            ) as result:
                rows = _arrow_rows(result)
                assert len(rows) == 0


# ===========================================================================
# Cross-check: scalar vs arrow return same data
# ===========================================================================
class TestScalarArrowConsistency:

    def test_query_table_scalar_vs_arrow(self):
        f = tag_eq("region", "north") & tag_eq("device", "dev_a")
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
            ) as result:
                scalar_rows = _scalar_rows(result)

        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
                batch_size=1024,
            ) as result:
                arrow_rows = _arrow_rows(result)

        assert len(scalar_rows) == len(arrow_rows)
        for s, a in zip(sorted(scalar_rows), sorted(arrow_rows)):
            assert s[0] == a[0]
            assert s[1] == a[1]
            assert s[2] == pytest.approx(a[2])

    def test_query_table_by_row_scalar_vs_arrow(self):
        f = tag_eq("region", "south")
        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
            ) as result:
                scalar_rows = _scalar_rows(result)

        with TsFileReader(TSFILE_PATH) as reader:
            with reader.query_table_by_row(
                TABLE_NAME,
                ["region", "device", "value"],
                tag_filter=f,
                batch_size=1024,
            ) as result:
                arrow_rows = _arrow_rows(result)

        assert len(scalar_rows) == len(arrow_rows)
        for s, a in zip(sorted(scalar_rows), sorted(arrow_rows)):
            assert s[0] == a[0]
            assert s[1] == a[1]
            assert s[2] == pytest.approx(a[2])
