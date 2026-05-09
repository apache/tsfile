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
    ColumnSchema,
    TableSchema,
    TSDataType,
    ColumnCategory,
    TsFileTableWriter,
    TsFileReader,
    Tablet,
    tag_eq,
    tag_neq,
    tag_lt,
    tag_lteq,
    tag_gt,
    tag_gteq,
    tag_regexp,
    tag_not_regexp,
    tag_between,
    tag_not_between,
)

TSFILE_PATH = "test_tag_filter.tsfile"

# Schema: table "sensors" with TAG columns "region" and "device",
# and FIELD column "value" (DOUBLE).
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
    """Write the test TsFile once for all tests in this module."""
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


def _query_values(reader, tag_filter):
    """Helper: query all columns with the given tag_filter, return list of (region, device, value) tuples."""
    result = reader.query_table(
        TABLE_NAME, ["region", "device", "value"], tag_filter=tag_filter
    )
    rows = []
    while result.next():
        region = result.get_value_by_name("region")
        device = result.get_value_by_name("device")
        value = result.get_value_by_name("value")
        rows.append((region, device, value))
    result.close()
    return rows


def test_tag_eq():
    with TsFileReader(TSFILE_PATH) as reader:
        rows = _query_values(reader, tag_eq("region", "north"))
        assert len(rows) == 10  # dev_a (5) + dev_b (5)
        assert all(r[0] == "north" for r in rows)


def test_tag_neq():
    with TsFileReader(TSFILE_PATH) as reader:
        rows = _query_values(reader, tag_neq("region", "north"))
        assert len(rows) == 10  # south (5) + east (5)
        assert all(r[0] != "north" for r in rows)


def test_tag_eq_device():
    with TsFileReader(TSFILE_PATH) as reader:
        rows = _query_values(reader, tag_eq("device", "dev_c"))
        assert len(rows) == 5
        assert all(r[1] == "dev_c" for r in rows)
        assert all(r[0] == "south" for r in rows)


def test_tag_lt():
    with TsFileReader(TSFILE_PATH) as reader:
        # Lexicographic: "east" < "north" < "south"
        rows = _query_values(reader, tag_lt("region", "north"))
        assert len(rows) == 5  # only "east"
        assert all(r[0] == "east" for r in rows)


def test_tag_lteq():
    with TsFileReader(TSFILE_PATH) as reader:
        rows = _query_values(reader, tag_lteq("region", "north"))
        assert len(rows) == 15  # "east" (5) + "north" (10)


def test_tag_gt():
    with TsFileReader(TSFILE_PATH) as reader:
        rows = _query_values(reader, tag_gt("region", "north"))
        assert len(rows) == 5  # only "south"
        assert all(r[0] == "south" for r in rows)


def test_tag_gteq():
    with TsFileReader(TSFILE_PATH) as reader:
        rows = _query_values(reader, tag_gteq("region", "north"))
        assert len(rows) == 15  # "north" (10) + "south" (5)


def test_tag_between():
    with TsFileReader(TSFILE_PATH) as reader:
        # Between "east" and "north" (inclusive)
        rows = _query_values(reader, tag_between("region", "east", "north"))
        assert len(rows) == 15  # "east" (5) + "north" (10)


def test_tag_not_between():
    with TsFileReader(TSFILE_PATH) as reader:
        rows = _query_values(reader, tag_not_between("region", "east", "north"))
        assert len(rows) == 5  # only "south"
        assert all(r[0] == "south" for r in rows)


def test_tag_regexp():
    with TsFileReader(TSFILE_PATH) as reader:
        # Match regions starting with 'n' or 's'
        rows = _query_values(reader, tag_regexp("region", "^[ns]"))
        assert len(rows) == 15  # "north" (10) + "south" (5)


def test_tag_not_regexp():
    with TsFileReader(TSFILE_PATH) as reader:
        rows = _query_values(reader, tag_not_regexp("region", "^[ns]"))
        assert len(rows) == 5  # only "east"
        assert all(r[0] == "east" for r in rows)


def test_tag_and():
    with TsFileReader(TSFILE_PATH) as reader:
        f = tag_eq("region", "north") & tag_eq("device", "dev_a")
        rows = _query_values(reader, f)
        assert len(rows) == 5
        assert all(r[0] == "north" and r[1] == "dev_a" for r in rows)


def test_tag_or():
    with TsFileReader(TSFILE_PATH) as reader:
        f = tag_eq("region", "south") | tag_eq("region", "east")
        rows = _query_values(reader, f)
        assert len(rows) == 10
        regions = {r[0] for r in rows}
        assert regions == {"south", "east"}


def test_tag_not():
    with TsFileReader(TSFILE_PATH) as reader:
        f = ~tag_eq("region", "north")
        rows = _query_values(reader, f)
        assert len(rows) == 10  # south + east
        assert all(r[0] != "north" for r in rows)


def test_tag_complex_combination():
    with TsFileReader(TSFILE_PATH) as reader:
        # (region == "north" AND device == "dev_b") OR region == "east"
        f = (tag_eq("region", "north") & tag_eq("device", "dev_b")) | tag_eq(
            "region", "east"
        )
        rows = _query_values(reader, f)
        assert len(rows) == 10  # dev_b (5) + east (5)
        for r in rows:
            assert (r[0] == "north" and r[1] == "dev_b") or r[0] == "east"


def test_no_tag_filter():
    """Verify that query_table without tag_filter still works (backward compat)."""
    with TsFileReader(TSFILE_PATH) as reader:
        result = reader.query_table(TABLE_NAME, ["region", "device", "value"])
        rows = []
        while result.next():
            rows.append(result.get_value_by_name("region"))
        result.close()
        assert len(rows) == 20


def test_tag_filter_with_time_range():
    """Tag filter combined with time range."""
    with TsFileReader(TSFILE_PATH) as reader:
        result = reader.query_table(
            TABLE_NAME,
            ["region", "device", "value"],
            start_time=0,
            end_time=7,
            tag_filter=tag_eq("region", "north"),
        )
        rows = []
        while result.next():
            rows.append(result.get_value_by_name("value"))
        result.close()
        # north has timestamps 0..9, but time range 0..7 gives 8 rows
        assert len(rows) == 8
