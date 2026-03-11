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

"""Tests for query_tree_by_row and query_table_by_row on TsFileReader."""

import pytest

from tsfile import (
    TsFileWriter,
    TsFileReader,
    TsFileTableWriter,
    TSDataType,
    TimeseriesSchema,
    ColumnSchema,
    TableSchema,
    ColumnCategory,
    Tablet,
    RowRecord,
    Field,
)

# ─────────────────────────────────────────────────────────────────────────────
# Shared constants
# ─────────────────────────────────────────────────────────────────────────────

TOTAL = 50
DEVICE = "root.device1"
MEA = "s1"
TABLE = "t1"


# ─────────────────────────────────────────────────────────────────────────────
# Write helpers
# ─────────────────────────────────────────────────────────────────────────────

def _write_tree_file(filepath, device, measurement, num_rows):
    """Write `num_rows` records for device/measurement (INT64).
    Timestamps are 0..num_rows-1, values are timestamp * 10.
    """
    writer = TsFileWriter(filepath)
    writer.register_timeseries(device, TimeseriesSchema(measurement, TSDataType.INT64))
    for i in range(num_rows):
        writer.write_row_record(
            RowRecord(device, i, [Field(measurement, i * 10, TSDataType.INT64)])
        )
    writer.close()


def _write_table_file(filepath, table_name, num_rows):
    """Write `num_rows` rows into a table with a single INT64 field column 's0'.
    Timestamps and values are both 0..num_rows-1.
    """
    schema = TableSchema(
        table_name,
        [ColumnSchema("s0", TSDataType.INT64, ColumnCategory.FIELD)],
    )
    with TsFileTableWriter(filepath, schema) as writer:
        tablet = Tablet(["s0"], [TSDataType.INT64], num_rows)
        for i in range(num_rows):
            tablet.add_timestamp(i, i)
            tablet.add_value_by_index(0, i, i)
        writer.write_table(tablet)


# ─────────────────────────────────────────────────────────────────────────────
# Count helpers
# ─────────────────────────────────────────────────────────────────────────────

def _count_tree(filepath, device, measurement, offset, limit):
    with TsFileReader(filepath) as r:
        rs = r.query_tree_by_row([device], [measurement], offset, limit)
        count = 0
        while rs.next():
            count += 1
        rs.close()
    return count


def _count_table(filepath, table_name, offset, limit):
    with TsFileReader(filepath) as r:
        rs = r.query_table_by_row(table_name, ["s0"], offset, limit)
        count = 0
        while rs.next():
            count += 1
        rs.close()
    return count


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def tree_file(tmp_path):
    fp = str(tmp_path / "tree_row_query.tsfile")
    _write_tree_file(fp, DEVICE, MEA, TOTAL)
    return fp


@pytest.fixture
def table_file(tmp_path):
    fp = str(tmp_path / "table_row_query.tsfile")
    _write_table_file(fp, TABLE, TOTAL)
    return fp


# ─────────────────────────────────────────────────────────────────────────────
# Tree model tests — query_tree_by_row
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryTreeByRow:

    # ① limit=0 → empty result
    def test_limit_zero(self, tree_file):
        assert _count_tree(tree_file, DEVICE, MEA, 0, 0) == 0

    # ② limit < total → exactly `limit` rows
    def test_limit_less_than_total(self, tree_file):
        assert _count_tree(tree_file, DEVICE, MEA, 0, 20) == 20

    # ③ limit > total → all rows
    def test_limit_exceeds_total(self, tree_file):
        assert _count_tree(tree_file, DEVICE, MEA, 0, 9999) == TOTAL

    # ④ limit=-1 → unlimited, returns all rows
    def test_negative_limit_means_unlimited(self, tree_file):
        assert _count_tree(tree_file, DEVICE, MEA, 0, -1) == TOTAL

    # ⑤ offset + limit in the middle
    def test_offset_plus_limit(self, tree_file):
        assert _count_tree(tree_file, DEVICE, MEA, 10, 15) == 15

    # ⑥ offset >= total → empty result
    def test_offset_beyond_total(self, tree_file):
        assert _count_tree(tree_file, DEVICE, MEA, 1000, 10) == 0

    # ⑦ offset + limit > total → return remaining rows
    def test_offset_plus_limit_exceeds_total(self, tree_file):
        # offset=40, limit=20 → 10 rows remain
        assert _count_tree(tree_file, DEVICE, MEA, 40, 20) == 10

    # ⑧ data correctness: verify timestamps start from `offset`
    def test_data_correctness(self, tree_file):
        with TsFileReader(tree_file) as r:
            rs = r.query_tree_by_row([DEVICE], [MEA], 5, 10)
            count = 0
            while rs.next():
                ts = rs.get_value_by_index(1)   # column 1 = time
                val = rs.get_value_by_index(2)  # column 2 = measurement
                assert ts == 5 + count
                assert val == (5 + count) * 10
                count += 1
            rs.close()
        assert count == 10

    # ⑨ paging consistency: two pages together equal the full result
    def test_pagination_consistency(self, tree_file):
        p1 = _count_tree(tree_file, DEVICE, MEA, 0, 25)
        p2 = _count_tree(tree_file, DEVICE, MEA, 25, 25)
        assert p1 + p2 == TOTAL

    # ⑩ metadata is accessible via the result set
    def test_metadata_accessible(self, tree_file):
        with TsFileReader(tree_file) as r:
            rs = r.query_tree_by_row([DEVICE], [MEA], 0, 5)
            meta = rs.get_metadata()
            assert meta is not None
            col_names = meta.get_column_list()
            assert "time" in col_names
            # Tree model returns full path as column name (e.g. "root.device1.s1")
            assert f"{DEVICE}.{MEA}".lower() in col_names
            rs.close()

    # ⑪ context-manager usage
    def test_context_manager(self, tree_file):
        count = 0
        with TsFileReader(tree_file) as r:
            with r.query_tree_by_row([DEVICE], [MEA], 0, 10) as rs:
                while rs.next():
                    count += 1
        assert count == 10

    # ⑫ multiple devices: offset/limit applied to merged result
    def test_multiple_devices(self, tmp_path):
        fp = str(tmp_path / "tree_multi_device.tsfile")
        # Both devices share the same timestamps 0..19.
        # The merger collapses identical timestamps → 20 distinct rows.
        # Use a single writer for both devices.
        writer = TsFileWriter(fp)
        writer.register_timeseries("device_1", TimeseriesSchema(MEA, TSDataType.INT64))
        writer.register_timeseries("device_2", TimeseriesSchema(MEA, TSDataType.INT64))
        for i in range(20):
            writer.write_row_record(
                RowRecord("device_1", i, [Field(MEA, i * 10, TSDataType.INT64)])
            )
            writer.write_row_record(
                RowRecord("device_2", i, [Field(MEA, i * 20, TSDataType.INT64)])
            )
        writer.close()

        with TsFileReader(fp) as r:
            rs = r.query_tree_by_row(["device_1", "device_2"], [MEA], 5, 10)
            count = 0
            while rs.next():
                count += 1
            rs.close()
        assert count == 10


# ─────────────────────────────────────────────────────────────────────────────
# Table model tests — query_table_by_row
# ─────────────────────────────────────────────────────────────────────────────

class TestQueryTableByRow:

    # ① limit=0 → empty result
    def test_limit_zero(self, table_file):
        assert _count_table(table_file, TABLE, 0, 0) == 0

    # ② limit < total → exactly `limit` rows
    def test_limit_less_than_total(self, table_file):
        assert _count_table(table_file, TABLE, 0, 10) == 10

    # ③ limit > total → all rows
    def test_limit_exceeds_total(self, table_file):
        assert _count_table(table_file, TABLE, 0, 9999) == TOTAL

    # ④ limit=-1 → unlimited, returns all rows
    def test_negative_limit_means_unlimited(self, table_file):
        assert _count_table(table_file, TABLE, 0, -1) == TOTAL

    # ⑤ offset + limit in the middle
    def test_offset_plus_limit(self, table_file):
        assert _count_table(table_file, TABLE, 10, 15) == 15

    # ⑥ offset >= total → empty result
    def test_offset_beyond_total(self, table_file):
        assert _count_table(table_file, TABLE, 1000, 10) == 0

    # ⑦ offset + limit > total → return remaining rows
    def test_offset_plus_limit_exceeds_total(self, table_file):
        # offset=40, limit=20 → 10 rows remain
        assert _count_table(table_file, TABLE, 40, 20) == 10

    # ⑧ data correctness: timestamps and values start from `offset`
    def test_data_correctness(self, table_file):
        with TsFileReader(table_file) as r:
            rs = r.query_table_by_row(TABLE, ["s0"], 5, 10)
            count = 0
            while rs.next():
                ts = rs.get_value_by_index(1)   # column 1 = time
                val = rs.get_value_by_index(2)  # column 2 = s0
                assert ts == 5 + count
                assert val == 5 + count
                count += 1
            rs.close()
        assert count == 10

    # ⑨ paging consistency: two pages together equal the full result
    def test_pagination_consistency(self, table_file):
        p1 = _count_table(table_file, TABLE, 0, 25)
        p2 = _count_table(table_file, TABLE, 25, 25)
        assert p1 + p2 == TOTAL

    # ⑩ metadata is accessible via the result set
    def test_metadata_accessible(self, table_file):
        with TsFileReader(table_file) as r:
            rs = r.query_table_by_row(TABLE, ["s0"], 0, 5)
            meta = rs.get_metadata()
            assert meta is not None
            col_names = meta.get_column_list()
            assert "time" in col_names
            assert "s0" in col_names
            rs.close()

    # ⑪ context-manager usage
    def test_context_manager(self, table_file):
        count = 0
        with TsFileReader(table_file) as r:
            with r.query_table_by_row(TABLE, ["s0"], 0, 10) as rs:
                while rs.next():
                    count += 1
        assert count == 10

    # ⑫ multiple flushes (multiple chunks): offset/limit still correct
    def test_multiple_chunks_correctness(self, tmp_path):
        fp = str(tmp_path / "table_multi_chunk.tsfile")
        schema = TableSchema(
            TABLE,
            [ColumnSchema("s0", TSDataType.INT64, ColumnCategory.FIELD)],
        )
        with TsFileTableWriter(fp, schema) as writer:
            # First chunk: rows 0..29
            tablet1 = Tablet(["s0"], [TSDataType.INT64], 30)
            for i in range(30):
                tablet1.add_timestamp(i, i)
                tablet1.add_value_by_index(0, i, i)
            writer.write_table(tablet1)
            writer.flush()

            # Second chunk: rows 30..59
            tablet2 = Tablet(["s0"], [TSDataType.INT64], 30)
            for i in range(30):
                tablet2.add_timestamp(i, 30 + i)
                tablet2.add_value_by_index(0, i, 30 + i)
            writer.write_table(tablet2)

        # offset=25, limit=20 → rows 25..44
        with TsFileReader(fp) as r:
            rs = r.query_table_by_row(TABLE, ["s0"], 25, 20)
            count = 0
            while rs.next():
                ts = rs.get_value_by_index(1)
                assert ts == 25 + count
                count += 1
            rs.close()
        assert count == 20
