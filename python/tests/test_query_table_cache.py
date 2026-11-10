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

"""Repeated table queries reuse prepared series (TSFILE_QUERY_CACHE_SIZE)."""

import os

import pytest

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    Tablet,
    TSDataType,
    TsFileReader,
    TsFileTableWriter,
    tag_eq,
    tag_gteq,
)

TSFILE_PATH = "test_query_table_cache.tsfile"
TABLE = "sensors"
SCHEMA = TableSchema(
    TABLE,
    [
        ColumnSchema("region", TSDataType.STRING, ColumnCategory.TAG),
        ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
        ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ColumnSchema("other", TSDataType.DOUBLE, ColumnCategory.FIELD),
    ],
)

# device -> (region, timestamps, values); None leaves the cell unset (null).
DEVICES = {
    "dev_a": ("north", list(range(100)), [float(i) for i in range(100)]),
    "dev_b": ("north", list(range(50, 150)), [float(i) for i in range(50, 150)]),
    "dev_c": ("south", list(range(0, 200, 2)), [float(i) for i in range(0, 200, 2)]),
    "dev_null": (
        "south",
        list(range(10)),
        [None if i % 3 else float(i) for i in range(10)],
    ),
}


@pytest.fixture(scope="module", autouse=True)
def create_tsfile():
    if os.path.exists(TSFILE_PATH):
        os.remove(TSFILE_PATH)
    with TsFileTableWriter(TSFILE_PATH, SCHEMA) as writer:
        for device, (region, timestamps, values) in DEVICES.items():
            tablet = Tablet(
                ["region", "device", "value", "other"],
                [
                    TSDataType.STRING,
                    TSDataType.STRING,
                    TSDataType.DOUBLE,
                    TSDataType.DOUBLE,
                ],
                len(timestamps),
            )
            for i, (timestamp, value) in enumerate(zip(timestamps, values)):
                tablet.add_timestamp(i, timestamp)
                tablet.add_value_by_name("region", i, region)
                tablet.add_value_by_name("device", i, device)
                if value is not None:
                    tablet.add_value_by_name("value", i, value)
                    tablet.add_value_by_name("other", i, value * 2.0)
            writer.write_table(tablet)
    yield
    if os.path.exists(TSFILE_PATH):
        os.remove(TSFILE_PATH)


def collect(reader, tag, start=None, end=None, columns=("value",), batch_size=8):
    kwargs = {}
    if start is not None:
        kwargs["start_time"] = start
    if end is not None:
        kwargs["end_time"] = end
    with reader.query_table(
        TABLE,
        list(columns),
        tag_filter=tag_eq("device", tag),
        batch_size=batch_size,
        **kwargs,
    ) as result:
        batches = []
        while True:
            batch = result.read_arrow_batch()
            if batch is None:
                break
            batches.append(batch.to_pydict())
        return batches


def fresh_reader(cache_size=None):
    if cache_size is None:
        os.environ.pop("TSFILE_QUERY_CACHE_SIZE", None)
    else:
        os.environ["TSFILE_QUERY_CACHE_SIZE"] = str(cache_size)
    return TsFileReader(TSFILE_PATH)


@pytest.fixture(autouse=True)
def restore_env():
    previous = os.environ.get("TSFILE_QUERY_CACHE_SIZE")
    yield
    if previous is None:
        os.environ.pop("TSFILE_QUERY_CACHE_SIZE", None)
    else:
        os.environ["TSFILE_QUERY_CACHE_SIZE"] = previous


@pytest.mark.parametrize("device", list(DEVICES))
def test_cached_results_match_uncached(device):
    with fresh_reader() as cached, fresh_reader(0) as uncached:
        for _ in range(3):
            assert collect(cached, device) == collect(uncached, device)


@pytest.mark.parametrize(
    "start,end",
    [(None, None), (0, 49), (50, 149), (10, 10), (150, 149)],
)
def test_time_ranges_match_uncached(start, end):
    with fresh_reader() as cached, fresh_reader(0) as uncached:
        assert collect(cached, "dev_c", start, end) == collect(
            uncached, "dev_c", start, end
        )


def test_repeated_queries_reuse_one_prepared_series():
    with fresh_reader() as reader:
        for _ in range(5):
            collect(reader, "dev_a")
        assert reader.prepared_query_cache_size() == 1
        collect(reader, "dev_b")
        assert reader.prepared_query_cache_size() == 2
        reader.clear_prepared_query_cache()
        assert reader.prepared_query_cache_size() == 0
        assert collect(reader, "dev_a")


def test_multi_device_filter_does_not_use_single_series_cache():
    with fresh_reader() as cached, fresh_reader(0) as uncached:
        # region == south matches two devices: the cache must fall back to the
        # regular query path and still return both devices.
        def region_rows(reader):
            with reader.query_table(
                TABLE, ["value"], tag_filter=tag_eq("region", "south"), batch_size=8
            ) as result:
                rows = []
                while True:
                    batch = result.read_arrow_batch()
                    if batch is None:
                        break
                    rows.append(batch.to_pydict())
                return rows

        expected = region_rows(uncached)
        for _ in range(3):
            assert region_rows(cached) == expected
        # The negative entry keeps the reader from retrying every call.
        assert cached.prepared_query_cache_size() == 1


def test_cache_eviction_keeps_results():
    with fresh_reader(1) as cached, fresh_reader(0) as uncached:
        for device in ("dev_a", "dev_b", "dev_c", "dev_a"):
            assert collect(cached, device) == collect(uncached, device)
            assert cached.prepared_query_cache_size() <= 1


def test_row_mode_unchanged():
    with fresh_reader() as cached, fresh_reader(0) as uncached:
        # Row-by-row mode never uses the cache.
        def rows(reader):
            with reader.query_table(
                TABLE,
                ["value"],
                start_time=0,
                end_time=99,
                tag_filter=tag_eq("device", "dev_a"),
                batch_size=0,
            ) as result:
                out = []
                while result.next():
                    out.append(result.get_value_by_name("value"))
                return out

        assert rows(cached) == rows(uncached)
        assert cached.prepared_query_cache_size() == 0


def test_two_columns_are_cached_and_match_uncached():
    with fresh_reader() as cached, fresh_reader(0) as uncached:
        for columns in (("value", "other"), ("other", "value")):
            expected = collect(uncached, "dev_a", columns=columns)
            for _ in range(3):
                assert collect(cached, "dev_a", columns=columns) == expected
        # One cache entry per distinct column combination.
        assert cached.prepared_query_cache_size() == 2


def test_cached_multi_column_keeps_requested_order():
    with fresh_reader() as reader:
        batches = collect(reader, "dev_a", columns=("other", "value"))
        assert list(batches[0]) == ["time", "other", "value"]


def test_tag_column_selection_falls_back():
    with fresh_reader() as cached, fresh_reader(0) as uncached:
        expected = collect(uncached, "dev_a", columns=("value", "region"))
        for _ in range(3):
            assert collect(cached, "dev_a", columns=("value", "region")) == expected


def test_compound_tag_filter_is_cacheable():
    with fresh_reader() as cached, fresh_reader(0) as uncached:

        def query(reader):
            with reader.query_table(
                TABLE,
                ["value"],
                tag_filter=tag_eq("region", "north") & tag_gteq("device", "dev_b"),
                batch_size=8,
            ) as result:
                batches = []
                while True:
                    batch = result.read_arrow_batch()
                    if batch is None:
                        break
                    batches.append(batch.to_pydict())
                return batches

        expected = query(uncached)
        for _ in range(3):
            assert query(cached) == expected
        assert cached.prepared_query_cache_size() == 1
