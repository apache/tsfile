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
"""Regression tests for caller-owned native value buffers.

The sanitizer job covers the same paths with LeakSanitizer; these assertions
keep the functional behavior (BLOB round trips and repeated schema
registration) correct in ordinary builds.
"""

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    Field,
    RowRecord,
    TableSchema,
    TSDataType,
    Tablet,
    TimeseriesSchema,
    TsFileReader,
    TsFileTableWriter,
    TsFileWriter,
)


def test_blob_preserves_embedded_nul_bytes(tmp_path):
    path = str(tmp_path / "blob_nul.tsfile")
    schema = TableSchema(
        "t",
        [
            ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
            ColumnSchema("payload", TSDataType.BLOB, ColumnCategory.FIELD),
        ],
    )
    payloads = [b"a\x00b", b"\x00\x00", b"", b"tail\x00"]

    with TsFileTableWriter(path, schema) as writer:
        tablet = Tablet(
            ["device", "payload"],
            [TSDataType.STRING, TSDataType.BLOB],
            len(payloads),
        )
        for row, payload in enumerate(payloads):
            tablet.add_timestamp(row, row)
            tablet.add_value_by_name("device", row, "d1")
            tablet.add_value_by_name("payload", row, payload)
        writer.write_table(tablet)

    with TsFileReader(path) as reader:
        result = reader.query_table("t", ["device", "payload"])
        try:
            values = []
            while result.next():
                values.append(result.get_value_by_index(3))
        finally:
            result.close()

    assert values == payloads


def test_repeated_schema_registration_round_trip(tmp_path):
    path = str(tmp_path / "many_series.tsfile")
    series_count = 64
    writer = TsFileWriter(path)
    for index in range(series_count):
        writer.register_timeseries(
            "root.device1", TimeseriesSchema(f"s{index}", TSDataType.INT64)
        )
    for timestamp in range(4):
        fields = [
            Field(f"s{index}", timestamp * 1000 + index, TSDataType.INT64)
            for index in range(series_count)
        ]
        writer.write_row_record(RowRecord("root.device1", timestamp, fields))
    writer.close()

    with TsFileReader(path) as reader:
        schemas = reader.get_all_timeseries_schemas()
        assert set(schemas) == {"root.device1"}
        assert len(schemas["root.device1"].get_timeseries_list()) == series_count
        result = reader.query_timeseries(
            "root.device1", [f"s{index}" for index in range(series_count)], 0, 10
        )
        try:
            rows = 0
            while result.next():
                assert result.get_value_by_index(2) == rows * 1000
                rows += 1
            assert rows == 4
        finally:
            result.close()
