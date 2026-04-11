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
"""query_tree_by_row on 3-segment device paths (Path / StringArrayDeviceID alignment)."""

import os

from tsfile import Field, RowRecord, TimeseriesSchema, TsFileReader, TsFileWriter
from tsfile import TSDataType


def _remove_if_exists(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)


def test_query_tree_by_row_two_level_device_ok():
    """Two-segment device path: query_tree_by_row should succeed."""
    path = "repro_query_tree_by_row_two_level.tsfile"
    device = "root.device1"
    measurements = ["s1"]
    try:
        _remove_if_exists(path)
        writer = TsFileWriter(path)
        writer.register_timeseries(
            device, TimeseriesSchema("s1", TSDataType.INT64)
        )
        writer.write_row_record(
            RowRecord(
                device,
                0,
                [Field("s1", 42, TSDataType.INT64)],
            )
        )
        writer.close()

        with TsFileReader(path) as reader:
            schemas = reader.get_all_timeseries_schemas()
            assert device.lower() in schemas
            rs = reader.query_tree_by_row([device], measurements, 0, 10)
            assert rs.next()
            assert rs.get_value_by_index(2) == 42
            rs.close()
    finally:
        _remove_if_exists(path)


def test_query_tree_by_row_three_level_device_ok():
    """Three-segment device path: query_tree_by_row should succeed (same as two-level)."""
    path = "repro_query_tree_by_row_three_level.tsfile"
    device = "root.db.device1"
    measurements = ["s1", "s2"]
    try:
        _remove_if_exists(path)
        writer = TsFileWriter(path)
        writer.register_timeseries(
            device, TimeseriesSchema("s1", TSDataType.INT64)
        )
        writer.register_timeseries(
            device, TimeseriesSchema("s2", TSDataType.INT64)
        )
        writer.write_row_record(
            RowRecord(
                device,
                0,
                [
                    Field("s1", 1, TSDataType.INT64),
                    Field("s2", 2, TSDataType.INT64),
                ],
            )
        )
        writer.close()

        with TsFileReader(path) as reader:
            schemas = reader.get_all_timeseries_schemas()
            assert device.lower() in schemas
            assert len(schemas[device.lower()].get_timeseries_list()) == 2

            rs = reader.query_tree_by_row([device], measurements, 0, 10)
            assert rs.next()
            assert rs.get_value_by_index(1) == 0
            assert rs.get_value_by_index(2) == 1
            assert rs.get_value_by_index(3) == 2
            rs.close()
    finally:
        _remove_if_exists(path)
