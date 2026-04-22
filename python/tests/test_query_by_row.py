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
    ColumnCategory,
    ColumnSchema,
    Field,
    RowRecord,
    TableSchema,
    TSDataType,
)
from tsfile import (
    TimeseriesSchema,
    TsFileReader,
    TsFileTableWriter,
    TsFileWriter,
    Tablet,
)


def test_query_tree_by_row_offset_limit():
    file_path = "python_tree_query_by_row_test.tsfile"
    if os.path.exists(file_path):
        os.remove(file_path)

    try:
        device_ids = ["root.d1", "root.d2"]
        measurement_names = ["s1", "s2"]
        num_rows = 10

        writer = TsFileWriter(file_path)
        for device_id in device_ids:
            for measurement in measurement_names:
                writer.register_timeseries(
                    device_id, TimeseriesSchema(measurement, TSDataType.INT64)
                )

        for t in range(num_rows):
            for dev_idx, device_id in enumerate(device_ids):
                fields = []
                for meas_idx, measurement in enumerate(measurement_names):
                    value = t * 100 + meas_idx + dev_idx * 10000
                    fields.append(Field(measurement, value, TSDataType.INT64))
                writer.write_row_record(RowRecord(device_id, t, fields))

        writer.close()

        reader = TsFileReader(file_path)
        offset = 3
        limit = 5
        with reader.query_tree_by_row(
            device_ids, measurement_names, offset, limit
        ) as result:
            row = 0
            while result.next():
                ts = result.get_value_by_index(1)
                assert ts == offset + row
                # Column order follows (device_ids outer loop) + (measurement_names inner loop).
                assert result.get_value_by_index(2) == ts * 100 + 0 + 0 * 10000
                assert result.get_value_by_index(3) == ts * 100 + 1 + 0 * 10000
                assert result.get_value_by_index(4) == ts * 100 + 0 + 1 * 10000
                assert result.get_value_by_index(5) == ts * 100 + 1 + 1 * 10000
                row += 1
            assert row == limit
        reader.close()
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def test_query_tree_by_row_multi_segment_device():
    file_path = "python_tree_query_by_row_multiseg_test.tsfile"
    if os.path.exists(file_path):
        os.remove(file_path)

    try:
        device_id = "root.sg1.FeederA"
        measurement_names = ["s1"]
        num_rows = 10

        writer = TsFileWriter(file_path)
        for measurement in measurement_names:
            writer.register_timeseries(
                device_id, TimeseriesSchema(measurement, TSDataType.INT64)
            )

        for t in range(num_rows):
            fields = [Field(measurement_names[0], t * 100, TSDataType.INT64)]
            writer.write_row_record(RowRecord(device_id, t, fields))

        writer.close()

        reader = TsFileReader(file_path)
        limit = 5
        with reader.query_tree_by_row(
            [device_id], measurement_names, 0, limit
        ) as result:
            row = 0
            while result.next():
                ts = result.get_value_by_index(1)
                assert ts == row
                assert result.get_value_by_index(2) == ts * 100
                row += 1
            assert row == limit
        reader.close()
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def test_query_table_by_row_offset_limit():
    file_path = "python_table_query_by_row_test.tsfile"
    if os.path.exists(file_path):
        os.remove(file_path)

    try:
        table_name = "t1"
        schema = TableSchema(
            table_name,
            [
                ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
                ColumnSchema("s1", TSDataType.INT64, ColumnCategory.FIELD),
            ],
        )

        num_rows = 10
        with TsFileTableWriter(file_path, schema) as writer:
            tablet = Tablet(
                ["device", "s1"], [TSDataType.STRING, TSDataType.INT64], num_rows
            )
            for t in range(num_rows):
                tablet.add_timestamp(t, t)
                tablet.add_value_by_name("device", t, f"device_{t}")
                tablet.add_value_by_name("s1", t, t * 10)
            writer.write_table(tablet)

        reader = TsFileReader(file_path)
        offset = 3
        limit = 5
        with reader.query_table_by_row(
            table_name, ["device", "s1"], offset, limit
        ) as result:
            row = 0
            while result.next():
                ts = result.get_value_by_index(1)
                assert ts == offset + row
                assert result.get_value_by_index(2) == f"device_{ts}"
                assert result.get_value_by_index(3) == ts * 10
                row += 1
            assert row == limit
        reader.close()
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)


def test_query_tree_by_row_skips_missing_device_and_measurement():
    """Tree queryByRow: missing device or measurement paths are skipped (Java-aligned)."""
    file_path = "python_tree_query_by_row_skip_missing.tsfile"
    if os.path.exists(file_path):
        os.remove(file_path)

    try:
        device_ids = ["d1"]
        measurement_names = ["s1"]
        num_rows = 5

        writer = TsFileWriter(file_path)
        for device_id in device_ids:
            for measurement in measurement_names:
                writer.register_timeseries(
                    device_id, TimeseriesSchema(measurement, TSDataType.INT64)
                )

        for t in range(num_rows):
            fields = [Field("s1", t * 100 + 0, TSDataType.INT64)]
            writer.write_row_record(RowRecord(device_ids[0], t, fields))

        writer.close()

        reader = TsFileReader(file_path)
        q_devices = ["d1", "d999"]
        q_measurements = ["s1", "ghost_m"]
        with reader.query_tree_by_row(q_devices, q_measurements, 0, -1) as result:
            assert result.get_metadata().get_column_num() == 2
            row = 0
            while result.next():
                ts = result.get_value_by_index(1)
                assert ts == row
                assert result.get_value_by_index(2) == row * 100 + 0
                row += 1
            assert row == num_rows
        reader.close()
    finally:
        if os.path.exists(file_path):
            os.remove(file_path)
