#!/usr/bin/env python3
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

"""Current table-model write/read example for the TsFile Python binding."""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd

try:
    from tsfile import (
        ColumnCategory,
        ColumnSchema,
        TableSchema,
        Tablet,
        TSDataType,
        TsFileReader,
        TsFileTableWriter,
    )
except ImportError:
    print("TsFile Python library not found.")
    print("Build it from the repository root with: ./mvnw -P with-python clean verify")
    sys.exit(1)


def generate_sample_data(num_devices=2, records_per_device=10):
    rows = []
    base_time = datetime.now()
    for device_id in range(1, num_devices + 1):
        for offset in range(records_per_device):
            rows.append(
                {
                    "timestamp": base_time + timedelta(seconds=offset * 10),
                    "device": f"sensor_{device_id:02d}",
                    "temperature": 20.0 + device_id + offset * 0.5,
                    "online": offset % 5 != 0,
                }
            )
    return pd.DataFrame(rows)


def normalize_schemas(schemas):
    if isinstance(schemas, dict):
        return schemas
    return {schema.get_table_name(): schema for schema in schemas or []}


def write_example(file_name):
    columns = [
        ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
        ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
        ColumnSchema("online", TSDataType.BOOLEAN, ColumnCategory.FIELD),
    ]
    schema = TableSchema("sensors", columns)
    frame = generate_sample_data()

    with TsFileTableWriter(file_name, schema) as writer:
        batch_size = 10
        for start in range(0, len(frame), batch_size):
            batch = frame.iloc[start : start + batch_size]
            tablet = Tablet(
                schema.get_column_names(),
                [column.get_data_type() for column in schema.get_columns()],
                len(batch),
            )
            for row_index, (_, row) in enumerate(batch.iterrows()):
                timestamp_ms = int(row["timestamp"].timestamp() * 1000)
                tablet.add_timestamp(row_index, timestamp_ms)
                tablet.add_value_by_name("device", row_index, str(row["device"]))
                tablet.add_value_by_name(
                    "temperature", row_index, float(row["temperature"])
                )
                tablet.add_value_by_name("online", row_index, bool(row["online"]))
            writer.write_table(tablet)


def read_example(file_name):
    with TsFileReader(file_name) as reader:
        schemas = normalize_schemas(reader.get_all_table_schemas())
        for table_name, schema in schemas.items():
            columns = [
                column.get_column_name()
                for column in schema.get_columns()
                if column.get_category() != ColumnCategory.TIME
            ]
            with reader.query_table_by_row(table_name, columns, limit=5) as result:
                print(result.read_data_frame(max_row_num=5))


def main():
    file_name = "example_python.tsfile"
    if os.path.exists(file_name):
        os.remove(file_name)
    write_example(file_name)
    read_example(file_name)
    print(f"Generated {file_name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
