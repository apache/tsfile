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

"""
TsFile utility script for common operations

This script provides utilities for working with TsFile format:
- Demonstrating Python API CSV-to-TsFile conversion
- Reading TsFile metadata
- Basic validation operations
"""

import sys
import pandas as pd
import time
from pathlib import Path


def _table_schemas(reader):
    schemas = reader.get_all_table_schemas()
    if isinstance(schemas, dict):
        return schemas
    return {schema.get_table_name(): schema for schema in schemas or []}


def _schema_columns(schema):
    return [
        {
            "name": column.get_column_name(),
            "type": str(column.get_data_type()),
            "category": str(column.get_category()),
        }
        for column in schema.get_columns()
    ]


def _python_value(value, data_type, ts_data_type):
    if data_type in (ts_data_type.INT32, ts_data_type.INT64):
        return int(value)
    if data_type in (ts_data_type.FLOAT, ts_data_type.DOUBLE):
        return float(value)
    if data_type == ts_data_type.BOOLEAN:
        return bool(value)
    return str(value)


def csv_to_tsfile(
    csv_path, tsfile_path, device_column="device", timestamp_column="timestamp"
):
    """
    Convert CSV data to TsFile format

    Args:
        csv_path: Path to input CSV file
        tsfile_path: Path to output TsFile
        device_column: Name of device identifier column
        timestamp_column: Name of timestamp column
    """
    try:
        # Import TsFile after ensuring it's available
        from tsfile import ColumnSchema, TableSchema, Tablet
        from tsfile import TsFileTableWriter, TSDataType, ColumnCategory

        # Read CSV
        df = pd.read_csv(csv_path)
        normalized_names = [str(column).lower() for column in df.columns]
        if len(normalized_names) != len(set(normalized_names)):
            raise ValueError("CSV columns must remain unique after lower-casing")
        df.columns = normalized_names
        device_column = device_column.lower()
        timestamp_column = timestamp_column.lower()

        if device_column not in df.columns:
            raise ValueError(f"Device column '{device_column}' not found in CSV")

        if timestamp_column not in df.columns:
            raise ValueError(f"Timestamp column '{timestamp_column}' not found in CSV")

        if df[[device_column, timestamp_column]].isnull().any().any():
            raise ValueError(
                "Device and timestamp columns must not contain null values"
            )

        if not pd.api.types.is_numeric_dtype(df[timestamp_column]):
            df[timestamp_column] = pd.to_datetime(df[timestamp_column], errors="raise")

        df = df.sort_values([device_column, timestamp_column], kind="stable")
        if df.duplicated([device_column, timestamp_column]).any():
            raise ValueError("Timestamps must be unique within each device")

        # Infer column types and create schema.
        columns = [ColumnSchema(device_column, TSDataType.STRING, ColumnCategory.TAG)]

        for col in df.columns:
            if col in [device_column, timestamp_column]:
                continue

            dtype = df[col].dtype
            if pd.api.types.is_bool_dtype(dtype):
                tsfile_type = TSDataType.BOOLEAN
            elif pd.api.types.is_integer_dtype(dtype):
                tsfile_type = TSDataType.INT64
            elif pd.api.types.is_float_dtype(dtype):
                tsfile_type = TSDataType.DOUBLE
            else:
                tsfile_type = TSDataType.STRING

            columns.append(ColumnSchema(col, tsfile_type, ColumnCategory.FIELD))

        table_schema = TableSchema("data", columns=columns)

        # Write to TsFile
        with TsFileTableWriter(tsfile_path, table_schema) as writer:
            batch_size = 1000
            total_rows = len(df)

            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]

                tablet = Tablet(
                    [column.get_column_name() for column in columns],
                    [column.get_data_type() for column in columns],
                    len(batch_df),
                )

                for i, (_, row) in enumerate(batch_df.iterrows()):
                    # Convert timestamp to milliseconds
                    if pd.api.types.is_datetime64_any_dtype(df[timestamp_column]):
                        timestamp_ms = int(
                            pd.to_datetime(row[timestamp_column]).timestamp() * 1000
                        )
                    else:
                        timestamp_ms = int(row[timestamp_column])

                    tablet.add_timestamp(i, timestamp_ms)

                    for column in columns:
                        column_name = column.get_column_name()
                        value = row[column_name]
                        if column_name == device_column:
                            tablet.add_value_by_name(column_name, i, str(value))
                        else:
                            if pd.isna(value):
                                continue  # Skip null values
                            tablet.add_value_by_name(
                                column_name,
                                i,
                                _python_value(
                                    value, column.get_data_type(), TSDataType
                                ),
                            )

                writer.write_table(tablet)

        print(f"Successfully converted {csv_path} to {tsfile_path}")
        print(f"Processed {total_rows} rows")

    except ImportError:
        print("Error: TsFile Python library not found.")
        print(
            "Build it from the repository root with: ./mvnw -P with-python clean verify"
        )
        sys.exit(1)
    except Exception as e:
        print(f"Error converting CSV to TsFile: {e}")
        sys.exit(1)


def inspect_tsfile(tsfile_path):
    """
    Inspect TsFile and display metadata information

    Args:
        tsfile_path: Path to TsFile
    """
    try:
        from tsfile import TsFileReader

        with TsFileReader(tsfile_path) as reader:
            tables = _table_schemas(reader)
            print(f"TsFile: {tsfile_path}")
            print(f"Number of tables: {len(tables)}")

            for table_name, schema in tables.items():
                print(f"\nTable: {table_name}")
                for column in _schema_columns(schema):
                    print(
                        f"  - {column['name']}: {column['type']} ({column['category']})"
                    )

            print(
                "\nNote: current packaged Python bindings expose query_table/query_table_by_row"
            )
            print(
                "for row reads; this utility intentionally performs metadata inspection only."
            )

    except ImportError:
        print("Error: TsFile Python library not found.")
        print(
            "Build it from the repository root with: ./mvnw -P with-python clean verify"
        )
        sys.exit(1)
    except Exception as e:
        print(f"Error reading TsFile: {e}")
        sys.exit(1)


def validate_tsfile(tsfile_path):
    """
    Validate TsFile format and check for common issues

    Args:
        tsfile_path: Path to TsFile
    """
    try:
        from tsfile import TsFileReader

        print(f"Validating TsFile: {tsfile_path}")

        # Check if file exists
        if not Path(tsfile_path).exists():
            print("❌ File does not exist")
            return False

        # Try to read metadata without depending on row-read APIs.
        start_time = time.time()
        with TsFileReader(tsfile_path) as reader:
            tables = _table_schemas(reader)

            if not tables:
                print("No tables found in TsFile")
                return False

        read_time = time.time() - start_time

        print("TsFile metadata validation successful")
        print(f"   Tables: {len(tables)}")
        print(f"   Metadata read time: {read_time:.2f}s")

        return True

    except ImportError:
        print("Error: TsFile Python library not found.")
        print(
            "Build it from the repository root with: ./mvnw -P with-python clean verify"
        )
        return False
    except Exception as e:
        print(f"❌ Validation failed: {e}")
        return False


def main():
    if len(sys.argv) < 2:
        print("TsFile Utility Script")
        print("\nUsage:")
        print(
            "  python example.py csv2tsfile <csv_file> <tsfile_output> [device_col] [timestamp_col]"
        )
        print("  python example.py inspect <tsfile>")
        print("  python example.py validate <tsfile>")
        return

    command = sys.argv[1]

    if command == "csv2tsfile":
        if len(sys.argv) < 4:
            print(
                "Usage: python example.py csv2tsfile <csv_file> <tsfile_output> [device_col] [timestamp_col]"
            )
            return

        csv_path = sys.argv[2]
        tsfile_path = sys.argv[3]
        device_col = sys.argv[4] if len(sys.argv) > 4 else "device"
        timestamp_col = sys.argv[5] if len(sys.argv) > 5 else "timestamp"

        csv_to_tsfile(csv_path, tsfile_path, device_col, timestamp_col)

    elif command == "inspect":
        if len(sys.argv) < 3:
            print("Usage: python example.py inspect <tsfile>")
            return
        inspect_tsfile(sys.argv[2])

    elif command == "validate":
        if len(sys.argv) < 3:
            print("Usage: python example.py validate <tsfile>")
            return
        validate_tsfile(sys.argv[2])

    else:
        print(f"Unknown command: {command}")
        print("Available commands: csv2tsfile, inspect, validate")


if __name__ == "__main__":
    main()
