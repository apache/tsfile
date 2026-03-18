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
from datetime import date, datetime

import pandas as pd

from tsfile import TableSchema, Tablet, TableNotExistError, ColumnCategory
from tsfile import TsFileWriter, ColumnSchema
from tsfile.constants import TSDataType
from tsfile.exceptions import TypeMismatchError, ColumnNotExistError


def validate_dataframe_for_tsfile(df: pd.DataFrame) -> None:
    if df is None or df.empty:
        raise ValueError("DataFrame cannot be None or empty")

    columns = list(df.columns)

    seen = set()
    duplicates = []
    for c in columns:
        if c is None or (isinstance(c, str) and len(c) == 0):
            raise ValueError("Column name cannot be None or empty")
        lower = c.lower()
        if lower in seen:
            duplicates.append(c)
        seen.add(lower)
    if duplicates:
        raise ValueError(
            f"Column names must be unique (case-insensitive). Duplicate columns: {duplicates}"
        )

    unsupported = []
    for col in columns:
        dtype = df[col].dtype
        try:
            TSDataType.from_pandas_datatype(dtype)
        except (ValueError, TypeError) as e:
            unsupported.append((col, str(dtype), str(e)))

    if unsupported:
        msg_parts = [f"  - {col}: dtype={dtype}" for col, dtype in unsupported]
        raise ValueError(
            "Data types not supported by tsfile:\n" + "\n".join(msg_parts)
        )


def infer_object_column_type(column_series: pd.Series) -> TSDataType:
    first_valid_idx = column_series.first_valid_index()
    if first_valid_idx is None:
        return TSDataType.STRING
    value = column_series[first_valid_idx]
    if isinstance(value, (bytes, bytearray)):
        return TSDataType.BLOB
    if isinstance(value, (date, datetime)):
        return TSDataType.DATE
    if isinstance(value, bool):
        return TSDataType.BOOLEAN
    if isinstance(value, str):
        return TSDataType.STRING
    raise TypeError(
        f"Cannot infer type from object column: expected str/bytes/date/bool, got {type(value).__name__}: {value!r}"
    )


class TsFileTableWriter:
    """
    Facilitates writing structured table data into a TsFile with a specified schema.

    The TsFileTableWriter class is designed to write structured data,
    particularly suitable for time-series data, into a file optimized for
    efficient storage and retrieval (referred to as TsFile here). It allows users
    to define the schema of the tables they want to write, add rows of data
    according to that schema, and serialize this data into a TsFile.
    """

    def __init__(self, path: str, table_schema: TableSchema, memory_threshold = 128 * 1024 * 1024):
        """
        :param path: The path of tsfile, will create if it doesn't exist.
        :param table_schema: describes the schema of the tables they want to write.
        :param memory_threshold(Byte): memory usage threshold for flushing data.
        """
        self.writer = TsFileWriter(path, memory_threshold)
        self.writer.register_table(table_schema)
        self.tableSchema = table_schema

    def write_table(self, tablet: Tablet):
        """
        Write a tablet into table in tsfile.
        :param tablet: stored batch data of a table.
        :return: no return value.
        :raise: TableNotExistError if table does not exist or tablet's table_name does not match tableschema.
        """
        if tablet.get_target_name() is None:
            tablet.set_table_name(self.tableSchema.get_table_name())
        elif (self.tableSchema.get_table_name() is not None
              and tablet.get_target_name() != self.tableSchema.get_table_name()):
            raise TableNotExistError
        self.writer.write_table(tablet)

    def write_dataframe(self, dataframe: pd.DataFrame):
        """
        Write a pandas DataFrame into table in tsfile.
        :param dataframe: pandas DataFrame with 'time' column and data columns matching schema.
        :return: no return value.
        :raise: ValueError if dataframe is None or is empty.
        :raise: ColumnNotExistError if DataFrame columns don't match schema.
        :raise: TypeMismatchError if DataFrame column types are incompatible with schema.
        """

        validate_dataframe_for_tsfile(dataframe)

        # rename columns to lowercase
        dataframe = dataframe.rename(columns=str.lower)
        time_column = self.tableSchema.get_time_column()
        # tag columns used for sorting
        tag_columns = self.tableSchema.get_tag_columns()
        if time_column is None:
            if 'time' in dataframe.columns:
                dtype = TSDataType.from_pandas_datatype(dataframe['time'].dtype)
                if not TSDataType.TIMESTAMP.is_compatible_with(dtype):
                    raise TypeMismatchError(
                        code=27,
                        context=f"time column require INT/Timestamp"
                    )

                self.tableSchema.add_column(ColumnSchema("time",
                                                         TSDataType.TIMESTAMP,
                                                         ColumnCategory.TIME))
                time_column = self.tableSchema.get_time_column()

        type_mismatches = []
        for col_name in dataframe.columns:
            if time_column is not None and col_name == time_column.get_column_name():
                continue
            schema_col = self.tableSchema.get_column(col_name)
            if schema_col is None:
                raise ColumnNotExistError(context=f"{col_name} is not define in table schema")
            # Object dtype can represent STRING, DATE, TEXT, BLOB; validation will be performed during insert, skip here
            if schema_col.get_data_type() in [TSDataType.INT64, TSDataType.INT32, TSDataType.DOUBLE, TSDataType.FLOAT,
                                              TSDataType.BOOLEAN, TSDataType.TIMESTAMP]:
                df_dtype = dataframe[col_name].dtype
                df_ts_type = TSDataType.from_pandas_datatype(df_dtype)
                expected_ts_type = schema_col.get_data_type()

                if not expected_ts_type.is_compatible_with(df_ts_type):
                    type_mismatches.append(
                        f"Column '{col_name}': expected {expected_ts_type.name}, got {df_ts_type.name}"
                    )

        if type_mismatches:
            raise TypeMismatchError(
                code=27,
                context=f"Type mismatches: {'; '.join(type_mismatches)}"
            )

        if time_column:
            time_column_name = time_column.get_column_name()
            time_series = dataframe[time_column_name]
            if time_series.isna().any():
                raise ValueError(
                    f"Time column '{time_column}' must not contain null/NaN values"
                )
            sort_by = [column.get_column_name() for column in tag_columns]
            sort_by.append(time_column_name)
            dataframe = dataframe.sort_values(by=sort_by)

        self.writer.write_dataframe(self.tableSchema.get_table_name(), dataframe, self.tableSchema)

    def write_arrow_batch(self, data):
        """
        Write a PyArrow RecordBatch or Table into tsfile using Arrow C Data
        Interface for efficient batch writing without Python-level row loops.
        :param data: pyarrow.RecordBatch or pyarrow.Table.  Must include a
            timestamp column.  All other columns must match the registered schema.
        :return: no return value.
        """
        time_col = self.tableSchema.get_time_column()
        if time_col is not None:
            time_col_name = time_col.get_column_name()
        else:
            time_col_name = "time"

        time_col_index = data.schema.get_field_index(time_col_name)
        if time_col_index < 0:
            raise ValueError(f"Time column '{time_col_name}' not found in Arrow schema.")
        self.writer.write_arrow_batch(self.tableSchema.get_table_name(), data, time_col_index)

    def close(self):
        """
        Close TsFileTableWriter and will flush data automatically.
        :return: no return value.
        """
        self.writer.close()

    def flush(self):
        """
        Flush current data to tsfile.
        :return: no return value.
        """
        self.writer.flush()

    def __dealloc__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
