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
import pandas as pd

from tsfile import TableSchema, Tablet, TableNotExistError
from tsfile import TsFileWriter, ColumnCategory
from tsfile.constants import TSDataType
from tsfile.exceptions import ColumnNotExistError, TypeMismatchError

def check_string_or_blob(ts_data_type: TSDataType, dtype, column_series: pd.Series) -> TSDataType:
    if ts_data_type == TSDataType.STRING and (dtype == 'object' or str(dtype) == "<class 'numpy.object_'>"):
        first_valid_idx = column_series.first_valid_index()
        if first_valid_idx is not None:
            first_value = column_series[first_valid_idx]
            if isinstance(first_value, bytes):
                return TSDataType.BLOB
    return ts_data_type


class TsFileTableWriter:
    """
    Facilitates writing structured table data into a TsFile with a specified schema.

    The TsFileTableWriter class is designed to write structured data,
    particularly suitable for time-series data, into a file optimized for
    efficient storage and retrieval (referred to as TsFile here). It allows users
    to define the schema of the tables they want to write, add rows of data
    according to that schema, and serialize this data into a TsFile.
    """

    def __init__(self, path: str, table_schema: TableSchema, memory_threshold=128 * 1024 * 1024):
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
        if dataframe is None or dataframe.empty:
            raise ValueError("DataFrame cannot be None or empty")

        # Create mapping from lowercase column name to original column name
        df_column_name_map = {col.lower(): col for col in dataframe.columns if col.lower() != 'time'}
        df_columns = list(df_column_name_map.keys())

        schema_column_names = set(self.tableSchema.get_column_names())
        df_columns_set = set(df_columns)

        extra_columns = df_columns_set - schema_column_names
        if extra_columns:
            raise ColumnNotExistError(
                code=50,
                context=f"DataFrame has columns not in schema: {', '.join(sorted(extra_columns))}"
            )

        schema_column_map = {
            col.get_column_name(): col for col in self.tableSchema.get_columns()
        }
        
        type_mismatches = []
        for col_name in df_columns:
            df_col_name_original = df_column_name_map[col_name]
                
            df_dtype = dataframe[df_col_name_original].dtype
            df_ts_type = TSDataType.from_pandas_datatype(df_dtype)
            df_ts_type = check_string_or_blob(df_ts_type, df_dtype, dataframe[df_col_name_original])

            schema_col = schema_column_map[col_name]
            expected_ts_type = schema_col.get_data_type()

            if df_ts_type != expected_ts_type:
                type_mismatches.append(
                    f"Column '{col_name}': expected {expected_ts_type.name}, got {df_ts_type.name}"
                )
        
        if type_mismatches:
            raise TypeMismatchError(
                code=27,
                context=f"Type mismatches: {'; '.join(type_mismatches)}"
            )

        tag_columns = []
        for col in self.tableSchema.get_columns():
            if col.get_category() == ColumnCategory.TAG:
                tag_col_name = col.get_column_name()
                if tag_col_name in df_column_name_map:
                    tag_columns.append(df_column_name_map[tag_col_name])

        time_column = None
        for col in dataframe.columns:
            if col.lower() == 'time':
                time_column = col
                break

        if time_column:
            sort_by = tag_columns.copy()
            sort_by.append(time_column)
            dataframe = dataframe.sort_values(by=sort_by)

        self.writer.write_dataframe(self.tableSchema.get_table_name(), dataframe)

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
