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
from typing import Iterator, Union
from typing import Optional

import numpy as np
import pandas as pd
from pandas.core.dtypes.common import is_integer_dtype, is_object_dtype

from tsfile import ColumnSchema, TableSchema, ColumnCategory, TSDataType, TIME_COLUMN
from tsfile.exceptions import TableNotExistError, ColumnNotExistError
from tsfile.tsfile_reader import TsFileReaderPy
from tsfile.tsfile_table_writer import TsFileTableWriter, infer_object_column_type, validate_dataframe_for_tsfile


def to_dataframe(file_path: str,
                 table_name: Optional[str] = None,
                 column_names: Optional[list[str]] = None,
                 start_time: Optional[int] = None,
                 end_time: Optional[int] = None,
                 max_row_num: Optional[int] = None,
                 as_iterator: bool = False) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
    """
       Read data from a TsFile and convert it into a Pandas DataFrame or
       an iterator of DataFrames.

       This function supports both table-model and tree-model TsFiles.
       Users can filter data by table name, column names, time range,
       and maximum number of rows.

       Parameters
       ----------
       file_path : str
           Path to the TsFile to be read.

       table_name : Optional[str], default None
           Name of the table to query in table-model TsFiles.
           If None and the file is in table model, the first table
           found in the schema will be used.

       column_names : Optional[list[str]], default None
           List of column names to query.
           - If None, all columns will be returned.
           - Column existence will be validated in table-model TsFiles.

       start_time : Optional[int], default None
           Start timestamp for the query.
           If None, the minimum int64 value is used.

       end_time : Optional[int], default None
           End timestamp for the query.
           If None, the maximum int64 value is used.

       max_row_num : Optional[int], default None
           Maximum number of rows to read.
           - If None, all available rows will be returned.
           - When `as_iterator` is False, the final DataFrame will be
             truncated to this size if necessary.

       as_iterator : bool, default False
           Whether to return an iterator of DataFrames instead of
           a single concatenated DataFrame.
           - True: returns an iterator yielding DataFrames in batches
           - False: returns a single Pandas DataFrame

       Returns
       -------
       Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
           - A Pandas DataFrame if `as_iterator` is False
           - An iterator of Pandas DataFrames if `as_iterator` is True

       Raises
       ------
       TableNotExistError
           If the specified table name does not exist in a table-model TsFile.

       ColumnNotExistError
           If any specified column does not exist in the table schema.
       """

    def _gen(is_iterator: bool) -> Iterator[pd.DataFrame]:
        _table_name = table_name
        _column_names = column_names
        _start_time = start_time if start_time is not None else np.iinfo(np.int64).min
        _end_time = end_time if end_time is not None else np.iinfo(np.int64).max

        ## Time column handling (table model):
        ## 1. Request has no column list (query all):
        ##    1.1 TsFile has a time column in schema: query only non-time columns; then rename
        ##        the first column of the returned DataFrame to the schema time column name.
        ##    1.2 TsFile has no time column in schema: query as-is; first column is "time".
        ## 2. Request has a column list but no time column:
        ##    2.1 TsFile has a time column in schema: query with requested columns; rename the
        ##        first column to the schema time column name.
        ##    2.2 TsFile has no time column in schema: first column stays "time"; no rename.
        ## 3. Request has a column list including the time column:
        ##    3.1 Query with requested columns (including time); do not rename the first column.
        with TsFileReaderPy(file_path) as reader:
            total_rows = 0
            table_schema = reader.get_all_table_schemas()

            is_tree_model = len(table_schema) == 0
            time_column = None
            column_name_to_query = []
            no_field_query = True
            if is_tree_model:
                if _column_names is None:
                    print("columns name is None, return all columns")
                # When querying tables in the tree, only measurements are allowed currently.
                no_field_query = False
            else:
                _table_name = _table_name.lower() if _table_name else None
                _column_names = [column.lower() for column in _column_names] if _column_names else None
                if _table_name is None:
                    _table_name, table_schema = next(iter(table_schema.items()))
                else:
                    _table_name = _table_name.lower()
                    if _table_name.lower() not in table_schema:
                        raise TableNotExistError(_table_name)
                    table_schema = table_schema[_table_name]

                column_names_in_file = []
                for column in table_schema.get_columns():
                    if column.get_category() == ColumnCategory.TIME:
                        time_column = column.get_column_name()
                    else:
                        column_names_in_file.append(column.get_column_name())

                if _column_names is not None:
                    for column in _column_names:
                        if column not in column_names_in_file and column != time_column:
                            raise ColumnNotExistError(column)
                        if table_schema.get_column(column).get_category() == ColumnCategory.FIELD:
                            no_field_query = False
                    if no_field_query:
                        if time_column is not None:
                            column_name_to_query.append(time_column)
                        column_name_to_query.extend(column_names_in_file)
                    else:
                        column_name_to_query = _column_names
                else:
                    no_field_query = False
                    column_name_to_query = column_names_in_file

            if is_tree_model:
                if _column_names is not None:
                    column_name_to_query = _column_names
                query_result = reader.query_table_on_tree(column_name_to_query, _start_time, _end_time)
            else:
                query_result = reader.query_table(_table_name, column_name_to_query, _start_time, _end_time)

            with query_result as result:
                while result.next():
                    if max_row_num is None:
                        dataframe = result.read_data_frame()
                    elif is_iterator:
                        dataframe = result.read_data_frame(max_row_num)
                    else:
                        remaining_rows = max_row_num - total_rows
                        if remaining_rows <= 0:
                            break
                        dataframe = result.read_data_frame(remaining_rows)
                    if dataframe is None or dataframe.empty:
                        continue
                    total_rows += len(dataframe)
                    if time_column is not None:
                        if _column_names is None or time_column not in _column_names:
                            dataframe = dataframe.rename(columns={dataframe.columns[0]: time_column})
                    if no_field_query and _column_names is not None:
                        _column_names.insert(0, TIME_COLUMN)
                        dataframe = dataframe[_column_names]
                    yield dataframe
                    if (not is_iterator) and max_row_num is not None and total_rows >= max_row_num:
                        break

    if as_iterator:
        return _gen(True)
    else:
        df_list = list(_gen(False))
        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            if max_row_num is not None and len(df) > max_row_num:
                df = df.iloc[:max_row_num]
            return df
        else:
            return pd.DataFrame()


def dataframe_to_tsfile(dataframe: pd.DataFrame,
                        file_path: str,
                        table_name: Optional[str] = None,
                        time_column: Optional[str] = None,
                        tag_column: Optional[list[str]] = None,
                        ):
    """
    Write a pandas DataFrame to a TsFile by inferring the table schema from the DataFrame.

    This function automatically infers the table schema based on the DataFrame's column
    names and data types, then writes the data to a TsFile.

    Parameters
    ----------
    dataframe : pd.DataFrame
        The pandas DataFrame to write to TsFile.
        - If a 'time' column (case-insensitive) exists, it will be used as the time column.
        - Otherwise, the DataFrame index will be used as timestamps.
        - All other columns will be treated as data columns.

    file_path : str
        Path to the TsFile to write. Will be created if it doesn't exist.

    table_name : Optional[str], default None
        Name of the table. If None, defaults to "default_table".

    time_column : Optional[str], default None
        Name of the time column. If None, will look for a column named 'time' (case-insensitive),
        or use the DataFrame index if no 'time' column is found.

    tag_column : Optional[list[str]], default None
        List of column names to be treated as TAG columns. All other columns will be FIELD columns.
        If None, all columns are treated as FIELD columns.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the DataFrame is empty or has no data columns.
    """
    validate_dataframe_for_tsfile(dataframe)
    df = dataframe.rename(columns=str.lower)

    if not table_name:
        table_name = "default_table"

    if time_column is not None:
        if time_column.lower() not in df.columns:
            raise ValueError(f"Time column '{time_column}' not found in DataFrame")
    if tag_column is not None:
        for tag_col in tag_column:
            if tag_col.lower() not in df.columns:
                raise ValueError(f"Tag column '{tag_col}' not found in DataFrame")
    tag_columns_lower = {t.lower() for t in (tag_column or [])}

    if time_column is not None:
        time_col_name = time_column.lower()
    elif 'time' in df.columns:
        time_col_name = 'time'
    else:
        time_col_name = None

    if time_col_name is not None:
        if not is_integer_dtype(df[time_col_name].dtype):
            raise TypeError(
                f"Time column '{time_col_name}' must be integer type (int64 or int), got {df[time_col_name].dtype}")

    column_schemas = []
    if time_col_name is not None:
        column_schemas.append(ColumnSchema(time_col_name, TSDataType.TIMESTAMP, ColumnCategory.TIME))

    for col in df.columns:
        if col == time_col_name:
            continue
        col_dtype = df[col].dtype
        if is_object_dtype(col_dtype):
            ts_data_type = infer_object_column_type(df[col])
        else:
            ts_data_type = TSDataType.from_pandas_datatype(col_dtype)

        category = ColumnCategory.TAG if col in tag_columns_lower else ColumnCategory.FIELD
        column_schemas.append(ColumnSchema(col, ts_data_type, category))

    data_columns = [s for s in column_schemas if s.get_category() != ColumnCategory.TIME]
    if len(data_columns) == 0:
        raise ValueError("DataFrame must have at least one data column besides the time column")

    table_schema = TableSchema(table_name, column_schemas)

    with TsFileTableWriter(file_path, table_schema) as writer:
        writer.write_dataframe(df)
