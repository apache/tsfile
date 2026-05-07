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

from tsfile.exceptions import TableNotExistError, ColumnNotExistError
from tsfile.tsfile_reader import TsFileReaderPy


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

        with TsFileReaderPy(file_path) as reader:
            total_rows = 0
            table_schema = reader.get_all_table_schemas()

            is_tree_model = len(table_schema) == 0

            if is_tree_model:
                if _column_names is None:
                    print("columns name is None, return all columns")
            else:
                if _table_name is None:
                    _table_name, columns = next(iter(table_schema.items()))
                else:
                    _table_name = _table_name.lower()
                    if _table_name.lower() not in table_schema:
                        raise TableNotExistError(_table_name)
                    columns = table_schema[_table_name]

                column_names_in_file = columns.get_column_names()

                if _column_names is not None:
                    for column in _column_names:
                        if column.lower() not in column_names_in_file:
                            raise ColumnNotExistError(column)
                else:
                    _column_names = column_names_in_file

            if is_tree_model:
                if _column_names is None:
                    _column_names = []
                query_result = reader.query_table_on_tree(_column_names, _start_time, _end_time)
            else:
                query_result = reader.query_table(_table_name, _column_names, _start_time, _end_time)

            with query_result as result:
                while result.next():
                    if max_row_num is None:
                        df = result.read_data_frame()
                    elif is_iterator:
                        df = result.read_data_frame(max_row_num)
                    else:
                        remaining_rows = max_row_num - total_rows
                        if remaining_rows <= 0:
                            break
                        df = result.read_data_frame(remaining_rows)
                    if df is None or df.empty:
                        continue
                    total_rows += len(df)
                    yield df
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
