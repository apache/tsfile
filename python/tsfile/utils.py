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
import numpy as np
import pandas as pd
from typing import Iterator, Union
from tsfile.tsfile_reader import TsFileReaderPy

from tsfile.exceptions import TableNotExistError, ColumnNotExistError


def to_dataframe(file_path: str,
                 table_name: str | None = None,
                 column_names: list[str] | None = None,
                 start_time: int | None = None,
                 end_time: int | None = None,
                 max_row_num: int | None = None,
                 as_iterator: bool = False) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:

    def _gen() -> Iterator[pd.DataFrame]:
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
                    if _table_name not in table_schema:
                        raise TableNotExistError(_table_name)
                    columns = table_schema[_table_name]

                column_names_in_file = columns.get_column_names()

                if _column_names is not None:
                    for column in _column_names:
                        if column not in column_names_in_file:
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
                    if max_row_num is not None:
                        remaining_rows = max_row_num - total_rows
                        if remaining_rows <= 0:
                            break
                        else:
                            batch_rows = min(remaining_rows, 1024)
                        df = result.read_data_frame(batch_rows)
                        total_rows += len(df)
                    else:
                        df = result.read_data_frame()
                    yield df

    if as_iterator:
        return _gen()
    else:
        df_list = list(_gen())
        if df_list:
            return pd.concat(df_list, ignore_index=True)
        else:
            return pd.DataFrame()