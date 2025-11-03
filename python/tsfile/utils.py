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

from tsfile.exceptions import TableNotExistError, ColumnNotExistError
from tsfile.tsfile_reader import TsFileReaderPy


def to_dataframe(file_path: str,
                 table_name: str = None,
                 column_names: list[str] = None,
                 max_row_num: int = None) -> pd.DataFrame:
    with TsFileReaderPy(file_path) as reader:
        total_rows = 0
        table_schema = reader.get_all_table_schemas()
        if len(table_schema) == 0:
            raise TableNotExistError("Not found any table in the TsFile.")
        if table_name is None:
            # get the first table name by default
            table_name, columns = next(iter(table_schema.items()))
        else:
            if table_name not in table_schema:
                raise TableNotExistError(table_name)
            columns = table_schema[table_name]

        column_names_in_file = columns.get_column_names()

        if column_names is not None:
            for column in column_names:
                if column not in column_names_in_file:
                    raise ColumnNotExistError(column)
        else:
            column_names = column_names_in_file

        df_list: list[pd.DataFrame] = []

        with reader.query_table(table_name, column_names) as result:
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
                df_list.append(df)
        df = pd.concat(df_list, ignore_index=True)
        return df
