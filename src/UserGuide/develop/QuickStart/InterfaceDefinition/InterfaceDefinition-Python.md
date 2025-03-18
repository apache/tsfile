<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Interface Definitions



## Schema

```Python

class ColumnSchema:
    """Defines schema for a table column (name, datatype, category)."""

    column_name = None
    data_type = None
    category = None

    def __init__(self, column_name: str, data_type: TSDataType, 
                 category: ColumnCategory = ColumnCategory.FIELD)

class TableSchema:
    """Schema definition for a table structure."""
    
    table_name = None
    columns = None

    def __init__(self, table_name: str, columns: List[ColumnSchema])


class ResultSetMetaData:
    """Metadata container for query result sets (columns, types, table name)."""
    
    column_list = None
    data_types = None
    table_name = None

    def __init__(self, column_list: List[str], data_types: List[TSDataType])

```



## Write interface

### TsFileWriter 

```python
class TsFileTableWriter:
    """
    Facilitates writing structured table data into a TsFile with a specified schema.

    The TsFileTableWriter class is designed to write structured data,
    particularly suitable for time-series data, into a file optimized for
    efficient storage and retrieval (referred to as TsFile here). It allows users
    to define the schema of the tables they want to write, add rows of data
    according to that schema, and serialize this data into a TsFile.
    """

    
    """
    :param path: The path of tsfile, will create if it doesn't exist.
    :param table_schema: describes the schema of the tables they want to write.
    """
    def __init__(self, path: str, table_schema: TableSchema)


    """
    Write a tablet into table in tsfile.
    :param tablet: stored batch data of a table.
    :return: no return value.
    :raise: TableNotExistError if table does not exist or tablet's table_name does not match tableschema.
    """
    def write_table(self, tablet: Tablet)
      
      
    """
    Close TsFileTableWriter and will flush data automatically.
    :return: no return value.
    """
    def close(self)

```



### Tablet definition

You can use Tablet to insert data into TsFile in batches, and you need to release the space occupied by the Tablet after use.

```Python
class Tablet(object)
    """
    A pre-allocated columnar data container for batch data with type constraints.

    Initializes:
    - column_name_list: Ordered names for data columns
    - type_list: TSDataType values specifying allowed types per column
    - max_row_num: Pre-allocated row capacity (default 1024)

    Creates timestamp buffer and typed data columns, with value range validation ranges
    for numeric types.
    """

    def __init__(self, column_name_list: list[str], type_list: list[TSDataType],
                 max_row_num: int = 1024)
    
```



## Read  Interface

### TsFileReader

```python
class TsFileReader:
    """
    Query table data from a TsFile.
    """
    
    
    """
    Initialize a TsFile reader for the specified file path.
    :param pathname: The path to the TsFile.
    """
    def __init__(self, pathname)


    """
    Executes a time range query on the specified table and columns.

    :param table_name: The name of the table to query.
    :param column_names: A list of column names to retrieve.
    :param start_time: The start time of the query range (default: minimum int64 value).
    :param end_time: The end time of the query range (default: maximum int64 value).
    :return: A query result handler.
    """
    def query_table(self, table_name : str, column_names : List[str],
                    start_time : int = np.iinfo(np.int64).min, 
                    end_time: int = np.iinfo(np.int64).max) -> ResultSetPy

    """
    Retrieves the schema of the specified table.

    :param table_name: The name of the table.
    :return: The schema of the specified table.
    """
    def get_table_schema(self, table_name : str)-> TableSchema


    """
    Retrieves the schemas of all tables in the TsFile.

    :return: A dictionary mapping table names to their schemas.
    """
    def get_all_table_schemas(self) ->dict[str, TableSchema]


    """
    Closes the TsFile reader. If the reader has active result sets, they will be invalidated.
    """
    def close(self)

```

### ResultSet



```python
class ResultSet:
    """
    Retrieves data from a query result set. When a query is executed, a query handler is returned.
    If the reader is closed, the result set will become invalid.
    """

    """
    Checks and moves to the next row in the query result set.

    :return: True if the next row exists, False otherwise.
    """
    def next(self) -> bool


    """
    Retrieves the column information of the result set.

    :return: A dictionary containing column names as keys and their data types as values.
    """
    def get_result_column_info(self) -> dict[str, TsDataType]

    
    """
    Fetches the next DataFrame from the query result set.

    :param max_row_num: The maximum number of rows to retrieve. Default is 1024.
    :return: A DataFrame containing data from the query result set.
    """
    def read_next_data_frame(self, max_row_num : int = 1024) -> DataFrame

    
    """
    Retrieves the value at the specified index from the query result set.

    NOTE: Index starts from 1.

    :param index: The index of the value to retrieve.
    :return: The value at the specified index.
    """
    def get_value_by_index(self, index : int)

      
    """
    Retrieves the value for the specified column name from the query result set.

    :param column_name: The name of the column to retrieve the value from.
    :return: The value of the specified column.
    """
    def get_value_by_name(self, column_name : str)

      
	
    """
    Retrieves the metadata of the result set.

    :return: The metadata of the result set as a ResultSetMetadata object.
    """
    def get_metadata(self)->ResultSetMetadata
      
    
    """
    Checks whether the field at the specified index in the result set is null.

    NOTE: Index starts from 1.

    :param index: The index of the field to check.
    :return: True if the field is null, False otherwise.
    """
    def is_null_by_index(self, index : int)

      
      
    """
    Checks whether the field with the specified column name in the result set is null.

    :param name: The name of the column to check.
    :return: True if the field is null, False otherwise.
    """
    def is_null_by_name(self, name : str)

      
    """
    Closes the result set and releases any associated resources.
    """
    def close(self)
```

