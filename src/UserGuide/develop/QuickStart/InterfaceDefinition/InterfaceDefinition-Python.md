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

    def __init__(self, column_name: str, data_type: TSDataType, 
                 category: ColumnCategory = ColumnCategory.FIELD)
    def get_column_name(self)
    def get_data_type(self)
    def get_category(self)

class TableSchema:
    """Schema definition for a table structure."""
    
    table_name = None
    columns = None

    def __init__(self, table_name: str, columns: List[ColumnSchema])
    def get_table_name(self)
    def get_columns(self)


class ResultSetMetaData:
    """Metadata container for query result sets (columns, types, table name)."""
    
    column_list = None
    data_types = None
    table_name = None

    def __init__(self, column_list: List[str], data_types: List[TSDataType])
    def set_table_name(self, table_name: str)
    def get_data_type(self, column_index: int) -> TSDataType
    def get_column_name(self, column_index: int) -> str
    def get_column_name_index(self, column_name: str) -> int
    def get_column_num(self)
    def get_column_list(self)
    def get_data_type_list(self)

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
    
    
    def set_table_name(self, table_name: str)
    def get_column_name_list(self)
    def get_data_type_list(self)
    def get_timestamp_list(self)
    def get_target_name(self)
    def get_value_list(self)
    def get_max_row_num(self)
    def add_column(self, column_name: str, column_type: TSDataType)
    def remove_column(self, column_name: str)
    def set_timestamp_list(self, timestamp_list: list[int])
    def add_timestamp(self, row_index: int, timestamp: int)
    def add_value_by_name(self, column_name: str, row_index: int, value: Union[int, float, bool, str, bytes])
    def add_value_by_index(self, col_index: int, row_index: int, value: Union[int, float, bool, str, bytes])
    def get_value_by_index(self, col_index: int, row_index: int)
    def get_value_by_name(self, column_name: str, row_index: int)
    def get_value_list_by_name(self, column_name: str)
```



## Read  Interface

### TsFileReader

```python
cdef class TsFileReaderPy:
    """
    Cython wrapper class for interacting with TsFileReader C implementation.

    Provides a Pythonic interface to read and query time series data from TsFiles.
    """
    
    
    """
    Initialize a TsFile reader for the specified file path.
    """
    def __init__(self, pathname)


    """
    Execute a time range query on specified table and columns.
    :return: query result handler.
    """
    def query_table(self, table_name : str, column_names : List[str],
                    start_time : int = 0, end_time : int = 0) -> ResultSetPy

    """
    Get table's schema with specify table name.
    """
    def get_table_schema(self, table_name : str)-> TableSchema


    """
    Get all tables schemas
    """
    def get_all_table_schemas(self) ->dict[str, TableSchema]


    """
    Close TsFile Reader, if reader has result sets, invalid them.
    """
    def close(self)

    
    
```

### ResultSet



```python
class ResultSet:
    """
    Get data from a query result. When reader run a query, a query handler will return.
    If reader is closed, result set will not invalid anymore.
    """

    """
    Check and get next rows in query result.
    :return: boolean, true means get next rows.
    """
    def next(self) -> bool


    """
    Get result set's columns info.
    :return: a dict contains column's name and datatype.
    """
    def get_result_column_info(self) -> dict[str, TsDataType]

    
    """
    :param max_row_num: default row num: 1024
    :return: a dataframe contains data from query result.
    """
    def read_next_data_frame(self, max_row_num : int = 1024) -> DataFrame

    
    """
    Get value by index from query result set.
    NOTE: index start from 1.
    """ 
    def get_value_by_index(self, index : int)

      
    """
    Get value by name from query result set.
    """
    def get_value_by_name(self, column_name : str)

      
	
  	"""
  	Get result set metadata in this result set.
  	"""
    def get_metadata(self)->ResultSetMetadata
      
    
    """
    Checks whether the field at the specified index in the result set is null.

    This method queries the underlying result set to determine if the value
    at the given column index position represents a null value.

    Index start from 1.
    """
    def is_null_by_index(self, index : int)

      
      
    """
    Checks whether the field with the specified column name in the result set is null.
    """
    def is_null_by_name(self, name : str)

      
    """
    Close result set.
    """
    def close(self)
```

