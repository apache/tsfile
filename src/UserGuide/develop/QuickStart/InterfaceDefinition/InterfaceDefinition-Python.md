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
# Interface Definitions - Python

## Schema

```Python

class TSDataType(IntEnum):
    """
    Enumeration of data types currently supported by TsFile.
    """
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    FLOAT = 3
    DOUBLE = 4
    TEXT = 5
    STRING = 11

class ColumnCategory(IntEnum):
    """
    Enumeration of column categories in TsFile.
    TAG: Represents a tag column, used for metadata.
    FIELD: Represents a field column, used for storing actual data values.
    """

    TAG = 0
    FIELD = 1

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
    """

    
    """
    :param path: The path of tsfile, will create if it doesn't exist.
    :param table_schema: describes the schema of the tables want to write.
    :return: no return value.
    """
    def __init__(self, path: str, table_schema: TableSchema)


    """
    Write a tablet into table in tsfile.
    :param tablet: stored batch data of a table.
    :return: no return value.
    """
    def write_table(self, tablet: Tablet)
      
    """
    Close TsFileTableWriter and flush data automatically.
    :return: no return value.
    """
    def close(self)

```



### Tablet definition

You can use Tablet to insert data into TsFile in batches.

```Python
class Tablet(object)
    """
    A pre-allocated columnar data container for batch data with type constraints.
    Creates timestamp buffer and typed data columns, with value range validation ranges
    for numeric types.

    Initializes:
    :param column_name_list: name list for data columns.
    :param type_list: TSDataType values specifying allowed types per column.
    :param max_row_num: Pre-allocated row capacity (default 1024)
    :return: no return value.
    """

    def __init__(self, column_name_list: list[str], type_list: list[TSDataType],
                 max_row_num: int = 1024)
    
```

## Read Interface

### TsFileReader

```python
class TsFileReader:
    """
    Query table data from a TsFile.
    """
    
    """
    Initialize a TsFile reader for the specified file path.
    :param pathname: The path to the TsFile.
    :return  no return value.
    """
    def __init__(self, pathname)


    """
    Executes a time range query on the specified table and columns.

    :param table_name: The name of the table to query.
    :param column_names: A list of column names to retrieve.
    :param start_time: The start time of the query range (default: minimum int64 value).
    :param end_time: The end time of the query range (default: maximum int64 value).
    :return: A query result set handler.
    """
    def query_table(self, table_name : str, column_names : List[str],
                    start_time : int = np.iinfo(np.int64).min, 
                    end_time: int = np.iinfo(np.int64).max) -> ResultSet

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
    def read_data_frame(self, max_row_num : int = 1024) -> DataFrame

    
    """
    Retrieves the value at the specified index from the query result set.

    :param index: The index of the value to retrieve, 1 <= index <= column_num.
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
    def get_metadata(self) -> ResultSetMetadata
      
    
    """
    Checks whether the field at the specified index in the result set is null.

    :param index: The index of the field to check.  1 <= index <= column_num.
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


### to_dataframe

```python

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
```
