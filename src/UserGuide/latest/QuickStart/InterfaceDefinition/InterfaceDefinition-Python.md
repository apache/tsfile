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

The public package exports `TsFileReader` / `ResultSet` (Cython `TsFileReaderPy` / `ResultSetPy`), `TsFileTableWriter` for **table** writes, and `TsFileWriter` for lower-level / tree-style registration. **Table** queries: `query_table`, `query_table_by_row`. **Tree** paths: `query_table_on_tree`, `query_tree_by_row`, `query_timeseries` (device + sensors). Use `tsfile.tag_filter` with `query_table` / `query_table_by_row` for TAG columns.

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
    TIMESTAMP = 8
    DATE = 9
    BLOB = 10
    STRING = 11

class ColumnCategory(IntEnum):
    """
    TAG / FIELD / ATTRIBUTE / TIME (see `tsfile.constants`).
    """
    TAG = 0
    FIELD = 1
    ATTRIBUTE = 2
    TIME = 3

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

## Read interface

### `TsFileReader` (`TsFileReaderPy`)

```python
class TsFileReader:
    """
    Open a TsFile reader.

    :param pathname: Path to the TsFile.
    :return: None.
    """
    def __init__(self, pathname: str) -> None

    """
    Query table-model data in a time range.

    :param table_name: Target table name.
    :param column_names: Requested columns.
    :param start_time: Start timestamp (default INT64_MIN).
    :param end_time: End timestamp (default INT64_MAX).
    :param tag_filter: Optional `TagFilter` on TAG columns.
    :param batch_size: <=0 row mode, >0 batch mode.
    :return: ResultSet handle.
    """
    def query_table(
        self,
        table_name: str,
        column_names: List[str],
        start_time: int = INT64_MIN,
        end_time: int = INT64_MAX,
        tag_filter: Optional[TagFilter] = None,
        batch_size: int = 0,
    ) -> ResultSet

    """
    Query table-model data by row window.

    :param table_name: Target table name.
    :param column_names: Requested columns.
    :param offset: Number of leading rows to skip.
    :param limit: Max rows to return; <0 means unlimited.
    :param tag_filter: Optional `TagFilter` on TAG columns.
    :param batch_size: <=0 row mode, >0 batch mode.
    :return: ResultSet handle.
    """
    def query_table_by_row(
        self,
        table_name: str,
        column_names: List[str],
        offset: int = 0,
        limit: int = -1,
        tag_filter: Optional[TagFilter] = None,
        batch_size: int = 0,
    ) -> ResultSet

    """
    Query tree-model full paths in a time range.

    :param column_names: Full path list, e.g. `device.measurement`.
    :param start_time: Start timestamp.
    :param end_time: End timestamp.
    :return: ResultSet handle.
    """
    def query_table_on_tree(
        self,
        column_names: List[str],
        start_time: int = INT64_MIN,
        end_time: int = INT64_MAX,
    ) -> ResultSet

    """
    Query tree-model data by row with offset/limit.

    :param device_ids: Device id list.
    :param measurement_names: Measurement name list.
    :param offset: Number of leading rows to skip.
    :param limit: Max rows to return; <0 means unlimited.
    :return: ResultSet handle.
    """
    def query_tree_by_row(
        self,
        device_ids: List[str],
        measurement_names: List[str],
        offset: int = 0,
        limit: int = -1,
    ) -> ResultSet

    """
    Query one device with selected sensors in time range.

    :param device_name: Device id.
    :param sensor_list: Sensor/measurement names.
    :param start_time: Start timestamp.
    :param end_time: End timestamp.
    :return: ResultSet handle.
    """
    def query_timeseries(
        self,
        device_name: str,
        sensor_list: List[str],
        start_time: int = 0,
        end_time: int = 0,
    ) -> ResultSet

    """
    Get schema of one table.

    :param table_name: Table name.
    :return: TableSchema object.
    """
    def get_table_schema(self, table_name: str) -> TableSchema
    """
    Get schemas of all tables.

    :return: Dict of table name -> TableSchema.
    """
    def get_all_table_schemas(self) -> Dict[str, TableSchema]
    """
    Get all timeseries schemas.

    :return: SDK-defined collection of device/timeseries schemas.
    """
    def get_all_timeseries_schemas(self):
        """All timeseries (device) schemas in the file; return type is SDK-defined."""

    """
    Get all devices in the file.

    :return: List of DeviceID.
    """
    def get_all_devices(self) -> List[DeviceID]
    """
    Get timeseries metadata for all or selected devices.

    :param device_ids: None means all devices; [] means empty result.
    :return: Dict keyed by device path.
    """
    def get_timeseries_metadata(
        self, device_ids: Optional[List] = None
    ) -> Dict[str, DeviceTimeseriesMetadataGroup]

    """
    Close reader and invalidate active result sets.
    """
    def close(self) -> None
    """
    Context manager enter.
    """
    def __enter__(self) -> "TsFileReader"
    """
    Context manager exit.
    """
    def __exit__(self, *args) -> None
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
    def read_data_frame(self, max_row_num: int = 1024) -> DataFrame

    """
    Fetch next Arrow batch in batch mode.

    :return: `pyarrow.Table` for next batch, or None when exhausted.
    """
    def read_arrow_batch(self):
        ...

    """
    Get value by 1-based column index.

    :param index: 1-based index.
    :return: Typed field value from current row.
    """
    def get_value_by_index(self, index: int)

      
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
           List of column/measurement names to query.
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