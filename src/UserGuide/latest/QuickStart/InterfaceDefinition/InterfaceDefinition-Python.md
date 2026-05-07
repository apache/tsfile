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
    TIMESTAMP = 8
    DATE = 9
    BLOB = 10
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
    Query table data and time-series data from TsFile, providing standardized file reading and query interfaces.
    Supports full core capabilities including table model query, tree model query, metadata acquisition, and resource management.
    """

    def __init__(self, pathname: str):
        """
        Initialize the TsFile reader for the specified path, complete file loading and underlying reader initialization,
        and maintain all active query result sets to ensure all result sets are invalidated synchronously when the reader is closed.

        :param pathname: Full path of the TsFile to be read
        :return: No return value
        """

    def query_table(self, table_name: str, column_names: List[str],
                    start_time: int = np.iinfo(np.int64).min,
                    end_time: int = np.iinfo(np.int64).max,
                    tag_filter: Optional[object] = None,
                    batch_size: int = 0) -> object:
        """
        Perform time-range query on the specified table and columns, supporting tag filtering and batch reading mode.
        Adapts to both row-by-row return and fixed-size data block return modes to meet reading requirements in different scenarios.

        :param table_name: Name of the target table to query, case-insensitive
        :param column_names: List of target column names to retrieve; all columns are queried by default if empty
        :param start_time: Start timestamp of the query range, default is the minimum value of int64 type
        :param end_time: End timestamp of the query range, default is the maximum value of int64 type
        :param tag_filter: Optional parameter, filter conditions based on tag columns, supporting equality, range, and logical combination filters
        :param batch_size: Batch reading size; row-by-row mode is enabled when ≤ 0, data blocks are returned by the specified size when > 0
        :return: Encapsulated query result set handler for traversing data, reading data, and obtaining metadata
        """

    def query_table_on_tree(self, column_names: List[str],
                            start_time: int = np.iinfo(np.int64).min,
                            end_time: int = np.iinfo(np.int64).max) -> object:
        """
        Perform table query on the tree model structure, adapted for query scenarios of native tree-structured time-series data.
        Query directly based on measurement names without specifying a table name; path names are case-sensitive.

        :param column_names: List of measurement names to query, corresponding to node paths in the tree structure
        :param start_time: Start timestamp of the query range, default is the minimum value of int64 type
        :param end_time: End timestamp of the query range, default is the maximum value of int64 type
        :return: Result set handler corresponding to the tree model query
        """

    def query_tree_by_row(self, device_ids: List[str], measurement_names: List[str],
                          offset: int = 0, limit: int = -1) -> object:
        """
        Query tree model time-series data by row with pagination, supporting offset skipping and maximum return row limit.
        Adapted for large data volume pagination reading to avoid memory overflow caused by loading excessive data at once.

        :param device_ids: List of device IDs to query, cannot be empty
        :param measurement_names: List of measurement names to query, cannot be empty
        :param offset: Number of starting rows to skip, starting from 0 by default
        :param limit: Maximum number of rows to return; no limit if less than 0
        :return: Result set handler for tree model pagination query
        """

    def query_table_by_row(self, table_name: str, column_names: List[str],
                           offset: int = 0, limit: int = -1,
                           tag_filter: Optional[object] = None,
                           batch_size: int = 0) -> object:
        """
        Query table model data by row with pagination, supporting offset and row limit pushdown, and can be used with tag filtering.
        Invalid data can be skipped at the data block level for dense devices, greatly improving pagination query efficiency.

        :param table_name: Name of the target table to query
        :param column_names: List of column names to query
        :param offset: Number of starting rows to skip, starting from 0 by default
        :param limit: Maximum number of rows to return; no limit if less than 0
        :param tag_filter: Optional parameter, tag filter condition to filter device data that meets the criteria
        :param batch_size: Batch reading size, adapted to the underlying data block reading logic
        :return: Result set handler for table model pagination query
        """

    def query_timeseries(self, device_name: str, sensor_list: List[str],
                         start_time: int = 0, end_time: int = 0) -> object:
        """
        Perform time-range time-series data query for a single specified device.
        Adapted for precise query scenarios of a single device with multiple sensors, simplifying query invocation logic.

        :param device_name: Name/path of the target device
        :param sensor_list: List of sensor (measurement) names to query
        :param start_time: Query start timestamp; starts from the earliest time of the file by default if 0
        :param end_time: Query end timestamp; ends at the latest time of the file by default if 0
        :return: Result set handler for single-device time-series query
        """

    def get_table_schema(self, table_name: str) -> object:
        """
        Get the complete schema information of the specified table, including full metadata such as column names, data types, tag columns, and time-series constraints.
        Used to verify the legality of query fields in advance and parse data structures.

        :param table_name: Name of the target table
        :return: Schema information object of the corresponding table, containing full configuration of the table structure
        """

    def get_all_table_schemas(self) -> Dict[str, object]:
        """
        Get schema information of all tables in the current TsFile.
        Traverse all data table structures in the file with one click without querying table by table.

        :return: Dictionary structure, key is table name, value is schema information object of the corresponding table
        """

    def get_all_timeseries_schemas(self) -> List[object]:
        """
        Get schema information of all time-series in the TsFile.
        Covers field, type, and constraint information of full time-series data in both tree model and table model.

        :return: List of all time-series schema information
        """

    def get_all_devices(self) -> List[str]:
        """
        Get identification information of all devices in the TsFile.
        Can traverse all devices in the file, adapted for full-device statistics and batch query pre-operations.

        :return: List composed of all device IDs/device paths
        """

    def get_timeseries_metadata(self, device_ids: Optional[List[str]] = None) -> Dict[str, object]:
        """
        Get time-series metadata of specified devices, including data storage segments, field constraints, data ranges, etc.
        Returns metadata of all devices by default if no device ID is passed, returns an empty dictionary if an empty list is passed.

        :param device_ids: Optional parameter, list of device IDs to query metadata for
        :return: Dictionary structure, key is device path, value is time-series metadata group of the corresponding device
        """

    def close(self) -> None:
        """
        Close the TsFile reader, release underlying file handles and memory resources.
        Mark all current active query result sets as invalid and prohibit subsequent data reading operations.
        No query or metadata acquisition operations can be performed after closing; the reader needs to be reinitialized.
        """
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