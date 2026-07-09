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

class TSEncoding(IntEnum):
    """
    Value encoding accepted by the writer. The comment after each
    member lists the data types it can be used with.
    """
    PLAIN = 0         # all types
    DICTIONARY = 1    # STRING, TEXT
    RLE = 2           # INT32, INT64, TIMESTAMP, DATE
    DIFF = 3
    TS_2DIFF = 4      # INT32, INT64, TIMESTAMP, DATE, FLOAT, DOUBLE
    BITMAP = 5
    GORILLA_V1 = 6
    REGULAR = 7
    GORILLA = 8       # INT32, INT64, TIMESTAMP, DATE, FLOAT, DOUBLE
    ZIGZAG = 9        # INT32, INT64
    CHIMP = 11        # INT32, INT64, TIMESTAMP, DATE, FLOAT, DOUBLE
    SPRINTZ = 12      # INT32, INT64, FLOAT, DOUBLE
    RLBE = 13         # INT32, INT64, TIMESTAMP, DATE, FLOAT, DOUBLE
    CAMEL = 14        # DOUBLE

class Compressor(IntEnum):
    """
    Compression accepted by the writer. The default is LZ4.
    """
    UNCOMPRESSED = 0
    SNAPPY = 1
    GZIP = 2
    LZO = 3
    SDT = 4
    PAA = 5
    PLA = 6
    LZ4 = 7
    ZSTD = 8
    LZMA2 = 9

class ColumnCategory(IntEnum):
    """
    Enumeration of column categories in TsFile.
    TAG: a tag column (part of the device identifier / joint primary key).
    FIELD: a field column, holding the measured values.
    ATTRIBUTE / TIME: reserved column roles.
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

@dataclass(frozen=True)
class DeviceID:
    path: Optional[str]
    table_name: Optional[str]
    segments: tuple[Optional[str], ...]

@dataclass(frozen=True)
class TimeseriesStatistic:
    has_statistic: bool
    row_count: int
    start_time: int
    end_time: int

@dataclass(frozen=True)
class TimeseriesMetadata:
    measurement_name: str
    data_type: TSDataType
    chunk_meta_count: int
    statistic: TimeseriesStatistic
    timeline_statistic: TimeseriesStatistic

@dataclass(frozen=True)
class DeviceTimeseriesMetadataGroup:
    table_name: Optional[str]
    segments: tuple[Optional[str], ...]
    timeseries: list[TimeseriesMetadata]
```



## Write interface

### TsFileTableWriter

```python
class TsFileTableWriter:
    """
    Facilitates writing structured table data into a TsFile with a specified schema.
    """

    
    """
    :param path: The path of tsfile, will create if it doesn't exist.
    :param table_schema: describes the schema of the tables want to write.
    :param memory_threshold: bytes buffered before an automatic flush (default 128MB).
    :return: no return value.
    """
    def __init__(self, path: str, table_schema: TableSchema,
                 memory_threshold: int = 128 * 1024 * 1024)


    """
    Write a tablet into table in tsfile.
    :param tablet: stored batch data of a table.
    :return: no return value.
    """
    def write_table(self, tablet: Tablet)

    """
    Write a pandas DataFrame into the table. Column encoding/compression follow
    the table schema (or the engine defaults).
    :param dataframe: the data to write.
    :return: no return value.
    """
    def write_dataframe(self, dataframe: pandas.DataFrame)

    """
    Flush buffered data to disk.
    :return: no return value.
    """
    def flush(self)

    """
    Close TsFileTableWriter and flush data automatically.
    :return: no return value.
    """
    def close(self)

    # Usable as a context manager:
    #   with TsFileTableWriter(path, schema) as w:
    #       w.write_table(tablet)
    def __enter__(self)
    def __exit__(self, exc_type, exc_val, exc_tb)
```

### TsFileWriter

`TsFileWriter` is the lower-level writer exposed from `writer.pyx`. It can write
tree-model data (`register_timeseries`, `register_device`, `write_tablet`,
`write_row_record`) and table-model data (`register_table`, `write_table`,
`write_dataframe`, `write_arrow_batch`).

```python
class TsFileWriter:
    """
    :param pathname: Destination TsFile path.
    :param memory_threshold: bytes buffered before an automatic flush (default 128MB).
    """
    def __init__(self, pathname: str, memory_threshold: int = 128 * 1024 * 1024)

    def register_timeseries(self, device_name: str,
                            timeseries_schema: TimeseriesSchema)

    def register_device(self, device_schema: DeviceSchema)

    def register_table(self, table_schema: TableSchema)

    def write_tablet(self, tablet: Tablet)

    def write_dataframe(self, target_table: str, dataframe: pandas.DataFrame,
                        tableschema: TableSchema)

    def write_row_record(self, record: RowRecord)

    def write_table(self, tablet: Tablet)

    def write_arrow_batch(self, table_name: str, data,
                          time_col_index: int = -1)

    def flush(self)

    def close(self)

    def __enter__(self)
    def __exit__(self, exc_type, exc_val, exc_tb)
```



### Tablet definition

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

### dataframe_to_tsfile

```python
def dataframe_to_tsfile(dataframe: pd.DataFrame,
                        file_path: str,
                        table_name: Optional[str] = None,
                        time_column: Optional[str] = None,
                        tag_column: Optional[list[str]] = None)
    """
    Write a pandas DataFrame to a TsFile.

    :param dataframe:   the data to write.
    :param file_path:   destination .tsfile path.
    :param table_name:  output table name.
    :param time_column: name of the column to use as the timestamp column.
    :param tag_column:  names of the columns to treat as TAG columns.
    """
```

## Configuration

Global write defaults — the default per-type encodings, the default compression,
and the time-column encoding/compression — are exposed as a single dictionary.
Change them **before** creating a writer.

```python
from tsfile import get_tsfile_config, set_tsfile_config
from tsfile import TSEncoding, Compressor

cfg = get_tsfile_config()          # -> dict of all config values
# e.g. cfg["default_compression_type_"], cfg["int64_encoding_type_"],
#      cfg["time_encoding_type_"], cfg["time_compress_type_"], ...

set_tsfile_config({
    "default_compression_type_": Compressor.LZ4,
    "int64_encoding_type_": TSEncoding.TS_2DIFF,
})
```

`set_tsfile_config` validates each value and only updates the keys you pass.
Encoding/compression values are `TSEncoding` / `Compressor` members. The allowed
encodings per data type, and the default used when you do not change it:

| Data type | Allowed encodings | Default |
|---|---|---|
| `BOOLEAN` | `PLAIN` | `PLAIN` |
| `INT32`, `INT64`, `TIMESTAMP`, `DATE` | `PLAIN`, `TS_2DIFF`, `GORILLA`, `ZIGZAG`, `RLE`, `SPRINTZ`, `CHIMP`, `RLBE` | `TS_2DIFF` |
| `FLOAT` | `PLAIN`, `TS_2DIFF`, `GORILLA`, `SPRINTZ`, `CHIMP`, `RLBE` | `GORILLA` |
| `DOUBLE` | `PLAIN`, `TS_2DIFF`, `GORILLA`, `SPRINTZ`, `CHIMP`, `RLBE`, `CAMEL` | `GORILLA` |
| `STRING`, `TEXT` | `PLAIN`, `DICTIONARY` | `PLAIN` |

Compression applies to any data type: `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZO`,
`LZ4`, `ZSTD`, or `LZMA2` (default `LZ4`). The enum also exposes legacy values
such as `SDT`, `PAA`, and `PLA`, but the global configuration setter rejects
them.

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
    :param tag_filter: Optional tag predicate for table-model TAG columns.
    :param batch_size: <= 0 returns rows one by one; > 0 returns blocks of that size.
    :return: A query result set handler.
    """
    def query_table(self, table_name : str, column_names : List[str],
                    start_time : int = np.iinfo(np.int64).min, 
                    end_time: int = np.iinfo(np.int64).max,
                    tag_filter = None, batch_size : int = 0) -> ResultSet

    """
    Execute a time range query on tree-model measurement columns.

    :param column_names: Measurement names to retrieve.
    :param start_time: The start time of the query range.
    :param end_time: The end time of the query range.
    :return: A query result set handler.
    """
    def query_table_on_tree(self, column_names : List[str],
                            start_time : int = np.iinfo(np.int64).min,
                            end_time : int = np.iinfo(np.int64).max) -> ResultSet

    """
    Execute tree-model query by row with offset/limit.

    :param device_ids: Device identifiers to query.
    :param measurement_names: Measurement names to retrieve.
    :param offset: Number of leading rows to skip.
    :param limit: Maximum number of rows to return; < 0 means unlimited.
    :return: A query result set handler.
    """
    def query_tree_by_row(self, device_ids : List[str],
                          measurement_names : List[str],
                          offset : int = 0, limit : int = -1) -> ResultSet

    """
    Execute a table query by row, with offset/limit pushdown and an optional
    tag filter. A TAG predicate restricts the query to the devices whose
    TAG-column values match. Build a filter with the helpers in tsfile.tag_filter
    (tag_eq, tag_neq, tag_lt, tag_lteq, tag_gt, tag_gteq, tag_between, ...) and
    combine filters with &, | and ~.

    :param table_name: The name of the table to query.
    :param column_names: A list of column names to retrieve.
    :param offset: Number of leading rows to skip (default 0).
    :param limit: Maximum number of rows to return; < 0 means unlimited.
    :param tag_filter: Optional tag predicate (TagFilter), or None for no filtering.
    :param batch_size: <= 0 returns rows one by one; > 0 returns blocks of that size.
    :return: A query result set handler.
    """
    def query_table_by_row(self, table_name : str, column_names : List[str],
                           offset : int = 0, limit : int = -1,
                           tag_filter = None, batch_size : int = 0) -> ResultSet

    """
    Execute a tree-model time range query for one device.

    :param device_name: Device identifier.
    :param sensor_list: Measurement names to retrieve.
    :param start_time: Query start time.
    :param end_time: Query end time.
    :return: A query result set handler.
    """
    def query_timeseries(self, device_name : str, sensor_list : List[str],
                         start_time : int = 0, end_time : int = 0) -> ResultSet

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
    Retrieves all tree-model timeseries schemas grouped by device.

    :return: A list of DeviceSchema objects.
    """
    def get_all_timeseries_schemas(self) -> list[DeviceSchema]

    """
    Retrieves all device identifiers in the file.

    :return: A list of DeviceID(path, table_name, segments).
    """
    def get_all_devices(self) -> List[DeviceID]

    """
    Retrieves per-timeseries metadata for all devices, or only the specified
    devices.

    :param device_ids: None for all devices, [] for an empty result, or a list
        of DeviceID / path-compatible device identifiers.
    :return: dict mapping device segment tuples to DeviceTimeseriesMetadataGroup.
    """
    def get_timeseries_metadata(
        self, device_ids: Optional[List] = None
    ) -> Dict[tuple, DeviceTimeseriesMetadataGroup]

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
    Fetches the next batch result as a pyarrow.Table. Returns None when no more
    TsBlock batches are available. This is only valid for result sets created
    with batch_size > 0.
    """
    def read_arrow_batch(self) -> Optional[pyarrow.Table]

    
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

    def __enter__(self)
    def __exit__(self, exc_type, exc_val, exc_tb)
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
