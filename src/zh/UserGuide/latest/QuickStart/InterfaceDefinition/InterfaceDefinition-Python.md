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
# 接口定义 - Python

## 数据模式

```Python
class TSDataType(IntEnum):
    """
    TsFile 当前支持的数据类型枚举。
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
    写入器支持的值编码。每个成员后的注释列出其可用于哪些数据类型。
    """
    PLAIN = 0         # 所有类型
    DICTIONARY = 1    # STRING、TEXT
    RLE = 2           # INT32、INT64、TIMESTAMP、DATE
    DIFF = 3
    TS_2DIFF = 4      # INT32、INT64、TIMESTAMP、DATE、FLOAT、DOUBLE
    BITMAP = 5
    GORILLA_V1 = 6
    REGULAR = 7
    GORILLA = 8       # INT32、INT64、TIMESTAMP、DATE、FLOAT、DOUBLE
    ZIGZAG = 9        # INT32、INT64
    CHIMP = 11        # INT32、INT64、TIMESTAMP、DATE、FLOAT、DOUBLE
    SPRINTZ = 12      # INT32、INT64、FLOAT、DOUBLE
    RLBE = 13         # INT32、INT64、TIMESTAMP、DATE、FLOAT、DOUBLE

class Compressor(IntEnum):
    """
    写入器支持的压缩，默认值为 LZ4。
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
    TsFile 中的列类别枚举。
    TAG：标签列
    FIELD：测点列，存储测量值。
    ATTRIBUTE / TIME：保留的列角色。
    """
    TAG = 0
    FIELD = 1
    ATTRIBUTE = 2
    TIME = 3

class ColumnSchema:
    """定义表中某一列的模式（名称、数据类型、类别）。"""

    column_name = None
    data_type = None
    category = None

    def __init__(self, column_name: str, data_type: TSDataType,
                 category: ColumnCategory = ColumnCategory.FIELD)

class TableSchema:
    """表结构的模式定义。"""

    table_name = None
    columns = None

    def __init__(self, table_name: str, columns: List[ColumnSchema])

class ResultSetMetaData:
    """查询结果集的元数据容器（列名、类型、表名）。"""

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



## 写入接口

### TsFileTableWriter

```python
class TsFileTableWriter:
    """
    用于将结构化表格数据写入具有指定模式的 TsFile。
    """

    """
    :param path: tsfile 文件路径，如果不存在则会创建。
    :param table_schema: 描述要写入表的结构信息。
    :param memory_threshold: 触发自动刷盘前缓冲的字节数（默认 128MB）。
    :return: 无返回值。
    """
    def __init__(self, path: str, table_schema: TableSchema,
                 memory_threshold: int = 128 * 1024 * 1024)

    """
    将一个 Tablet 写入 TsFile 中的表中。
    :param tablet: 存储表的批量数据。
    :return: 无返回值。
    """
    def write_table(self, tablet: Tablet)

    """
    将一个 pandas DataFrame 写入表中。列的编码/压缩遵循表 schema（或引擎默认值）。
    :param dataframe: 要写入的数据。
    :return: 无返回值。
    """
    def write_dataframe(self, dataframe: pandas.DataFrame)

    """
    将缓冲数据刷新到磁盘。
    :return: 无返回值。
    """
    def flush(self)

    """
    关闭 TsFileTableWriter，并自动刷新数据。
    :return: 无返回值。
    """
    def close(self)

    # 可作为上下文管理器使用：
    #   with TsFileTableWriter(path, schema) as w:
    #       w.write_table(tablet)
    def __enter__(self)
    def __exit__(self, exc_type, exc_val, exc_tb)

```

### TsFileWriter

`TsFileWriter` 是 `writer.pyx` 暴露的低层写入接口。它可以写入 tree 模型数据
（`register_timeseries`、`register_device`、`write_tablet`、`write_row_record`），
也可以写入 table 模型数据（`register_table`、`write_table`、`write_dataframe`、
`write_arrow_batch`）。

```python
class TsFileWriter:
    """
    :param pathname: 目标 TsFile 路径。
    :param memory_threshold: 触发自动刷盘前缓冲的字节数（默认 128MB）。
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
    一个为批量数据预分配的列式数据容器，具有类型约束。
    它会创建时间戳缓冲区和带类型的数据列，并对数值类型执行有效值范围校验。

    初始化参数:
    :param column_name_list: 数据列的名称列表。
    :param type_list: 每列允许的数据类型（TSDataType 枚举值）。
    :param max_row_num: 预分配的最大行数（默认值为 1024）。
    :return: 无返回值。
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
    将 pandas DataFrame 写入 TsFile。

    :param dataframe:   要写入的数据。
    :param file_path:   目标 .tsfile 路径。
    :param table_name:  输出表名。
    :param time_column: 用作时间戳列的列名。
    :param tag_column:  作为 TAG 列处理的列名列表。
    """
```

## 配置

全局写入默认值——包括各类型的默认编码、默认压缩、时间列编码/压缩——以一个字典暴露。
请在创建写入器 **之前** 修改它们。

```python
from tsfile import get_tsfile_config, set_tsfile_config
from tsfile import TSEncoding, Compressor

cfg = get_tsfile_config()          # -> 包含所有配置项的 dict
# 例如 cfg["default_compression_type_"]、cfg["int64_encoding_type_"]、
#      cfg["time_encoding_type_"]、cfg["time_compress_type_"] 等。

set_tsfile_config({
    "default_compression_type_": Compressor.LZ4,
    "int64_encoding_type_": TSEncoding.TS_2DIFF,
})
```

`set_tsfile_config` 会校验每个取值，且只更新你传入的键。编码/压缩取值为 `TSEncoding` / `Compressor`
成员。各数据类型允许的编码，以及不修改时使用的默认值：

| 数据类型 | 允许的编码 | 默认值 |
|---|---|---|
| `BOOLEAN` | `PLAIN` | `PLAIN` |
| `INT32`、`INT64`、`DATE` | `PLAIN`、`TS_2DIFF`、`GORILLA`、`ZIGZAG`、`RLE`、`SPRINTZ` | `TS_2DIFF` |
| `FLOAT`、`DOUBLE` | `PLAIN`、`TS_2DIFF`、`GORILLA`、`SPRINTZ` | `GORILLA` |
| `STRING`、`TEXT` | `PLAIN`、`DICTIONARY` | `PLAIN` |

时间列使用全局时间配置，支持 `PLAIN`、`TS_2DIFF`、`GORILLA`、`ZIGZAG`、`RLE` 或 `SPRINTZ`。

压缩适用于所有数据类型：`UNCOMPRESSED`、`SNAPPY`、`GZIP`、`LZO`、`LZ4`（默认 `LZ4`）。Python 枚举还暴露 `CHIMP`、`RLBE`、`ZSTD`、`LZMA2` 等值，但 `origin/develop` 当前的 writer/config 转换会拒绝它们。

## 读取接口

### TsFileReader

```python
class TsFileReader:
    """
    从 TsFile 中查询表格数据。
    """

    """
    初始化指定路径的 TsFile 读取器。
    :param pathname: TsFile 文件的路径。
    :return: 无返回值。
    """
    def __init__(self, pathname)

    """
    对指定的表和列执行时间范围查询。

    :param table_name: 要查询的表名。
    :param column_names: 要检索的列名列表。
    :param start_time: 查询范围的起始时间（默认：int64 最小值）。
    :param end_time: 查询范围的结束时间（默认：int64 最大值）。
    :param tag_filter: 可选的 table 模型 TAG 列谓词。
    :param batch_size: <= 0 逐行返回；> 0 按该大小返回数据块。
    :return: 查询结果集处理器。
    """
    def query_table(self, table_name : str, column_names : List[str],
                    start_time : int = np.iinfo(np.int64).min,
                    end_time: int = np.iinfo(np.int64).max,
                    tag_filter = None, batch_size : int = 0) -> ResultSet

    """
    对 tree 模型测点列执行时间范围查询。

    :param column_names: 要检索的测点名列表。
    :param start_time: 查询范围的起始时间。
    :param end_time: 查询范围的结束时间。
    :return: 查询结果集处理器。
    """
    def query_table_on_tree(self, column_names : List[str],
                            start_time : int = np.iinfo(np.int64).min,
                            end_time : int = np.iinfo(np.int64).max) -> ResultSet

    """
    按行查询 tree 模型数据，支持 offset/limit。

    :param device_ids: 要查询的设备标识列表。
    :param measurement_names: 要检索的测点名列表。
    :param offset: 需要跳过的起始行数。
    :param limit: 最多返回行数；< 0 表示不限制。
    :return: 查询结果集处理器。
    """
    def query_tree_by_row(self, device_ids : List[str],
                          measurement_names : List[str],
                          offset : int = 0, limit : int = -1) -> ResultSet

    """
    按行查询表，支持偏移量/行数限制下推与可选的标签过滤。标签谓词把查询限定到
    标签列取值满足条件的设备。用 tsfile.tag_filter 中的辅助函数构造过滤器
    （tag_eq、tag_neq、tag_lt、tag_lteq、tag_gt、tag_gteq、tag_between 等），
    并用 &、| 和 ~ 组合。

    :param table_name: 要查询的表名。
    :param column_names: 要检索的列名列表。
    :param offset: 需要跳过的起始行数（默认 0）。
    :param limit: 最多返回的行数；< 0 表示不限制。
    :param tag_filter: 可选的标签谓词（TagFilter），None 表示不过滤。
    :param batch_size: <= 0 逐行返回；> 0 按该大小返回数据块。
    :return: 查询结果集处理器。
    """
    def query_table_by_row(self, table_name : str, column_names : List[str],
                           offset : int = 0, limit : int = -1,
                           tag_filter = None, batch_size : int = 0) -> ResultSet

    """
    对单个设备执行 tree 模型时间范围查询。

    :param device_name: 设备标识。
    :param sensor_list: 要检索的测点名列表。
    :param start_time: 查询起始时间。
    :param end_time: 查询结束时间。
    :return: 查询结果集处理器。
    """
    def query_timeseries(self, device_name : str, sensor_list : List[str],
                         start_time : int = 0, end_time : int = 0) -> ResultSet

    """
    获取指定表的模式信息。

    :param table_name: 表名。
    :return: 指定表的模式信息。
    """
    def get_table_schema(self, table_name : str) -> TableSchema

    """
    获取 TsFile 中所有表的模式信息。

    :return: 一个将表名映射到其模式的字典。
    """
    def get_all_table_schemas(self) -> dict[str, TableSchema]

    """
    获取所有 tree 模型 timeseries schema，按设备分组。

    :return: DeviceSchema 对象列表。
    """
    def get_all_timeseries_schemas(self) -> list[DeviceSchema]

    """
    获取文件中的所有设备标识。

    :return: DeviceID(path, table_name, segments) 列表。
    """
    def get_all_devices(self) -> List[DeviceID]

    """
    获取所有设备或指定设备的 per-timeseries metadata。

    :param device_ids: None 表示全部设备，[] 表示空结果，或传入 DeviceID /
        路径兼容的设备标识列表。
    :return: dict，key 为设备 segment tuple，value 为 DeviceTimeseriesMetadataGroup。
    """
    def get_timeseries_metadata(
        self, device_ids: Optional[List] = None
    ) -> Dict[tuple, DeviceTimeseriesMetadataGroup]

    """
    关闭 TsFile 读取器。如果读取器中有活动的结果集，它们将失效。
    """
    def close(self)


```

### ResultSet


```python
class ResultSet:
    """
    用于从查询结果集中获取数据。当执行查询时，将返回一个结果集处理器。
    如果读取器被关闭，该结果集将失效。
    """

    """
    检查并移动到查询结果集中的下一行。

    :return: 如果存在下一行则返回 True，否则返回 False。
    """
    def next(self) -> bool

    """
    获取结果集的列信息。

    :return: 一个字典，键为列名，值为对应的数据类型。
    """
    def get_result_column_info(self) -> dict[str, TsDataType]

    """
    从查询结果集中读取下一个 DataFrame。

    :param max_row_num: 要读取的最大行数，默认值为 1024。
    :return: 包含查询结果数据的 DataFrame。
    """
    def read_data_frame(self, max_row_num : int = 1024) -> DataFrame

    """
    将下一个批结果读取为 pyarrow.Table。没有更多 TsBlock 批时返回 None。
    仅适用于通过 batch_size > 0 创建的结果集。
    """
    def read_arrow_batch(self) -> Optional[pyarrow.Table]

    """
    从查询结果集中按索引获取值。

    :param index: 要获取值的索引，1 <= index <= column_num。
    :return: 指定索引处的值。
    """
    def get_value_by_index(self, index : int)

    """
    从查询结果集中按列名获取值。

    :param column_name: 要获取值的列名。
    :return: 指定列的值。
    """
    def get_value_by_name(self, column_name : str)

    """
    获取结果集的元数据信息。

    :return: 结果集的元数据，类型为 ResultSetMetadata 对象。
    """
    def get_metadata(self) -> ResultSetMetadata

    """
    检查结果集中指定索引位置的字段是否为 null。

    :param index: 要检查的字段索引，1 <= index <= column_num。
    :return: 若字段为 null 返回 True，否则返回 False。
    """
    def is_null_by_index(self, index : int)

    """
    检查结果集中指定列名的字段是否为 null。

    :param name: 要检查的列名。
    :return: 若字段为 null 返回 True，否则返回 False。
    """
    def is_null_by_name(self, name : str)

    """
    关闭结果集并释放相关资源。
    """
    def close(self)

    def __enter__(self)
    def __exit__(self, exc_type, exc_val, exc_tb)

```


### to_dataframe

```Python

def to_dataframe(file_path: str,
                 table_name: Optional[str] = None,
                 column_names: Optional[list[str]] = None,
                 start_time: Optional[int] = None,
                 end_time: Optional[int] = None,
                 max_row_num: Optional[int] = None,
                 as_iterator: bool = False) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        """
       从 TsFile 中读取数据，并将其转换为 Pandas DataFrame
       或 DataFrame 迭代器。

       用户可以通过表名、列名、时间范围以及最大行数对数据进行过滤。

       Parameters
       ----------
       file_path : str
           要读取的 TsFile 文件路径。

       table_name : Optional[str], default None
           表模型 TsFile 中要查询的表名。
           如果为 None 且文件为表模型，
           将使用 schema 中找到的第一个表。

       column_names : Optional[list[str]], default None
           要查询的列名/测点名列表。
           - 如果为 None，则返回所有列。
           - 在表模型 TsFile 中会校验列是否存在。

       start_time : Optional[int], default None
           查询的起始时间戳。
           如果为 None，则使用 int64 的最小值。

       end_time : Optional[int], default None
           查询的结束时间戳。
           如果为 None，则使用 int64 的最大值。

       max_row_num : Optional[int], default None
           读取的最大行数。
           - 如果为 None，则返回所有可用数据。
           - 当 `as_iterator` 为 False 时，
             若结果行数超过该值，DataFrame 将被截断。

       as_iterator : bool, default False
           是否返回 DataFrame 迭代器，而不是单个合并后的 DataFrame。
           - True：返回按批次生成 DataFrame 的迭代器
           - False：返回单个 Pandas DataFrame

       Returns
       -------
       Union[pandas.DataFrame, Iterator[pandas.DataFrame]]
           - 当 `as_iterator` 为 False 时，返回 Pandas DataFrame
           - 当 `as_iterator` 为 True 时，返回 Pandas DataFrame 迭代器

       Raises
       ------
       TableNotExistError
           当指定的表名在表模型 TsFile 中不存在时抛出。

       ColumnNotExistError
           当指定的列在表结构中不存在时抛出。
    """

```
