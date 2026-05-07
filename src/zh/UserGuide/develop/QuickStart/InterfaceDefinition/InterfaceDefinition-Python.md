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

class ColumnCategory(IntEnum):
    """
    TsFile 中的列类别枚举。
    TAG：表示标签列，用于存储元数据。
    FIELD：表示测点列，用于存储实际数据值。
    """
    TAG = 0
    FIELD = 1

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


```



## 写入接口

### TsFileWriter 

```python
class TsFileTableWriter:
    """
    用于将结构化表格数据写入具有指定模式的 TsFile。
    """

    """
    :param path: tsfile 文件路径，如果不存在则会创建。
    :param table_schema: 描述要写入表的结构信息。
    :return: 无返回值。
    """
    def __init__(self, path: str, table_schema: TableSchema)

    """
    将一个 Tablet 写入 TsFile 中的表中。
    :param tablet: 存储表的批量数据。
    :return: 无返回值。
    """
    def write_table(self, tablet: Tablet)

    """
    关闭 TsFileTableWriter，并自动刷新数据。
    :return: 无返回值。
    """
    def close(self)


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

## 读取接口

### TsFileReader

```python
class TsFileReader:
    """
    从 TsFile 中查询表格数据、时序数据，提供标准化的文件读取与查询接口，
    支持表模型查询、树模型查询、元数据获取、资源管控等全量核心能力。
    """

    def __init__(self, pathname: str):
        """
        初始化指定路径的 TsFile 读取器，完成文件加载与底层读取器初始化，
        同时维护当前所有活跃的查询结果集，确保读取器关闭时同步失效所有结果集。
        :param pathname: 待读取的 TsFile 文件的完整路径
        :return: 无返回值
        """

    def query_table(self, table_name: str, column_names: List[str],
                    start_time: int = np.iinfo(np.int64).min,
                    end_time: int = np.iinfo(np.int64).max,
                    tag_filter: Optional[object] = None,
                    batch_size: int = 0) -> object:
        """
        对指定的表和列执行时间范围查询，支持标签过滤与批量读取模式。
        可适配逐行返回与固定大小数据块返回两种模式，满足不同场景的读取需求。

        :param table_name: 要查询的目标表名，不区分大小写
        :param column_names: 要检索的目标列名列表，为空时默认查询全列
        :param start_time: 查询范围的起始时间戳，默认值为 int64 类型最小值
        :param end_time: 查询范围的结束时间戳，默认值为 int64 类型最大值
        :param tag_filter: 可选参数，基于标签列的过滤条件，支持等值、范围、逻辑组合过滤
        :param batch_size: 批量读取大小，小于等于0时启用逐行返回模式，大于0时按指定大小返回数据块
        :return: 封装完成的查询结果集处理器，可用于遍历、读取数据、获取元数据
        """

    def query_table_on_tree(self, column_names: List[str],
                            start_time: int = np.iinfo(np.int64).min,
                            end_time: int = np.iinfo(np.int64).max) -> object:
        """
        在树模型结构上执行表查询，适配原生树结构时序数据的查询场景，
        直接基于测量项名称查询，无需指定表名，路径名称区分大小写。

        :param column_names: 待查询的测量项名称列表，对应树结构中的节点路径
        :param start_time: 查询范围的起始时间戳，默认值为 int64 类型最小值
        :param end_time: 查询范围的结束时间戳，默认值为 int64 类型最大值
        :return: 树模型查询对应的结果集处理器
        """

    def query_tree_by_row(self, device_ids: List[str], measurement_names: List[str],
                          offset: int = 0, limit: int = -1) -> object:
        """
        按行分页查询树模型时序数据，支持偏移量跳过、最大返回行数限制，
        适配大数据量分页读取场景，避免单次加载过多数据导致内存溢出。

        :param device_ids: 待查询的设备ID列表，不能为空
        :param measurement_names: 待查询的测量项名称列表，不能为空
        :param offset: 需要跳过的起始行数，默认从0开始
        :param limit: 最大返回行数，小于0表示不限制返回行数
        :return: 树模型分页查询的结果集处理器
        """

    def query_table_by_row(self, table_name: str, column_names: List[str],
                           offset: int = 0, limit: int = -1,
                           tag_filter: Optional[object] = None,
                           batch_size: int = 0) -> object:
        """
        按行分页查询表模型数据，支持偏移量与行数限制下推，可结合标签过滤使用，
        密集型设备可在数据块级别跳过无效数据，大幅提升分页查询效率。

        :param table_name: 待查询的目标表名
        :param column_names: 待查询的列名列表
        :param offset: 需要跳过的起始行数，默认从0开始
        :param limit: 最大返回行数，小于0表示不限制返回行数
        :param tag_filter: 可选参数，标签过滤条件，过滤符合条件的设备数据
        :param batch_size: 批量读取大小，适配底层数据块读取逻辑
        :return: 表模型分页查询的结果集处理器
        """

    def query_timeseries(self, device_name: str, sensor_list: List[str],
                         start_time: int = 0, end_time: int = 0) -> object:
        """
        针对单个指定设备，执行时间范围时序数据查询，
        适配单设备多传感器的精准查询场景，简化查询调用逻辑。

        :param device_name: 目标设备的名称/路径
        :param sensor_list: 待查询的传感器（测量项）名称列表
        :param start_time: 查询起始时间戳，为0时默认从文件最早时间开始
        :param end_time: 查询结束时间戳，为0时默认到文件最晚时间结束
        :return: 单设备时序查询的结果集处理器
        """

    def get_table_schema(self, table_name: str) -> object:
        """
        获取指定表的完整模式信息，包含列名、数据类型、标签列、时序约束等全量元数据，
        用于提前校验查询字段合法性、解析数据结构。

        :param table_name: 目标表名
        :return: 对应表的模式信息对象，包含表结构全量配置
        """

    def get_all_table_schemas(self) -> Dict[str, object]:
        """
        获取当前 TsFile 文件中所有表的模式信息，
        一键遍历文件内全部数据表结构，无需逐个表查询。

        :return: 字典结构，key为表名，value为对应表的模式信息对象
        """

    def get_all_timeseries_schemas(self) -> List[object]:
        """
        获取 TsFile 内所有时序序列的模式信息，
        覆盖树模型、表模型全量时序数据的字段、类型、约束信息。

        :return: 所有时序模式信息组成的列表
        """

    def get_all_devices(self) -> List[str]:
        """
        获取 TsFile 文件内所有设备的标识信息，
        可遍历文件内全部设备，适配全设备统计、批量查询前置操作。

        :return: 所有设备ID/设备路径组成的列表
        """

    def get_timeseries_metadata(self, device_ids: Optional[List[str]] = None) -> Dict[str, object]:
        """
        获取指定设备的时序元数据，包含数据存储分段、字段约束、数据范围等信息，
        不传设备ID时默认返回全设备元数据，传入空列表返回空字典。

        :param device_ids: 可选参数，待查询元数据的设备ID列表
        :return: 字典结构，key为设备路径，value为对应设备的时序元数据组
        """

    def close(self) -> None:
        """
        关闭 TsFile 读取器，释放底层文件句柄、内存资源，
        同时将当前所有活跃的查询结果集标记为失效，禁止后续数据读取操作。
        关闭后不可再次执行查询、元数据获取操作，需重新初始化读取器。
        """
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

       该函数同时支持表模型（table-model）和树模型（tree-model）的 TsFile。
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
