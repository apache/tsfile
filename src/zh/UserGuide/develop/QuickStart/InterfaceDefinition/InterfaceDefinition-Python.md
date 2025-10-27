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
    :return: 查询结果集处理器。
    """
    def query_table(self, table_name : str, column_names : List[str],
                    start_time : int = np.iinfo(np.int64).min,
                    end_time: int = np.iinfo(np.int64).max) -> ResultSet

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

