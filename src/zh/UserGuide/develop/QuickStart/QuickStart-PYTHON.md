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
# 快速上手 - Python

## 依赖

如果需要在在本地编译，依赖如下所示：

- CMake >=3.11
- Maven >=3.9.6
- GCC >=4.8.5
- Make >=4.3
- cython >= 3.0.10
- numpy >= 1.26.4
- pandas >= 2.2.2
- setuptools >= 70.0.0

如果使用 Pip 在线安装 TsFile，依赖包如下：

numpy >= 1.26.4
pandas >= 2.2.2

## 安装

### 使用 PIP 进行在线安装

使用pip 指令来在线安装 TsFile 包

```bash
pip install tsfile
```

支持的架构平台如下：

| Platform      | python                           |
| ------------- | -------------------------------- |
| Linux_x86_64  | py39, py310, py311, py312, py313 |
| Linus_aarch64 | py39, py310, py311, py312, py313 |
| MacOS_arm64   | py39, py310, py311, py312, py313 |
| MacOS_X86_64  | py39, py310, py311, py312, py313 |
| Win_amd64     | py39, py310, py311, py312, py313 |

或者直接下载 wheel 文件安装：https://pypi.org/project/tsfile/#files


### 在本地进行编译

从git克隆源代码:

```shell
git clone https://github.com/apache/tsfile.git
```
在 TsFile 根目录下执行 maven 编译:

```shell
mvn clean install -P with-python -DskipTests
```

如果没有安装 maven, 你可以执行下面的指令完成编译:

在 Linux 或 Macos上：
```shell
mvnw clean install -P with-python -DskipTests
```
在 Windows 上:

```shell
mvnw.cmd clean install -P with-python -DskipTests
```
#### 目录结构

• **wheel**: wheel文件位于  `tsfile/python/dist`, 你可以使用 pip install 命令来进行本地安装。

### 安装到本地

你可以执行 `pip install`命令来安装编译得到的 tsfile包（假设他的名字是 tsfile.wheel）

```bash
pip install tsfile.wheel
```


## 写入示例

```Python
import os
from tsfile import *

table_data_dir = os.path.join(os.path.dirname(__file__), "table_data.tsfile")
if os.path.exists(table_data_dir):
    os.remove(table_data_dir)

column1 = ColumnSchema("id", TSDataType.STRING, ColumnCategory.TAG)
column2 = ColumnSchema("id2", TSDataType.STRING, ColumnCategory.TAG)
column3 = ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD)
table_schema = TableSchema("test_table", columns=[column1, column2, column3])


### Free resource automatically
with TsFileTableWriter(table_data_dir, table_schema) as writer:
    tablet_row_num = 100
    tablet = Tablet(
                    ["id", "id2", "value"],
                    [TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE],
                    tablet_row_num)

    for i in range(tablet_row_num):
        tablet.add_timestamp(i, i * 10)
        tablet.add_value_by_name("id", i, "test1")
        tablet.add_value_by_name("id2", i, "test" + str(i))
        tablet.add_value_by_index(2, i, i * 100.2)

    writer.write_table(tablet)
```

## 读取示例

```Python
import os
from tsfile import *

table_data_dir = os.path.join(os.path.dirname(__file__), "table_data.tsfile")
### Free resource automatically
with TsFileReader(table_data_dir) as reader:
    print(reader.get_all_table_schemas())
    with reader.query_table("test_table", ["id2", "value"], 0, 50) as result:
        print(result.get_metadata())
        while result.next():
            print(result.get_value_by_name("id2"))
            print(result.get_value_by_name("value"))
            print(result.read_data_frame())
```

## 示例代码

使用这些接口的示例代码可以在以下链接中找到：https://github.com/apache/tsfile/blob/develop/python/examples/example.py

> 注意：以上读写示例均基于表模型接口，接口定义介绍可见[Python 接口定义](./InterfaceDefinition/InterfaceDefinition-Python.md)。若需了解树模型相关内容，请联系我们。
