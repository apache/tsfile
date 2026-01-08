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

## 依赖安装

### 使用 PIP 在线安装

1. 架构平台支持情况

| Platform      | python                           |
| ------------- | -------------------------------- |
| Linux_x86_64  | py39, py310, py311, py312, py313 |
| Linus_aarch64 | py39, py310, py311, py312, py313 |
| MacOS_arm64   | py39, py310, py311, py312, py313 |
| MacOS_X86_64  | py39, py310, py311, py312, py313 |
| Win_amd64     | py39, py310, py311, py312, py313 |

2. 安装依赖版本要求

   * numpy >= 1.26.4
   * pandas >= 2.2.2

3. 安装步骤

通过 pip 指令在线安装

```bash
pip install tsfile
```

### 下载 wheel 文件手动安装

1. 下载 wheel 文件：https://pypi.org/project/tsfile/#files
2. 通过 pip install 命令安装 wheel 文件
```bash
pip install tsfile.wheel
```

### 源码编译安装
1. 安装依赖版本要求

   - CMake >=3.11
   - Maven >=3.9.6
   - GCC >=4.8.5
   - Make >=4.3
   - cython >= 3.0.10
   - numpy >= 1.26.4
   - pandas >= 2.2.2
   - setuptools >= 70.0.0

2. 安装步骤
* 从git克隆源代码:

```shell
git clone https://github.com/apache/tsfile.git
```
* 在 TsFile 根目录下执行 maven 编译:

```shell
mvn clean install -P with-python -DskipTests
```

* 如果没有安装 maven, 你可以执行下面的指令完成编译:

  * 在 Linux 或 Macos上：
    ```shell
    mvnw clean install -P with-python -DskipTests
    ```
  * 在 Windows 上:

    ```shell
    mvnw.cmd clean install -P with-python -DskipTests
    ```

* 编译成功后，wheel 文件将位于  `tsfile/python/dist` 目录下, 可通过 pip install 命令进行本地安装（假设他的名字是 `tsfile.wheel`）

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


使用 `to_dataframe` 读取 TsFile 为 Dataframe.

```Python
import os
import tsfile as ts
table_data_dir = os.path.join(os.path.dirname(__file__), "table_data.tsfile")
print(ts.to_dataframe(table_data_dir))
```


## 示例代码

使用这些接口的示例代码可以在以下链接中找到：https://github.com/apache/tsfile/blob/develop/python/examples/example.py

> 注意：以上读写示例均基于表模型接口，接口定义介绍可见[Python 接口定义](./InterfaceDefinition/InterfaceDefinition-Python.md)。若需了解树模型相关内容，请联系我们。

