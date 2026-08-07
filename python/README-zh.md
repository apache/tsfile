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

# TsFile Python 文档

<p align="center">
  <img src="https://www.apache.org/logos/originals/tsfile.svg"
       alt="TsFile Logo"
       width="400"/>
</p>

## 简介

本目录包含 TsFile 的 Python 实现版本。Python 版本基于 C++ 版本构建，并通过 Cython 包将 TsFile 的读写能力集成到 Python 环境中。用户可以像在 Pandas 中使用 read_csv 和 write_csv 一样，方便地读取和写入 TsFile。

源代码位于 `./tsfile` 目录。
以 `.pyx` 和 `.pyd` 结尾的文件为使用 Cython 编写的封装代码。
`tsfile/tsfile.py` 中定义了一些对用户开放的接口。

你可以在 `./examples/examples.py` 中找到读写示例。

## TsFileDataFrame 列角色描述

`TsFileDataFrame` 可以加载表和列的 JSON 描述文件。列值使用 `C` 表示协变量，
使用 `T` 表示目标变量：

```json
{
  "weather": {
    "temperature": "C",
    "humidity": "T"
  }
}
```

创建 dataframe 时传入描述文件路径。`get` 和 `set` 提供对 JSON 顶层键的字典式
访问，其中 `set` 会将更新后的描述写回文件；另外还可以按表获取协变量列、目标
变量列及其数量：

```python
df = TsFileDataFrame("weather.tsfile", description_path="description.json")
df.get("weather")
df.get_covariate_columns("weather")
df.get_target_columns("weather")
df.get_covariate_column_count("weather")
df.get_target_column_count("weather")
df.get_device_count("weather")
df.get_device_node_counts("weather")
df.get_device_point_counts("weather")
df.get_device_statistics("weather")
df.get_device_point_count("weather", "device_a")
df.get_device_stats("weather", {"device": "device_a"})
df.list_device_metadata("weather")
df.set("weather", {"temperature": "T", "humidity": "T"})
df.close()
```

这里的节点数量指设备下实际存在的数值时间序列数量。`get_device_node_counts`
使用设备有序标签值元组作为 key。`get_device_statistics` 还会返回点数、非空值数量
和时间范围，`list_device_metadata` 则将标签展开为带名称的列。其中 `point_count`
遵循 DataFrame 的时间线数量（包含 NaN 行），`value_count` 只统计非空值。

---

## 如何贡献

建议使用 pylint 对 Python 代码进行检查。

目前尚无合适的 Cython 代码风格检查工具，因此 Cython 部分代码应遵循 pylint 所要求的 Python 代码风格。

**功能列表**

- [ ] 在 pywrapper 中调用 TsFile C++ 版本实现的批量读取接口。
- [ ] 支持将多个 DataFrame 写入同一个 TsFile 文件。

---

## 构建

在构建 TsFile 的 Python 版本之前，必须先构建 [TsFile C++ 版本](../cpp/README.md)，因为 Python 版本依赖于 C++ 版本生成的共享库文件。

### 使用 Maven 在根目录构建

```sh
mvn -P with-cpp,with-python clean verify
```

### 使用 Python 命令构建

```sh
python setup.py build_ext --inplace
```
## 文件级 Properties

`TsFileWriter` 和 `TsFileTableWriter` 可以在打开期间写入二进制 property。
setter 仅接受 `bytes`。reader 返回 `dict[str, bytes | None]`，并区分 null 与
零长度 bytes。

```python
with TsFileWriter("example.tsfile") as writer:
    writer.add_tsfile_property("binary-property", b"\x01\x00\xff")

with TsFileReader("example.tsfile") as reader:
    properties = reader.get_tsfile_properties()
```

Property value 不携带数据类型；保存数字或结构体时应使用明确、可跨语言的字节编码。
