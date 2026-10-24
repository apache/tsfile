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

## Dataset 索引

通过 `use_index=True` 启用持久化 Dataset 索引。第一次打开时构建索引，之后
复用已有索引。`trust_index=True` 是默认值，假设索引对应的 TsFile 不再变更，
因此加载索引和查询时跳过源文件大小、修改时间和文件代次检查。
索引格式与 locator 边界始终会检查。

```python
from tsfile import TsFileDataFrame

with TsFileDataFrame("dataset/", use_index=True) as dataset:
    values = dataset[0][:]

# 加载索引和获取查询 reader 时检查源文件是否发生变更。
with TsFileDataFrame("dataset/", use_index=True, trust_index=False) as dataset:
    values = dataset[0][:]
```

设置 `trust_index=False` 时，打开 dataset 会重建已过期的索引；获取查询 reader
时检测到文件变更则报错。`trust_index` 仅限关键字传入，在默认的
`use_index=False` 模式下没有作用。

## Dataset 读取资源上限

`TsFileDataFrame` 在 `use_index=True` 时支持以下仅限关键字的配置参数：

```python
from tsfile import TsFileDataFrame

with TsFileDataFrame(
    ["part1.tsfile", "part2.tsfile"],
    use_index=True,
    max_prepared_series=32,
    descriptor_cache_size=32,
    max_open_files=16,
    query_workers=4,
) as frame:
    values = frame[0][:]
```

| 参数 | 环境变量 | 默认值 | 有效取值 |
|------|----------|--------|----------|
| `max_prepared_series` | `TSFILE_DATAFRAME_MAX_PREPARED_SERIES` | 4096 | 非负整数 |
| `descriptor_cache_size` | `TSFILE_DATAFRAME_DESCRIPTOR_CACHE_SIZE` | 4096 | 非负整数 |
| `max_open_files` | `TSFILE_DATAFRAME_MAX_OPEN_FILES` | 16 | 正整数 |
| `query_workers` | `TSFILE_DATAFRAME_QUERY_WORKERS` | `min(4, os.cpu_count() or 1)` | 正整数 |
| `query_parallel_min_rows` | `TSFILE_DATAFRAME_QUERY_PARALLEL_MIN_ROWS` | 8192 | 正整数 |

显式参数优先于对应的环境变量。参数为 `None` 时读取环境变量；变量未设置时
使用内置默认值。配置在构造 frame 时一次性确定，之后修改环境变量不会影响
已有 frame。子集共享父 frame 的 runtime 和配置。非法值在打开数据文件或索引前
报错。显式传入这些参数要求 `use_index=True`；默认的无索引模式不读取这些环境变量。

预备序列缓存保留已解析的原生序列元数据，按 LRU 淘汰空闲条目并释放原生句柄。
查询正在使用的条目，以及其他序列仍依赖的共享时间元数据，会保持有效，因而
并发查询期间条目数可能暂时超过上限，查询结束后再回收。这是每个 runtime 的
条目数量上限，不是整个进程的内存上限；设置为 `0` 表示使用结束后不保留缓存。
`descriptor_cache_size` 分别限制名称描述符缓存和序列路由缓存，`0` 禁用二者。
`max_open_files` 限制打开的 reader 数量，`query_workers=1` 表示串行执行查询组。
多个 runtime 或工作进程分别维护各自的上限。

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

## 本地文件读取后端

Python reader 会在打开文件时继承进程级读取后端配置。默认使用 `PREAD`，
以保持传统的定位读取行为；`MMAP` 要求必须使用内存映射，`AUTO` 则优先
使用映射，并在映射不可用时回退到 `PREAD`。

```python
from tsfile import FileReadBackend, TsFileReader, set_file_read_backend

set_file_read_backend(FileReadBackend.MMAP)
with TsFileReader("example.tsfile") as reader:
    ...

# 配置字典入口具有相同效果。
from tsfile import set_tsfile_config
set_tsfile_config({"file_read_backend_": FileReadBackend.AUTO})
```

该配置只影响之后打开的 reader。通过内存映射后端打开文件期间，请勿修改或
截断该文件。

## 可定位的二进制文件对象

`TsFileReader` 也可以直接接收可定位的二进制文件对象。因此，通过 `fsspec`
等库打开远程文件后，无需先把整个文件复制到本地即可读取。

```python
import fsspec
from tsfile import TsFileReader

with fsspec.open("s3://bucket/example.tsfile", "rb") as source:
    with TsFileReader(source) as reader:
        result = reader.query_table("table_name", ["column_name"])
```

文件对象必须提供 `seek()`、`tell()` 和带明确长度的二进制 `read(size)`。
该对象仍由调用方管理：`TsFileReader` 会在使用期间保持其存活、恢复其游标位置，
但不会关闭它。本地 `FileReadBackend` 配置不适用于文件对象。
