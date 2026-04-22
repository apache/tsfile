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