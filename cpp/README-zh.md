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

# TsFile C++ 文档

<p align="center">
  <img src="https://www.apache.org/logos/originals/tsfile.svg"
       alt="TsFile Logo"
       width="400"/>
</p>

## 简介

本目录包含 TsFile 的 C++ 实现版本。目前，C++ 版本支持 TsFile 的查询与写入功能，包括基于时间过滤的查询。

源代码位于 `./src` 目录。
C/C++ 示例代码位于 `./examples` 目录。
TsFile_cpp 的性能基准测试位于 `./bench_mark` 目录。

此外，在 `./src/cwrapper` 目录中提供了 C 函数封装接口，Python 工具依赖该封装。

---

## 如何贡献

我们使用 `clang-format` 来确保 C++ 代码遵循 `./clang-format` 文件中定义的一致规范（类似于 Google 风格）。

欢迎提交任何 Bug 报告。
你可以创建一个以 `[CPP]` 开头的 Issue 来描述问题，例如：
https://github.com/apache/tsfile/issues/94

---

## 构建

### 环境要求

```bash
sudo apt-get update
sudo apt-get install -y cmake make g++ clang-format libuuid-dev
```

构建 tsfile：

```bash
bash build.sh
```

如果你安装了 Maven 工具，也可以运行：

```bash
mvn package -P with-cpp clean verify
```

构建完成后，可在 `./build` 目录下找到生成的共享库文件。

在向 GitHub 提交代码之前，请确保 `mvn` 编译通过。

---

### Windows 下 MinGW 编译问题

如果你在 Windows 下使用 MinGW 编译时遇到错误，可以尝试使用以下我们验证通过的版本：

- GCC 14.2.0（**POSIX** 线程） + LLVM/Clang/LLD/LLDB 18.1.8 + MinGW-w64 12.0.0 UCRT - release 1
- GCC 12.2.0 + LLVM/Clang/LLD/LLDB 16.0.0 + MinGW-w64 10.0.0（UCRT）- release 5
- GCC 12.2.0 + LLVM/Clang/LLD/LLDB 16.0.0 + MinGW-w64 10.0.0（MSVCRT）- release 5
- GCC 11.2.0 + MinGW-w64 10.0.0（MSVCRT）- release 1

---

## 配置交叉编译工具链

修改工具链文件 `cmake/ToolChain.cmake`，定义以下变量：

- `CMAKE_C_COMPILER`：指定 C 编译器路径。
- `CMAKE_CXX_COMPILER`：指定 C++ 编译器路径。
- `CMAKE_FIND_ROOT_PATH`：设置交叉编译环境的根路径（例如交叉编译工具链目录）。

在 `cpp/` 目录下执行以下命令创建构建目录并开始编译：

```bash
mkdir build && cd build
cmake .. -DToolChain=ON
make
```

---

## 使用 TsFile

你可以在 `./examples/cpp_examples` 目录下的 `demo_read.cpp` 和 `demo_write.cpp` 中查看读写数据的示例。

在 `./examples/c_examples` 目录下，还提供了使用 C 风格 API 在 C 环境中读写数据的示例。

在 `./examples` 目录下执行：

```bash
bash build.sh
```

即可在 `./examples/build` 目录下生成可执行文件。