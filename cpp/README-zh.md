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

`mvn spotless` 会使用 `clang-format v17.0.6` 来格式化 C++ 代码。在提交代码前，请先确认你环境里的 `clang-format` 版本与其一致，并且已经加入 `PATH`。

`clang-format v17.0.6` 安装方式如下：

- macOS

```bash
brew install llvm@17
ln -sf /opt/homebrew/opt/llvm@17/bin/clang-format /opt/homebrew/bin/clang-format
```

- Windows

```bash
choco install llvm --version 17.0.6 --force
```

安装完成后，可通过以下命令确认版本：

```bash
clang-format --version
```

如需格式化 C++ 代码，可执行：

```bash
mvn spotless:apply -P with-cpp
```

如果你需要临时跳过代码格式化检查，可以添加 `-Dspotless.skip=true`，例如：

```bash
mvn package -P with-cpp clean verify -Dspotless.skip=true
```

欢迎提交任何 Bug 报告。
你可以创建一个以 `[CPP]` 开头的 Issue 来描述问题，例如：
https://github.com/apache/tsfile/issues/94

---

## 构建

### 环境要求

```bash
sudo apt-get update
sudo apt-get install -y cmake make g++ clang-format libuuid-dev dpkg-dev
```

在 RHEL/CentOS/Fedora 系统上，可使用 `yum` 或 `dnf` 安装对应依赖：

```bash
sudo yum install -y cmake make gcc-c++ clang-tools-extra libuuid-devel rpm-build
# 或
sudo dnf install -y cmake make gcc-c++ clang-tools-extra libuuid-devel rpm-build
```

构建 tsfile：

```bash
bash build.sh
```

`build.sh` 默认只编译，不执行安装。如果需要安装到 CMake 的安装前缀目录，显式传入 `install` 参数：

```bash
bash build.sh install
```

如果你安装了 Maven 工具，也可以运行：

```bash
mvn package -P with-cpp clean verify
```

构建完成后，可在 `./build` 目录下找到生成的共享库文件。

在向 GitHub 提交代码之前，请确保 `mvn` 编译通过。

### 构建 Linux 安装包

C++ CMake 构建可以生成 Linux 发行版可安装包：

- Debian/Ubuntu：`libtsfile`、`libtsfile-dev`、`tsfile-cli` DEB 包
- RHEL/CentOS/Fedora：`libtsfile`、`libtsfile-devel`、`tsfile-cli` RPM 包

在仓库根目录下，Maven 会先构建 C++ 模块，然后调用 CPack：

```bash
./mvnw package -P with-cpp
```

如果只需要快速生成安装包、不运行 C++ 测试：

```bash
./mvnw package -P with-cpp -Dbuild.test=OFF -DskipTests
```

也可以在 `cpp/` 目录下直接调用 CMake：

```bash
cmake -S . -B build/package -DCMAKE_BUILD_TYPE=Release
cmake --build build/package --target package
```

如果只需要生成其中一种包格式，可以在构建目录下直接运行 `cpack`：

```bash
cd build/package
cpack -G DEB
cpack -G RPM
```

使用 `apt` 安装生成的 DEB 包：

```bash
sudo apt install ./libtsfile_*.deb ./libtsfile-dev_*.deb ./tsfile-cli_*.deb
```

使用 `yum` 或 `dnf` 安装生成的 RPM 包：

```bash
sudo yum install ./libtsfile-*.rpm ./libtsfile-devel-*.rpm ./tsfile-cli-*.rpm
# 或
sudo dnf install ./libtsfile-*.rpm ./libtsfile-devel-*.rpm ./tsfile-cli-*.rpm
```

安装包会把共享库安装到系统库目录，头文件安装到 `/usr/include/tsfile`，
CMake 包配置安装到 `/usr/lib/cmake/tsfile` 或 `/usr/lib64/cmake/tsfile`，
命令行工具安装为 `/usr/bin/tsfile-cli`。

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

## 并行写入

TsFile C++ 支持基于线程池的列级并行编码，适用于表模型写入路径（`write_table`）。启用后，时间列和所有值列使用预计算的 page 边界并行写入，同时保证各列 page 对齐封盘。

### 编译选项

并行写入通过 `ENABLE_THREADS` CMake 选项控制（默认开启）：

```bash
cmake .. -DENABLE_THREADS=ON   # 开启（默认）
cmake .. -DENABLE_THREADS=OFF  # 关闭——编译期剥离所有线程代码
```

### 运行时配置

```cpp
#include "common/global.h"

// 运行时开启或关闭并行写入（单核机器自动禁用）
storage::set_parallel_write_enabled(true);

// 设置工作线程数（必须在创建 TsFileWriter 之前调用）
storage::set_write_thread_count(4);
```

默认情况下，当机器 CPU 核数大于 1 时自动启用并行写入，线程数设为硬件核数（上限 64）。

---

## 使用 TsFile

你可以在 `./examples/cpp_examples` 目录下的 `demo_read.cpp` 和 `demo_write.cpp` 中查看读写数据的示例。

在 `./examples/c_examples` 目录下，还提供了使用 C 风格 API 在 C 环境中读写数据的示例。

在 `./examples` 目录下执行：

```bash
bash build.sh
```

即可在 `./examples/build` 目录下生成可执行文件。
