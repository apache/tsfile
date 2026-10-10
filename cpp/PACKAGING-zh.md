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

# TsFile C++ 安装与打包规范

本文档定义 TsFile C++ 动态库、公共头文件、CMake package、pkg-config module、
命令行工具和平台软件包的统一发布接口。

## 发布边界

Apache 社区投票通过的源码发布是正式发布物。Homebrew、Debian/Ubuntu 等平台
软件包和便携归档包均由对应源码发布构建。各产物的名称、版本和来源信息遵循对应
平台的打包规范，并可追溯到该源码发布。

## 安装布局

CMake 安装使用 `GNUInstallDirs`，安装位置由 `CMAKE_INSTALL_PREFIX`、`DESTDIR`
以及各平台的标准目录变量共同确定。

| 内容 | 安装路径 |
| --- | --- |
| 动态库 | `${CMAKE_INSTALL_LIBDIR}/libtsfile.*` |
| 公共头文件 | `${CMAKE_INSTALL_INCLUDEDIR}/tsfile/...` |
| CMake package | `${CMAKE_INSTALL_LIBDIR}/cmake/TsFile/` |
| pkg-config module | `${CMAKE_INSTALL_LIBDIR}/pkgconfig/libtsfile.pc` |
| 命令行工具 | `${CMAKE_INSTALL_BINDIR}/tsfile-cli` |
| 项目声明文件 | `${CMAKE_INSTALL_DATADIR}/doc/tsfile/` |

公共头文件位于：

```text
<prefix>/<includedir>/tsfile/...
```

CMake package 和 pkg-config module 向消费者提供的 include 根目录是：

```text
<prefix>/<includedir>
```

用户代码使用：

```cpp
#include <tsfile/reader/tsfile_reader.h>
#include <tsfile/writer/tsfile_writer.h>
```

开发组件包含用户使用 TsFile C++ API 所需的全部头文件。安装完成后，用户无需
访问源码树或构建目录即可编译使用这些头文件的程序。

## CMake 用户接口

安装内容包括：

- `TsFileConfig.cmake`；
- `TsFileConfigVersion.cmake`；
- `TsFileTargets.cmake`；
- imported target `TsFile::tsfile`。

标准用法为：

```cmake
find_package(TsFile CONFIG REQUIRED)

add_executable(my_application main.cpp)
target_link_libraries(my_application PRIVATE TsFile::tsfile)
```

`TsFile::tsfile` 提供以下 usage requirements：

- `<prefix>/<includedir>` include 根目录；
- TsFile 公共接口要求的 C++ 标准；
- 公共接口要求的平台库；
- 公共头文件可观察到的编译定义；
- 公共接口中出现的依赖 target。

用户通过 `CMAKE_PREFIX_PATH` 或 `TsFile_DIR` 指定 TsFile 的安装位置：

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/tsfile
```

生成的 CMake package 使用相对安装路径，可随安装 prefix 一起移动，不包含源码树
或构建目录的绝对路径。

## pkg-config 用户接口

pkg-config module 名称为 `libtsfile`：

```bash
pkg-config --cflags --libs libtsfile
```

`libtsfile.pc` 的核心字段为：

```pkg-config
Name: Apache TsFile
Version: <source-release-version>
Libs: -L${libdir} -ltsfile
Cflags: -I${includedir}
```

其中 `includedir` 对应 `<prefix>/<includedir>`，CMake 和 pkg-config 因此产生相同
的头文件使用形式：

```cpp
#include <tsfile/...>
```

仅在动态库内部使用的依赖不进入公开的 include 和 link 参数。类型出现在公共头
文件中的依赖通过 `Requires` 表达。

## 公共 API

公共 API 包括：

- 公共头文件清单中的入口头文件；
- 入口头文件公开并记录的类型、函数、常量和枚举；
- 面向外部用户安装的 C wrapper；
- CMake target `TsFile::tsfile`；
- pkg-config module `libtsfile`；
- 命令行工具的稳定参数和退出码。

动态库使用默认隐藏的符号可见性，只导出公共 API 和 C wrapper 所需符号。私有
实现符号和捆绑依赖符号不进入公开符号表。

## 动态库版本与 ABI

TsFile C++ 对外发布动态库，不发布公共静态 `libtsfile`。

源码发布版本和 ABI epoch 分别设置：

```cmake
set(TSFILE_VERSION <source-release-version>)
set(TSFILE_ABI_VERSION 1)

set_target_properties(tsfile PROPERTIES
    VERSION "${TSFILE_VERSION}"
    SOVERSION "${TSFILE_ABI_VERSION}")
```

在 ELF 平台上，源码版本 `2.4.0`、ABI epoch `1` 对应：

```text
libtsfile.so -> libtsfile.so.1
libtsfile.so.1 -> libtsfile.so.2.4.0
libtsfile.so.2.4.0
```

动态库记录的 SONAME 为：

```text
libtsfile.so.1
```

在 macOS 上对应：

```text
libtsfile.dylib -> libtsfile.1.dylib
libtsfile.1.dylib -> libtsfile.2.4.0.dylib
libtsfile.2.4.0.dylib
```

ABI epoch 仅在发生 ABI 不兼容修改时增加。以下修改改变 ABI epoch：

- 删除或修改已导出的函数和符号；
- 修改公共函数的参数或返回类型；
- 修改公共类或结构体的二进制布局；
- 修改虚函数表；
- 修改公共枚举的值或底层类型；
- 修改编译选项或功能宏并导致公共类型布局变化；
- 修改平台工具链或 C++ runtime 基线并破坏既有二进制兼容性。

新增不改变既有二进制布局的 API、修复内部实现以及保持 ABI 的功能扩展沿用原 ABI
epoch。开发版本后缀不进入 SONAME。

## 依赖配置

构建使用显式的依赖来源配置：

```text
TSFILE_DEPENDENCY_PROVIDER=system
TSFILE_DEPENDENCY_PROVIDER=bundled
```

### 原生软件包

Homebrew 和 Debian/Ubuntu 软件包使用 `system` 配置：

- 依赖由平台软件包管理器安装和升级；
- `libtsfile` 动态链接平台提供的依赖；
- 软件包元数据声明准确的运行时和开发依赖；
- 软件包中不安装依赖库的私有副本；
- 公共 CMake 和 pkg-config 元数据不包含依赖源码树或构建树路径。

### All-in-one 便携包

All-in-one 便携包使用 `bundled` 配置：

- 第三方依赖版本固定；
- 对外提供的 TsFile 库仍然是动态库；
- 第三方依赖私有链接到 `libtsfile`，或放入归档包的私有动态库目录；
- 私有动态库使用相对于归档目录的运行时查找路径；
- 第三方符号不进入 TsFile 公共 ABI；
- 归档包包含与实际内容一致的第三方 `LICENSE` 和 `NOTICE`；
- 归档包不捆绑 glibc；
- 每个归档包对应明确的操作系统、CPU 架构、工具链和最低系统版本。

`system` 和 `bundled` 配置分别生成独立产物。

## 软件包组成

软件包使用三个逻辑组件：

| 组件 | 内容 | Debian/Ubuntu 名称 |
| --- | --- | --- |
| `runtime` | 带 ABI 版本的动态库和声明文件 | `libtsfile1` |
| `development` | 公共头文件、无版本链接名、CMake package、pkg-config module | `libtsfile-dev` |
| `tools` | `tsfile-cli` 和声明文件 | `tsfile-tools` |

Homebrew Formula 在一个安装单元中提供动态库、开发文件和命令行工具。

All-in-one 便携包的目录结构为：

```text
tsfile-<version>-<platform>/
├── bin/
│   └── tsfile-cli
├── include/
│   └── tsfile/
├── lib/
│   ├── libtsfile.*
│   ├── cmake/TsFile/
│   └── pkgconfig/libtsfile.pc
├── LICENSE
└── NOTICE
```

当前平台软件包范围包括：

- macOS Homebrew；
- Debian/Ubuntu DEB；
- Linux all-in-one TGZ。

RPM 不在当前软件包范围内。

## 软件包生成

公开的 Homebrew 和 Debian/Ubuntu 软件包使用各平台的原生打包元数据。

CPack 用于：

- 生成 all-in-one `TGZ`；
- 生成本地 DEB 验证产物；
- 验证 runtime、development 和 tools 的文件归属；
- 验证版本、架构、依赖、许可证和组件关系。

CPack 配置由 TsFile 顶层项目统一生成，第三方依赖不修改 TsFile 的 CPack package
名称、版本、许可证或维护者信息。

## 验收标准

每个受支持的平台配置通过以下检查：

1. 使用临时非默认 prefix 和 `DESTDIR` 完成安装。
2. 安装结果只包含动态库、公共头文件、CMake package、pkg-config module、CLI 和
   对应声明文件。
3. 独立 CMake consumer 通过 `find_package(TsFile CONFIG REQUIRED)` 和
   `TsFile::tsfile` 完成编译、链接和运行。
4. 独立 pkg-config consumer 通过 `libtsfile.pc` 完成编译、链接和运行。
5. Consumer 代码使用 `#include <tsfile/...>` 完成小型 TsFile 读写。
6. 每个公共入口头文件均可在独立 translation unit 中编译。
7. CMake package 和 pkg-config module 不包含源码目录或构建目录路径。
8. 动态库的文件名、SONAME、软链接和导出符号与 ABI epoch 一致。
9. 同一 ABI epoch 内的新版本通过二进制兼容检查。
10. 原生软件包使用平台依赖，不包含依赖库的私有副本。
11. 软件包完成安装、升级和卸载后没有非预期残留文件。
12. All-in-one 包移动目录后仍可运行，并只从归档包内部解析私有动态库。
13. 每个产物的 `LICENSE`、`NOTICE`、版本、架构和来源信息与实际内容一致。
