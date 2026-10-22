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
# 快速上手 - C++

## 安装

C/C++ 2.5.0 提供预编译运行库、开发包和 CLI，**不必先用 Maven 编译 TsFile**。
开发应用需要 C/C++ 编译器。安装包提供的 CMake 配置支持 CMake 3.11+；选择 Windows 运行时需要 CMake 3.15+。只运行 CLI 不需要编译工具链。
完整下载链接和校验步骤见[下载与安装页面](../../../Download/README.md#cpp-packages)。

### 选择安装方式

| 平台 | 开发 C/C++ 应用 | 说明 |
| --- | --- | --- |
| Ubuntu 22.04 / 24.04 | `tsfile` + `tsfile-dev` | [DEB 下载、校验和安装](../../../Download/README.md#cpp-ubuntu) |
| AlmaLinux 9 | `tsfile` + `tsfile-devel` | [RPM 下载、校验和安装](../../../Download/README.md#cpp-rpm) |
| macOS | Homebrew Formula，或 SDK 压缩包 | [Homebrew 安装](../../../Download/README.md#cpp-homebrew)，包含库、头文件和 CLI |
| Linux / macOS | 完整 SDK 压缩包 | 适合不修改系统目录的安装，示例见下文 |
| Windows | 对应架构的 SDK/CLI ZIP | [Windows 安装](../../../Download/README.md#cpp-windows)，保留 DLL 与 `bin/` 布局 |

`tsfile-dev` / `tsfile-devel` 是开发文件包，不是开发版发行；CLI 位于 `tsfile-tools`。
下载并校验 DEB/RPM 后，可直接安装本地运行库与开发包，例如：

```bash
# Ubuntu amd64；arm64 使用对应文件名
sudo apt install ./tsfile_2.5.0-1_amd64.deb ./tsfile-dev_2.5.0-1_amd64.deb
```

```bash
# AlmaLinux 9 x86_64；aarch64 使用对应文件名
sudo dnf install ./tsfile-2.5.0-1.el9.x86_64.rpm ./tsfile-devel-2.5.0-1.el9.x86_64.rpm
```

### 解压安装 SDK（Linux / macOS）

在空目录执行下例。Apple Silicon Mac 使用 `macos12-arm64`；Intel Mac 使用 `macos12-x86_64`；
Linux 根据架构使用 `manylinux_2_28-x86_64` 或 `manylinux_2_28-aarch64`。

```bash
VERSION=2.5.0
PLATFORM=macos12-arm64
ARCHIVE="tsfile-sdk-${PLATFORM}-${VERSION}.tar.gz"
URL="https://packages.apache.org/artifactory/tsfile/sdk/release/versions/${VERSION}/${PLATFORM}/${ARCHIVE}"
curl --fail --location "$URL" --output "$ARCHIVE"
SHA256="$(curl --fail --silent --show-error "$URL.sha256")"
if command -v sha256sum >/dev/null 2>&1; then
  printf '%s  %s\n' "$SHA256" "$ARCHIVE" | sha256sum -c - || exit 1
else
  printf '%s  %s\n' "$SHA256" "$ARCHIVE" | shasum -a 256 -c - || exit 1
fi
mkdir tsfile-sdk
tar -xzf "$ARCHIVE" -C tsfile-sdk --strip-components=1
export TSFILE_HOME="$PWD/tsfile-sdk"
export PATH="$TSFILE_HOME/bin:$PATH"
tsfile-cli --version
```

若使用 Homebrew，先按下载页导入 Formula，再设置 `TSFILE_HOME="$(brew --prefix local/tsfile250/tsfile)"`。
SDK 包含 `include/`、`lib/`、`lib/cmake/TsFile/`、`lib/pkgconfig/`、`bin/` 和 `share/`；保留其相对布局。

### CMake 集成

在项目的 `CMakeLists.txt` 中使用安装包导出的 target，而不是只用 `find_library` 查找单个库文件：

```cmake
cmake_minimum_required(VERSION 3.11)
project(tsfile_example LANGUAGES CXX)
find_package(TsFile 2.5.0 CONFIG REQUIRED)
add_executable(tsfile_example main.cpp)
target_link_libraries(tsfile_example PRIVATE TsFile::tsfile)
```

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH="$TSFILE_HOME"
cmake --build build --config Release
```

在系统标准目录安装 DEB/RPM 后，通常无需设置 `CMAKE_PREFIX_PATH`。导出的 target 会传递所需头文件目录和编译定义。
Windows 使用匹配架构的 MSVC 和 CMake 3.15+，选择包要求的 Release `/MT` 运行时：

```powershell
cmake -S . -B build -DCMAKE_PREFIX_PATH="$env:TSFILE_HOME" `
  -DCMAKE_POLICY_DEFAULT_CMP0091=NEW -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded
cmake --build build --config Release
```

### pkg-config（Linux / macOS）

SDK 或 Homebrew 安装可通过前缀定位元数据；系统包通常可以直接查询：

```bash
export PKG_CONFIG_PATH="$TSFILE_HOME/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
pkg-config --modversion libtsfile
pkg-config --cflags --libs libtsfile
```

### 可选：从源码构建

只有需要修改 TsFile 或定制构建选项时才需要源码构建。此时需 JDK 17+、CMake、C++11 编译器和 Make/Ninja；
Maven 可使用仓库自带的 wrapper。源码构建版本取决于检出的分支或 commit，不等同于上面的 2.5.0 二进制包。

```bash
git clone https://github.com/apache/tsfile.git
cd tsfile
./mvnw clean package -P with-cpp -Dbuild.test=OFF
cmake --install cpp/target/build --prefix "$PWD/tsfile-sdk"
```

Windows 使用 `mvnw.cmd`。随后将 `CMAKE_PREFIX_PATH` 指向安装后的 `tsfile-sdk`；
不要将源码树内部的头文件与另一个版本的已安装库混用。CLI 的安装与使用另见 [TsFile CLI](../Tools/Tsfile-CLI.md)。

## 写入流程

### 构造 TsFileWriter

```cpp
    std::string file_name = "test.tsfile";
    storage::libtsfile_init();

    std::string table_name = "table1";

    // Create a file with specify path to write tsfile.
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    mode_t mode = 0666;
    file.create("test_cpp.tsfile", flags, mode);

    // Create table schema to describe a table in a tsfile.
    auto* schema = new storage::TableSchema(
        table_name,
        {
            common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("id2", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("s1", common::INT64, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::FIELD),
        });

    // Create a file with specify path to write tsfile.
    auto* writer = new storage::TsFileTableWriter(&file, schema);
```

### 写入数据

```cpp
    // Create tablet to insert data.
    storage::Tablet tablet(
        table_name, {"id1", "id2", "s1"},
        {common::STRING, common::STRING, common::INT64},
        {common::ColumnCategory::TAG, common::ColumnCategory::TAG,
         common::ColumnCategory::FIELD},
        10);


    for (int row = 0; row < 5; row++) {
        long timestamp = row;
        tablet.add_timestamp(row, timestamp);
        tablet.add_value(row, "id1", "id1_filed_1");
        tablet.add_value(row, "id2", "id1_filed_2");
        tablet.add_value(row, "s1", static_cast<int64_t>(row));
    }

    // Write tablet data.
    writer->write_table(tablet);

    // Flush data
    writer->flush();
```

### 关闭文件

```cpp
    writer->close()
```

### 示例代码

使用这些接口的示例代码可以在以下链接中找到 <https://github.com/apache/tsfile/blob/develop/cpp/examples/cpp_examples/demo_write.cpp>

## 查询流程

### 构造 TsFileReader

```cpp
    int code = 0;
    storage::libtsfile_init();
    std::string table_name = "table1";

    // Create tsfile reader and open tsfile with specify path.
    storage::TsFileReader reader;
    reader.open("test_cpp.tsfile");
```

### 构造 Query Request

```cpp
    // Query data with tsfile reader.
    storage::ResultSet* temp_ret = nullptr;
    std::vector<std::string> columns;
    columns.emplace_back("id1");
    columns.emplace_back("id2");
    columns.emplace_back("s1");

    auto table_schema = reader.get_table_schema(table_name);
    storage::Filter* tag_filter1 = storage::TagFilterBuilder(table_schema.get()).eq("id1", "id1_filed_1");
    storage::Filter* tag_filter2 = storage::TagFilterBuilder(table_schema.get()).eq("id2", "id1_filed_2");
    storage::Filter* tag_filter = storage::TagFilterBuilder(table_schema.get()).and_filter(tag_filter1, tag_filter2);
    // Column vector contains the columns you want to select.
    reader.query(table_name, columns, 0, 100, temp_ret, tag_filter);
```

### 查询数据

```cpp
    // Get query handler.
    auto ret = dynamic_cast<storage::TableResultSet*>(temp_ret);

    // Metadata in query handler.
    auto metadata = ret->get_metadata();
    int column_num = metadata->get_column_count();
    for (int i = 1; i <= column_num; i++) {
        std::cout << "column name: " << metadata->get_column_name(i)
                  << std::endl;
        std::cout << "column type: "
                  << std::to_string(metadata->get_column_type(i)) << std::endl;
    }

    // Check and get next data.
    bool has_next = false;
    while ((code = ret->next(has_next)) == common::E_OK && has_next) {
        // Timestamp at column 1 and column index begin from 1.
        Timestamp timestamp = ret->get_value<Timestamp>(1);
        for (int i = 1; i <= column_num; i++) {
            if (ret->is_null(i)) {
                std::cout << "null" << std::endl;
            } else {
                switch (metadata->get_column_type(i)) {
                    case common::BOOLEAN:
                        std::cout << ret->get_value<bool>(i) << std::endl;
                        break;
                    case common::INT32:
                        std::cout << ret->get_value<int32_t>(i) << std::endl;
                        break;
                    case common::INT64:
                        std::cout << ret->get_value<int64_t>(i) << std::endl;
                        break;
                    case common::FLOAT:
                        std::cout << ret->get_value<float>(i) << std::endl;
                        break;
                    case common::DOUBLE:
                        std::cout << ret->get_value<double>(i) << std::endl;
                        break;
                    case common::STRING:
                        std::cout << ret->get_value<common::String*>(i)
                                         ->to_std_string()
                                  << std::endl;
                        break;
                    default:;
                }
            }
        }
    }
```

### 关闭文件

```cpp
    // Close query result set.
    ret->close();
    // Close reader.
    reader.close();
```

### 示例代码

使用这些接口的示例代码可以在以下链接中找到 <https://github.com/apache/tsfile/blob/develop/cpp/examples/cpp_examples/demo_read.cpp>

> 注意：以上读写示例均基于表模型接口，接口定义介绍可见[C++ 接口定义](./InterfaceDefinition/InterfaceDefinition-CPP.md)。若需了解树模型相关内容，请联系我们。
