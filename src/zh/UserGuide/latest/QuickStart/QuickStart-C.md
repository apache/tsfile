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
# 快速上手 - C

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
project(tsfile_example LANGUAGES C CXX)
find_package(TsFile 2.5.0 CONFIG REQUIRED)
add_executable(tsfile_example main.c)
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

```C
ERRNO code = 0;
    char* table_name = "table1";

    // Create table schema to describe a table in a tsfile.
    TableSchema table_schema;
    table_schema.table_name = strdup(table_name);
    table_schema.column_num = 3;
    table_schema.column_schemas =
        (ColumnSchema*)malloc(sizeof(ColumnSchema) * 3);
    table_schema.column_schemas[0] =
        (ColumnSchema){.column_name = strdup("id1"),
                     .data_type = TS_DATATYPE_STRING,
                     .column_category = TAG};
    table_schema.column_schemas[1] =
        (ColumnSchema){.column_name = strdup("id2"),
                     .data_type = TS_DATATYPE_STRING,
                     .column_category = TAG};
    table_schema.column_schemas[2] =
        (ColumnSchema){.column_name = strdup("s1"),
                     .data_type = TS_DATATYPE_INT32,
                     .column_category = FIELD};

    // Create a file with specify path to write tsfile.
    WriteFile file = write_file_new("test_c.tsfile", &code);
    HANDLE_ERROR(code);

    // Create tsfile writer with specify table schema.
    TsFileWriter writer = tsfile_writer_new(file, &table_schema, &code);
    HANDLE_ERROR(code);
```

### 写入数据

```C
// Create tablet to insert data.
    Tablet tablet =
        tablet_new((char*[]){"id1", "id2", "s1"},
                   (TSDataType[]){TS_DATATYPE_STRING, TS_DATATYPE_STRING,
                                  TS_DATATYPE_INT32},
                   3, 5);

    for (int row = 0; row < 5; row++) {
        Timestamp timestamp = row;
        tablet_add_timestamp(tablet, row, timestamp);
        tablet_add_value_by_name_string(tablet, row, "id1", "id_field_1");
        tablet_add_value_by_name_string(tablet, row, "id2", "id_field_2");
        tablet_add_value_by_name_int32_t(tablet, row, "s1", row);
    }

    // Write tablet data.
    HANDLE_ERROR(tsfile_writer_write(writer, tablet));

    // Free tablet.
    free_tablet(&tablet);

    // Free table schema we used before.
    free_table_schema(table_schema);
```
### 关闭文件

```C
    // Close writer.
    HANDLE_ERROR(tsfile_writer_close(writer));

    // Close write file after closing writer.
    free_write_file(&file);
```
### 示例代码

The sample code of using these interfaces is in <https://github.com/apache/tsfile/blob/develop/cpp/examples/c_examples/demo_write.c>

## 读取流程

### 构造 TsFileReader

```C
    ERRNO code = 0;
    char* table_name = "table1";

    // Create tsfile reader with specify tsfile's path
    TsFileReader reader = tsfile_reader_new("test_c.tsfile", &code);
    HANDLE_ERROR(code);
```

### 构造 Query Request
```C
    ResultSet ret = tsfile_query_table(
        reader, table_name, (char*[]){"id1", "id2", "s1"}, 3, 0, 10, &code);
    HANDLE_ERROR(code);

    if (ret == NULL) {
        HANDLE_ERROR(RET_INVALID_QUERY);
    }
```

### 查询数据

```C
// Get query result metadata: column name and datatype
    ResultSetMetaData metadata = tsfile_result_set_get_metadata(ret);
    int column_num = tsfile_result_set_metadata_get_column_num(metadata);

    for (int i = 1; i <= column_num; i++) {
        printf("column:%s, datatype:%d\n", tsfile_result_set_metadata_get_column_name(metadata, i),
               tsfile_result_set_metadata_get_data_type(metadata, i));
    }

    // Get data by column name or index.
    while (tsfile_result_set_next(ret, &code) && code == RET_OK) {
        // Timestamp at column 1 and column index begin from 1.
        Timestamp timestamp =
            tsfile_result_set_get_value_by_index_int64_t(ret, 1);
        printf("%ld\n", timestamp);
        for (int i = 1; i <= column_num; i++) {
            if (tsfile_result_set_is_null_by_index(ret, i)) {
                printf(" null ");
            } else {
                switch (tsfile_result_set_metadata_get_data_type(metadata, i)) {
                    case TS_DATATYPE_BOOLEAN:
                        printf("%d\n", tsfile_result_set_get_value_by_index_bool(
                                         ret, i));
                        break;
                    case TS_DATATYPE_INT32:
                        printf("%d\n",
                               tsfile_result_set_get_value_by_index_int32_t(ret,
                                                                            i));
                        break;
                    case TS_DATATYPE_INT64:
                        printf("%ld\n",
                               tsfile_result_set_get_value_by_index_int64_t(ret,
                                                                            i));
                        break;
                    case TS_DATATYPE_FLOAT:
                        printf("%f\n", tsfile_result_set_get_value_by_index_float(
                                         ret, i));
                        break;
                    case TS_DATATYPE_DOUBLE:
                        printf("%lf\n",
                               tsfile_result_set_get_value_by_index_double(ret,
                                                                           i));
                        break;
                    case TS_DATATYPE_STRING:
                        printf("%s\n",
                               tsfile_result_set_get_value_by_index_string(ret,
                                                                           i));
                        break;
                    default:
                        printf("unknown_type");
                        break;
                }
            }
        }
    }

    // Free query meta data
    free_result_set_meta_data(metadata);

    // Free query handler.
    free_tsfile_result_set(&ret);
```

### 关闭文件

```C
    // Close tsfile reader.
    tsfile_reader_close(reader);
```
### 示例代码

The sample code of using these interfaces is in <https://github.com/apache/tsfile/blob/develop/cpp/examples/c_examples/demo_read.c>


> 注意：以上读写示例均基于表模型接口，接口定义介绍可见[C 接口定义](./InterfaceDefinition/InterfaceDefinition-C.md)。若需了解树模型相关内容，请联系我们。
