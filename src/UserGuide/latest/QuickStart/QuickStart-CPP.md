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
# Quick Start - C++

## Installation

C/C++ 2.5.0 provides prebuilt runtimes, development packages, and the CLI; **building TsFile with Maven is no longer a prerequisite**.
Application development requires a C/C++ compiler. The installed CMake package supports CMake 3.11+; selecting the Windows runtime requires CMake 3.15+.
Running only the CLI does not require a compiler. See the [download and installation page](../../../Download/README.md#cpp-packages) for all artifacts and checksum instructions.

### Choose an installation method

| Platform | Packages for C/C++ development | Instructions |
| --- | --- | --- |
| Ubuntu 22.04 / 24.04 | `tsfile` + `tsfile-dev` | [Download, verify, and install DEBs](../../../Download/README.md#cpp-ubuntu) |
| AlmaLinux 9 | `tsfile` + `tsfile-devel` | [Download, verify, and install RPMs](../../../Download/README.md#cpp-rpm) |
| macOS | Homebrew Formula or SDK archive | [Homebrew installation](../../../Download/README.md#cpp-homebrew), including library, headers, and CLI |
| Linux / macOS | Complete SDK archive | Install without changing system directories, as shown below |
| Windows | Matching-architecture SDK/CLI ZIP | [Windows installation](../../../Download/README.md#cpp-windows); retain the DLL and `bin/` layout |

`tsfile-dev` / `tsfile-devel` are development-file packages, not development-release channels. The CLI is in `tsfile-tools`.
After downloading and verifying the DEB/RPM files, install the local runtime and development packages, for example:

```bash
# Ubuntu amd64; use matching filenames for arm64
sudo apt install ./tsfile_2.5.0-1_amd64.deb ./tsfile-dev_2.5.0-1_amd64.deb
```

```bash
# AlmaLinux 9 x86_64; use matching filenames for aarch64
sudo dnf install ./tsfile-2.5.0-1.el9.x86_64.rpm ./tsfile-devel-2.5.0-1.el9.x86_64.rpm
```

### Extract an SDK (Linux / macOS)

Run in an empty directory. Use `macos12-arm64` on Apple Silicon Macs, `macos12-x86_64` on Intel Macs,
or `manylinux_2_28-x86_64` / `manylinux_2_28-aarch64` on Linux, according to architecture.

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

For Homebrew, first import the Formula as described on the download page, then set `TSFILE_HOME="$(brew --prefix local/tsfile250/tsfile)"`.
SDKs contain `include/`, `lib/`, `lib/cmake/TsFile/`, `lib/pkgconfig/`, `bin/`, and `share/`; preserve their relative layout.

### CMake integration

Use the installed package's exported target in your project's `CMakeLists.txt`, rather than finding just the library file with `find_library`:

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

For DEB/RPM installations in standard system locations, `CMAKE_PREFIX_PATH` is normally unnecessary. The exported target supplies include directories and required compile definitions.
On Windows, use matching-architecture MSVC and CMake 3.15+ to select the package's Release `/MT` runtime:

```powershell
cmake -S . -B build -DCMAKE_PREFIX_PATH="$env:TSFILE_HOME" `
  -DCMAKE_POLICY_DEFAULT_CMP0091=NEW -DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded
cmake --build build --config Release
```

### pkg-config (Linux / macOS)

Locate metadata under an SDK or Homebrew prefix; system packages are normally discoverable without this assignment:

```bash
export PKG_CONFIG_PATH="$TSFILE_HOME/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
pkg-config --modversion libtsfile
pkg-config --cflags --libs libtsfile
```

### Optional: build from source

Build from source only to change TsFile or customize build options. This requires JDK 17+, CMake, a C++11 compiler, and Make/Ninja;
the repository supplies a Maven wrapper. The source-built version follows your checked-out branch or commit, not necessarily the 2.5.0 binary packages above.

```bash
git clone https://github.com/apache/tsfile.git
cd tsfile
./mvnw clean package -P with-cpp -Dbuild.test=OFF
cmake --install cpp/target/build --prefix "$PWD/tsfile-sdk"
```

Use `mvnw.cmd` on Windows. Then point `CMAKE_PREFIX_PATH` at the installed `tsfile-sdk` prefix;
do not mix source-tree headers with an installed library from a different version. See [TsFile CLI](../Tools/Tsfile-CLI.md) for CLI installation and usage.

## Writing Process

### Construct TsFileWriter

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

### Write Data

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

### Close File

```cpp
    writer->close()
```

### Sample Code

The sample code of using these interfaces is in <https://github.com/apache/tsfile/blob/develop/cpp/examples/cpp_examples/demo_write.cpp>

## Query Process

### Construct TsFileReader

```cpp
    int code = 0;
    storage::libtsfile_init();
    std::string table_name = "table1";

    // Create tsfile reader and open tsfile with specify path.
    storage::TsFileReader reader;
    reader.open("test_cpp.tsfile");
```

### Construct Query Request

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

### Query Data

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

### Close File

```cpp
    // Close query result set.
    ret->close();
    // Close reader.
    reader.close();
```

### Sample Code

The sample code of using these interfaces is in <https://github.com/apache/tsfile/blob/develop/cpp/examples/cpp_examples/demo_read.cpp>

> Note: The above read/write examples are all based on the table model interface. For details about the interface definition, please refer to [C++ Interface Definition](./InterfaceDefinition/InterfaceDefinition-CPP.md). If you need information regarding the tree model, please contact us.
