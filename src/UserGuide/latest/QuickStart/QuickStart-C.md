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
# Quick Start - C

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
project(tsfile_example LANGUAGES C CXX)
find_package(TsFile 2.5.0 CONFIG REQUIRED)
add_executable(tsfile_example main.c)
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

### Write Data

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
### Close File

```C
    // Close writer.
    HANDLE_ERROR(tsfile_writer_close(writer));

    // Close write file after closing writer.
    free_write_file(&file);
```
### Sample Code

The sample code of using these interfaces is in <https://github.com/apache/tsfile/blob/develop/cpp/examples/c_examples/demo_write.c>

## Reading Process

### Construct TsFileReader

```C
    ERRNO code = 0;
    char* table_name = "table1";

    // Create tsfile reader with specify tsfile's path
    TsFileReader reader = tsfile_reader_new("test_c.tsfile", &code);
    HANDLE_ERROR(code);
```

### Construct Query 
```C
    ResultSet ret = tsfile_query_table(
        reader, table_name, (char*[]){"id1", "id2", "s1"}, 3, 0, 10, &code);
    HANDLE_ERROR(code);

    if (ret == NULL) {
        HANDLE_ERROR(RET_INVALID_QUERY);
    }
```

### Query Data

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

### Close File

```C
    // Close tsfile reader.
    tsfile_reader_close(reader);
```
### Sample Code

The sample code of using these interfaces is in <https://github.com/apache/tsfile/blob/develop/cpp/examples/c_examples/demo_read.c>

> Note: The above read/write examples are all based on the table model interface. For details about the interface definition, please refer to [C Interface Definition](./InterfaceDefinition/InterfaceDefinition-C.md). If you need information regarding the tree model, please contact us.
