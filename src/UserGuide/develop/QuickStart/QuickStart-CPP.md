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

## Dependencies

- CMake >=3.11
- Maven >=3.9.6
- GCC >=4.8.5
- Make >=4.3

## Installation Method

Clone the source code from git:

```shell
git clone https://github.com/apache/tsfile.git
```
Run Maven to compile in the TsFile root directory:

```shell
mvn clean install -P with-cpp -DskipTests
```

Note: If Maven is not installed, you can use mvnw on Linux or mvnw.cmd on Windows.
### Compilation Options
TsFile provides build options to control the size of the generated library:
```shell
# Minimal build
mvn clean install -P with-cpp -DskipTests \
  -Dbuild.test=OFF    \
  -Denable.snappy=OFF \
  -Denable.lz4=OFF    \
  -Denable.lzokay=OFF \
  -Denable.zlib=OFF   \
  -Denable.antlr4=OFF
```

**Option Descriptions**
- `enable.*=OFF:` Do not compile compression algorithms (reduces library size)
- `enable.antlr4=OFF:` Do not compile ANTLR4 dependency (reduces library size)

**Library Size Comparison**
- `Full build:` ~3.2MB
- `With ANTLR4 disabled:` ~1.9MB
- `With all compression algorithms disabled:` ~1.7MB
### Directory Structure

• **Include Directory**: Located at `tsfile/cpp/target/build/include`, it contains header files for integration. Add this path to the compiler's include path (e.g., using `-I` flag).

• **Lib Directory**: At `tsfile/cpp/target/build/lib`, it holds compiled library files. Specify this path and library name when linking (e.g., using `-L` and `-l` flags).

### CMake Integration
Add to your CMakeLists.txt:
```CMAKE
find_library(TSFILE_LIB NAMES tsfile PATHS ${SDK_LIB} REQUIRED)
target_link_libraries(your_target ${TSFILE_LIB})
```
Note: Set ${SDK_LIB} to your TSFile library directory.

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

    // Column vector contains the columns you want to select.
    reader.query(table_name, columns, 0, 100, temp_ret);
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

