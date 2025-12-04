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

If Maven is not installed, you can compile tsfile using the following command:

Linux or Macos:
```shell
mvnw clean install -P with-cpp -DskipTests
```
Windows:
```shell
mvnw.cmd clean install -P with-cpp -DskipTests
```
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

• **Include Directory**: Located at `tsfile/cpp/target/build/include/cwrapper`, it contains header files for integration. Add this path to the compiler's include path (e.g., using `-I` flag).

• **Lib Directory**: At `tsfile/cpp/target/build/lib`, it holds compiled library files. Specify this path and library name when linking (e.g., using `-L` and `-l` flags).

### CMake Integration
Add to your CMakeLists.txt:
```CMAKE
find_library(TSFILE_LIB NAMES tsfile PATHS ${SDK_LIB} REQUIRED)
target_link_libraries(your_target ${TSFILE_LIB})
```
Note: Set ${SDK_LIB} to your TsFile library directory.

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

