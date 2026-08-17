---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
name: tsfile
description: Comprehensive toolkit for working with Apache TsFile - a columnar storage format for time series data. Use when working with TsFile files (.tsfile extension), time series data storage, IoT data processing, or when users ask about reading, writing, querying, or analyzing time series data in Java, Python, C++, or C. Supports data conversion, schema design, performance optimization, and cross-language integration.
---

# TsFile

Apache TsFile is a columnar storage file format designed specifically for time series data, offering efficient compression, high throughput read/write operations, and compatibility with various big data frameworks.

## Command-Line Boundary

Use the sibling `tsfile-cli` skill at `../tsfile-cli/SKILL.md` for shell-oriented
inspection, preview, export, sampling, and CSV/TSV-to-TsFile conversion. Keep
this skill focused on programmatic SDK workflows, schema design, and
cross-language integration.

## Quick Start Guide

Choose your programming language to get started:

### Java

```java
// Resolve the version from the current checkout's root pom.xml.
// This checkout currently builds org.apache.tsfile:tsfile:2.3.2-SNAPSHOT.

try (TsFileTreeWriter writer =
    new TsFileTreeWriterBuilder().file(new File("data.tsfile")).build()) {
  writer.registerTimeseries("device1", schema);
  writer.write(tsRecord);
}

try (ITsFileTreeReader reader =
    new TsFileTreeReaderBuilder().file(new File("data.tsfile")).build()) {
  // Query into a closeable ResultSet; see references/api_reference.md.
}
```

### Python

```python
# Python 3.9+; the with-python profile builds the required C++ module.
# ./mvnw -P with-python clean verify

from tsfile import TsFileTableWriter, TsFileReader, TableSchema

# Write data
with TsFileTableWriter("data.tsfile", schema) as writer:
    writer.write_table(tablet)

# Query data. Current packaged Python bindings expose query_table/query_table_by_row,
# not the older get_table_names/read_table pandas-style helpers.
with TsFileReader("data.tsfile") as reader:
    schemas = reader.get_all_table_schemas()
    with reader.query_table_by_row(
        table_name, ["device_id", "temperature"], limit=10
    ) as result:
        frame = result.read_data_frame(max_row_num=10)
```

### C++

```cpp
// Build from the repository root: ./mvnw -P with-cpp clean verify

#include <writer/tsfile_table_writer.h>

storage::TsFileTableWriter writer(&file, schema);
writer.write_table(tablet);
writer.flush();
writer.close();

storage::TsFileReader reader;
reader.open("data.tsfile");
storage::ResultSet* result = nullptr;
reader.query(table_name, columns, start_time, end_time, result);
result->close();
reader.close();
```

## Core Workflows

### 1. Data Writing Workflow

**Single Record Writing** (Java, lower throughput)

1. Create `TsFileTreeWriter` with `TsFileTreeWriterBuilder`
2. Register time series schema with `registerTimeseries()`
3. Create `TSRecord` objects with device id first: `TSRecord(deviceId, timestamp)`
4. Write records using `writer.write(tsRecord)`
5. Close writer to finalize file

**Batch Writing** (All languages, recommended)

1. Define table schema with columns and data types
2. Create writer instance
3. Create tablets/batches with multiple records
4. Write complete tablets for better performance
5. Flush explicitly where required by the language API, then close resources

### 2. Data Reading Workflow

**Basic Reading**

1. Open TsFile with appropriate reader
2. Get available tables/time series
3. Build query expressions (optional filters)
4. Execute query and iterate through results
5. Process data and close reader

**Advanced Querying**

1. Define a bounded time range
2. Add supported tag/field filters for the selected language API
3. Select specific measurements/columns
4. Apply aggregation or analysis logic

### 3. Schema Design Workflow

**Column Categories**

- **TAG**: Device identifiers, locations, static metadata
- **FIELD**: Actual measurements (temperature, pressure, etc.)

**Data Type Selection**

- INT32/INT64: Counters, IDs, discrete values
- FLOAT/DOUBLE: Sensor readings, calculations
- BOOLEAN: Status flags, binary states
- STRING/TEXT: Device names, error messages
- TIMESTAMP/DATE: Time-valued fields
- BLOB: Binary payloads

**Encoding and Compression**

- Prefer each language implementation's current defaults unless measurements justify tuning.
- In the current Java implementation, defaults are TS_2DIFF for time/integer columns, GORILLA for floating-point columns, RLE for booleans, and LZ4 compression.
- Validate an encoding against the selected data type and language implementation before overriding defaults; support is not identical across every binding.

## Language-Specific Operations

### Java Development

- **Setup**: Use JDK 17 and resolve `org.apache.tsfile:tsfile` from the current checkout's root `pom.xml`; use a deployed release for external projects
- **Writing**: Prefer `Tablet` API for batch operations over `TSRecord`
- **Reading**: Build `ITsFileTreeReader` with `TsFileTreeReaderBuilder`; query into a closeable `ResultSet`
- **Error Handling**: Catch `WriteProcessException` and `IOException`
- **Template**: Use `assets/TsFileExample.java` and `assets/pom.xml`

### Python Integration

- **Prerequisites**: Python 3.9+; the `with-python` Maven profile builds the required C++ module
- **API Style**: Table writer/reader APIs with `Tablet`, `TableSchema`, `query_table`, and `query_table_by_row`
- **Context Managers**: Use `with` statements for automatic resource cleanup
- **DataFrames/Arrow**: Result sets expose `read_data_frame()` and `read_arrow_batch()` where available
- **Tools**: Use `scripts/example.py` as a Python API example. For routine shell conversion, load the sibling `tsfile-cli` skill.

### C++ Implementation

- **Build Requirements**: CMake 3.11+, a C++11 compiler, make, clang-format, and UUID headers where required by the platform
- **Resource Management**: Prefer stack/RAII objects and close writers, readers, and result sets explicitly where required
- **API**: Lower-level control over encoding and compression
- **Template**: Use `assets/tsfile_example.cpp`

### C Wrapper

- **Use Case**: Integration with C projects or other language bindings
- **API**: Function-based interface around C++ implementation
- **Memory**: Explicit create/free patterns for all objects
- **Portability**: Cross-platform compatibility layer

## Development Tools

- Use the repository-level Maven profiles or language-specific build files for SDK development.
- Use `scripts/build_tsfile.sh` for current repository language build checks.
- Use `scripts/example.py` when you need a minimal Python API example for metadata inspection or writer code.
- Load the sibling `tsfile-cli` skill for command-line builds and operations.

## Performance Optimization

### Writing Performance

- Use tablet/batch writing instead of individual records
- Benchmark tablet sizes against the target workload and memory budget
- Group related measurements in same device for locality
- Choose efficient encoding for your data patterns

### Reading Performance

- Use time range filters to limit data scanned
- Select only needed columns in queries
- Leverage indexes on device and time dimensions
- Consider memory constraints for large result sets

### Storage Efficiency

- Apply recommended encoding/compression combinations
- Use appropriate data types (don't over-specify precision)
- Design schema with proper tag vs field categorization
- Monitor compression ratios and adjust settings

## Common Patterns

### IoT Sensor Data

```java
// Tag columns: device_id, location, sensor_type
// Field columns: temperature, humidity, battery_level
// Time series per device with multiple measurements
```

### Industrial Monitoring

```cpp
// Batch writing for high-frequency data
// Time-based partitioning for historical analysis
// Real-time queries with time range filters
```

### Data Pipeline Integration

```python
# CSV/TSV conversion is available through the sibling tsfile-cli skill
# Apache Spark/Flink compatibility
# ETL workflow integration
```

## Troubleshooting

**Build Issues**

- Java: Verify JDK 17 and Maven 3.6+ (the repository includes `./mvnw`)
- C++: Install required system packages (CMake 3.11+, a C++11 compiler, make, and libuuid development headers where required)
- Python: Verify Python 3.9+ and build through `./mvnw -P with-python clean verify`

**Runtime Errors**

- File corruption: Use validation tools to check file integrity
- Memory issues: Reduce tablet batch sizes or use streaming reads
- Performance: Profile encoding choices and query patterns

**Integration Problems**

- Classpath: Ensure TsFile JAR is in application classpath
- Native libraries: Verify shared libraries (.so/.dll) are accessible
- Version compatibility: Match TsFile versions across language bindings

## Resources

### Reference Documentation

- **API Reference**: Complete documentation for all supported languages in `references/api_reference.md`

### Code Templates

- **Java**: `assets/TsFileExample.java` and `assets/pom.xml`
- **C++**: `assets/tsfile_example.cpp`
- **Python**: `assets/tsfile_example.py`

### Utility Scripts

- **Build Automation**: `scripts/build_tsfile.sh` for cross-platform builds
- **Python Tools**: `scripts/example.py` for data conversion and validation
