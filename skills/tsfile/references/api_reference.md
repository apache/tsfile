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

# TsFile API Reference

This reference targets the APIs in the current repository checkout. At the
time of this update, the Maven modules are `2.3.2-SNAPSHOT` and the Python
package is `2.3.2.dev`. Re-read the root `pom.xml` and `python/pyproject.toml`
instead of copying these versions into long-lived external projects.

## Build Baseline

- Java: JDK 17, Maven 3.6+; prefer the repository's `./mvnw`.
- C++: CMake 3.11+, a C++11 compiler, make, clang-format, and platform UUID headers where required.
- Python: Python 3.9+; the Python binding depends on the C++ module.

```bash
./mvnw -P with-java clean verify
./mvnw -P with-cpp clean verify
./mvnw -P with-python clean verify
```

## Data Model

- Tree model identifies a series by device and measurement.
- Table model uses TAG columns for device identity and FIELD columns for values.
- Supported public data types include BOOLEAN, INT32, INT64, FLOAT, DOUBLE,
  STRING/TEXT, TIMESTAMP, DATE, and BLOB.
- Use implementation defaults for encoding and compression unless workload
  measurements justify an override.

## Java API

### Dependency

For this checkout, install the Java module locally and use its current version:

```bash
./mvnw -P with-java clean install -DskipTests
```

```xml
<dependency>
  <groupId>org.apache.tsfile</groupId>
  <artifactId>tsfile</artifactId>
  <version>2.3.2-SNAPSHOT</version>
</dependency>
```

External projects should use an available released version instead of assuming
the snapshot exists in a public repository.

### Tree-model write

```java
try (TsFileTreeWriter writer =
    new TsFileTreeWriterBuilder().file(new File("data.tsfile")).build()) {
  writer.registerTimeseries(
      "device1", new MeasurementSchema("temperature", TSDataType.FLOAT));
  writer.registerTimeseries(
      "device1", new MeasurementSchema("humidity", TSDataType.FLOAT));
  TSRecord record = new TSRecord("device1", 1L)
      .addPoint("temperature", 20.5f)
      .addPoint("humidity", 50.0f);
  writer.write(record);
}
```

The current constructor order is `TSRecord(deviceId, timestamp)`, and the
current v4 tree writer method is `write`. Avoid the deprecated
`TsFileWriter.registerTimeseries(Path, ...)` overloads.

### Tree-model read

```java
try (ITsFileTreeReader reader =
        new TsFileTreeReaderBuilder().file(new File("data.tsfile")).build();
    ResultSet rows =
        reader.query(
            List.of("device1"),
            List.of("temperature", "humidity"),
            Long.MIN_VALUE,
            Long.MAX_VALUE)) {
  while (rows.next()) {
    long timestamp = rows.getLong(1);
    Float temperature =
        rows.isNull("device1.temperature")
            ? null
            : rows.getFloat("device1.temperature");
  }
}
```

Use `java/examples/` as the executable source of truth for Java APIs.

## Python API

### Build

```bash
./mvnw -P with-python clean verify
```

For a direct Python build, first build C++, then set `TSFILE_CPP_BUILD` if the
build output is outside the locations recognized by `python/setup.py`.

### Table-model write

```python
from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    Tablet,
    TSDataType,
    TsFileTableWriter,
)

columns = [
    ColumnSchema("device", TSDataType.STRING, ColumnCategory.TAG),
    ColumnSchema("temperature", TSDataType.DOUBLE, ColumnCategory.FIELD),
]
schema = TableSchema("sensors", columns)
tablet = Tablet(
    schema.get_column_names(),
    [column.get_data_type() for column in schema.get_columns()],
    2,
)
tablet.add_timestamp(0, 1)
tablet.add_value_by_name("device", 0, "d1")
tablet.add_value_by_name("temperature", 0, 20.5)

with TsFileTableWriter("data.tsfile", schema) as writer:
    writer.write_table(tablet)
```

`ColumnSchema` exposes `get_column_name()` and `get_data_type()`; it does not
expose a `name` attribute.

### Table-model read

```python
from tsfile import TsFileReader

with TsFileReader("data.tsfile") as reader:
    schemas = reader.get_all_table_schemas()
    schema = next(iter(schemas.values())) if isinstance(schemas, dict) else schemas[0]
    table_name = schema.get_table_name()
    with reader.query_table_by_row(
        table_name, ["device", "temperature"], limit=10
    ) as result:
        frame = result.read_data_frame(max_row_num=10)
```

Depending on the binding layer, schema collections may be lists or mappings;
normalize them before iteration when writing reusable utilities. Result sets
are context managers and should be closed promptly.

Use `python/examples/example.py` and `python/tests/` as the executable source of
truth for Python APIs.

## C++ API

### Build

```bash
./mvnw -P with-cpp clean verify
# or
cd cpp && bash build.sh -t=Debug --disable-antlr4
```

### Table-model write

```cpp
storage::WriteFile file;
file.create("data.tsfile", O_WRONLY | O_CREAT | O_TRUNC, 0666);

storage::TableSchema schema(
    "sensors",
    {common::ColumnSchema("device", common::STRING, common::UNCOMPRESSED,
                          common::PLAIN, common::ColumnCategory::TAG),
     common::ColumnSchema("temperature", common::DOUBLE, common::UNCOMPRESSED,
                          common::PLAIN, common::ColumnCategory::FIELD)});

storage::TsFileTableWriter writer(&file, &schema);
storage::Tablet tablet(
    "sensors", {"device", "temperature"},
    {common::STRING, common::DOUBLE},
    {common::ColumnCategory::TAG, common::ColumnCategory::FIELD}, 1);
tablet.add_timestamp(0, 1);
tablet.add_value(0, "device", "d1");
tablet.add_value(0, "temperature", 20.5);
writer.write_table(tablet);
writer.flush();
writer.close();
```

The current `TsFileTableWriter` method is `write_table(Tablet&)`, not
`write_tablet`. Flush buffered rows before closing, as shown by the maintained
C++ example.

### Table-model read

```cpp
storage::TsFileReader reader;
reader.open("data.tsfile");

storage::ResultSet* result = nullptr;
std::vector<std::string> columns{"device", "temperature"};
reader.query("sensors", columns, 0, 100, result);

bool has_next = false;
while (result->next(has_next) == common::E_OK && has_next) {
    double temperature = result->get_value<double>("temperature");
}
result->close();
reader.close();
```

The current reader uses `query(..., ResultSet*&)`; it does not expose the old
`get_table_names()` and `read_table()` helpers.

Use `cpp/examples/cpp_examples/` and public headers under `cpp/src/` as the
executable source of truth for C++ APIs.

## C API

The C wrapper is declared in `cpp/src/cwrapper/tsfile_cwrapper.h`. Prefer the
maintained examples instead of inventing opaque wrapper names:

- Write: `cpp/examples/c_examples/demo_write.c`
- Read: `cpp/examples/c_examples/demo_read.c`

Current entry points include `write_file_new`, `tsfile_writer_new`,
`tablet_new`, `tablet_add_timestamp`, typed
`tablet_add_value_by_name_*`, `tsfile_writer_write`, `tsfile_reader_new`, and
`tsfile_query_table`. Release resources with the matching `free_*` and close
functions shown in those examples.

## Compatibility Checklist

1. Resolve the version and language requirements from the current checkout.
2. Prefer examples and public headers in the same commit over copied snippets.
3. Keep Java, C++, Python, and C APIs separate; similarly named operations do
   not necessarily share signatures.
4. Close writers, readers, and result sets so file footers and native resources
   are finalized correctly.
5. Validate cross-language files with a reader from each language involved.
