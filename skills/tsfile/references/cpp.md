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

# C++ SDK

Use this reference only for C++ SDK work. Verify exact constructors and
ownership against public headers under `cpp/src/` in the target checkout.

## Table-Model Write

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

The current method is `write_table(Tablet&)`, not `write_tablet`. Flush
buffered rows before closing as demonstrated by the maintained examples.

## Table-Model Read

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

## Guidance

- Current readers use `query(..., ResultSet*&)`; do not reuse old
  `get_table_names()` or `read_table()` helpers without version verification.
- Prefer stack/RAII ownership where supported, but still follow explicit
  close/free requirements in the public API and examples.
- Use `cpp/examples/cpp_examples/` as executable API documentation.
- Verify with `./mvnw -P with-cpp clean verify` or the checkout's documented
  `cpp/build.sh` flow.
