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

# Java SDK

Use this reference only for Java SDK work. Resolve the dependency version from
the target project or current checkout before using an API.

## Tree-Model Write

```java
try (TsFileTreeWriter writer =
    new TsFileTreeWriterBuilder().file(new File("data.tsfile")).build()) {
  writer.registerTimeseries(
      "device1", new MeasurementSchema("temperature", TSDataType.FLOAT));
  TSRecord record =
      new TSRecord("device1", 1L).addPoint("temperature", 20.5f);
  writer.write(record);
}
```

For the current v4 API, the constructor order is
`TSRecord(deviceId, timestamp)` and the tree writer method is `write`. Avoid
deprecated `TsFileWriter.registerTimeseries(Path, ...)` examples from older
source lines.

## Tree-Model Read

```java
try (ITsFileTreeReader reader =
        new TsFileTreeReaderBuilder().file(new File("data.tsfile")).build();
    ResultSet rows =
        reader.query(
            List.of("device1"),
            List.of("temperature"),
            Long.MIN_VALUE,
            Long.MAX_VALUE)) {
  while (rows.next()) {
    Float value =
        rows.isNull("device1.temperature")
            ? null
            : rows.getFloat("device1.temperature");
  }
}
```

## Guidance

- Prefer `Tablet` for batch writes and `TSRecord` for genuinely record-oriented
  flows.
- Use try-with-resources for writers, readers, and result sets.
- For work inside the source checkout, use `java/examples/` as executable API
  documentation and verify with `./mvnw -P with-java clean verify`.
- For external projects, use an available release instead of assuming the
  checkout snapshot is publicly deployed.
- Confirm Tree versus Table examples before adapting a class with a similar
  name.
