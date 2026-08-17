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

# TsFile Core Concepts

Use this offline reference for model selection, schema design, supported value
types, and the language-independent read/write lifecycle. Read a language
reference separately only after selecting the binding.

## Data Models

- Tree model identifies a time series by device and measurement. Use it when
  device paths and measurement hierarchies are the natural identity.
- Table model organizes data into tables. TAG columns identify a device or
  entity; FIELD columns store measured values.
- Do not translate Tree and Table APIs by name alone. Their registration,
  filtering, and result APIs differ.

## Schema Decisions

- Use TAG columns for identifiers and relatively static dimensions such as
  device, location, or sensor type.
- Use FIELD columns for observations such as temperature, pressure, status, or
  counters.
- Choose the narrowest type that preserves the input domain:
  - `BOOLEAN` for flags.
  - `INT32` or `INT64` for integral values and counters.
  - `FLOAT` or `DOUBLE` for measurements requiring fractional precision.
  - `STRING` or `TEXT` for textual values; availability and naming may differ
    by binding.
  - `TIMESTAMP` and `DATE` for time-valued fields.
  - `BLOB` for opaque binary data.
- Confirm that the chosen binding supports a type/encoding combination before
  overriding defaults.

## Write Lifecycle

1. Select Tree or Table model.
2. Define and register the schema required by that model.
3. Create a writer.
4. Populate tablets/batches for normal throughput; use single-record writes
   only when latency or application structure requires them.
5. Write complete batches, flush when required by the binding, and close the
   writer to finalize the file.
6. Reopen the file and validate schema plus representative values.

## Read Lifecycle

1. Open the file with the matching reader.
2. Discover or provide the model and schema.
3. Select only required measurements or columns.
4. Bound the time range and supported filters when possible.
5. Consume rows or columnar batches incrementally.
6. Close the result set before closing the reader.

## Cross-Language Files

- Write with one binding and read with every binding required by the target
  system.
- Verify nulls, timestamps, textual values, `DATE`, and `BLOB` explicitly;
  language representations may differ even when the file type is compatible.
- Keep the writer and readers on compatible TsFile format/library versions.
