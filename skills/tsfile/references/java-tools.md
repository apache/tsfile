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

# Java Tools

Use this reference for the packaged Java command-line tools. Use
`java/tools/README.md`, its tests, and the current source as the exact command
authority. Do not infer these commands from the C++ `tsfile-cli` interface.

## Locate or Build

- In a packaged distribution, run scripts from `$TSFILE_HOME/tools/`; they use
  the jars under `$TSFILE_HOME/lib/`.
- In a source checkout, build the Java distribution with the checkout's
  documented `./mvnw clean package -P with-java -DskipTests` flow before using
  the packaged scripts. Treat `java/tools/src/assembly/resources/tools/` as
  packaging source, not as an independently installed runtime.
- Require Java and `JAVA_HOME` as documented by the current checkout.

## Import CSV, Parquet, or Arrow

Prefer the Java importer for format-aware or batch conversion: a source file or
directory, CSV/Parquet/Arrow input, explicit schema mapping, schema inference,
failed-file collection, threading, or CSV chunking.

```sh
$TSFILE_HOME/tools/csv2tsfile.sh --source input.csv --target output
$TSFILE_HOME/tools/parquet2tsfile.sh --source parquet-dir --target output
$TSFILE_HOME/tools/arrow2tsfile.sh --source input.arrow --target output
$TSFILE_HOME/tools/csv2tsfile.sh --source input-dir --target output \
  --fail_dir failed --schema import.schema
```

Use the corresponding `.bat` scripts on Windows. Omit `--schema` only when the
current auto-inference rules are acceptable. Read `java/tools/README.md` before
generating a schema or relying on time-column, tag-column, null, delimiter,
type-inference, file-naming, or failure-handling behavior.

Use the C++ `tsfile-cli write` command only when the requested operation is its
narrow pipe-friendly case: one CSV stream or file, one new table-model TsFile,
and an explicit `--tag`/`--field` schema.

## Check or Backfill Table Point Count

Use the Java point-count tool for the persisted table-level property:

```sh
$TSFILE_HOME/tools/tsfile-table-point-count.sh /data/example.tsfile
```

Use the `.bat` wrapper on Windows. Pass exactly one complete TsFile. The count
is the total number of non-null FIELD values for each table; TAG and time
columns are excluded.

Interpret `UPDATED` as an in-place metadata backfill, `ALREADY_PRESENT` as no
change, and `NO_TABLE` as a tree-only/no-table result. Before running it,
resolve the exact input path, ensure the containing directory is writable and
has room for a complete temporary copy, and preserve a backup when the data is
not otherwise recoverable. Missing and incomplete files must remain errors.

Do not use C++ `tsfile-cli count` as a substitute. That command reads
per-series statistics and prints counts; it neither validates nor writes the
table-level point-count property.

## Source Anchors

- Import behavior: `java/tools/README.md`,
  `java/tools/src/main/java/org/apache/tsfile/tools/`, and its tests.
- Import wrappers: `java/tools/src/assembly/resources/tools/`.
- Point-count implementation:
  `java/tsfile/src/main/java/org/apache/tsfile/utils/TsFileTablePointCountTool.java`.
- Point-count wrapper and user contract: `java/tools/README.md` and
  `java/tools/src/assembly/resources/tools/tsfile-table-point-count.*`.
