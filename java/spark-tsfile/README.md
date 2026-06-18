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

# TsFile Spark Connector

This module provides a Spark 3.x DataSource V2 connector for TsFile table model
files. The short name is `tsfile`.

## Read

```scala
val df = spark.read
  .format("tsfile")
  .option("model", "table")
  .option("table", "weather")
  .load("/data/tsfile/weather")

df.select("time", "city", "temperature")
  .where("time >= 1700000000000 and city = 'beijing'")
  .show()
```

If every input TsFile contains exactly one table, `table` can be omitted and the
connector will infer it from the TsFile metadata. If an input file contains
multiple tables, `table` must be provided.

The input path can be a single `.tsfile`, a directory containing `.tsfile` files,
or a glob path. The initial connector discovers paths through Hadoop APIs but
opens TsFile data with the local Java reader, so only local file paths are
supported.

## Write

```scala
val rows = Seq(
  (1700000000000L, "beijing", 20, 0.32d),
  (1700000001000L, "shanghai", 21, 0.35d)
).toDF("time", "city", "temperature", "humidity")

rows.write
  .format("tsfile")
  .option("model", "table")
  .option("table", "weather")
  .option("timeColumn", "time")
  .option("tagColumns", "city")
  .option("compression", "lz4")
  .mode("append")
  .save("/data/tsfile/weather")
```

Each Spark task writes a separate `part-*.tsfile`. Append mode adds new TsFile
files to the output directory; it does not append rows to an existing TsFile.

TAG columns must be non-null strings. FIELD columns default to all columns that
are not the time column or TAG columns, and null FIELD values are written as
sparse TsFile values.

## SQL Temporary View

```scala
spark.read
  .format("tsfile")
  .option("table", "weather")
  .load("/data/tsfile/weather/*.tsfile")
  .createOrReplaceTempView("weather_tsfile")

spark.sql(
  """
    |select time, city, temperature
    |from weather_tsfile
    |where city = 'beijing'
    |order by time
    |""".stripMargin)
  .show()
```

## Options

| Option | Default | Description |
| --- | --- | --- |
| `model` | `table` | Must be `table`. |
| `table` | none | Table name. Required for writes and for multi-table reads. |
| `timeColumn` | `time` | Spark time column name. |
| `tagColumns` | none | Comma-separated TAG columns. Required for writes. |
| `fieldColumns` | inferred | Comma-separated FIELD columns for writes. |
| `timestampAs` | `long` | Use `long` or `timestamp` for TsFile `TIMESTAMP` fields and the Spark time column. |
| `timestampPrecision` | `ms` | Raw TsFile timestamp precision: `ms`, `us`, or `ns`. |
| `mergeSchema` | `false` | `true` is rejected in the initial connector. |
| `pushdown` | `true` | Enables supported time and TAG equality predicate pushdown. |
| `compression` | default TsFile setting | Compression codec for written FIELD columns. |
| `encoding` | default TsFile setting | Encoding for written FIELD columns. |
| `nullTagPolicy` | `error` | Only `error` is supported. |
| `maxRowsPerTablet` | `1024` | Maximum rows buffered in each TsFile `Tablet` before flushing. |

Table and column names are normalized to lower case to match TsFile table model
metadata behavior.

## Initial Scope

This module is the initial Spark 3.x DataSource V2 connector for TsFile table
model files. It intentionally keeps the first production surface narrow:

- Batch read and batch write are supported; streaming read/write are not.
- Writes are append-only and create new `part-*.tsfile` files. Overwrite and
  truncate semantics are not supported in this initial connector.
- Only TsFile table model is supported. Tree model files are outside this
  module's scope.
- Input discovery supports a single `.tsfile`, a directory of `.tsfile` files,
  and glob paths, but actual TsFile reading and writing is local-file only in
  this initial version. Non-`file` Hadoop paths should be handled in a follow-up
  change.
- `mergeSchema=true` is rejected. All files selected for one read must contain
  a compatible schema for the selected table.
- User-provided read schemas are validated against TsFile table metadata and
  may be used as read projections. They cannot change the stored column types.
- Predicate pushdown is limited to `time =`, `time >`, `time >=`, `time <`,
  `time <=`, AND-combined time ranges, and string equality on TAG columns.
  Unsupported predicates are returned to Spark as residual filters.
- Unsupported table categories and types fail fast: `ATTRIBUTE`, `TIME`,
  `VECTOR`, `UNKNOWN`, and `OBJECT` are not part of the first connector scope.
- TAG columns must be non-null strings. FIELD columns may be null and are
  written/read as sparse TsFile values.

Follow-up issues should track non-local filesystem support, schema merging, a
broader predicate pushdown matrix, streaming semantics, and expanded type or
category support.
