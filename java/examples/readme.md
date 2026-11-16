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

# Function
```
The examples show how to write and read a TsFile file.
```
# Usage
## Dependencies with Maven

```
<dependencies>
    <dependency>
        <groupId>org.apache.tsfile</groupId>
        <artifactId>tsfile</artifactId>
        <version>2.5.0</version>
    </dependency>
</dependencies>
```

## Write examples

All writers below live in `src/main/java/org/apache/tsfile/` and can be run from
an IDE or with `java -cp <classpath> org.apache.tsfile.<ClassName>`.

| Class | Write model | API | Output file |
|---|---|---|---|
| `TsFileWriteWithTSRecord` | Non-aligned (tree model) | `TsFileWriter.registerTimeseries` + `TSRecord` | `Record.tsfile` |
| `TsFileWriteWithTablet` | Non-aligned (tree model) | `TsFileWriter.registerTimeseries` + `Tablet` | `Tablet.tsfile` |
| `TsFileWriteAlignedWithTSRecord` | Aligned | `TsFileWriter.registerAlignedTimeseries` + `TSRecord` | `alignedRecord.tsfile` |
| `TsFileWriteAlignedWithTablet` | Aligned and non-aligned in one file | `registerAlignedTimeseries` + `registerTimeseries` + `Tablet` | `alignedTablet.tsfile` |
| `TsFileForceAppendWrite` | Non-aligned (tree model) | `TsFileWriter.writeRecord` in batches | `test.tsfile` |

`TSRecord` is the row-oriented API: one timestamp with a list of data points.
`Tablet` is the column-oriented batch API and is preferred for volume writes.
The aligned variants require every measurement of a device to share the same
timestamp column.

## Read examples

### Run TsFileRead.java

`TsFileRead` queries `test.tsfile`, so run `TsFileForceAppendWrite` first to
generate it. The example shows a plain query, a time-range filter
(`GlobalTimeExpression` with `TimeFilterApi`), a value filter
(`SingleSeriesExpression` with `ValueFilterApi`), and a combined filter.

### Run TsFileSequenceRead.java

`TsFileSequenceRead` scans a TsFile from head to tail and prints its physical
layout: magic strings, metadata positions, chunk groups, chunks, and pages. Pass
the file to scan as the first argument; it defaults to `test.tsfile`.

## V4 examples

The `v4/` package contains examples for the newer reader/writer APIs:
`ITsFileReaderAndITsFileWriter`, `WriteTabletWithITsFileWriter`,
`TsFileTreeReaderExample`, and `TagFilterExample`.

### Notice
  For detail, please refer to https://github.com/apache/tsfile/blob/develop/README.md.
