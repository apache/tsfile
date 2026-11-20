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

# Current Advanced APIs

Load only the section matching the task and verify exact constructors against
the listed current source.

## Row-window Queries

- C++ exposes `TsFileReader::queryByRow` for Tree paths and for Table columns,
  with `offset`, `limit`, optional Table TAG filter, and optional batch size.
  `TsFileTreeReader::queryByRow` accepts device and measurement lists.
- Python exposes `query_tree_by_row` and `query_table_by_row`; Table queries
  accept `tag_filter` and `batch_size`.
- Treat `limit < 0` as unlimited and reject negative offsets. Close or destroy
  every result set through the binding that created it.
- Table `TsFileReader::query` also has an overload combining start/end times,
  offset/limit, a TAG filter, and batch size; `queryByRow` has no time bounds.

## Timestamp Ordering

Strict order is default; equal timestamps can be preserved as separate rows.
Before writer creation, use Java config `setAllowNonDecreasingTimestamps(true)`,
C++ `common::set_allow_non_decreasing_timestamps(true)` (C: without `common::`),
or Python writer argument `allow_non_decreasing_timestamps=True` (default `False`).
This is process-wide, including Python constructor defaults; avoid conflicting
writers. Decreases remain invalid. Go exposes no timestamp-policy switch and
follows the process-wide native default. See `properties-filters.md` for the
persisted policy; CLI `write` has no opt-in.

## Timeseries Metadata

- C++ `TsFileReader::get_timeseries_metadata()` returns metadata for all
  devices; its device-list overload restricts the request.
- Python `get_timeseries_metadata(device_ids=None)` returns a mapping keyed by
  the full device-segment tuple. `None` selects all devices, while `[]` returns
  an empty map; null TAG segments remain `None`.
- C callers must use the all-devices or selected-devices declarations and the
  matching free routine in `tsfile_cwrapper.h`.

## Java TsBlock Write

Use `TableTsBlock2TsFileWriter` when the input is already a Table-model
`TsBlock`. Its constructor maps time, TAG, and FIELD column indexes and can
generate a monotonically increasing time column per device. Feed complete
blocks through `write(TsBlock)`, then close the writer. Build the mapping from
the current `TableSchema`; do not infer constructor positions from memory.

## Java Tablet Object and Size APIs

- Use `Tablet.addObjectPathValue` only for an `OBJECT` column; overloads accept
  a measurement name or column index and a `String` or `byte[]` path.
- Use `Tablet.serializedSize()` to allocate or validate the serialized Tablet
  payload. Do not replace it with a RAM-size estimate.

## Custom and Buffered Input

`TsFileSequenceReader` accepts a `TsFileInput`, enabling custom storage/input
implementations. For local buffered reads, construct `BufferedTsFileInput(Path,
bufferSize)` and pass it to the reader. Buffer size must be positive, and the
reader owns and closes the supplied input if initialization fails or when the
reader is closed.

C++ `TsFileReader::open(unique_ptr<RandomAccessReadFile>)` owns its source;
custom sources must handle short/concurrent reads. Writers borrow an open
`RandomAccessWriteFile*` through closure. For in-memory readback, move the closed
`MemoryRandomAccessWriteFile::release_data()` into `MemoryRandomAccessReadFile`.
`common::set_file_read_backend(common::FileReadBackend::MMAP)` affects new local
opens: PREAD is default, MMAP requires mapping, AUTO allows PREAD fallback.
Do not mutate mapped files; Python file objects (`python.md`) bypass this setting.

## C++ Recovery and Append

Use `storage::RestorableTsFileIOWriter` to inspect and optionally truncate an
incomplete file before continuing a write. After `open(path, true)`, require
`has_crashed()` and `can_write()` before constructing `TsFileTreeWriter` or
`TsFileTableWriter` with the restorable writer. The recovered schema is used;
do not register a replacement schema. A complete file reports
`can_write() == false`. Keep the restorable writer alive longer than the facade
writer and preserve increasing timestamps beyond the recovered last values.

## Java BitMap Ranges

Use `markRange(start, length)` and `unmarkRange(start, length)` for mutations,
and `isRangeAnyMarked`, `isRangeAllMarked`, or `isRangeNoneMarked` for range
tests. Ranges are `(start, length)`, not `(start, end)`; empty ranges return
`false`, `true`, and `true`, respectively. Out-of-bounds ranges throw.

## Source Anchors

- Row queries: `cpp/src/reader/tsfile_reader.h`,
  `cpp/src/reader/tsfile_tree_reader.h`, and `python/tsfile/tsfile_reader.pyx`
- Metadata: those readers, `cpp/src/cwrapper/tsfile_cwrapper.h`,
  `python/tsfile/schema.py`, and `python/tests/test_reader_metadata.py`
- TsBlock: `java/tsfile/src/main/java/org/apache/tsfile/write/v4/TableTsBlock2TsFileWriter.java`
- Tablet: `java/tsfile/src/main/java/org/apache/tsfile/write/record/Tablet.java`
- Input: `TsFileSequenceReader.java`, `TsFileInput.java`, and
  `BufferedTsFileInput.java` under
  `java/tsfile/src/main/java/org/apache/tsfile/read/reader/`
- Recovery: `cpp/src/file/restorable_tsfile_io_writer.h` and the current C++
  Tree/Table writer headers and recovery tests
- Timestamp/backend config: `cpp/src/common/global.h`, Java `TSFileConfig.java`,
  and the Python writer bindings
- Random access: `cpp/src/file/random_access_*_file.h` and `memory_random_access_*_file.h`
- BitMap: `java/common/src/main/java/org/apache/tsfile/utils/BitMap.java` and
  `java/tsfile/src/test/java/org/apache/tsfile/utils/BitMapTest.java`
