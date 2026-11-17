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

# C Wrapper

Use this reference only for the C wrapper around the C++ implementation. Read
`cpp/src/cwrapper/tsfile_cwrapper.h` before generating exact signatures.

## Maintained Examples

- Write: `cpp/examples/c_examples/demo_write.c`
- Read: `cpp/examples/c_examples/demo_read.c`

## Current API Families

- File/writer creation: `write_file_new`, `tsfile_writer_new`.
- Tablet creation and population: `tablet_new`, `tablet_add_timestamp`, and
  typed `tablet_add_value_by_name_*` functions.
- Write: `tsfile_writer_write`.
- Reader/query: `tsfile_reader_new`, `tsfile_query_table`.
- Runtime: `tsfile_set_parallel_read_enabled`, `tsfile_set_parallel_write_enabled`,
  `tsfile_set_thread_count` and matching getters; defaults enabled, enabled, 6.
  Counts must be `1..64`; do not resize an in-use pool (see `cpp-build-write.md`).
- Local reads: `tsfile_set_file_read_backend` / `tsfile_get_file_read_backend`;
  use the declared `TSFILE_READ_BACKEND_*` values (see `advanced-apis.md`).
- Timestamp policy: `set_allow_non_decreasing_timestamps(bool)` (no `tsfile_`
  prefix), default false; configure before creating writers (see `advanced-apis.md`).
- Cleanup: use the matching close and `free_*` functions demonstrated by the
  maintained examples.

## Guardrails

- Do not invent opaque wrapper names or infer signatures from C++ methods.
- Pair every allocation with the matching cleanup call, including error paths.
- Preserve ownership long enough for writer/tablet operations that retain
  pointers.
- Verify the wrapper and linked C++ library come from compatible builds.
- Compile and run the maintained C examples before adapting them into another
  build system.
