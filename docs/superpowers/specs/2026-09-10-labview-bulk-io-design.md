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

# LabVIEW Bulk I/O Modernization Design

## Summary

Modernize the LabVIEW C ABI from `Young-Leo/tsfile:labview` on top of the
latest Apache TsFile `develop` branch, without rewriting or moving the
original `labview` branch. Preserve all existing LabVIEW exports and their
row-major input layout, optimize numeric block writes internally, and add an
additive batch-query and numeric block-read API.

The work is isolated on `feat/labview-bulk-io`, created from
`origin/develop` at `2c2d416e2764460cdee8aabbeb80b1afbe639e1c`. The old
LabVIEW commits `4bf0be2` and `e5cde40` are read-only migration sources. No
remote branch is pushed or force-updated as part of this work.

## Goals

- Port the LabVIEW wrapper source, build integration, documentation, and
  source-based tests to the current TsFile C++ implementation.
- Keep every existing LabVIEW exported symbol, argument type, status-code
  convention, U64 handle, and row-major numeric input contract compatible.
- Remove per-block Tablet allocation and per-cell C-wrapper calls from the
  steady-state numeric write path.
- Add a LabVIEW-friendly batch read path that consumes TsFile `TsBlock`
  batches and fills caller-owned flat buffers in one DLL call.
- Verify file-format compatibility and result equivalence against the normal
  TsFile reader and the legacy row-oriented LabVIEW API.
- Preserve explicit benchmark coverage for write materialization, codec cost,
  row reading, batch reading, flush, and close.

## Non-goals

- Changing the existing LabVIEW ABI or switching existing calls to
  column-major buffers.
- Batch support for mixed-type or string value columns in the first version.
- Exposing Arrow structures or allocator ownership to LabVIEW.
- Making one Writer or ResultSet handle safe for concurrent use and close.
- Making `writer_close` asynchronous.
- Rebuilding or replacing the checked-in Windows x86/x64 DLLs on macOS.
- Updating, rebasing, force-pushing, or otherwise modifying
  `Young-Leo/tsfile:labview`.

## Migration Strategy

1. Fetch `ly/labview` into a read-only remote-tracking reference.
2. Keep `feat/labview-bulk-io` based on the latest `origin/develop` rather than
   merging `develop` into the old branch.
3. Port the logical changes from `4bf0be2` and `e5cde40` in reviewable commits.
   Resolve moved reader/file code against its new upstream location instead
   of restoring obsolete files.
4. Restore the existing smoke, read-only, block-write, and codec benchmark
   programs as source targets. Integrate correctness tests with CTest so they
   are not silently omitted from normal verification.
5. Do not copy old DLL or executable artifacts as if they represented the new
   source. A later Windows build may refresh distribution artifacts after the
   source implementation is stable.

The clean `origin/develop` baseline must build before migration. On the
development host, `./mvnw -P with-cpp clean verify` completed successfully
with 952 tests passed and no failures; the remaining reported tests were
upstream skipped or disabled cases.

## Compatibility Contract

Existing functions, including the following numeric block writes, retain the
same signatures and behavior:

```c
LV_Status lv_tsfile_write_block_i32(LV_Handle writer, const int64_t* ts,
                                    const int32_t* data, int32_t nrows,
                                    int32_t ncols);
LV_Status lv_tsfile_write_block_f32(LV_Handle writer, const int64_t* ts,
                                    const float* data, int32_t nrows,
                                    int32_t ncols);
LV_Status lv_tsfile_write_block_f64(LV_Handle writer, const int64_t* ts,
                                    const double* data, int32_t nrows,
                                    int32_t ncols);
```

`ts` contains `nrows` timestamps. `data` contains `nrows * ncols` homogeneous
numeric values in row-major order, so cell `(row, col)` is
`data[row * ncols + col]`. The call borrows both buffers for its duration and
does not retain LabVIEW-owned pointers.

All old query, `rs_next`, scalar getter, string getter, and free functions
remain available. New read functions are additive.

## Optimized Write Path

Each `WriterCtx` owns a reusable numeric Tablet and a reusable typed
column-major staging buffer. The first numeric block allocates these buffers.
A later block reuses them when `nrows` fits the current capacity and recreates
them only when capacity must grow.

For each block:

1. Validate the writer handle, pointers, dimensions, multiplication overflow,
   schema column count, and homogeneous value type.
2. Reset the cached Tablet without releasing its backing storage.
3. Copy all timestamps with one `set_timestamps` operation.
4. Deinterleave the caller's row-major values into the reusable column-major
   staging buffer.
5. Populate each Tablet column with one `set_column_values` operation and an
   all-valid null mask.
6. Submit the Tablet through `tsfile_writer_write`.

The C wrapper receives small additive bulk helpers for Tablet reset,
timestamp copy, and fixed-width column copy. They delegate to the existing
`Tablet::reset`, `Tablet::set_timestamps`, and `Tablet::set_column_values`
methods so the LabVIEW module does not depend on private Tablet fields.

This design intentionally accepts one deinterleave plus one contiguous copy
per value. Eliminating that copy would require changing the public LabVIEW
layout to column-major or coupling the wrapper to Tablet internals, both of
which conflict with the compatibility and layering requirements.

The cached Tablet and staging storage are released when the Writer handle is
closed. Error paths must release partially constructed storage and must not
invalidate a still-open writer unless the underlying TsFile writer reports an
unrecoverable error.

## Batch Read API

The batch query creates a batch-mode table ResultSet backed by the existing
TsFile `TsBlock` reader:

```c
LV_Status lv_tsfile_query_table_batch(
    LV_Handle reader, const char* table_name,
    const char* columns_newline_separated, int64_t start_time,
    int64_t end_time, int32_t batch_rows, LV_Handle* out_result_set);
```

`batch_rows` must be positive. The selected column list follows the existing
newline-separated convention and names value columns; the timestamp remains
column zero in returned metadata.

Three homogeneous numeric read functions are added:

```c
LV_Status lv_tsfile_rs_read_block_i32(
    LV_Handle rs, int64_t* out_ts, int32_t* out_data,
    uint8_t* out_is_null, int32_t capacity_rows, int32_t ncols,
    int32_t* out_rows);
LV_Status lv_tsfile_rs_read_block_f32(
    LV_Handle rs, int64_t* out_ts, float* out_data,
    uint8_t* out_is_null, int32_t capacity_rows, int32_t ncols,
    int32_t* out_rows);
LV_Status lv_tsfile_rs_read_block_f64(
    LV_Handle rs, int64_t* out_ts, double* out_data,
    uint8_t* out_is_null, int32_t capacity_rows, int32_t ncols,
    int32_t* out_rows);
```

The caller provides space for `capacity_rows` timestamps and
`capacity_rows * ncols` values. `out_data` is row-major and excludes the
timestamp. When `out_is_null` is non-null, it has the same row-major element
count as `out_data`; `0` means valid and `1` means null. Null numeric values
are written as zero so output is deterministic. Passing null for
`out_is_null` is allowed when the caller does not need null information.

`capacity_rows` must be at least the `batch_rows` configured by the query.
The wrapper validates this before fetching a `TsBlock`, preventing a capacity
error from consuming unread rows. On success, `out_rows` is the number of
rows copied. End of input is reported as `E_OK` with `*out_rows == 0`.

The ResultSet records whether it is row or batch mode. Scalar row functions
reject batch-mode handles with `E_INVALID_ARG`; block functions reject
row-mode handles. Value columns must all match the requested read function's
type. Mixed-type and string queries continue to use the legacy row API.

## Internal Read Path

The LabVIEW wrapper uses the existing batch query and `TsBlock` facilities,
but does not convert batches through Arrow. A small C-wrapper primitive copies
one numeric `TsBlock` into caller-owned buffers. It validates the batch mode,
timestamp type, value-column count, homogeneous type, output capacity, and
size arithmetic.

For all-valid fixed-width columns, the copy loop reads each column's
contiguous vector and writes row-major destinations. For columns containing
nulls, it walks the column bitmap/iterator, writes zero for null values, and
fills the optional byte-per-cell null array. This removes per-row host/DLL
transitions and avoids Arrow schema/array allocation on every batch.

## Configuration Correctness

Codec selection affects the measured write path. The migrated wrapper must
initialize TsFile configuration before applying LabVIEW compression or
encoding setters, and tests must prove that settings made before the first
Writer open remain effective. Benchmarks must report the effective encoding
and compression rather than assuming the requested setting took effect.

## Errors and Ownership

- Every fallible LabVIEW call returns `LV_Status`; no C++ exception may cross
  the C ABI.
- `E_INVALID_ARG` covers invalid handles, null required pointers, non-positive
  dimensions, row/batch mode mismatch, and incompatible column counts.
- Type mismatches return the existing type-mismatch status.
- Capacity and arithmetic overflow return the appropriate range/argument
  status without consuming a batch.
- Output handles and `out_rows` are initialized to zero on entry so failures
  cannot expose stale values.
- Host buffers are borrowed only for the duration of a call.
- Writer, Reader, Tablet, and ResultSet resources remain owned by their U64
  registry handles and are released by the corresponding close/free call.
- Calls that use the same handle, especially use versus close, must be
  serialized by the caller; changing registry concurrency semantics is out of
  scope.

## Testing Strategy

Implementation follows test-driven development. Each behavior is represented
by a failing test before production code is added.

Migration tests:

- Build and load the LabVIEW shim against current `develop`.
- Run the original smoke, read-only, and block-write cases.
- Read files written through the wrapper with the normal TsFile reader.
- Read an old-wrapper fixture with the migrated wrapper where a portable
  fixture is available.

Write tests:

- Verify i32, f32, and f64 row-major blocks across one and multiple calls.
- Verify reuse with smaller equal-capacity batches and growth with a larger
  batch.
- Verify exact timestamps and values after close and reopen.
- Verify invalid handles, null pointers, zero/negative sizes, multiplication
  overflow, column-count mismatch, and type mismatch.
- Instrument a test-only allocation counter or reusable-buffer identity to
  prove that steady-state same-capacity calls do not recreate the Tablet.

Read tests:

- Verify i32, f32, and f64 blocks over multiple batches and EOF.
- Compare batch output with the legacy `rs_next` and scalar getters.
- Verify empty results, a final partial batch, null cells, deterministic zero
  fill, optional null output, type mismatch, mode mismatch, insufficient
  capacity, and invalid dimensions.
- Verify that a rejected capacity call does not consume the next batch.

Repository verification:

- Run the focused LabVIEW/C-wrapper tests while iterating.
- Run formatting and license checks.
- Run `./mvnw -P with-cpp clean verify` before completion.
- Perform a Windows x64 and x86 build/test pass before publishing DLLs.

## Benchmark and Acceptance

Performance benchmarks are informational and are not timing-based CI tests.
They use fixed data, codec, row count, column count, warm-up, and repeated
median reporting. They time Tablet materialization separately from codec and
I/O, and time read decoding separately from host-buffer conversion.

The benchmark comparison includes:

- Legacy per-cell Tablet construction.
- The old `e5cde40` block-write implementation.
- The reusable/bulk block-write implementation.
- Legacy row/scalar reading.
- New batch reading.
- `flush` and `writer_close` as separate phases.

Correctness is the hard acceptance gate. On the development host, the target
is at least a 2x reduction in the 10,000-row by 9-column write materialization
phase relative to `e5cde40`, and at least a 3x improvement for 150,000 rows by
9 columns through the batch-read API relative to the legacy LabVIEW row API.
If codec or storage time dominates end-to-end results, phase timings take
precedence over total-time ratios. Any regression is investigated before the
change is accepted.

## Documentation and Delivery

The wrapper README and LabVIEW manual will document:

- Existing continuous-write usage and unchanged row-major layout.
- Recommended batch sizes and Writer lifetime.
- The new batch-query and block-read signatures.
- LabVIEW Call Library Function Node parameter mappings.
- Buffer sizing, EOF, null, ownership, and error semantics.
- Producer/consumer guidance for acquisition and file rotation.
- Codec benchmarking guidance using real device data.

The completed branch contains source, tests, benchmarks, and documentation.
Remote publication and refreshed Windows binary distributions are separate,
explicit follow-up actions.
