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

# LabVIEW Bulk I/O Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在最新 Apache TsFile `develop` 上迁移现有 LabVIEW C ABI，保持旧写入接口兼容，在内部复用 Tablet 并批量填充，同时新增基于 TsBlock 的 LabVIEW 数值块读取接口。

**Architecture:** 新分支只从 `origin/develop` 演进，旧 `ly/labview` 仅作为只读源码参考。LabVIEW Wrapper 继续暴露 U64 Handle 和扁平 row-major 数组；写入端在 `WriterCtx` 内复用 Tablet 与 column-major 暂存区，读取端使用 batch ResultSet，并通过 C Wrapper 将一个 TsBlock 复制到调用方预分配缓冲区。

**Tech Stack:** C++11、C99 C ABI、CMake、Maven CMake Plugin、GoogleTest/CTest、Python `ctypes` Benchmark、Apache TsFile table model。

**Spec:** `docs/superpowers/specs/2026-09-10-labview-bulk-io-design.zh.md`

## Global Constraints

- 所有实现只提交到 `feat/labview-bulk-io`；不修改、rebase、force-push 或推送 `ly/labview`。
- 保留所有现有 LabVIEW 导出符号、参数类型、状态码、U64 Handle 和 row-major 写入布局。
- 新读取 API 只支持同类型 i32、f32 或 f64 数值列；混合类型和字符串继续使用旧逐行 API。
- C++ 异常不得跨越 C ABI；输出 Handle 和 `out_rows` 在失败路径必须保持为零。
- LabVIEW 指针只在函数调用期间借用，不保存宿主缓冲区地址。
- 不在 macOS 上替换 `dist/` 中的 Windows DLL 或 EXE。
- 新文件必须包含 Apache License 2.0 头；提交信息不得包含 `Co-Authored-By`。
- 采用 TDD：每个行为先添加失败测试并观察预期失败，再编写最小实现。

---

### Task 1: 将旧 LabVIEW Shim 迁移到最新 develop

**Files:**
- Modify: `cpp/src/CMakeLists.txt`
- Create: `cpp/src/labview_wrapper/CMakeLists.txt`
- Create: `cpp/src/labview_wrapper/tsfile_labview.h`
- Create: `cpp/src/labview_wrapper/tsfile_labview.cc`
- Create: `cpp/src/labview_wrapper/test_smoke.c`
- Create: `cpp/src/labview_wrapper/test_read_only.c`
- Create: `cpp/src/labview_wrapper/test_dump.c`
- Create: `cpp/src/labview_wrapper/test_block.c`
- Create: `cpp/src/labview_wrapper/README.md`
- Modify: `cpp/test/CMakeLists.txt`

**Interfaces:**
- Consumes: `ly/labview` commits `4bf0be2` and `e5cde40` as read-only source material; current `cpp/src/cwrapper/tsfile_cwrapper.h`.
- Produces: `tsfile_labview` shared-library target and all pre-existing `lv_tsfile_*` exports, including `lv_tsfile_write_block_i32/f32/f64`.

- [ ] **Step 1: Fetch the old branch without creating or moving a local branch**

```bash
git fetch ly labview:refs/remotes/ly/labview
git rev-parse refs/remotes/ly/labview
git rev-parse feat/labview-bulk-io
```

Expected: the first command resolves to `e5cde40b7061dc874586c6a0aebacae9e88bbc83`; `feat/labview-bulk-io` remains based on current `origin/develop` plus design commits.

- [ ] **Step 2: Add the old public header, CMake test declarations, and legacy tests before the implementation**

Build the correctness executables in the wrapper subdirectory, then register
them from `cpp/test/CMakeLists.txt`, where `enable_testing()` is active:

```cmake
if(TARGET tsfile_labview_smoke_test)
  add_test(NAME LabVIEWSmokeTest
           COMMAND $<TARGET_FILE:tsfile_labview_smoke_test>)
  # Register the read-only, dump, and block targets in the same way.
endif()
```

- [ ] **Step 3: Configure/build and verify the migration tests fail for the missing implementation**

```bash
./mvnw -P with-cpp -DskipTests package
```

Expected: link or compile failure referencing missing `lv_tsfile_*` implementation, proving the migrated tests exercise the new target.

- [ ] **Step 4: Port the legacy implementation against current C Wrapper APIs**

Port the U64 registry, Schema Builder, Writer, Tablet, Reader, row ResultSet, convenience writer, and CSV dump logic from `ly/labview`. Keep this legacy block behavior for the migration checkpoint:

```cpp
template <typename T>
LV_Status write_block(/* existing signature */) {
    // Temporary Tablet and per-cell calls are intentionally retained only
    // until Task 3 establishes the optimized behavior with tests.
}
```

Do not port the old changes to `chunk_reader.cc`, `tsfile_series_scan_iterator.h`, `read_file.cc`, or `util_define.h` when the equivalent fixes already exist in current `develop`. Resolve includes and renamed files using the current tree.

- [ ] **Step 5: Build and run the four focused tests**

```bash
./mvnw -P with-cpp -DskipTests package
ctest --test-dir cpp/target/build/test -R '^LabVIEW(Smoke|ReadOnly|Dump|Block)Test$' --output-on-failure
```

Expected: four LabVIEW tests pass.

- [ ] **Step 6: Commit the migration checkpoint**

```bash
git add cpp/src/CMakeLists.txt cpp/src/labview_wrapper cpp/test/CMakeLists.txt
git commit -m "feat(cpp): port LabVIEW C shim to current develop"
```

---

### Task 2: 修复首次 Writer 打开前的 Codec 配置顺序

**Files:**
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.cc`
- Create: `cpp/src/labview_wrapper/test_config.c`
- Modify: `cpp/src/labview_wrapper/CMakeLists.txt`
- Modify: `cpp/test/CMakeLists.txt`

**Interfaces:**
- Consumes: existing `init_tsfile_config`, `set_datatype_encoding`, `set_global_compression`, and getters.
- Produces: setters that initialize default configuration first and then apply the requested value.

- [ ] **Step 1: Add a failing fresh-process configuration-order test**

Add a standalone CTest executable so no earlier unit test can initialize the
process first. It calls the LabVIEW setters before Writer creation, checks the
C-wrapper getters, opens/closes one Writer, and checks the same getters again:

```c
CHECK_OK(lv_tsfile_set_datatype_encoding(LV_TYPE_DOUBLE,
                                         TS_ENCODING_PLAIN));
CHECK_OK(lv_tsfile_set_global_compression(TS_COMPRESSION_LZ4));
CHECK(get_datatype_encoding(TS_DATATYPE_DOUBLE) == TS_ENCODING_PLAIN);
CHECK(get_global_compression() == TS_COMPRESSION_LZ4);
// Open and close one Writer, then repeat both getter checks.
```

Register it as `LabVIEWConfigTest` from `cpp/test/CMakeLists.txt`.

- [ ] **Step 2: Run the focused test and verify RED**

```bash
ctest --test-dir cpp/target/build/test -R '^LabVIEWConfigTest$' --output-on-failure
```

Expected: the post-open assertion observes the default encoding/compression instead of the requested values.

- [ ] **Step 3: Initialize before applying all public configuration setters**

```cpp
int set_datatype_encoding(uint8_t data_type, uint8_t encoding) {
    init_tsfile_config();
    return common::set_datatype_encoding(data_type, encoding);
}

int set_global_compression(uint8_t compression) {
    init_tsfile_config();
    return common::set_global_compression(compression);
}
```

Apply the same order to global time encoding/compression setters for consistent public behavior.

- [ ] **Step 4: Rebuild and verify GREEN**

```bash
cmake --build cpp/target/build --target tsfile_labview_config_test -j 8
ctest --test-dir cpp/target/build/test -R '^LabVIEWConfigTest$' --output-on-failure
```

Expected: all configuration tests pass.

- [ ] **Step 5: Commit**

```bash
git add cpp/src/cwrapper/tsfile_cwrapper.cc \
  cpp/src/labview_wrapper/test_config.c \
  cpp/src/labview_wrapper/CMakeLists.txt cpp/test/CMakeLists.txt
git commit -m "fix(cpp): preserve pre-open codec configuration"
```

---

### Task 3: 增加 Tablet 批量 C API 并复用写入缓冲区

**Files:**
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.h`
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.cc`
- Modify: `cpp/src/labview_wrapper/tsfile_labview.cc`
- Test: `cpp/test/cwrapper/cwrapper_test.cc`
- Test: `cpp/src/labview_wrapper/test_block.c`
- Modify: `cpp/src/labview_wrapper/benchmark_block.py`

**Interfaces:**
- Consumes: `Tablet::reset`, `Tablet::set_timestamps`, and `Tablet::set_column_values`.
- Produces:

```c
ERRNO tablet_reset(Tablet tablet, uint32_t row_count);
ERRNO tablet_set_timestamps(Tablet tablet, const int64_t* timestamps,
                            uint32_t count);
ERRNO tablet_set_column_values(Tablet tablet, uint32_t column_index,
                               const void* values, const uint8_t* null_bitmap,
                               uint32_t count);
```

- [ ] **Step 1: Add failing C-wrapper bulk Tablet tests**

```cpp
TEST(CWrapperTabletBulkTest, CopiesTimestampsValuesAndNullBitmap) {
    const int64_t ts[] = {10, 20, 30};
    const double values[] = {1.5, 2.5, 3.5};
    const uint8_t nulls[] = {0x02};
    ASSERT_EQ(RET_OK, tablet_reset(tablet, 0));
    ASSERT_EQ(RET_OK, tablet_set_timestamps(tablet, ts, 3));
    ASSERT_EQ(RET_OK,
              tablet_set_column_values(tablet, 0, values, nulls, 3));
    // Assert rows 0 and 2 values and row 1 null through existing getters.
}
```

Add invalid-handle, null-pointer, count-over-capacity, and column-index cases.

- [ ] **Step 2: Build and verify RED**

```bash
cmake --build cpp/target/build --target TsFile_Test -j 8
```

Expected: compilation fails because the three bulk C functions do not exist.

- [ ] **Step 3: Implement the minimal bulk C wrappers**

```cpp
ERRNO tablet_set_timestamps(Tablet tablet, const int64_t* timestamps,
                            uint32_t count) {
    if (tablet == nullptr || (timestamps == nullptr && count != 0)) {
        return common::E_INVALID_ARG;
    }
    return static_cast<storage::Tablet*>(tablet)->set_timestamps(timestamps,
                                                                 count);
}
```

Implement equivalent validation/delegation for reset and fixed-width values.

- [ ] **Step 4: Add a failing multi-call LabVIEW write test**

Write three f64 blocks with row counts `1024`, `256`, and `2048`, close the file, read it with the standard C Wrapper, and assert every timestamp/value. This forces reuse, reuse with a smaller batch, and capacity growth.

```c
CHECK_OK(lv_tsfile_write_block_f64(writer, ts_a, data_a, 1024, 9));
CHECK_OK(lv_tsfile_write_block_f64(writer, ts_b, data_b, 256, 9));
CHECK_OK(lv_tsfile_write_block_f64(writer, ts_c, data_c, 2048, 9));
```

Expected before implementation: correctness passes. Capture an allocation
profile of this test/benchmark as the legacy comparison; the optimized version
must show no new Tablet allocation for the smaller second block and only one
capacity-growth event for the third block.

- [ ] **Step 5: Replace per-cell write construction with reusable buffers**

Extend `WriterCtx`:

```cpp
struct WriterCtx {
    WriteFile wf = nullptr;
    TsFileWriter writer = nullptr;
    std::vector<std::string> col_names;
    std::vector<TSDataType> col_types;
    Tablet block_tablet = nullptr;
    uint32_t block_capacity = 0;
    std::vector<uint8_t> column_major_scratch;
};
```

For each typed call, ensure capacity, reset the Tablet, bulk-copy timestamps,
deinterleave with the exact mapping below, then bulk-copy each column:

```cpp
column_major[col * rows + row] = data[row * cols + col];
```

Release `block_tablet` from Writer close/error paths. Never retain `ts` or
`data`.

- [ ] **Step 6: Verify functionality and measure the write materialization phase**

```bash
cmake --build cpp/target/build --target tsfile_labview_block_test -j 8
ctest --test-dir cpp/target/build/test -R '^LabVIEWBlockTest$' --output-on-failure
python3 cpp/src/labview_wrapper/benchmark_block.py --rows 10000 --cols 9 --batches 30
```

Expected: correctness passes; steady-state same-capacity calls reuse storage;
the materialization median is at least 2x faster than the recorded `e5cde40`
baseline on the same host.

- [ ] **Step 7: Commit**

```bash
git add cpp/src/cwrapper/tsfile_cwrapper.h cpp/src/cwrapper/tsfile_cwrapper.cc \
  cpp/src/labview_wrapper/tsfile_labview.cc \
  cpp/src/labview_wrapper/test_block.c \
  cpp/src/labview_wrapper/benchmark_block.py cpp/test/cwrapper/cwrapper_test.cc
git commit -m "perf(cpp): reuse LabVIEW numeric write buffers"
```

---

### Task 4: 为 Batch ResultSet 增加数值 TsBlock 复制能力

**Files:**
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.h`
- Modify: `cpp/src/cwrapper/tsfile_cwrapper.cc`
- Create: `cpp/test/cwrapper/cwrapper_numeric_block_test.cc`

**Interfaces:**
- Consumes: `tsfile_query_table_batch`, `TableResultSet::get_next_tsblock`, `TsBlock`, and `ColIterator`.
- Produces:

```c
ERRNO tsfile_result_set_read_numeric_block(
    ResultSet result_set, TSDataType expected_type,
    int64_t* out_timestamps, void* out_values, uint8_t* out_is_null,
    uint32_t capacity_rows, uint32_t value_column_count,
    uint32_t* out_rows);
```

- [ ] **Step 1: Add a failing multi-batch numeric-copy test**

Create a table with two DOUBLE fields, seven rows, and nulls in each field.
Query with batch size three, then assert blocks of `3`, `3`, `1`, and `0`
rows. Assert row-major values and byte-per-cell null flags:

```cpp
ASSERT_EQ(common::E_OK, tsfile_result_set_read_numeric_block(
    rs, TS_DATATYPE_DOUBLE, ts, values, nulls, 3, 2, &rows));
ASSERT_EQ(3u, rows);
EXPECT_DOUBLE_EQ(values[1 * 2 + 0], 0.0);  // null is deterministic zero
EXPECT_EQ(1u, nulls[1 * 2 + 0]);
```

Add tests for row-mode ResultSet, wrong type, wrong column count, null required
outputs, and a final EOF call.

- [ ] **Step 2: Verify RED**

The existing recursive `cwrapper/*_test.cc` glob includes this source.

```bash
cmake --build cpp/target/build --target TsFile_Test -j 8
```

Expected: compilation fails because `tsfile_result_set_read_numeric_block`
is undefined.

- [ ] **Step 3: Implement typed TsBlock copying without Arrow allocation**

Use a template for i32/f32/f64. Initialize `*out_rows = 0`, dynamic-cast to
`TableResultSet`, fetch one block, validate timestamp/value schema, and copy:

```cpp
for (uint32_t col = 0; col < value_column_count; ++col) {
    common::ColIterator it(col + 1, block);
    for (uint32_t row = 0; row < row_count; ++row) {
        bool is_null = false;
        uint32_t len = 0;
        const char* src = it.read(&len, &is_null);
        T value = T();
        if (!is_null) std::memcpy(&value, src, sizeof(T));
        out[row * value_column_count + col] = value;
        if (out_is_null != nullptr) {
            out_is_null[row * value_column_count + col] = is_null ? 1 : 0;
        }
        it.next();
    }
}
```

Return `E_OK` with zero rows for `E_NO_MORE_DATA`. Ensure the timestamp column
is copied as int64 and non-null.

- [ ] **Step 4: Run focused tests and verify GREEN**

```bash
cmake --build cpp/target/build --target TsFile_Test -j 8
cpp/target/build/test/lib/TsFile_Test --gtest_filter=CWrapperNumericBlockTest.*
```

Expected: all numeric block tests pass.

- [ ] **Step 5: Commit**

```bash
git add cpp/src/cwrapper/tsfile_cwrapper.h cpp/src/cwrapper/tsfile_cwrapper.cc \
  cpp/test/cwrapper/cwrapper_numeric_block_test.cc
git commit -m "feat(cpp): copy numeric TsBlocks through C API"
```

---

### Task 5: 增加 LabVIEW Batch Query 和 Block Read API

**Files:**
- Modify: `cpp/src/labview_wrapper/tsfile_labview.h`
- Modify: `cpp/src/labview_wrapper/tsfile_labview.cc`
- Create: `cpp/src/labview_wrapper/test_block_read.c`
- Modify: `cpp/src/labview_wrapper/CMakeLists.txt`

**Interfaces:**
- Consumes: `tsfile_query_table_batch` and `tsfile_result_set_read_numeric_block`.
- Produces: `lv_tsfile_query_table_batch` and `lv_tsfile_rs_read_block_i32/f32/f64` with signatures fixed by the design spec.

- [ ] **Step 1: Add the public declarations and failing C test**

The test writes 150,005 rows with nine f64 columns, queries with
`batch_rows=10000`, allocates capacity for 10000 rows, and verifies 15 full
batches, one five-row batch, then EOF:

```c
CHECK_OK(lv_tsfile_query_table_batch(reader, "signals", columns, 0,
                                     INT64_MAX, 10000, &rs));
CHECK_OK(lv_tsfile_rs_read_block_f64(rs, ts, data, nulls,
                                     10000, 9, &rows));
CHECK(rows == 10000);
```

Add separate i32/f32 cases plus capacity, type, row-mode, batch-mode scalar,
empty-result, optional-null-buffer, and invalid-pointer cases.

- [ ] **Step 2: Build and verify RED**

```bash
cmake --build cpp/target/build --target tsfile_labview_block_read_test -j 8
```

Expected: link failure for the new LabVIEW exports.

- [ ] **Step 3: Extend ResultSetCtx with mode and configured capacity**

```cpp
enum class ResultMode { kRow, kBatch };

struct ResultSetCtx {
    ResultSet rs = nullptr;
    std::vector<TSDataType> col_types;
    ResultMode mode = ResultMode::kRow;
    uint32_t batch_rows = 0;
};
```

The old query creates `kRow`; the new query creates `kBatch`. Validate
`capacity_rows >= ctx->batch_rows` before calling the C-wrapper block copy so
an insufficient buffer never consumes a TsBlock.

- [ ] **Step 4: Implement the three typed functions through one template**

```cpp
template <typename T>
LV_Status read_numeric_block(LV_Handle rs, TSDataType expected_type,
                             int64_t* out_ts, T* out_data,
                             uint8_t* out_is_null, int32_t capacity_rows,
                             int32_t ncols, int32_t* out_rows);
```

Initialize `*out_rows = 0`, validate all dimensions/overflow and homogeneous
metadata, call the C-wrapper primitive, and convert the returned uint32 row
count only after checking it fits int32.

- [ ] **Step 5: Run the new test and all legacy LabVIEW tests**

```bash
cmake --build cpp/target/build --target tsfile_labview_block_read_test -j 8
ctest --test-dir cpp/target/build/test -R '^LabVIEW.*Test$' --output-on-failure
```

Expected: new batch tests and every legacy test pass.

- [ ] **Step 6: Commit**

```bash
git add cpp/src/labview_wrapper/tsfile_labview.h \
  cpp/src/labview_wrapper/tsfile_labview.cc \
  cpp/src/labview_wrapper/test_block_read.c \
  cpp/src/labview_wrapper/CMakeLists.txt
git commit -m "feat(cpp): add LabVIEW numeric block reads"
```

---

### Task 6: 更新 Benchmark 与 LabVIEW 文档

**Files:**
- Modify: `cpp/src/labview_wrapper/benchmark_block.py`
- Modify: `cpp/src/labview_wrapper/benchmark_codec_matrix.c`
- Create: `cpp/src/labview_wrapper/benchmark_read_block.py`
- Modify: `cpp/src/labview_wrapper/README.md`
- Create: `cpp/src/labview_wrapper/LabVIEW读写TsFile操作手册.md`

**Interfaces:**
- Consumes: all migrated and optimized LabVIEW APIs.
- Produces: repeatable phase benchmarks and complete LabVIEW CLFN usage documentation.

- [ ] **Step 1: Add benchmark smoke checks that fail if required phases are missing**

The write benchmark must emit `materialize_seconds`, `writer_seconds`,
`flush_seconds`, and `close_seconds`. The read benchmark must emit
`legacy_row_seconds`, `batch_decode_seconds`, `host_copy_seconds`, and total
rows/values. Add a `--smoke` mode using 100 rows and two batches that verifies
all output counters are positive and both read paths produce identical data.

```bash
python3 cpp/src/labview_wrapper/benchmark_block.py --smoke
python3 cpp/src/labview_wrapper/benchmark_read_block.py --smoke
```

Expected before the benchmark update: missing option/script or missing phase
keys.

- [ ] **Step 2: Implement deterministic benchmark inputs and median reporting**

Use fixed sine-like and seeded random inputs, at least one warm-up, and three
measured repetitions. Print effective codec values through existing getters.
Do not include Python array construction in DLL timings.

- [ ] **Step 3: Document exact LabVIEW CLFN mappings**

Document U64 handles, signed 32-bit status and dimensions, Array Data Pointer
arguments, row-major indexing, `capacity_rows >= batch_rows`, optional null
array, `E_OK + out_rows=0` EOF, ownership, Writer lifetime, recommended
1000-10000 row batches, Producer/Consumer rotation, and real-data codec
comparison.

- [ ] **Step 4: Run benchmark smoke checks and record directional results**

```bash
python3 cpp/src/labview_wrapper/benchmark_block.py --smoke
python3 cpp/src/labview_wrapper/benchmark_read_block.py --smoke
python3 cpp/src/labview_wrapper/benchmark_block.py --rows 10000 --cols 9 --batches 30
python3 cpp/src/labview_wrapper/benchmark_read_block.py --rows 150000 --cols 9 --batch-rows 10000
```

Expected: smoke checks pass, read results match, and phase medians are printed.

- [ ] **Step 5: Commit**

```bash
git add cpp/src/labview_wrapper/benchmark_block.py \
  cpp/src/labview_wrapper/benchmark_codec_matrix.c \
  cpp/src/labview_wrapper/benchmark_read_block.py \
  cpp/src/labview_wrapper/README.md \
  cpp/src/labview_wrapper/LabVIEW读写TsFile操作手册.md
git commit -m "docs(cpp): document and benchmark LabVIEW bulk I/O"
```

---

### Task 7: 完整验证与交付检查

**Files:**
- Modify only if verification reveals a scoped defect in files changed by Tasks 1-6.

**Interfaces:**
- Consumes: complete `feat/labview-bulk-io` implementation.
- Produces: a clean, locally verified branch ready for review; no remote push and no Windows binary claim.

- [ ] **Step 1: Run formatting**

```bash
./mvnw -P with-cpp spotless:apply
git diff --check
```

- [ ] **Step 2: Run focused LabVIEW and C-wrapper tests**

```bash
cmake --build cpp/target/build --target TsFile_Test -j 8
cpp/target/build/test/lib/TsFile_Test --gtest_filter='CWrapperConfigTest.*:CWrapperTabletBulkTest.*:CWrapperNumericBlockTest.*'
ctest --test-dir cpp/target/build/test -R '^LabVIEW.*Test$' --output-on-failure
```

Expected: all focused tests pass.

- [ ] **Step 3: Run the full C++ verification from a clean build**

```bash
./mvnw -P with-cpp clean verify
```

Expected: build success, no test failures, no Spotless or RAT violations.

- [ ] **Step 4: Verify branch and artifact boundaries**

```bash
git status --short
git log --oneline origin/develop..HEAD
git diff --name-only origin/develop..HEAD -- dist
git rev-parse refs/remotes/ly/labview
```

Expected: worktree is clean; `dist/` has no modified Windows binaries; the
remote-tracking `ly/labview` still resolves to
`e5cde40b7061dc874586c6a0aebacae9e88bbc83`.

- [ ] **Step 5: Report results without pushing**

Report the new branch name, commits, full/focused test counts, write/read
benchmark medians, known macOS-only validation limitation, and the separate
Windows x86/x64 build follow-up. Do not push or create a PR without explicit
authorization.
