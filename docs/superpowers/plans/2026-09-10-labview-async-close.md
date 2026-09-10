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

# LabVIEW Single-Threaded Async Close Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional two-phase LabVIEW Writer close that uses at most one process-wide background close thread and always waits for the previous background close before starting another.

**Architecture:** A focused internal `AsyncCloseCoordinator` owns one `std::thread` and serializes submissions. The public C shim unregisters the Writer immediately, transfers its context into a close action, returns a Close Task Handle for asynchronous mode, and preserves the existing synchronous API unchanged.

**Tech Stack:** C++11 threads, mutexes and condition variables; C99 LabVIEW ABI tests; CMake/CTest; Python `ctypes` benchmark.

**Spec:** `docs/superpowers/specs/2026-09-10-labview-async-close-design.zh.md`

## Global Constraints

- Work only on `feat/labview-bulk-io`; do not move or rewrite `ly/labview`.
- Keep `lv_tsfile_writer_close(LV_Handle)` source and ABI compatible.
- Accept only `async_close` values `0` and `1`.
- Run at most one process-wide background close thread.
- A new asynchronous submission waits indefinitely for the previous close thread.
- Do not add timeout, polling, cancellation, write queue, asynchronous write, or asynchronous flush behavior.
- The asynchronous call borrows no LabVIEW-owned array or string after it returns.
- Every Close Task Handle must be consumed exactly once by its blocking wait API.
- Convert every C++ exception at the C ABI boundary to an existing `LV_Status`.
- Add the Apache License 2.0 header to every new source file.

---

### Task 1: Deterministic Single-Thread Close Coordinator

**Files:**
- Create: `cpp/src/labview_wrapper/async_close.h`
- Create: `cpp/src/labview_wrapper/async_close.cc`
- Create: `cpp/src/labview_wrapper/test_async_close.cc`
- Modify: `cpp/src/labview_wrapper/CMakeLists.txt`
- Modify: `cpp/test/CMakeLists.txt`

**Interfaces:**
- Consumes: `LV_Status` from `tsfile_labview.h` and `RET_OK`/`RET_OOM` status constants from the existing C wrapper.
- Produces: `labview::AsyncCloseTask::Wait()` and `labview::AsyncCloseCoordinator::Submit(CloseAction, std::shared_ptr<AsyncCloseTask>*)` for Task 2.

- [x] **Step 1: Write the failing coordinator test**

Create a standalone C++ test that uses promises/condition variables rather than timing thresholds. Its essential cases are:

```cpp
labview::AsyncCloseCoordinator coordinator;
std::shared_ptr<labview::AsyncCloseTask> first;
std::shared_ptr<labview::AsyncCloseTask> second;
std::atomic<int> running(0);
std::atomic<int> max_running(0);
std::promise<void> first_started;
std::promise<void> release_first;
std::shared_future<void> release = release_first.get_future().share();
auto track_max = [&] {
    int now = ++running;
    int observed = max_running.load();
    while (observed < now &&
           !max_running.compare_exchange_weak(observed, now)) {
    }
};

CHECK(coordinator.Submit(
          [&] {
              track_max();
              first_started.set_value();
              release.wait();
              --running;
              return 11;
          },
          &first) == 0);
first_started.get_future().wait();

std::promise<void> second_submit_started;
std::promise<void> second_submit_done;
std::future<void> second_done = second_submit_done.get_future();
LV_Status second_submit_status = -1;
std::thread submitter([&] {
    second_submit_started.set_value();
    second_submit_status = coordinator.Submit(
        [&] {
            track_max();
            --running;
            return 22;
        },
        &second);
    second_submit_done.set_value();
});

second_submit_started.get_future().wait();
CHECK(second_done.wait_for(std::chrono::milliseconds(20)) ==
      std::future_status::timeout);
release_first.set_value();
submitter.join();
CHECK(second_submit_status == 0);
CHECK(first->Wait() == 11);
CHECK(second->Wait() == 22);
CHECK(max_running.load() == 1);
```

Also verify that destroying a coordinator waits for its last blocked action before the destructor returns.

- [x] **Step 2: Register and run the test to verify it fails**

Add `tsfile_labview_async_close_coordinator_test` to the LabVIEW CMake file and register `LabVIEWAsyncCloseCoordinatorTest` with CTest.

Run:

```bash
cmake --build cpp/target/build --target \
  tsfile_labview_async_close_coordinator_test -j 8
```

Expected: build fails because `async_close.h` and its types do not exist.

- [x] **Step 3: Implement the minimal coordinator**

Declare the focused internal interface:

```cpp
namespace labview {

class AsyncCloseTask {
   public:
    LV_Status Wait();

   private:
    friend class AsyncCloseCoordinator;
    void Complete(LV_Status status);
    std::mutex mutex_;
    std::condition_variable completed_;
    bool done_ = false;
    LV_Status status_ = 0;
};

class AsyncCloseCoordinator {
   public:
    using CloseAction = std::function<LV_Status()>;
    ~AsyncCloseCoordinator();
    LV_Status Submit(CloseAction action,
                     std::shared_ptr<AsyncCloseTask>* out_task);

   private:
    void JoinWorker();
    std::mutex submission_mutex_;
    std::thread worker_;
};

}  // namespace labview
```

`Submit` must hold `submission_mutex_` across joining the old thread and creating the new thread. The worker catches all exceptions, completes the task exactly once, and owns the close action by value. If task allocation or thread construction fails, invoke the action synchronously, leave `out_task` empty, and return its final status. The destructor obtains submission ownership and joins `worker_` if it is joinable.

- [x] **Step 4: Run the coordinator test to verify it passes**

Run:

```bash
cmake --build cpp/target/build --target \
  tsfile_labview_async_close_coordinator_test -j 8
ctest --test-dir cpp/target/build/test \
  -R '^LabVIEWAsyncCloseCoordinatorTest$' --output-on-failure
```

Expected: one test passes, including deterministic serialization and destructor join cases.

- [x] **Step 5: Commit the coordinator**

```bash
git add cpp/src/labview_wrapper/async_close.h \
  cpp/src/labview_wrapper/async_close.cc \
  cpp/src/labview_wrapper/test_async_close.cc \
  cpp/src/labview_wrapper/CMakeLists.txt cpp/test/CMakeLists.txt
git commit -m "feat(cpp): add single-thread async close coordinator"
```

### Task 2: Two-Phase LabVIEW Close C ABI

**Files:**
- Modify: `cpp/src/labview_wrapper/tsfile_labview.h`
- Modify: `cpp/src/labview_wrapper/tsfile_labview.cc`
- Create: `cpp/src/labview_wrapper/test_async_close.c`
- Modify: `cpp/src/labview_wrapper/CMakeLists.txt`
- Modify: `cpp/test/CMakeLists.txt`

**Interfaces:**
- Consumes: `labview::AsyncCloseCoordinator` and `labview::AsyncCloseTask` from Task 1; existing `WriterCtx`, Handle Registry, and synchronous close resources.
- Produces: `lv_tsfile_writer_close_ex(LV_Handle, int32_t, LV_Handle*)` and `lv_tsfile_close_task_wait(LV_Handle)`.

- [x] **Step 1: Write the failing C ABI test**

Create `test_async_close.c` with isolated file names and these assertions:

```c
LV_Handle task = 123;
CHECK_REJECTED(lv_tsfile_writer_close_ex(0, 1, &task));
CHECK(task == 0);
CHECK_REJECTED(lv_tsfile_writer_close_ex(writer, 2, &task));

CHECK_OK(lv_tsfile_writer_close_ex(writer, 1, &task));
CHECK(task != 0);
CHECK_REJECTED(lv_tsfile_writer_flush(writer));
CHECK_OK(lv_tsfile_close_task_wait(task));
CHECK_REJECTED(lv_tsfile_close_task_wait(task));
CHECK_OK(verify_written_file(path));
```

Open a second isolated Writer and verify `close_ex(writer, 0, &task)` returns with `task == 0` and produces a readable file. Keep the old `lv_tsfile_writer_close` test coverage intact.

- [x] **Step 2: Register and run the ABI test to verify it fails**

Add `async_close` to the LabVIEW standalone test targets and add the CTest name `LabVIEWAsyncCloseTest`.

Run:

```bash
cmake --build cpp/target/build --target tsfile_labview_async_close_test -j 8
```

Expected: compile or link fails because the two new C functions are absent.

- [x] **Step 3: Add the public declarations and close-task Handle type**

Add to `tsfile_labview.h`:

```c
LV_API LV_Status lv_tsfile_writer_close_ex(LV_Handle writer,
                                            int32_t async_close,
                                            LV_Handle* out_close_task);
LV_API LV_Status lv_tsfile_close_task_wait(LV_Handle close_task);
```

Extend the Registry kind with `kCloseTask`. Store each task as a heap-allocated `std::shared_ptr<labview::AsyncCloseTask>` so the Registry owns the caller's task reference while the worker owns its own reference.

- [x] **Step 4: Centralize Writer resource destruction**

Extract the existing close sequence into one no-throw helper:

```cpp
LV_Status close_writer(WriterCtx* ctx) noexcept {
    if (ctx == nullptr) return E_INVALID_ARG;
    LV_Status status = E_OK;
    try {
        if (ctx->writer != nullptr) status = tsfile_writer_close(ctx->writer);
    } catch (...) {
        status = RET_FILE_CLOSE_ERR;
    }
    if (ctx->wf != nullptr) free_write_file(&ctx->wf);
    if (ctx->block_tablet != nullptr) free_tablet(&ctx->block_tablet);
    delete ctx;
    return status;
}
```

Preserve the first close failure while always releasing the `WriteFile`, cached Tablet, scratch vectors, and context.

- [x] **Step 5: Implement synchronous and asynchronous close paths**

Use a function-local process-wide coordinator whose destructor joins the last worker. `close_ex` validates and zeroes `out_close_task`, unregisters the Writer once, and then either calls `close_writer` directly or submits `[ctx] { return close_writer(ctx); }`.

On successful async submission, allocate/register the task wrapper and return `E_OK`. If registering the task wrapper fails after the worker started, wait for the task and return its close result so no task or Writer resource is orphaned. `lv_tsfile_close_task_wait` unregisters the task Handle before blocking, waits indefinitely, returns the task's final status, and deletes the heap wrapper. Make `lv_tsfile_writer_close` delegate to the synchronous path.

- [x] **Step 6: Run focused tests**

Run:

```bash
cmake --build cpp/target/build --target \
  tsfile_labview_async_close_test tsfile_labview_block_test -j 8
ctest --test-dir cpp/target/build/test \
  -R '^LabVIEW(AsyncClose|AsyncCloseCoordinator|Block)Test$' \
  --output-on-failure
```

Expected: all three focused tests pass.

- [x] **Step 7: Commit the public async close API**

```bash
git add cpp/src/labview_wrapper/tsfile_labview.h \
  cpp/src/labview_wrapper/tsfile_labview.cc \
  cpp/src/labview_wrapper/test_async_close.c \
  cpp/src/labview_wrapper/CMakeLists.txt cpp/test/CMakeLists.txt
git commit -m "feat(cpp): add two-phase LabVIEW writer close"
```

### Task 3: LabVIEW Guidance and Async Close Benchmark

**Files:**
- Modify: `cpp/src/labview_wrapper/README.md`
- Modify: `cpp/src/labview_wrapper/LabVIEW读写TsFile操作手册.md`
- Modify: `cpp/src/labview_wrapper/benchmark_block.py`

**Interfaces:**
- Consumes: the two C ABI functions from Task 2.
- Produces: documented LabVIEW CLFN mappings and an `--async-close` benchmark mode reporting submission and wait separately.

- [x] **Step 1: Extend the benchmark smoke contract before implementation**

Add command-line parsing and output assertions so this invocation requires the new metrics:

```bash
python3 cpp/src/labview_wrapper/benchmark_block.py --smoke --async-close \
  --output-dir /private/tmp/tsfile-labview-async-close-smoke
```

Expected before binding the API: fail because `--async-close` is unknown or the required symbols/metrics are absent.

- [x] **Step 2: Bind and measure the two-phase API**

Add `ctypes` signatures for `lv_tsfile_writer_close_ex` and
`lv_tsfile_close_task_wait`. In async mode measure:

```python
submit_started = time.perf_counter()
check(lib.lv_tsfile_writer_close_ex(writer, 1, ctypes.byref(close_task)),
      "writer_close_ex")
close_submit_seconds = time.perf_counter() - submit_started

wait_started = time.perf_counter()
check(lib.lv_tsfile_close_task_wait(close_task), "close_task_wait")
close_wait_seconds = time.perf_counter() - wait_started
```

Print `close_submit_seconds`, `close_wait_seconds`, and
`close_total_seconds`. Keep the existing synchronous benchmark output when the flag is absent.

- [x] **Step 3: Document exact LabVIEW use**

Document `async_close` as signed I32 by value and `out_close_task` as U64 Pointer to Value. Show a shift-register sequence that stores the previous Close Task, calls blocking wait outside the DAQ timing loop, sets the consumed task to zero, and always waits for the final task before application/DLL shutdown.

State explicitly that write and explicit flush remain synchronous, the switch affects only close, and a new close submission waits indefinitely for the previous background close before starting.

- [x] **Step 4: Run formatting and benchmark smoke checks**

Run:

```bash
python3 -m black --check cpp/src/labview_wrapper/benchmark_block.py
python3 cpp/src/labview_wrapper/benchmark_block.py --smoke \
  --output-dir /private/tmp/tsfile-labview-sync-close-smoke
python3 cpp/src/labview_wrapper/benchmark_block.py --smoke --async-close \
  --output-dir /private/tmp/tsfile-labview-async-close-smoke
```

Expected: Black passes; both benchmark modes succeed; async output includes positive submission, wait, and total durations.

- [x] **Step 5: Commit documentation and benchmark support**

```bash
git add cpp/src/labview_wrapper/README.md \
  cpp/src/labview_wrapper/LabVIEW读写TsFile操作手册.md \
  cpp/src/labview_wrapper/benchmark_block.py
git commit -m "docs(cpp): document LabVIEW asynchronous close"
```

### Task 4: Full Regression and Branch Audit

**Files:**
- Verify only; no intended source edits.

**Interfaces:**
- Consumes: all deliverables from Tasks 1–3.
- Produces: fresh completion evidence and a clean feature branch.

- [x] **Step 1: Run all focused close tests in parallel**

Run:

```bash
ctest --test-dir cpp/target/build/test \
  -R '^LabVIEW(AsyncClose|AsyncCloseCoordinator|Block|Config|Smoke)Test$' \
  -j 15 --output-on-failure
```

Expected: all selected tests pass under parallel CTest execution.

- [x] **Step 2: Run the complete clean C++ verification**

Use the persistent verified dependency archive cache so `clean` does not force network downloads:

```bash
./mvnw -P with-cpp \
  -Dtsfile.dependency.cache=/private/tmp/tsfile-labview-dependency-cache \
  clean verify
```

Expected: Maven `BUILD SUCCESS`, all active CTest cases pass, Spotless reports no changes needed, and Apache RAT reports zero unapproved files.

- [x] **Step 3: Restore Maven's generated CMake version edit and audit the branch**

If Maven rewrites `TsFile_CPP_VERSION`, restore `cpp/CMakeLists.txt` to its committed value without touching other files. Then run:

```bash
git diff --check
git status --short
git diff --name-only origin/develop..HEAD -- dist
git rev-parse refs/remotes/ly/labview
```

Expected: no whitespace errors, clean status, no `dist` changes, and the old LabVIEW reference remains `e5cde40b7061dc874586c6a0aebacae9e88bbc83`.
