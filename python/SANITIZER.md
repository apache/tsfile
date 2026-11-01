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

# Sanitizer builds for the Python bindings

The Python package crosses four layers:

```
Python tests -> Cython extensions (.so/.dylib) -> C wrapper -> libtsfile (C++)
```

The ordinary C++ sanitizer jobs only cover the last layer. This mode builds
**all four** with AddressSanitizer, keeps LeakSanitizer enabled, and runs
ownership/lifetime scenarios in fresh interpreters so allocated buffers,
invalid frees, use-after-free and result-buffer lifetime errors are visible
across the language boundary.

## Supported platforms

| Platform | ASan | LSan | Status |
|----------|------|------|--------|
| Linux x86_64/aarch64 (GCC or Clang) | yes | yes | Supported; used by CI |
| macOS (Apple Clang) | yes | no | Build/run manually with `detect_leaks=0` |
| Windows (MSVC/MinGW) | — | — | Not supported for the bindings |

The CI job runs on `ubuntu-24.04` with the distribution GCC and Python 3.11.

The C++-only `ENABLE_ASAN` jobs keep their historical static-runtime
configuration; this mode is the coordinated C++ + bindings build, where the
sanitizer compile flags are also re-applied after the bundled third-party
projects reset `CMAKE_CXX_FLAGS`.

## Build

### One command (C++ library + Cython extensions)

```bash
./mvnw -P with-python \
  -Dtsfile.sanitizer=address \
  -Dbuild.type=RelWithDebInfo \
  -Dbuild.test=OFF \
  -Dspotless.skip=true \
  clean test-compile
```

`-Dtsfile.sanitizer=<checks>` is the single switch. It:

* enables `ENABLE_ASAN` and passes `<checks>` to `-fsanitize=` for the C++
  library, adding the same flags to the shared-library link step;
* keeps the sanitizer runtime dynamic (`TSFILE_SANITIZER_SHARED_RUNTIME=ON`) so
  one preloaded runtime can serve Python, the extensions and `libtsfile`;
* exports `TSFILE_SANITIZER=<checks>` to `setup.py`, which instruments every
  extension (including `python_random_access_read_file.cc`) and verifies the
  result;
* disables wheel packaging. Sanitizer binaries are never release artifacts.

`RelWithDebInfo` is recommended: it provides `-O2 -g`, and the sanitizer mode
additionally adds `-g`. Release builds skip LTO when ASan is enabled because LTO
removes the frames reports need. `-Dbuild.test=OFF` only skips the C++ gtest
target; the Python module is still built and tested.

### Direct CMake + setup.py

```bash
# 1. C++ library, dynamic sanitizer runtime
bash cpp/build.sh -t=RelWithDebInfo -a=ON --sanitizer-shared-runtime=ON

# 2. Point setup.py at that build and instrument the extensions
cd python
TSFILE_CPP_BUILD=../cpp/build/relwithdebinfo \
TSFILE_SANITIZER=address \
python setup.py build_ext --inplace
```

`TSFILE_SANITIZER` takes the `-fsanitize=` list (use `address` for the
supported ASan + LSan mode). Use the same list for the C++ build
(`--sanitizer-checks=...` / `-DTSFILE_SANITIZER_CHECKS=...`), otherwise the
instrumented modules disagree about the runtime they need. The instrumentation
verification is ASan-specific.

`setup.py` inspects every built extension and the copied `libtsfile` with `nm`
and `readelf`/`otool` and fails if any of them lacks ASan references **or** a
link-time dependency on the sanitizer runtime. It also refuses `bdist_wheel`
and refuses to package any staged binary that carries `__asan_` symbols, so a
stale sanitizer object cannot slip into a release wheel.

## Run

A standard (non-instrumented) Python interpreter must load the sanitizer
runtime **before** any instrumented module; otherwise ASan aborts with
"ASan runtime does not come first in initial library list". Preload it:

```bash
ASAN_LIB="$(gcc -print-file-name=libasan.so)"          # GCC
LIBSTDCXX="$(gcc -print-file-name=libstdc++.so.6)"
# Or, with Clang: "$(clang -print-file-name=libclang_rt.asan-x86_64.so)"

TSFILE_SANITIZER=address \
LD_PRELOAD="$ASAN_LIB:$LIBSTDCXX" \
ASAN_OPTIONS="detect_leaks=1:abort_on_error=1:halt_on_error=1" \
LSAN_OPTIONS="suppressions=$PWD/python/tests/sanitizer/lsan_suppressions.txt" \
python -m pytest python/tests/test_sanitizer_ownership.py -v
```

`libasan.so` must be first so it owns process startup. `libstdc++.so.6`
immediately follows: GCC's ASan resolves the real `__cxa_throw` when the
interceptor initializes, and if libstdc++ is not in the initial library list it
aborts with `AddressSanitizer: CHECK failed ... real___cxa_throw != 0` the first
time a C++ exception is thrown. Putting libstdc++ before libasan instead fails
ASan's runtime-first check.

Run from the repository root (the suite sets the child working directory
itself) so `tests.sanitizer_scenarios` is importable in the children.
The pytest runner is started with `detect_leaks=0` in CI to avoid interpreter
noise; every scenario child forces `detect_leaks=1` back on, and the ownership
assertions only apply to those children. The import-only and runtime scenarios
show what the interpreter maps, and the positive controls prove the harness
fails on a real defect.

On macOS (manual ASan runs only; there is no `LD_PRELOAD`) use
`DYLD_INSERT_LIBRARIES`:

```bash
DYLD_INSERT_LIBRARIES="$(clang -print-file-name=libclang_rt.asan_osx_dynamic.dylib)" \
ASAN_OPTIONS="detect_leaks=0:halt_on_error=1" ...
```

### Manual scenario runs

```bash
cd python
ASAN_LIB="$(gcc -print-file-name=libasan.so)"
LIBSTDCXX="$(gcc -print-file-name=libstdc++.so.6)"
LD_PRELOAD="$ASAN_LIB:$LIBSTDCXX" TSFILE_SANITIZER=address \
  python -m tests.sanitizer_scenarios tree_full /tmp/tsfile-sanitizer
```

Use `build_fixtures` first; it writes `tree_all`, `table_all`, `tree_numeric`
and `table_numeric` files into the directory.

## What is covered

* Low-level tree and table reads, row and batch APIs, dataset reads.
* Numeric values plus STRING/TEXT/BLOB buffers, including NULLs and BLOBs with
  embedded NUL bytes.
* Full consumption, early termination, exception cleanup, repeated open/close,
  and reader/result close-order permutations.
* Arrow C data interface import and release.
* The file-object adapter (`python_random_access_read_file.cc`).

## Symbolization

Build with `RelWithDebInfo` (or `Debug`) and keep the binaries unstripped.
GCC's runtime uses `llvm-symbolizer` when available and falls back to
`addr2line` from binutils. To pin a symbolizer:

```bash
export ASAN_SYMBOLIZER_PATH="$(command -v llvm-symbolizer)"
```

Cython stacks show the generated frame followed by the `.pyx` line; the TsFile
allocation site appears as `libtsfile.so!...` or the extension module name.

## Suppression policy

The file is `python/tests/sanitizer/lsan_suppressions.txt`. Rules:

* Prefer no entries. The import-only baseline exists to identify noise.
* Scope every entry to a specific third-party module or allocator function. No
  wildcard modules.
* Never suppress `tsfile/`, `libtsfile`, or the extension modules. A leak there
  is the defect this job exists to find.
* Never set `detect_leaks=0` for scenario processes.
* Add a comment above each rule explaining the upstream allocation.

A normal CPython process does not release every code object, interned string or
thread lock before the sanitizer's exit-time check, and binary wheels keep
process-lifetime caches. The file therefore suppresses specific CPython
allocator functions (`PyObject_Malloc`, `PyMem_Malloc`, ...) and a few
third-party C extension modules; it never contains a TsFile module.

Reports that survive the suppressions are attributed by the harness
(`test_sanitizer_ownership.py`): a leak **fails the run only when its allocation
stack contains a `libtsfile` or extension frame**. Blocks that involve only
CPython or third-party code are counted and printed but do not fail the run.
This is attribution, not suppression: any leak that passes through the TsFile
modules fails, and the positive controls prove that a real defect is reported.

## Positive controls

`positive_control_leak` (ctypes `malloc` without free) and
`positive_control_uaf` (`malloc`/`free`/write-after-free) intentionally fail.
`test_sanitizer_ownership.py` asserts that they exit nonzero with a
`LeakSanitizer`/`AddressSanitizer` report. If a suppression file becomes too
broad, these controls fail first.

To validate that a real binding regression is detected, temporarily remove a
buffer release (for example the `tsfile_free_buffer(string)` call in
`get_value_by_index`) and re-run the suite: `tree_full`/`table_full` must fail
with a leak whose allocation stack names
`tsfile_result_set_get_value_by_index_string`.

## Troubleshooting

* **"ASan runtime does not come first in initial library list"** — `LD_PRELOAD`
  is missing or points at a different compiler's runtime. Use the runtime
  printed by the compiler that built the library.
* **`CHECK failed: asan_interceptors.cpp ... real___cxa_throw != 0`** — a C++
  exception was thrown while libstdc++ was not in the initial library list.
  Preload it immediately after libasan:
  `LD_PRELOAD="$ASAN_LIB:$(gcc -print-file-name=libstdc++.so.6)"`.
* **undefined symbol `__ubsan_*`** — the C++ library and extensions were built
  with different check lists. Rebuild both with the same
  `-Dtsfile.sanitizer=...` / `TSFILE_SANITIZER=...` value.
* **Leak reports in `libpython`/`pyarrow`/`numpy` only** — add a narrowly
  scoped suppression for that symbol; do not touch TsFile entries.
* **No leaks reported at all on macOS** — LeakSanitizer is not available on
  Darwin; run with `detect_leaks=0` and rely on ASan's access checks.
