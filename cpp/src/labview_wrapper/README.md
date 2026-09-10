<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# TsFile LabVIEW C wrapper

This directory provides a small C ABI over the TsFile C++ library. It avoids
passing C++ objects or pointer-containing structs through a LabVIEW Call
Library Function Node (CLFN).

## Which API to use

- One file, one homogeneous DOUBLE array: call
  `lv_tsfile_write_file_f64`. It creates the schema, opens the file, writes one
  block, and closes the file in one CLFN call.
- Continuous acquisition: open one writer, call
  `lv_tsfile_write_block_f64` once per acquisition block, and close the writer
  when the file rolls over.
- Existing mixed-type applications: use the schema, writer, and tablet builder
  functions in `tsfile_labview.h`.

The block functions are available for `INT32`, `FLOAT`, and `DOUBLE`. A writer
used by a block function must have exactly `ncols` columns, and every column
must have the matching type.

## Array layout

`ts` contains `nrows` signed 64-bit timestamps. `data` contains
`nrows * ncols` values in row-major order:

```text
data[row * ncols + column]
```

For a 9-channel LabVIEW acquisition, reshape the 2D buffer to a 1D array whose
first nine elements are the nine channels at the first timestamp. Configure
both array arguments as **Array Data Pointer**. Do not pass a LabVIEW array
handle or a pointer to a handle.

## Minimal one-call CLFN configuration

Configure `lv_tsfile_write_file_f64` with C calling convention:

| Parameter | LabVIEW configuration |
|---|---|
| return type | Signed 32-bit Integer, by value |
| `tsfile_path` | String, C String Pointer |
| `table_name` | String, C String Pointer |
| `column_names_newline_separated` | String, C String Pointer |
| `ts` | 1D I64 Array, Array Data Pointer |
| `data` | 1D DBL Array, Array Data Pointer |
| `nrows` | Signed 32-bit Integer, by value |
| `ncols` | Signed 32-bit Integer, by value |

For example, the column string for three channels is
`front_x\nfront_y\nfront_z`, where each `\n` represents an actual LF byte.
The return value is `0` on success.

The parent directory must already exist, and the output `.tsfile` path must
not already exist. Delete/rename the old file or use a new rolling-file name
before opening a writer.

## Streaming CLFN configuration

The streaming sequence is:

1. Create a schema builder and add all columns.
2. Call `lv_tsfile_writer_open` once. Configure `out_writer` as a U64 Pointer
   to Value.
3. Call `lv_tsfile_write_block_f64` for each block. Configure `writer` as U64
   by value, `ts` as 1D I64 Array Data Pointer, and `data` as 1D DBL Array Data
   Pointer.
4. Call `lv_tsfile_writer_close` exactly once.

The block call creates and releases its temporary tablet. The caller retains
ownership of both input arrays.

## Build and package

The LabVIEW process and shared library must have the same bitness. A 32-bit
LabVIEW installation requires the x86 DLL even on 64-bit Windows.

```bash
cmake -S cpp -B cpp/build/labview \
  -DBUILD_LABVIEW_WRAPPER=ON \
  -DBUILD_LABVIEW_WRAPPER_TESTS=ON
cmake --build cpp/build/labview --target tsfile_labview_block_test
```

On Windows, place these files beside the VI or executable:

- `libtsfile_labview.dll`
- `libtsfile.dll`
- the MinGW runtime DLLs used by that build

Distribute the complete matching x86 or x64 directory, not just
`libtsfile_labview.dll`: the wrapper depends on the core library and compiler
runtime DLLs. The LabVIEW process bitness, not the Windows bitness, selects the
package.

On Linux, place `libtsfile_labview.so` and `libtsfile.so` together and make the
directory discoverable by the dynamic loader, for example with an application
rpath or `LD_LIBRARY_PATH`. The exported C API and array layout are identical
on Windows and Linux. A Windows DLL cannot be used on Linux; build the same
source on the target Linux architecture and reselect `libtsfile_labview.so` in
the LabVIEW Call Library Function Node. A minimal Linux release build is:

```bash
cmake -S cpp -B cpp/build/labview-linux \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_LABVIEW_WRAPPER=ON \
  -DBUILD_LABVIEW_WRAPPER_TESTS=ON
cmake --build cpp/build/labview-linux --target tsfile_labview_block_test
LD_LIBRARY_PATH=cpp/build/labview-linux/lib \
  cpp/build/labview-linux/lib/tsfile_labview_block_test
```

Run the standalone correctness test from the library output directory. The
FFI-call comparison utility can be run with:

```bat
dist\TsFile-LabVIEW-x86\examples\run_block_test.cmd
```

```bash
python cpp/src/labview_wrapper/benchmark_block.py \
  --dll /path/to/libtsfile_labview.so \
  --rows 10000 --cols 9 --batches 3 --repeats 5
```
