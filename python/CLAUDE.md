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

# CLAUDE.md — Python Module

This file provides guidance to Claude Code (claude.ai/code) when working with the Python module.

## Prerequisites

The Python module depends on the C++ shared library (`libtsfile`). **Build C++ first:**

```bash
# From repo root
./mvnw clean package -P with-cpp -DskipTests
```

## Build Commands

```bash
# Build everything via Maven (builds C++ then Python)
./mvnw clean verify -P with-python

# Or build directly (after C++ is built)
cd python
python setup.py build_ext --inplace

# Run tests
pytest tests/

# Run a single test
pytest tests/test_write_and_read.py -k "test_name"

# Format code
cd ../ && ./mvnw spotless:apply   # Uses Black 26.3.1
```

## Source Structure

```
python/tsfile/
├── __init__.py              # Public API exports
├── constants.py             # Enums: TSDataType, TSEncoding, Compressor, ColumnCategory
├── schema.py                # TimeseriesSchema, DeviceSchema, TableSchema, ColumnSchema
├── tablet.py                # Tablet class for batch columnar data
├── field.py / row_record.py # Row-based data structures
├── tag_filter.py            # Tag filtering DSL for queries
├── tsfile_table_writer.py   # High-level table writer API
├── utils.py                 # to_dataframe(), dataframe_to_tsfile()
├── exceptions.py            # Custom exception types
│
├── tsfile_py_cpp.pyx        # Cython core: Python ↔ C++ type conversion
├── tsfile_reader.pyx        # Cython reader wrapper
├── tsfile_writer.pyx        # Cython writer wrapper
├── tsfile_cpp.pxd           # C++ interface declarations (cwrapper)
│
└── dataset/                 # High-level multi-file API
    ├── dataframe.py         # TsFileDataFrame (pandas-like interface)
    ├── timeseries.py        # Timeseries, AlignedTimeseries
    ├── reader.py            # Multi-file reader coordination
    └── merge.py             # K-way merge for overlapping time ranges
```

## Architecture Notes

- **Layered design**: C++ core → Cython bridge (`*.pyx`) → Pure Python API
- `setup.py` copies pre-built C++ headers and shared libraries from `../cpp/target/build/`
- Cython binds to `cwrapper/tsfile_cwrapper.h` from the C++ module
- C error codes are mapped to Python exceptions via `check_error()` in `tsfile_py_cpp.pyx`
- Requires Python 3.9+, numpy >= 2.0, pandas >= 2.0, pyarrow >= 16.0

## Code Style

- **Formatter**: Black 26.3.1 (via Spotless Maven plugin)

## Testing

- **Framework**: pytest
- Tests in `python/tests/`, 19 test files covering write, read, filtering, DataFrame conversion, and Arrow format

## License Header

Every new file must include the Apache License 2.0 header at the top. For Python files, use `#` line comments. For `.pyx`/`.pxd` files, also use `#` comments. See any existing file for the exact wording.

## Git Commit

- Do NOT add `Co-Authored-By` trailer to commit messages.
