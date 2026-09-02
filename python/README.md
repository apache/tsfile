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

# TsFile Python Document

<p align="center">
  <img src="https://www.apache.org/logos/originals/tsfile.svg"
       alt="TsFile Logo"
       width="400"/>
</p>


## Introduction

This directory contains the Python implementation of TsFile. The Python version is built on the CPP version and uses the Cython package to integrate TsFile's read and write capabilities into the Python environment. Users can read and write TsFile as easily as they use read_csv and write_csv in Pandas.

The source code can be found in the `./tsfile` directory. Files ending with `.pyx` and `.pyd` are wrapper code written in Cython. The `tsfile/tsfile.py` defines some user interfaces. You can find some examples of reading and writing in the `.examples/examples.py`.


## How to make contributions

Using pylint to check Python code is recommended. However, there is no suitable style checking tool for Cython code, and this part of the code should be consistent with the Python style required by pylint.

**Feature List**
- [ ] In pywrapper, invoke the batch reading interface implemented in CPP version of TsFile.
- [ ] Supports writing multiple DataFrames into one single TsFile.



## Build

Before constructing Python version of TsFile, it is necessary to build [CPP version of TsFile](../cpp/README.md) first, because Python version of TsFile relies on the shared library files provided by CPP version of TsFile.

Build by mvn in root directory:

```sh
mvn -P with-cpp,with-python clean verify
```

Build by python command:

```sh
python setup.py build_ext --inplace
```

## File-level properties

`TsFileWriter` and `TsFileTableWriter` accept binary properties while they are
open. The setter accepts `bytes` only. Readers return `dict[str, bytes | None]`,
preserving null and empty values separately.

```python
with TsFileWriter("example.tsfile") as writer:
    writer.add_tsfile_property("binary-property", b"\x01\x00\xff")

with TsFileReader("example.tsfile") as reader:
    properties = reader.get_tsfile_properties()
```

Values do not carry a data type; use an explicit portable encoding when storing
numbers or structures.

## Local File Read Backend

Python readers inherit the process-wide backend setting when they open a file.
`PREAD` is the default to preserve the traditional positioned-read behavior,
`MMAP` requires memory mapping, and `AUTO` prefers mapping with a fallback to
`PREAD`.

```python
from tsfile import FileReadBackend, TsFileReader, set_file_read_backend

set_file_read_backend(FileReadBackend.MMAP)
with TsFileReader("example.tsfile") as reader:
    ...

# The configuration-dictionary API is equivalent.
from tsfile import set_tsfile_config
set_tsfile_config({"file_read_backend_": FileReadBackend.AUTO})
```

The setting only affects readers opened afterward. Do not modify or truncate a
file while it is open through the memory-mapped backend.
