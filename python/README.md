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

## TsFileDataFrame column roles

`TsFileDataFrame` can load a JSON description for table columns. Use `C` for a
covariate and `T` for a target column:

```json
{
  "weather": {
    "temperature": "C",
    "humidity": "T"
  }
}
```

Pass the description path when opening the dataframe. `get` and `set` provide
dictionary-style access to top-level JSON values; `set` writes the updated
description back to disk.

```python
df = TsFileDataFrame("weather.tsfile", description_path="description.json")
df.get("weather")
df.get_covariate_columns("weather")
df.get_target_columns("weather")
df.get_covariate_column_count("weather")
df.get_target_column_count("weather")
df.get_device_count("weather")
df.get_device_node_counts("weather")
df.get_device_point_counts("weather")
df.get_device_statistics("weather")
df.get_device_point_count("weather", "device_a")
df.get_device_stats("weather", {"device": "device_a"})
df.list_device_metadata("weather")
df.set("weather", {"temperature": "T", "humidity": "T"})
df.close()
```

Device node counts refer to the physically present numeric time series under
each device. `get_device_node_counts` uses each device's ordered tag-value tuple
as its key, while `get_device_statistics` also reports point/value counts and
time bounds, and `list_device_metadata` expands those tags into named columns.
`point_count` follows the DataFrame timeline count (including NaN rows), while
`value_count` counts non-null values.


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
