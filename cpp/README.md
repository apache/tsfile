<!--

​    Licensed to the Apache Software Foundation (ASF) under one
​    or more contributor license agreements.  See the NOTICE file
​    distributed with this work for additional information
​    regarding copyright ownership.  The ASF licenses this file
​    to you under the Apache License, Version 2.0 (the
​    "License"); you may not use this file except in compliance
​    with the License.  You may obtain a copy of the License at

​        http://www.apache.org/licenses/LICENSE-2.0

​    Unless required by applicable law or agreed to in writing,
​    software distributed under the License is distributed on an
​    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
​    KIND, either express or implied.  See the License for the
​    specific language governing permissions and limitations
​    under the License.

-->

# TsFile C++ Document

<pre>
___________    ___________.__.__          
\__    ___/____\_   _____/|__|  |   ____  
  |    | /  ___/|    __)  |  |  | _/ __ \ 
  |    | \___ \ |     \   |  |  |_\  ___/ 
  |____|/____  >\___  /   |__|____/\___  >  version 1.0.0
             \/     \/                 \/  
</pre>


## Introduction


This directory contains the C++ implementation of TsFile. The C++ version currently supports the query and write functions of TsFile, including time filtering queries.

The source code can be found in the `./src` directory. C/C++ examples are located in the `./examples` directory, and a benchmark for TsFile_cpp can be found in the `./bench_mark` directory. Additionally, a C function wrapper is available in the `./src/cwrapper` directory, which the Python tool relies on.

## How to make contributions

We use `clang-format` to ensure that our C++ code adheres to a consistent set of rules defined in `./clang-format`. This is similar to the Google style.

**Feature List**:

- [ ] Add unit tests for the reader, writer, compression, etc.
- [ ] Add unit tests for the C wrapper.
- [ ] Support multiple data flushes.
- [ ] Support aligned timeseries.
- [ ] Support table description in tsfile.
- [ ] Retrieve all table schemas/names.
- [ ] Implement automatic flush.
- [ ] Support out-of-order data writing.
- [ ] Support TsFile V4. Note: TsFile CPP does not implement support for the table model, therefore there are differences in file output compared to the Java version.

**Bug List**:

- [ ] Flushing without writing after registering a timeseries will cause a core dump.
- [ ] Misalignment in memory may lead to a bus error.

We welcome any bug reports. You can open an issue with a title starting with [CPP] to describe the bug, like: https://github.com/apache/tsfile/issues/94

## Build

### Requirements

```bash
sudo apt-get update
sudo apt-get install -y cmake make g++ clang-format
```

To build tsfile, you can run: `bash build.sh`. If you have Maven tools, you can run: `mvn package -P with-cpp clean verify`. Then, you can find the shared object at `./build`.

Before you submit your code to GitHub, please ensure that the `mvn` compilation is correct.

## Use TsFile

You can find examples on how to read and write data in `demo_read.cpp` and `demo_write.cpp` located under `./examples/cpp_examples`. There are also examples under `./examples/c_examples`on how to use a C-style API to read and write data in a C environment. You can run `bash build.sh` under `./examples` to generate an executable output under `./examples/build`.