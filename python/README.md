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

<pre>
___________    ___________.__.__          
\__    ___/____\_   _____/|__|  |   ____  
  |    | /  ___/|    __)  |  |  | _/ __ \ 
  |    | \___ \ |     \   |  |  |_\  ___/ 
  |____|/____  >\___  /   |__|____/\___  >  version 1.0.0
             \/     \/                 \/  
</pre>


## Introduction

This directory contians the Python implementation of TsFile. The Python version of TsFile is implemented based on the CPP version. It utilizes the Cython package to integrate the read and write capabilities of TsFile CPP into the Python environment. Users can read and write TsFile just like they use read_csv and write_csv in Pandas.

The source code can be found in the `./tsfile` directory. Files ending with `.pyx` and `.pyd` are wrapper code written in Cython. The `tsfile/tsfile.py` defines some user interfaces. You can find some examples of reading and writing in the `.examples/examples.py`.


## How to make contributions

Using pylint to check Python code is recommended. However, there is no suitable style checking tool for Cython code, and this part of the code should be consistent with the Python style required by pylint.

**Feature List**
- [ ] In pywrapper, invoke the batch reading interface implemented in tsfile_CPP.



## Build

You can build tsfile_python by mvn or shell command. 

Before constructing tsfile_python, it is necessary to build [tsfile_cpp](../cpp/README.md) first, because tsfile_python relies on the shared library files provided by tsfile_cpp.

Build by mvn in this directory:

```sh
mvn initialize compile test
```

Build by mvn in tsfile root directory:

```sh
mvn clean package -P with-python
```

Build by python command:
```sh
python3 setup.py build_ext --inplace
```

