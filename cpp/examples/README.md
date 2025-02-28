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

# TsFile Reader/Writer Integration Guide

## 1. Building TSFile Shared Library

Build Methods (Choose either approach)

### Method 1: Maven Build
Execute from the project root directory:

```BASH
mvn package -P with-cpp clean verify
```
Output location: cpp/target/build/lib

If maven is not installed, may use 'mvnw' in linux/macos or 'mvnw.cmd' in win instead"

### Method 2: Script Build
Run the build script:

```BASH
bash build.sh
```
Output location: cpp/build/Release/lib

## Project Configuration
### CMake Integration

Add to your CMakeLists.txt:

```CMAKE
find_library(TSFILE_LIB NAMES tsfile PATHS ${SDK_LIB} REQUIRED)
target_link_libraries(your_target ${TSFILE_LIB})
```

Note: Set ${SDK_LIB} to your TSFile library directory.

## 3. Implementation Examples
   
### Directory Structure
```TEXT
   ├── CMakeLists.txt
   ├── c_examples/
   │   ├── demo_write.c    # C write implementation
   │   └── demo_read.c     # C read implementation
   ├── cpp_examples/
   │   ├── demo_write.cpp  # C++ write implementation
   │   └── demo_read.cpp   # C++ read implementation
   └── examples.cc         # Combined use cases
```

### Code References
Writing TSFiles:\
C: c_examples/demo_write.c\
C++: cpp_examples/demo_write.cpp

Reading TSFiles:\
C: c_examples/demo_read.c\
C++: cpp_examples/demo_read.cpp