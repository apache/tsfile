#[[
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
]]

message("Running in examples/cpp_examples directory")

add_library(cpp_examples_obj OBJECT
    demo_read.cpp
    demo_write.cpp
    bench_read.cpp)

# bench_read.cpp requires C++17 (TsFile headers use [[maybe_unused]])
# and Arrow/Parquet headers. Both are provided by the parent scope.
set_target_properties(cpp_examples_obj PROPERTIES
    CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
target_compile_options(cpp_examples_obj PRIVATE -std=c++17)
target_link_libraries(cpp_examples_obj PRIVATE
    Arrow::arrow_shared
    Parquet::parquet_shared)
