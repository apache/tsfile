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
message("Running in bench_mark directory")
cmake_minimum_required(VERSION 3.11)
project(tsfile_bench_mark_project)

if(DEFINED ENV{CXX})
    set(CMAKE_CXX_COMPILER $ENV{CXX})
endif()

include_directories(
        ${LIBRARY_INCLUDE_DIR}
        ${THIRD_PARTY_INCLUDE}
        ${CMAKE_SOURCE_DIR}/third_party/lz4
        ${CMAKE_SOURCE_DIR}/third_party/lzokay
        ${CMAKE_SOURCE_DIR}/third_party/zlib-1.2.13
        ${CMAKE_SOURCE_DIR}/third_party/google_snappy
        ${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src
        ${CMAKE_SOURCE_DIR}/src

)

link_directories(${LIBRARY_OUTPUT_PATH})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")
else()
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3")
endif()
message("CMAKE DEBUG: CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

add_executable(bench_mark
        src/bench_mark_cpp.cc
        src/bench_mark_c.cc
        src/bench_mark.cc
        src/bench_mark_utils.cc)

if (WIN32)
    target_link_libraries(bench_mark Psapi)
endif()

target_link_libraries(bench_mark tsfile)
