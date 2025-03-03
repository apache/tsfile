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
cmake_minimum_required(VERSION 3.10)
project(examples)
message("Running in exampes directory")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# TsFile include dir
set(SDK_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/../src/)
message("SDK_INCLUDE_DIR: ${SDK_INCLUDE_DIR}")

# TsFile shared object dir
set(SDK_LIB_DIR_RELEASE ${PROJECT_SOURCE_DIR}/../build/Release/lib)
message("SDK_LIB_DIR_RELEASE: ${SDK_LIB_DIR_RELEASE}")

set(SDK_LIB_DIR_DEBUG ${PROJECT_SOURCE_DIR}/../build/Debug/lib)
message("SDK_LIB_DIR_DEBUG: ${SDK_LIB_DIR_DEBUG}")
include_directories(${PROJECT_SOURCE_DIR}/../third_party/antlr4-cpp-runtime-4/runtime/src)

set(BUILD_TYPE "Debug")
include_directories(${SDK_INCLUDE_DIR})

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g")
set(CMAKE_CXX_FLAGS_DEBUG" ${CMAKE_CXX_FLAGS} -O0 -g")

# Find libtsfile in SDK_LIB_RIR
if (BUILD_TYPE STREQUAL "Release")
    find_library(my_tsfile_lib NAMES tsfile PATHS ${SDK_LIB_DIR_RELEASE} NO_DEFAULT_PATH REQUIRED)
elseif(BUILD_TYPE STREQUAL "Debug")
    find_library(my_tsfile_lib NAMES tsfile PATHS ${SDK_LIB_DIR_DEBUG} NO_DEFAULT_PATH REQUIRED)
endif ()

add_subdirectory(cpp_examples)
add_subdirectory(c_examples)

add_executable(examples examples.cc)
target_link_libraries(examples ${my_tsfile_lib} cpp_examples_obj c_examples_obj)