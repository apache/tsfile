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
cmake_minimum_required(VERSION 3.14)
project(unit_tests)

# GoogleTest requires at least C++14
# https://github.com/google/leveldb/blob/main/CMakeLists.txt#L353
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

set(SDK_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/../src)
message("SDK_INCLUDE_DIR: ${SDK_INCLUDE_DIR}")
set(SDK_LIB_DIR_RELEASE ${PROJECT_SOURCE_DIR}/../build/Release/lib)
message("SDK_LIB_DIR_RELEASE: ${SDK_LIB_DIR_RELEASE}")

include_directories(${SDK_INCLUDE_DIR})
find_library(tsfile_lib NAMES tsfile PATHS ${SDK_LIB_DIR_RELEASE} NO_DEFAULT_PATH REQUIRED)

enable_testing()

file(GLOB_RECURSE TEST_SRCS "common/*_test.cc" "encoding/*_test.cc" "utils/*_test.cc" "file/*_test.cc")
add_executable(TsFile_Test ${TEST_SRCS})

target_link_libraries(
  TsFile_Test
  GTest::gtest_main
  ${tsfile_lib}
)

include(GoogleTest)
gtest_discover_tests(TsFile_Test)