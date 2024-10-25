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
cmake_minimum_required(VERSION 3.11)
project(TsFile_CPP)

cmake_policy(SET CMP0079 NEW)
set(TsFile_CPP_VERSION_MAJOR 0)
set(TsFile_CPP_VERSION_MINOR 1)

set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -Wall -Werror")
message("cmake using: USE_CPP11=${USE_CPP11}")


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

if(DEFINED ENV{CXX})
    set(CMAKE_CXX_COMPILER $ENV{CXX})
    message("cmake using: CXX=${CMAKE_CXX_COMPILER}")
endif()

if(DEFINED ENV{CC})
  set(CMAKE_C_COMPILER $ENV{CC})
  message("cmake using: CC=${CMAKE_C_COMPILER}")
endif()

message("cmake using: DEBUG_SE=${DEBUG_SE}")
if (${DEBUG_SE})
  add_definitions(-DDEBUG_SE=1)
  message("add_definitions -DDEBUG_SE=1")
endif()

if (${COV_ENABLED})
  add_definitions(-DCOV_ENABLED=1)
  message("add_definitions -DCOV_ENABLED=1")
endif()

message("build type: ${BUILD_TYPE}")
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -g")
  set(CMAKE_CXX_FLAGS_DEBUG" ${CMAKE_CXX_FLAGS} -O0 -g")
elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g") # disable -g
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS}")
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS}")
elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffunction-sections -fdata-sections -Os")
  set(LDFLAGS "-Wl,--gc-sections")
  set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS}")
endif()
message("CMAKE DEBUG: CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)
set(PROJECT_INCLUDE_DIR ${PROJECT_INCLUDE_DIR} 
                        ${PROJECT_SOURCE_DIR}/src
                        ${PROJECT_SOURCE_DIR}/third_party/lz4
                        ${PROJECT_SOURCE_DIR}/third_party/lzokay
)

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
include_directories(${PROJECT_INCLUDE_DIR})

add_subdirectory(third_party)

add_subdirectory(src)

configure_file(
  "${PROJECT_SOURCE_DIR}/cmake/config.h.in"
  "${PROJECT_BINARY_DIR}/cmake_config.h"
)

add_subdirectory(test)
if(TESTS_ENABLED)
    add_dependencies(TsFile_Test tsfile)
endif()

