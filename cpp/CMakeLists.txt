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
set(TsFile_CPP_VERSION 2.1.0.dev)
set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -Wall")
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-error=maybe-uninitialized -D__STDC_FORMAT_MACROS")
endif()

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


if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
endif ()

message("CMAKE BUILD TYPE " ${CMAKE_BUILD_TYPE})
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O2")
elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O2 -g")
elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
  set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -ffunction-sections -fdata-sections -Os")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gc-sections")
endif ()
message("CMAKE DEBUG: CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

# disable asan by default.
option(ENABLE_ASAN "Enable Address Sanitizer" OFF)

if (NOT WIN32)
  if (ENABLE_ASAN)
    message("Address Sanitizer is enabled.")

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")

    if (NOT APPLE)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static-libasan")
      set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address -static-libasan")
    else ()
      set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
    endif ()
  else ()
    message("Address Sanitizer is disabled.")
  endif ()
endif ()


# All libs will be stored here, including libtsfile, compress-encoding lib.
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

# TsFile code will be stored here.
set(PROJECT_SRC_DIR ${PROJECT_SOURCE_DIR}/src)

# All include files will be installed here.
set(LIBRARY_INCLUDE_DIR ${PROJECT_BINARY_DIR}/include)
set(THIRD_PARTY_INCLUDE ${PROJECT_BINARY_DIR}/third_party)

add_subdirectory(third_party)
add_subdirectory(src)
add_subdirectory(test)
add_subdirectory(examples)
if(TESTS_ENABLED)
    add_dependencies(TsFile_Test tsfile)
endif()

