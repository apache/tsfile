#[[
Licensed to the Apache Software Foundation(ASF) under one
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

if (DEFINED ToolChain)
    include(${CMAKE_SOURCE_DIR}/cmake/ToolChain.cmake)
    message(STATUS "Using ToolChain: ${CMAKE_TOOLCHAIN_FILE}")
else()
    message(STATUS "Not using ToolChain")
endif ()

if (POLICY CMP0079)
    cmake_policy(SET CMP0079 NEW)
endif ()
set(TsFile_CPP_VERSION 2.2.1.dev)

if (MSVC)
    # MSVC has no /std:c++11 flag; pin the closest supported standard mode.
    set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} /W3 /utf-8 /EHsc /bigobj /Zc:__cplusplus /std:c++14")
    add_definitions(-DNOMINMAX -D_CRT_SECURE_NO_WARNINGS -D_CRT_NONSTDC_NO_WARNINGS
                    -D_SCL_SECURE_NO_WARNINGS -D_WINSOCK_DEPRECATED_NO_WARNINGS)
    set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
else ()
    set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -Wall")
endif ()

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunused -Wuninitialized -D__STDC_FORMAT_MACROS")
endif ()

message("cmake using: USE_CPP11=${USE_CPP11}")
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED OFF)
if (NOT MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif ()

if (DEFINED ENV{CXX})
    set(CMAKE_CXX_COMPILER $ENV{CXX})
    message("cmake using: CXX=${CMAKE_CXX_COMPILER}")
endif ()

if (DEFINED ENV{CC})
    set(CMAKE_C_COMPILER $ENV{CC})
    message("cmake using: CC=${CMAKE_C_COMPILER}")
endif ()

message("cmake using: DEBUG_SE=${DEBUG_SE}")
if (${DEBUG_SE})
    add_definitions(-DDEBUG_SE=1)
    message("add_definitions -DDEBUG_SE=1")
endif ()

if (${COV_ENABLED})
    add_definitions(-DCOV_ENABLED=1)
    message("add_definitions -DCOV_ENABLED=1")
endif ()


if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
endif ()

if (NOT DEFINED CMAKE_BUILD_PARALLEL_LEVEL)
    include(ProcessorCount)
    ProcessorCount(N)
    if (N EQUAL 0)
        set(N 1)
    endif ()
    set(CMAKE_BUILD_PARALLEL_LEVEL ${N} CACHE STRING "Number of parallel build jobs")
    message("CMAKE BUILD PARALLEL LEVEL: ${CMAKE_BUILD_PARALLEL_LEVEL} (auto-detected)")
else ()
    message("CMAKE BUILD PARALLEL LEVEL: ${CMAKE_BUILD_PARALLEL_LEVEL} (from environment)")
endif ()

message("CMAKE BUILD TYPE " ${CMAKE_BUILD_TYPE})
if (NOT MSVC)
    if (CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
    elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native -flto")
    elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
        set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O2 -g")
    elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
        set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -ffunction-sections -fdata-sections -Os")
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gc-sections")
    endif ()
endif ()
message("CMAKE DEBUG: CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

# disable asan by default.
option(ENABLE_ASAN "Enable Address Sanitizer" OFF)

if (ENABLE_ASAN)
    message("Address Sanitizer is enabled.")
    if (MSVC)
        if (MSVC_VERSION LESS 1928)
            message(FATAL_ERROR
                "ENABLE_ASAN requires MSVC 19.28+ (Visual Studio 2019 16.9); "
                "detected MSVC_VERSION=${MSVC_VERSION}.")
        endif ()
        add_compile_options(/fsanitize=address /Zi)
        foreach (flagsVar
                 CMAKE_C_FLAGS_DEBUG CMAKE_CXX_FLAGS_DEBUG
                 CMAKE_C_FLAGS_RELWITHDEBINFO CMAKE_CXX_FLAGS_RELWITHDEBINFO)
            string(REGEX REPLACE "/RTC[1csu]+" "" ${
    flagsVar} "${${flagsVar}}")
        endforeach ()
        add_link_options(/INCREMENTAL:NO /DEBUG)
    elseif (NOT WIN32)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address,undefined -fno-omit-frame-pointer")

        if (NOT APPLE)
            set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static-libasan")
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined -static-libasan -static-libubsan")
        else ()
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address,undefined")
        endif ()
    else ()
        message(WARNING
            "ENABLE_ASAN on Windows is only supported with the MSVC toolchain; "
            "ignoring it for the current generator.")
    endif ()
else ()
    message("Address Sanitizer is disabled.")
endif ()

option(BUILD_TEST "Build tests" ON)
message("cmake using: BUILD_TEST=${BUILD_TEST}")

option(ENABLE_ANTLR4 "Enable ANTLR4 runtime" ON)
message("cmake using: ENABLE_ANTLR4=${ENABLE_ANTLR4}")

option(ENABLE_SNAPPY "Enable Google Snappy compression" ON)
message("cmake using: ENABLE_SNAPPY=${ENABLE_SNAPPY}")

if (ENABLE_SNAPPY)
    add_definitions(-DENABLE_SNAPPY)
endif()

option(ENABLE_LZ4 "Enable LZ4 compression" ON)
message("cmake using: ENABLE_LZ4=${ENABLE_LZ4}")

if (ENABLE_LZ4)
    add_definitions(-DENABLE_LZ4)
endif()

option(ENABLE_LZOKAY "Enable LZOKAY compression" ON)
message("cmake using: ENABLE_LZOKAY=${ENABLE_LZOKAY}")

if (ENABLE_LZOKAY)
    add_definitions(-DENABLE_LZOKAY)
endif()

option(ENABLE_ZLIB "Enable Zlib compression" ON)
message("cmake using: ENABLE_ZLIB=${ENABLE_ZLIB}")

if (ENABLE_ZLIB)
    add_definitions(-DENABLE_ZLIB)
    add_definitions(-DENABLE_GZIP)
endif()

option(ENABLE_SIMD "Enable SIMD acceleration via SIMDe" ON)
message("cmake using: ENABLE_SIMD=${ENABLE_SIMD}")
set(ENABLE_SIMDE ${ENABLE_SIMD} CACHE BOOL "Enable SIMDe (SIMD Everywhere)" FORCE)

option(ENABLE_THREADS "Enable multi-threaded read/write (requires pthreads)" ON)
message("cmake using: ENABLE_THREADS=${ENABLE_THREADS}")

if (ENABLE_THREADS)
    add_definitions(-DENABLE_THREADS)
    find_package(Threads REQUIRED)
    link_libraries(Threads::Threads)
endif()

option(ENABLE_MEM_STAT "Enable per-module memory allocation statistics" ON)
message("cmake using: ENABLE_MEM_STAT=${ENABLE_MEM_STAT}")

if (ENABLE_MEM_STAT)
    add_definitions(-DENABLE_MEM_STAT)
endif()

# All libs will be stored here, including libtsfile, compress-encoding lib.
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

# TsFile code will be stored here.
set(PROJECT_SRC_DIR ${PROJECT_SOURCE_DIR}/src)

# All include files will be installed here.
# Use global var so that tests may also use this
set(LIBRARY_INCLUDE_DIR ${PROJECT_BINARY_DIR}/include CACHE STRING "TsFile includes")
set(THIRD_PARTY_INCLUDE ${PROJECT_BINARY_DIR}/third_party)

set(SAVED_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
if (MSVC)
    set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} /W3 /utf-8 /EHsc /bigobj /Zc:__cplusplus /std:c++14")
else ()
    set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -Wall -std=c++11")
endif ()
add_subdirectory(third_party)
set(CMAKE_CXX_FLAGS "${SAVED_CXX_FLAGS}")

add_subdirectory(src)
if (BUILD_TEST)
    add_subdirectory(test)
    if (TESTS_ENABLED)
        add_dependencies(TsFile_Test tsfile)
    endif ()
else()
    message("BUILD_TEST is OFF, skipping test directory")
endif ()

option(BUILD_EXAMPLES "Build examples (requires Arrow/Parquet)" OFF)
if (BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

if (EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/experiment/CMakeLists.txt")
    add_subdirectory(experiment)
endif()
