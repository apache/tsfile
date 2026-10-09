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

if (DEFINED ToolChain)
    include(${CMAKE_SOURCE_DIR}/cmake/ToolChain.cmake)
    message(STATUS "Using ToolChain: ${CMAKE_TOOLCHAIN_FILE}")
else()
    message(STATUS "Not using ToolChain")
endif ()

if (POLICY CMP0079)
    cmake_policy(SET CMP0079 NEW)
endif ()
if (POLICY CMP0074)
    cmake_policy(SET CMP0074 NEW)
endif ()
set(TsFile_CPP_VERSION 2.3.2.dev)

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/DependencySource.cmake)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(TSFILE_BUILD_SHARED "Build libtsfile as a shared library" ON)
message("cmake using: TSFILE_BUILD_SHARED=${TSFILE_BUILD_SHARED}")

if (MSVC)
    # MSVC does not provide a /std:c++11 flag; C++11 is its implicit baseline.
    # The lowest explicitly settable standard is /std:c++14. Without this flag,
    # the default varies by VS version (VS2017+ defaults to C++14 mode with some
    # C++17 extensions), so we pin it explicitly for reproducibility.
    set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} /W3 /utf-8 /EHsc /bigobj /Zc:__cplusplus /std:c++14")
    add_definitions(-DNOMINMAX -D_CRT_SECURE_NO_WARNINGS -D_CRT_NONSTDC_NO_WARNINGS
                    -D_SCL_SECURE_NO_WARNINGS -D_WINSOCK_DEPRECATED_NO_WARNINGS)
    if (TSFILE_BUILD_SHARED)
        # Export all symbols of the tsfile shared library automatically so that
        # consumers do not need __declspec(dllexport) annotations.
        set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
    endif ()
else ()
    set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -Wall")
endif ()

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wunused -Wuninitialized -D__STDC_FORMAT_MACROS")
endif ()

message("cmake using: USE_CPP11=${USE_CPP11}")
# MSVC has no /std:c++11; CMake maps this to the closest supported standard
# (C++14 default on MSVC), which compiles the C++11 codebase fine.
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

option(ENABLE_MEM_STAT "Enable memory status" ON)

if (ENABLE_MEM_STAT)
    add_definitions(-DENABLE_MEM_STAT)
    message("add_definitions -DENABLE_MEM_STAT")
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
# Tune Release builds for the build-host CPU (-march=native). OFF by default:
# native is non-portable (a binary built on a newer CPU can fault with an
# illegal instruction on an older one) and must not leak into CI artifacts or
# shipped wheels. On virtualized CI hosts native can even mis-detect the
# feature set (e.g. drop +crc), which breaks third_party/snappy's crc32 path.
# Turn ON only for local-only builds where the binary never leaves the machine.
option(TSFILE_ENABLE_NATIVE_ARCH
        "Tune Release builds for the build host CPU via -march=native (local-only, non-portable)"
        OFF)
# Keep optimization policy external by default (caller/toolchain/CMake defaults).
set(TSFILE_OPTIMIZATION_FLAGS ""
        CACHE STRING
        "Optional extra optimization flags for tsfile-cpp (e.g. -O3). Empty means inherit caller defaults.")
if (TSFILE_OPTIMIZATION_FLAGS)
    # Apply after CMake defaults for each config so explicit optimization can
    # override default -O flags in Release/RelWithDebInfo/Debug/MinSizeRel.
    set(CMAKE_CXX_FLAGS_DEBUG
            "${CMAKE_CXX_FLAGS_DEBUG} ${TSFILE_OPTIMIZATION_FLAGS}")
    set(CMAKE_CXX_FLAGS_RELEASE
            "${CMAKE_CXX_FLAGS_RELEASE} ${TSFILE_OPTIMIZATION_FLAGS}")
    set(CMAKE_CXX_FLAGS_RELWITHDEBINFO
            "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} ${TSFILE_OPTIMIZATION_FLAGS}")
    set(CMAKE_CXX_FLAGS_MINSIZEREL
            "${CMAKE_CXX_FLAGS_MINSIZEREL} ${TSFILE_OPTIMIZATION_FLAGS}")
    message("cmake using: TSFILE_OPTIMIZATION_FLAGS=${TSFILE_OPTIMIZATION_FLAGS}")
else ()
    message("cmake using: TSFILE_OPTIMIZATION_FLAGS=<inherit>")
    # MSVC provides sensible per-configuration optimization flags by default; the
    # GCC-style flags below would be rejected by cl.exe, so skip them on MSVC.
    if (NOT MSVC)
        if (CMAKE_BUILD_TYPE STREQUAL "Debug")
            set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -g")
        elseif (CMAKE_BUILD_TYPE STREQUAL "Release")
            # -flto + MinGW gcc + statically-linked antlr4_static produces
            # unresolved-reference errors at link time (LTO intermediate
            # objects can't see the .a's vtable thunks), so LTO is Linux/macOS
            # only. -march=native is portable poison for shipped/CI binaries
            # (see TSFILE_ENABLE_NATIVE_ARCH above): the default target for the
            # deployment platform is the portable baseline, and on macOS arm64
            # it still includes +crc so snappy's fast path stays available.
            if (MINGW OR WIN32)
                set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
            elseif (TSFILE_ENABLE_NATIVE_ARCH)
                set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -march=native -flto")
            else ()
                set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3 -flto")
            endif ()
        elseif (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
            set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -O2 -g")
        elseif (CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
            set(CMAKE_CXX_FLAGS_MINSIZEREL "${CMAKE_CXX_FLAGS_MINSIZEREL} -ffunction-sections -fdata-sections -Os")
            set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--gc-sections")
        endif ()
    endif ()
endif ()
message("CMAKE DEBUG: CMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}")

# disable asan by default.
option(ENABLE_ASAN "Enable Address Sanitizer" OFF)

if (ENABLE_ASAN)
    message("Address Sanitizer is enabled.")
    if (MSVC)
        # MSVC ships AddressSanitizer; it requires Visual Studio 2019 16.9 or
        # newer (MSVC_VERSION >= 1928). Only the address sanitizer is available
        # (there is no UndefinedBehaviorSanitizer for MSVC).
        if (MSVC_VERSION LESS 1928)
            message(FATAL_ERROR
                "ENABLE_ASAN requires MSVC 19.28+ (Visual Studio 2019 16.9); "
                "detected MSVC_VERSION=${MSVC_VERSION}.")
        endif ()
        # /fsanitize=address is incompatible with the /RTC* runtime checks that
        # CMake injects into Debug builds, and with incremental linking. Strip
        # /RTC* from the per-config flags and force non-incremental linking.
        #
        # ASan also needs debug info: /Zi (compile) + /DEBUG (link). Without it
        # MSVC emits warning C5072 ("ASAN enabled without debug information
        # emission"), which the bundled googletest build promotes to an error
        # via /WX in Release builds, and ASan reports lose symbol/line info.
        add_compile_options(/fsanitize=address /Zi)
        foreach (flagsVar
                 CMAKE_C_FLAGS_DEBUG CMAKE_CXX_FLAGS_DEBUG
                 CMAKE_C_FLAGS_RELWITHDEBINFO CMAKE_CXX_FLAGS_RELWITHDEBINFO)
            string(REGEX REPLACE "/RTC[1csu]+" "" ${flagsVar} "${${flagsVar}}")
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
if (BUILD_TEST)
    add_definitions(-DENABLE_TEST)
endif ()

option(BUILD_TOOLS "Build the tsfile command-line tools" ON)
message("cmake using: BUILD_TOOLS=${BUILD_TOOLS}")

option(BUILD_BENCHMARK "Build the read-backend benchmark" OFF)
message("cmake using: BUILD_BENCHMARK=${BUILD_BENCHMARK}")

option(ENABLE_ANTLR4 "Enable ANTLR4 runtime" ON)
message("cmake using: ENABLE_ANTLR4=${ENABLE_ANTLR4}")

if (ENABLE_ANTLR4)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/ANTLR4Dependency.cmake)
endif ()

option(ENABLE_SNAPPY "Enable Google Snappy compression" ON)
message("cmake using: ENABLE_SNAPPY=${ENABLE_SNAPPY}")

if (ENABLE_SNAPPY)
    add_definitions(-DENABLE_SNAPPY)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/SnappyDependency.cmake)
endif()

option(ENABLE_LZ4 "Enable LZ4 compression" ON)
message("cmake using: ENABLE_LZ4=${ENABLE_LZ4}")

if (ENABLE_LZ4)
    add_definitions(-DENABLE_LZ4)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/LZ4Dependency.cmake)
endif()

option(ENABLE_LZOKAY "Enable LZOKAY compression" ON)
message("cmake using: ENABLE_LZOKAY=${ENABLE_LZOKAY}")

if (ENABLE_LZOKAY)
    add_definitions(-DENABLE_LZOKAY)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/LZOKAYDependency.cmake)
endif()

option(ENABLE_ZLIB "Enable Zlib compression" ON)
message("cmake using: ENABLE_ZLIB=${ENABLE_ZLIB}")

if (ENABLE_ZLIB)
    add_definitions(-DENABLE_ZLIB)
    add_definitions(-DENABLE_GZIP)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/ZLIBDependency.cmake)
endif()

option(ENABLE_ZSTD "Enable Zstandard compression" ON)
message("cmake using: ENABLE_ZSTD=${ENABLE_ZSTD}")

if (ENABLE_ZSTD)
    add_definitions(-DENABLE_ZSTD)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/ZSTDDependency.cmake)
endif ()

# LZMA2 is opt-in so this optional codec does not raise the CMake 3.11 baseline
# used by long-lived industrial and legacy toolchains. The verified bundled XZ
# Utils 5.8.3 build requires CMake 3.20 or newer; older CMake versions may still
# enable LZMA2 when a compatible system liblzma package is available.
option(ENABLE_LZMA2
        "Enable LZMA2 compression (bundled liblzma requires CMake 3.20+)"
        OFF)
message("cmake using: ENABLE_LZMA2=${ENABLE_LZMA2}")

if (ENABLE_LZMA2)
    add_definitions(-DENABLE_LZMA2)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/LibLZMADependency.cmake)
else ()
    message(STATUS
            "LZMA2 is disabled. It defaults to OFF so this optional codec "
            "does not raise the build-system floor for legacy toolchains; "
            "enable it explicitly with a compatible system liblzma or "
            "with bundled XZ Utils on CMake 3.20+.")
endif ()

option(ENABLE_THREADS "Enable multi-threaded read/write (requires pthreads)" ON)
message("cmake using: ENABLE_THREADS=${ENABLE_THREADS}")

if (ENABLE_THREADS)
    add_definitions(-DENABLE_THREADS)
    find_package(Threads REQUIRED)
    link_libraries(Threads::Threads)
endif()

option(ENABLE_SIMD "Enable SIMD acceleration" ON)
message("cmake using: ENABLE_SIMD=${ENABLE_SIMD}")

if (ENABLE_SIMD)
    add_definitions(-DENABLE_SIMD)
    include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/SIMDeDependency.cmake)
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
    # MSVC does not provide a /std:c++11 flag; C++11 is its implicit baseline.
    # The lowest explicitly settable standard is /std:c++14. Without this flag,
    # the default varies by VS version (VS2017+ defaults to C++14 mode with some
    # C++17 extensions), so we pin it explicitly for reproducibility.
    set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} /W3 /utf-8 /EHsc /bigobj /Zc:__cplusplus /std:c++14")
else ()
    set(CMAKE_CXX_FLAGS "$ENV{CXXFLAGS} -Wall -std=c++11")
endif ()
add_subdirectory(third_party)

# CMake 3.11 cannot call target_link_libraries() on object-library targets, but
# directory-level link_libraries() initializes both their usage requirements
# and the final library's link dependencies. Apply every enabled project
# dependency before creating the object libraries below. The test target links
# these dependencies explicitly, so temporarily restore the original directory
# property while configuring downloaded GoogleTest targets.
set(_TSFILE_PROJECT_DEPENDENCIES "")
if (ENABLE_ANTLR4)
    list(APPEND _TSFILE_PROJECT_DEPENDENCIES TsFile::ANTLR4)
endif ()
if (ENABLE_SNAPPY)
    list(APPEND _TSFILE_PROJECT_DEPENDENCIES TsFile::Snappy)
endif ()
if (ENABLE_LZ4)
    list(APPEND _TSFILE_PROJECT_DEPENDENCIES TsFile::LZ4)
endif ()
if (ENABLE_LZOKAY)
    list(APPEND _TSFILE_PROJECT_DEPENDENCIES TsFile::LZOKAY)
endif ()
if (ENABLE_ZLIB)
    list(APPEND _TSFILE_PROJECT_DEPENDENCIES TsFile::ZLIB)
endif ()
if (ENABLE_ZSTD)
    list(APPEND _TSFILE_PROJECT_DEPENDENCIES TsFile::ZSTD)
endif ()
if (ENABLE_LZMA2)
    list(APPEND _TSFILE_PROJECT_DEPENDENCIES TsFile::LibLZMA)
endif ()
if (ENABLE_SIMD)
    list(APPEND _TSFILE_PROJECT_DEPENDENCIES TsFile::SIMDe)
endif ()

if (NOT "${_TSFILE_PROJECT_DEPENDENCIES}" STREQUAL "")
    get_property(_TSFILE_BASE_LINK_LIBRARIES
            DIRECTORY PROPERTY LINK_LIBRARIES)
    link_libraries(${_TSFILE_PROJECT_DEPENDENCIES})
    get_property(_TSFILE_PROJECT_LINK_LIBRARIES
            DIRECTORY PROPERTY LINK_LIBRARIES)
endif ()

add_subdirectory(src)
if (BUILD_BENCHMARK)
    find_package(Threads REQUIRED)
    add_executable(read_backend_benchmark
            bench_mark/bench_mark_src/read_backend_benchmark.cc)
    target_include_directories(read_backend_benchmark PRIVATE
            ${PROJECT_SRC_DIR})
    target_link_libraries(read_backend_benchmark PRIVATE
            tsfile Threads::Threads)
endif ()
if (BUILD_TOOLS)
    add_subdirectory(tools)
endif ()
if (BUILD_TEST)
    # The test target links project dependencies explicitly. Do not make
    # downloaded GoogleTest targets inherit them, because their upstream
    # install export cannot export TsFile-owned targets.
    if (NOT "${_TSFILE_PROJECT_DEPENDENCIES}" STREQUAL "")
        set_property(DIRECTORY PROPERTY LINK_LIBRARIES
                "${_TSFILE_BASE_LINK_LIBRARIES}")
    endif ()
    add_subdirectory(test)
    if (TESTS_ENABLED)
        add_dependencies(TsFile_Test tsfile)
    endif ()
else()
    message("BUILD_TEST is OFF, skipping test directory")
endif ()

if (NOT "${_TSFILE_PROJECT_DEPENDENCIES}" STREQUAL "")
    set_property(DIRECTORY PROPERTY LINK_LIBRARIES
            "${_TSFILE_PROJECT_LINK_LIBRARIES}")
    unset(_TSFILE_BASE_LINK_LIBRARIES)
    unset(_TSFILE_PROJECT_LINK_LIBRARIES)
endif ()
unset(_TSFILE_PROJECT_DEPENDENCIES)

add_subdirectory(examples)
