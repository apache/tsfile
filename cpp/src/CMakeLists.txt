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
project(TsFile_CPP_SDK)

include (${CMAKE_SOURCE_DIR}/cmake/CopyToDir.cmake)

if(POLICY CMP0079)
    cmake_policy(SET CMP0079 NEW)
endif()

option(ENABLE_SNAPPY "Enable Google Snappy compression" ON)
message("cmake using: ENABLE_SNAPPY=${ENABLE_SNAPPY}")

option(ENABLE_LZ4 "Enable LZ4 compression" ON)
message("cmake using: ENABLE_LZ4=${ENABLE_LZ4}")

option(ENABLE_LZOKAY "Enable LZOKAY compression" ON)
message("cmake using: ENABLE_LZOKAY=${ENABLE_LZOKAY}")

option(ENABLE_ZLIB "Enable Zlib compression" ON)
message("cmake using: ENABLE_ZLIB=${ENABLE_ZLIB}")

# ENABLE_SIMD is defined in the top-level CMakeLists.txt
message("cmake using: ENABLE_SIMD=${ENABLE_SIMD}")

message("Running in src directory")
if (${COV_ENABLED})
    add_compile_options(-fprofile-arcs -ftest-coverage)
endif ()

if (NOT ENABLE_ANTLR4)
    message("ANTLR4 is disabled")
endif()

set(PROJECT_INCLUDE_DIR
        ${CMAKE_SOURCE_DIR}/src
)

include_directories(${PROJECT_INCLUDE_DIR})

# Configure TSFILE_API (see utils/util_define.h) for every translation unit
# compiled into the tsfile library. Shared builds export data symbols, while
# static builds do not use DLL import/export decorations.
if (TSFILE_BUILD_SHARED)
    add_definitions(-DTSFILE_BUILDING)
else()
    add_definitions(-DTSFILE_STATIC)
endif()

if (ENABLE_ANTLR4)
    add_subdirectory(parser)
    message("Adding parser subdirectory")
endif()

add_subdirectory(common)
add_subdirectory(compress)
add_subdirectory(cwrapper)
add_subdirectory(encoding)
add_subdirectory(file)
add_subdirectory(reader)
add_subdirectory(utils)
add_subdirectory(writer)

# Object libraries need the directory-level project dependencies on older
# CMake versions, but those dependencies are implementation details of the
# shared library and must not become exported consumer requirements. Keep the
# platform thread target in the directory scope for its public link interface,
# then link codecs explicitly as PRIVATE below.
get_property(_TSFILE_SRC_LINK_LIBRARIES DIRECTORY PROPERTY LINK_LIBRARIES)
if (ENABLE_THREADS)
    set_property(DIRECTORY PROPERTY LINK_LIBRARIES Threads::Threads)
else ()
    set_property(DIRECTORY PROPERTY LINK_LIBRARIES "")
endif ()

set(_TSFILE_OBJECT_TARGETS
        common_obj
        compress_obj
        cwrapper_obj
        file_obj
        read_obj
        write_obj)
if (ENABLE_ANTLR4)
    list(APPEND _TSFILE_OBJECT_TARGETS parser_obj)
endif ()

if (CMAKE_VERSION VERSION_LESS "3.12")
    set(_TSFILE_OBJECT_SOURCES "")
    foreach (_TSFILE_OBJECT_TARGET IN LISTS _TSFILE_OBJECT_TARGETS)
        list(APPEND _TSFILE_OBJECT_SOURCES
                $<TARGET_OBJECTS:${_TSFILE_OBJECT_TARGET}>)
    endforeach ()
endif ()

if (TSFILE_BUILD_SHARED)
    if (CMAKE_VERSION VERSION_LESS "3.12")
        add_library(tsfile SHARED ${_TSFILE_OBJECT_SOURCES})
    else ()
        add_library(tsfile SHARED)
    endif ()
else()
    if (CMAKE_VERSION VERSION_LESS "3.12")
        add_library(tsfile STATIC ${_TSFILE_OBJECT_SOURCES})
    else ()
        add_library(tsfile STATIC)
    endif ()
    # Consumers of the CMake target must see TSFILE_API without DLL import
    # decoration when linking the static library on MSVC.
    target_compile_definitions(tsfile INTERFACE TSFILE_STATIC)
endif()

if (${COV_ENABLED})
    message("Enable code cov...")
    # Apple clang ships coverage runtime via --coverage; libgcov isn't a
    # standalone library on macOS.  Use --coverage there.
    if (APPLE)
        set(COV_LINK_LIB --coverage)
    else()
        set(COV_LINK_LIB -lgcov)
    endif()
    target_link_libraries(tsfile PRIVATE ${COV_LINK_LIB})
else()
    message("Disable code cov...")
endif()

# Linking object libraries directly makes static-library exports depend on
# private, non-exported object targets. Add their object files as sources
# instead; this works on the CMake 3.11 baseline and keeps the install target
# self-contained for both shared and static builds.
foreach (_TSFILE_OBJECT_TARGET IN LISTS _TSFILE_OBJECT_TARGETS)
    target_sources(tsfile PRIVATE
            $<TARGET_OBJECTS:${_TSFILE_OBJECT_TARGET}>)
endforeach ()

if (NOT "${_TSFILE_PROJECT_DEPENDENCIES}" STREQUAL "")
    target_link_libraries(tsfile PRIVATE ${_TSFILE_PROJECT_DEPENDENCIES})
endif ()

unset(_TSFILE_OBJECT_SOURCES)
unset(_TSFILE_OBJECT_TARGET)
unset(_TSFILE_OBJECT_TARGETS)

add_dependencies(tsfile utils_obj encoding_obj)

set_property(DIRECTORY PROPERTY LINK_LIBRARIES
        "${_TSFILE_SRC_LINK_LIBRARIES}")
unset(_TSFILE_SRC_LINK_LIBRARIES)

if (TSFILE_BUILD_SHARED)
    # Keep development suffixes out of the install name and SONAME. The ABI
    # epoch changes only for incompatible binary interface changes.
    set(LIBTSFILE_PROJECT_VERSION ${TSFILE_PACKAGE_VERSION})
    set(LIBTSFILE_SO_VERSION ${TSFILE_ABI_VERSION})
    set_target_properties(tsfile PROPERTIES VERSION ${LIBTSFILE_PROJECT_VERSION})
    set_target_properties(tsfile PROPERTIES SOVERSION ${LIBTSFILE_SO_VERSION})
endif()

add_library(TsFile::tsfile ALIAS tsfile)

target_include_directories(tsfile PUBLIC
        $<BUILD_INTERFACE:${LIBRARY_INCLUDE_DIR}>
        $<BUILD_INTERFACE:${PROJECT_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/tsfile>)
if (ENABLE_SIMD AND TSFILE_SIMDE_INCLUDE_ROOT)
    # Public TsFile headers include SIMDe directly. Copy the resolved header
    # tree into the install prefix so consumers do not need the build host's
    # system or dependency-cache paths.
    target_include_directories(tsfile PUBLIC
            $<BUILD_INTERFACE:${TSFILE_SIMDE_INCLUDE_ROOT}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
endif ()
if (ENABLE_ANTLR4 AND TSFILE_ANTLR4_INCLUDE_ROOT)
    target_include_directories(tsfile PUBLIC
            $<BUILD_INTERFACE:${TSFILE_ANTLR4_INCLUDE_ROOT}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
endif ()
if (ENABLE_ANTLR4 AND TSFILE_UTF8CPP_INCLUDE_ROOT)
    target_include_directories(tsfile PUBLIC
            $<BUILD_INTERFACE:${TSFILE_UTF8CPP_INCLUDE_ROOT}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
endif ()
target_compile_features(tsfile PUBLIC cxx_std_11)
set(_TSFILE_PUBLIC_FEATURE_DEFINITIONS "")
foreach (_TSFILE_PUBLIC_FEATURE
        ENABLE_ANTLR4
        ENABLE_MEM_STAT
        ENABLE_SNAPPY
        ENABLE_LZ4
        ENABLE_LZOKAY
        ENABLE_ZLIB
        ENABLE_GZIP
        ENABLE_ZSTD
        ENABLE_LZMA2
        ENABLE_THREADS
        ENABLE_SIMD)
    if (${_TSFILE_PUBLIC_FEATURE})
        list(APPEND _TSFILE_PUBLIC_FEATURE_DEFINITIONS
                ${_TSFILE_PUBLIC_FEATURE})
    endif ()
endforeach ()
if (NOT "${_TSFILE_PUBLIC_FEATURE_DEFINITIONS}" STREQUAL "")
    target_compile_definitions(tsfile INTERFACE
            ${_TSFILE_PUBLIC_FEATURE_DEFINITIONS})
endif ()
unset(_TSFILE_PUBLIC_FEATURE)
unset(_TSFILE_PUBLIC_FEATURE_DEFINITIONS)

if (TSFILE_BUILD_SHARED)
    # A shared library already resolves its private codec and object-library
    # dependencies at link time. Do not export those build-only targets as
    # consumer link requirements; they are not installed with the package.
    set_property(TARGET tsfile PROPERTY INTERFACE_LINK_LIBRARIES "")
endif ()

# A shared library is a RUNTIME plus an import ARCHIVE on Windows and a LIBRARY
# on Unix. A static library is an ARCHIVE on every platform. Cover all three so
# the install step works for either library type.
install(TARGETS tsfile
        EXPORT TsFileTargets
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT runtime
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT runtime
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT development)

install(EXPORT TsFileTargets
        FILE TsFileTargets.cmake
        NAMESPACE TsFile::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/TsFile
        COMPONENT development)
