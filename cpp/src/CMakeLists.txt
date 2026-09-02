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
add_subdirectory(dataset)
add_subdirectory(encoding)
add_subdirectory(file)
add_subdirectory(reader)
add_subdirectory(utils)
add_subdirectory(writer)

set(_TSFILE_OBJECT_TARGETS
        common_obj
        compress_obj
        cwrapper_obj
        dataset_obj
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
    if (CMAKE_VERSION VERSION_LESS "3.12")
        target_link_libraries(tsfile ${COV_LINK_LIB})
    else()
        target_link_libraries(tsfile ${_TSFILE_OBJECT_TARGETS} ${COV_LINK_LIB})
    endif()
else()
    message("Disable code cov...")
    if (NOT CMAKE_VERSION VERSION_LESS "3.12")
        target_link_libraries(tsfile ${_TSFILE_OBJECT_TARGETS})
    endif()
endif()

unset(_TSFILE_OBJECT_SOURCES)
unset(_TSFILE_OBJECT_TARGET)
unset(_TSFILE_OBJECT_TARGETS)

add_dependencies(tsfile utils_obj encoding_obj)

if (TSFILE_BUILD_SHARED)
    set(LIBTSFILE_PROJECT_VERSION ${TsFile_CPP_VERSION})
    set(LIBTSFILE_SO_VERSION ${TsFile_CPP_VERSION})
    set_target_properties(tsfile PROPERTIES VERSION ${LIBTSFILE_PROJECT_VERSION})
    set_target_properties(tsfile PROPERTIES SOVERSION ${LIBTSFILE_SO_VERSION})
endif()

# A shared library is a RUNTIME plus an import ARCHIVE on Windows and a LIBRARY
# on Unix. A static library is an ARCHIVE on every platform. Cover all three so
# the install step works for either library type.
install(TARGETS tsfile
        RUNTIME DESTINATION ${LIBRARY_OUTPUT_PATH}
        LIBRARY DESTINATION ${LIBRARY_OUTPUT_PATH}
        ARCHIVE DESTINATION ${LIBRARY_OUTPUT_PATH})

if (BUILD_SQLITE_EXTENSION)
    add_subdirectory(sqlite)
endif ()
