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

message("Running in src directory")
if (${COV_ENABLED})
    add_compile_options(-fprofile-arcs -ftest-coverage)
endif ()

if (ENABLE_ANTLR4)
    add_definitions(-DANTLR4CPP_STATIC)
    set(ANTLR4_WITH_STATIC_CRT OFF)
    message("ANTLR4 is enabled, adding ANTLR4 definitions")
else()
    message("ANTLR4 is disabled")
endif()

set(PROJECT_INCLUDE_DIR
        ${CMAKE_SOURCE_DIR}/src
)

if (ENABLE_SNAPPY)
    list(APPEND PROJECT_INCLUDE_DIR
            ${THIRD_PARTY_INCLUDE}/google_snappy
            ${CMAKE_SOURCE_DIR}/third_party/google_snappy
    )
endif()

if (ENABLE_LZ4)
    list(APPEND PROJECT_INCLUDE_DIR
            ${CMAKE_SOURCE_DIR}/third_party/lz4
    )
endif()

if (ENABLE_LZOKAY)
    list(APPEND PROJECT_INCLUDE_DIR
            ${CMAKE_SOURCE_DIR}/third_party/lzokay
    )
endif()

if (ENABLE_ZLIB)
    list(APPEND PROJECT_INCLUDE_DIR
            ${CMAKE_SOURCE_DIR}/third_party/zlib-1.2.13
    )
endif()

if (ENABLE_ANTLR4)
    list(APPEND PROJECT_INCLUDE_DIR
            ${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src
    )
    message("Adding ANTLR4 include directory")
endif()

include_directories(${PROJECT_INCLUDE_DIR})

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

set(COMPRESSION_LIBS "")

if (ENABLE_SNAPPY)
    list(APPEND COMPRESSION_LIBS snappy)
endif()

if (ENABLE_LZ4)
    list(APPEND COMPRESSION_LIBS LZ4)
endif()

if (ENABLE_LZOKAY)
    list(APPEND COMPRESSION_LIBS lzokay)
endif()

if (ENABLE_ZLIB)
    list(APPEND COMPRESSION_LIBS zlibstatic)
endif()

message("Compression libraries: ${COMPRESSION_LIBS}")

if (ENABLE_ANTLR4)
    target_link_libraries(parser_obj antlr4_static)
    message("Linking parser_obj with antlr4_static")
endif()

target_link_libraries(compress_obj ${COMPRESSION_LIBS})
target_link_libraries(common_obj ${COMPRESSION_LIBS})
target_link_libraries(read_obj ${COMPRESSION_LIBS})
target_link_libraries(write_obj ${COMPRESSION_LIBS})

add_library(tsfile SHARED)

if (${COV_ENABLED})
    message("Enable code cov...")
    if (ENABLE_ANTLR4)
        target_link_libraries(tsfile common_obj compress_obj cwrapper_obj file_obj read_obj write_obj parser_obj -lgcov)
    else()
        target_link_libraries(tsfile common_obj compress_obj cwrapper_obj file_obj read_obj write_obj -lgcov)
    endif()
else()
    message("Disable code cov...")
    if (ENABLE_ANTLR4)
        target_link_libraries(tsfile common_obj compress_obj cwrapper_obj file_obj read_obj write_obj parser_obj)
    else()
        target_link_libraries(tsfile common_obj compress_obj cwrapper_obj file_obj read_obj write_obj)
    endif()
endif()

add_dependencies(tsfile utils_obj encoding_obj)

set(LIBTSFILE_PROJECT_VERSION ${TsFile_CPP_VERSION})
set(LIBTSFILE_SO_VERSION ${TsFile_CPP_VERSION})
set_target_properties(tsfile PROPERTIES VERSION ${LIBTSFILE_PROJECT_VERSION})
set_target_properties(tsfile PROPERTIES SOVERSION ${LIBTSFILE_SO_VERSION})

install(TARGETS tsfile LIBRARY DESTINATION ${LIBRARY_OUTPUT_PATH})