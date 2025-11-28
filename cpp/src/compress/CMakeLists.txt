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

message("Running in src/storage/tsfile/compress directory")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Initialize source file list
set(compress_SRC_LIST "")

# Add common source files (always compiled if any algorithm is enabled)
list(APPEND compress_SRC_LIST "")

# Conditionally add Snappy compression
if(ENABLE_SNAPPY)
    list(APPEND compress_SRC_LIST "snappy_compressor.cc")
    add_definitions(-DENABLE_SNAPPY)
    message(STATUS "Snappy compression enabled")
endif()

# Conditionally add LZ4 compression
if(ENABLE_LZ4)
    list(APPEND compress_SRC_LIST "lz4_compressor.cc")
    add_definitions(-DENABLE_LZ4)
    message(STATUS "LZ4 compression enabled")
endif()

# Conditionally add LZO-Kay compression
if(ENABLE_LZOKAY)
    list(APPEND compress_SRC_LIST "lzo_compressor.cc")
    add_definitions(-DENABLE_LZOKAY)
    message(STATUS "LZO-Kay compression enabled")
endif()

# Conditionally add Zlib compression
if(ENABLE_ZLIB)
    list(APPEND compress_SRC_LIST "gzip_compressor.cc")
    add_definitions(-DENABLE_ZLIB)
    message(STATUS "Zlib compression enabled")
endif()
add_library(compress_obj OBJECT ${compress_SRC_LIST})
file(GLOB HEADERS "${CMAKE_CURRENT_SOURCE_DIR}/*.h")
copy_to_dir(${HEADERS} "compress_obj")
