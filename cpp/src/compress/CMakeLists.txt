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
set(compress_SRC_LIST "compressor.cc")

# Initialize header file list
set(HEADERS "")

# Conditionally add Snappy compression
if(ENABLE_SNAPPY)
    list(APPEND compress_SRC_LIST "snappy_compressor.cc")
    list(APPEND HEADERS "snappy_compressor.h")
endif()

# Conditionally add LZ4 compression
if(ENABLE_LZ4)
    list(APPEND compress_SRC_LIST "lz4_compressor.cc")
    list(APPEND HEADERS "lz4_compressor.h")
endif()

# Conditionally add LZO-Kay compression
if(ENABLE_LZOKAY)
    list(APPEND compress_SRC_LIST "lzo_compressor.cc")
    list(APPEND HEADERS "lzo_compressor.h")
endif()

# Conditionally add Zlib compression
if(ENABLE_ZLIB)
    list(APPEND compress_SRC_LIST "gzip_compressor.cc")
    list(APPEND HEADERS "gzip_compressor.h")
endif()

# Add common headers (always copied)
file(GLOB COMMON_HEADERS "*.h")
foreach(header IN LISTS HEADERS)
    list(REMOVE_ITEM COMMON_HEADERS ${header})
endforeach()

list(APPEND HEADERS ${COMMON_HEADERS})

add_library(compress_obj OBJECT ${compress_SRC_LIST})

# make header paths absolute (relative to this CMakeLists.txt) ---
set(ABS_HEADERS "")
foreach(h IN LISTS HEADERS)
    if(IS_ABSOLUTE "${h}")
        list(APPEND ABS_HEADERS "${h}")
    else()
        # prepend current source dir so copy_to_dir sees the real file path
        list(APPEND ABS_HEADERS "${CMAKE_CURRENT_SOURCE_DIR}/${h}")
    endif()
endforeach()
# call copy function with absolute/qualified header paths
copy_to_dir(${ABS_HEADERS} "compress_obj")