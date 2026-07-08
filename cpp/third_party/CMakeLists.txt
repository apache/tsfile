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
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if (ENABLE_ANTLR4)
    # Build the ANTLR4 runtime against the dynamic CRT (/MD) so it links the
    # same C runtime as the rest of the project (avoiding LNK2038 mismatches).
    # ANTLR4's own option is named WITH_STATIC_CRT and defaults to ON (=> /MT);
    # pre-seeding it in the cache makes its option() call a no-op.
    set(WITH_STATIC_CRT OFF CACHE BOOL
        "Link the ANTLR4 runtime against the dynamic CRT" FORCE)
    add_subdirectory(antlr4-cpp-runtime-4)
    message("ANTLR4 runtime is enabled")
else()
    message("ANTLR4 runtime is disabled")
endif()
if (ENABLE_SNAPPY)
    add_subdirectory(google_snappy)
endif()

if (ENABLE_LZ4)
    add_subdirectory(lz4)
endif()

if (ENABLE_LZOKAY)
    add_subdirectory(lzokay)
endif()

if (ENABLE_ZLIB)
    add_subdirectory(zlib-1.3.1)
endif()

if (ENABLE_ZSTD)
    set(ZSTD_BUILD_STATIC ON CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_SHARED OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_CONTRIB OFF CACHE BOOL "" FORCE)
    set(ZSTD_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(ZSTD_LEGACY_SUPPORT OFF CACHE BOOL "" FORCE)
    set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE BOOL "" FORCE)
    add_subdirectory(zstd-1.5.7/build/cmake)
endif()

if (ENABLE_LZMA2)
    if (CMAKE_VERSION VERSION_LESS 3.20)
        message(FATAL_ERROR "ENABLE_LZMA2 requires CMake 3.20 or newer because bundled XZ Utils 5.8.3 requires it")
    endif()
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
    set(XZ_NLS OFF CACHE BOOL "" FORCE)
    set(XZ_THREADS "no" CACHE STRING "" FORCE)
    set(XZ_ENCODERS "lzma1;lzma2" CACHE STRING "" FORCE)
    set(XZ_DECODERS "lzma1;lzma2" CACHE STRING "" FORCE)
    set(XZ_MATCH_FINDERS "bt4" CACHE STRING "" FORCE)
    set(XZ_CHECKS "crc64" CACHE STRING "" FORCE)
    set(XZ_MICROLZMA_ENCODER OFF CACHE BOOL "" FORCE)
    set(XZ_MICROLZMA_DECODER OFF CACHE BOOL "" FORCE)
    set(XZ_LZIP_DECODER OFF CACHE BOOL "" FORCE)
    set(XZ_TOOL_XZ OFF CACHE BOOL "" FORCE)
    set(XZ_TOOL_XZDEC OFF CACHE BOOL "" FORCE)
    set(XZ_TOOL_LZMADEC OFF CACHE BOOL "" FORCE)
    set(XZ_TOOL_LZMAINFO OFF CACHE BOOL "" FORCE)
    set(XZ_TOOL_SCRIPTS OFF CACHE BOOL "" FORCE)
    set(XZ_DOC OFF CACHE BOOL "" FORCE)
    set(XZ_DOXYGEN OFF CACHE BOOL "" FORCE)
    set(XZ_SANDBOX "no" CACHE STRING "" FORCE)
    set(_tsfile_build_testing "${BUILD_TESTING}")
    set(BUILD_TESTING OFF)
    add_subdirectory(xz-5.8.3)
    set_target_properties(liblzma PROPERTIES POSITION_INDEPENDENT_CODE ON)
    set(BUILD_TESTING "${_tsfile_build_testing}")
    unset(_tsfile_build_testing)
endif()

if (ENABLE_SIMD)
    add_subdirectory(simde-0.8.4-rc3)
endif()
