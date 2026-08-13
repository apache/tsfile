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
    if (TSFILE_LZ4_SOURCE STREQUAL "BUNDLED")
        include(${CMAKE_SOURCE_DIR}/cmake/LZ4Source.cmake)
        add_library(tsfile_lz4_bundled STATIC
                "${TSFILE_LZ4_SOURCE_DIR}/lib/lz4.c")
        set_target_properties(tsfile_lz4_bundled PROPERTIES
                POSITION_INDEPENDENT_CODE ON)
        target_include_directories(tsfile_lz4_bundled PUBLIC
                "${TSFILE_LZ4_SOURCE_DIR}/lib")
    elseif (NOT TARGET LZ4::LZ4)
        message(FATAL_ERROR
                "System LZ4 was selected, but the LZ4::LZ4 target is missing.")
    endif ()

    add_library(tsfile_lz4 INTERFACE)
    if (TSFILE_LZ4_SOURCE STREQUAL "BUNDLED")
        target_link_libraries(tsfile_lz4 INTERFACE tsfile_lz4_bundled)
    else ()
        target_link_libraries(tsfile_lz4 INTERFACE LZ4::LZ4)
    endif ()
    add_library(TsFile::LZ4 ALIAS tsfile_lz4)
endif()

if (ENABLE_LZOKAY)
    add_subdirectory(lzokay)
endif()

if (ENABLE_ZLIB)
    if (TSFILE_ZLIB_SOURCE STREQUAL "BUNDLED")
        include(${CMAKE_SOURCE_DIR}/cmake/ZLIBSource.cmake)
        set(ZLIB_BUILD_EXAMPLES OFF CACHE BOOL
                "Disable examples in the TsFile-managed zlib build" FORCE)
        set(SKIP_INSTALL_ALL ON)
        add_subdirectory(
                "${TSFILE_ZLIB_SOURCE_DIR}"
                "${CMAKE_BINARY_DIR}/third_party/zlib"
                EXCLUDE_FROM_ALL)
        unset(SKIP_INSTALL_ALL)
        set_target_properties(zlibstatic PROPERTIES
                POSITION_INDEPENDENT_CODE ON)
    elseif (NOT TARGET ZLIB::ZLIB)
        message(FATAL_ERROR
                "System zlib was selected, but the ZLIB::ZLIB target is missing.")
    endif ()

    add_library(tsfile_zlib INTERFACE)
    if (TSFILE_ZLIB_SOURCE STREQUAL "BUNDLED")
        target_link_libraries(tsfile_zlib INTERFACE zlibstatic)
    else ()
        target_link_libraries(tsfile_zlib INTERFACE ZLIB::ZLIB)
    endif ()
    add_library(TsFile::ZLIB ALIAS tsfile_zlib)
endif()

if (ENABLE_SIMD)
    add_subdirectory(simde-0.8.4-rc3)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
