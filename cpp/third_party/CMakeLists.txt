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
    if (TSFILE_ANTLR4_SOURCE STREQUAL "BUNDLED")
        include(${CMAKE_SOURCE_DIR}/cmake/ANTLR4Source.cmake)

        file(GLOB _TSFILE_ANTLR4_SOURCES
                "${TSFILE_ANTLR4_SOURCE_DIR}/runtime/Cpp/runtime/src/*.cpp"
                "${TSFILE_ANTLR4_SOURCE_DIR}/runtime/Cpp/runtime/src/atn/*.cpp"
                "${TSFILE_ANTLR4_SOURCE_DIR}/runtime/Cpp/runtime/src/dfa/*.cpp"
                "${TSFILE_ANTLR4_SOURCE_DIR}/runtime/Cpp/runtime/src/misc/*.cpp"
                "${TSFILE_ANTLR4_SOURCE_DIR}/runtime/Cpp/runtime/src/support/*.cpp"
                "${TSFILE_ANTLR4_SOURCE_DIR}/runtime/Cpp/runtime/src/tree/*.cpp"
                "${TSFILE_ANTLR4_SOURCE_DIR}/runtime/Cpp/runtime/src/tree/pattern/*.cpp"
                "${TSFILE_ANTLR4_SOURCE_DIR}/runtime/Cpp/runtime/src/tree/xpath/*.cpp")

        add_library(tsfile_utf8cpp_bundled INTERFACE)
        target_include_directories(tsfile_utf8cpp_bundled INTERFACE
                "${TSFILE_UTF8CPP_SOURCE_DIR}/source")

        add_library(tsfile_antlr4_bundled STATIC ${_TSFILE_ANTLR4_SOURCES})
        set_target_properties(tsfile_antlr4_bundled PROPERTIES
                POSITION_INDEPENDENT_CODE ON
                CXX_STANDARD 11)
        target_include_directories(tsfile_antlr4_bundled PUBLIC
                "${TSFILE_ANTLR4_SOURCE_DIR}/runtime/Cpp/runtime/src")
        target_compile_definitions(tsfile_antlr4_bundled PUBLIC
                ANTLR4CPP_STATIC
                USE_UTF8_INSTEAD_OF_CODECVT)
        target_link_libraries(tsfile_antlr4_bundled PUBLIC
                tsfile_utf8cpp_bundled)

        if (MSVC)
            target_compile_options(tsfile_antlr4_bundled PRIVATE /wd4251)
        else ()
            target_compile_options(tsfile_antlr4_bundled PRIVATE
                    -Wno-overloaded-virtual)
            if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
                target_compile_options(tsfile_antlr4_bundled PRIVATE
                        -Wno-dollar-in-identifier-extension
                        -Wno-four-char-constants)
            elseif (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Intel")
                target_compile_options(tsfile_antlr4_bundled PRIVATE
                        -Wno-multichar)
            endif ()
        endif ()

        if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
            find_library(TSFILE_UUID_LIBRARY NAMES uuid)
            if (NOT TSFILE_UUID_LIBRARY)
                message(FATAL_ERROR
                        "The bundled ANTLR4 runtime requires libuuid on Linux.")
            endif ()
            target_link_libraries(tsfile_antlr4_bundled PUBLIC
                    "${TSFILE_UUID_LIBRARY}")
        elseif (APPLE)
            find_library(TSFILE_COREFOUNDATION_LIBRARY CoreFoundation)
            if (NOT TSFILE_COREFOUNDATION_LIBRARY)
                message(FATAL_ERROR
                        "The bundled ANTLR4 runtime requires CoreFoundation "
                        "on Apple platforms.")
            endif ()
            target_link_libraries(tsfile_antlr4_bundled PUBLIC
                    "${TSFILE_COREFOUNDATION_LIBRARY}")
        elseif (WIN32)
            target_link_libraries(tsfile_antlr4_bundled PUBLIC ole32)
        endif ()
        unset(_TSFILE_ANTLR4_SOURCES)
    elseif ("${TSFILE_ANTLR4_SYSTEM_TARGET}" STREQUAL "")
        message(FATAL_ERROR
                "System ANTLR4 was selected, but no runtime target is "
                "available.")
    endif ()

    add_library(tsfile_antlr4 INTERFACE)
    if (TSFILE_ANTLR4_SOURCE STREQUAL "BUNDLED")
        target_link_libraries(tsfile_antlr4 INTERFACE
                tsfile_antlr4_bundled)
    else ()
        target_link_libraries(tsfile_antlr4 INTERFACE
                ${TSFILE_ANTLR4_SYSTEM_TARGET})
        if (TSFILE_ANTLR4_SYSTEM_INCLUDE_DIR)
            target_include_directories(tsfile_antlr4 INTERFACE
                    "${TSFILE_ANTLR4_SYSTEM_INCLUDE_DIR}")
        endif ()
        if (TSFILE_ANTLR4_SYSTEM_STATIC)
            target_compile_definitions(tsfile_antlr4 INTERFACE
                    ANTLR4CPP_STATIC)
        endif ()
    endif ()
    add_library(TsFile::ANTLR4 ALIAS tsfile_antlr4)
    message("ANTLR4 runtime is enabled (${TSFILE_ANTLR4_SOURCE})")
else()
    message("ANTLR4 runtime is disabled")
endif()
if (ENABLE_SNAPPY)
    if (TSFILE_SNAPPY_SOURCE STREQUAL "BUNDLED")
        include(${CMAKE_SOURCE_DIR}/cmake/SnappySource.cmake)

        set(SNAPPY_BUILD_TESTS OFF CACHE BOOL
                "Disable tests in the TsFile-managed Snappy build" FORCE)
        set(SNAPPY_BUILD_BENCHMARKS OFF CACHE BOOL
                "Disable benchmarks in the TsFile-managed Snappy build" FORCE)
        set(SNAPPY_INSTALL OFF CACHE BOOL
                "Disable installation from the TsFile-managed Snappy build"
                FORCE)

        # Always embed a position-independent static Snappy library in TsFile,
        # while preserving a caller's global BUILD_SHARED_LIBS cache value.
        get_property(_TSFILE_BUILD_SHARED_LIBS_WAS_SET
                CACHE BUILD_SHARED_LIBS PROPERTY TYPE SET)
        if (_TSFILE_BUILD_SHARED_LIBS_WAS_SET)
            get_property(_TSFILE_BUILD_SHARED_LIBS_VALUE
                    CACHE BUILD_SHARED_LIBS PROPERTY VALUE)
        endif ()
        set(BUILD_SHARED_LIBS OFF CACHE BOOL
                "Build the TsFile-managed Snappy library statically" FORCE)
        add_subdirectory(
                "${TSFILE_SNAPPY_SOURCE_DIR}"
                "${CMAKE_BINARY_DIR}/third_party/snappy"
                EXCLUDE_FROM_ALL)
        if (_TSFILE_BUILD_SHARED_LIBS_WAS_SET)
            set(BUILD_SHARED_LIBS ${_TSFILE_BUILD_SHARED_LIBS_VALUE}
                    CACHE BOOL "Build shared libraries" FORCE)
        else ()
            unset(BUILD_SHARED_LIBS CACHE)
        endif ()
        unset(_TSFILE_BUILD_SHARED_LIBS_VALUE)
        unset(_TSFILE_BUILD_SHARED_LIBS_WAS_SET)

        set_target_properties(snappy PROPERTIES
                POSITION_INDEPENDENT_CODE ON)
    elseif (NOT TARGET Snappy::snappy)
        message(FATAL_ERROR
                "System Snappy was selected, but the Snappy::snappy target "
                "is missing.")
    endif ()

    add_library(tsfile_snappy INTERFACE)
    if (TSFILE_SNAPPY_SOURCE STREQUAL "BUNDLED")
        target_link_libraries(tsfile_snappy INTERFACE snappy)
    else ()
        target_link_libraries(tsfile_snappy INTERFACE Snappy::snappy)
    endif ()
    add_library(TsFile::Snappy ALIAS tsfile_snappy)
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
    if (TSFILE_LZOKAY_SOURCE STREQUAL "BUNDLED")
        include(${CMAKE_SOURCE_DIR}/cmake/LZOKAYSource.cmake)
        add_library(tsfile_lzokay_bundled STATIC
                "${TSFILE_LZOKAY_SOURCE_DIR}/lzokay.cpp")
        set_target_properties(tsfile_lzokay_bundled PROPERTIES
                POSITION_INDEPENDENT_CODE ON
                CXX_STANDARD 14
                CXX_STANDARD_REQUIRED ON)
        if (CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(tsfile_lzokay_bundled PRIVATE
                    -Wno-unused-variable)
        endif ()
        target_include_directories(tsfile_lzokay_bundled PUBLIC
                "${TSFILE_LZOKAY_SOURCE_DIR}")
    elseif (NOT TARGET lzokay::lzokay)
        message(FATAL_ERROR
                "System lzokay was selected, but the lzokay::lzokay target "
                "is missing.")
    endif ()

    add_library(tsfile_lzokay INTERFACE)
    if (TSFILE_LZOKAY_SOURCE STREQUAL "BUNDLED")
        target_link_libraries(tsfile_lzokay INTERFACE
                tsfile_lzokay_bundled)
    else ()
        target_link_libraries(tsfile_lzokay INTERFACE lzokay::lzokay)
    endif ()
    add_library(TsFile::LZOKAY ALIAS tsfile_lzokay)
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

if (ENABLE_ZSTD)
    if (TSFILE_ZSTD_SOURCE STREQUAL "BUNDLED")
        include(${CMAKE_SOURCE_DIR}/cmake/ZSTDSource.cmake)
        set(ZSTD_BUILD_PROGRAMS OFF CACHE BOOL
                "Disable programs in the TsFile-managed Zstandard build"
                FORCE)
        set(ZSTD_BUILD_TESTS OFF CACHE BOOL
                "Disable tests in the TsFile-managed Zstandard build" FORCE)
        set(ZSTD_BUILD_CONTRIB OFF CACHE BOOL
                "Disable contrib in the TsFile-managed Zstandard build"
                FORCE)
        set(ZSTD_BUILD_STATIC ON CACHE BOOL
                "Build the TsFile-managed Zstandard library statically"
                FORCE)
        set(ZSTD_BUILD_SHARED OFF CACHE BOOL
                "Do not build a shared TsFile-managed Zstandard library"
                FORCE)
        set(ZSTD_BUILD_DICTBUILDER OFF CACHE BOOL
                "Disable dictionary builder in TsFile-managed Zstandard"
                FORCE)
        set(ZSTD_BUILD_DEPRECATED OFF CACHE BOOL
                "Disable deprecated TsFile-managed Zstandard APIs" FORCE)
        set(ZSTD_LEGACY_SUPPORT OFF CACHE BOOL
                "Disable legacy frame support in TsFile-managed Zstandard"
                FORCE)
        set(ZSTD_MULTITHREAD_SUPPORT OFF CACHE BOOL
                "Disable Zstandard-internal threading in TsFile" FORCE)
        add_subdirectory(
                "${TSFILE_ZSTD_SOURCE_DIR}/build/cmake"
                "${CMAKE_BINARY_DIR}/third_party/zstd"
                EXCLUDE_FROM_ALL)
        set_target_properties(libzstd_static PROPERTIES
                POSITION_INDEPENDENT_CODE ON)
    elseif ("${TSFILE_ZSTD_SYSTEM_TARGET}" STREQUAL "")
        message(FATAL_ERROR
                "System Zstandard was selected, but no usable target is "
                "available.")
    endif ()

    add_library(tsfile_zstd INTERFACE)
    if (TSFILE_ZSTD_SOURCE STREQUAL "BUNDLED")
        target_link_libraries(tsfile_zstd INTERFACE libzstd_static)
    else ()
        target_link_libraries(tsfile_zstd INTERFACE
                ${TSFILE_ZSTD_SYSTEM_TARGET})
    endif ()
    add_library(TsFile::ZSTD ALIAS tsfile_zstd)
endif ()

if (ENABLE_LZMA2)
    if (TSFILE_LIBLZMA_SOURCE STREQUAL "BUNDLED")
        if (CMAKE_VERSION VERSION_LESS "3.20")
            message(FATAL_ERROR
                    "ENABLE_LZMA2=ON with the verified bundled XZ Utils 5.8.3 "
                    "build requires CMake 3.20 or newer. TsFile retains a "
                    "CMake 3.11 baseline for legacy industrial toolchains, "
                    "so LZMA2 is disabled by default. Disable LZMA2 or use "
                    "TSFILE_DEPENDENCY_SOURCE=SYSTEM with a compatible "
                    "liblzma package when using older CMake.")
        endif ()
        include(${CMAKE_SOURCE_DIR}/cmake/LibLZMASource.cmake)

        # Build only the 0BSD liblzma LZMA1/LZMA2 implementation. XZ's CLI,
        # scripts, documentation, tests, and GNU getopt replacement are not
        # linked into this target.
        set(XZ_NLS OFF CACHE BOOL "Disable XZ translations" FORCE)
        set(XZ_THREADS no CACHE STRING
                "Disable liblzma-internal threading in TsFile" FORCE)
        set(XZ_CHECKS "crc32;crc64" CACHE STRING
                "Checks enabled in the TsFile-managed liblzma" FORCE)
        set(XZ_ENCODERS "lzma1;lzma2" CACHE STRING
                "Encoders enabled in the TsFile-managed liblzma" FORCE)
        set(XZ_DECODERS "lzma1;lzma2" CACHE STRING
                "Decoders enabled in the TsFile-managed liblzma" FORCE)
        set(XZ_MICROLZMA_ENCODER OFF CACHE BOOL
                "Disable MicroLZMA encoding in TsFile" FORCE)
        set(XZ_MICROLZMA_DECODER OFF CACHE BOOL
                "Disable MicroLZMA decoding in TsFile" FORCE)
        set(XZ_LZIP_DECODER OFF CACHE BOOL
                "Disable lzip decoding in TsFile" FORCE)
        set(XZ_TOOL_XZ OFF CACHE BOOL "Do not build the xz CLI" FORCE)
        set(XZ_TOOL_XZDEC OFF CACHE BOOL "Do not build xzdec" FORCE)
        set(XZ_TOOL_LZMADEC OFF CACHE BOOL "Do not build lzmadec" FORCE)
        set(XZ_TOOL_LZMAINFO OFF CACHE BOOL "Do not build lzmainfo" FORCE)
        set(XZ_TOOL_SCRIPTS OFF CACHE BOOL
                "Do not install XZ's GPL scripts" FORCE)
        set(XZ_DOXYGEN OFF CACHE BOOL "Do not build XZ API docs" FORCE)
        set(XZ_DOC OFF CACHE BOOL "Do not install XZ documentation" FORCE)
        set(XZ_SANDBOX no CACHE STRING
                "Disable sandbox probes for disabled XZ tools" FORCE)
        set(XZ_SYMBOL_VERSIONING no CACHE STRING
                "Disable symbol versioning for static liblzma" FORCE)

        get_property(_TSFILE_BUILD_SHARED_LIBS_WAS_SET
                CACHE BUILD_SHARED_LIBS PROPERTY TYPE SET)
        if (_TSFILE_BUILD_SHARED_LIBS_WAS_SET)
            get_property(_TSFILE_BUILD_SHARED_LIBS_VALUE
                    CACHE BUILD_SHARED_LIBS PROPERTY VALUE)
        endif ()
        get_property(_TSFILE_BUILD_TESTING_WAS_SET
                CACHE BUILD_TESTING PROPERTY TYPE SET)
        if (_TSFILE_BUILD_TESTING_WAS_SET)
            get_property(_TSFILE_BUILD_TESTING_VALUE
                    CACHE BUILD_TESTING PROPERTY VALUE)
        endif ()
        set(BUILD_SHARED_LIBS OFF CACHE BOOL
                "Build the TsFile-managed liblzma statically" FORCE)
        set(BUILD_TESTING OFF CACHE BOOL
                "Disable tests in the TsFile-managed XZ build" FORCE)
        add_subdirectory(
                "${TSFILE_LIBLZMA_SOURCE_DIR}"
                "${CMAKE_BINARY_DIR}/third_party/xz"
                EXCLUDE_FROM_ALL)
        if (_TSFILE_BUILD_SHARED_LIBS_WAS_SET)
            set(BUILD_SHARED_LIBS ${_TSFILE_BUILD_SHARED_LIBS_VALUE}
                    CACHE BOOL "Build shared libraries" FORCE)
        else ()
            unset(BUILD_SHARED_LIBS CACHE)
        endif ()
        if (_TSFILE_BUILD_TESTING_WAS_SET)
            set(BUILD_TESTING ${_TSFILE_BUILD_TESTING_VALUE}
                    CACHE BOOL "Build tests" FORCE)
        else ()
            unset(BUILD_TESTING CACHE)
        endif ()
        unset(_TSFILE_BUILD_SHARED_LIBS_VALUE)
        unset(_TSFILE_BUILD_SHARED_LIBS_WAS_SET)
        unset(_TSFILE_BUILD_TESTING_VALUE)
        unset(_TSFILE_BUILD_TESTING_WAS_SET)

        set_target_properties(liblzma PROPERTIES
                POSITION_INDEPENDENT_CODE ON)
        get_target_property(_TSFILE_LIBLZMA_SOURCES liblzma SOURCES)
        foreach (_TSFILE_LIBLZMA_SOURCE IN LISTS _TSFILE_LIBLZMA_SOURCES)
            if (_TSFILE_LIBLZMA_SOURCE MATCHES "^\\$<")
                message(FATAL_ERROR
                        "Cannot verify the license of generated liblzma "
                        "source expression: ${_TSFILE_LIBLZMA_SOURCE}")
            elseif (IS_ABSOLUTE "${_TSFILE_LIBLZMA_SOURCE}")
                set(_TSFILE_LIBLZMA_SOURCE_FILE
                        "${_TSFILE_LIBLZMA_SOURCE}")
            else ()
                set(_TSFILE_LIBLZMA_SOURCE_FILE
                        "${TSFILE_LIBLZMA_SOURCE_DIR}/${_TSFILE_LIBLZMA_SOURCE}")
            endif ()
            if (EXISTS "${_TSFILE_LIBLZMA_SOURCE_FILE}")
                file(READ "${_TSFILE_LIBLZMA_SOURCE_FILE}"
                        _TSFILE_LIBLZMA_LICENSE_HEADER LIMIT 512)
                if (NOT _TSFILE_LIBLZMA_LICENSE_HEADER MATCHES
                        "SPDX-License-Identifier: 0BSD")
                    message(FATAL_ERROR
                            "The TsFile-managed liblzma target contains a "
                            "source that is not marked 0BSD: "
                            "${_TSFILE_LIBLZMA_SOURCE_FILE}")
                endif ()
            else ()
                message(FATAL_ERROR
                        "Cannot verify missing liblzma target source: "
                        "${_TSFILE_LIBLZMA_SOURCE_FILE}")
            endif ()
        endforeach ()
        unset(_TSFILE_LIBLZMA_LICENSE_HEADER)
        unset(_TSFILE_LIBLZMA_SOURCE)
        unset(_TSFILE_LIBLZMA_SOURCE_FILE)
        unset(_TSFILE_LIBLZMA_SOURCES)
        get_target_property(_TSFILE_LIBLZMA_LINK_LIBRARIES
                liblzma LINK_LIBRARIES)
        if (_TSFILE_LIBLZMA_LINK_LIBRARIES MATCHES
                "(^|;)libgnu(;|$)")
            message(FATAL_ERROR
                    "The TsFile-managed liblzma target must not link XZ's "
                    "LGPL-licensed GNU getopt replacement.")
        endif ()
        unset(_TSFILE_LIBLZMA_LINK_LIBRARIES)
    elseif ("${TSFILE_LIBLZMA_SYSTEM_TARGET}" STREQUAL "")
        message(FATAL_ERROR
                "System liblzma was selected, but no usable target is "
                "available.")
    endif ()

    add_library(tsfile_liblzma INTERFACE)
    if (TSFILE_LIBLZMA_SOURCE STREQUAL "BUNDLED")
        target_link_libraries(tsfile_liblzma INTERFACE liblzma)
        # XZ 5.8.3 marks its source-tree API include directory PRIVATE on the
        # liblzma target. Expose only the public API directory through TsFile's
        # facade so consumers can include <lzma.h> without depending on the
        # downloaded archive layout themselves.
        target_include_directories(tsfile_liblzma INTERFACE
                "${TSFILE_LIBLZMA_SOURCE_DIR}/src/liblzma/api")
    else ()
        target_link_libraries(tsfile_liblzma INTERFACE
                ${TSFILE_LIBLZMA_SYSTEM_TARGET})
    endif ()
    add_library(TsFile::LibLZMA ALIAS tsfile_liblzma)
endif ()

if (ENABLE_SIMD)
    if (TSFILE_SIMDE_SOURCE STREQUAL "BUNDLED")
        include(${CMAKE_SOURCE_DIR}/cmake/SIMDeSource.cmake)
    elseif (NOT TARGET simde::simde)
        message(FATAL_ERROR
                "System SIMDe was selected, but the simde::simde target "
                "is missing.")
    endif ()

    add_library(tsfile_simde INTERFACE)
    if (TSFILE_SIMDE_SOURCE STREQUAL "BUNDLED")
        target_include_directories(tsfile_simde INTERFACE
                "${TSFILE_SIMDE_SOURCE_DIR}")
    else ()
        target_link_libraries(tsfile_simde INTERFACE simde::simde)
    endif ()
    add_library(TsFile::SIMDe ALIAS tsfile_simde)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
