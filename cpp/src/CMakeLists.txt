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
set(TSFILE_PUBLIC_THIRD_PARTY_INCLUDE_DIRS "")

if (ENABLE_SNAPPY)
    set(TSFILE_SNAPPY_INCLUDE_DIRS
            ${THIRD_PARTY_INCLUDE}/google_snappy
            ${CMAKE_SOURCE_DIR}/third_party/google_snappy
    )
    list(APPEND PROJECT_INCLUDE_DIR
            ${TSFILE_SNAPPY_INCLUDE_DIRS}
    )
    list(APPEND TSFILE_PUBLIC_THIRD_PARTY_INCLUDE_DIRS
            ${TSFILE_SNAPPY_INCLUDE_DIRS}
    )
endif()

if (ENABLE_LZ4)
    set(TSFILE_LZ4_INCLUDE_DIRS
            ${CMAKE_SOURCE_DIR}/third_party/lz4
    )
    list(APPEND PROJECT_INCLUDE_DIR
            ${TSFILE_LZ4_INCLUDE_DIRS}
    )
    list(APPEND TSFILE_PUBLIC_THIRD_PARTY_INCLUDE_DIRS
            ${TSFILE_LZ4_INCLUDE_DIRS}
    )
endif()

if (ENABLE_LZOKAY)
    set(TSFILE_LZOKAY_INCLUDE_DIRS
            ${CMAKE_SOURCE_DIR}/third_party/lzokay
    )
    list(APPEND PROJECT_INCLUDE_DIR
            ${TSFILE_LZOKAY_INCLUDE_DIRS}
    )
    list(APPEND TSFILE_PUBLIC_THIRD_PARTY_INCLUDE_DIRS
            ${TSFILE_LZOKAY_INCLUDE_DIRS}
    )
endif()

if (ENABLE_ZLIB)
    set(TSFILE_ZLIB_INCLUDE_DIRS
            ${CMAKE_SOURCE_DIR}/third_party/zlib-1.3.1
            ${THIRD_PARTY_INCLUDE}/zlib-1.3.1
    )
    list(APPEND PROJECT_INCLUDE_DIR
            # zlib.h ships in the source tree; zconf.h is generated into the
            # build tree by zlib's own CMake, so both directories are needed.
            ${TSFILE_ZLIB_INCLUDE_DIRS}
    )
    list(APPEND TSFILE_PUBLIC_THIRD_PARTY_INCLUDE_DIRS
            ${TSFILE_ZLIB_INCLUDE_DIRS}
    )
endif()

if (ENABLE_ANTLR4)
    list(APPEND PROJECT_INCLUDE_DIR
            ${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src
    )
    message("Adding ANTLR4 include directory")
endif()

if (ENABLE_SIMD)
    add_definitions(-DENABLE_SIMD)
    list(APPEND PROJECT_INCLUDE_DIR
            ${CMAKE_SOURCE_DIR}/third_party/simde-0.8.4-rc3
    )
endif()

include_directories(${PROJECT_INCLUDE_DIR})

# Mark every translation unit that is compiled into the tsfile library so that
# TSFILE_API (see utils/util_define.h) resolves to an export-side (empty)
# decoration here, and to __declspec(dllimport) for external consumers.
add_definitions(-DTSFILE_BUILDING)

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
target_include_directories(tsfile PUBLIC
        $<BUILD_INTERFACE:${LIBRARY_INCLUDE_DIR}>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/tsfile>)
foreach(include_dir IN LISTS TSFILE_PUBLIC_THIRD_PARTY_INCLUDE_DIRS)
    target_include_directories(tsfile PUBLIC $<BUILD_INTERFACE:${include_dir}>)
endforeach()

if (${COV_ENABLED})
    message("Enable code cov...")
    # Apple clang ships coverage runtime via --coverage; libgcov isn't a
    # standalone library on macOS.  Use --coverage there.
    if (APPLE)
        set(COV_LINK_LIB --coverage)
    else()
        set(COV_LINK_LIB -lgcov)
    endif()
    if (ENABLE_ANTLR4)
        target_link_libraries(tsfile PRIVATE common_obj compress_obj cwrapper_obj file_obj read_obj write_obj parser_obj ${COV_LINK_LIB})
    else()
        target_link_libraries(tsfile PRIVATE common_obj compress_obj cwrapper_obj file_obj read_obj write_obj ${COV_LINK_LIB})
    endif()
else()
    message("Disable code cov...")
    if (ENABLE_ANTLR4)
        target_link_libraries(tsfile PRIVATE common_obj compress_obj cwrapper_obj file_obj read_obj write_obj parser_obj)
    else()
        target_link_libraries(tsfile PRIVATE common_obj compress_obj cwrapper_obj file_obj read_obj write_obj)
    endif()
endif()

add_dependencies(tsfile utils_obj encoding_obj)

set(LIBTSFILE_PROJECT_VERSION ${TsFile_CPP_VERSION})
set(LIBTSFILE_SO_VERSION ${TsFile_CPP_VERSION})
set_target_properties(tsfile PROPERTIES VERSION ${LIBTSFILE_PROJECT_VERSION})
set_target_properties(tsfile PROPERTIES SOVERSION ${LIBTSFILE_SO_VERSION})

# On Windows a SHARED library produces a .dll (RUNTIME) plus an import .lib
# (ARCHIVE); on Unix it produces a .so (LIBRARY). Cover all three so the
# install step works for every platform and for CPack-based system packages.
install(TARGETS tsfile
        EXPORT TsFileTargets
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT runtime
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT runtime
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        COMPONENT development)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/src/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/tsfile
        COMPONENT development
        FILES_MATCHING PATTERN "*.h")

include(CMakePackageConfigHelpers)
set(TSFILE_CMAKE_INSTALL_DIR ${CMAKE_INSTALL_LIBDIR}/cmake/tsfile)
configure_package_config_file(
        ${CMAKE_SOURCE_DIR}/cmake/TsFileConfig.cmake.in
        ${CMAKE_CURRENT_BINARY_DIR}/TsFileConfig.cmake
        INSTALL_DESTINATION ${TSFILE_CMAKE_INSTALL_DIR})
write_basic_package_version_file(
        ${CMAKE_CURRENT_BINARY_DIR}/TsFileConfigVersion.cmake
        VERSION ${TsFile_CPP_VERSION}
        COMPATIBILITY SameMajorVersion)
install(EXPORT TsFileTargets
        NAMESPACE TsFile::
        DESTINATION ${TSFILE_CMAKE_INSTALL_DIR}
        COMPONENT development)
install(FILES
        ${CMAKE_CURRENT_BINARY_DIR}/TsFileConfig.cmake
        ${CMAKE_CURRENT_BINARY_DIR}/TsFileConfigVersion.cmake
        DESTINATION ${TSFILE_CMAKE_INSTALL_DIR}
        COMPONENT development)
