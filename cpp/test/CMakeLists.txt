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
project(TsFile_CPP_TEST)

set(CMAKE_VERBOSE_MAKEFILE ON)

set(GTEST_URL_LIST
        "https://github.com/google/googletest/archive/refs/tags/release-1.12.1.zip"
        "https://hub.nuaa.cf/google/googletest/archive/refs/tags/release-1.12.1.zip"
        "https://hub.yzuu.cf/google/googletest/archive/refs/tags/release-1.12.1.zip"
)
set(GTEST_SHA256 "24564e3b712d3eb30ac9a85d92f7d720f60cc0173730ac166f27dda7fed76cb2")

set(GTEST_ZIP_PATH "${CMAKE_SOURCE_DIR}/third_party/googletest-release-1.12.1.zip")
set(DOWNLOADED 0)
set(GTEST_URL "")
set(TIMEOUT 30)

# Trust a cached archive only when it exactly matches the expected upstream file.
set(GTEST_ZIP_LOCAL_VALID 0)
if (EXISTS "${GTEST_ZIP_PATH}")
    file(SHA256 "${GTEST_ZIP_PATH}" GTEST_ZIP_LOCAL_SHA256)
    if ("${GTEST_ZIP_LOCAL_SHA256}" STREQUAL "${GTEST_SHA256}")
        set(GTEST_ZIP_LOCAL_VALID 1)
    else ()
        message(
                WARNING
                "Local googletest zip has an unexpected SHA-256 (${GTEST_ZIP_PATH}); "
                "will try download."
        )
        file(REMOVE "${GTEST_ZIP_PATH}")
    endif ()
endif ()

if (GTEST_ZIP_LOCAL_VALID)
    message(STATUS "Using local gtest zip file: ${GTEST_ZIP_PATH}")
    set(DOWNLOADED 1)
    set(GTEST_URL ${GTEST_ZIP_PATH})
else ()
    message(STATUS "Local gtest zip missing or invalid, trying to download from network...")
endif ()

if (NOT DOWNLOADED)
    foreach (URL ${GTEST_URL_LIST})
        message(STATUS "Trying to download from ${URL}")
        file(DOWNLOAD "${URL}" "${GTEST_ZIP_PATH}" STATUS DOWNLOAD_STATUS TIMEOUT
                ${TIMEOUT} EXPECTED_HASH "SHA256=${GTEST_SHA256}")

        list(GET DOWNLOAD_STATUS 0 DOWNLOAD_RESULT)
        if (${DOWNLOAD_RESULT} EQUAL 0 AND EXISTS "${GTEST_ZIP_PATH}")
            set(DOWNLOADED 1)
            set(GTEST_URL ${GTEST_ZIP_PATH})
            break()
        else ()
            file(REMOVE "${GTEST_ZIP_PATH}")
        endif ()
    endforeach ()
endif ()

if (${DOWNLOADED})
    message(STATUS "Successfully get googletest from ${GTEST_URL}")
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    # Extract GitHub release zip via CMake (top folder googletest-release-1.12.1/).
    # Avoid FetchContent here: deferred populate / wrong extract dir broke configure.
    set(_gtest_stage "${CMAKE_BINARY_DIR}/googletest-extract")
    set(GTEST_SRC_ROOT "${_gtest_stage}/googletest-release-1.12.1")
    if (NOT EXISTS "${GTEST_SRC_ROOT}/CMakeLists.txt")
        file(REMOVE_RECURSE "${_gtest_stage}")
        file(MAKE_DIRECTORY "${_gtest_stage}")
        execute_process(
                COMMAND ${CMAKE_COMMAND} -E tar xf "${GTEST_ZIP_PATH}"
                WORKING_DIRECTORY "${_gtest_stage}"
                RESULT_VARIABLE _gtest_tar_result
        )
        if (NOT _gtest_tar_result EQUAL 0)
            message(FATAL_ERROR "Failed to extract googletest zip: ${GTEST_ZIP_PATH}")
        endif ()
    endif ()
    if (NOT EXISTS "${GTEST_SRC_ROOT}/CMakeLists.txt")
        message(
                FATAL_ERROR
                "googletest zip layout unexpected (missing ${GTEST_SRC_ROOT}/CMakeLists.txt)."
        )
    endif ()
    add_subdirectory("${GTEST_SRC_ROOT}" "${CMAKE_BINARY_DIR}/googletest-build"
            EXCLUDE_FROM_ALL)
    # AppleClang searches /usr/local/include before CMake's generated -isystem
    # paths, so a system-installed GTest can shadow the vendored headers. Force
    # the vendored include dirs ahead for GTest's own sources here; the same is
    # done for the consuming TsFile_Test target below (where header resolution
    # actually matters for the test code).
    foreach (GTEST_TARGET gtest gtest_main gmock gmock_main)
        if (TARGET ${GTEST_TARGET})
            target_include_directories(${GTEST_TARGET} BEFORE PRIVATE
                    ${GTEST_SRC_ROOT}/googletest/include
                    ${GTEST_SRC_ROOT}/googletest
                    ${GTEST_SRC_ROOT}/googlemock/include
                    ${GTEST_SRC_ROOT}/googlemock)
            if (APPLE AND NOT MSVC)
                target_compile_options(${GTEST_TARGET} BEFORE PRIVATE
                        -iquote${GTEST_SRC_ROOT}/googletest/include
                        -iquote${GTEST_SRC_ROOT}/googletest
                        -I${GTEST_SRC_ROOT}/googletest/include
                        -I${GTEST_SRC_ROOT}/googletest
                        -std=c++14)
            endif ()
        endif ()
    endforeach ()
    # Remember the vendored GTest header roots so they can be forced ahead of any
    # system installation when compiling TsFile_Test itself.
    set(VENDORED_GTEST_INCLUDE_DIRS
            ${GTEST_SRC_ROOT}/googletest/include
            ${GTEST_SRC_ROOT}/googlemock/include)
    set(TESTS_ENABLED ON PARENT_SCOPE)
else ()
    message(WARNING "Failed to download googletest from all provided URLs, setting TESTS_ENABLED to OFF")
    set(TESTS_ENABLED OFF PARENT_SCOPE)
    return()
endif ()

message(STATUS "Adding test configurations...")

set(LIB_TSFILE_SDK_DIR ${PROJECT_BINARY_DIR}/lib)
message("LIB_TSFILE_SDK_DIR: ${LIB_TSFILE_SDK_DIR}")

include_directories(
        ${CMAKE_CURRENT_SOURCE_DIR}/reader
        ${LIBRARY_INCLUDE_DIR}
        ${THIRD_PARTY_INCLUDE}
)

enable_testing()

# Script-mode tests launch nested CMake configurations, which otherwise select
# a platform default instead of inheriting the generator used by this build.
set(_TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS
        "-DTEST_CMAKE_GENERATOR=${CMAKE_GENERATOR}")
if (CMAKE_MAKE_PROGRAM)
    list(APPEND _TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS
            "-DTEST_CMAKE_MAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}")
endif ()
if (CMAKE_GENERATOR_PLATFORM)
    list(APPEND _TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS
            "-DTEST_CMAKE_GENERATOR_PLATFORM=${CMAKE_GENERATOR_PLATFORM}")
endif ()
if (CMAKE_GENERATOR_TOOLSET)
    list(APPEND _TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS
            "-DTEST_CMAKE_GENERATOR_TOOLSET=${CMAKE_GENERATOR_TOOLSET}")
endif ()

add_test(
        NAME DependencySourceCMakeTest
        COMMAND ${CMAKE_COMMAND} -P
                ${CMAKE_SOURCE_DIR}/cmake/tests/DependencySourceTest.cmake)
add_test(
        NAME SnappyDependencyCMakeTest
        COMMAND ${CMAKE_COMMAND}
                ${_TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS}
                -DTEST_BINARY_ROOT=${CMAKE_CURRENT_BINARY_DIR}/cmake-tests
                -P ${CMAKE_SOURCE_DIR}/cmake/tests/SnappyDependencyTest.cmake)
add_test(
        NAME LZ4DependencyCMakeTest
        COMMAND ${CMAKE_COMMAND}
                ${_TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS}
                -DTEST_BINARY_ROOT=${CMAKE_CURRENT_BINARY_DIR}/cmake-tests
                -P ${CMAKE_SOURCE_DIR}/cmake/tests/LZ4DependencyTest.cmake)
add_test(
        NAME ZLIBDependencyCMakeTest
        COMMAND ${CMAKE_COMMAND}
                ${_TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS}
                -DTEST_BINARY_ROOT=${CMAKE_CURRENT_BINARY_DIR}/cmake-tests
                -P ${CMAKE_SOURCE_DIR}/cmake/tests/ZLIBDependencyTest.cmake)
add_test(
        NAME ZSTDDependencyCMakeTest
        COMMAND ${CMAKE_COMMAND}
                ${_TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS}
                -DTEST_BINARY_ROOT=${CMAKE_CURRENT_BINARY_DIR}/cmake-tests
                -P ${CMAKE_SOURCE_DIR}/cmake/tests/ZSTDDependencyTest.cmake)
add_test(
        NAME LibLZMADependencyCMakeTest
        COMMAND ${CMAKE_COMMAND}
                ${_TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS}
                -DTEST_BINARY_ROOT=${CMAKE_CURRENT_BINARY_DIR}/cmake-tests
                -P ${CMAKE_SOURCE_DIR}/cmake/tests/LibLZMADependencyTest.cmake)
add_test(
        NAME LZOKAYDependencyCMakeTest
        COMMAND ${CMAKE_COMMAND}
                ${_TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS}
                -DTEST_BINARY_ROOT=${CMAKE_CURRENT_BINARY_DIR}/cmake-tests
                -P ${CMAKE_SOURCE_DIR}/cmake/tests/LZOKAYDependencyTest.cmake)
add_test(
        NAME SIMDeDependencyCMakeTest
        COMMAND ${CMAKE_COMMAND}
                ${_TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS}
                -DTEST_BINARY_ROOT=${CMAKE_CURRENT_BINARY_DIR}/cmake-tests
                -P ${CMAKE_SOURCE_DIR}/cmake/tests/SIMDeDependencyTest.cmake)
add_test(
        NAME ANTLR4DependencyCMakeTest
        COMMAND ${CMAKE_COMMAND}
                ${_TSFILE_DEPENDENCY_TEST_CMAKE_ARGUMENTS}
                -DTEST_BINARY_ROOT=${CMAKE_CURRENT_BINARY_DIR}/cmake-tests
                -DTEST_ANTLR4_ARCHIVE=${TSFILE_ANTLR4_ARCHIVE_RESOLVED}
                -P ${CMAKE_SOURCE_DIR}/cmake/tests/ANTLR4DependencyTest.cmake)

file(GLOB_RECURSE TEST_SRCS
        "common/*_test.cc"
        "encoding/*_test.cc"
        "utils/*_test.cc"
        "file/*_test.cc"
        "reader/*_test.cc"
        "writer/*_test.cc"
        "cwrapper/*_test.cc"
        "compress/*uncompressed*_test.cc"
        "dataset/*_test.cc"
)

# Parser tests depend on the ANTLR4 runtime; only build them when it is enabled.
if (ENABLE_ANTLR4)
    file(GLOB_RECURSE PARSER_TEST_SRCS "parser/*_test.cc")
    list(APPEND TEST_SRCS ${PARSER_TEST_SRCS})
endif()

if (ENABLE_SNAPPY)
    file(GLOB_RECURSE SNAPPY_TEST_SRCS "compress/*snappy*_test.cc")
    list(APPEND TEST_SRCS ${SNAPPY_TEST_SRCS})
endif()

if (ENABLE_LZ4)
    file(GLOB_RECURSE LZ4_TEST_SRCS "compress/*lz4*_test.cc")
    list(APPEND TEST_SRCS ${LZ4_TEST_SRCS})
endif()

if (ENABLE_LZOKAY)
    file(GLOB_RECURSE LZOKAY_TEST_SRCS "compress/*lzo*_test.cc")
    list(APPEND TEST_SRCS ${LZOKAY_TEST_SRCS})
endif()

if (ENABLE_ZLIB)
    file(GLOB_RECURSE ZLIB_TEST_SRCS "compress/*gzip*_test.cc")
    list(APPEND TEST_SRCS ${ZLIB_TEST_SRCS})
endif()

if (ENABLE_ZSTD)
    file(GLOB_RECURSE ZSTD_TEST_SRCS "compress/*zstd*_test.cc")
    list(APPEND TEST_SRCS ${ZSTD_TEST_SRCS})
endif()

if (ENABLE_LZMA2)
    file(GLOB_RECURSE LZMA2_TEST_SRCS "compress/*lzma2*_test.cc")
    list(APPEND TEST_SRCS ${LZMA2_TEST_SRCS})
endif()

if (BUILD_TOOLS)
    file(GLOB_RECURSE TOOLS_TEST_SRCS "tools/*_test.cc")
    list(APPEND TEST_SRCS ${TOOLS_TEST_SRCS})
endif ()

if (${COV_ENABLED})
    message("Enable code cov...")
    add_compile_options(-fprofile-arcs -ftest-coverage)
endif ()

add_executable(TsFile_Test ${TEST_SRCS})
# Force the vendored GTest headers ahead of any system installation so the test
# code reliably compiles against the vendored 1.12.1 headers.
if (VENDORED_GTEST_INCLUDE_DIRS)
    target_include_directories(TsFile_Test BEFORE PRIVATE
            ${VENDORED_GTEST_INCLUDE_DIRS})
endif ()
if (BUILD_TOOLS)
    target_include_directories(TsFile_Test PRIVATE ${CMAKE_SOURCE_DIR}/tools)
    target_compile_definitions(TsFile_Test PRIVATE
            TSFILE_CPP_SOURCE_DIR="${CMAKE_SOURCE_DIR}")
endif ()
if (APPLE AND NOT MSVC)
    target_compile_options(TsFile_Test PRIVATE -std=c++14)
endif ()
target_link_libraries(
        TsFile_Test
        GTest::gtest_main
        GTest::gmock
        tsfile
)
if (ENABLE_SNAPPY)
    target_link_libraries(TsFile_Test TsFile::Snappy)
endif ()
if (ENABLE_LZ4)
    target_link_libraries(TsFile_Test TsFile::LZ4)
endif ()
if (ENABLE_LZOKAY)
    target_link_libraries(TsFile_Test TsFile::LZOKAY)
endif ()
if (ENABLE_ZLIB)
    target_link_libraries(TsFile_Test TsFile::ZLIB)
endif ()
if (ENABLE_ZSTD)
    target_link_libraries(TsFile_Test TsFile::ZSTD)
endif ()
if (ENABLE_LZMA2)
    target_link_libraries(TsFile_Test TsFile::LibLZMA)
endif ()
if (ENABLE_SIMD)
    target_link_libraries(TsFile_Test TsFile::SIMDe)
endif ()
if (ENABLE_ANTLR4)
    target_link_libraries(TsFile_Test TsFile::ANTLR4)
endif ()
if (BUILD_TOOLS)
    target_link_libraries(TsFile_Test tsfile_cli_obj)
endif ()

set_target_properties(TsFile_Test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${LIB_TSFILE_SDK_DIR})

# On Windows, copy tsfile DLL next to the test exe so it can load at runtime
# (and when gtest_discover_tests runs the exe). Use TARGET_FILE so the path
# is correct for the current build config (e.g. Release).
if (WIN32 AND TSFILE_BUILD_SHARED)
    add_custom_command(TARGET TsFile_Test POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:tsfile>
            "$<TARGET_FILE_DIR:TsFile_Test>"
            COMMENT "Copying libtsfile.dll to test executable directory"
            VERBATIM
    )
endif ()

include(GoogleTest)
# On Windows, delay test discovery until ctest runs (PRE_TEST) so the test exe
# runs with the correct env (e.g. PATH has MinGW, libtsfile.dll is present).
# Avoids 0xc0000139 when discovery runs at build time.
#
# DISCOVERY_TIMEOUT is raised well above the 5s default: the first execution of
# the freshly-built test executable can be delayed by on-access antivirus
# scanning (e.g. Windows Defender), which otherwise trips a spurious
# "Process terminated due to timeout" while gtest_discover_tests enumerates it.
if(WIN32)
  gtest_discover_tests(TsFile_Test DISCOVERY_MODE PRE_TEST DISCOVERY_TIMEOUT 120)
else()
  gtest_discover_tests(TsFile_Test)
endif()
