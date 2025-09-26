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
include(FetchContent)

set(CMAKE_VERBOSE_MAKEFILE ON)

set(GTEST_URL_LIST
        "https://github.com/google/googletest/archive/refs/tags/release-1.12.1.zip"
        "https://hub.nuaa.cf/google/googletest/archive/refs/tags/release-1.12.1.zip"
        "https://hub.yzuu.cf/google/googletest/archive/refs/tags/release-1.12.1.zip"
)

set(GTEST_ZIP_PATH "${CMAKE_SOURCE_DIR}/third_party/googletest-release-1.12.1.zip")
set(DOWNLOADED 0)
set(GTEST_URL "")
set(TIMEOUT 30)

if (EXISTS ${GTEST_ZIP_PATH})
    message(STATUS "Using local gtest zip file: ${GTEST_ZIP_PATH}")
    set(DOWNLOADED 1)
    set(GTEST_URL ${GTEST_ZIP_PATH})
else ()
    message(STATUS "Local gtest zip file not found, trying to download from network...")
endif ()

if (NOT DOWNLOADED)
    foreach (URL ${GTEST_URL_LIST})
        message(STATUS "Trying to download from ${URL}")
        file(DOWNLOAD ${URL} "${CMAKE_SOURCE_DIR}/third_party/googletest-release-1.12.1.zip" STATUS DOWNLOAD_STATUS TIMEOUT ${TIMEOUT})

        list(GET DOWNLOAD_STATUS 0 DOWNLOAD_RESULT)
        if (${DOWNLOAD_RESULT} EQUAL 0)
            set(DOWNLOADED 1)
            set(GTEST_URL ${GTEST_ZIP_PATH})
            break()
        endif ()
    endforeach ()
endif ()

if (${DOWNLOADED})
    message(STATUS "Successfully get googletest from ${GTEST_URL}")
    FetchContent_Declare(
            googletest
            URL ${GTEST_URL}
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
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
        ${LIBRARY_INCLUDE_DIR}
        ${THIRD_PARTY_INCLUDE}
        ${THIRD_PARTY_INCLUDE}/google_snappy
        ${CMAKE_SOURCE_DIR}/third_party/lz4
        ${CMAKE_SOURCE_DIR}/third_party/google_snappy
        ${CMAKE_SOURCE_DIR}/third_party/lzokay
        ${CMAKE_SOURCE_DIR}/third_party/zlib-1.2.13
        ${CMAKE_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src
)

enable_testing()

file(GLOB_RECURSE TEST_SRCS
        "common/*_test.cc"
        "encoding/*_test.cc"
        "utils/*_test.cc"
        "file/*_test.cc"
        "parser/*_test.cc"
        "compress/*_test.cc"
        "reader/*_test.cc"
        "writer/*_test.cc"
        "cwrapper/*_test.cc"
)

if (${COV_ENABLED})
    message("Enable code cov...")
    add_compile_options(-fprofile-arcs -ftest-coverage)
endif ()

add_definitions(-DANTLR4CPP_STATIC)
set(ANTLR4_WITH_STATIC_CRT OFF)

add_executable(TsFile_Test ${TEST_SRCS})
target_link_libraries(
        TsFile_Test
        GTest::gtest_main
        GTest::gmock
        tsfile
)

set_target_properties(TsFile_Test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${LIB_TSFILE_SDK_DIR})

if (WIN32)
    add_custom_command(TARGET TsFile_Test POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy
            "${LIBRARY_OUTPUT_PATH}/libtsfile.dll"
            "$<TARGET_FILE_DIR:TsFile_Test>"
            COMMENT "Copying libtsfile.dll to test executable directory"
            VERBATIM
    )
endif ()

include(GoogleTest)
gtest_discover_tests(TsFile_Test)
