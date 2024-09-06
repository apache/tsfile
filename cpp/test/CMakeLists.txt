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
include(FetchContent)

set(URL_LIST
    "https://github.com/google/googletest/archive/refs/tags/release-1.12.1.zip"
    "https://hub.nuaa.cf/google/googletest/archive/refs/tags/release-1.12.1.zip"
    "https://hub.yzuu.cf/google/googletest/archive/refs/tags/release-1.12.1.zip"
)

set(DOWNLOADED 0)
set(GTEST_URL "")
set(TIMEOUT 30)

foreach(URL ${URL_LIST})
    message(STATUS "Trying to download from ${URL}")
    file(DOWNLOAD ${URL} "${CMAKE_BINARY_DIR}/googletest-release-1.12.1.zip" STATUS DOWNLOAD_STATUS TIMEOUT ${TIMEOUT})

    list(GET DOWNLOAD_STATUS 0 DOWNLOAD_RESULT)
    if(${DOWNLOAD_RESULT} EQUAL 0)
        set(DOWNLOADED 1)
        set(GTEST_URL ${URL})
        break()
    endif()
endforeach()

if(${DOWNLOADED})
    message(STATUS "Successfully downloaded googletest from ${GTEST_URL}")
    FetchContent_Declare(
      googletest
      URL ${GTEST_URL}
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
    set(TESTS_ENABLED ON PARENT_SCOPE)
else()
    message(WARNING "Failed to download googletest from all provided URLs, setting TESTS_ENABLED to OFF")
    set(TESTS_ENABLED OFF PARENT_SCOPE)
    return()
endif()

message(STATUS "Adding test configurations...")

set(SDK_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/../src)
message("SDK_INCLUDE_DIR: ${SDK_INCLUDE_DIR}")
set(LIBTSFILE_SDK_DIR ${PROJECT_BINARY_DIR}/lib)
message("LIBTSFILE_SDK_DIR: ${LIBTSFILE_SDK_DIR}")

include_directories(${SDK_INCLUDE_DIR})

enable_testing()

file(GLOB_RECURSE TEST_SRCS
        "common/*_test.cc"
        "encoding/*_test.cc"
        "utils/*_test.cc"
        "file/*_test.cc"
        "compress/*_test.cc"
        "writer/*_test.cc"
        )
if (${COV_ENABLED})
    message("Enable code cov...")
    add_compile_options(-fprofile-arcs -ftest-coverage)
endif ()

if(NOT WIN32)
# enable address sanitizer default
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g")
endif()

add_executable(TsFile_Test ${TEST_SRCS})
target_link_libraries(
  TsFile_Test
  GTest::gtest_main
  GTest::gmock
  tsfile
)
set_target_properties(TsFile_Test PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${LIBTSFILE_SDK_DIR})


include(GoogleTest)
gtest_discover_tests(TsFile_Test)
