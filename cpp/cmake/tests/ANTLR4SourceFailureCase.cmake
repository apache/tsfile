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

if (NOT TEST_BINARY_ROOT)
    message(FATAL_ERROR "TEST_BINARY_ROOT must be provided.")
endif ()

get_filename_component(_TSFILE_CMAKE_DIR "${CMAKE_CURRENT_LIST_DIR}/.."
        ABSOLUTE)
set(TSFILE_ANTLR4_BUNDLED_VERSION "4.9.3")
set(TSFILE_DEPENDENCY_OFFLINE ON)
set(CMAKE_BINARY_DIR "${TEST_BINARY_ROOT}/source-failure-${TEST_CASE}")
file(REMOVE_RECURSE "${CMAKE_BINARY_DIR}")

if (TEST_CASE STREQUAL "OFFLINE_MISSING")
    set(TSFILE_DEPENDENCY_CACHE "${TEST_BINARY_ROOT}/missing-cache")
    file(REMOVE_RECURSE "${TSFILE_DEPENDENCY_CACHE}")
elseif (TEST_CASE STREQUAL "INVALID_ARCHIVE")
    set(TSFILE_DEPENDENCY_CACHE "${TEST_BINARY_ROOT}/invalid-cache")
    set(TSFILE_ANTLR4_ARCHIVE "${CMAKE_CURRENT_LIST_FILE}"
            CACHE FILEPATH "" FORCE)
elseif (TEST_CASE MATCHES "^UTF8CPP_")
    if (NOT TEST_ANTLR4_ARCHIVE)
        message(FATAL_ERROR
                "TEST_ANTLR4_ARCHIVE is required for ${TEST_CASE}.")
    endif ()
    set(TSFILE_DEPENDENCY_CACHE
            "${TEST_BINARY_ROOT}/utf8cpp-failure-cache")
    file(REMOVE_RECURSE "${TSFILE_DEPENDENCY_CACHE}")
    set(TSFILE_ANTLR4_ARCHIVE "${TEST_ANTLR4_ARCHIVE}"
            CACHE FILEPATH "" FORCE)
    if (TEST_CASE STREQUAL "UTF8CPP_INVALID_ARCHIVE")
        set(TSFILE_UTF8CPP_ARCHIVE "${CMAKE_CURRENT_LIST_FILE}"
                CACHE FILEPATH "" FORCE)
    endif ()
else ()
    message(FATAL_ERROR "Unknown TEST_CASE='${TEST_CASE}'.")
endif ()

include("${_TSFILE_CMAKE_DIR}/ANTLR4Source.cmake")
