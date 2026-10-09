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
set(TSFILE_SIMDE_BUNDLED_VERSION "0.8.4-rc3")
set(TSFILE_DEPENDENCY_OFFLINE ON)

if (TEST_CASE STREQUAL "OFFLINE_MISSING")
    set(TSFILE_DEPENDENCY_CACHE "${TEST_BINARY_ROOT}/missing-cache")
    file(REMOVE_RECURSE "${TSFILE_DEPENDENCY_CACHE}")
elseif (TEST_CASE STREQUAL "INVALID_ARCHIVE")
    set(TSFILE_DEPENDENCY_CACHE "${TEST_BINARY_ROOT}/invalid-cache")
    set(TSFILE_SIMDE_ARCHIVE "${CMAKE_CURRENT_LIST_FILE}"
            CACHE FILEPATH "" FORCE)
else ()
    message(FATAL_ERROR "Unknown TEST_CASE='${TEST_CASE}'.")
endif ()

include("${_TSFILE_CMAKE_DIR}/SIMDeSource.cmake")
