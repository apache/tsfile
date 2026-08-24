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

get_filename_component(_TSFILE_CMAKE_DIR "${CMAKE_CURRENT_LIST_DIR}/.."
        ABSOLUTE)
set(TSFILE_DEPENDENCY_SOURCE "auto" CACHE STRING "" FORCE)
include("${_TSFILE_CMAKE_DIR}/DependencySource.cmake")

function(_tsfile_assert_resolution POLICY SYSTEM_FOUND EXPECTED)
    set(TSFILE_DEPENDENCY_SOURCE "${POLICY}")
    tsfile_resolve_dependency_source(
            "Example" "${SYSTEM_FOUND}" _TSFILE_ACTUAL)
    if (NOT _TSFILE_ACTUAL STREQUAL "${EXPECTED}")
        message(FATAL_ERROR
                "${POLICY} with SYSTEM_FOUND=${SYSTEM_FOUND} resolved to "
                "${_TSFILE_ACTUAL}; expected ${EXPECTED}.")
    endif ()
endfunction()

if (NOT TSFILE_DEPENDENCY_SOURCE STREQUAL "AUTO")
    message(FATAL_ERROR
            "Dependency source was not normalized to AUTO: "
            "${TSFILE_DEPENDENCY_SOURCE}")
endif ()

_tsfile_assert_resolution(AUTO TRUE SYSTEM)
_tsfile_assert_resolution(AUTO FALSE BUNDLED)
_tsfile_assert_resolution(SYSTEM TRUE SYSTEM)
_tsfile_assert_resolution(BUNDLED TRUE BUNDLED)
_tsfile_assert_resolution(BUNDLED FALSE BUNDLED)

set(_TSFILE_FAILURE_CASE_SCRIPT
        "${CMAKE_CURRENT_LIST_DIR}/DependencySourceFailureCase.cmake")

execute_process(
        COMMAND "${CMAKE_COMMAND}"
                -DTEST_CASE=INVALID_SOURCE
                -P "${_TSFILE_FAILURE_CASE_SCRIPT}"
        RESULT_VARIABLE _TSFILE_INVALID_RESULT
        OUTPUT_VARIABLE _TSFILE_INVALID_OUTPUT
        ERROR_VARIABLE _TSFILE_INVALID_ERROR)
if (_TSFILE_INVALID_RESULT EQUAL 0)
    message(FATAL_ERROR "An invalid dependency source was accepted.")
endif ()
set(_TSFILE_INVALID_LOG
        "${_TSFILE_INVALID_OUTPUT}${_TSFILE_INVALID_ERROR}")
if (NOT _TSFILE_INVALID_LOG MATCHES "Expected one of")
    message(FATAL_ERROR
            "Invalid-source error was unclear: ${_TSFILE_INVALID_LOG}")
endif ()

execute_process(
        COMMAND "${CMAKE_COMMAND}"
                -DTEST_CASE=SYSTEM_MISSING
                -P "${_TSFILE_FAILURE_CASE_SCRIPT}"
        RESULT_VARIABLE _TSFILE_SYSTEM_RESULT
        OUTPUT_VARIABLE _TSFILE_SYSTEM_OUTPUT
        ERROR_VARIABLE _TSFILE_SYSTEM_ERROR)
if (_TSFILE_SYSTEM_RESULT EQUAL 0)
    message(FATAL_ERROR
            "SYSTEM mode accepted a missing required dependency.")
endif ()
set(_TSFILE_SYSTEM_LOG
        "${_TSFILE_SYSTEM_OUTPUT}${_TSFILE_SYSTEM_ERROR}")
if (NOT _TSFILE_SYSTEM_LOG MATCHES "requires a compatible")
    message(FATAL_ERROR
            "Missing-system-dependency error was unclear: "
            "${_TSFILE_SYSTEM_LOG}")
endif ()
