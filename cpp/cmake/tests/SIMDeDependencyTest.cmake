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
include("${CMAKE_CURRENT_LIST_DIR}/TestGeneratorArguments.cmake")

set(_TSFILE_FIXTURE_SOURCE
        "${CMAKE_CURRENT_LIST_DIR}/projects/SIMDeDependency")
set(_TSFILE_TEST_ROOT "${TEST_BINARY_ROOT}/simde-dependency")
set(_TSFILE_COMPATIBLE_ROOT "${_TSFILE_TEST_ROOT}/compatible")
set(_TSFILE_TOO_OLD_ROOT "${_TSFILE_TEST_ROOT}/too-old")
set(_TSFILE_INCOMPATIBLE_ROOT "${_TSFILE_TEST_ROOT}/incompatible")
set(_TSFILE_MISSING_ROOT "${_TSFILE_TEST_ROOT}/missing")
file(REMOVE_RECURSE "${_TSFILE_TEST_ROOT}")

function(_tsfile_write_fake_simde ROOT MAJOR MINOR MICRO)
    set(_TSFILE_INCLUDE_DIR "${ROOT}/include/simde")
    file(MAKE_DIRECTORY "${_TSFILE_INCLUDE_DIR}/x86")
    file(WRITE "${_TSFILE_INCLUDE_DIR}/simde-common.h"
            "#define SIMDE_VERSION_MAJOR ${MAJOR}\n"
            "#define SIMDE_VERSION_MINOR ${MINOR}\n"
            "#define SIMDE_VERSION_MICRO ${MICRO}\n")
    file(WRITE "${_TSFILE_INCLUDE_DIR}/x86/ssse3.h" "\n")
endfunction()

function(_tsfile_run_simde_case NAME POLICY EXPECTED_SOURCE EXPECT_SUCCESS ROOT)
    set(_TSFILE_CASE_BINARY "${_TSFILE_TEST_ROOT}/build-${NAME}")
    execute_process(
            COMMAND "${CMAKE_COMMAND}"
                    ${_TSFILE_TEST_GENERATOR_ARGUMENTS}
                    "-DTSFILE_DEPENDENCY_SOURCE=${POLICY}"
                    "-DEXPECTED_SIMDE_SOURCE=${EXPECTED_SOURCE}"
                    "-DCMAKE_DISABLE_FIND_PACKAGE_simde=TRUE"
                    "-DSIMDE_ROOT=${ROOT}"
                    "-DCMAKE_FIND_ROOT_PATH=${ROOT}"
                    "-DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY"
                    -S "${_TSFILE_FIXTURE_SOURCE}"
                    -B "${_TSFILE_CASE_BINARY}"
            RESULT_VARIABLE _TSFILE_RESULT
            OUTPUT_VARIABLE _TSFILE_OUTPUT
            ERROR_VARIABLE _TSFILE_ERROR)
    set(_TSFILE_LOG "${_TSFILE_OUTPUT}${_TSFILE_ERROR}")

    if (EXPECT_SUCCESS)
        if (NOT _TSFILE_RESULT EQUAL 0)
            message(FATAL_ERROR
                    "SIMDe dependency case ${NAME} failed: ${_TSFILE_LOG}")
        endif ()
    elseif (_TSFILE_RESULT EQUAL 0)
        message(FATAL_ERROR
                "SIMDe dependency case ${NAME} unexpectedly succeeded.")
    elseif (NOT _TSFILE_LOG MATCHES
            "requires a compatible system SIMDe")
        message(FATAL_ERROR
                "SIMDe dependency case ${NAME} had an unclear error: "
                "${_TSFILE_LOG}")
    endif ()
endfunction()

_tsfile_write_fake_simde("${_TSFILE_COMPATIBLE_ROOT}" 0 8 4)
_tsfile_write_fake_simde("${_TSFILE_TOO_OLD_ROOT}" 0 8 3)
_tsfile_write_fake_simde("${_TSFILE_INCOMPATIBLE_ROOT}" 1 0 0)
file(MAKE_DIRECTORY "${_TSFILE_MISSING_ROOT}")

_tsfile_run_simde_case(system-compatible SYSTEM SYSTEM TRUE
        "${_TSFILE_COMPATIBLE_ROOT}")
_tsfile_run_simde_case(auto-compatible AUTO SYSTEM TRUE
        "${_TSFILE_COMPATIBLE_ROOT}")
_tsfile_run_simde_case(auto-missing AUTO BUNDLED TRUE
        "${_TSFILE_MISSING_ROOT}")
_tsfile_run_simde_case(auto-too-old AUTO BUNDLED TRUE
        "${_TSFILE_TOO_OLD_ROOT}")
_tsfile_run_simde_case(auto-incompatible AUTO BUNDLED TRUE
        "${_TSFILE_INCOMPATIBLE_ROOT}")
_tsfile_run_simde_case(bundled BUNDLED BUNDLED TRUE
        "${_TSFILE_INCOMPATIBLE_ROOT}")
_tsfile_run_simde_case(system-missing SYSTEM "" FALSE
        "${_TSFILE_MISSING_ROOT}")
_tsfile_run_simde_case(system-too-old SYSTEM "" FALSE
        "${_TSFILE_TOO_OLD_ROOT}")
_tsfile_run_simde_case(system-incompatible SYSTEM "" FALSE
        "${_TSFILE_INCOMPATIBLE_ROOT}")

set(_TSFILE_SOURCE_FAILURE_SCRIPT
        "${CMAKE_CURRENT_LIST_DIR}/SIMDeSourceFailureCase.cmake")
function(_tsfile_assert_simde_source_failure TEST_CASE EXPECTED_ERROR)
    execute_process(
            COMMAND "${CMAKE_COMMAND}"
                    -DTEST_CASE=${TEST_CASE}
                    -DTEST_BINARY_ROOT=${_TSFILE_TEST_ROOT}
                    -P "${_TSFILE_SOURCE_FAILURE_SCRIPT}"
            RESULT_VARIABLE _TSFILE_RESULT
            OUTPUT_VARIABLE _TSFILE_OUTPUT
            ERROR_VARIABLE _TSFILE_ERROR)
    if (_TSFILE_RESULT EQUAL 0)
        message(FATAL_ERROR
                "SIMDe source failure case ${TEST_CASE} unexpectedly "
                "succeeded.")
    endif ()
    set(_TSFILE_LOG "${_TSFILE_OUTPUT}${_TSFILE_ERROR}")
    if (NOT _TSFILE_LOG MATCHES "${EXPECTED_ERROR}")
        message(FATAL_ERROR
                "SIMDe source failure case ${TEST_CASE} had an unclear "
                "error: ${_TSFILE_LOG}")
    endif ()
endfunction()

_tsfile_assert_simde_source_failure(
        OFFLINE_MISSING "Offline dependency mode requires")
_tsfile_assert_simde_source_failure(
        INVALID_ARCHIVE "unexpected SHA-256")
