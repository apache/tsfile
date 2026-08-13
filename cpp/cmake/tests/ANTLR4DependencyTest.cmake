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

set(_TSFILE_FIXTURE_SOURCE
        "${CMAKE_CURRENT_LIST_DIR}/projects/ANTLR4Dependency")
set(_TSFILE_TEST_ROOT "${TEST_BINARY_ROOT}/antlr4-dependency")
set(_TSFILE_COMPATIBLE_ROOT "${_TSFILE_TEST_ROOT}/compatible")
set(_TSFILE_TOO_OLD_ROOT "${_TSFILE_TEST_ROOT}/too-old")
set(_TSFILE_INCOMPATIBLE_ROOT "${_TSFILE_TEST_ROOT}/incompatible")
set(_TSFILE_MISSING_ROOT "${_TSFILE_TEST_ROOT}/missing")
file(REMOVE_RECURSE "${_TSFILE_TEST_ROOT}")

function(_tsfile_write_fake_antlr4 ROOT VERSION TARGET_NAME)
    set(_TSFILE_PACKAGE_DIR "${ROOT}/lib/cmake/antlr4-runtime")
    file(MAKE_DIRECTORY "${_TSFILE_PACKAGE_DIR}")
    file(WRITE "${_TSFILE_PACKAGE_DIR}/antlr4-runtime-config.cmake"
            "set(ANTLR_VERSION \"${VERSION}\")\n"
            "set(antlr4-runtime_VERSION \"${VERSION}\")\n"
            "add_library(${TARGET_NAME} INTERFACE IMPORTED)\n")
endfunction()

function(_tsfile_run_antlr4_case NAME POLICY EXPECTED_SOURCE EXPECT_SUCCESS ROOT)
    set(_TSFILE_CASE_BINARY "${_TSFILE_TEST_ROOT}/build-${NAME}")
    execute_process(
            COMMAND "${CMAKE_COMMAND}"
                    "-DTSFILE_DEPENDENCY_SOURCE=${POLICY}"
                    "-DEXPECTED_ANTLR4_SOURCE=${EXPECTED_SOURCE}"
                    "-DCMAKE_PREFIX_PATH=${ROOT}"
                    -DCMAKE_FIND_USE_PACKAGE_REGISTRY=FALSE
                    -DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=FALSE
                    -S "${_TSFILE_FIXTURE_SOURCE}"
                    -B "${_TSFILE_CASE_BINARY}"
            RESULT_VARIABLE _TSFILE_RESULT
            OUTPUT_VARIABLE _TSFILE_OUTPUT
            ERROR_VARIABLE _TSFILE_ERROR)
    set(_TSFILE_LOG "${_TSFILE_OUTPUT}${_TSFILE_ERROR}")

    if (EXPECT_SUCCESS)
        if (NOT _TSFILE_RESULT EQUAL 0)
            message(FATAL_ERROR
                    "ANTLR4 dependency case ${NAME} failed: ${_TSFILE_LOG}")
        endif ()
    elseif (_TSFILE_RESULT EQUAL 0)
        message(FATAL_ERROR
                "ANTLR4 dependency case ${NAME} unexpectedly succeeded.")
    elseif (NOT _TSFILE_LOG MATCHES
            "requires a compatible system ANTLR4")
        message(FATAL_ERROR
                "ANTLR4 dependency case ${NAME} had an unclear error: "
                "${_TSFILE_LOG}")
    endif ()
endfunction()

_tsfile_write_fake_antlr4("${_TSFILE_COMPATIBLE_ROOT}" 4.9.3 antlr4_static)
_tsfile_write_fake_antlr4("${_TSFILE_TOO_OLD_ROOT}" 4.9.2 antlr4_static)
_tsfile_write_fake_antlr4("${_TSFILE_INCOMPATIBLE_ROOT}" 5.0.0 antlr4_shared)
file(MAKE_DIRECTORY "${_TSFILE_MISSING_ROOT}")

_tsfile_run_antlr4_case(system-compatible SYSTEM SYSTEM TRUE
        "${_TSFILE_COMPATIBLE_ROOT}")
_tsfile_run_antlr4_case(auto-compatible AUTO SYSTEM TRUE
        "${_TSFILE_COMPATIBLE_ROOT}")
_tsfile_run_antlr4_case(auto-missing AUTO BUNDLED TRUE
        "${_TSFILE_MISSING_ROOT}")
_tsfile_run_antlr4_case(auto-too-old AUTO BUNDLED TRUE
        "${_TSFILE_TOO_OLD_ROOT}")
_tsfile_run_antlr4_case(auto-incompatible AUTO BUNDLED TRUE
        "${_TSFILE_INCOMPATIBLE_ROOT}")
_tsfile_run_antlr4_case(bundled BUNDLED BUNDLED TRUE
        "${_TSFILE_INCOMPATIBLE_ROOT}")
_tsfile_run_antlr4_case(system-missing SYSTEM "" FALSE
        "${_TSFILE_MISSING_ROOT}")
_tsfile_run_antlr4_case(system-too-old SYSTEM "" FALSE
        "${_TSFILE_TOO_OLD_ROOT}")
_tsfile_run_antlr4_case(system-incompatible SYSTEM "" FALSE
        "${_TSFILE_INCOMPATIBLE_ROOT}")

set(_TSFILE_SOURCE_FAILURE_SCRIPT
        "${CMAKE_CURRENT_LIST_DIR}/ANTLR4SourceFailureCase.cmake")
function(_tsfile_assert_antlr4_source_failure TEST_CASE EXPECTED_ERROR)
    execute_process(
            COMMAND "${CMAKE_COMMAND}"
                    -DTEST_CASE=${TEST_CASE}
                    -DTEST_BINARY_ROOT=${_TSFILE_TEST_ROOT}
                    -DTEST_ANTLR4_ARCHIVE=${TEST_ANTLR4_ARCHIVE}
                    -P "${_TSFILE_SOURCE_FAILURE_SCRIPT}"
            RESULT_VARIABLE _TSFILE_RESULT
            OUTPUT_VARIABLE _TSFILE_OUTPUT
            ERROR_VARIABLE _TSFILE_ERROR)
    if (_TSFILE_RESULT EQUAL 0)
        message(FATAL_ERROR
                "ANTLR4 source failure case ${TEST_CASE} unexpectedly "
                "succeeded.")
    endif ()
    set(_TSFILE_LOG "${_TSFILE_OUTPUT}${_TSFILE_ERROR}")
    if (NOT _TSFILE_LOG MATCHES "${EXPECTED_ERROR}")
        message(FATAL_ERROR
                "ANTLR4 source failure case ${TEST_CASE} had an unclear "
                "error: ${_TSFILE_LOG}")
    endif ()
endfunction()

_tsfile_assert_antlr4_source_failure(
        OFFLINE_MISSING "Offline dependency mode requires")
_tsfile_assert_antlr4_source_failure(
        INVALID_ARCHIVE "unexpected SHA-256")
if (TEST_ANTLR4_ARCHIVE)
    _tsfile_assert_antlr4_source_failure(
            UTF8CPP_OFFLINE_MISSING "Offline dependency mode requires.*utf8cpp")
    _tsfile_assert_antlr4_source_failure(
            UTF8CPP_INVALID_ARCHIVE "utf8cpp.*unexpected SHA-256")
endif ()
