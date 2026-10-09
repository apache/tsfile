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
        "${CMAKE_CURRENT_LIST_DIR}/projects/ZLIBDependency")
set(_TSFILE_TEST_ROOT "${TEST_BINARY_ROOT}/zlib-dependency")
set(_TSFILE_COMPATIBLE_ROOT "${_TSFILE_TEST_ROOT}/compatible")
set(_TSFILE_TOO_OLD_ROOT "${_TSFILE_TEST_ROOT}/too-old")
set(_TSFILE_INCOMPATIBLE_ROOT "${_TSFILE_TEST_ROOT}/incompatible")
file(REMOVE_RECURSE "${_TSFILE_TEST_ROOT}")

function(_tsfile_write_fake_zlib ROOT VERSION)
    file(MAKE_DIRECTORY "${ROOT}/include" "${ROOT}/lib")
    file(WRITE "${ROOT}/include/zlib.h"
            "#define ZLIB_VERSION \"${VERSION}\"\n")
    foreach (_TSFILE_LIBRARY_NAME
            libz.a libz.so libz.dylib z.lib zlib.lib zlibstatic.lib)
        file(WRITE "${ROOT}/lib/${_TSFILE_LIBRARY_NAME}" "")
    endforeach ()
endfunction()

function(_tsfile_run_zlib_case NAME POLICY EXPECTED_SOURCE EXPECT_SUCCESS)
    set(_TSFILE_CASE_BINARY "${_TSFILE_TEST_ROOT}/build-${NAME}")
    set(_TSFILE_CMAKE_ARGUMENTS
            "-DTSFILE_DEPENDENCY_SOURCE=${POLICY}"
            "-DEXPECTED_ZLIB_SOURCE=${EXPECTED_SOURCE}")
    if (ARGC GREATER 4 AND NOT "${ARGV4}" STREQUAL "")
        list(APPEND _TSFILE_CMAKE_ARGUMENTS "-DZLIB_ROOT=${ARGV4}")
    endif ()
    if (ARGC GREATER 5 AND ARGV5)
        list(APPEND _TSFILE_CMAKE_ARGUMENTS
                "-DCMAKE_DISABLE_FIND_PACKAGE_ZLIB=TRUE")
    endif ()

    execute_process(
            COMMAND "${CMAKE_COMMAND}"
                    ${_TSFILE_TEST_GENERATOR_ARGUMENTS}
                    ${_TSFILE_CMAKE_ARGUMENTS}
                    -S "${_TSFILE_FIXTURE_SOURCE}"
                    -B "${_TSFILE_CASE_BINARY}"
            RESULT_VARIABLE _TSFILE_RESULT
            OUTPUT_VARIABLE _TSFILE_OUTPUT
            ERROR_VARIABLE _TSFILE_ERROR)
    set(_TSFILE_LOG "${_TSFILE_OUTPUT}${_TSFILE_ERROR}")

    if (EXPECT_SUCCESS)
        if (NOT _TSFILE_RESULT EQUAL 0)
            message(FATAL_ERROR
                    "zlib dependency case ${NAME} failed: ${_TSFILE_LOG}")
        endif ()
    elseif (_TSFILE_RESULT EQUAL 0)
        message(FATAL_ERROR
                "zlib dependency case ${NAME} unexpectedly succeeded.")
    elseif (NOT _TSFILE_LOG MATCHES
            "requires a compatible system zlib package")
        message(FATAL_ERROR
                "zlib dependency case ${NAME} had an unclear error: "
                "${_TSFILE_LOG}")
    endif ()
endfunction()

_tsfile_write_fake_zlib("${_TSFILE_COMPATIBLE_ROOT}" "1.3.1")
_tsfile_write_fake_zlib("${_TSFILE_TOO_OLD_ROOT}" "1.3.0")
_tsfile_write_fake_zlib("${_TSFILE_INCOMPATIBLE_ROOT}" "2.0.0")

_tsfile_run_zlib_case(system-compatible SYSTEM SYSTEM TRUE
        "${_TSFILE_COMPATIBLE_ROOT}" FALSE)
_tsfile_run_zlib_case(auto-compatible AUTO SYSTEM TRUE
        "${_TSFILE_COMPATIBLE_ROOT}" FALSE)
_tsfile_run_zlib_case(auto-missing AUTO BUNDLED TRUE "" TRUE)
_tsfile_run_zlib_case(auto-too-old AUTO BUNDLED TRUE
        "${_TSFILE_TOO_OLD_ROOT}" FALSE)
_tsfile_run_zlib_case(auto-incompatible AUTO BUNDLED TRUE
        "${_TSFILE_INCOMPATIBLE_ROOT}" FALSE)
_tsfile_run_zlib_case(bundled BUNDLED BUNDLED TRUE
        "${_TSFILE_INCOMPATIBLE_ROOT}" FALSE)
_tsfile_run_zlib_case(system-missing SYSTEM "" FALSE "" TRUE)
_tsfile_run_zlib_case(system-too-old SYSTEM "" FALSE
        "${_TSFILE_TOO_OLD_ROOT}" FALSE)
_tsfile_run_zlib_case(system-incompatible SYSTEM "" FALSE
        "${_TSFILE_INCOMPATIBLE_ROOT}" FALSE)

set(_TSFILE_SOURCE_FAILURE_SCRIPT
        "${CMAKE_CURRENT_LIST_DIR}/ZLIBSourceFailureCase.cmake")
function(_tsfile_assert_zlib_source_failure TEST_CASE EXPECTED_ERROR)
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
                "zlib source failure case ${TEST_CASE} unexpectedly succeeded.")
    endif ()
    set(_TSFILE_LOG "${_TSFILE_OUTPUT}${_TSFILE_ERROR}")
    if (NOT _TSFILE_LOG MATCHES "${EXPECTED_ERROR}")
        message(FATAL_ERROR
                "zlib source failure case ${TEST_CASE} had an unclear error: "
                "${_TSFILE_LOG}")
    endif ()
endfunction()

_tsfile_assert_zlib_source_failure(
        OFFLINE_MISSING "Offline dependency mode requires")
_tsfile_assert_zlib_source_failure(
        INVALID_ARCHIVE "unexpected SHA-256")
