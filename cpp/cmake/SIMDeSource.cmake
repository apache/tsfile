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

set(_TSFILE_SIMDE_ARCHIVE_NAME
        "simde-v${TSFILE_SIMDE_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_SIMDE_URL
        "https://github.com/simd-everywhere/simde/archive/refs/tags/v${TSFILE_SIMDE_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_SIMDE_SHA256
        "a5407985439fef1435ac1f091a4d2e6c71981faed213e1be156aca575ce7052c")
set(TSFILE_SIMDE_ARCHIVE ""
        CACHE FILEPATH
        "Optional pre-downloaded SIMDe source archive")

if (TSFILE_SIMDE_ARCHIVE)
    get_filename_component(_TSFILE_SIMDE_ARCHIVE
            "${TSFILE_SIMDE_ARCHIVE}" ABSOLUTE BASE_DIR "${CMAKE_BINARY_DIR}")
    if (NOT EXISTS "${_TSFILE_SIMDE_ARCHIVE}")
        message(FATAL_ERROR
                "TSFILE_SIMDE_ARCHIVE does not exist: "
                "${_TSFILE_SIMDE_ARCHIVE}")
    endif ()
else ()
    file(MAKE_DIRECTORY "${TSFILE_DEPENDENCY_CACHE}")
    set(_TSFILE_SIMDE_ARCHIVE
            "${TSFILE_DEPENDENCY_CACHE}/${_TSFILE_SIMDE_ARCHIVE_NAME}")
endif ()

set(_TSFILE_SIMDE_ARCHIVE_VALID FALSE)
if (EXISTS "${_TSFILE_SIMDE_ARCHIVE}")
    file(SHA256 "${_TSFILE_SIMDE_ARCHIVE}"
            _TSFILE_SIMDE_ACTUAL_SHA256)
    if (_TSFILE_SIMDE_ACTUAL_SHA256 STREQUAL _TSFILE_SIMDE_SHA256)
        set(_TSFILE_SIMDE_ARCHIVE_VALID TRUE)
    elseif (TSFILE_SIMDE_ARCHIVE OR TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "SIMDe source archive has an unexpected SHA-256: "
                "${_TSFILE_SIMDE_ARCHIVE}\n"
                "Expected: ${_TSFILE_SIMDE_SHA256}\n"
                "Actual:   ${_TSFILE_SIMDE_ACTUAL_SHA256}")
    else ()
        message(STATUS
                "Removing invalid cached SIMDe archive: "
                "${_TSFILE_SIMDE_ARCHIVE}")
        file(REMOVE "${_TSFILE_SIMDE_ARCHIVE}")
    endif ()
endif ()

if (NOT _TSFILE_SIMDE_ARCHIVE_VALID)
    if (TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "Offline dependency mode requires the verified SIMDe "
                "archive at ${_TSFILE_SIMDE_ARCHIVE}. Pre-download "
                "${_TSFILE_SIMDE_URL} or set TSFILE_SIMDE_ARCHIVE to a "
                "verified local archive.")
    endif ()

    set(_TSFILE_SIMDE_PARTIAL_ARCHIVE
            "${_TSFILE_SIMDE_ARCHIVE}.part")
    file(REMOVE "${_TSFILE_SIMDE_PARTIAL_ARCHIVE}")
    message(STATUS "Downloading SIMDe v${TSFILE_SIMDE_BUNDLED_VERSION}")
    file(DOWNLOAD
            "${_TSFILE_SIMDE_URL}"
            "${_TSFILE_SIMDE_PARTIAL_ARCHIVE}"
            STATUS _TSFILE_SIMDE_DOWNLOAD_STATUS
            TLS_VERIFY ON
            TIMEOUT 120
            INACTIVITY_TIMEOUT 30)
    list(GET _TSFILE_SIMDE_DOWNLOAD_STATUS 0
            _TSFILE_SIMDE_DOWNLOAD_RESULT)
    list(GET _TSFILE_SIMDE_DOWNLOAD_STATUS 1
            _TSFILE_SIMDE_DOWNLOAD_MESSAGE)
    if (NOT _TSFILE_SIMDE_DOWNLOAD_RESULT EQUAL 0)
        file(REMOVE "${_TSFILE_SIMDE_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Failed to download SIMDe from ${_TSFILE_SIMDE_URL}: "
                "${_TSFILE_SIMDE_DOWNLOAD_MESSAGE}. To build without "
                "network access, provide ${_TSFILE_SIMDE_ARCHIVE_NAME} in "
                "TSFILE_DEPENDENCY_CACHE or set TSFILE_SIMDE_ARCHIVE.")
    endif ()

    file(SHA256 "${_TSFILE_SIMDE_PARTIAL_ARCHIVE}"
            _TSFILE_SIMDE_ACTUAL_SHA256)
    if (NOT _TSFILE_SIMDE_ACTUAL_SHA256 STREQUAL _TSFILE_SIMDE_SHA256)
        file(REMOVE "${_TSFILE_SIMDE_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Downloaded SIMDe archive has an unexpected SHA-256. "
                "Expected ${_TSFILE_SIMDE_SHA256}, got "
                "${_TSFILE_SIMDE_ACTUAL_SHA256}.")
    endif ()
    file(RENAME "${_TSFILE_SIMDE_PARTIAL_ARCHIVE}"
            "${_TSFILE_SIMDE_ARCHIVE}")
endif ()

set(_TSFILE_SIMDE_EXTRACT_ROOT "${CMAKE_BINARY_DIR}/_deps")
set(TSFILE_SIMDE_SOURCE_DIR
        "${_TSFILE_SIMDE_EXTRACT_ROOT}/simde-${TSFILE_SIMDE_BUNDLED_VERSION}")
set(_TSFILE_SIMDE_STAMP
        "${TSFILE_SIMDE_SOURCE_DIR}/.tsfile-archive-sha256")
set(_TSFILE_SIMDE_EXTRACTED FALSE)
if (EXISTS "${_TSFILE_SIMDE_STAMP}" AND
        EXISTS "${TSFILE_SIMDE_SOURCE_DIR}/simde/simde-common.h" AND
        EXISTS "${TSFILE_SIMDE_SOURCE_DIR}/simde/x86/ssse3.h" AND
        EXISTS "${TSFILE_SIMDE_SOURCE_DIR}/COPYING")
    file(READ "${_TSFILE_SIMDE_STAMP}" _TSFILE_SIMDE_STAMP_SHA256)
    string(STRIP "${_TSFILE_SIMDE_STAMP_SHA256}"
            _TSFILE_SIMDE_STAMP_SHA256)
    if (_TSFILE_SIMDE_STAMP_SHA256 STREQUAL _TSFILE_SIMDE_SHA256)
        set(_TSFILE_SIMDE_EXTRACTED TRUE)
    endif ()
endif ()

if (NOT _TSFILE_SIMDE_EXTRACTED)
    file(REMOVE_RECURSE "${TSFILE_SIMDE_SOURCE_DIR}")
    file(MAKE_DIRECTORY "${_TSFILE_SIMDE_EXTRACT_ROOT}")
    execute_process(
            COMMAND "${CMAKE_COMMAND}" -E tar xzf
                    "${_TSFILE_SIMDE_ARCHIVE}"
            WORKING_DIRECTORY "${_TSFILE_SIMDE_EXTRACT_ROOT}"
            RESULT_VARIABLE _TSFILE_SIMDE_EXTRACT_RESULT
            ERROR_VARIABLE _TSFILE_SIMDE_EXTRACT_ERROR)
    if (NOT _TSFILE_SIMDE_EXTRACT_RESULT EQUAL 0 OR
            NOT EXISTS "${TSFILE_SIMDE_SOURCE_DIR}/simde/simde-common.h" OR
            NOT EXISTS "${TSFILE_SIMDE_SOURCE_DIR}/simde/x86/ssse3.h" OR
            NOT EXISTS "${TSFILE_SIMDE_SOURCE_DIR}/COPYING")
        file(REMOVE_RECURSE "${TSFILE_SIMDE_SOURCE_DIR}")
        message(FATAL_ERROR
                "Failed to extract the SIMDe source archive: "
                "${_TSFILE_SIMDE_EXTRACT_ERROR}")
    endif ()
    file(WRITE "${_TSFILE_SIMDE_STAMP}" "${_TSFILE_SIMDE_SHA256}\n")
endif ()

set(TSFILE_SIMDE_LICENSE_FILE "${TSFILE_SIMDE_SOURCE_DIR}/COPYING")
message(STATUS
        "Using verified SIMDe v${TSFILE_SIMDE_BUNDLED_VERSION} source from "
        "${_TSFILE_SIMDE_ARCHIVE}")

unset(_TSFILE_SIMDE_ACTUAL_SHA256)
unset(_TSFILE_SIMDE_ARCHIVE)
unset(_TSFILE_SIMDE_ARCHIVE_NAME)
unset(_TSFILE_SIMDE_ARCHIVE_VALID)
unset(_TSFILE_SIMDE_DOWNLOAD_MESSAGE)
unset(_TSFILE_SIMDE_DOWNLOAD_RESULT)
unset(_TSFILE_SIMDE_DOWNLOAD_STATUS)
unset(_TSFILE_SIMDE_EXTRACT_ERROR)
unset(_TSFILE_SIMDE_EXTRACT_RESULT)
unset(_TSFILE_SIMDE_EXTRACT_ROOT)
unset(_TSFILE_SIMDE_EXTRACTED)
unset(_TSFILE_SIMDE_PARTIAL_ARCHIVE)
unset(_TSFILE_SIMDE_SHA256)
unset(_TSFILE_SIMDE_STAMP)
unset(_TSFILE_SIMDE_STAMP_SHA256)
unset(_TSFILE_SIMDE_URL)
