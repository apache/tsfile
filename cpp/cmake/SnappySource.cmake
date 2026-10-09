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

set(_TSFILE_SNAPPY_ARCHIVE_NAME
        "snappy-${TSFILE_SNAPPY_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_SNAPPY_URL
        "https://github.com/google/snappy/archive/refs/tags/${TSFILE_SNAPPY_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_SNAPPY_SHA256
        "90f74bc1fbf78a6c56b3c4a082a05103b3a56bb17bca1a27e052ea11723292dc")
set(TSFILE_SNAPPY_ARCHIVE ""
        CACHE FILEPATH
        "Optional pre-downloaded Snappy source archive")

if (TSFILE_SNAPPY_ARCHIVE)
    get_filename_component(_TSFILE_SNAPPY_ARCHIVE
            "${TSFILE_SNAPPY_ARCHIVE}" ABSOLUTE BASE_DIR "${CMAKE_BINARY_DIR}")
    if (NOT EXISTS "${_TSFILE_SNAPPY_ARCHIVE}")
        message(FATAL_ERROR
                "TSFILE_SNAPPY_ARCHIVE does not exist: "
                "${_TSFILE_SNAPPY_ARCHIVE}")
    endif ()
else ()
    file(MAKE_DIRECTORY "${TSFILE_DEPENDENCY_CACHE}")
    set(_TSFILE_SNAPPY_ARCHIVE
            "${TSFILE_DEPENDENCY_CACHE}/${_TSFILE_SNAPPY_ARCHIVE_NAME}")
endif ()

set(_TSFILE_SNAPPY_ARCHIVE_VALID FALSE)
if (EXISTS "${_TSFILE_SNAPPY_ARCHIVE}")
    file(SHA256 "${_TSFILE_SNAPPY_ARCHIVE}"
            _TSFILE_SNAPPY_ACTUAL_SHA256)
    if (_TSFILE_SNAPPY_ACTUAL_SHA256 STREQUAL _TSFILE_SNAPPY_SHA256)
        set(_TSFILE_SNAPPY_ARCHIVE_VALID TRUE)
    elseif (TSFILE_SNAPPY_ARCHIVE OR TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "Snappy source archive has an unexpected SHA-256: "
                "${_TSFILE_SNAPPY_ARCHIVE}\n"
                "Expected: ${_TSFILE_SNAPPY_SHA256}\n"
                "Actual:   ${_TSFILE_SNAPPY_ACTUAL_SHA256}")
    else ()
        message(STATUS
                "Removing invalid cached Snappy archive: "
                "${_TSFILE_SNAPPY_ARCHIVE}")
        file(REMOVE "${_TSFILE_SNAPPY_ARCHIVE}")
    endif ()
endif ()

if (NOT _TSFILE_SNAPPY_ARCHIVE_VALID)
    if (TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "Offline dependency mode requires the verified Snappy "
                "archive at ${_TSFILE_SNAPPY_ARCHIVE}. Pre-download "
                "${_TSFILE_SNAPPY_URL} or set TSFILE_SNAPPY_ARCHIVE to a "
                "verified local archive.")
    endif ()

    set(_TSFILE_SNAPPY_PARTIAL_ARCHIVE
            "${_TSFILE_SNAPPY_ARCHIVE}.part")
    file(REMOVE "${_TSFILE_SNAPPY_PARTIAL_ARCHIVE}")
    message(STATUS "Downloading Snappy v${TSFILE_SNAPPY_BUNDLED_VERSION}")
    file(DOWNLOAD
            "${_TSFILE_SNAPPY_URL}"
            "${_TSFILE_SNAPPY_PARTIAL_ARCHIVE}"
            STATUS _TSFILE_SNAPPY_DOWNLOAD_STATUS
            TLS_VERIFY ON
            TIMEOUT 120
            INACTIVITY_TIMEOUT 30)
    list(GET _TSFILE_SNAPPY_DOWNLOAD_STATUS 0
            _TSFILE_SNAPPY_DOWNLOAD_RESULT)
    list(GET _TSFILE_SNAPPY_DOWNLOAD_STATUS 1
            _TSFILE_SNAPPY_DOWNLOAD_MESSAGE)
    if (NOT _TSFILE_SNAPPY_DOWNLOAD_RESULT EQUAL 0)
        file(REMOVE "${_TSFILE_SNAPPY_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Failed to download Snappy from ${_TSFILE_SNAPPY_URL}: "
                "${_TSFILE_SNAPPY_DOWNLOAD_MESSAGE}. To build without "
                "network access, provide ${_TSFILE_SNAPPY_ARCHIVE_NAME} in "
                "TSFILE_DEPENDENCY_CACHE or set TSFILE_SNAPPY_ARCHIVE.")
    endif ()

    file(SHA256 "${_TSFILE_SNAPPY_PARTIAL_ARCHIVE}"
            _TSFILE_SNAPPY_ACTUAL_SHA256)
    if (NOT _TSFILE_SNAPPY_ACTUAL_SHA256 STREQUAL _TSFILE_SNAPPY_SHA256)
        file(REMOVE "${_TSFILE_SNAPPY_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Downloaded Snappy archive has an unexpected SHA-256. "
                "Expected ${_TSFILE_SNAPPY_SHA256}, got "
                "${_TSFILE_SNAPPY_ACTUAL_SHA256}.")
    endif ()
    file(RENAME "${_TSFILE_SNAPPY_PARTIAL_ARCHIVE}"
            "${_TSFILE_SNAPPY_ARCHIVE}")
endif ()

set(_TSFILE_SNAPPY_EXTRACT_ROOT "${CMAKE_BINARY_DIR}/_deps")
set(TSFILE_SNAPPY_SOURCE_DIR
        "${_TSFILE_SNAPPY_EXTRACT_ROOT}/snappy-${TSFILE_SNAPPY_BUNDLED_VERSION}")
set(_TSFILE_SNAPPY_STAMP
        "${TSFILE_SNAPPY_SOURCE_DIR}/.tsfile-archive-sha256")
set(_TSFILE_SNAPPY_EXTRACTED FALSE)
if (EXISTS "${_TSFILE_SNAPPY_STAMP}" AND
        EXISTS "${TSFILE_SNAPPY_SOURCE_DIR}/CMakeLists.txt" AND
        EXISTS "${TSFILE_SNAPPY_SOURCE_DIR}/snappy.cc" AND
        EXISTS "${TSFILE_SNAPPY_SOURCE_DIR}/snappy.h" AND
        EXISTS "${TSFILE_SNAPPY_SOURCE_DIR}/COPYING")
    file(READ "${_TSFILE_SNAPPY_STAMP}" _TSFILE_SNAPPY_STAMP_SHA256)
    string(STRIP "${_TSFILE_SNAPPY_STAMP_SHA256}"
            _TSFILE_SNAPPY_STAMP_SHA256)
    if (_TSFILE_SNAPPY_STAMP_SHA256 STREQUAL _TSFILE_SNAPPY_SHA256)
        set(_TSFILE_SNAPPY_EXTRACTED TRUE)
    endif ()
endif ()

if (NOT _TSFILE_SNAPPY_EXTRACTED)
    file(REMOVE_RECURSE "${TSFILE_SNAPPY_SOURCE_DIR}")
    file(MAKE_DIRECTORY "${_TSFILE_SNAPPY_EXTRACT_ROOT}")
    execute_process(
            COMMAND "${CMAKE_COMMAND}" -E tar xzf
                    "${_TSFILE_SNAPPY_ARCHIVE}"
            WORKING_DIRECTORY "${_TSFILE_SNAPPY_EXTRACT_ROOT}"
            RESULT_VARIABLE _TSFILE_SNAPPY_EXTRACT_RESULT
            ERROR_VARIABLE _TSFILE_SNAPPY_EXTRACT_ERROR)
    if (NOT _TSFILE_SNAPPY_EXTRACT_RESULT EQUAL 0 OR
            NOT EXISTS "${TSFILE_SNAPPY_SOURCE_DIR}/CMakeLists.txt" OR
            NOT EXISTS "${TSFILE_SNAPPY_SOURCE_DIR}/snappy.cc" OR
            NOT EXISTS "${TSFILE_SNAPPY_SOURCE_DIR}/snappy.h" OR
            NOT EXISTS "${TSFILE_SNAPPY_SOURCE_DIR}/COPYING")
        file(REMOVE_RECURSE "${TSFILE_SNAPPY_SOURCE_DIR}")
        message(FATAL_ERROR
                "Failed to extract the Snappy source archive: "
                "${_TSFILE_SNAPPY_EXTRACT_ERROR}")
    endif ()
    file(WRITE "${_TSFILE_SNAPPY_STAMP}" "${_TSFILE_SNAPPY_SHA256}\n")
endif ()

set(TSFILE_SNAPPY_LICENSE_FILE "${TSFILE_SNAPPY_SOURCE_DIR}/COPYING")
message(STATUS
        "Using verified Snappy v${TSFILE_SNAPPY_BUNDLED_VERSION} source from "
        "${_TSFILE_SNAPPY_ARCHIVE}")

unset(_TSFILE_SNAPPY_ACTUAL_SHA256)
unset(_TSFILE_SNAPPY_ARCHIVE)
unset(_TSFILE_SNAPPY_ARCHIVE_NAME)
unset(_TSFILE_SNAPPY_ARCHIVE_VALID)
unset(_TSFILE_SNAPPY_DOWNLOAD_MESSAGE)
unset(_TSFILE_SNAPPY_DOWNLOAD_RESULT)
unset(_TSFILE_SNAPPY_DOWNLOAD_STATUS)
unset(_TSFILE_SNAPPY_EXTRACT_ERROR)
unset(_TSFILE_SNAPPY_EXTRACT_RESULT)
unset(_TSFILE_SNAPPY_EXTRACT_ROOT)
unset(_TSFILE_SNAPPY_EXTRACTED)
unset(_TSFILE_SNAPPY_PARTIAL_ARCHIVE)
unset(_TSFILE_SNAPPY_SHA256)
unset(_TSFILE_SNAPPY_STAMP)
unset(_TSFILE_SNAPPY_STAMP_SHA256)
unset(_TSFILE_SNAPPY_URL)
