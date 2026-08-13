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

set(_TSFILE_LZ4_ARCHIVE_NAME "lz4-v${TSFILE_LZ4_MIN_VERSION}.tar.gz")
set(_TSFILE_LZ4_URL
        "https://github.com/lz4/lz4/archive/refs/tags/v${TSFILE_LZ4_MIN_VERSION}.tar.gz")
set(_TSFILE_LZ4_SHA256
        "0b0e3aa07c8c063ddf40b082bdf7e37a1562bda40a0ff5272957f3e987e0e54b")
set(TSFILE_LZ4_ARCHIVE ""
        CACHE FILEPATH
        "Optional pre-downloaded LZ4 v${TSFILE_LZ4_MIN_VERSION} source archive")

if (TSFILE_LZ4_ARCHIVE)
    get_filename_component(_TSFILE_LZ4_ARCHIVE
            "${TSFILE_LZ4_ARCHIVE}" ABSOLUTE BASE_DIR "${CMAKE_BINARY_DIR}")
    if (NOT EXISTS "${_TSFILE_LZ4_ARCHIVE}")
        message(FATAL_ERROR
                "TSFILE_LZ4_ARCHIVE does not exist: ${_TSFILE_LZ4_ARCHIVE}")
    endif ()
else ()
    file(MAKE_DIRECTORY "${TSFILE_DEPENDENCY_CACHE}")
    set(_TSFILE_LZ4_ARCHIVE
            "${TSFILE_DEPENDENCY_CACHE}/${_TSFILE_LZ4_ARCHIVE_NAME}")
endif ()

set(_TSFILE_LZ4_ARCHIVE_VALID FALSE)
if (EXISTS "${_TSFILE_LZ4_ARCHIVE}")
    file(SHA256 "${_TSFILE_LZ4_ARCHIVE}" _TSFILE_LZ4_ACTUAL_SHA256)
    if (_TSFILE_LZ4_ACTUAL_SHA256 STREQUAL _TSFILE_LZ4_SHA256)
        set(_TSFILE_LZ4_ARCHIVE_VALID TRUE)
    elseif (TSFILE_LZ4_ARCHIVE OR TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "LZ4 source archive has an unexpected SHA-256: "
                "${_TSFILE_LZ4_ARCHIVE}\n"
                "Expected: ${_TSFILE_LZ4_SHA256}\n"
                "Actual:   ${_TSFILE_LZ4_ACTUAL_SHA256}")
    else ()
        message(STATUS
                "Removing invalid cached LZ4 archive: ${_TSFILE_LZ4_ARCHIVE}")
        file(REMOVE "${_TSFILE_LZ4_ARCHIVE}")
    endif ()
endif ()

if (NOT _TSFILE_LZ4_ARCHIVE_VALID)
    if (TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "Offline dependency mode requires the verified LZ4 archive at "
                "${_TSFILE_LZ4_ARCHIVE}. Pre-download ${_TSFILE_LZ4_URL} "
                "or set TSFILE_LZ4_ARCHIVE to a verified local archive.")
    endif ()

    set(_TSFILE_LZ4_PARTIAL_ARCHIVE "${_TSFILE_LZ4_ARCHIVE}.part")
    file(REMOVE "${_TSFILE_LZ4_PARTIAL_ARCHIVE}")
    message(STATUS "Downloading LZ4 v${TSFILE_LZ4_MIN_VERSION}")
    file(DOWNLOAD
            "${_TSFILE_LZ4_URL}"
            "${_TSFILE_LZ4_PARTIAL_ARCHIVE}"
            STATUS _TSFILE_LZ4_DOWNLOAD_STATUS
            TLS_VERIFY ON
            TIMEOUT 120
            INACTIVITY_TIMEOUT 30)
    list(GET _TSFILE_LZ4_DOWNLOAD_STATUS 0 _TSFILE_LZ4_DOWNLOAD_RESULT)
    list(GET _TSFILE_LZ4_DOWNLOAD_STATUS 1 _TSFILE_LZ4_DOWNLOAD_MESSAGE)
    if (NOT _TSFILE_LZ4_DOWNLOAD_RESULT EQUAL 0)
        file(REMOVE "${_TSFILE_LZ4_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Failed to download LZ4 from ${_TSFILE_LZ4_URL}: "
                "${_TSFILE_LZ4_DOWNLOAD_MESSAGE}. To build without network "
                "access, provide ${_TSFILE_LZ4_ARCHIVE_NAME} in "
                "TSFILE_DEPENDENCY_CACHE or set TSFILE_LZ4_ARCHIVE.")
    endif ()

    file(SHA256 "${_TSFILE_LZ4_PARTIAL_ARCHIVE}"
            _TSFILE_LZ4_ACTUAL_SHA256)
    if (NOT _TSFILE_LZ4_ACTUAL_SHA256 STREQUAL _TSFILE_LZ4_SHA256)
        file(REMOVE "${_TSFILE_LZ4_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Downloaded LZ4 archive has an unexpected SHA-256. "
                "Expected ${_TSFILE_LZ4_SHA256}, got "
                "${_TSFILE_LZ4_ACTUAL_SHA256}.")
    endif ()
    file(RENAME "${_TSFILE_LZ4_PARTIAL_ARCHIVE}"
            "${_TSFILE_LZ4_ARCHIVE}")
endif ()

set(_TSFILE_LZ4_EXTRACT_ROOT "${CMAKE_BINARY_DIR}/_deps")
set(TSFILE_LZ4_SOURCE_DIR
        "${_TSFILE_LZ4_EXTRACT_ROOT}/lz4-${TSFILE_LZ4_MIN_VERSION}")
set(_TSFILE_LZ4_STAMP "${TSFILE_LZ4_SOURCE_DIR}/.tsfile-archive-sha256")
set(_TSFILE_LZ4_EXTRACTED FALSE)
if (EXISTS "${_TSFILE_LZ4_STAMP}" AND
        EXISTS "${TSFILE_LZ4_SOURCE_DIR}/lib/lz4.c" AND
        EXISTS "${TSFILE_LZ4_SOURCE_DIR}/lib/lz4.h" AND
        EXISTS "${TSFILE_LZ4_SOURCE_DIR}/lib/LICENSE")
    file(READ "${_TSFILE_LZ4_STAMP}" _TSFILE_LZ4_STAMP_SHA256)
    string(STRIP "${_TSFILE_LZ4_STAMP_SHA256}"
            _TSFILE_LZ4_STAMP_SHA256)
    if (_TSFILE_LZ4_STAMP_SHA256 STREQUAL _TSFILE_LZ4_SHA256)
        set(_TSFILE_LZ4_EXTRACTED TRUE)
    endif ()
endif ()

if (NOT _TSFILE_LZ4_EXTRACTED)
    file(REMOVE_RECURSE "${TSFILE_LZ4_SOURCE_DIR}")
    file(MAKE_DIRECTORY "${_TSFILE_LZ4_EXTRACT_ROOT}")
    execute_process(
            COMMAND "${CMAKE_COMMAND}" -E tar xzf "${_TSFILE_LZ4_ARCHIVE}"
            WORKING_DIRECTORY "${_TSFILE_LZ4_EXTRACT_ROOT}"
            RESULT_VARIABLE _TSFILE_LZ4_EXTRACT_RESULT
            ERROR_VARIABLE _TSFILE_LZ4_EXTRACT_ERROR)
    if (NOT _TSFILE_LZ4_EXTRACT_RESULT EQUAL 0 OR
            NOT EXISTS "${TSFILE_LZ4_SOURCE_DIR}/lib/lz4.c" OR
            NOT EXISTS "${TSFILE_LZ4_SOURCE_DIR}/lib/lz4.h" OR
            NOT EXISTS "${TSFILE_LZ4_SOURCE_DIR}/lib/LICENSE")
        file(REMOVE_RECURSE "${TSFILE_LZ4_SOURCE_DIR}")
        message(FATAL_ERROR
                "Failed to extract the LZ4 source archive: "
                "${_TSFILE_LZ4_EXTRACT_ERROR}")
    endif ()
    file(WRITE "${_TSFILE_LZ4_STAMP}" "${_TSFILE_LZ4_SHA256}\n")
endif ()

set(TSFILE_LZ4_LICENSE_FILE "${TSFILE_LZ4_SOURCE_DIR}/lib/LICENSE")
message(STATUS
        "Using verified LZ4 v${TSFILE_LZ4_MIN_VERSION} source from "
        "${_TSFILE_LZ4_ARCHIVE}")

unset(_TSFILE_LZ4_ACTUAL_SHA256)
unset(_TSFILE_LZ4_ARCHIVE)
unset(_TSFILE_LZ4_ARCHIVE_NAME)
unset(_TSFILE_LZ4_ARCHIVE_VALID)
unset(_TSFILE_LZ4_DOWNLOAD_MESSAGE)
unset(_TSFILE_LZ4_DOWNLOAD_RESULT)
unset(_TSFILE_LZ4_DOWNLOAD_STATUS)
unset(_TSFILE_LZ4_EXTRACT_ERROR)
unset(_TSFILE_LZ4_EXTRACT_RESULT)
unset(_TSFILE_LZ4_EXTRACT_ROOT)
unset(_TSFILE_LZ4_EXTRACTED)
unset(_TSFILE_LZ4_PARTIAL_ARCHIVE)
unset(_TSFILE_LZ4_SHA256)
unset(_TSFILE_LZ4_STAMP)
unset(_TSFILE_LZ4_STAMP_SHA256)
unset(_TSFILE_LZ4_URL)
