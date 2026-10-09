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

set(_TSFILE_ZSTD_ARCHIVE_NAME
        "zstd-v${TSFILE_ZSTD_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_ZSTD_URL
        "https://github.com/facebook/zstd/archive/refs/tags/v${TSFILE_ZSTD_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_ZSTD_SHA256
        "37d7284556b20954e56e1ca85b80226768902e2edabd3b649e9e72c0c9012ee3")
set(TSFILE_ZSTD_ARCHIVE ""
        CACHE FILEPATH
        "Optional pre-downloaded Zstandard source archive")

if (TSFILE_ZSTD_ARCHIVE)
    get_filename_component(_TSFILE_ZSTD_ARCHIVE
            "${TSFILE_ZSTD_ARCHIVE}" ABSOLUTE BASE_DIR "${CMAKE_BINARY_DIR}")
    if (NOT EXISTS "${_TSFILE_ZSTD_ARCHIVE}")
        message(FATAL_ERROR
                "TSFILE_ZSTD_ARCHIVE does not exist: "
                "${_TSFILE_ZSTD_ARCHIVE}")
    endif ()
else ()
    file(MAKE_DIRECTORY "${TSFILE_DEPENDENCY_CACHE}")
    set(_TSFILE_ZSTD_ARCHIVE
            "${TSFILE_DEPENDENCY_CACHE}/${_TSFILE_ZSTD_ARCHIVE_NAME}")
endif ()

set(_TSFILE_ZSTD_ARCHIVE_VALID FALSE)
if (EXISTS "${_TSFILE_ZSTD_ARCHIVE}")
    file(SHA256 "${_TSFILE_ZSTD_ARCHIVE}" _TSFILE_ZSTD_ACTUAL_SHA256)
    if (_TSFILE_ZSTD_ACTUAL_SHA256 STREQUAL _TSFILE_ZSTD_SHA256)
        set(_TSFILE_ZSTD_ARCHIVE_VALID TRUE)
    elseif (TSFILE_ZSTD_ARCHIVE OR TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "Zstandard source archive has an unexpected SHA-256: "
                "${_TSFILE_ZSTD_ARCHIVE}\n"
                "Expected: ${_TSFILE_ZSTD_SHA256}\n"
                "Actual:   ${_TSFILE_ZSTD_ACTUAL_SHA256}")
    else ()
        message(STATUS
                "Removing invalid cached Zstandard archive: "
                "${_TSFILE_ZSTD_ARCHIVE}")
        file(REMOVE "${_TSFILE_ZSTD_ARCHIVE}")
    endif ()
endif ()

if (NOT _TSFILE_ZSTD_ARCHIVE_VALID)
    if (TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "Offline dependency mode requires the verified Zstandard "
                "archive at ${_TSFILE_ZSTD_ARCHIVE}. Pre-download "
                "${_TSFILE_ZSTD_URL} or set TSFILE_ZSTD_ARCHIVE to a "
                "verified local archive.")
    endif ()

    set(_TSFILE_ZSTD_PARTIAL_ARCHIVE "${_TSFILE_ZSTD_ARCHIVE}.part")
    file(REMOVE "${_TSFILE_ZSTD_PARTIAL_ARCHIVE}")
    message(STATUS
            "Downloading Zstandard v${TSFILE_ZSTD_BUNDLED_VERSION}")
    file(DOWNLOAD
            "${_TSFILE_ZSTD_URL}"
            "${_TSFILE_ZSTD_PARTIAL_ARCHIVE}"
            STATUS _TSFILE_ZSTD_DOWNLOAD_STATUS
            TLS_VERIFY ON
            TIMEOUT 120
            INACTIVITY_TIMEOUT 30)
    list(GET _TSFILE_ZSTD_DOWNLOAD_STATUS 0
            _TSFILE_ZSTD_DOWNLOAD_RESULT)
    list(GET _TSFILE_ZSTD_DOWNLOAD_STATUS 1
            _TSFILE_ZSTD_DOWNLOAD_MESSAGE)
    if (NOT _TSFILE_ZSTD_DOWNLOAD_RESULT EQUAL 0)
        file(REMOVE "${_TSFILE_ZSTD_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Failed to download Zstandard from ${_TSFILE_ZSTD_URL}: "
                "${_TSFILE_ZSTD_DOWNLOAD_MESSAGE}. To build without "
                "network access, provide ${_TSFILE_ZSTD_ARCHIVE_NAME} in "
                "TSFILE_DEPENDENCY_CACHE or set TSFILE_ZSTD_ARCHIVE.")
    endif ()

    file(SHA256 "${_TSFILE_ZSTD_PARTIAL_ARCHIVE}"
            _TSFILE_ZSTD_ACTUAL_SHA256)
    if (NOT _TSFILE_ZSTD_ACTUAL_SHA256 STREQUAL _TSFILE_ZSTD_SHA256)
        file(REMOVE "${_TSFILE_ZSTD_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Downloaded Zstandard archive has an unexpected SHA-256. "
                "Expected ${_TSFILE_ZSTD_SHA256}, got "
                "${_TSFILE_ZSTD_ACTUAL_SHA256}.")
    endif ()
    file(RENAME "${_TSFILE_ZSTD_PARTIAL_ARCHIVE}"
            "${_TSFILE_ZSTD_ARCHIVE}")
endif ()

set(_TSFILE_ZSTD_EXTRACT_ROOT "${CMAKE_BINARY_DIR}/_deps")
set(TSFILE_ZSTD_SOURCE_DIR
        "${_TSFILE_ZSTD_EXTRACT_ROOT}/zstd-${TSFILE_ZSTD_BUNDLED_VERSION}")
set(_TSFILE_ZSTD_STAMP
        "${TSFILE_ZSTD_SOURCE_DIR}/.tsfile-archive-sha256")
set(_TSFILE_ZSTD_EXTRACTED FALSE)
if (EXISTS "${_TSFILE_ZSTD_STAMP}" AND
        EXISTS "${TSFILE_ZSTD_SOURCE_DIR}/build/cmake/CMakeLists.txt" AND
        EXISTS "${TSFILE_ZSTD_SOURCE_DIR}/lib/common/zstd_common.c" AND
        EXISTS "${TSFILE_ZSTD_SOURCE_DIR}/lib/zstd.h" AND
        EXISTS "${TSFILE_ZSTD_SOURCE_DIR}/LICENSE")
    file(READ "${_TSFILE_ZSTD_STAMP}" _TSFILE_ZSTD_STAMP_SHA256)
    string(STRIP "${_TSFILE_ZSTD_STAMP_SHA256}"
            _TSFILE_ZSTD_STAMP_SHA256)
    if (_TSFILE_ZSTD_STAMP_SHA256 STREQUAL _TSFILE_ZSTD_SHA256)
        set(_TSFILE_ZSTD_EXTRACTED TRUE)
    endif ()
endif ()

if (NOT _TSFILE_ZSTD_EXTRACTED)
    file(REMOVE_RECURSE "${TSFILE_ZSTD_SOURCE_DIR}")
    file(MAKE_DIRECTORY "${_TSFILE_ZSTD_EXTRACT_ROOT}")
    execute_process(
            COMMAND "${CMAKE_COMMAND}" -E tar xzf
                    "${_TSFILE_ZSTD_ARCHIVE}"
            WORKING_DIRECTORY "${_TSFILE_ZSTD_EXTRACT_ROOT}"
            RESULT_VARIABLE _TSFILE_ZSTD_EXTRACT_RESULT
            ERROR_VARIABLE _TSFILE_ZSTD_EXTRACT_ERROR)
    if (NOT _TSFILE_ZSTD_EXTRACT_RESULT EQUAL 0 OR
            NOT EXISTS "${TSFILE_ZSTD_SOURCE_DIR}/build/cmake/CMakeLists.txt" OR
            NOT EXISTS "${TSFILE_ZSTD_SOURCE_DIR}/lib/common/zstd_common.c" OR
            NOT EXISTS "${TSFILE_ZSTD_SOURCE_DIR}/lib/zstd.h" OR
            NOT EXISTS "${TSFILE_ZSTD_SOURCE_DIR}/LICENSE")
        file(REMOVE_RECURSE "${TSFILE_ZSTD_SOURCE_DIR}")
        message(FATAL_ERROR
                "Failed to extract the Zstandard source archive: "
                "${_TSFILE_ZSTD_EXTRACT_ERROR}")
    endif ()
    file(WRITE "${_TSFILE_ZSTD_STAMP}" "${_TSFILE_ZSTD_SHA256}\n")
endif ()

set(TSFILE_ZSTD_LICENSE_FILE "${TSFILE_ZSTD_SOURCE_DIR}/LICENSE")
message(STATUS
        "Using verified Zstandard v${TSFILE_ZSTD_BUNDLED_VERSION} source "
        "from ${_TSFILE_ZSTD_ARCHIVE}")

unset(_TSFILE_ZSTD_ACTUAL_SHA256)
unset(_TSFILE_ZSTD_ARCHIVE)
unset(_TSFILE_ZSTD_ARCHIVE_NAME)
unset(_TSFILE_ZSTD_ARCHIVE_VALID)
unset(_TSFILE_ZSTD_DOWNLOAD_MESSAGE)
unset(_TSFILE_ZSTD_DOWNLOAD_RESULT)
unset(_TSFILE_ZSTD_DOWNLOAD_STATUS)
unset(_TSFILE_ZSTD_EXTRACT_ERROR)
unset(_TSFILE_ZSTD_EXTRACT_RESULT)
unset(_TSFILE_ZSTD_EXTRACT_ROOT)
unset(_TSFILE_ZSTD_EXTRACTED)
unset(_TSFILE_ZSTD_PARTIAL_ARCHIVE)
unset(_TSFILE_ZSTD_SHA256)
unset(_TSFILE_ZSTD_STAMP)
unset(_TSFILE_ZSTD_STAMP_SHA256)
unset(_TSFILE_ZSTD_URL)
