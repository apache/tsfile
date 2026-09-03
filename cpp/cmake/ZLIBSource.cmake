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

set(_TSFILE_ZLIB_ARCHIVE_NAME
        "zlib-v${TSFILE_ZLIB_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_ZLIB_URL
        "https://github.com/madler/zlib/archive/refs/tags/v${TSFILE_ZLIB_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_ZLIB_SHA256
        "17e88863f3600672ab49182f217281b6fc4d3c762bde361935e436a95214d05c")
set(TSFILE_ZLIB_ARCHIVE ""
        CACHE FILEPATH
        "Optional pre-downloaded zlib v${TSFILE_ZLIB_BUNDLED_VERSION} source archive")

if (TSFILE_ZLIB_ARCHIVE)
    get_filename_component(_TSFILE_ZLIB_ARCHIVE
            "${TSFILE_ZLIB_ARCHIVE}" ABSOLUTE BASE_DIR "${CMAKE_BINARY_DIR}")
    if (NOT EXISTS "${_TSFILE_ZLIB_ARCHIVE}")
        message(FATAL_ERROR
                "TSFILE_ZLIB_ARCHIVE does not exist: ${_TSFILE_ZLIB_ARCHIVE}")
    endif ()
else ()
    file(MAKE_DIRECTORY "${TSFILE_DEPENDENCY_CACHE}")
    set(_TSFILE_ZLIB_ARCHIVE
            "${TSFILE_DEPENDENCY_CACHE}/${_TSFILE_ZLIB_ARCHIVE_NAME}")
endif ()

set(_TSFILE_ZLIB_ARCHIVE_VALID FALSE)
if (EXISTS "${_TSFILE_ZLIB_ARCHIVE}")
    file(SHA256 "${_TSFILE_ZLIB_ARCHIVE}" _TSFILE_ZLIB_ACTUAL_SHA256)
    if (_TSFILE_ZLIB_ACTUAL_SHA256 STREQUAL _TSFILE_ZLIB_SHA256)
        set(_TSFILE_ZLIB_ARCHIVE_VALID TRUE)
    elseif (TSFILE_ZLIB_ARCHIVE OR TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "zlib source archive has an unexpected SHA-256: "
                "${_TSFILE_ZLIB_ARCHIVE}\n"
                "Expected: ${_TSFILE_ZLIB_SHA256}\n"
                "Actual:   ${_TSFILE_ZLIB_ACTUAL_SHA256}")
    else ()
        message(STATUS
                "Removing invalid cached zlib archive: "
                "${_TSFILE_ZLIB_ARCHIVE}")
        file(REMOVE "${_TSFILE_ZLIB_ARCHIVE}")
    endif ()
endif ()

if (NOT _TSFILE_ZLIB_ARCHIVE_VALID)
    if (TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "Offline dependency mode requires the verified zlib archive "
                "at ${_TSFILE_ZLIB_ARCHIVE}. Pre-download "
                "${_TSFILE_ZLIB_URL} or set TSFILE_ZLIB_ARCHIVE to a "
                "verified local archive.")
    endif ()

    set(_TSFILE_ZLIB_PARTIAL_ARCHIVE "${_TSFILE_ZLIB_ARCHIVE}.part")
    file(REMOVE "${_TSFILE_ZLIB_PARTIAL_ARCHIVE}")
    message(STATUS "Downloading zlib v${TSFILE_ZLIB_BUNDLED_VERSION}")
    file(DOWNLOAD
            "${_TSFILE_ZLIB_URL}"
            "${_TSFILE_ZLIB_PARTIAL_ARCHIVE}"
            STATUS _TSFILE_ZLIB_DOWNLOAD_STATUS
            TLS_VERIFY ON
            TIMEOUT 120
            INACTIVITY_TIMEOUT 30)
    list(GET _TSFILE_ZLIB_DOWNLOAD_STATUS 0 _TSFILE_ZLIB_DOWNLOAD_RESULT)
    list(GET _TSFILE_ZLIB_DOWNLOAD_STATUS 1 _TSFILE_ZLIB_DOWNLOAD_MESSAGE)
    if (NOT _TSFILE_ZLIB_DOWNLOAD_RESULT EQUAL 0)
        file(REMOVE "${_TSFILE_ZLIB_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Failed to download zlib from ${_TSFILE_ZLIB_URL}: "
                "${_TSFILE_ZLIB_DOWNLOAD_MESSAGE}. To build without network "
                "access, provide ${_TSFILE_ZLIB_ARCHIVE_NAME} in "
                "TSFILE_DEPENDENCY_CACHE or set TSFILE_ZLIB_ARCHIVE.")
    endif ()

    file(SHA256 "${_TSFILE_ZLIB_PARTIAL_ARCHIVE}"
            _TSFILE_ZLIB_ACTUAL_SHA256)
    if (NOT _TSFILE_ZLIB_ACTUAL_SHA256 STREQUAL _TSFILE_ZLIB_SHA256)
        file(REMOVE "${_TSFILE_ZLIB_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Downloaded zlib archive has an unexpected SHA-256. "
                "Expected ${_TSFILE_ZLIB_SHA256}, got "
                "${_TSFILE_ZLIB_ACTUAL_SHA256}.")
    endif ()
    file(RENAME "${_TSFILE_ZLIB_PARTIAL_ARCHIVE}"
            "${_TSFILE_ZLIB_ARCHIVE}")
endif ()

set(_TSFILE_ZLIB_EXTRACT_ROOT "${CMAKE_BINARY_DIR}/_deps")
set(TSFILE_ZLIB_SOURCE_DIR
        "${_TSFILE_ZLIB_EXTRACT_ROOT}/zlib-${TSFILE_ZLIB_BUNDLED_VERSION}")
set(_TSFILE_ZLIB_STAMP
        "${TSFILE_ZLIB_SOURCE_DIR}/.tsfile-archive-sha256")
set(_TSFILE_ZLIB_EXTRACTED FALSE)
if (EXISTS "${_TSFILE_ZLIB_STAMP}" AND
        EXISTS "${TSFILE_ZLIB_SOURCE_DIR}/CMakeLists.txt" AND
        EXISTS "${TSFILE_ZLIB_SOURCE_DIR}/zlib.h" AND
        EXISTS "${TSFILE_ZLIB_SOURCE_DIR}/LICENSE")
    file(READ "${_TSFILE_ZLIB_STAMP}" _TSFILE_ZLIB_STAMP_SHA256)
    string(STRIP "${_TSFILE_ZLIB_STAMP_SHA256}"
            _TSFILE_ZLIB_STAMP_SHA256)
    if (_TSFILE_ZLIB_STAMP_SHA256 STREQUAL _TSFILE_ZLIB_SHA256)
        set(_TSFILE_ZLIB_EXTRACTED TRUE)
    endif ()
endif ()

if (NOT _TSFILE_ZLIB_EXTRACTED)
    file(REMOVE_RECURSE "${TSFILE_ZLIB_SOURCE_DIR}")
    file(MAKE_DIRECTORY "${_TSFILE_ZLIB_EXTRACT_ROOT}")
    execute_process(
            COMMAND "${CMAKE_COMMAND}" -E tar xzf "${_TSFILE_ZLIB_ARCHIVE}"
            WORKING_DIRECTORY "${_TSFILE_ZLIB_EXTRACT_ROOT}"
            RESULT_VARIABLE _TSFILE_ZLIB_EXTRACT_RESULT
            ERROR_VARIABLE _TSFILE_ZLIB_EXTRACT_ERROR)
    if (NOT _TSFILE_ZLIB_EXTRACT_RESULT EQUAL 0 OR
            NOT EXISTS "${TSFILE_ZLIB_SOURCE_DIR}/CMakeLists.txt" OR
            NOT EXISTS "${TSFILE_ZLIB_SOURCE_DIR}/zlib.h" OR
            NOT EXISTS "${TSFILE_ZLIB_SOURCE_DIR}/LICENSE")
        file(REMOVE_RECURSE "${TSFILE_ZLIB_SOURCE_DIR}")
        message(FATAL_ERROR
                "Failed to extract the zlib source archive: "
                "${_TSFILE_ZLIB_EXTRACT_ERROR}")
    endif ()
    file(WRITE "${_TSFILE_ZLIB_STAMP}" "${_TSFILE_ZLIB_SHA256}\n")
endif ()

set(TSFILE_ZLIB_LICENSE_FILE "${TSFILE_ZLIB_SOURCE_DIR}/LICENSE")
message(STATUS
        "Using verified zlib v${TSFILE_ZLIB_BUNDLED_VERSION} source from "
        "${_TSFILE_ZLIB_ARCHIVE}")

unset(_TSFILE_ZLIB_ACTUAL_SHA256)
unset(_TSFILE_ZLIB_ARCHIVE)
unset(_TSFILE_ZLIB_ARCHIVE_NAME)
unset(_TSFILE_ZLIB_ARCHIVE_VALID)
unset(_TSFILE_ZLIB_DOWNLOAD_MESSAGE)
unset(_TSFILE_ZLIB_DOWNLOAD_RESULT)
unset(_TSFILE_ZLIB_DOWNLOAD_STATUS)
unset(_TSFILE_ZLIB_EXTRACT_ERROR)
unset(_TSFILE_ZLIB_EXTRACT_RESULT)
unset(_TSFILE_ZLIB_EXTRACT_ROOT)
unset(_TSFILE_ZLIB_EXTRACTED)
unset(_TSFILE_ZLIB_PARTIAL_ARCHIVE)
unset(_TSFILE_ZLIB_SHA256)
unset(_TSFILE_ZLIB_STAMP)
unset(_TSFILE_ZLIB_STAMP_SHA256)
unset(_TSFILE_ZLIB_URL)
