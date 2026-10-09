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

set(_TSFILE_LZOKAY_ARCHIVE_NAME
        "lzokay-${TSFILE_LZOKAY_COMMIT}.tar.gz")
set(_TSFILE_LZOKAY_URL
        "https://github.com/AxioDL/lzokay/archive/${TSFILE_LZOKAY_COMMIT}.tar.gz")
set(_TSFILE_LZOKAY_SHA256
        "eb518bf793da0b4420a3ffdf1511851575bc62ef350b303f14ff7355f370da6a")
set(TSFILE_LZOKAY_ARCHIVE ""
        CACHE FILEPATH
        "Optional pre-downloaded lzokay source archive")

if (TSFILE_LZOKAY_ARCHIVE)
    get_filename_component(_TSFILE_LZOKAY_ARCHIVE
            "${TSFILE_LZOKAY_ARCHIVE}" ABSOLUTE BASE_DIR "${CMAKE_BINARY_DIR}")
    if (NOT EXISTS "${_TSFILE_LZOKAY_ARCHIVE}")
        message(FATAL_ERROR
                "TSFILE_LZOKAY_ARCHIVE does not exist: "
                "${_TSFILE_LZOKAY_ARCHIVE}")
    endif ()
else ()
    file(MAKE_DIRECTORY "${TSFILE_DEPENDENCY_CACHE}")
    set(_TSFILE_LZOKAY_ARCHIVE
            "${TSFILE_DEPENDENCY_CACHE}/${_TSFILE_LZOKAY_ARCHIVE_NAME}")
endif ()

set(_TSFILE_LZOKAY_ARCHIVE_VALID FALSE)
if (EXISTS "${_TSFILE_LZOKAY_ARCHIVE}")
    file(SHA256 "${_TSFILE_LZOKAY_ARCHIVE}"
            _TSFILE_LZOKAY_ACTUAL_SHA256)
    if (_TSFILE_LZOKAY_ACTUAL_SHA256 STREQUAL _TSFILE_LZOKAY_SHA256)
        set(_TSFILE_LZOKAY_ARCHIVE_VALID TRUE)
    elseif (TSFILE_LZOKAY_ARCHIVE OR TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "lzokay source archive has an unexpected SHA-256: "
                "${_TSFILE_LZOKAY_ARCHIVE}\n"
                "Expected: ${_TSFILE_LZOKAY_SHA256}\n"
                "Actual:   ${_TSFILE_LZOKAY_ACTUAL_SHA256}")
    else ()
        message(STATUS
                "Removing invalid cached lzokay archive: "
                "${_TSFILE_LZOKAY_ARCHIVE}")
        file(REMOVE "${_TSFILE_LZOKAY_ARCHIVE}")
    endif ()
endif ()

if (NOT _TSFILE_LZOKAY_ARCHIVE_VALID)
    if (TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "Offline dependency mode requires the verified lzokay archive "
                "at ${_TSFILE_LZOKAY_ARCHIVE}. Pre-download "
                "${_TSFILE_LZOKAY_URL} or set TSFILE_LZOKAY_ARCHIVE to a "
                "verified local archive.")
    endif ()

    set(_TSFILE_LZOKAY_PARTIAL_ARCHIVE
            "${_TSFILE_LZOKAY_ARCHIVE}.part")
    file(REMOVE "${_TSFILE_LZOKAY_PARTIAL_ARCHIVE}")
    message(STATUS "Downloading lzokay ${TSFILE_LZOKAY_COMMIT}")
    file(DOWNLOAD
            "${_TSFILE_LZOKAY_URL}"
            "${_TSFILE_LZOKAY_PARTIAL_ARCHIVE}"
            STATUS _TSFILE_LZOKAY_DOWNLOAD_STATUS
            TLS_VERIFY ON
            TIMEOUT 120
            INACTIVITY_TIMEOUT 30)
    list(GET _TSFILE_LZOKAY_DOWNLOAD_STATUS 0
            _TSFILE_LZOKAY_DOWNLOAD_RESULT)
    list(GET _TSFILE_LZOKAY_DOWNLOAD_STATUS 1
            _TSFILE_LZOKAY_DOWNLOAD_MESSAGE)
    if (NOT _TSFILE_LZOKAY_DOWNLOAD_RESULT EQUAL 0)
        file(REMOVE "${_TSFILE_LZOKAY_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Failed to download lzokay from ${_TSFILE_LZOKAY_URL}: "
                "${_TSFILE_LZOKAY_DOWNLOAD_MESSAGE}. To build without "
                "network access, provide ${_TSFILE_LZOKAY_ARCHIVE_NAME} in "
                "TSFILE_DEPENDENCY_CACHE or set TSFILE_LZOKAY_ARCHIVE.")
    endif ()

    file(SHA256 "${_TSFILE_LZOKAY_PARTIAL_ARCHIVE}"
            _TSFILE_LZOKAY_ACTUAL_SHA256)
    if (NOT _TSFILE_LZOKAY_ACTUAL_SHA256 STREQUAL _TSFILE_LZOKAY_SHA256)
        file(REMOVE "${_TSFILE_LZOKAY_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Downloaded lzokay archive has an unexpected SHA-256. "
                "Expected ${_TSFILE_LZOKAY_SHA256}, got "
                "${_TSFILE_LZOKAY_ACTUAL_SHA256}.")
    endif ()
    file(RENAME "${_TSFILE_LZOKAY_PARTIAL_ARCHIVE}"
            "${_TSFILE_LZOKAY_ARCHIVE}")
endif ()

set(_TSFILE_LZOKAY_EXTRACT_ROOT "${CMAKE_BINARY_DIR}/_deps")
set(TSFILE_LZOKAY_SOURCE_DIR
        "${_TSFILE_LZOKAY_EXTRACT_ROOT}/lzokay-${TSFILE_LZOKAY_COMMIT}")
set(_TSFILE_LZOKAY_STAMP
        "${TSFILE_LZOKAY_SOURCE_DIR}/.tsfile-archive-sha256")
set(_TSFILE_LZOKAY_EXTRACTED FALSE)
if (EXISTS "${_TSFILE_LZOKAY_STAMP}" AND
        EXISTS "${TSFILE_LZOKAY_SOURCE_DIR}/lzokay.cpp" AND
        EXISTS "${TSFILE_LZOKAY_SOURCE_DIR}/lzokay.hpp" AND
        EXISTS "${TSFILE_LZOKAY_SOURCE_DIR}/LICENSE")
    file(READ "${_TSFILE_LZOKAY_STAMP}" _TSFILE_LZOKAY_STAMP_SHA256)
    string(STRIP "${_TSFILE_LZOKAY_STAMP_SHA256}"
            _TSFILE_LZOKAY_STAMP_SHA256)
    if (_TSFILE_LZOKAY_STAMP_SHA256 STREQUAL _TSFILE_LZOKAY_SHA256)
        set(_TSFILE_LZOKAY_EXTRACTED TRUE)
    endif ()
endif ()

if (NOT _TSFILE_LZOKAY_EXTRACTED)
    file(REMOVE_RECURSE "${TSFILE_LZOKAY_SOURCE_DIR}")
    file(MAKE_DIRECTORY "${_TSFILE_LZOKAY_EXTRACT_ROOT}")
    execute_process(
            COMMAND "${CMAKE_COMMAND}" -E tar xzf
                    "${_TSFILE_LZOKAY_ARCHIVE}"
            WORKING_DIRECTORY "${_TSFILE_LZOKAY_EXTRACT_ROOT}"
            RESULT_VARIABLE _TSFILE_LZOKAY_EXTRACT_RESULT
            ERROR_VARIABLE _TSFILE_LZOKAY_EXTRACT_ERROR)
    if (NOT _TSFILE_LZOKAY_EXTRACT_RESULT EQUAL 0 OR
            NOT EXISTS "${TSFILE_LZOKAY_SOURCE_DIR}/lzokay.cpp" OR
            NOT EXISTS "${TSFILE_LZOKAY_SOURCE_DIR}/lzokay.hpp" OR
            NOT EXISTS "${TSFILE_LZOKAY_SOURCE_DIR}/LICENSE")
        file(REMOVE_RECURSE "${TSFILE_LZOKAY_SOURCE_DIR}")
        message(FATAL_ERROR
                "Failed to extract the lzokay source archive: "
                "${_TSFILE_LZOKAY_EXTRACT_ERROR}")
    endif ()
    file(WRITE "${_TSFILE_LZOKAY_STAMP}" "${_TSFILE_LZOKAY_SHA256}\n")
endif ()

set(TSFILE_LZOKAY_LICENSE_FILE "${TSFILE_LZOKAY_SOURCE_DIR}/LICENSE")
message(STATUS
        "Using verified lzokay source at ${TSFILE_LZOKAY_COMMIT} from "
        "${_TSFILE_LZOKAY_ARCHIVE}")

unset(_TSFILE_LZOKAY_ACTUAL_SHA256)
unset(_TSFILE_LZOKAY_ARCHIVE)
unset(_TSFILE_LZOKAY_ARCHIVE_NAME)
unset(_TSFILE_LZOKAY_ARCHIVE_VALID)
unset(_TSFILE_LZOKAY_DOWNLOAD_MESSAGE)
unset(_TSFILE_LZOKAY_DOWNLOAD_RESULT)
unset(_TSFILE_LZOKAY_DOWNLOAD_STATUS)
unset(_TSFILE_LZOKAY_EXTRACT_ERROR)
unset(_TSFILE_LZOKAY_EXTRACT_RESULT)
unset(_TSFILE_LZOKAY_EXTRACT_ROOT)
unset(_TSFILE_LZOKAY_EXTRACTED)
unset(_TSFILE_LZOKAY_PARTIAL_ARCHIVE)
unset(_TSFILE_LZOKAY_SHA256)
unset(_TSFILE_LZOKAY_STAMP)
unset(_TSFILE_LZOKAY_STAMP_SHA256)
unset(_TSFILE_LZOKAY_URL)
