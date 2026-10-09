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

set(_TSFILE_LIBLZMA_ARCHIVE_NAME
        "xz-${TSFILE_LIBLZMA_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_LIBLZMA_URL
        "https://github.com/tukaani-project/xz/releases/download/v${TSFILE_LIBLZMA_BUNDLED_VERSION}/${_TSFILE_LIBLZMA_ARCHIVE_NAME}")
set(_TSFILE_LIBLZMA_SHA256
        "3d3a1b973af218114f4f889bbaa2f4c037deaae0c8e815eec381c3d546b974a0")
set(TSFILE_LIBLZMA_ARCHIVE ""
        CACHE FILEPATH
        "Optional pre-downloaded XZ source archive for liblzma")

if (TSFILE_LIBLZMA_ARCHIVE)
    get_filename_component(_TSFILE_LIBLZMA_ARCHIVE
            "${TSFILE_LIBLZMA_ARCHIVE}" ABSOLUTE
            BASE_DIR "${CMAKE_BINARY_DIR}")
    if (NOT EXISTS "${_TSFILE_LIBLZMA_ARCHIVE}")
        message(FATAL_ERROR
                "TSFILE_LIBLZMA_ARCHIVE does not exist: "
                "${_TSFILE_LIBLZMA_ARCHIVE}")
    endif ()
else ()
    file(MAKE_DIRECTORY "${TSFILE_DEPENDENCY_CACHE}")
    set(_TSFILE_LIBLZMA_ARCHIVE
            "${TSFILE_DEPENDENCY_CACHE}/${_TSFILE_LIBLZMA_ARCHIVE_NAME}")
endif ()

set(_TSFILE_LIBLZMA_ARCHIVE_VALID FALSE)
if (EXISTS "${_TSFILE_LIBLZMA_ARCHIVE}")
    file(SHA256 "${_TSFILE_LIBLZMA_ARCHIVE}"
            _TSFILE_LIBLZMA_ACTUAL_SHA256)
    if (_TSFILE_LIBLZMA_ACTUAL_SHA256 STREQUAL _TSFILE_LIBLZMA_SHA256)
        set(_TSFILE_LIBLZMA_ARCHIVE_VALID TRUE)
    elseif (TSFILE_LIBLZMA_ARCHIVE OR TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "XZ source archive for liblzma has an unexpected SHA-256: "
                "${_TSFILE_LIBLZMA_ARCHIVE}\n"
                "Expected: ${_TSFILE_LIBLZMA_SHA256}\n"
                "Actual:   ${_TSFILE_LIBLZMA_ACTUAL_SHA256}")
    else ()
        message(STATUS
                "Removing invalid cached XZ archive: "
                "${_TSFILE_LIBLZMA_ARCHIVE}")
        file(REMOVE "${_TSFILE_LIBLZMA_ARCHIVE}")
    endif ()
endif ()

if (NOT _TSFILE_LIBLZMA_ARCHIVE_VALID)
    if (TSFILE_DEPENDENCY_OFFLINE)
        message(FATAL_ERROR
                "Offline dependency mode requires the verified XZ archive "
                "for liblzma at ${_TSFILE_LIBLZMA_ARCHIVE}. Pre-download "
                "${_TSFILE_LIBLZMA_URL} or set TSFILE_LIBLZMA_ARCHIVE to a "
                "verified local archive.")
    endif ()

    set(_TSFILE_LIBLZMA_PARTIAL_ARCHIVE
            "${_TSFILE_LIBLZMA_ARCHIVE}.part")
    file(REMOVE "${_TSFILE_LIBLZMA_PARTIAL_ARCHIVE}")
    message(STATUS
            "Downloading XZ Utils v${TSFILE_LIBLZMA_BUNDLED_VERSION} "
            "for liblzma")
    file(DOWNLOAD
            "${_TSFILE_LIBLZMA_URL}"
            "${_TSFILE_LIBLZMA_PARTIAL_ARCHIVE}"
            STATUS _TSFILE_LIBLZMA_DOWNLOAD_STATUS
            TLS_VERIFY ON
            TIMEOUT 120
            INACTIVITY_TIMEOUT 30)
    list(GET _TSFILE_LIBLZMA_DOWNLOAD_STATUS 0
            _TSFILE_LIBLZMA_DOWNLOAD_RESULT)
    list(GET _TSFILE_LIBLZMA_DOWNLOAD_STATUS 1
            _TSFILE_LIBLZMA_DOWNLOAD_MESSAGE)
    if (NOT _TSFILE_LIBLZMA_DOWNLOAD_RESULT EQUAL 0)
        file(REMOVE "${_TSFILE_LIBLZMA_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Failed to download XZ Utils from ${_TSFILE_LIBLZMA_URL}: "
                "${_TSFILE_LIBLZMA_DOWNLOAD_MESSAGE}. To build without "
                "network access, provide ${_TSFILE_LIBLZMA_ARCHIVE_NAME} "
                "in TSFILE_DEPENDENCY_CACHE or set TSFILE_LIBLZMA_ARCHIVE.")
    endif ()

    file(SHA256 "${_TSFILE_LIBLZMA_PARTIAL_ARCHIVE}"
            _TSFILE_LIBLZMA_ACTUAL_SHA256)
    if (NOT _TSFILE_LIBLZMA_ACTUAL_SHA256 STREQUAL
            _TSFILE_LIBLZMA_SHA256)
        file(REMOVE "${_TSFILE_LIBLZMA_PARTIAL_ARCHIVE}")
        message(FATAL_ERROR
                "Downloaded XZ archive has an unexpected SHA-256. Expected "
                "${_TSFILE_LIBLZMA_SHA256}, got "
                "${_TSFILE_LIBLZMA_ACTUAL_SHA256}.")
    endif ()
    file(RENAME "${_TSFILE_LIBLZMA_PARTIAL_ARCHIVE}"
            "${_TSFILE_LIBLZMA_ARCHIVE}")
endif ()

set(_TSFILE_LIBLZMA_EXTRACT_ROOT "${CMAKE_BINARY_DIR}/_deps")
set(TSFILE_LIBLZMA_SOURCE_DIR
        "${_TSFILE_LIBLZMA_EXTRACT_ROOT}/xz-${TSFILE_LIBLZMA_BUNDLED_VERSION}")
set(_TSFILE_LIBLZMA_STAMP
        "${TSFILE_LIBLZMA_SOURCE_DIR}/.tsfile-archive-sha256")
set(_TSFILE_LIBLZMA_EXTRACTED FALSE)
if (EXISTS "${_TSFILE_LIBLZMA_STAMP}" AND
        EXISTS "${TSFILE_LIBLZMA_SOURCE_DIR}/CMakeLists.txt" AND
        EXISTS "${TSFILE_LIBLZMA_SOURCE_DIR}/src/liblzma/api/lzma.h" AND
        EXISTS "${TSFILE_LIBLZMA_SOURCE_DIR}/src/liblzma/lzma/lzma2_encoder.c" AND
        EXISTS "${TSFILE_LIBLZMA_SOURCE_DIR}/COPYING.0BSD")
    file(READ "${_TSFILE_LIBLZMA_STAMP}" _TSFILE_LIBLZMA_STAMP_SHA256)
    string(STRIP "${_TSFILE_LIBLZMA_STAMP_SHA256}"
            _TSFILE_LIBLZMA_STAMP_SHA256)
    if (_TSFILE_LIBLZMA_STAMP_SHA256 STREQUAL _TSFILE_LIBLZMA_SHA256)
        set(_TSFILE_LIBLZMA_EXTRACTED TRUE)
    endif ()
endif ()

if (NOT _TSFILE_LIBLZMA_EXTRACTED)
    file(REMOVE_RECURSE "${TSFILE_LIBLZMA_SOURCE_DIR}")
    file(MAKE_DIRECTORY "${_TSFILE_LIBLZMA_EXTRACT_ROOT}")
    execute_process(
            COMMAND "${CMAKE_COMMAND}" -E tar xzf
                    "${_TSFILE_LIBLZMA_ARCHIVE}"
            WORKING_DIRECTORY "${_TSFILE_LIBLZMA_EXTRACT_ROOT}"
            RESULT_VARIABLE _TSFILE_LIBLZMA_EXTRACT_RESULT
            ERROR_VARIABLE _TSFILE_LIBLZMA_EXTRACT_ERROR)
    if (NOT _TSFILE_LIBLZMA_EXTRACT_RESULT EQUAL 0 OR
            NOT EXISTS "${TSFILE_LIBLZMA_SOURCE_DIR}/CMakeLists.txt" OR
            NOT EXISTS "${TSFILE_LIBLZMA_SOURCE_DIR}/src/liblzma/api/lzma.h" OR
            NOT EXISTS "${TSFILE_LIBLZMA_SOURCE_DIR}/src/liblzma/lzma/lzma2_encoder.c" OR
            NOT EXISTS "${TSFILE_LIBLZMA_SOURCE_DIR}/COPYING.0BSD")
        file(REMOVE_RECURSE "${TSFILE_LIBLZMA_SOURCE_DIR}")
        message(FATAL_ERROR
                "Failed to extract the XZ source archive for liblzma: "
                "${_TSFILE_LIBLZMA_EXTRACT_ERROR}")
    endif ()
    file(WRITE "${_TSFILE_LIBLZMA_STAMP}"
            "${_TSFILE_LIBLZMA_SHA256}\n")
endif ()

set(TSFILE_LIBLZMA_LICENSE_FILE
        "${TSFILE_LIBLZMA_SOURCE_DIR}/COPYING.0BSD")
message(STATUS
        "Using verified XZ Utils v${TSFILE_LIBLZMA_BUNDLED_VERSION} source "
        "for the 0BSD liblzma target from ${_TSFILE_LIBLZMA_ARCHIVE}")

unset(_TSFILE_LIBLZMA_ACTUAL_SHA256)
unset(_TSFILE_LIBLZMA_ARCHIVE)
unset(_TSFILE_LIBLZMA_ARCHIVE_NAME)
unset(_TSFILE_LIBLZMA_ARCHIVE_VALID)
unset(_TSFILE_LIBLZMA_DOWNLOAD_MESSAGE)
unset(_TSFILE_LIBLZMA_DOWNLOAD_RESULT)
unset(_TSFILE_LIBLZMA_DOWNLOAD_STATUS)
unset(_TSFILE_LIBLZMA_EXTRACT_ERROR)
unset(_TSFILE_LIBLZMA_EXTRACT_RESULT)
unset(_TSFILE_LIBLZMA_EXTRACT_ROOT)
unset(_TSFILE_LIBLZMA_EXTRACTED)
unset(_TSFILE_LIBLZMA_PARTIAL_ARCHIVE)
unset(_TSFILE_LIBLZMA_SHA256)
unset(_TSFILE_LIBLZMA_STAMP)
unset(_TSFILE_LIBLZMA_STAMP_SHA256)
unset(_TSFILE_LIBLZMA_URL)
