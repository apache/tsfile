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

set(_TSFILE_ANTLR4_ARCHIVE_NAME
        "antlr4-${TSFILE_ANTLR4_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_ANTLR4_URL
        "https://github.com/antlr/antlr4/archive/refs/tags/${TSFILE_ANTLR4_BUNDLED_VERSION}.tar.gz")
set(_TSFILE_ANTLR4_SHA256
        "efe4057d75ab48145d4683100fec7f77d7f87fa258707330cadd1f8e6f7eecae")
set(_TSFILE_UTF8CPP_VERSION "3.1.1")
set(_TSFILE_UTF8CPP_ARCHIVE_NAME
        "utfcpp-v${_TSFILE_UTF8CPP_VERSION}.tar.gz")
set(_TSFILE_UTF8CPP_URL
        "https://github.com/nemtrif/utfcpp/archive/refs/tags/v${_TSFILE_UTF8CPP_VERSION}.tar.gz")
set(_TSFILE_UTF8CPP_SHA256
        "33496a4c3cc2de80e9809c4997052331af5fb32079f43ab4d667cd48c3a36e88")

set(TSFILE_ANTLR4_ARCHIVE ""
        CACHE FILEPATH
        "Optional pre-downloaded ANTLR4 source archive")
set(TSFILE_UTF8CPP_ARCHIVE ""
        CACHE FILEPATH
        "Optional pre-downloaded utf8cpp source archive for ANTLR4")

function(_tsfile_prepare_antlr4_archive
        DEPENDENCY ARCHIVE_VARIABLE ARCHIVE_NAME URL SHA256 SOURCE_DIR
        REQUIRED_FILES STAMP_VALUE PATCH_SCRIPT)
    set(_TSFILE_ARCHIVE "${${ARCHIVE_VARIABLE}}")
    if (_TSFILE_ARCHIVE)
        get_filename_component(_TSFILE_ARCHIVE
                "${_TSFILE_ARCHIVE}" ABSOLUTE BASE_DIR "${CMAKE_BINARY_DIR}")
        if (NOT EXISTS "${_TSFILE_ARCHIVE}")
            message(FATAL_ERROR
                    "${ARCHIVE_VARIABLE} does not exist: "
                    "${_TSFILE_ARCHIVE}")
        endif ()
        set(_TSFILE_EXPLICIT_ARCHIVE TRUE)
    else ()
        file(MAKE_DIRECTORY "${TSFILE_DEPENDENCY_CACHE}")
        set(_TSFILE_ARCHIVE
                "${TSFILE_DEPENDENCY_CACHE}/${ARCHIVE_NAME}")
        set(_TSFILE_EXPLICIT_ARCHIVE FALSE)
    endif ()

    set(_TSFILE_ARCHIVE_VALID FALSE)
    if (EXISTS "${_TSFILE_ARCHIVE}")
        file(SHA256 "${_TSFILE_ARCHIVE}" _TSFILE_ACTUAL_SHA256)
        if (_TSFILE_ACTUAL_SHA256 STREQUAL SHA256)
            set(_TSFILE_ARCHIVE_VALID TRUE)
        elseif (_TSFILE_EXPLICIT_ARCHIVE OR TSFILE_DEPENDENCY_OFFLINE)
            message(FATAL_ERROR
                    "${DEPENDENCY} source archive has an unexpected SHA-256: "
                    "${_TSFILE_ARCHIVE}\n"
                    "Expected: ${SHA256}\n"
                    "Actual:   ${_TSFILE_ACTUAL_SHA256}")
        else ()
            message(STATUS
                    "Removing invalid cached ${DEPENDENCY} archive: "
                    "${_TSFILE_ARCHIVE}")
            file(REMOVE "${_TSFILE_ARCHIVE}")
        endif ()
    endif ()

    if (NOT _TSFILE_ARCHIVE_VALID)
        if (TSFILE_DEPENDENCY_OFFLINE)
            message(FATAL_ERROR
                    "Offline dependency mode requires the verified "
                    "${DEPENDENCY} archive at ${_TSFILE_ARCHIVE}. "
                    "Pre-download ${URL} or set ${ARCHIVE_VARIABLE} to a "
                    "verified local archive.")
        endif ()

        set(_TSFILE_PARTIAL_ARCHIVE "${_TSFILE_ARCHIVE}.part")
        file(REMOVE "${_TSFILE_PARTIAL_ARCHIVE}")
        message(STATUS "Downloading ${DEPENDENCY}")
        file(DOWNLOAD
                "${URL}"
                "${_TSFILE_PARTIAL_ARCHIVE}"
                STATUS _TSFILE_DOWNLOAD_STATUS
                TLS_VERIFY ON
                TIMEOUT 120
                INACTIVITY_TIMEOUT 30)
        list(GET _TSFILE_DOWNLOAD_STATUS 0 _TSFILE_DOWNLOAD_RESULT)
        list(GET _TSFILE_DOWNLOAD_STATUS 1 _TSFILE_DOWNLOAD_MESSAGE)
        if (NOT _TSFILE_DOWNLOAD_RESULT EQUAL 0)
            file(REMOVE "${_TSFILE_PARTIAL_ARCHIVE}")
            message(FATAL_ERROR
                    "Failed to download ${DEPENDENCY} from ${URL}: "
                    "${_TSFILE_DOWNLOAD_MESSAGE}. To build without network "
                    "access, provide ${ARCHIVE_NAME} in "
                    "TSFILE_DEPENDENCY_CACHE or set ${ARCHIVE_VARIABLE}.")
        endif ()

        file(SHA256 "${_TSFILE_PARTIAL_ARCHIVE}" _TSFILE_ACTUAL_SHA256)
        if (NOT _TSFILE_ACTUAL_SHA256 STREQUAL SHA256)
            file(REMOVE "${_TSFILE_PARTIAL_ARCHIVE}")
            message(FATAL_ERROR
                    "Downloaded ${DEPENDENCY} archive has an unexpected "
                    "SHA-256. Expected ${SHA256}, got "
                    "${_TSFILE_ACTUAL_SHA256}.")
        endif ()
        file(RENAME "${_TSFILE_PARTIAL_ARCHIVE}" "${_TSFILE_ARCHIVE}")
    endif ()

    set(_TSFILE_STAMP "${SOURCE_DIR}/.tsfile-source-stamp")
    set(_TSFILE_EXTRACTED FALSE)
    set(_TSFILE_REQUIRED_FILES_PRESENT TRUE)
    foreach (_TSFILE_REQUIRED_FILE ${REQUIRED_FILES})
        if (NOT EXISTS "${SOURCE_DIR}/${_TSFILE_REQUIRED_FILE}")
            set(_TSFILE_REQUIRED_FILES_PRESENT FALSE)
        endif ()
    endforeach ()
    if (EXISTS "${_TSFILE_STAMP}" AND _TSFILE_REQUIRED_FILES_PRESENT)
        file(READ "${_TSFILE_STAMP}" _TSFILE_EXISTING_STAMP)
        string(STRIP "${_TSFILE_EXISTING_STAMP}" _TSFILE_EXISTING_STAMP)
        if (_TSFILE_EXISTING_STAMP STREQUAL STAMP_VALUE)
            set(_TSFILE_EXTRACTED TRUE)
        endif ()
    endif ()

    if (NOT _TSFILE_EXTRACTED)
        file(REMOVE_RECURSE "${SOURCE_DIR}")
        file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/_deps")
        execute_process(
                COMMAND "${CMAKE_COMMAND}" -E tar xzf "${_TSFILE_ARCHIVE}"
                WORKING_DIRECTORY "${CMAKE_BINARY_DIR}/_deps"
                RESULT_VARIABLE _TSFILE_EXTRACT_RESULT
                ERROR_VARIABLE _TSFILE_EXTRACT_ERROR)
        set(_TSFILE_REQUIRED_FILES_PRESENT TRUE)
        foreach (_TSFILE_REQUIRED_FILE ${REQUIRED_FILES})
            if (NOT EXISTS "${SOURCE_DIR}/${_TSFILE_REQUIRED_FILE}")
                set(_TSFILE_REQUIRED_FILES_PRESENT FALSE)
            endif ()
        endforeach ()
        if (NOT _TSFILE_EXTRACT_RESULT EQUAL 0 OR
                NOT _TSFILE_REQUIRED_FILES_PRESENT)
            file(REMOVE_RECURSE "${SOURCE_DIR}")
            message(FATAL_ERROR
                    "Failed to extract the ${DEPENDENCY} source archive: "
                    "${_TSFILE_EXTRACT_ERROR}")
        endif ()
        if (PATCH_SCRIPT)
            set(TSFILE_ANTLR4_RUNTIME_DIR "${SOURCE_DIR}/runtime/Cpp/runtime")
            include("${PATCH_SCRIPT}")
            unset(TSFILE_ANTLR4_RUNTIME_DIR)
        endif ()
        file(WRITE "${_TSFILE_STAMP}" "${STAMP_VALUE}\n")
    endif ()

    set(${ARCHIVE_VARIABLE}_RESOLVED "${_TSFILE_ARCHIVE}" PARENT_SCOPE)
endfunction()

set(TSFILE_ANTLR4_SOURCE_DIR
        "${CMAKE_BINARY_DIR}/_deps/antlr4-${TSFILE_ANTLR4_BUNDLED_VERSION}")
set(TSFILE_UTF8CPP_SOURCE_DIR
        "${CMAKE_BINARY_DIR}/_deps/utfcpp-${_TSFILE_UTF8CPP_VERSION}")

_tsfile_prepare_antlr4_archive(
        "ANTLR4 ${TSFILE_ANTLR4_BUNDLED_VERSION}"
        TSFILE_ANTLR4_ARCHIVE
        "${_TSFILE_ANTLR4_ARCHIVE_NAME}"
        "${_TSFILE_ANTLR4_URL}"
        "${_TSFILE_ANTLR4_SHA256}"
        "${TSFILE_ANTLR4_SOURCE_DIR}"
        "LICENSE.txt;runtime/Cpp/runtime/src/antlr4-runtime.h;runtime/Cpp/runtime/src/support/StringUtils.cpp"
        "${_TSFILE_ANTLR4_SHA256}:tsfile-compat-1"
        "${CMAKE_CURRENT_LIST_DIR}/ANTLR4Patch.cmake")

_tsfile_prepare_antlr4_archive(
        "utf8cpp v${_TSFILE_UTF8CPP_VERSION}"
        TSFILE_UTF8CPP_ARCHIVE
        "${_TSFILE_UTF8CPP_ARCHIVE_NAME}"
        "${_TSFILE_UTF8CPP_URL}"
        "${_TSFILE_UTF8CPP_SHA256}"
        "${TSFILE_UTF8CPP_SOURCE_DIR}"
        "LICENSE;source/utf8.h;source/utf8/core.h"
        "${_TSFILE_UTF8CPP_SHA256}"
        "")

set(TSFILE_ANTLR4_LICENSE_FILE "${TSFILE_ANTLR4_SOURCE_DIR}/LICENSE.txt")
set(TSFILE_UTF8CPP_LICENSE_FILE "${TSFILE_UTF8CPP_SOURCE_DIR}/LICENSE")
message(STATUS
        "Using verified ANTLR4 ${TSFILE_ANTLR4_BUNDLED_VERSION} source from "
        "${TSFILE_ANTLR4_ARCHIVE_RESOLVED}")
message(STATUS
        "Using verified utf8cpp v${_TSFILE_UTF8CPP_VERSION} source from "
        "${TSFILE_UTF8CPP_ARCHIVE_RESOLVED}")

unset(_TSFILE_ANTLR4_ARCHIVE_NAME)
unset(_TSFILE_ANTLR4_SHA256)
unset(_TSFILE_ANTLR4_URL)
unset(_TSFILE_UTF8CPP_ARCHIVE_NAME)
unset(_TSFILE_UTF8CPP_SHA256)
unset(_TSFILE_UTF8CPP_URL)
unset(_TSFILE_UTF8CPP_VERSION)
