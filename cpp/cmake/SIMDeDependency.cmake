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

set(TSFILE_SIMDE_MIN_VERSION "0.8.4")
set(TSFILE_SIMDE_BUNDLED_VERSION "0.8.4-rc3")
set(TSFILE_SIMDE_NEXT_INCOMPATIBLE_VERSION "1.0.0")
set(_TSFILE_SYSTEM_SIMDE_FOUND FALSE)

function(_tsfile_read_simde_version INCLUDE_DIR OUT_VERSION)
    set(_TSFILE_SIMDE_VERSION_HEADER
            "${INCLUDE_DIR}/simde/simde-common.h")
    if (NOT EXISTS "${_TSFILE_SIMDE_VERSION_HEADER}")
        set(${OUT_VERSION} "" PARENT_SCOPE)
        return()
    endif ()

    file(STRINGS "${_TSFILE_SIMDE_VERSION_HEADER}"
            _TSFILE_SIMDE_VERSION_LINES
            REGEX "^#define SIMDE_VERSION_(MAJOR|MINOR|MICRO) [0-9]+$")
    foreach (_TSFILE_SIMDE_COMPONENT MAJOR MINOR MICRO)
        string(REGEX MATCH
                "SIMDE_VERSION_${_TSFILE_SIMDE_COMPONENT} ([0-9]+)"
                _TSFILE_SIMDE_MATCH "${_TSFILE_SIMDE_VERSION_LINES}")
        set(_TSFILE_SIMDE_${_TSFILE_SIMDE_COMPONENT}
                "${CMAKE_MATCH_1}")
    endforeach ()

    if (NOT "${_TSFILE_SIMDE_MAJOR}" STREQUAL "" AND
            NOT "${_TSFILE_SIMDE_MINOR}" STREQUAL "" AND
            NOT "${_TSFILE_SIMDE_MICRO}" STREQUAL "")
        set(${OUT_VERSION}
                "${_TSFILE_SIMDE_MAJOR}.${_TSFILE_SIMDE_MINOR}.${_TSFILE_SIMDE_MICRO}"
                PARENT_SCOPE)
    else ()
        set(${OUT_VERSION} "" PARENT_SCOPE)
    endif ()
endfunction()

if (NOT TSFILE_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
    cmake_policy(PUSH)
    if (POLICY CMP0144)
        cmake_policy(SET CMP0144 NEW)
    endif ()
    find_package(simde CONFIG QUIET)
    cmake_policy(POP)
    if (simde_FOUND AND TARGET simde::simde AND
            NOT "${simde_VERSION}" STREQUAL "")
        set(_TSFILE_SYSTEM_SIMDE_VERSION "${simde_VERSION}")
    endif ()

    if (NOT TARGET simde::simde)
        find_path(TSFILE_SYSTEM_SIMDE_INCLUDE_DIR
                NAMES simde/x86/ssse3.h
                HINTS "${SIMDE_ROOT}" "$ENV{SIMDE_ROOT}"
                PATH_SUFFIXES include)
        if (TSFILE_SYSTEM_SIMDE_INCLUDE_DIR)
            _tsfile_read_simde_version(
                    "${TSFILE_SYSTEM_SIMDE_INCLUDE_DIR}"
                    _TSFILE_SYSTEM_SIMDE_VERSION)
        endif ()
    elseif ("${_TSFILE_SYSTEM_SIMDE_VERSION}" STREQUAL "")
        get_target_property(_TSFILE_SIMDE_INCLUDE_DIRS simde::simde
                INTERFACE_INCLUDE_DIRECTORIES)
        foreach (_TSFILE_SIMDE_INCLUDE_DIR ${_TSFILE_SIMDE_INCLUDE_DIRS})
            if (NOT _TSFILE_SIMDE_INCLUDE_DIR MATCHES "^\\$<")
                _tsfile_read_simde_version("${_TSFILE_SIMDE_INCLUDE_DIR}"
                        _TSFILE_SYSTEM_SIMDE_VERSION)
                if (NOT "${_TSFILE_SYSTEM_SIMDE_VERSION}" STREQUAL "")
                    break()
                endif ()
            endif ()
        endforeach ()
    endif ()

    if (NOT "${_TSFILE_SYSTEM_SIMDE_VERSION}" STREQUAL "" AND
            NOT _TSFILE_SYSTEM_SIMDE_VERSION VERSION_LESS
                    TSFILE_SIMDE_MIN_VERSION AND
            _TSFILE_SYSTEM_SIMDE_VERSION VERSION_LESS
                    TSFILE_SIMDE_NEXT_INCOMPATIBLE_VERSION)
        if (NOT TARGET simde::simde)
            add_library(tsfile_system_simde INTERFACE)
            target_include_directories(tsfile_system_simde INTERFACE
                    "${TSFILE_SYSTEM_SIMDE_INCLUDE_DIR}")
            add_library(simde::simde ALIAS tsfile_system_simde)
        endif ()
        set(_TSFILE_SYSTEM_SIMDE_FOUND TRUE)
        message(STATUS
                "Found compatible system SIMDe ${_TSFILE_SYSTEM_SIMDE_VERSION}")
    elseif (TARGET simde::simde OR TSFILE_SYSTEM_SIMDE_INCLUDE_DIR)
        message(STATUS
                "Ignoring incompatible system SIMDe "
                "${_TSFILE_SYSTEM_SIMDE_VERSION}; TsFile requires "
                ">=${TSFILE_SIMDE_MIN_VERSION} and "
                "<${TSFILE_SIMDE_NEXT_INCOMPATIBLE_VERSION}")
    endif ()
endif ()

tsfile_resolve_dependency_source(
        "SIMDe" "${_TSFILE_SYSTEM_SIMDE_FOUND}" TSFILE_SIMDE_SOURCE)

unset(_TSFILE_SIMDE_INCLUDE_DIR)
unset(_TSFILE_SIMDE_INCLUDE_DIRS)
unset(_TSFILE_SYSTEM_SIMDE_FOUND)
unset(_TSFILE_SYSTEM_SIMDE_VERSION)
