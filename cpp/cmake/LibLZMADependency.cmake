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

set(TSFILE_LIBLZMA_MIN_VERSION "5.8.3")
set(TSFILE_LIBLZMA_BUNDLED_VERSION "5.8.3")
set(TSFILE_LIBLZMA_NEXT_INCOMPATIBLE_VERSION "6.0.0")
set(TSFILE_LIBLZMA_SYSTEM_TARGET "")
set(_TSFILE_SYSTEM_LIBLZMA_FOUND FALSE)
set(_TSFILE_SYSTEM_LIBLZMA_VERSION "")

if (NOT TSFILE_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
    # Prefer the package configuration installed by current XZ releases.
    find_package(liblzma ${TSFILE_LIBLZMA_MIN_VERSION} CONFIG QUIET)
    if (liblzma_FOUND)
        set(_TSFILE_SYSTEM_LIBLZMA_VERSION "${liblzma_VERSION}")
        if (TARGET LibLZMA::LibLZMA)
            set(TSFILE_LIBLZMA_SYSTEM_TARGET LibLZMA::LibLZMA)
        elseif (TARGET liblzma::liblzma)
            set(TSFILE_LIBLZMA_SYSTEM_TARGET liblzma::liblzma)
        endif ()
    endif ()

    # Fall back to CMake's FindLibLZMA module for traditional installations.
    if ("${TSFILE_LIBLZMA_SYSTEM_TARGET}" STREQUAL "")
        find_package(LibLZMA ${TSFILE_LIBLZMA_MIN_VERSION} MODULE QUIET)
        if (LibLZMA_FOUND)
            if (DEFINED LibLZMA_VERSION)
                set(_TSFILE_SYSTEM_LIBLZMA_VERSION "${LibLZMA_VERSION}")
            elseif (DEFINED LIBLZMA_VERSION)
                set(_TSFILE_SYSTEM_LIBLZMA_VERSION "${LIBLZMA_VERSION}")
            else ()
                set(_TSFILE_SYSTEM_LIBLZMA_VERSION
                        "${LIBLZMA_VERSION_STRING}")
            endif ()

            if (TARGET LibLZMA::LibLZMA)
                set(TSFILE_LIBLZMA_SYSTEM_TARGET LibLZMA::LibLZMA)
            else ()
                # FindLibLZMA only gained its imported target in CMake 3.14.
                # Keep SYSTEM mode usable with TsFile's CMake 3.11 baseline.
                add_library(tsfile_system_liblzma INTERFACE)
                target_include_directories(tsfile_system_liblzma INTERFACE
                        ${LIBLZMA_INCLUDE_DIRS})
                target_link_libraries(tsfile_system_liblzma INTERFACE
                        ${LIBLZMA_LIBRARIES})
                set(TSFILE_LIBLZMA_SYSTEM_TARGET tsfile_system_liblzma)
            endif ()
        endif ()
    endif ()

    if (NOT "${TSFILE_LIBLZMA_SYSTEM_TARGET}" STREQUAL "" AND
            NOT "${_TSFILE_SYSTEM_LIBLZMA_VERSION}" STREQUAL "" AND
            _TSFILE_SYSTEM_LIBLZMA_VERSION VERSION_LESS
                    TSFILE_LIBLZMA_NEXT_INCOMPATIBLE_VERSION)
        set(_TSFILE_SYSTEM_LIBLZMA_FOUND TRUE)
        message(STATUS
                "Found compatible system liblzma "
                "${_TSFILE_SYSTEM_LIBLZMA_VERSION}")
    elseif (NOT "${TSFILE_LIBLZMA_SYSTEM_TARGET}" STREQUAL "")
        message(STATUS
                "Ignoring incompatible system liblzma "
                "${_TSFILE_SYSTEM_LIBLZMA_VERSION}; TsFile requires "
                ">=${TSFILE_LIBLZMA_MIN_VERSION} and "
                "<${TSFILE_LIBLZMA_NEXT_INCOMPATIBLE_VERSION}")
        set(TSFILE_LIBLZMA_SYSTEM_TARGET "")
    endif ()
endif ()

tsfile_resolve_dependency_source(
        "liblzma" "${_TSFILE_SYSTEM_LIBLZMA_FOUND}" TSFILE_LIBLZMA_SOURCE)

unset(_TSFILE_SYSTEM_LIBLZMA_FOUND)
unset(_TSFILE_SYSTEM_LIBLZMA_VERSION)
