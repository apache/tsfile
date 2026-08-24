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

set(TSFILE_ZSTD_MIN_VERSION "1.5.7")
set(TSFILE_ZSTD_BUNDLED_VERSION "1.5.7")
set(TSFILE_ZSTD_NEXT_INCOMPATIBLE_VERSION "2.0.0")
set(TSFILE_ZSTD_SYSTEM_TARGET "")
set(_TSFILE_SYSTEM_ZSTD_FOUND FALSE)

if (NOT TSFILE_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
    find_package(zstd ${TSFILE_ZSTD_MIN_VERSION} CONFIG QUIET)
    if (zstd_FOUND AND
            NOT "${zstd_VERSION}" STREQUAL "" AND
            zstd_VERSION VERSION_LESS
                    TSFILE_ZSTD_NEXT_INCOMPATIBLE_VERSION)
        foreach (_TSFILE_ZSTD_TARGET
                zstd::libzstd_static
                zstd::libzstd_shared
                zstd::libzstd)
            if (TARGET ${_TSFILE_ZSTD_TARGET})
                set(TSFILE_ZSTD_SYSTEM_TARGET ${_TSFILE_ZSTD_TARGET})
                break()
            endif ()
        endforeach ()
        if (NOT "${TSFILE_ZSTD_SYSTEM_TARGET}" STREQUAL "")
            set(_TSFILE_SYSTEM_ZSTD_FOUND TRUE)
            message(STATUS "Found compatible system Zstandard ${zstd_VERSION}")
        endif ()
    elseif (zstd_FOUND)
        message(STATUS
                "Ignoring incompatible system Zstandard ${zstd_VERSION}; "
                "TsFile requires >=${TSFILE_ZSTD_MIN_VERSION} and "
                "<${TSFILE_ZSTD_NEXT_INCOMPATIBLE_VERSION}")
    endif ()
endif ()

tsfile_resolve_dependency_source(
        "Zstandard" "${_TSFILE_SYSTEM_ZSTD_FOUND}" TSFILE_ZSTD_SOURCE)

unset(_TSFILE_SYSTEM_ZSTD_FOUND)
unset(_TSFILE_ZSTD_TARGET)
