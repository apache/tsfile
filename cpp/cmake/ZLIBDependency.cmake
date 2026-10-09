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

set(TSFILE_ZLIB_MIN_VERSION "1.3.1")
set(TSFILE_ZLIB_BUNDLED_VERSION "1.3.1")
set(TSFILE_ZLIB_NEXT_INCOMPATIBLE_VERSION "2.0.0")
set(_TSFILE_SYSTEM_ZLIB_FOUND FALSE)

if (NOT TSFILE_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
    find_package(ZLIB QUIET)
    if (ZLIB_VERSION)
        set(_TSFILE_SYSTEM_ZLIB_VERSION "${ZLIB_VERSION}")
    else ()
        set(_TSFILE_SYSTEM_ZLIB_VERSION "${ZLIB_VERSION_STRING}")
    endif ()

    if (ZLIB_FOUND AND TARGET ZLIB::ZLIB AND
            NOT "${_TSFILE_SYSTEM_ZLIB_VERSION}" STREQUAL "" AND
            NOT _TSFILE_SYSTEM_ZLIB_VERSION VERSION_LESS
                    TSFILE_ZLIB_MIN_VERSION AND
            _TSFILE_SYSTEM_ZLIB_VERSION VERSION_LESS
                    TSFILE_ZLIB_NEXT_INCOMPATIBLE_VERSION)
        set(_TSFILE_SYSTEM_ZLIB_FOUND TRUE)
        message(STATUS
                "Found compatible system zlib ${_TSFILE_SYSTEM_ZLIB_VERSION}")
    elseif (ZLIB_FOUND)
        message(STATUS
                "Ignoring incompatible system zlib "
                "${_TSFILE_SYSTEM_ZLIB_VERSION}; TsFile requires "
                ">=${TSFILE_ZLIB_MIN_VERSION} and "
                "<${TSFILE_ZLIB_NEXT_INCOMPATIBLE_VERSION}")
    endif ()
endif ()

tsfile_resolve_dependency_source(
        "zlib" "${_TSFILE_SYSTEM_ZLIB_FOUND}" TSFILE_ZLIB_SOURCE)

unset(_TSFILE_SYSTEM_ZLIB_FOUND)
unset(_TSFILE_SYSTEM_ZLIB_VERSION)
