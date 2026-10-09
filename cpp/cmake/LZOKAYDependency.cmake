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

set(TSFILE_LZOKAY_MIN_VERSION "0.1")
set(TSFILE_LZOKAY_NEXT_INCOMPATIBLE_VERSION "1.0")
set(TSFILE_LZOKAY_COMMIT
        "5cb18da508cc4d3ec41bc04dccdeef9c5ffedfb2")
set(_TSFILE_SYSTEM_LZOKAY_FOUND FALSE)

if (NOT TSFILE_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
    find_package(lzokay ${TSFILE_LZOKAY_MIN_VERSION} CONFIG QUIET)
    if (lzokay_FOUND AND TARGET lzokay::lzokay AND
            NOT "${lzokay_VERSION}" STREQUAL "" AND
            lzokay_VERSION VERSION_LESS
                    TSFILE_LZOKAY_NEXT_INCOMPATIBLE_VERSION)
        set(_TSFILE_SYSTEM_LZOKAY_FOUND TRUE)
        message(STATUS "Found compatible system lzokay ${lzokay_VERSION}")
    elseif (lzokay_FOUND)
        message(STATUS
                "Ignoring incompatible system lzokay ${lzokay_VERSION}; "
                "TsFile requires >=${TSFILE_LZOKAY_MIN_VERSION} and "
                "<${TSFILE_LZOKAY_NEXT_INCOMPATIBLE_VERSION}")
    endif ()
endif ()

tsfile_resolve_dependency_source(
        "lzokay" "${_TSFILE_SYSTEM_LZOKAY_FOUND}" TSFILE_LZOKAY_SOURCE)

unset(_TSFILE_SYSTEM_LZOKAY_FOUND)
