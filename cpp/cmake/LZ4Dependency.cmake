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

set(TSFILE_LZ4_MIN_VERSION "1.9.4")
set(TSFILE_LZ4_NEXT_INCOMPATIBLE_VERSION "2.0.0")
set(_TSFILE_SYSTEM_LZ4_FOUND FALSE)

if (NOT TSFILE_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
    find_package(LZ4 ${TSFILE_LZ4_MIN_VERSION} QUIET)
    if (LZ4_FOUND AND
            LZ4_VERSION VERSION_LESS TSFILE_LZ4_NEXT_INCOMPATIBLE_VERSION)
        set(_TSFILE_SYSTEM_LZ4_FOUND TRUE)
        message(STATUS "Found compatible system LZ4 ${LZ4_VERSION}")
    elseif (LZ4_FOUND)
        message(STATUS
                "Ignoring incompatible system LZ4 ${LZ4_VERSION}; "
                "TsFile requires >=${TSFILE_LZ4_MIN_VERSION} and "
                "<${TSFILE_LZ4_NEXT_INCOMPATIBLE_VERSION}")
    endif ()
endif ()

tsfile_resolve_dependency_source(
        "LZ4" "${_TSFILE_SYSTEM_LZ4_FOUND}" TSFILE_LZ4_SOURCE)

unset(_TSFILE_SYSTEM_LZ4_FOUND)
