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

set(TSFILE_SNAPPY_MIN_VERSION "1.2.1")
set(TSFILE_SNAPPY_BUNDLED_VERSION "1.2.2")
set(TSFILE_SNAPPY_NEXT_INCOMPATIBLE_VERSION "2.0.0")
set(_TSFILE_SYSTEM_SNAPPY_FOUND FALSE)

if (NOT TSFILE_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
    find_package(Snappy ${TSFILE_SNAPPY_MIN_VERSION} CONFIG QUIET)
    if (Snappy_FOUND AND TARGET Snappy::snappy AND
            NOT "${Snappy_VERSION}" STREQUAL "" AND
            Snappy_VERSION VERSION_LESS
                    TSFILE_SNAPPY_NEXT_INCOMPATIBLE_VERSION)
        set(_TSFILE_SYSTEM_SNAPPY_FOUND TRUE)
        message(STATUS "Found compatible system Snappy ${Snappy_VERSION}")
    elseif (Snappy_FOUND)
        message(STATUS
                "Ignoring incompatible system Snappy ${Snappy_VERSION}; "
                "TsFile requires >=${TSFILE_SNAPPY_MIN_VERSION} and "
                "<${TSFILE_SNAPPY_NEXT_INCOMPATIBLE_VERSION}")
    endif ()
endif ()

tsfile_resolve_dependency_source(
        "Snappy" "${_TSFILE_SYSTEM_SNAPPY_FOUND}" TSFILE_SNAPPY_SOURCE)

unset(_TSFILE_SYSTEM_SNAPPY_FOUND)
