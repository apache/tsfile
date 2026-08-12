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

include_guard(GLOBAL)

set(TSFILE_DEPENDENCY_SOURCE "AUTO"
        CACHE STRING
        "Dependency source policy: AUTO, SYSTEM, or BUNDLED")
set_property(CACHE TSFILE_DEPENDENCY_SOURCE
        PROPERTY STRINGS AUTO SYSTEM BUNDLED)

string(TOUPPER "${TSFILE_DEPENDENCY_SOURCE}"
        _TSFILE_NORMALIZED_DEPENDENCY_SOURCE)
set(TSFILE_DEPENDENCY_SOURCE
        "${_TSFILE_NORMALIZED_DEPENDENCY_SOURCE}"
        CACHE STRING
        "Dependency source policy: AUTO, SYSTEM, or BUNDLED"
        FORCE)
unset(_TSFILE_NORMALIZED_DEPENDENCY_SOURCE)

set(_TSFILE_SUPPORTED_DEPENDENCY_SOURCES AUTO SYSTEM BUNDLED)
list(FIND _TSFILE_SUPPORTED_DEPENDENCY_SOURCES
        "${TSFILE_DEPENDENCY_SOURCE}"
        _TSFILE_DEPENDENCY_SOURCE_INDEX)
if (_TSFILE_DEPENDENCY_SOURCE_INDEX EQUAL -1)
    message(FATAL_ERROR
            "Invalid TSFILE_DEPENDENCY_SOURCE='${TSFILE_DEPENDENCY_SOURCE}'. "
            "Expected one of: AUTO, SYSTEM, BUNDLED.")
endif ()
unset(_TSFILE_DEPENDENCY_SOURCE_INDEX)
unset(_TSFILE_SUPPORTED_DEPENDENCY_SOURCES)

message(STATUS
        "TsFile dependency source policy: ${TSFILE_DEPENDENCY_SOURCE}")

# Resolve the source for one dependency after its migration has probed for a
# compatible system package. SYSTEM_FOUND must be a boolean value, not the name
# of a CMake variable. The selected source is returned as SYSTEM or BUNDLED.
function(tsfile_resolve_dependency_source DEPENDENCY SYSTEM_FOUND OUTPUT_VARIABLE)
    if (NOT ARGC EQUAL 3)
        message(FATAL_ERROR
                "tsfile_resolve_dependency_source expects: "
                "DEPENDENCY SYSTEM_FOUND OUTPUT_VARIABLE")
    endif ()
    if ("${DEPENDENCY}" STREQUAL "")
        message(FATAL_ERROR "Dependency name must not be empty.")
    endif ()
    if ("${OUTPUT_VARIABLE}" STREQUAL "")
        message(FATAL_ERROR
                "Output variable must not be empty for dependency ${DEPENDENCY}.")
    endif ()

    if (TSFILE_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
        set(_TSFILE_SELECTED_SOURCE BUNDLED)
    elseif (SYSTEM_FOUND)
        set(_TSFILE_SELECTED_SOURCE SYSTEM)
    elseif (TSFILE_DEPENDENCY_SOURCE STREQUAL "SYSTEM")
        message(FATAL_ERROR
                "TSFILE_DEPENDENCY_SOURCE=SYSTEM requires a compatible "
                "system ${DEPENDENCY} package, but none was found.")
    else ()
        set(_TSFILE_SELECTED_SOURCE BUNDLED)
    endif ()

    message(STATUS
            "TsFile dependency ${DEPENDENCY}: ${_TSFILE_SELECTED_SOURCE}")
    set(${OUTPUT_VARIABLE} "${_TSFILE_SELECTED_SOURCE}" PARENT_SCOPE)
endfunction()
