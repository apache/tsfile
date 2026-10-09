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

find_path(LZ4_INCLUDE_DIR
        NAMES lz4.h
        HINTS "${LZ4_ROOT}" ENV LZ4_ROOT
        PATH_SUFFIXES include)

find_library(LZ4_LIBRARY
        NAMES lz4 liblz4
        HINTS "${LZ4_ROOT}" ENV LZ4_ROOT
        PATH_SUFFIXES lib lib64)

if (LZ4_INCLUDE_DIR AND EXISTS "${LZ4_INCLUDE_DIR}/lz4.h")
    file(READ "${LZ4_INCLUDE_DIR}/lz4.h" _LZ4_HEADER_CONTENTS)
    foreach (_LZ4_VERSION_COMPONENT MAJOR MINOR RELEASE)
        string(REGEX MATCH
                "#[ \t]*define[ \t]+LZ4_VERSION_${_LZ4_VERSION_COMPONENT}[ \t\\\\\r\n]+([0-9]+)"
                _LZ4_VERSION_MATCH
                "${_LZ4_HEADER_CONTENTS}")
        set(_LZ4_VERSION_${_LZ4_VERSION_COMPONENT} "${CMAKE_MATCH_1}")
    endforeach ()

    if (_LZ4_VERSION_MAJOR AND
            NOT "${_LZ4_VERSION_MINOR}" STREQUAL "" AND
            NOT "${_LZ4_VERSION_RELEASE}" STREQUAL "")
        set(LZ4_VERSION
                "${_LZ4_VERSION_MAJOR}.${_LZ4_VERSION_MINOR}.${_LZ4_VERSION_RELEASE}")
    endif ()
endif ()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LZ4
        REQUIRED_VARS LZ4_LIBRARY LZ4_INCLUDE_DIR LZ4_VERSION
        VERSION_VAR LZ4_VERSION)

if (LZ4_FOUND)
    set(LZ4_INCLUDE_DIRS "${LZ4_INCLUDE_DIR}")
    set(LZ4_LIBRARIES "${LZ4_LIBRARY}")

    if (NOT TARGET LZ4::LZ4)
        add_library(LZ4::LZ4 UNKNOWN IMPORTED)
        set_target_properties(LZ4::LZ4 PROPERTIES
                IMPORTED_LOCATION "${LZ4_LIBRARY}"
                INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}")
    endif ()
endif ()

mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARY)

unset(_LZ4_HEADER_CONTENTS)
unset(_LZ4_VERSION_MAJOR)
unset(_LZ4_VERSION_MINOR)
unset(_LZ4_VERSION_RELEASE)
unset(_LZ4_VERSION_COMPONENT)
unset(_LZ4_VERSION_MATCH)
