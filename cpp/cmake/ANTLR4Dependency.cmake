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

set(TSFILE_ANTLR4_MIN_VERSION "4.9.3")
set(TSFILE_ANTLR4_BUNDLED_VERSION "4.9.3")
# The generated parser in this repository follows the 4.9 runtime API. ANTLR
# 4.10 introduced breaking C++ runtime changes (and raised its language floor
# to C++17), so newer system runtimes must use the verified bundled fallback
# until the parser is regenerated and reviewed.
set(TSFILE_ANTLR4_NEXT_INCOMPATIBLE_VERSION "4.10.0")
set(TSFILE_ANTLR4_SYSTEM_INCLUDE_DIR "")
set(_TSFILE_SYSTEM_ANTLR4_FOUND FALSE)

if (NOT TSFILE_DEPENDENCY_SOURCE STREQUAL "BUNDLED")
    # ANTLR4 4.9.3 exports its header-only utf8cpp dependency as the bare
    # target name "utf8cpp". Load that package first so CMake resolves the
    # name as a target instead of translating it to the nonexistent
    # -lutf8cpp linker option.
    find_package(utf8cpp CONFIG QUIET)
    find_package(antlr4-runtime CONFIG QUIET)

    set(_TSFILE_SYSTEM_ANTLR4_VERSION "${antlr4-runtime_VERSION}")
    if ("${_TSFILE_SYSTEM_ANTLR4_VERSION}" STREQUAL "" AND
            DEFINED ANTLR_VERSION)
        set(_TSFILE_SYSTEM_ANTLR4_VERSION "${ANTLR_VERSION}")
    endif ()

    set(TSFILE_ANTLR4_SYSTEM_TARGET "")
    set(TSFILE_ANTLR4_SYSTEM_STATIC FALSE)
    foreach (_TSFILE_ANTLR4_TARGET
            antlr4_static
            antlr4-runtime::antlr4_static
            antlr4_shared
            antlr4-runtime::antlr4_shared)
        if (TARGET ${_TSFILE_ANTLR4_TARGET})
            set(TSFILE_ANTLR4_SYSTEM_TARGET ${_TSFILE_ANTLR4_TARGET})
            if (_TSFILE_ANTLR4_TARGET MATCHES "static$")
                set(TSFILE_ANTLR4_SYSTEM_STATIC TRUE)
            endif ()
            break()
        endif ()
    endforeach ()

    if (NOT "${TSFILE_ANTLR4_SYSTEM_TARGET}" STREQUAL "" AND
            NOT "${_TSFILE_SYSTEM_ANTLR4_VERSION}" STREQUAL "" AND
            NOT _TSFILE_SYSTEM_ANTLR4_VERSION VERSION_LESS
                    TSFILE_ANTLR4_MIN_VERSION AND
            _TSFILE_SYSTEM_ANTLR4_VERSION VERSION_LESS
                    TSFILE_ANTLR4_NEXT_INCOMPATIBLE_VERSION)
        set(_TSFILE_SYSTEM_ANTLR4_FOUND TRUE)
        if (IS_DIRECTORY "${ANTLR4_INCLUDE_DIR}")
            # ANTLR4 4.9.3's installed targets don't propagate their public
            # include directory. Keep SYSTEM mode compatible with that
            # upstream package while newer packages continue to use their
            # target metadata normally.
            set(TSFILE_ANTLR4_SYSTEM_INCLUDE_DIR
                    "${ANTLR4_INCLUDE_DIR}")
        endif ()
        message(STATUS
                "Found compatible system ANTLR4 "
                "${_TSFILE_SYSTEM_ANTLR4_VERSION} target "
                "${TSFILE_ANTLR4_SYSTEM_TARGET}")
    elseif (antlr4-runtime_FOUND)
        message(STATUS
                "Ignoring incompatible system ANTLR4 "
                "${_TSFILE_SYSTEM_ANTLR4_VERSION}; TsFile requires "
                ">=${TSFILE_ANTLR4_MIN_VERSION} and "
                "<${TSFILE_ANTLR4_NEXT_INCOMPATIBLE_VERSION}, with an "
                "antlr4_static or antlr4_shared target")
    endif ()
endif ()

tsfile_resolve_dependency_source(
        "ANTLR4" "${_TSFILE_SYSTEM_ANTLR4_FOUND}" TSFILE_ANTLR4_SOURCE)

unset(_TSFILE_ANTLR4_TARGET)
unset(_TSFILE_SYSTEM_ANTLR4_FOUND)
unset(_TSFILE_SYSTEM_ANTLR4_VERSION)
