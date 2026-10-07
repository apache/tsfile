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

# A configure-time contract test also runnable on non-MSVC hosts. Runtime
# properties initialize on every platform; Windows CI checks actual PE imports.
function(tsfile_check_static_runtime DIRECTORY)
    get_property(_TARGETS DIRECTORY "${DIRECTORY}" PROPERTY BUILDSYSTEM_TARGETS)
    foreach (_TARGET IN LISTS _TARGETS)
        get_target_property(_TYPE ${_TARGET} TYPE)
        if (_TYPE MATCHES "^(STATIC_LIBRARY|SHARED_LIBRARY|OBJECT_LIBRARY|EXECUTABLE)$")
            get_target_property(_RUNTIME ${_TARGET} MSVC_RUNTIME_LIBRARY)
            if (NOT _RUNTIME STREQUAL "MultiThreaded$<$<CONFIG:Debug>:Debug>")
                message(FATAL_ERROR "${_TARGET} has inconsistent MSVC runtime: ${_RUNTIME}")
            endif ()
        endif ()
    endforeach ()
    get_property(_SUBDIRECTORIES DIRECTORY "${DIRECTORY}" PROPERTY SUBDIRECTORIES)
    foreach (_SUBDIRECTORY IN LISTS _SUBDIRECTORIES)
        tsfile_check_static_runtime("${_SUBDIRECTORY}")
    endforeach ()
endfunction()
cmake_language(DEFER CALL tsfile_check_static_runtime "${CMAKE_SOURCE_DIR}")
