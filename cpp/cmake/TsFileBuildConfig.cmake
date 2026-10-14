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

# Record the resolved runtime on both the build tree and the installed SDK.
# file(GENERATE) evaluates Debug suffixes for each configuration separately.
set(_TSFILE_MSVC_RUNTIME "")
if (MSVC)
    get_target_property(_TSFILE_MSVC_RUNTIME tsfile MSVC_RUNTIME_LIBRARY)
    if (NOT _TSFILE_MSVC_RUNTIME)
        set(_TSFILE_MSVC_RUNTIME "MultiThreaded$<$<CONFIG:Debug>:Debug>DLL")
    endif ()
endif ()
set(_TSFILE_RUNTIME_LIBRARY "")
if (TSFILE_BUILD_SHARED)
    if (WIN32)
        set(_TSFILE_RUNTIME_LIBRARY "$<TARGET_FILE_NAME:tsfile>")
    else ()
        set(_TSFILE_RUNTIME_LIBRARY "$<TARGET_SONAME_FILE_NAME:tsfile>")
    endif ()
endif ()
set(_TSFILE_BUILD_CONFIG "$<TARGET_FILE_DIR:tsfile>/tsfile-build-config.json")
file(GENERATE OUTPUT "${_TSFILE_BUILD_CONFIG}" CONTENT
"{\n  \"schema_version\": 1,\n  \"version\": \"${TSFILE_PACKAGE_VERSION}\",\n  \"compiler\": \"${CMAKE_CXX_COMPILER_ID}\",\n  \"configuration\": \"$<CONFIG>\",\n  \"msvc_runtime\": \"${_TSFILE_MSVC_RUNTIME}\",\n  \"runtime_library\": \"${_TSFILE_RUNTIME_LIBRARY}\"\n}\n")
install(FILES "${_TSFILE_BUILD_CONFIG}"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/tsfile"
        COMPONENT development)
unset(_TSFILE_BUILD_CONFIG)
unset(_TSFILE_RUNTIME_LIBRARY)
unset(_TSFILE_MSVC_RUNTIME)
