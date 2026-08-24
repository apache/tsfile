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

get_filename_component(_TSFILE_CMAKE_DIR "${CMAKE_CURRENT_LIST_DIR}/.."
        ABSOLUTE)

if (TEST_CASE STREQUAL "INVALID_SOURCE")
    set(TSFILE_DEPENDENCY_SOURCE "invalid" CACHE STRING "" FORCE)
    include("${_TSFILE_CMAKE_DIR}/DependencySource.cmake")
elseif (TEST_CASE STREQUAL "SYSTEM_MISSING")
    set(TSFILE_DEPENDENCY_SOURCE "SYSTEM" CACHE STRING "" FORCE)
    include("${_TSFILE_CMAKE_DIR}/DependencySource.cmake")
    tsfile_resolve_dependency_source("Example" FALSE _TSFILE_RESULT)
else ()
    message(FATAL_ERROR "Unknown TEST_CASE='${TEST_CASE}'.")
endif ()
