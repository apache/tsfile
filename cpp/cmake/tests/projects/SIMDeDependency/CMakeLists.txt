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

cmake_minimum_required(VERSION 3.11)
project(TsFileSIMDeDependencyTest NONE)

get_filename_component(_TSFILE_CMAKE_DIR
        "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${_TSFILE_CMAKE_DIR}")
include("${_TSFILE_CMAKE_DIR}/DependencySource.cmake")
include("${_TSFILE_CMAKE_DIR}/SIMDeDependency.cmake")

if (NOT TSFILE_SIMDE_SOURCE STREQUAL EXPECTED_SIMDE_SOURCE)
    message(FATAL_ERROR
            "SIMDe resolved to ${TSFILE_SIMDE_SOURCE}; "
            "expected ${EXPECTED_SIMDE_SOURCE}.")
endif ()

if (TSFILE_SIMDE_SOURCE STREQUAL "SYSTEM" AND
        NOT TARGET simde::simde)
    message(FATAL_ERROR
            "System SIMDe did not provide the simde::simde target.")
endif ()
