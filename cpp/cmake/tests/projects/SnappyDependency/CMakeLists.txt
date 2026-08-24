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
project(TsFileSnappyDependencyTest NONE)

get_filename_component(_TSFILE_CMAKE_DIR
        "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
include("${_TSFILE_CMAKE_DIR}/DependencySource.cmake")
include("${_TSFILE_CMAKE_DIR}/SnappyDependency.cmake")

if (NOT TSFILE_SNAPPY_SOURCE STREQUAL EXPECTED_SNAPPY_SOURCE)
    message(FATAL_ERROR
            "Snappy resolved to ${TSFILE_SNAPPY_SOURCE}; "
            "expected ${EXPECTED_SNAPPY_SOURCE}.")
endif ()

if (TSFILE_SNAPPY_SOURCE STREQUAL "SYSTEM" AND
        NOT TARGET Snappy::snappy)
    message(FATAL_ERROR
            "System Snappy did not provide the Snappy::snappy target.")
endif ()
