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
project(TsFileANTLR4DependencyTest NONE)

get_filename_component(_TSFILE_CMAKE_DIR
        "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${_TSFILE_CMAKE_DIR}")
include("${_TSFILE_CMAKE_DIR}/DependencySource.cmake")
include("${_TSFILE_CMAKE_DIR}/ANTLR4Dependency.cmake")

if (NOT TSFILE_ANTLR4_SOURCE STREQUAL EXPECTED_ANTLR4_SOURCE)
    message(FATAL_ERROR
            "ANTLR4 resolved to ${TSFILE_ANTLR4_SOURCE}; "
            "expected ${EXPECTED_ANTLR4_SOURCE}.")
endif ()

if (TSFILE_ANTLR4_SOURCE STREQUAL "SYSTEM" AND
        "${TSFILE_ANTLR4_SYSTEM_TARGET}" STREQUAL "")
    message(FATAL_ERROR
            "System ANTLR4 did not provide a supported runtime target.")
endif ()

if (TSFILE_ANTLR4_SOURCE STREQUAL "SYSTEM" AND
        NOT IS_DIRECTORY "${TSFILE_ANTLR4_SYSTEM_INCLUDE_DIR}")
    message(FATAL_ERROR
            "System ANTLR4 did not preserve its public include directory.")
endif ()
