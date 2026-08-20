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
project(TsFileInstalledConsumer LANGUAGES CXX)

find_package(TsFile CONFIG REQUIRED)

get_target_property(_TSFILE_INCLUDE_DIRS TsFile::tsfile
        INTERFACE_INCLUDE_DIRECTORIES)
if (NOT _TSFILE_INCLUDE_DIRS MATCHES "/include")
    message(FATAL_ERROR "TsFile target does not expose the installed include root")
endif ()
if (_TSFILE_INCLUDE_DIRS MATCHES "cpp/src|build/")
    message(FATAL_ERROR "TsFile target leaks a source or build include path: ${_TSFILE_INCLUDE_DIRS}")
endif ()

get_target_property(_TSFILE_LINK_LIBRARIES TsFile::tsfile
        INTERFACE_LINK_LIBRARIES)
if (_TSFILE_LINK_LIBRARIES MATCHES "(^|;)[^;]*_obj($|;)|cpp/src|build/")
    message(FATAL_ERROR
            "TsFile target leaks private build link requirements: "
            "${_TSFILE_LINK_LIBRARIES}")
endif ()

add_executable(tsfile_installed_consumer main.cc)
target_link_libraries(tsfile_installed_consumer PRIVATE TsFile::tsfile)
