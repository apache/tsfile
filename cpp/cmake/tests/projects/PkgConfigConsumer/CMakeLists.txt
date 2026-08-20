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
project(TsFilePkgConfigConsumer LANGUAGES C CXX)

find_package(PkgConfig REQUIRED)
pkg_check_modules(TsFile REQUIRED IMPORTED_TARGET libtsfile)
if (NOT TsFile_INCLUDEDIR MATCHES "/include$")
    message(FATAL_ERROR "pkg-config did not report the installed include root: ${TsFile_INCLUDEDIR}")
endif ()

add_executable(tsfile_pkgconfig_consumer main.c)
target_link_libraries(tsfile_pkgconfig_consumer PRIVATE PkgConfig::TsFile)

add_executable(tsfile_pkgconfig_cpp_consumer main.cc)
target_link_libraries(tsfile_pkgconfig_cpp_consumer PRIVATE PkgConfig::TsFile)
