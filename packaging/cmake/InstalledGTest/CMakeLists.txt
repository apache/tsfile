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

cmake_minimum_required(VERSION 3.15)
if (POLICY CMP0135)
    cmake_policy(SET CMP0135 NEW)
endif ()
project(TsFileInstalledTestDependencies LANGUAGES CXX)

# Build the test framework only; the TsFile implementation comes from the
# downloaded and installed package. Match the Windows SDK's static CRT.
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(INSTALL_GTEST ON CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
set(gtest_force_shared_crt OFF CACHE BOOL "" FORCE)
include(FetchContent)
FetchContent_Declare(googletest
        URL "https://github.com/google/googletest/archive/refs/tags/release-1.12.1.zip"
        URL_HASH SHA256=24564e3b712d3eb30ac9a85d92f7d720f60cc0173730ac166f27dda7fed76cb2)
FetchContent_MakeAvailable(googletest)
