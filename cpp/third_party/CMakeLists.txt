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
if (ENABLE_ANTLR4)
    add_subdirectory(antlr4-cpp-runtime-4)
    message("ANTLR4 runtime is enabled")
else()
    message("ANTLR4 runtime is disabled")
endif()
if (ENABLE_SNAPPY)
    add_subdirectory(google_snappy)
endif()

if (ENABLE_LZ4)
    add_subdirectory(lz4)
endif()

if (ENABLE_LZOKAY)
    add_subdirectory(lzokay)
endif()

if (ENABLE_ZLIB)
    add_subdirectory(zlib-1.3.1)
endif()

if (ENABLE_SIMDE)
    add_subdirectory(simde-0.8.4-rc3)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)