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

message("Running in tools directory")

file(GLOB_RECURSE TSFILE_CLI_SRCS
        "cli/*.cc"
        "format/*.cc"
        "commands/*.cc")

add_library(tsfile_cli_obj OBJECT ${TSFILE_CLI_SRCS})
target_include_directories(tsfile_cli_obj PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
        ${PROJECT_SOURCE_DIR}/src)

if (ENABLE_ANTLR4)
    target_include_directories(tsfile_cli_obj PUBLIC
            ${PROJECT_SOURCE_DIR}/third_party/antlr4-cpp-runtime-4/runtime/src)
endif ()

target_compile_definitions(tsfile_cli_obj PRIVATE
        TSFILE_CLI_VERSION="${TsFile_CPP_VERSION}")

add_executable(tsfile_cli tools_main.cc $<TARGET_OBJECTS:tsfile_cli_obj>)
target_include_directories(tsfile_cli PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(tsfile_cli tsfile)
set_target_properties(tsfile_cli PROPERTIES
        OUTPUT_NAME tsfile
        RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)

install(TARGETS tsfile_cli RUNTIME DESTINATION bin)
