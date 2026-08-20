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

file(GLOB_RECURSE TSFILE_CLI_SRCS
        "cli/*.cc"
        "format/*.cc"
        "commands/*.cc")

add_library(tsfile_cli_obj OBJECT ${TSFILE_CLI_SRCS})
# Tool-own headers (cli/, commands/, format/) resolve from this directory;
# library headers come from the staged include tree like the rest of the build.
target_include_directories(tsfile_cli_obj PUBLIC
        ${CMAKE_SOURCE_DIR}/tools
        ${LIBRARY_INCLUDE_DIR}
        ${THIRD_PARTY_INCLUDE})

# Library headers are compiled from the staged include tree
# (LIBRARY_INCLUDE_DIR), which is populated by the copy_* targets that
# tsfile depends on transitively. Without this edge a clean parallel build
# can compile these sources before the headers exist.
add_dependencies(tsfile_cli_obj tsfile)

execute_process(
        COMMAND git rev-parse HEAD
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        OUTPUT_VARIABLE TSFILE_CLI_GIT_COMMIT
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
if (NOT TSFILE_CLI_GIT_COMMIT)
    set(TSFILE_CLI_GIT_COMMIT "unknown")
endif ()
string(TIMESTAMP TSFILE_CLI_BUILD_TIME "%Y-%m-%dT%H:%M:%SZ" UTC)

target_compile_definitions(tsfile_cli_obj PRIVATE
        TSFILE_CLI_VERSION="${TsFile_CPP_VERSION}"
        TSFILE_CLI_COMMIT="${TSFILE_CLI_GIT_COMMIT}"
        TSFILE_CLI_BUILT="${TSFILE_CLI_BUILD_TIME}")

add_executable(tsfile_cli tools_main.cc $<TARGET_OBJECTS:tsfile_cli_obj>)
target_include_directories(tsfile_cli PRIVATE ${CMAKE_SOURCE_DIR}/tools)
target_link_libraries(tsfile_cli tsfile)
set_target_properties(tsfile_cli PROPERTIES
        OUTPUT_NAME tsfile-cli
        RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)

install(TARGETS tsfile_cli RUNTIME DESTINATION bin)
