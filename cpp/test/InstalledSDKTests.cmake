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
# This entry point compiles test sources only. All TsFile headers, feature
# definitions and libraries must come from the installed CMake package.
find_package(TsFile CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)
find_package(Threads REQUIRED)

enable_testing()

# Reuse existing API regression tests without linking CLI implementation objects,
# codec implementation targets, or a library compiled with ENABLE_TEST hooks.
add_executable(TsFile_Installed_Test
        cwrapper/cwrapper_test.cc
        cwrapper/cwrapper_public_writer_test.cc
        cwrapper/cwrapper_metadata_test.cc
        cwrapper/cwrapper_properties_test.cc
        cwrapper/c_release_test.cc
        cwrapper/query_by_row_cwrapper_test.cc
        reader/table_view/tsfile_reader_table_test.cc
        reader/table_view/tsfile_reader_table_batch_test.cc
        reader/table_view/tsfile_table_query_by_row_test.cc
        reader/tree_view/tsfile_reader_tree_test.cc
        reader/tree_view/tsfile_tree_query_by_row_test.cc
        writer/table_view/tsfile_writer_table_test.cc
        writer/table_view/tsfile_parallel_tablet_write_regression_test.cc
        writer/tsfile_writer_test.cc
        writer/tsfile_properties_test.cc)
target_compile_features(TsFile_Installed_Test PRIVATE cxx_std_14)
target_link_libraries(TsFile_Installed_Test PRIVATE
        TsFile::tsfile GTest::gtest_main Threads::Threads)

# Windows resolves DLLs beside the executable before searching PATH. Copy the
# imported SDK library, rather than relying on a build machine's library path.
get_target_property(_TSFILE_INSTALLED_LIBRARY_TYPE TsFile::tsfile TYPE)
if (WIN32 AND _TSFILE_INSTALLED_LIBRARY_TYPE STREQUAL "SHARED_LIBRARY")
    add_custom_command(TARGET TsFile_Installed_Test POST_BUILD
            COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_FILE:TsFile::tsfile>
            $<TARGET_FILE_DIR:TsFile_Installed_Test>
            VERBATIM)
endif ()
unset(_TSFILE_INSTALLED_LIBRARY_TYPE)

include(GoogleTest)
gtest_discover_tests(TsFile_Installed_Test DISCOVERY_TIMEOUT 120
        PROPERTIES TIMEOUT 120)
