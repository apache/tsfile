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

message(STATUS "Running in src/labview_wrapper directory")

add_library(tsfile_labview SHARED tsfile_labview.cc async_close.cc)
target_include_directories(tsfile_labview PRIVATE ${PROJECT_SRC_DIR})
target_link_libraries(tsfile_labview PRIVATE tsfile)

set_target_properties(tsfile_labview PROPERTIES
    OUTPUT_NAME "tsfile_labview"
    CXX_STANDARD 11
    CXX_STANDARD_REQUIRED ON)

if (WIN32)
    set_target_properties(tsfile_labview PROPERTIES
        WINDOWS_EXPORT_ALL_SYMBOLS ON)
endif ()

install(TARGETS tsfile_labview
        LIBRARY DESTINATION ${LIBRARY_OUTPUT_PATH}
        RUNTIME DESTINATION ${LIBRARY_OUTPUT_PATH})
install(FILES tsfile_labview.h DESTINATION include)

option(BUILD_LABVIEW_WRAPPER_TESTS "Build LabVIEW shim standalone tests"
       ${BUILD_TEST})
if (BUILD_LABVIEW_WRAPPER_TESTS)
    add_executable(tsfile_labview_async_close_coordinator_test
                   test_async_close.cc async_close.cc)
    target_include_directories(tsfile_labview_async_close_coordinator_test
                               PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    set_target_properties(tsfile_labview_async_close_coordinator_test PROPERTIES
        CXX_STANDARD 11
        CXX_STANDARD_REQUIRED ON
        RUNTIME_OUTPUT_DIRECTORY ${LIBRARY_OUTPUT_PATH})

    foreach(test_name smoke read_only dump block block_read)
        add_executable(tsfile_labview_${test_name}_test test_${test_name}.c)
        target_link_libraries(tsfile_labview_${test_name}_test
                              PRIVATE tsfile_labview)
        if (UNIX)
            target_link_libraries(tsfile_labview_${test_name}_test PRIVATE m)
        endif ()
        set_target_properties(tsfile_labview_${test_name}_test PROPERTIES
            C_STANDARD 99
            C_STANDARD_REQUIRED ON
            RUNTIME_OUTPUT_DIRECTORY ${LIBRARY_OUTPUT_PATH})
    endforeach()

    add_executable(tsfile_labview_config_test test_config.c)
    target_include_directories(tsfile_labview_config_test
                               PRIVATE ${PROJECT_SRC_DIR})
    target_link_libraries(tsfile_labview_config_test
                          PRIVATE tsfile_labview tsfile)
    set_target_properties(tsfile_labview_config_test PROPERTIES
        C_STANDARD 99
        C_STANDARD_REQUIRED ON
        RUNTIME_OUTPUT_DIRECTORY ${LIBRARY_OUTPUT_PATH})

    add_executable(tsfile_labview_codec_benchmark benchmark_codec_matrix.c)
    target_include_directories(tsfile_labview_codec_benchmark
                               PRIVATE ${PROJECT_SRC_DIR})
    target_link_libraries(tsfile_labview_codec_benchmark
                          PRIVATE tsfile_labview tsfile)
    set_target_properties(tsfile_labview_codec_benchmark PROPERTIES
        C_STANDARD 99
        C_STANDARD_REQUIRED ON
        RUNTIME_OUTPUT_DIRECTORY ${LIBRARY_OUTPUT_PATH})
endif ()
