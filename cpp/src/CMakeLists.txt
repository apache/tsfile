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
message("Running in src diectory")
include(${PROJECT_SOURCE_DIR}/cmake/build_function.cmake)
if (${COV_ENABLED})
    add_compile_options(-fprofile-arcs -ftest-coverage)
endif ()
add_subdirectory(common)
add_subdirectory(compress)
add_subdirectory(cwrapper)
add_subdirectory(encoding)
add_subdirectory(file)
add_subdirectory(reader)
add_subdirectory(utils)
add_subdirectory(writer)

add_library(tsfile SHARED)
if (${COV_ENABLED})
    message("Enable code cov...")
    target_link_libraries(tsfile common_obj compress_obj cwrapper_obj file_obj read_obj write_obj -lgcov)
else()
    message("Disable code cov...")
    target_link_libraries(tsfile common_obj compress_obj cwrapper_obj file_obj read_obj write_obj)
endif()

set(LIBTSFILE_PROJECT_VERSION 1.0)
set(LIBTSFILE_SO_VERSION 1.0)
set_target_properties(tsfile PROPERTIES VERSION ${LIBTSFILE_PROJECT_VERSION})
set_target_properties(tsfile PROPERTIES SOVERSION ${LIBTSFILE_SO_VERSION})

set(LIBTSFILE_SDK_DIR ${LIBRARY_OUTPUT_PATH})
install(TARGETS tsfile LIBRARY DESTINATION ${LIBTSFILE_SDK_DIR})

set(SNAPPY_LIB_NAME "snappy")
set(LZ4_LIB_NAME "LZ4")
set(LZO_LIB_NAME "lzokay")

target_link_libraries(compress_obj ${SNAPPY_LIB_NAME} ${LZ4_LIB_NAME} ${LZO_LIB_NAME})
target_link_libraries(common_obj ${SNAPPY_LIB_NAME} ${LZ4_LIB_NAME} ${LZO_LIB_NAME})
target_link_libraries(read_obj ${SNAPPY_LIB_NAME} ${LZ4_LIB_NAME} ${LZO_LIB_NAME})
target_link_libraries(write_obj ${SNAPPY_LIB_NAME} ${LZ4_LIB_NAME} ${LZO_LIB_NAME})

# set(CMAKE_PREFIX_PATH ../../third-party/zlib-1.2.13/install/lib)
# set(ZLIB_LIB_DIR ../../third-party/zlib-1.2.13/install/lib)
# # find_library(my_z_lib NAMES z PATHS ${ZLIB_LIB_DIR} NO_DEFAULT_PATH REQUIRED)
# # target_link_libraries(tsfile ${my_z_lib})

# set(CMAKE_PREFIX_PATH ../../third-party/lz4-dev/lib)
# set(LZ4_LIB_DIR ../../third-party/lz4-dev/lib)
# find_library(my_lz4_lib NAMES lz4 PATHS ${LZ4_LIB_DIR} NO_DEFAULT_PATH REQUIRED)
# link_directories(${LZ4_LIB_DIR})
# target_link_libraries(tsfile ${my_lz4_lib})

# if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
#   add_custom_command(TARGET tsfile POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change `otool -L ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib | grep liblz4 | sed 's/dylib.*/dylib/g'` ${my_lz4_lib} ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib)
#   add_custom_command(TARGET tsfile POST_BUILD COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change `otool -L ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib | grep libz | sed 's/dylib.*/dylib/g'` ${my_z_lib} ${LIBRARY_OUTPUT_PATH}/libtsfile.dylib)
# endif()
