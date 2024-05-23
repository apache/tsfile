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
message("Running in src/common directory")

aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR} common_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/allocator common_allocator_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/container common_container_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/tsblock common_tsblock_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/mutex common_mutex_SRC_LIST)
aux_source_directory(${CMAKE_CURRENT_SOURCE_DIR}/datatype common_datatype_SRC_LIST)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_library(common_obj OBJECT ${common_SRC_LIST} 
    ${common_allocator_SRC_LIST}
    ${common_container_SRC_LIST}
    ${common_tsblock_SRC_LIST} 
    ${common_mutex_SRC_LIST} 
    ${common_datatype_SRC_LIST})

