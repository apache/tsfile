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

# CopyToDir.cmake

# This function is used to copy files to a directory and it will handle relative paths automatically.
function(copy_to_dir)
    set(INCLUDE_EXPORT_DR ${LIBRARY_INCLUDE_DIR} CACHE INTERNAL "Include export directory")
    foreach(file ${ARGN})
        get_filename_component(file_name ${file} NAME)
        get_filename_component(file_path ${file} PATH)
        string(REPLACE "${CMAKE_SOURCE_DIR}/src" "" relative_path "${file_path}")
        add_custom_target(
            copy_${file_name} ALL
            COMMAND ${CMAKE_COMMAND} -E make_directory ${INCLUDE_EXPORT_DR}/${relative_path}
            COMMAND ${CMAKE_COMMAND} -E copy_if_different ${file} ${INCLUDE_EXPORT_DR}/${relative_path}/${file_name}
            COMMENT "Copying ${file_name} to ${INCLUDE_EXPORT_DR}/${relative_path}"
        )
    endforeach()
endfunction()


