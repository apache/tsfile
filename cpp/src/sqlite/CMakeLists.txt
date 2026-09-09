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

find_package(SQLite3 3.31 REQUIRED)

# Apple's SDK SQLite header disables extension loading. Require headers from
# the same extension-capable SQLite installation used by the application.
include(CheckCXXSourceCompiles)
set(_tsfile_saved_required_includes "${CMAKE_REQUIRED_INCLUDES}")
set(CMAKE_REQUIRED_INCLUDES "${SQLite3_INCLUDE_DIRS}")
unset(TSFILE_SQLITE_CAN_LOAD_EXTENSIONS CACHE)
check_cxx_source_compiles("
#include <sqlite3ext.h>
#ifdef SQLITE_OMIT_LOAD_EXTENSION
#error SQLite extension loading is disabled
#endif
int main() { return 0; }
" TSFILE_SQLITE_CAN_LOAD_EXTENSIONS)
set(CMAKE_REQUIRED_INCLUDES "${_tsfile_saved_required_includes}")
if (NOT TSFILE_SQLITE_CAN_LOAD_EXTENSIONS)
    message(FATAL_ERROR "SQLite headers disable extension loading. Set SQLite3_INCLUDE_DIR and SQLite3_LIBRARY to an extension-capable SQLite installation (for example Homebrew SQLite on macOS).")
endif ()

add_library(tsfile_sqlite MODULE tsfile_sqlite.cc)
target_include_directories(tsfile_sqlite PRIVATE
        ${CMAKE_SOURCE_DIR}/src
        ${SQLite3_INCLUDE_DIRS})
target_link_libraries(tsfile_sqlite PRIVATE tsfile)
set_target_properties(tsfile_sqlite PROPERTIES
        PREFIX ""
        OUTPUT_NAME "tsfile_sqlite"
        LIBRARY_OUTPUT_DIRECTORY ${LIBRARY_OUTPUT_PATH})

if (APPLE)
    set_target_properties(tsfile_sqlite PROPERTIES
            SUFFIX ".dylib"
            BUILD_RPATH "@loader_path"
            INSTALL_RPATH "@loader_path")
else ()
    set_target_properties(tsfile_sqlite PROPERTIES
            BUILD_RPATH "$ORIGIN"
            INSTALL_RPATH "$ORIGIN")
endif ()

install(TARGETS tsfile_sqlite
        LIBRARY DESTINATION ${LIBRARY_OUTPUT_PATH})
