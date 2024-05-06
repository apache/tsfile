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

add_library(tsfile
        src/tsfile.cpp
        )

target_include_directories(
        tsfile
        PUBLIC
        "include"
        "../common/include")

IF (NOT WIN32)
    target_link_libraries(tsfile
            m
            )
ELSE()
    target_link_libraries(tsfile
            )
ENDIF()

if (BUILD_PHASE STREQUAL test-compile)
    add_executable(tsfile-test
            test/tsfile_test.cpp
            )
    target_link_libraries(tsfile-test
            tsfile
            unity

            )
    add_test(NAME tsfile-test COMMAND tsfile-test)
endif ()