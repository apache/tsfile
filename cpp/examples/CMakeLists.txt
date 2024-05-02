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

add_library(tsfile-examples
        src/example.cpp
        )

target_include_directories(
        tsfile-examples
        PUBLIC
        "include"
        "../common/include"
        "../tsfile/include")

IF (NOT WIN32)
    target_link_libraries(tsfile-examples
            m
            )
ELSE()
    target_link_libraries(tsfile-examples
            )
ENDIF()

#if (BUILD_PHASE STREQUAL test-compile)
#    add_executable(tsfile-examples-test
#            test/system_test.c
#            )
#    target_link_libraries(tsfile-examples-test
#            tsfile-examples
#            unity
#
#            )
#    add_test(NAME tsfile-examples-test COMMAND tsfile-examples-test)
#endif ()