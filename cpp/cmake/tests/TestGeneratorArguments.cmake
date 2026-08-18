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

set(_TSFILE_TEST_GENERATOR_ARGUMENTS "")
if (TEST_CMAKE_GENERATOR)
    list(APPEND _TSFILE_TEST_GENERATOR_ARGUMENTS
            -G "${TEST_CMAKE_GENERATOR}")
endif ()
if (TEST_CMAKE_GENERATOR_PLATFORM)
    list(APPEND _TSFILE_TEST_GENERATOR_ARGUMENTS
            -A "${TEST_CMAKE_GENERATOR_PLATFORM}")
endif ()
if (TEST_CMAKE_GENERATOR_TOOLSET)
    list(APPEND _TSFILE_TEST_GENERATOR_ARGUMENTS
            -T "${TEST_CMAKE_GENERATOR_TOOLSET}")
endif ()
