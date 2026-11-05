#[[
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
]]

# CTest driver for asan_negative_control. The program performs a deliberate
# out-of-bounds heap write, so the test passes only when the sanitizer reports
# it. The exit status is ignored on purpose: ASan aborts on some platforms and
# exits with a code on others, and CTest marks a subprocess abort as a failure
# regardless of PASS_REGULAR_EXPRESSION.

if (NOT DEFINED ASAN_NEGATIVE_CONTROL OR NOT EXISTS "${ASAN_NEGATIVE_CONTROL}")
    message(FATAL_ERROR
            "ASAN_NEGATIVE_CONTROL does not name an existing executable: "
            "${ASAN_NEGATIVE_CONTROL}")
endif ()

execute_process(
        COMMAND "${ASAN_NEGATIVE_CONTROL}"
        RESULT_VARIABLE _result
        OUTPUT_VARIABLE _stdout
        ERROR_VARIABLE _stderr)

set(_combined "${_stdout}\n${_stderr}")
string(FIND "${_combined}" "AddressSanitizer" _report_position)
if (_report_position EQUAL -1)
    message(FATAL_ERROR
            "The sanitizer did not report the deliberate out-of-bounds write "
            "(exit status: ${_result}). The build is probably not compiled "
            "with instrumentation.\nOutput:\n${_combined}")
endif ()
