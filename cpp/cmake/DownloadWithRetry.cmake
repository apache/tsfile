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

include_guard(GLOBAL)

# Retry only transient transport/server failures. Archive identity is still
# checked by the caller before the downloaded file enters the dependency cache.
function(tsfile_download_with_retry URL DESTINATION OUTPUT_STATUS)
    set(_TSFILE_TRANSIENT_CURL_CODES 6 7 18 28 35 52 55 56)
    set(_TSFILE_TRANSIENT_HTTP_CODES 408 429 500 502 503 504)
    foreach (_TSFILE_ATTEMPT RANGE 1 3)
        file(REMOVE "${DESTINATION}")
        file(DOWNLOAD "${URL}" "${DESTINATION}"
                STATUS _TSFILE_STATUS
                LOG _TSFILE_LOG
                TLS_VERIFY ON
                TIMEOUT 120
                INACTIVITY_TIMEOUT 30)
        list(GET _TSFILE_STATUS 0 _TSFILE_RESULT)
        if (_TSFILE_RESULT EQUAL 0)
            break()
        endif ()
        file(REMOVE "${DESTINATION}")
        list(FIND _TSFILE_TRANSIENT_CURL_CODES "${_TSFILE_RESULT}"
                _TSFILE_TRANSIENT_INDEX)
        if (_TSFILE_RESULT EQUAL 22)
            # Use the final HTTP response after redirects/proxy negotiation.
            string(REGEX MATCHALL "HTTP/[0-9.]+ [0-9][0-9][0-9]"
                    _TSFILE_RESPONSES "${_TSFILE_LOG}")
            if (_TSFILE_RESPONSES)
                list(GET _TSFILE_RESPONSES -1 _TSFILE_RESPONSE)
                string(REGEX REPLACE "^HTTP/[0-9.]+ " ""
                        _TSFILE_HTTP_CODE "${_TSFILE_RESPONSE}")
                list(FIND _TSFILE_TRANSIENT_HTTP_CODES "${_TSFILE_HTTP_CODE}"
                        _TSFILE_TRANSIENT_INDEX)
            endif ()
        endif ()
        if (_TSFILE_TRANSIENT_INDEX EQUAL -1 OR _TSFILE_ATTEMPT EQUAL 3)
            break()
        endif ()
        list(GET _TSFILE_STATUS 1 _TSFILE_MESSAGE)
        message(STATUS "Dependency download attempt ${_TSFILE_ATTEMPT}/3 failed: "
                "${_TSFILE_MESSAGE}; retrying ${URL}")
        execute_process(COMMAND "${CMAKE_COMMAND}" -E sleep "${_TSFILE_ATTEMPT}")
    endforeach ()
    set(${OUTPUT_STATUS} "${_TSFILE_STATUS}" PARENT_SCOPE)
endfunction()
