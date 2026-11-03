/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Built and registered as a CTest case only when ENABLE_ASAN is ON.
//
// The program deliberately writes past the end of a heap allocation. The
// accompanying CTest entry passes only when the sanitizer reports the access,
// so a build that links the runtime without instrumenting TsFile code fails
// this test instead of silently losing its memory-safety checks.

#include <cstdlib>

int main() {
    char* buffer = static_cast<char*>(std::malloc(4));
    if (buffer == nullptr) {
        return 0;
    }
    volatile char* access = buffer;
    access[8] = 1;
    std::free(buffer);
    return 0;
}
