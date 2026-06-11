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

#ifndef TEST_LARGE_FILE_TEST_COMMON_H
#define TEST_LARGE_FILE_TEST_COMMON_H

#include <sys/stat.h>

#include <cstdint>
#include <random>
#include <string>

namespace large_file_test {

constexpr int64_t kTargetFileSize =
    static_cast<int64_t>(4) * 1024 * 1024 * 1024;
constexpr int64_t kMinAcceptableFileSize =
    static_cast<int64_t>(3800) * 1024 * 1024;
constexpr int64_t kStartTime = 1622505600000LL;
constexpr uint32_t kTabletRows = 50000;
constexpr int64_t kFlushRows = 1000000;

inline int64_t GetFileSize(const std::string& path) {
    struct stat s;
    if (stat(path.c_str(), &s) != 0) {
        return -1;
    }
    return static_cast<int64_t>(s.st_size);
}

inline std::string RandomSuffix(int length = 10) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 61);
    const std::string chars =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    std::string out;
    out.reserve(length);
    for (int i = 0; i < length; ++i) {
        out += chars[dis(gen)];
    }
    return out;
}

}  // namespace large_file_test

#endif  // TEST_LARGE_FILE_TEST_COMMON_H
