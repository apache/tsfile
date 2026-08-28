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

#pragma once

#include <fcntl.h>

#include <cerrno>
#include <climits>
#include <string>

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace storage {
namespace file_internal {

inline int open_utf8(const std::string& path, int flags, int mode = 0) {
#ifdef _WIN32
    if (path.find('\0') != std::string::npos || path.size() > INT_MAX) {
        errno = EINVAL;
        return -1;
    }
    if (path.empty()) {
        errno = ENOENT;
        return -1;
    }
    const int size =
        MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.data(),
                            static_cast<int>(path.size()), nullptr, 0);
    if (size <= 0) {
        errno = EINVAL;
        return -1;
    }
    std::wstring wide_path(static_cast<size_t>(size), L'\0');
    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path.data(),
                            static_cast<int>(path.size()), &wide_path[0],
                            size) != size) {
        errno = EINVAL;
        return -1;
    }
    return ::_wopen(wide_path.c_str(), flags, mode);
#else
    return ::open(path.c_str(), flags, mode);
#endif
}

}  // namespace file_internal
}  // namespace storage
