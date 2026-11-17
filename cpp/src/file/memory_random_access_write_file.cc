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

#include "file/memory_random_access_write_file.h"

#include <cstring>
#include <new>

#include "utils/errno_define.h"

namespace storage {

MemoryRandomAccessWriteFile::MemoryRandomAccessWriteFile(
    const std::string& name)
    : path_(name), data_(), position_(0), opened_(true) {}

int MemoryRandomAccessWriteFile::write(const char* buf, uint32_t len) {
    if (!opened_) {
        return common::E_FILE_WRITE_ERR;
    }
    if (buf == nullptr) {
        return len == 0 ? common::E_OK : common::E_INVALID_ARG;
    }
    if (len == 0) {
        return common::E_OK;
    }
    const int64_t end = position_ + static_cast<int64_t>(len);
    try {
        // A previous truncate() may have left the cursor past the end (the
        // same hole POSIX ftruncate() allows); extend with zeros first.
        if (end > static_cast<int64_t>(data_.size())) {
            data_.resize(static_cast<size_t>(end), 0);
        }
    } catch (const std::bad_alloc&) {
        return common::E_OOM;
    }
    std::memcpy(data_.data() + position_, buf, len);
    position_ = end;
    return common::E_OK;
}

int64_t MemoryRandomAccessWriteFile::get_position() {
    return opened_ ? position_ : 0;
}

int MemoryRandomAccessWriteFile::sync() {
    return opened_ ? common::E_OK : common::E_FILE_WRITE_ERR;
}

int MemoryRandomAccessWriteFile::truncate(int64_t size) {
    if (!opened_) {
        return common::E_FILE_WRITE_ERR;
    }
    if (size < 0) {
        return common::E_INVALID_ARG;
    }
    try {
        data_.resize(static_cast<size_t>(size), 0);
    } catch (const std::bad_alloc&) {
        return common::E_OOM;
    }
    // ftruncate() does not move the write cursor, so neither do we.
    return common::E_OK;
}

int MemoryRandomAccessWriteFile::seek_to_end() {
    if (!opened_) {
        return common::E_FILE_WRITE_ERR;
    }
    position_ = static_cast<int64_t>(data_.size());
    return common::E_OK;
}

int MemoryRandomAccessWriteFile::close() {
    opened_ = false;
    return common::E_OK;
}

std::vector<char> MemoryRandomAccessWriteFile::release_data() {
    std::vector<char> data;
    data.swap(data_);
    position_ = 0;
    return data;
}

}  // namespace storage
