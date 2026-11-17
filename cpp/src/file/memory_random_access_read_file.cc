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

#include "file/memory_random_access_read_file.h"

#include <cstring>
#include <utility>

#include "utils/errno_define.h"

namespace storage {

namespace {
// Same FNV-1a construction the local backend uses for size+mtime, but over
// the bytes themselves: an in-memory source has no stat data and the content
// is immutable, so the content digest is the generation.
uint64_t content_fingerprint(const std::vector<char>& data) {
    uint64_t hash = 1469598103934665603ULL;
    const uint64_t size = static_cast<uint64_t>(data.size());
    for (size_t byte = 0; byte < 8; ++byte) {
        hash ^= static_cast<uint8_t>(size >> (byte * 8));
        hash *= 1099511628211ULL;
    }
    for (size_t i = 0; i < data.size(); ++i) {
        hash ^= static_cast<uint8_t>(data[i]);
        hash *= 1099511628211ULL;
    }
    return hash;
}
}  // namespace

MemoryRandomAccessReadFile::MemoryRandomAccessReadFile(std::vector<char> data,
                                                       const std::string& name)
    : path_(name), data_(std::move(data)), fingerprint_(0), opened_(true) {
    init_fingerprint();
}

MemoryRandomAccessReadFile::MemoryRandomAccessReadFile(const char* data,
                                                       size_t size,
                                                       const std::string& name)
    : path_(name), data_(), fingerprint_(0), opened_(true) {
    if (data != nullptr && size > 0) {
        data_.assign(data, data + size);
    }
    init_fingerprint();
}

void MemoryRandomAccessReadFile::init_fingerprint() {
    fingerprint_ = content_fingerprint(data_);
}

int64_t MemoryRandomAccessReadFile::file_size() const {
    return opened_ ? static_cast<int64_t>(data_.size()) : -1;
}

int MemoryRandomAccessReadFile::generation(uint64_t& size,
                                           uint64_t& fingerprint) const {
    if (!opened_) {
        return common::E_FILE_READ_ERR;
    }
    size = static_cast<uint64_t>(data_.size());
    fingerprint = fingerprint_;
    return common::E_OK;
}

int MemoryRandomAccessReadFile::read(int64_t offset, char* buffer, int32_t size,
                                     int32_t& read_size) {
    read_size = 0;
    if (offset < 0 || size < 0 || (buffer == nullptr && size > 0)) {
        return common::E_INVALID_ARG;
    }
    if (!opened_) {
        return common::E_FILE_READ_ERR;
    }
    if (size == 0 || offset >= static_cast<int64_t>(data_.size())) {
        return common::E_OK;
    }
    const int64_t available = static_cast<int64_t>(data_.size()) - offset;
    const int32_t target_size = available < static_cast<int64_t>(size)
                                    ? static_cast<int32_t>(available)
                                    : size;
    std::memcpy(buffer, data_.data() + offset,
                static_cast<size_t>(target_size));
    read_size = target_size;
    return common::E_OK;
}

void MemoryRandomAccessReadFile::close() {
    opened_ = false;
    std::vector<char>().swap(data_);
}

}  // namespace storage
