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

#ifndef FILE_MEMORY_RANDOM_ACCESS_READ_FILE_H
#define FILE_MEMORY_RANDOM_ACCESS_READ_FILE_H

#include <stdint.h>

#include <cstddef>
#include <string>
#include <vector>

#include "file/random_access_read_file.h"

namespace storage {

/**
 * A RandomAccessReadFile over an in-memory byte sequence.
 *
 * Takes ownership of the bytes (move the result of
 * MemoryRandomAccessWriteFile::release_data() in) and serves reads from them
 * until close().  The content is immutable, so the generation fingerprint is
 * computed once at construction.
 */
class MemoryRandomAccessReadFile : public RandomAccessReadFile {
   public:
    explicit MemoryRandomAccessReadFile(
        std::vector<char> data, const std::string& name = std::string());
    MemoryRandomAccessReadFile(const char* data, size_t size,
                               const std::string& name = std::string());
    ~MemoryRandomAccessReadFile() override = default;

    MemoryRandomAccessReadFile(const MemoryRandomAccessReadFile&) = delete;
    MemoryRandomAccessReadFile& operator=(const MemoryRandomAccessReadFile&) =
        delete;

    bool is_opened() const override { return opened_; }
    int64_t file_size() const override;
    const std::string& file_path() const override { return path_; }
    int generation(uint64_t& size, uint64_t& fingerprint) const override;

    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override;
    /** Drops the backing bytes; read() afterwards returns E_FILE_READ_ERR. */
    void close() override;

   private:
    void init_fingerprint();

    std::string path_;
    std::vector<char> data_;
    uint64_t fingerprint_;
    bool opened_;
};

}  // namespace storage

#endif  // FILE_MEMORY_RANDOM_ACCESS_READ_FILE_H
