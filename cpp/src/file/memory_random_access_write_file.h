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

#ifndef FILE_MEMORY_RANDOM_ACCESS_WRITE_FILE_H
#define FILE_MEMORY_RANDOM_ACCESS_WRITE_FILE_H

#include <stdint.h>

#include <cstddef>
#include <string>
#include <vector>

#include "file/random_access_write_file.h"

namespace storage {

/**
 * A RandomAccessWriteFile that keeps the written bytes in a growable buffer.
 *
 * A default-constructed sink is open and empty, so it can be handed to
 * TsFileIOWriter/TsFileWriter directly.  close() marks it closed (further
 * writes fail) but keeps the bytes: read data()/size() afterwards to obtain
 * the serialized TsFile, or hand release_data() to a
 * MemoryRandomAccessReadFile to read the file back.
 *
 * The sink cannot be opened for append: it never reads an existing file and
 * has no path, so recovery (RestorableTsFileIOWriter) stays local-file only.
 */
class MemoryRandomAccessWriteFile : public RandomAccessWriteFile {
   public:
    explicit MemoryRandomAccessWriteFile(
        const std::string& name = std::string());
    ~MemoryRandomAccessWriteFile() override = default;

    MemoryRandomAccessWriteFile(const MemoryRandomAccessWriteFile&) = delete;
    MemoryRandomAccessWriteFile& operator=(const MemoryRandomAccessWriteFile&) =
        delete;

    bool is_opened() const override { return opened_; }
    const std::string& file_path() const override { return path_; }

    int write(const char* buf, uint32_t len) override;
    /** Cursor position; 0 after close(), matching the local backend. */
    int64_t get_position() override;
    /** No-op: the bytes are already in memory. Returns E_OK while open. */
    int sync() override;
    int truncate(int64_t size) override;
    int seek_to_end() override;
    /** Marks the sink closed; the accumulated bytes remain available. */
    int close() override;

    /** Bytes written so far, regardless of the open/closed state. */
    const std::vector<char>& data() const { return data_; }
    int64_t size() const { return static_cast<int64_t>(data_.size()); }
    /** Move the accumulated bytes out and reset the cursor; the sink becomes
     * empty but stays in its current open/closed state. */
    std::vector<char> release_data();

   private:
    std::string path_;
    std::vector<char> data_;
    int64_t position_;
    bool opened_;
};

}  // namespace storage

#endif  // FILE_MEMORY_RANDOM_ACCESS_WRITE_FILE_H
