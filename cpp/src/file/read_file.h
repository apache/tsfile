/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#ifndef FILE_READ_FILE_H
#define FILE_READ_FILE_H

#include <stdint.h>

#include <cstddef>
#include <string>

#include "common/config/config.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

class ReadFile {
   public:
    ReadFile();
    ~ReadFile() { destroy(); }
    ReadFile(const ReadFile&) = delete;
    ReadFile& operator=(const ReadFile&) = delete;

    void destroy() { close(); }

    int open(const std::string& file_path);
    FORCE_INLINE bool is_opened() const {
        return fd_ >= 0 || mapped_data_ != nullptr;
    }
    FORCE_INLINE int64_t file_size() const { return file_size_; }
    FORCE_INLINE const std::string& file_path() const { return file_path_; }
    FORCE_INLINE unsigned char file_version() const { return file_version_; }
    FORCE_INLINE common::FileReadBackend active_backend() const {
        return active_backend_;
    }

    /** Return size and the Dataset Index v1 FNV fingerprint of size+mtime_ns.
     * MMAP readers return the generation captured before closing the file
     * descriptor.
     */
    int generation(uint64_t& size, uint64_t& fingerprint) const;

    /*
     * try to reader @buf_size bytes from @offset of this file
     * into @buf. @read_len return the actual len reader.
     */
    int read(int64_t offset, char* buf, int32_t buf_size,
             int32_t& ret_read_len);
    // open()/close() must not race with read() or generation(). Concurrent
    // reads are supported after the file has been opened.
    void close();

   private:
    int get_file_size(int64_t& file_size);
    int get_file_generation_from_descriptor(uint64_t& size,
                                            uint64_t& fingerprint) const;
    int check_file_magic();
    int map_file();
    void unmap_file();

   private:
    // 2 magic strings + file_version
    static const int32_t MIN_FILE_SIZE = 13;

   private:
    std::string file_path_;
    int fd_;
    int64_t file_size_;
    unsigned char file_version_;
    uint64_t mapped_file_fingerprint_;
    const char* mapped_data_;
    size_t mapped_size_;
    common::FileReadBackend active_backend_;
#ifdef _WIN32
    void* mapping_handle_;
#else
    uint64_t file_device_;
    uint64_t file_inode_;
#endif
};

}  // end namespace storage
#endif  // FILE_READ_FILE_H
