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

#ifndef FILE_RANDOM_ACCESS_FILE_H
#define FILE_RANDOM_ACCESS_FILE_H

#include <stdint.h>

#include <string>

namespace storage {

/**
 * A readable, seek-independent byte source used by the TsFile reader stack.
 *
 * Implementations must continue short underlying reads until the requested
 * range is filled or EOF is reached. Concurrent read() calls must be safe
 * after initialization; close() must not race with read() or generation().
 */
class RandomAccessFile {
   public:
    virtual ~RandomAccessFile() = default;

    virtual bool is_opened() const = 0;
    virtual int64_t file_size() const = 0;
    virtual const std::string& file_path() const = 0;
    virtual int generation(uint64_t& size, uint64_t& fingerprint) const = 0;

    /** Read up to @p size bytes at @p offset without exposing a cursor. */
    virtual int read(int64_t offset, char* buffer, int32_t size,
                     int32_t& read_size) = 0;
    virtual void close() = 0;
};

int validate_tsfile(RandomAccessFile& file,
                    unsigned char* file_version = nullptr);

}  // namespace storage

#endif  // FILE_RANDOM_ACCESS_FILE_H
