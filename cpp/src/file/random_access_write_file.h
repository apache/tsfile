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

#ifndef FILE_RANDOM_ACCESS_WRITE_FILE_H
#define FILE_RANDOM_ACCESS_WRITE_FILE_H

#include <stdint.h>

#include <string>

namespace storage {

/**
 * A writable byte sink used by the TsFile writer stack.
 *
 * The writer only ever appends at the current write cursor, truncates a
 * partially written file during recovery, and seeks to the end to resume
 * an existing file, so a backend only has to support those operations.
 * All methods return common error codes (see utils/errno_define.h) rather
 * than throwing; buffers are owned by the caller.
 *
 * Contract:
 *  - After close(), every mutating call (write/truncate/seek_to_end/sync)
 *    returns common::E_FILE_WRITE_ERR and is a no-op.  close() is
 *    idempotent and returns an error code for backends whose close can fail.
 *  - write() appends @p len bytes at the current position and advances the
 *    position by @p len on success.
 *  - truncate() shrinks or extends the byte sequence to @p size bytes,
 *    zero-filling when extending.  Like POSIX ftruncate() it does not move
 *    the write cursor, so a later write may land past the current end and
 *    leave a zero-filled hole.
 *  - seek_to_end() places the cursor at the current end of the byte
 *    sequence; get_position() reports the cursor.
 *  - file_path() is a human-readable identifier.  Backends that have no real
 *    path return the name they were constructed with (possibly empty).
 *
 * A TsFileReader cannot read from this interface; a backend that must be
 * read back (e.g. to verify a written file) exposes the bytes through a
 * matching RandomAccessReadFile implementation instead.
 *
 * Implementations are handed to the writers already open.  Reopening an
 * existing file for append/recovery is a local-file capability
 * (RestorableTsFileIOWriter reads the old bytes back by path), so a
 * non-local sink cannot be recovered in place.
 */
class RandomAccessWriteFile {
   public:
    virtual ~RandomAccessWriteFile() = default;

    virtual bool is_opened() const = 0;
    virtual const std::string& file_path() const = 0;

    virtual int write(const char* buf, uint32_t len) = 0;
    virtual int64_t get_position() = 0;
    virtual int sync() = 0;
    virtual int truncate(int64_t size) = 0;
    virtual int seek_to_end() = 0;
    virtual int close() = 0;
};

}  // namespace storage

#endif  // FILE_RANDOM_ACCESS_WRITE_FILE_H
