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

#ifndef FILE_RESTORABLE_TSFILE_IO_WRITER_H
#define FILE_RESTORABLE_TSFILE_IO_WRITER_H

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/schema.h"
#include "common/tsfile_common.h"
#include "file/tsfile_io_writer.h"
#include "file/write_file.h"

namespace storage {

/**
 * TsFile check status constants for self-check result.
 * COMPLETE_FILE (0): File is complete, no recovery needed.
 * INCOMPATIBLE_FILE (-2): File is not in TsFile format.
 */
constexpr int64_t TSFILE_CHECK_COMPLETE = 0;
constexpr int64_t TSFILE_CHECK_INCOMPATIBLE = -2;

/**
 * RestorableTsFileIOWriter opens and optionally recovers a TsFile.
 * Inherits from TsFileIOWriter for continued writing after recovery.
 *
 * (1) If the TsFile is closed normally: has_crashed()=false, can_write()=false
 *
 * (2) If the TsFile is incomplete/crashed: has_crashed()=true,
 * can_write()=true, the writer truncates corrupted data and allows continued
 * writing.
 *
 * Uses standard C++11 and avoids memory leaks via RAII and smart pointers.
 */
class RestorableTsFileIOWriter : public TsFileIOWriter {
   public:
    RestorableTsFileIOWriter();
    ~RestorableTsFileIOWriter();

    // Non-copyable
    RestorableTsFileIOWriter(const RestorableTsFileIOWriter&) = delete;
    RestorableTsFileIOWriter& operator=(const RestorableTsFileIOWriter&) =
        delete;

    /**
     * Open a TsFile for recovery/append.
     * Uses O_RDWR|O_CREAT without O_TRUNC, so existing file content is
     * preserved.
     *
     * @param file_path Path to the TsFile
     * @param truncate_corrupted If true, truncate corrupted data. If false,
     *        do not truncate (incomplete file will remain as-is).
     * @return E_OK on success, error code otherwise.
     */
    int open(const std::string& file_path, bool truncate_corrupted = true);

    void close();

    bool can_write() const { return can_write_; }
    bool has_crashed() const { return crashed_; }
    int64_t get_truncated_size() const { return truncated_size_; }
    std::shared_ptr<Schema> get_known_schema() { return get_schema(); }

    /** True if the device was recovered as aligned (has time column). */
    bool is_device_aligned(const std::string& device) const;

    /**
     * Recovered chunk group metas from self_check (actual device_id and chunk
     * metas from file). TsFileWriter::init() uses this to rebuild schemas_
     * with the real device keys (aligned with Java). Valid until close().
     */
    const std::vector<ChunkGroupMeta*>& get_recovered_chunk_group_metas()
        const {
        return self_check_recovered_cgm_;
    }

    /**
     * Get the TsFileIOWriter for continued writing. Only valid when
     * can_write() is true. Returns this (since we inherit TsFileIOWriter).
     */
    TsFileIOWriter* get_tsfile_io_writer();

    /**
     * Get the WriteFile for TsFileWriter::init(). Only valid when can_write().
     * Caller must not destroy the returned pointer.
     */
    WriteFile* get_write_file();

    std::string get_file_path() const;

   private:
    int self_check(bool truncate_corrupted);

   private:
    std::string file_path_;
    WriteFile* write_file_;
    bool write_file_owned_;

    int64_t truncated_size_;
    bool crashed_;
    bool can_write_;

    std::set<std::string> aligned_devices_;
    common::PageArena self_check_arena_;
    /** ChunkGroupMeta* allocated from self_check_arena_; reset device_id before
     * arena destroy to avoid leak. */
    std::vector<ChunkGroupMeta*> self_check_recovered_cgm_;
};

}  // namespace storage

#endif  // FILE_RESTORABLE_TSFILE_IO_WRITER_H
