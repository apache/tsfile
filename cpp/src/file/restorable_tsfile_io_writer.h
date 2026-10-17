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
 * (3) open_for_append() turns a file closed normally into (2)'s writable state
 * by removing its metadata tail, so writing can continue without recovering
 * from a crash.
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

    /**
     * Open a TsFile so that data can be appended to it.
     *
     * A normally-closed file is writable here: its metadata tail is removed in
     * place, the schema and chunk metadata it described are recovered from the
     * chunks themselves, and close() regenerates the tail from what was
     * appended. An incomplete file is recovered exactly as
     * open(file_path, true) recovers it, so a file that never got a footer
     * still appends instead of failing.
     *
     * Unlike open(), failing here leaves the file untouched. The caller's file
     * is known-good when it carries a footer, so a region this class cannot
     * read is a reason to refuse rather than a place to truncate — truncating
     * on a partial read would discard data that was still valid.
     *
     * On success can_write() is true and has_crashed() is false for a file that
     * had been closed normally. Supported versions are those open() supports.
     *
     * @param file_path Path to the TsFile
     * @return E_OK on success, error code otherwise.
     */
    int open_for_append(const std::string& file_path);

    void close();

    bool can_write() const { return can_write_; }
    bool has_crashed() const { return crashed_; }

    /**
     * True when open_for_append() trimmed the metadata tail of a file that had
     * been closed normally. False on the crash-recovery path, where the file
     * had no tail to remove.
     */
    bool is_append_on_complete() const { return append_on_complete_; }
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
    /**
     * kSalvage: the file's tail was never written, so every chunk the scan can
     * read is kept and the first offset it cannot read is where the file is
     * cut.
     * kAppend: the file carries a footer, so the region below it is known-good
     * and a chunk the scan cannot read means the file is not appendable here.
     */
    enum CheckMode { kSalvage, kAppend };

    /** Opens file_path_ for reading and writing without discarding content. */
    int open_write_file(const std::string& file_path);

    int self_check(bool truncate_corrupted, CheckMode mode);

    /**
     * Position the write handle at the offset the scan stopped at, hand the
     * file to the base writer and attach the recovered chunk group metadata.
     * Shared by both recovery paths. On success this is also where the object
     * becomes writable.
     *
     * @param truncate_to Byte offset to truncate the file to, or -1 to leave
     *        the file's length as it is.
     * @param salvage_happened Whether data was discarded because it could not
     *        be read, which is what has_crashed() reports.
     * @param recovered_cgm_list ChunkGroupMeta* allocated from
     *        self_check_arena_ by the scan.
     */
    int adopt_recovered_file(
        int64_t truncate_to, bool salvage_happened,
        const std::vector<ChunkGroupMeta*>& recovered_cgm_list);

   private:
    std::string file_path_;
    WriteFile* write_file_;
    bool write_file_owned_;

    int64_t truncated_size_;
    bool crashed_;
    bool can_write_;
    bool append_on_complete_;

    std::set<std::string> aligned_devices_;
    common::PageArena self_check_arena_;
    /** ChunkGroupMeta* allocated from self_check_arena_; reset device_id before
     * arena destroy to avoid leak. */
    std::vector<ChunkGroupMeta*> self_check_recovered_cgm_;
};

}  // namespace storage

#endif  // FILE_RESTORABLE_TSFILE_IO_WRITER_H
