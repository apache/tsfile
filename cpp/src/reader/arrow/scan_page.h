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
#ifndef READER_ARROW_SCAN_PAGE_H
#define READER_ARROW_SCAN_PAGE_H

#include <atomic>

#include "common/arrow_batch.h"
#include "common/tsfile_common.h"
#include "file/random_access_read_file.h"

namespace storage {
using ArrowInputBudget = std::shared_ptr<std::atomic<size_t>>;
// One immutable compressed file range, charged once even when many pages share
// it.
struct ArrowInputRange {
    static const size_t MAX_QUERY_BYTES = 160u * 1024 * 1024;
    static int read(RandomAccessReadFile& file, const ArrowInputBudget& usage,
                    int64_t offset, size_t length,
                    std::shared_ptr<ArrowInputRange>& input);
    ArrowInputBudget usage;
    int64_t offset;
    std::unique_ptr<char[]> bytes;
    size_t length;
    ArrowInputRange(const ArrowInputBudget& u, int64_t o, size_t n)
        : usage(u), offset(o), bytes(new char[n]), length(n) {}
    ~ArrowInputRange() { usage->fetch_sub(length); }
};

struct ArrowPageLocation {
    int64_t chunk_offset = 0;
    int64_t header_offset = 0;
    int64_t offset = 0;
    uint32_t compressed_size = 0;
    uint32_t uncompressed_size = 0;
    common::TSDataType type = common::INVALID_DATATYPE;
    common::TSEncoding encoding = common::INVALID_ENCODING;
    common::CompressionType compression = common::INVALID_COMPRESSION;
    std::shared_ptr<Statistic> statistic;
    std::shared_ptr<ArrowInputRange> input;
};
// Task-local read-ahead, capped at 64 KiB regardless of projection width or
// selected page count. The caller supplies the last byte range it needs; no
// buffer escapes into a page plan or a decoder retained by an output batch.
class ArrowReadWindow {
   public:
    int read(RandomAccessReadFile& file, int64_t offset, char* destination,
             int32_t size, int64_t end);

   private:
    std::vector<char> bytes_;
    int64_t offset_ = 0;
};
// Plans own their page statistics and borrow no parser buffers.
int plan_arrow_pages(RandomAccessReadFile& file, ChunkMeta& chunk,
                     Statistic* single_chunk_stat,
                     std::vector<ArrowPageLocation>& pages,
                     const ArrowInputBudget& usage = {},
                     const std::shared_ptr<ArrowInputRange>& device_input = {});
int decode_arrow_times(RandomAccessReadFile& file,
                       const ArrowPageLocation& page, bool aligned,
                       std::vector<int64_t>& times);
// Retains decompression and codec state when an output batch ends mid-page.
// A column task owns its cursor exclusively; no mutable decoder is shared.
class ArrowValueCursor {
   public:
    ArrowValueCursor(RandomAccessReadFile& file, ArrowPageLocation page,
                     bool aligned, uint32_t page_rows);
    ~ArrowValueCursor();
    int write(const std::vector<uint32_t>& rows, uint32_t begin, uint32_t count,
              common::ArrowColumn& output, uint32_t destination,
              ArrowReadWindow* window = nullptr, int64_t read_end = 0);

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
// Rows are ordered logical page positions. Non-NULL numeric runs can decode
// straight into the final Arrow buffer, including partial-page output slices.
int decode_arrow_values(RandomAccessReadFile& file,
                        const ArrowPageLocation& page, bool aligned,
                        uint32_t page_rows, const std::vector<uint32_t>& rows,
                        uint32_t begin, uint32_t count,
                        common::ArrowColumn& output, uint32_t destination);
}  // namespace storage
#endif
