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

#ifndef READER_CHUNK_ALIGNED_READER_H
#define READER_CHUNK_ALIGNED_READER_H

#include "common/allocator/my_string.h"
#include "common/tsfile_common.h"
#include "compress/compressor.h"
#include "encoding/decoder.h"
#include "file/read_file.h"
#include "reader/filter/filter.h"
#include "reader/ichunk_reader.h"

#ifdef ENABLE_THREADS
namespace common {
class ThreadPool;
}
#endif

namespace storage {

// Page classification for chunk-level parallel decode.
enum class PagePassType { SKIP, FULL_PASS, BOUNDARY };

// Metadata collected per page during the chunk scan phase.
struct ChunkPageInfo {
    PagePassType pass_type = PagePassType::SKIP;
    int64_t time_file_offset = 0;
    uint32_t time_compressed_size = 0;
    uint32_t time_uncompressed_size = 0;
    std::vector<int64_t> value_file_offsets;
    std::vector<uint32_t> value_compressed_sizes;
    std::vector<uint32_t> value_uncompressed_sizes;
};

// Pre-decoded timestamps for one page (chunk-level decode).
struct PageTimesDecoded {
    std::vector<int64_t> times;
    int count = 0;
    int cursor = 0;
};

// Pre-decoded values for one (column, page) pair (chunk-level decode).
struct ColPageDecoded {
    std::vector<char> values;
    std::vector<uint8_t> bitmap;
    uint32_t data_num = 0;
    int nonnull_count = 0;
    int read_pos = 0;
    char* uncompressed_buf = nullptr;
};

// Per-value-column state for multi-value AlignedChunkReader.
struct ValueColumnState {
    ChunkMeta* chunk_meta = nullptr;
    ChunkHeader chunk_header;
    Decoder* decoder = nullptr;
    Compressor* compressor = nullptr;
    common::ByteStream in_stream;
    common::ByteStream in;
    char* uncompressed_buf = nullptr;
    int32_t file_data_buf_size = 0;
    uint32_t chunk_visit_offset = 0;
    PageHeader cur_page_header;
    std::vector<uint8_t> notnull_bitmap;
    int32_t cur_value_index = -1;
};

class AlignedChunkReader : public IChunkReader {
   public:
    AlignedChunkReader()
        : read_file_(nullptr),
          time_chunk_meta_(nullptr),
          measurement_name_(),
          time_chunk_header_(),
          cur_time_page_header_(),
          time_in_stream_(),
          file_data_time_buf_size_(0),
          time_chunk_visit_offset_(0),
          time_compressor_(nullptr),
          time_filter_(nullptr),
          time_decoder_(nullptr),
          time_in_(),
          time_uncompressed_buf_(nullptr) {}
    int init(ReadFile* read_file, common::String m_name,
             common::TSDataType data_type, Filter* time_filter) override;
    void reset() override;
    void destroy() override;
    ~AlignedChunkReader() override = default;

    bool has_more_data() const override { return has_more_data_multi(); }
    ChunkHeader& get_chunk_header() override {
        if (!value_columns_.empty()) {
            return value_columns_[0]->chunk_header;
        }
        return chunk_header_;
    }
    int load_by_aligned_meta(ChunkMeta* time_meta,
                             ChunkMeta* value_meta) override;

    // Multi-value: load one time chunk + N value chunks.
    int load_by_aligned_meta_multi(ChunkMeta* time_meta,
                                   const std::vector<ChunkMeta*>& value_metas);

    int get_next_page(common::TsBlock* tsblock, Filter* oneshoot_filter,
                      common::PageArena& pa) override;
    int get_next_page(common::TsBlock* tsblock, Filter* oneshoot_filter,
                      common::PageArena& pa, int64_t min_time_hint,
                      int& row_offset, int& row_limit) override;

    uint32_t get_value_column_count() const { return value_columns_.size(); }

    ChunkHeader& get_value_chunk_header(uint32_t col) {
        return value_columns_[col]->chunk_header;
    }

#ifdef ENABLE_THREADS
    void set_decode_pool(common::ThreadPool* pool) { decode_pool_ = pool; }
#endif

   private:
    FORCE_INLINE bool chunk_has_only_one_page(
        const ChunkHeader& chunk_header) const {
        return (chunk_header.chunk_type_ & ONLY_ONE_PAGE_CHUNK_HEADER_MARKER) ==
               ONLY_ONE_PAGE_CHUNK_HEADER_MARKER;
    }
    int alloc_compressor_and_decoder(storage::Decoder*& decoder,
                                     storage::Compressor*& compressor,
                                     common::TSEncoding encoding,
                                     common::TSDataType data_type,
                                     common::CompressionType compression_type);
    int get_cur_page_header(ChunkMeta*& chunk_meta,
                            common::ByteStream& in_stream_,
                            PageHeader& cur_page_header_,
                            uint32_t& chunk_visit_offset,
                            ChunkHeader& chunk_header,
                            int32_t* override_buf_size = nullptr);
    int read_from_file_and_rewrap(common::ByteStream& in_stream_,
                                  ChunkMeta*& chunk_meta,
                                  uint32_t& chunk_visit_offset,
                                  int32_t& file_data_buf_size,
                                  int want_size = 0, bool may_shrink = true);
    int decode_cur_time_page_data();
    bool prev_time_page_not_finish() const {
        if (time_predecoded_) return page_time_cursor_ < page_time_count_;
        return (time_decoder_ && time_decoder_->has_remaining(time_in_)) ||
               time_in_.has_remaining();
    }

    // ── Multi-value private methods ──────────────────────────────────────
    bool has_more_data_multi() const;
    bool prev_any_value_page_not_finish_multi() const;
    int get_next_page_multi(common::TsBlock* ret_tsblock,
                            Filter* oneshoot_filter, common::PageArena& pa,
                            int64_t min_time_hint, int* row_offset,
                            int* row_limit);
    int get_next_page_multi_serial(common::TsBlock* ret_tsblock, Filter* filter,
                                   common::PageArena& pa, int64_t min_time_hint,
                                   int* row_offset);
    int skip_cur_page_multi();
    bool cur_page_statisify_filter_multi(Filter* filter);
    int decode_cur_value_pages_multi();
    int ensure_value_page_loaded(ValueColumnState& col);
    static int decompress_and_parse_value_page(ValueColumnState& col);
    int decode_time_value_buf_into_tsblock_multi(common::TsBlock*& ret_tsblock,
                                                 Filter* filter,
                                                 common::PageArena* pa,
                                                 int* row_offset,
                                                 int* row_limit);
    int multi_decode_tv_row_by_row(common::TsBlock* ret_tsblock,
                                   common::RowAppender& row_appender,
                                   Filter* filter, common::PageArena* pa,
                                   int* row_offset, int* row_limit);

    // ── Chunk-level parallel decode methods ─────────────────────────────
    int scan_chunk_pages(Filter* filter);
    int decode_chunk_pages();
    int scatter_chunk_pages(common::TsBlock* tsblock,
                            common::RowAppender& row_appender, Filter* filter,
                            common::PageArena* pa, int* row_offset,
                            int* row_limit);
    void cleanup_chunk_decode();

   private:
    ReadFile* read_file_;
    ChunkMeta* time_chunk_meta_;
    common::String measurement_name_;
    ChunkHeader time_chunk_header_;
    PageHeader cur_time_page_header_;

    common::ByteStream time_in_stream_;
    int32_t file_data_time_buf_size_;
    uint32_t time_chunk_visit_offset_;

    Compressor* time_compressor_;
    Filter* time_filter_;

    Decoder* time_decoder_;
    common::ByteStream time_in_;
    char* time_uncompressed_buf_;

    std::vector<ValueColumnState*> value_columns_;

    // Pre-decoded timestamps for page-level parallel decode.
    std::vector<int64_t> page_all_times_;
    int page_time_count_ = 0;
    int page_time_cursor_ = 0;
    bool time_predecoded_ = false;

    // ── Chunk-level parallel decode state ────────────────────────────────
    std::vector<ChunkPageInfo> chunk_pages_;
    std::vector<PageTimesDecoded> chunk_times_;
    std::vector<std::vector<ColPageDecoded>> chunk_cols_;
    int chunk_page_cursor_ = 0;
    bool chunk_level_active_ = false;

#ifdef ENABLE_THREADS
    common::ThreadPool* decode_pool_ = nullptr;  // borrowed, not owned
#endif
};

}  // end namespace storage
#endif  // READER_CHUNK_ALIGNED_READER_H
