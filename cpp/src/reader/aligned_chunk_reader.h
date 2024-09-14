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

namespace storage {

class AlignedChunkReader : public IChunkReader {
   public:
    AlignedChunkReader()
        : read_file_(nullptr),
          time_chunk_meta_(nullptr),
          value_chunk_meta_(nullptr),
          measurement_name_(),
          time_chunk_header_(),
          value_chunk_header_(),
          cur_time_page_header_(),
          cur_value_page_header_(),
          time_in_stream_(),
          value_in_stream_(),
          file_data_time_buf_size_(0),
          file_data_value_buf_size_(0),
          time_chunk_visit_offset_(0),
          value_chunk_visit_offset_(0),
          time_compressor_(nullptr),
          value_compressor_(nullptr),
          time_filter_(nullptr),
          time_decoder_(nullptr),
          value_decoder_(nullptr),
          time_in_(),
          value_in_(),
          time_uncompressed_buf_(nullptr),
          value_uncompressed_buf_(nullptr),
          cur_value_index(-1) {}
    virtual int init(ReadFile *read_file, common::String m_name,
                     common::TSDataType data_type, Filter *time_filter);
    virtual void reset();
    virtual void destroy();

    virtual bool has_more_data() const {
        return prev_value_page_not_finish() ||
               (value_chunk_visit_offset_ -
                    value_chunk_header_.serialized_size_ <
                value_chunk_header_.data_size_);
    }
    virtual ChunkHeader &get_chunk_header() { return value_chunk_header_; }
    virtual int load_by_aligned_meta(ChunkMeta *time_meta,
                                     ChunkMeta *value_meta);

    virtual int get_next_page(common::TsBlock *tsblock,
                              Filter *oneshoot_filter);

   private:
    FORCE_INLINE bool chunk_has_only_one_page(
        const ChunkHeader &chunk_header) const {
        return chunk_header.chunk_type_ == ONLY_ONE_PAGE_CHUNK_HEADER_MARKER;
    }
    int alloc_compressor_and_decoder(storage::Decoder *&decoder,
                                     storage::Compressor *&compressor,
                                     common::TSEncoding encoding,
                                     common::TSDataType data_type,
                                     common::CompressionType compression_type);
    int get_cur_page_header(ChunkMeta *&chunk_meta,
                            common::ByteStream &in_stream_,
                            PageHeader &cur_page_header_,
                            uint32_t &chunk_visit_offset,
                            ChunkHeader &chunk_header);
    int read_from_file_and_rewrap(common::ByteStream &in_stream_,
                                  ChunkMeta *&chunk_meta,
                                  uint32_t &chunk_visit_offset,
                                  int32_t file_data_buf_size,
                                  int want_size = 0);
    bool cur_page_statisify_filter(Filter *filter);
    int skip_cur_page();
    int decode_cur_time_page_data();
    int decode_cur_value_page_data();
    int decode_time_value_buf_into_tsblock(common::TsBlock *&ret_tsblock,
                                           Filter *filter);
    int decode_tv_buf_into_tsblock(char *time_buf, char *value_buf,
                                   uint32_t time_buf_size,
                                   uint32_t value_buf_size,
                                   common::TsBlock *ret_tsblock,
                                   Filter *filter);
    bool prev_time_page_not_finish() const {
        return (time_decoder_ && time_decoder_->has_remaining()) ||
               time_in_.has_remaining();
    }

    bool prev_value_page_not_finish() const {
        return (value_decoder_ && value_decoder_->has_remaining()) ||
               value_in_.has_remaining();
    }

    int decode_tv_buf_into_tsblock_by_datatype(common::ByteStream &time_in,
                                               common::ByteStream &value_in,
                                               common::TsBlock *ret_tsblock,
                                               Filter *filter);
    int i32_DECODE_TYPED_TV_INTO_TSBLOCK(common::ByteStream &time_in,
                                         common::ByteStream &value_in,
                                         common::RowAppender &row_appender,
                                         Filter *filter);

   private:
    ReadFile *read_file_;
    ChunkMeta *time_chunk_meta_;
    ChunkMeta *value_chunk_meta_;
    common::String measurement_name_;
    ChunkHeader time_chunk_header_;
    // TODO: support reading more than one measurement in AlignedChunkReader.
    ChunkHeader value_chunk_header_;
    PageHeader cur_time_page_header_;
    PageHeader cur_value_page_header_;

    /*
     * Data reader from file is stored in @in_stream_, and the size
     * is stored in @file_data_buf_size_. Note, in_stream_.total_size_
     * is used to limit deserialization, that is why we still have
     * @file_data_buf_size_.
     *
     * Since we may want keep data of current page (and page header
     * of next page) in memory, we need a byte-size cursor to tell
     * us which byte we are processing, so we have @chunk_visit_offset_
     * it refer to position from the start of chunk_header_,
     * also refer to offset within the chunk (including chunk header).
     * It advanced by step of a page header or a page tv data.
     */
    common::ByteStream time_in_stream_;
    common::ByteStream value_in_stream_;
    int32_t file_data_time_buf_size_;
    int32_t file_data_value_buf_size_;
    uint32_t time_chunk_visit_offset_;
    uint32_t value_chunk_visit_offset_;

    // Statistic *page_statistic_;
    Compressor *time_compressor_;
    Compressor *value_compressor_;
    Filter *time_filter_;

    Decoder *time_decoder_;
    Decoder *value_decoder_;
    common::ByteStream time_in_;
    common::ByteStream value_in_;
    char *time_uncompressed_buf_;
    char *value_uncompressed_buf_;
    std::vector<uint8_t> value_page_col_notnull_bitmap_;
    uint32_t value_page_data_num_;
    int32_t cur_value_index;
};

}  // end namespace storage
#endif  // READER_CHUNK_READER_H
