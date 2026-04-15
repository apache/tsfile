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

#include "aligned_chunk_reader.h"

#include <algorithm>
#include <limits>

#include "common/global.h"
#ifdef ENABLE_THREADS
#include "common/thread_pool.h"
#endif
#include "compress/compressor_factory.h"
#include "encoding/decoder_factory.h"

using namespace common;
namespace storage {

int AlignedChunkReader::init(ReadFile* read_file, String m_name,
                             TSDataType data_type, Filter* time_filter) {
    read_file_ = read_file;
    measurement_name_.shallow_copy_from(m_name);
    time_decoder_ = DecoderFactory::alloc_time_decoder();
    value_decoder_ = nullptr;
    time_compressor_ = nullptr;
    value_compressor_ = nullptr;
    time_filter_ = time_filter;
    time_uncompressed_buf_ = nullptr;
    value_uncompressed_buf_ = nullptr;
    if (IS_NULL(time_decoder_)) {
        return E_OOM;
    }
    return E_OK;
}

void AlignedChunkReader::reset() {
    time_chunk_meta_ = nullptr;
    value_chunk_meta_ = nullptr;
    time_chunk_header_.reset();
    value_chunk_header_.reset();
    cur_time_page_header_.reset();
    cur_value_page_header_.reset();

    char* file_data_buf = time_in_stream_.get_wrapped_buf();
    if (file_data_buf != nullptr) {
        mem_free(file_data_buf);
    }
    time_in_stream_.clear_wrapped_buf();
    time_in_stream_.reset();
    file_data_buf = value_in_stream_.get_wrapped_buf();
    if (file_data_buf != nullptr) {
        mem_free(file_data_buf);
    }
    value_in_stream_.clear_wrapped_buf();
    value_in_stream_.reset();
    file_data_time_buf_size_ = 0;
    file_data_value_buf_size_ = 0;
    time_chunk_visit_offset_ = 0;
    value_chunk_visit_offset_ = 0;
    page_plan_built_ = false;
    current_page_loaded_ = false;
    current_page_plan_index_ = 0;
    time_predecoded_ = false;
    page_all_times_.clear();
    page_time_count_ = 0;
    page_time_cursor_ = 0;

    // Free leftover uncompressed buffers from the previous chunk.
    if (time_uncompressed_buf_ != nullptr && time_compressor_ != nullptr) {
        time_compressor_->after_uncompress(time_uncompressed_buf_);
        time_uncompressed_buf_ = nullptr;
    }

    // Multi-value reset
    for (auto* col : value_columns_) {
        // Free uncompressed buffer before resetting.
        if (col->uncompressed_buf != nullptr && col->compressor != nullptr) {
            col->compressor->after_uncompress(col->uncompressed_buf);
            col->uncompressed_buf = nullptr;
        }
        char* buf = col->in_stream.get_wrapped_buf();
        if (buf != nullptr) mem_free(buf);
        col->in_stream.clear_wrapped_buf();
        col->in_stream.reset();
        col->in.reset();
        col->chunk_header.reset();
        col->cur_page_header.reset();
        col->file_data_buf_size = 0;
        col->chunk_visit_offset = 0;
        col->notnull_bitmap.clear();
        col->cur_value_index = -1;
        col->chunk_meta = nullptr;
        col->predecoded_values.clear();
        col->predecoded_strings.clear();
        col->predecoded_count = 0;
        col->predecoded_read_pos = 0;
        col->predecoded = false;
        col->predecode_pa.destroy();
        // Note: decoder/compressor are NOT freed here — they are reused by
        // alloc_compressor_and_decoder() in load_by_aligned_meta_multi().
    }
    release_current_page_state();
    cleanup_chunk_decode();
}

void AlignedChunkReader::destroy() {
    cleanup_chunk_decode();
    if (time_uncompressed_buf_ != nullptr && time_compressor_ != nullptr) {
        time_compressor_->after_uncompress(time_uncompressed_buf_);
        time_uncompressed_buf_ = nullptr;
    }
    if (value_uncompressed_buf_ != nullptr && value_compressor_ != nullptr) {
        value_compressor_->after_uncompress(value_uncompressed_buf_);
        value_uncompressed_buf_ = nullptr;
    }
    value_page_col_notnull_bitmap_.clear();
    value_page_col_notnull_bitmap_.shrink_to_fit();
    if (time_decoder_ != nullptr) {
        time_decoder_->~Decoder();
        DecoderFactory::free(time_decoder_);
        time_decoder_ = nullptr;
    }
    if (value_decoder_ != nullptr) {
        value_decoder_->~Decoder();
        DecoderFactory::free(value_decoder_);
        value_decoder_ = nullptr;
    }
    if (time_compressor_ != nullptr) {
        time_compressor_->~Compressor();
        CompressorFactory::free(time_compressor_);
        time_compressor_ = nullptr;
    }
    if (value_compressor_ != nullptr) {
        value_compressor_->~Compressor();
        CompressorFactory::free(value_compressor_);
        value_compressor_ = nullptr;
    }
    char* buf = time_in_stream_.get_wrapped_buf();
    if (buf != nullptr) {
        mem_free(buf);
        time_in_stream_.clear_wrapped_buf();
    }
    cur_time_page_header_.reset();
    buf = value_in_stream_.get_wrapped_buf();
    if (buf != nullptr) {
        mem_free(buf);
        value_in_stream_.clear_wrapped_buf();
    }
    cur_value_page_header_.reset();
    chunk_header_.~ChunkHeader();

    // Multi-value destroy
    for (size_t ci = 0; ci < value_columns_.size(); ci++) {
        auto* col = value_columns_[ci];
        if (col->decoder != nullptr) {
            col->decoder->~Decoder();
            DecoderFactory::free(col->decoder);
            col->decoder = nullptr;
        }
        if (col->compressor != nullptr) {
            col->compressor->~Compressor();
            CompressorFactory::free(col->compressor);
            col->compressor = nullptr;
        }
        col->predecode_pa.destroy();
        buf = col->in_stream.get_wrapped_buf();
        if (buf != nullptr) {
            mem_free(buf);
            col->in_stream.clear_wrapped_buf();
        }
        col->cur_page_header.reset();
        delete col;
    }
    value_columns_.clear();
    release_current_page_state();
#ifdef ENABLE_THREADS
    decode_pool_ = nullptr;  // borrowed, not owned
#endif
}

int AlignedChunkReader::load_by_aligned_meta(ChunkMeta* time_chunk_meta,
                                             ChunkMeta* value_chunk_meta) {
    int ret = E_OK;
    time_chunk_meta_ = time_chunk_meta;
    value_chunk_meta_ = value_chunk_meta;
#if DEBUG_SE
    std::cout << "AlignedChunkReader::load_by_meta, meta=" << *time_chunk_meta
              << ", " << *value_chunk_meta << std::endl;
#endif
    /* ================ deserialize time_chunk_header ================*/
    // TODO configurable
    file_data_time_buf_size_ = 1024;
    file_data_value_buf_size_ = 1024;
    int32_t ret_read_len = 0;
    char* time_file_data_buf =
        (char*)mem_alloc(file_data_time_buf_size_, MOD_CHUNK_READER);
    if (IS_NULL(time_file_data_buf)) {
        return E_OOM;
    }
    ret = read_file_->read(time_chunk_meta_->offset_of_chunk_header_,
                           time_file_data_buf, file_data_time_buf_size_,
                           ret_read_len);
    if (IS_SUCC(ret) && ret_read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
        ret = E_TSFILE_CORRUPTED;
        LOGE("file corrupted, ret=" << ret << ", offset="
                                    << time_chunk_meta_->offset_of_chunk_header_
                                    << "read_len=" << ret_read_len);
        mem_free(time_file_data_buf);
    }
    if (IS_SUCC(ret)) {
        time_in_stream_.wrap_from(time_file_data_buf, ret_read_len);
        if (RET_FAIL(time_chunk_header_.deserialize_from(time_in_stream_))) {
        } else {
            time_chunk_visit_offset_ = time_in_stream_.read_pos();
        }
    }
    /* ================ deserialize value_chunk_header ================*/
    ret_read_len = 0;
    char* value_file_data_buf =
        (char*)mem_alloc(file_data_value_buf_size_, MOD_CHUNK_READER);
    if (IS_NULL(value_file_data_buf)) {
        return E_OOM;
    }
    ret = read_file_->read(value_chunk_meta_->offset_of_chunk_header_,
                           value_file_data_buf, file_data_value_buf_size_,
                           ret_read_len);
    if (IS_SUCC(ret) && ret_read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
        ret = E_TSFILE_CORRUPTED;
        LOGE("file corrupted, ret="
             << ret << ", offset=" << value_chunk_meta_->offset_of_chunk_header_
             << "read_len=" << ret_read_len);
        mem_free(value_file_data_buf);
    }
    if (IS_SUCC(ret)) {
        value_in_stream_.wrap_from(value_file_data_buf, ret_read_len);
        if (RET_FAIL(value_chunk_header_.deserialize_from(value_in_stream_))) {
        } else if (RET_FAIL(alloc_compressor_and_decoder(
                       time_decoder_, time_compressor_,
                       time_chunk_header_.encoding_type_,
                       time_chunk_header_.data_type_,
                       time_chunk_header_.compression_type_))) {
        } else if (RET_FAIL(alloc_compressor_and_decoder(
                       value_decoder_, value_compressor_,
                       value_chunk_header_.encoding_type_,
                       value_chunk_header_.data_type_,
                       value_chunk_header_.compression_type_))) {
        } else {
            value_chunk_visit_offset_ = value_in_stream_.read_pos();
#if DEBUG_SE
            std::cout << "AlignedChunkReader::load_by_meta, time_chunk_header="
                      << time_chunk_header_
                      << ", value_chunk_header=" << value_chunk_header_
                      << std::endl;
#endif
        }
    }
    return ret;
}

int AlignedChunkReader::alloc_compressor_and_decoder(
    storage::Decoder*& decoder, storage::Compressor*& compressor,
    TSEncoding encoding, TSDataType data_type, CompressionType compression) {
    if (decoder != nullptr) {
        decoder->reset();
    } else {
        decoder = DecoderFactory::alloc_value_decoder(encoding, data_type);
        if (IS_NULL(decoder)) {
            return E_OOM;
        }
    }

    if (compressor != nullptr) {
        compressor->reset(false);
    } else {
        compressor = CompressorFactory::alloc_compressor(compression);
        if (compressor == nullptr) {
            return E_OOM;
        }
    }
    return E_OK;
}

int AlignedChunkReader::get_next_page(TsBlock* ret_tsblock,
                                      Filter* oneshoot_filter, PageArena& pa) {
    if (multi_value_mode_) {
        return get_next_page_multi(ret_tsblock, oneshoot_filter, pa);
    }
    int ret = E_OK;
    Filter* filter =
        (oneshoot_filter != nullptr ? oneshoot_filter : time_filter_);
    bool pt = prev_time_page_not_finish();
    bool pv = prev_value_page_not_finish();
    if (pt && pv) {
        ret = decode_time_value_buf_into_tsblock(ret_tsblock, filter, &pa);
        return ret;
    }
    if (!pt && !pv) {
        while (IS_SUCC(ret)) {
            if (RET_FAIL(get_cur_page_header(
                    time_chunk_meta_, time_in_stream_, cur_time_page_header_,
                    time_chunk_visit_offset_, time_chunk_header_))) {
            } else if (RET_FAIL(get_cur_page_header(
                           value_chunk_meta_, value_in_stream_,
                           cur_value_page_header_, value_chunk_visit_offset_,
                           value_chunk_header_))) {
            } else if (cur_page_statisify_filter(filter)) {
                break;
            } else if (RET_FAIL(skip_cur_page())) {
            }
            if (!has_more_data()) {
                ret = E_NO_MORE_DATA;
                break;
            }
        }
        if (IS_SUCC(ret)) {
            ret = decode_cur_time_page_data() || decode_cur_value_page_data();
        }
    }
    if (IS_SUCC(ret)) {
        ret = decode_time_value_buf_into_tsblock(ret_tsblock, filter, &pa);
    }
    return ret;
}

int AlignedChunkReader::get_cur_page_header(ChunkMeta*& chunk_meta,
                                            common::ByteStream& in_stream,
                                            PageHeader& cur_page_header,
                                            uint32_t& chunk_visit_offset,
                                            ChunkHeader& chunk_header,
                                            int32_t* override_buf_size) {
    int ret = E_OK;
    bool retry = true;
    int cur_page_header_serialized_size = 0;
    // TODO： configurable
    int retry_read_want_size = 1024;
    if (chunk_visit_offset - chunk_header.serialized_size_ >=
        chunk_header.data_size_) {
        cur_page_header.reset();
        return E_OK;
    }

    do {
        in_stream.mark_read_pos();
        cur_page_header.reset();
        ret = cur_page_header.deserialize_from(
            in_stream, !chunk_has_only_one_page(chunk_header),
            chunk_header.data_type_);
        cur_page_header_serialized_size = in_stream.get_mark_len();
        if (deserialize_buf_not_enough(ret) && retry) {
            retry = false;
            retry_read_want_size += 1024;
            int32_t& file_data_buf_size =
                override_buf_size != nullptr ? *override_buf_size
                : chunk_header.data_type_ == common::VECTOR
                    ? file_data_time_buf_size_
                    : file_data_value_buf_size_;
            // do not shrink buffer for page header, otherwise, the buffer is
            // most likely to grow back when reading page data
            if (E_OK == read_from_file_and_rewrap(
                            in_stream, chunk_meta, chunk_visit_offset,
                            file_data_buf_size, retry_read_want_size, false)) {
                continue;
            }
        }
        break;
    } while (true);
    if (IS_SUCC(ret)) {
        // visit a header
        chunk_visit_offset += cur_page_header_serialized_size;
    }
#if DEBUG_SE
    std::cout << "get_cur_page_header, ret=" << ret << ", retry=" << retry
              << ", cur_page_header=" << cur_page_header
              << ", chunk_meta->offset_of_chunk_header_="
              << chunk_meta->offset_of_chunk_header_
              << ", cur_page_header_serialized_size="
              << cur_page_header_serialized_size << std::endl;
#endif
    return ret;
}

// reader at least @want_size bytes from file and wrap the buffer into
// @in_stream_
int AlignedChunkReader::read_from_file_and_rewrap(
    common::ByteStream& in_stream_, ChunkMeta*& chunk_meta,
    uint32_t& chunk_visit_offset, int32_t& file_data_buf_size, int want_size,
    bool may_shrink) {
    int ret = E_OK;
    const int DEFAULT_READ_SIZE = 4096;  // may use page_size + page_header_size
    char* file_data_buf = in_stream_.get_wrapped_buf();
    int64_t offset = chunk_meta->offset_of_chunk_header_ + chunk_visit_offset;
    int read_size =
        (want_size < DEFAULT_READ_SIZE ? DEFAULT_READ_SIZE : want_size);
    if (file_data_buf_size < read_size ||
        (may_shrink && read_size < file_data_buf_size / 10)) {
        file_data_buf = (char*)mem_realloc(file_data_buf, read_size);
        if (IS_NULL(file_data_buf)) {
            in_stream_.clear_wrapped_buf();
            return E_OOM;
        }
        file_data_buf_size = read_size;
        // Update stream pointer immediately so it stays valid even if
        // the subsequent read fails and the caller frees via destroy().
        in_stream_.wrap_from(file_data_buf, read_size);
    }
    int ret_read_len = 0;
    if (RET_FAIL(
            read_file_->read(offset, file_data_buf, read_size, ret_read_len))) {
    } else {
        in_stream_.wrap_from(file_data_buf, ret_read_len);
#ifdef DEBUG_SE
        std::cout << "file offset = " << offset << " len = " << ret_read_len
                  << std::endl;
        DEBUG_hex_dump_buf("wrapped buf = ", file_data_buf, 256);
#endif
    }
    return ret;
}

bool AlignedChunkReader::cur_page_statisify_filter(Filter* filter) {
    bool value_satisfy = filter == nullptr ||
                         cur_value_page_header_.statistic_ == nullptr ||
                         filter->satisfy(cur_value_page_header_.statistic_);
    bool time_satisfy = filter == nullptr ||
                        cur_time_page_header_.statistic_ == nullptr ||
                        filter->satisfy(cur_time_page_header_.statistic_);
    return time_satisfy && value_satisfy;
}

int AlignedChunkReader::skip_cur_page() {
    int ret = E_OK;
    // visit a page tv data
    time_chunk_visit_offset_ += cur_time_page_header_.compressed_size_;
    time_in_stream_.wrapped_buf_advance_read_pos(
        cur_time_page_header_.compressed_size_);
    value_chunk_visit_offset_ += cur_value_page_header_.compressed_size_;
    value_in_stream_.wrapped_buf_advance_read_pos(
        cur_value_page_header_.compressed_size_);
    return ret;
}

int AlignedChunkReader::decode_cur_time_page_data() {
    int ret = E_OK;

    // Step 1: make sure we load the whole page data in @in_stream_
    if (time_in_stream_.remaining_size() <
        cur_time_page_header_.compressed_size_) {
        // std::cout << "decode_cur_page_data. in_stream_.remaining_size="<<
        // in_stream_.remaining_size() << ", cur_page_header_.compressed_size_="
        // << cur_page_header_.compressed_size_ << std::endl;
        if (RET_FAIL(read_from_file_and_rewrap(
                time_in_stream_, time_chunk_meta_, time_chunk_visit_offset_,
                file_data_time_buf_size_,
                cur_time_page_header_.compressed_size_))) {
        }
    }

    char* time_compressed_buf = nullptr;
    char* time_uncompressed_buf = nullptr;
    uint32_t time_compressed_buf_size = 0;
    uint32_t time_uncompressed_buf_size = 0;

    // Step 2: do uncompress
    if (IS_SUCC(ret)) {
        time_compressed_buf =
            time_in_stream_.get_wrapped_buf() + time_in_stream_.read_pos();
#ifdef DEBUG_SE
        std::cout << "AlignedChunkReader::decode_cur_page_data,time_in_stream_."
                     "get_wrapped_buf="
                  << (void*)(time_in_stream_.get_wrapped_buf())
                  << ", time_in_stream_.read_pos=" << time_in_stream_.read_pos()
                  << std::endl;
#endif
        time_compressed_buf_size = cur_time_page_header_.compressed_size_;
        time_in_stream_.wrapped_buf_advance_read_pos(time_compressed_buf_size);
        time_chunk_visit_offset_ += time_compressed_buf_size;
        if (RET_FAIL(time_compressor_->reset(false))) {
        } else if (RET_FAIL(time_compressor_->uncompress(
                       time_compressed_buf, time_compressed_buf_size,
                       time_uncompressed_buf, time_uncompressed_buf_size))) {
        } else {
            time_uncompressed_buf_ = time_uncompressed_buf;
        }
#ifdef DEBUG_SE
        DEBUG_hex_dump_buf(
            "AlignedChunkReader reader, time_uncompressed buf = ",
            time_uncompressed_buf, time_uncompressed_buf_size);
#endif
        if (ret != E_OK || time_uncompressed_buf_size !=
                               cur_time_page_header_.uncompressed_size_) {
            ret = E_TSFILE_CORRUPTED;
            ASSERT(false);
        }
    }

    time_decoder_->reset();
#ifdef DEBUG_SE
    DEBUG_hex_dump_buf("AlignedChunkReader reader, time_buf = ", time_buf,
                       time_buf_size);
#endif
    time_in_.wrap_from(time_uncompressed_buf_, time_uncompressed_buf_size);
    return ret;
}

int AlignedChunkReader::decode_cur_value_page_data() {
    int ret = E_OK;

    // Step 1: make sure we load the whole page data in @in_stream_
    if (value_in_stream_.remaining_size() <
        cur_value_page_header_.compressed_size_) {
        // std::cout << "decode_cur_page_data. in_stream_.remaining_size="<<
        // in_stream_.remaining_size() << ", cur_page_header_.compressed_size_="
        // << cur_page_header_.compressed_size_ << std::endl;
        if (RET_FAIL(read_from_file_and_rewrap(
                value_in_stream_, value_chunk_meta_, value_chunk_visit_offset_,
                file_data_value_buf_size_,
                cur_value_page_header_.compressed_size_))) {
        }
    }

    char* value_compressed_buf = nullptr;
    char* value_uncompressed_buf = nullptr;
    uint32_t value_compressed_buf_size = 0;
    uint32_t value_uncompressed_buf_size = 0;
    char* value_buf = nullptr;
    uint32_t value_buf_size = 0;

    if (cur_value_page_header_.compressed_size_ == 0) {
        value_in_.wrap_from(value_buf, 0);
        return E_OK;
    }

    // Step 2: do uncompress
    if (IS_SUCC(ret)) {
        value_compressed_buf =
            value_in_stream_.get_wrapped_buf() + value_in_stream_.read_pos();
        value_compressed_buf_size = cur_value_page_header_.compressed_size_;
        value_in_stream_.wrapped_buf_advance_read_pos(
            value_compressed_buf_size);
        value_chunk_visit_offset_ += value_compressed_buf_size;
        if (RET_FAIL(value_compressor_->reset(false))) {
        } else if (RET_FAIL(value_compressor_->uncompress(
                       value_compressed_buf, value_compressed_buf_size,
                       value_uncompressed_buf, value_uncompressed_buf_size))) {
        } else {
            value_uncompressed_buf_ = value_uncompressed_buf;
        }
#ifdef DEBUG_SE
        DEBUG_hex_dump_buf(
            "AlignedChunkReader reader, value_uncompressed buf = ",
            value_uncompressed_buf, value_uncompressed_buf_size);
#endif
        if (ret != E_OK || value_uncompressed_buf_size !=
                               cur_value_page_header_.uncompressed_size_) {
            ret = E_TSFILE_CORRUPTED;
            ASSERT(false);
        }
    }
    // Step 3: get value_buf
    if (IS_SUCC(ret)) {
        uint32_t value_uncompressed_buf_offset = 0;
        value_page_data_num_ =
            SerializationUtil::read_ui32(value_uncompressed_buf);
        value_uncompressed_buf_offset += sizeof(uint32_t);
        value_page_col_notnull_bitmap_.resize((value_page_data_num_ + 7) / 8);
        for (unsigned char& i : value_page_col_notnull_bitmap_) {
            i = *(value_uncompressed_buf + value_uncompressed_buf_offset);
            value_uncompressed_buf_offset++;
        }
        cur_value_index = -1;
        value_buf = value_uncompressed_buf + value_uncompressed_buf_offset;
        value_buf_size =
            value_uncompressed_buf_size - value_uncompressed_buf_offset;
    }
    value_decoder_->reset();
#ifdef DEBUG_SE
    DEBUG_hex_dump_buf("AlignedChunkReader reader, value_buf = ", value_buf,
                       value_buf_size);
#endif
    value_in_.wrap_from(value_buf, value_buf_size);
    return ret;
}

int AlignedChunkReader::decode_time_value_buf_into_tsblock(
    TsBlock*& ret_tsblock, Filter* filter, common::PageArena* pa) {
    int ret = common::E_OK;
    ret = decode_tv_buf_into_tsblock_by_datatype(time_in_, value_in_,
                                                 ret_tsblock, filter, pa);
    // if we return during @decode_tv_buf_into_tsblock, we should keep
    // @uncompressed_buf_ valid until all TV pairs are decoded.
    if (ret != E_OVERFLOW) {
        if (time_uncompressed_buf_ != nullptr) {
            time_compressor_->after_uncompress(time_uncompressed_buf_);
            time_uncompressed_buf_ = nullptr;
        }
        if (value_uncompressed_buf_ != nullptr) {
            value_compressor_->after_uncompress(value_uncompressed_buf_);
            value_uncompressed_buf_ = nullptr;
        }
        if (!prev_value_page_not_finish()) {
            value_in_.reset();
        }
        if (!prev_time_page_not_finish()) {
            time_in_.reset();
        }
        value_page_col_notnull_bitmap_.clear();
        value_page_col_notnull_bitmap_.shrink_to_fit();
    } else {
        ret = E_OK;
    }
    return ret;
}

#define DECODE_TYPED_TV_INTO_TSBLOCK(CppType, ReadType, time_in, value_in,     \
                                     row_appender)                             \
    do {                                                                       \
        uint32_t mask = 1 << 7;                                                \
        int64_t time = 0;                                                      \
        CppType value;                                                         \
        while (time_decoder_->has_remaining(time_in)) {                        \
            cur_value_index++;                                                 \
            if (value_page_col_notnull_bitmap_.empty() ||                      \
                ((value_page_col_notnull_bitmap_[cur_value_index / 8] &        \
                  0xFF) &                                                      \
                 (mask >> (cur_value_index % 8))) == 0) {                      \
                ret = time_decoder_->read_int64(time, time_in);                \
                if (ret != E_OK) {                                             \
                    break;                                                     \
                }                                                              \
                if (UNLIKELY(!row_appender.add_row())) {                       \
                    ret = E_OVERFLOW;                                          \
                    break;                                                     \
                }                                                              \
                row_appender.append(0, (char*)&time, sizeof(time));            \
                row_appender.append_null(1);                                   \
                continue;                                                      \
            }                                                                  \
            assert(value_decoder_->has_remaining(value_in));                   \
            if (!value_decoder_->has_remaining(value_in)) {                    \
                return common::E_DATA_INCONSISTENCY;                           \
            }                                                                  \
            if (UNLIKELY(!row_appender.add_row())) {                           \
                ret = E_OVERFLOW;                                              \
                cur_value_index--;                                             \
                break;                                                         \
            } else if (RET_FAIL(time_decoder_->read_int64(time, time_in))) {   \
            } else if (RET_FAIL(value_decoder_->read_##ReadType(value,         \
                                                                value_in))) {  \
            } else if (filter != nullptr && !filter->satisfy(time, value)) {   \
                row_appender.backoff_add_row();                                \
                continue;                                                      \
            } else {                                                           \
                /*std::cout << "decoder: time=" << time << ", value=" << value \
                 * << std::endl;*/                                             \
                row_appender.append(0, (char*)&time, sizeof(time));            \
                row_appender.append(1, (char*)&value, sizeof(value));          \
            }                                                                  \
        }                                                                      \
    } while (false)

int AlignedChunkReader::i32_DECODE_TYPED_TV_INTO_TSBLOCK(
    ByteStream& time_in, ByteStream& value_in, RowAppender& row_appender,
    Filter* filter) {
    int ret = E_OK;
    uint32_t mask = 1 << 7;
    int64_t time = 0;
    int32_t value;
    while (time_decoder_->has_remaining(time_in)) {
        cur_value_index++;
        if (value_page_col_notnull_bitmap_.empty() ||
            ((value_page_col_notnull_bitmap_[cur_value_index / 8] & 0xFF) &
             (mask >> (cur_value_index % 8))) == 0) {
            ret = time_decoder_->read_int64(time, time_in);
            if (ret != E_OK) {
                break;
            }
            if (UNLIKELY(!row_appender.add_row())) {
                ret = E_OVERFLOW;
                break;
            }
            row_appender.append(0, (char*)&time, sizeof(time));
            row_appender.append_null(1);
            continue;
        }
        assert(value_decoder_->has_remaining(value_in));
        if (!value_decoder_->has_remaining(value_in)) {
            return common::E_DATA_INCONSISTENCY;
        }
        if (UNLIKELY(!row_appender.add_row())) {
            ret = E_OVERFLOW;
            cur_value_index--;
            break;
        } else if (RET_FAIL(time_decoder_->read_int64(time, time_in))) {
        } else if (RET_FAIL(value_decoder_->read_int32(value, value_in))) {
        } else if (filter != nullptr && !filter->satisfy(time, value)) {
            row_appender.backoff_add_row();
            continue;
        } else {
            /*std::cout << "decoder: time=" << time << ", value=" << value
             * << std::endl;*/
            row_appender.append(0, (char*)&time, sizeof(time));
            row_appender.append(1, (char*)&value, sizeof(value));
        }
    }
    return ret;
}

int AlignedChunkReader::i32_DECODE_TV_BATCH(ByteStream& time_in,
                                            ByteStream& value_in,
                                            RowAppender& row_appender,
                                            Filter* filter) {
    int ret = E_OK;
    const int BATCH = 129;
    int64_t times[BATCH];
    int32_t values[BATCH];
    const uint32_t null_mask_base = 1 << 7;

    while (time_decoder_->has_remaining(time_in)) {
        if (row_appender.remaining() < (uint32_t)BATCH) {
            ret = E_OVERFLOW;
            break;
        }

        // Block-level time filter check
        bool block_all_pass = false;
        if (filter != nullptr) {
            int64_t block_min, block_max;
            int block_count;
            if (time_decoder_->peek_next_block_range_int64(
                    time_in, block_min, block_max, block_count)) {
                if (!filter->satisfy_start_end_time(block_min, block_max)) {
                    int skipped = 0;
                    time_decoder_->skip_peeked_block_int64(time_in, skipped);
                    int nonnull = 0;
                    for (int i = 0; i < block_count; ++i) {
                        int vi = cur_value_index + 1 + i;
                        if (!value_page_col_notnull_bitmap_.empty() &&
                            ((value_page_col_notnull_bitmap_[vi / 8] & 0xFF) &
                             (null_mask_base >> (vi % 8))) != 0) {
                            ++nonnull;
                        }
                    }
                    cur_value_index += block_count;
                    if (nonnull > 0) {
                        int sk = 0;
                        value_decoder_->skip_int32(nonnull, sk, value_in);
                    }
                    continue;
                }
                if (filter->contain_start_end_time(block_min, block_max)) {
                    block_all_pass = true;
                }
            }
        }

        int time_count = 0;
        if (RET_FAIL(time_decoder_->read_batch_int64(times, BATCH, time_count,
                                                     time_in))) {
            break;
        }
        if (time_count == 0) break;

        bool is_null[BATCH];
        int nonnull_count = 0;
        for (int i = 0; i < time_count; ++i) {
            int vi = cur_value_index + 1 + i;
            if (value_page_col_notnull_bitmap_.empty() ||
                ((value_page_col_notnull_bitmap_[vi / 8] & 0xFF) &
                 (null_mask_base >> (vi % 8))) == 0) {
                is_null[i] = true;
            } else {
                is_null[i] = false;
                ++nonnull_count;
            }
        }

        bool time_mask[BATCH];
        int pass_count = time_count;
        if (filter != nullptr && !block_all_pass) {
            pass_count =
                filter->satisfy_batch_time(times, time_count, time_mask);
        }

        if (pass_count == 0) {
            if (nonnull_count > 0) {
                int skipped = 0;
                value_decoder_->skip_int32(nonnull_count, skipped, value_in);
            }
            cur_value_index += time_count;
            continue;
        }

        int value_count = 0;
        if (nonnull_count > 0) {
            if (RET_FAIL(value_decoder_->read_batch_int32(
                    values, nonnull_count, value_count, value_in))) {
                break;
            }
        }

        int val_idx = 0;
        for (int i = 0; i < time_count; ++i) {
            cur_value_index++;
            if (filter != nullptr && !block_all_pass && !time_mask[i]) {
                if (!is_null[i]) ++val_idx;
                continue;
            }
            if (is_null[i]) {
                if (UNLIKELY(!row_appender.add_row())) {
                    ret = E_OVERFLOW;
                    break;
                }
                row_appender.append(0, (char*)&times[i], sizeof(int64_t));
                row_appender.append_null(1);
            } else {
                int32_t val = values[val_idx++];
                if (filter != nullptr && !block_all_pass &&
                    !filter->satisfy(times[i], (int64_t)val)) {
                    continue;
                }
                if (UNLIKELY(!row_appender.add_row())) {
                    ret = E_OVERFLOW;
                    break;
                }
                row_appender.append(0, (char*)&times[i], sizeof(int64_t));
                row_appender.append(1, (char*)&val, sizeof(int32_t));
            }
        }
        if (ret != E_OK) break;
    }
    return ret;
}

int AlignedChunkReader::i64_DECODE_TV_BATCH(ByteStream& time_in,
                                            ByteStream& value_in,
                                            RowAppender& row_appender,
                                            Filter* filter) {
    int ret = E_OK;
    const int BATCH = 129;
    int64_t times[BATCH];
    int64_t values[BATCH];
    const uint32_t null_mask_base = 1 << 7;

    while (time_decoder_->has_remaining(time_in)) {
        if (row_appender.remaining() < (uint32_t)BATCH) {
            ret = E_OVERFLOW;
            break;
        }

        // Block-level time filter check: skip entire block if out of range
        bool block_all_pass = false;
        if (filter != nullptr) {
            int64_t block_min, block_max;
            int block_count;
            if (time_decoder_->peek_next_block_range_int64(
                    time_in, block_min, block_max, block_count)) {
                if (!filter->satisfy_start_end_time(block_min, block_max)) {
                    int skipped = 0;
                    time_decoder_->skip_peeked_block_int64(time_in, skipped);
                    int nonnull = 0;
                    for (int i = 0; i < block_count; ++i) {
                        int vi = cur_value_index + 1 + i;
                        if (!value_page_col_notnull_bitmap_.empty() &&
                            ((value_page_col_notnull_bitmap_[vi / 8] & 0xFF) &
                             (null_mask_base >> (vi % 8))) != 0) {
                            ++nonnull;
                        }
                    }
                    cur_value_index += block_count;
                    if (nonnull > 0) {
                        int sk = 0;
                        value_decoder_->skip_int64(nonnull, sk, value_in);
                    }
                    continue;
                }
                if (filter->contain_start_end_time(block_min, block_max)) {
                    block_all_pass = true;
                }
            }
        }

        int time_count = 0;
        if (RET_FAIL(time_decoder_->read_batch_int64(times, BATCH, time_count,
                                                     time_in))) {
            break;
        }
        if (time_count == 0) break;

        bool is_null[BATCH];
        int nonnull_count = 0;
        for (int i = 0; i < time_count; ++i) {
            int vi = cur_value_index + 1 + i;
            if (value_page_col_notnull_bitmap_.empty() ||
                ((value_page_col_notnull_bitmap_[vi / 8] & 0xFF) &
                 (null_mask_base >> (vi % 8))) == 0) {
                is_null[i] = true;
            } else {
                is_null[i] = false;
                ++nonnull_count;
            }
        }

        bool time_mask[BATCH];
        int pass_count = time_count;
        if (filter != nullptr && !block_all_pass) {
            pass_count =
                filter->satisfy_batch_time(times, time_count, time_mask);
        }

        if (pass_count == 0) {
            if (nonnull_count > 0) {
                int skipped = 0;
                value_decoder_->skip_int64(nonnull_count, skipped, value_in);
            }
            cur_value_index += time_count;
            continue;
        }

        int value_count = 0;
        if (nonnull_count > 0) {
            if (RET_FAIL(value_decoder_->read_batch_int64(
                    values, nonnull_count, value_count, value_in))) {
                break;
            }
        }

        int val_idx = 0;
        for (int i = 0; i < time_count; ++i) {
            cur_value_index++;
            if (filter != nullptr && !block_all_pass && !time_mask[i]) {
                if (!is_null[i]) ++val_idx;
                continue;
            }
            if (is_null[i]) {
                if (UNLIKELY(!row_appender.add_row())) {
                    ret = E_OVERFLOW;
                    break;
                }
                row_appender.append(0, (char*)&times[i], sizeof(int64_t));
                row_appender.append_null(1);
            } else {
                int64_t val = values[val_idx++];
                if (filter != nullptr && !block_all_pass &&
                    !filter->satisfy(times[i], val)) {
                    continue;
                }
                if (UNLIKELY(!row_appender.add_row())) {
                    ret = E_OVERFLOW;
                    break;
                }
                row_appender.append(0, (char*)&times[i], sizeof(int64_t));
                row_appender.append(1, (char*)&val, sizeof(int64_t));
            }
        }
        if (ret != E_OK) break;
    }
    return ret;
}

int AlignedChunkReader::float_DECODE_TV_BATCH(ByteStream& time_in,
                                              ByteStream& value_in,
                                              RowAppender& row_appender,
                                              Filter* filter) {
    int ret = E_OK;
    const int BATCH = 129;
    int64_t times[BATCH];
    float values[BATCH];
    const uint32_t null_mask_base = 1 << 7;

    while (time_decoder_->has_remaining(time_in)) {
        if (row_appender.remaining() < (uint32_t)BATCH) {
            ret = E_OVERFLOW;
            break;
        }

        // Block-level time filter check
        bool block_all_pass = false;
        if (filter != nullptr) {
            int64_t block_min, block_max;
            int block_count;
            if (time_decoder_->peek_next_block_range_int64(
                    time_in, block_min, block_max, block_count)) {
                if (!filter->satisfy_start_end_time(block_min, block_max)) {
                    int skipped = 0;
                    time_decoder_->skip_peeked_block_int64(time_in, skipped);
                    int nonnull = 0;
                    for (int i = 0; i < block_count; ++i) {
                        int vi = cur_value_index + 1 + i;
                        if (!value_page_col_notnull_bitmap_.empty() &&
                            ((value_page_col_notnull_bitmap_[vi / 8] & 0xFF) &
                             (null_mask_base >> (vi % 8))) != 0) {
                            ++nonnull;
                        }
                    }
                    cur_value_index += block_count;
                    if (nonnull > 0) {
                        int sk = 0;
                        value_decoder_->skip_float(nonnull, sk, value_in);
                    }
                    continue;
                }
                if (filter->contain_start_end_time(block_min, block_max)) {
                    block_all_pass = true;
                }
            }
        }

        int time_count = 0;
        if (RET_FAIL(time_decoder_->read_batch_int64(times, BATCH, time_count,
                                                     time_in))) {
            break;
        }
        if (time_count == 0) break;

        bool is_null[BATCH];
        int nonnull_count = 0;
        for (int i = 0; i < time_count; ++i) {
            int vi = cur_value_index + 1 + i;
            if (value_page_col_notnull_bitmap_.empty() ||
                ((value_page_col_notnull_bitmap_[vi / 8] & 0xFF) &
                 (null_mask_base >> (vi % 8))) == 0) {
                is_null[i] = true;
            } else {
                is_null[i] = false;
                ++nonnull_count;
            }
        }

        bool time_mask[BATCH];
        int pass_count = time_count;
        if (filter != nullptr && !block_all_pass) {
            pass_count =
                filter->satisfy_batch_time(times, time_count, time_mask);
        }

        if (pass_count == 0) {
            if (nonnull_count > 0) {
                int skipped = 0;
                value_decoder_->skip_float(nonnull_count, skipped, value_in);
            }
            cur_value_index += time_count;
            continue;
        }

        int value_count = 0;
        if (nonnull_count > 0) {
            if (RET_FAIL(value_decoder_->read_batch_float(
                    values, nonnull_count, value_count, value_in))) {
                break;
            }
        }

        int val_idx = 0;
        for (int i = 0; i < time_count; ++i) {
            cur_value_index++;
            if (filter != nullptr && !block_all_pass && !time_mask[i]) {
                if (!is_null[i]) ++val_idx;
                continue;
            }
            if (is_null[i]) {
                if (UNLIKELY(!row_appender.add_row())) {
                    ret = E_OVERFLOW;
                    break;
                }
                row_appender.append(0, (char*)&times[i], sizeof(int64_t));
                row_appender.append_null(1);
            } else {
                float val = values[val_idx++];
                if (UNLIKELY(!row_appender.add_row())) {
                    ret = E_OVERFLOW;
                    break;
                }
                row_appender.append(0, (char*)&times[i], sizeof(int64_t));
                row_appender.append(1, (char*)&val, sizeof(float));
            }
        }
        if (ret != E_OK) break;
    }
    return ret;
}

int AlignedChunkReader::double_DECODE_TV_BATCH(ByteStream& time_in,
                                               ByteStream& value_in,
                                               RowAppender& row_appender,
                                               Filter* filter) {
    int ret = E_OK;
    const int BATCH = 129;
    int64_t times[BATCH];
    double values[BATCH];
    const uint32_t null_mask_base = 1 << 7;

    while (time_decoder_->has_remaining(time_in)) {
        if (row_appender.remaining() < (uint32_t)BATCH) {
            ret = E_OVERFLOW;
            break;
        }

        // Block-level time filter check
        bool block_all_pass = false;
        if (filter != nullptr) {
            int64_t block_min, block_max;
            int block_count;
            if (time_decoder_->peek_next_block_range_int64(
                    time_in, block_min, block_max, block_count)) {
                if (!filter->satisfy_start_end_time(block_min, block_max)) {
                    int skipped = 0;
                    time_decoder_->skip_peeked_block_int64(time_in, skipped);
                    int nonnull = 0;
                    for (int i = 0; i < block_count; ++i) {
                        int vi = cur_value_index + 1 + i;
                        if (!value_page_col_notnull_bitmap_.empty() &&
                            ((value_page_col_notnull_bitmap_[vi / 8] & 0xFF) &
                             (null_mask_base >> (vi % 8))) != 0) {
                            ++nonnull;
                        }
                    }
                    cur_value_index += block_count;
                    if (nonnull > 0) {
                        int sk = 0;
                        value_decoder_->skip_double(nonnull, sk, value_in);
                    }
                    continue;
                }
                if (filter->contain_start_end_time(block_min, block_max)) {
                    block_all_pass = true;
                }
            }
        }

        int time_count = 0;
        if (RET_FAIL(time_decoder_->read_batch_int64(times, BATCH, time_count,
                                                     time_in))) {
            break;
        }
        if (time_count == 0) break;

        bool is_null[BATCH];
        int nonnull_count = 0;
        for (int i = 0; i < time_count; ++i) {
            int vi = cur_value_index + 1 + i;
            if (value_page_col_notnull_bitmap_.empty() ||
                ((value_page_col_notnull_bitmap_[vi / 8] & 0xFF) &
                 (null_mask_base >> (vi % 8))) == 0) {
                is_null[i] = true;
            } else {
                is_null[i] = false;
                ++nonnull_count;
            }
        }

        bool time_mask[BATCH];
        int pass_count = time_count;
        if (filter != nullptr && !block_all_pass) {
            pass_count =
                filter->satisfy_batch_time(times, time_count, time_mask);
        }

        if (pass_count == 0) {
            if (nonnull_count > 0) {
                int skipped = 0;
                value_decoder_->skip_double(nonnull_count, skipped, value_in);
            }
            cur_value_index += time_count;
            continue;
        }

        int value_count = 0;
        if (nonnull_count > 0) {
            if (RET_FAIL(value_decoder_->read_batch_double(
                    values, nonnull_count, value_count, value_in))) {
                break;
            }
        }

        int val_idx = 0;
        for (int i = 0; i < time_count; ++i) {
            cur_value_index++;
            if (filter != nullptr && !block_all_pass && !time_mask[i]) {
                if (!is_null[i]) ++val_idx;
                continue;
            }
            if (is_null[i]) {
                if (UNLIKELY(!row_appender.add_row())) {
                    ret = E_OVERFLOW;
                    break;
                }
                row_appender.append(0, (char*)&times[i], sizeof(int64_t));
                row_appender.append_null(1);
            } else {
                double val = values[val_idx++];
                if (UNLIKELY(!row_appender.add_row())) {
                    ret = E_OVERFLOW;
                    break;
                }
                row_appender.append(0, (char*)&times[i], sizeof(int64_t));
                row_appender.append(1, (char*)&val, sizeof(double));
            }
        }
        if (ret != E_OK) break;
    }
    return ret;
}

int AlignedChunkReader::decode_tv_buf_into_tsblock_by_datatype(
    ByteStream& time_in, ByteStream& value_in, TsBlock* ret_tsblock,
    Filter* filter, common::PageArena* pa) {
    int ret = E_OK;
    RowAppender row_appender(ret_tsblock);
    switch (value_chunk_header_.data_type_) {
        case common::BOOLEAN:
            DECODE_TYPED_TV_INTO_TSBLOCK(bool, boolean, time_in_, value_in_,
                                         row_appender);
            break;
        case common::DATE:
        case common::INT32:
            ret = i32_DECODE_TYPED_TV_INTO_TSBLOCK(time_in_, value_in_,
                                                   row_appender, filter);
            break;
        case common::TIMESTAMP:
        case common::INT64:
            DECODE_TYPED_TV_INTO_TSBLOCK(int64_t, int64, time_in_, value_in_,
                                         row_appender);
            break;
        case common::FLOAT:
            DECODE_TYPED_TV_INTO_TSBLOCK(float, float, time_in_, value_in_,
                                         row_appender);
            break;
        case common::DOUBLE:
            DECODE_TYPED_TV_INTO_TSBLOCK(double, double, time_in_, value_in_,
                                         row_appender);
            break;
        case common::STRING:
        case common::BLOB:
        case common::TEXT:
            ret = STRING_DECODE_TYPED_TV_INTO_TSBLOCK(
                time_in, value_in, row_appender, *pa, filter);
            break;
        default:
            ret = E_NOT_SUPPORT;
            ASSERT(false);
    }
    if (ret_tsblock->get_row_count() == 0 && ret == E_OK) {
        ret = E_NO_MORE_DATA;
    }
    return ret;
}

int AlignedChunkReader::STRING_DECODE_TYPED_TV_INTO_TSBLOCK(
    ByteStream& time_in, ByteStream& value_in, RowAppender& row_appender,
    PageArena& pa, Filter* filter) {
    int ret = E_OK;
    int64_t time = 0;
    common::String value;
    uint32_t mask = 1 << 7;
    while (time_decoder_->has_remaining(time_in)) {
        cur_value_index++;
        bool should_read_data = true;
        if (value_page_col_notnull_bitmap_.empty() ||
            ((value_page_col_notnull_bitmap_[cur_value_index / 8] & 0xFF) &
             (mask >> (cur_value_index % 8))) == 0) {
            should_read_data = false;
        }

        if (should_read_data) {
            assert(value_decoder_->has_remaining(value_in));
            if (!value_decoder_->has_remaining(value_in)) {
                return E_DATA_INCONSISTENCY;
            }
        }

        if (UNLIKELY(!row_appender.add_row())) {
            ret = E_OVERFLOW;
            cur_value_index--;
            break;
        } else if (RET_FAIL(time_decoder_->read_int64(time, time_in))) {
        } else if (should_read_data &&
                   RET_FAIL(value_decoder_->read_String(value, pa, value_in))) {
        } else if (filter != nullptr && !filter->satisfy(time, value)) {
            row_appender.backoff_add_row();
            continue;
        } else {
            row_appender.append(0, (char*)&time, sizeof(time));
            if (!should_read_data) {
                row_appender.append_null(1);
            } else {
                row_appender.append(1, value.buf_, value.len_);
            }
        }
    }
    return ret;
}

bool AlignedChunkReader::should_skip_page_by_time(int64_t min_time_hint) {
    if (min_time_hint == std::numeric_limits<int64_t>::min()) {
        return false;
    }
    // Use time page statistic for time-based skipping.
    if (cur_time_page_header_.statistic_ != nullptr) {
        return cur_time_page_header_.statistic_->end_time_ < min_time_hint;
    }
    if (cur_value_page_header_.statistic_ != nullptr) {
        return cur_value_page_header_.statistic_->end_time_ < min_time_hint;
    }
    return false;
}

bool AlignedChunkReader::should_skip_page_by_offset(int& row_offset) {
    if (row_offset <= 0) {
        return false;
    }
    // Use time page statistic for count.
    Statistic* stat = cur_time_page_header_.statistic_;
    if (stat == nullptr) {
        stat = cur_value_page_header_.statistic_;
    }
    if (stat == nullptr || stat->count_ == 0) {
        return false;
    }
    int32_t count = stat->count_;
    if (row_offset >= count) {
        row_offset -= count;
        return true;
    }
    return false;
}

int AlignedChunkReader::get_next_page(TsBlock* ret_tsblock,
                                      Filter* oneshoot_filter, PageArena& pa,
                                      int64_t min_time_hint, int& row_offset,
                                      int& row_limit) {
    if (multi_value_mode_) {
        return get_next_page_multi(ret_tsblock, oneshoot_filter, pa);
    }
    int ret = E_OK;
    Filter* filter =
        (oneshoot_filter != nullptr ? oneshoot_filter : time_filter_);

    if (row_limit == 0) {
        return E_NO_MORE_DATA;
    }

    if (prev_time_page_not_finish() && prev_value_page_not_finish()) {
        ret = decode_time_value_buf_into_tsblock(ret_tsblock, filter, &pa);
        return ret;
    }
    if (!prev_time_page_not_finish() && !prev_value_page_not_finish()) {
        while (IS_SUCC(ret)) {
            if (RET_FAIL(get_cur_page_header(
                    time_chunk_meta_, time_in_stream_, cur_time_page_header_,
                    time_chunk_visit_offset_, time_chunk_header_))) {
            } else if (RET_FAIL(get_cur_page_header(
                           value_chunk_meta_, value_in_stream_,
                           cur_value_page_header_, value_chunk_visit_offset_,
                           value_chunk_header_))) {
            } else if (!cur_page_statisify_filter(filter)) {
                if (RET_FAIL(skip_cur_page())) {
                }
            } else if (should_skip_page_by_time(min_time_hint)) {
                if (RET_FAIL(skip_cur_page())) {
                }
            } else if (should_skip_page_by_offset(row_offset)) {
                if (RET_FAIL(skip_cur_page())) {
                }
            } else {
                break;
            }
            if (!has_more_data()) {
                ret = E_NO_MORE_DATA;
                break;
            }
        }
        if (IS_SUCC(ret)) {
            ret = decode_cur_time_page_data() || decode_cur_value_page_data();
        }
    }
    if (IS_SUCC(ret)) {
        ret = decode_time_value_buf_into_tsblock(ret_tsblock, filter, &pa);
    }
    return ret;
}

// ══════════════════════════════════════════════════════════════════════════
//  Multi-value AlignedChunkReader implementation
// ══════════════════════════════════════════════════════════════════════════

int AlignedChunkReader::load_by_aligned_meta_multi(
    ChunkMeta* time_chunk_meta, const std::vector<ChunkMeta*>& value_metas) {
    int ret = E_OK;
    multi_value_mode_ = true;
    time_chunk_meta_ = time_chunk_meta;
    page_plan_built_ = false;
    current_page_loaded_ = false;
    current_page_plan_index_ = 0;
    time_predecoded_ = false;
    page_all_times_.clear();
    page_time_count_ = 0;
    page_time_cursor_ = 0;

    // ── Load time chunk header ──
    file_data_time_buf_size_ = 1024;
    int32_t ret_read_len = 0;
    char* time_file_data_buf =
        (char*)mem_alloc(file_data_time_buf_size_, MOD_CHUNK_READER);
    if (IS_NULL(time_file_data_buf)) return E_OOM;

    ret = read_file_->read(time_chunk_meta_->offset_of_chunk_header_,
                           time_file_data_buf, file_data_time_buf_size_,
                           ret_read_len);
    if (IS_SUCC(ret) && ret_read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
        ret = E_TSFILE_CORRUPTED;
        mem_free(time_file_data_buf);
        return ret;
    }
    if (IS_SUCC(ret)) {
        time_in_stream_.wrap_from(time_file_data_buf, ret_read_len);
        if (RET_FAIL(time_chunk_header_.deserialize_from(time_in_stream_))) {
            return ret;
        }
        time_chunk_visit_offset_ = time_in_stream_.read_pos();
    }

    // Alloc time decoder/compressor
    if (IS_SUCC(ret)) {
        if (RET_FAIL(alloc_compressor_and_decoder(
                time_decoder_, time_compressor_,
                time_chunk_header_.encoding_type_,
                time_chunk_header_.data_type_,
                time_chunk_header_.compression_type_))) {
            return ret;
        }
    }

    // ── Load each value column ──
    // Reuse existing ValueColumnState objects if count matches (reset() already
    // cleared their internal state).  Otherwise, recreate.
    if (value_columns_.size() != value_metas.size()) {
        for (auto* p : value_columns_) delete p;
        value_columns_.clear();
        value_columns_.reserve(value_metas.size());
        for (size_t c = 0; c < value_metas.size(); c++) {
            value_columns_.push_back(new ValueColumnState);
        }
    }
    for (size_t c = 0; c < value_metas.size() && IS_SUCC(ret); c++) {
        auto* col = value_columns_[c];
        col->chunk_meta = value_metas[c];
        col->file_data_buf_size = 1024;
        ret_read_len = 0;
        char* vbuf =
            (char*)mem_alloc(col->file_data_buf_size, MOD_CHUNK_READER);
        if (IS_NULL(vbuf)) return E_OOM;

        ret = read_file_->read(col->chunk_meta->offset_of_chunk_header_, vbuf,
                               col->file_data_buf_size, ret_read_len);
        if (IS_SUCC(ret) && ret_read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
            ret = E_TSFILE_CORRUPTED;
            mem_free(vbuf);
            break;
        }
        if (IS_SUCC(ret)) {
            col->in_stream.wrap_from(vbuf, ret_read_len);
            if (RET_FAIL(col->chunk_header.deserialize_from(col->in_stream))) {
                break;
            }
            col->chunk_visit_offset = col->in_stream.read_pos();
            if (RET_FAIL(alloc_compressor_and_decoder(
                    col->decoder, col->compressor,
                    col->chunk_header.encoding_type_,
                    col->chunk_header.data_type_,
                    col->chunk_header.compression_type_))) {
                break;
            }
        }
    }

    return ret;
}

bool AlignedChunkReader::has_more_data_multi() const {
    if (page_plan_built_) {
        if (current_page_loaded_) {
            return page_time_cursor_ < page_time_count_;
        }
        return current_page_plan_index_ < chunk_pages_.size();
    }
    if (chunk_level_active_) return true;
    if (prev_time_page_not_finish() || prev_any_value_page_not_finish_multi()) {
        return true;
    }
    if (time_chunk_visit_offset_ - time_chunk_header_.serialized_size_ <
        time_chunk_header_.data_size_) {
        return true;
    }
    for (const auto* col : value_columns_) {
        if (col->chunk_visit_offset - col->chunk_header.serialized_size_ <
            col->chunk_header.data_size_) {
            return true;
        }
    }
    return false;
}

bool AlignedChunkReader::prev_any_value_page_not_finish_multi() const {
    for (const auto* col : value_columns_) {
        if ((col->decoder && col->decoder->has_remaining(col->in)) ||
            col->in.has_remaining()) {
            return true;
        }
    }
    return false;
}

bool AlignedChunkReader::has_variable_length_value_column() const {
    for (const auto* col : value_columns_) {
        if (col->chunk_header.data_type_ == common::STRING ||
            col->chunk_header.data_type_ == common::TEXT ||
            col->chunk_header.data_type_ == common::BLOB) {
            return true;
        }
    }
    return false;
}

int AlignedChunkReader::count_non_null_prefix(
    const std::vector<uint8_t>& bitmap, int32_t row_limit) const {
    if (row_limit <= 0 || bitmap.empty()) {
        return 0;
    }
    const uint32_t mask_base = 1 << 7;
    int count = 0;
    for (int32_t i = 0; i < row_limit; i++) {
        if (((bitmap[i / 8] & 0xFF) & (mask_base >> (i % 8))) != 0) {
            count++;
        }
    }
    return count;
}

int AlignedChunkReader::decode_time_page_direct(
    const ChunkPageInfo& page_info, std::vector<int64_t>& out_times) {
    out_times.clear();
    if (page_info.time_compressed_size == 0) {
        return E_OK;
    }

    char stack_buf[4096];
    char* compressed_buf = stack_buf;
    bool heap = page_info.time_compressed_size > sizeof(stack_buf);
    if (heap) {
        compressed_buf = static_cast<char*>(common::mem_alloc(
            page_info.time_compressed_size, common::MOD_DEFAULT));
        if (compressed_buf == nullptr) {
            return E_OOM;
        }
    }

    int32_t read_len = 0;
    int ret = read_file_->read(page_info.time_file_offset, compressed_buf,
                               page_info.time_compressed_size, read_len);
    if (IS_FAIL(ret)) {
        if (heap) common::mem_free(compressed_buf);
        return ret;
    }

    char* uncompressed_buf = nullptr;
    uint32_t uncompressed_size = 0;
    if (RET_FAIL(time_compressor_->reset(false))) {
        if (heap) common::mem_free(compressed_buf);
        return ret;
    }
    ret = time_compressor_->uncompress(compressed_buf,
                                       page_info.time_compressed_size,
                                       uncompressed_buf, uncompressed_size);
    if (heap && compressed_buf != uncompressed_buf) {
        common::mem_free(compressed_buf);
    }
    if (IS_FAIL(ret) || uncompressed_size != page_info.time_uncompressed_size) {
        if (uncompressed_buf != nullptr) {
            time_compressor_->after_uncompress(uncompressed_buf);
        }
        return E_TSFILE_CORRUPTED;
    }

    common::ByteStream in;
    in.wrap_from(uncompressed_buf, uncompressed_size);
    time_decoder_->reset();
    const int batch_size = 1024;
    int64_t batch[batch_size];
    while (time_decoder_->has_remaining(in)) {
        int actual = 0;
        if (RET_FAIL(time_decoder_->read_batch_int64(batch, batch_size, actual,
                                                     in))) {
            break;
        }
        if (actual == 0) {
            break;
        }
        out_times.insert(out_times.end(), batch, batch + actual);
    }
    time_compressor_->after_uncompress(uncompressed_buf);
    return ret;
}

int AlignedChunkReader::build_page_plan(Filter* filter) {
    int ret = E_OK;
    chunk_pages_.clear();
    current_page_plan_index_ = 0;
    current_page_loaded_ = false;
    page_plan_built_ = false;

    const uint32_t num_cols = value_columns_.size();
    while (IS_SUCC(ret)) {
        if (time_chunk_visit_offset_ - time_chunk_header_.serialized_size_ >=
            time_chunk_header_.data_size_) {
            break;
        }

        if (RET_FAIL(get_cur_page_header(
                time_chunk_meta_, time_in_stream_, cur_time_page_header_,
                time_chunk_visit_offset_, time_chunk_header_))) {
            break;
        }
        if (cur_time_page_header_.compressed_size_ == 0 &&
            cur_time_page_header_.uncompressed_size_ == 0) {
            break;
        }

        ChunkPageInfo page_info;
        page_info.time_file_offset = time_chunk_meta_->offset_of_chunk_header_ +
                                     time_chunk_visit_offset_;
        page_info.time_compressed_size = cur_time_page_header_.compressed_size_;
        page_info.time_uncompressed_size =
            cur_time_page_header_.uncompressed_size_;
        page_info.value_file_offsets.resize(num_cols);
        page_info.value_compressed_sizes.resize(num_cols);
        page_info.value_uncompressed_sizes.resize(num_cols);

        for (uint32_t c = 0; c < num_cols && IS_SUCC(ret); c++) {
            auto* col = value_columns_[c];
            if (RET_FAIL(get_cur_page_header(
                    col->chunk_meta, col->in_stream, col->cur_page_header,
                    col->chunk_visit_offset, col->chunk_header,
                    &col->file_data_buf_size))) {
                break;
            }
            page_info.value_file_offsets[c] =
                col->chunk_meta->offset_of_chunk_header_ +
                col->chunk_visit_offset;
            page_info.value_compressed_sizes[c] =
                col->cur_page_header.compressed_size_;
            page_info.value_uncompressed_sizes[c] =
                col->cur_page_header.uncompressed_size_;
        }
        if (IS_FAIL(ret)) {
            break;
        }

        Statistic* stat = cur_time_page_header_.statistic_;
        if (filter == nullptr) {
            page_info.pass_type = PagePassType::FULL_PASS;
            page_info.row_begin = 0;
            page_info.row_end = stat != nullptr ? stat->count_ : 0;
        } else if (stat != nullptr && !filter->satisfy(stat)) {
            page_info.pass_type = PagePassType::SKIP;
        } else if (stat != nullptr && filter->contain_start_end_time(
                                          stat->start_time_, stat->end_time_)) {
            page_info.pass_type = PagePassType::FULL_PASS;
            page_info.row_begin = 0;
            page_info.row_end = stat->count_;
        } else {
            page_info.pass_type = PagePassType::BOUNDARY;
            std::vector<int64_t> times;
            if (RET_FAIL(decode_time_page_direct(page_info, times))) {
                break;
            }
            int32_t first = -1;
            int32_t last = -1;
            for (int32_t i = 0; i < static_cast<int32_t>(times.size()); i++) {
                if (filter->satisfy_start_end_time(times[i], times[i])) {
                    if (first < 0) first = i;
                    last = i;
                }
            }
            if (first >= 0) {
                page_info.row_begin = first;
                page_info.row_end = last + 1;
            } else {
                page_info.pass_type = PagePassType::SKIP;
            }
        }

        if (page_info.pass_type != PagePassType::SKIP) {
            if (page_info.row_end == 0) {
                std::vector<int64_t> times;
                if (RET_FAIL(decode_time_page_direct(page_info, times))) {
                    break;
                }
                page_info.row_end = static_cast<int32_t>(times.size());
            }
            if (page_info.row_begin < page_info.row_end) {
                chunk_pages_.push_back(std::move(page_info));
            }
        }

        time_chunk_visit_offset_ += cur_time_page_header_.compressed_size_;
        time_in_stream_.wrapped_buf_advance_read_pos(
            cur_time_page_header_.compressed_size_);
        for (uint32_t c = 0; c < num_cols; c++) {
            auto* col = value_columns_[c];
            col->chunk_visit_offset += col->cur_page_header.compressed_size_;
            col->in_stream.wrapped_buf_advance_read_pos(
                col->cur_page_header.compressed_size_);
        }
    }

    page_plan_built_ = IS_SUCC(ret);
    return ret;
}

void AlignedChunkReader::release_current_page_state() {
    time_predecoded_ = false;
    page_all_times_.clear();
    page_time_count_ = 0;
    page_time_cursor_ = 0;
    for (auto* col : value_columns_) {
        if (col->uncompressed_buf != nullptr && col->compressor != nullptr) {
            col->compressor->after_uncompress(col->uncompressed_buf);
            col->uncompressed_buf = nullptr;
        }
        col->notnull_bitmap.clear();
        col->predecoded_values.clear();
        col->predecoded_strings.clear();
        col->predecoded_count = 0;
        col->predecoded_read_pos = 0;
        col->predecoded = false;
        col->cur_value_index = -1;
        col->in.reset();
        col->predecode_pa.destroy();
    }
    current_page_loaded_ = false;
}

int AlignedChunkReader::predecode_value_page_for_plan(
    uint32_t col_idx, const ChunkPageInfo& page_info) {
    auto* col = value_columns_[col_idx];
    col->notnull_bitmap.clear();
    col->predecoded_values.clear();
    col->predecoded_strings.clear();
    col->predecoded_read_pos = 0;
    col->predecoded_count = 0;
    col->predecoded = false;
    col->predecode_pa.destroy();

    if (page_info.value_compressed_sizes[col_idx] == 0) {
        col->in.wrap_from(nullptr, 0);
        return E_OK;
    }

    char stack_buf[4096];
    char* compressed_buf = stack_buf;
    bool heap = page_info.value_compressed_sizes[col_idx] > sizeof(stack_buf);
    if (heap) {
        compressed_buf = static_cast<char*>(common::mem_alloc(
            page_info.value_compressed_sizes[col_idx], common::MOD_DEFAULT));
        if (compressed_buf == nullptr) {
            return E_OOM;
        }
    }

    int32_t read_len = 0;
    int ret =
        read_file_->read(page_info.value_file_offsets[col_idx], compressed_buf,
                         page_info.value_compressed_sizes[col_idx], read_len);
    if (IS_FAIL(ret)) {
        if (heap) common::mem_free(compressed_buf);
        return ret;
    }

    char* uncompressed_buf = nullptr;
    uint32_t uncompressed_size = 0;
    if (RET_FAIL(col->compressor->reset(false))) {
        if (heap) common::mem_free(compressed_buf);
        return ret;
    }
    ret = col->compressor->uncompress(compressed_buf,
                                      page_info.value_compressed_sizes[col_idx],
                                      uncompressed_buf, uncompressed_size);
    if (heap && compressed_buf != uncompressed_buf) {
        common::mem_free(compressed_buf);
    }
    if (IS_FAIL(ret) ||
        uncompressed_size != page_info.value_uncompressed_sizes[col_idx]) {
        if (uncompressed_buf != nullptr) {
            col->compressor->after_uncompress(uncompressed_buf);
        }
        return E_TSFILE_CORRUPTED;
    }
    col->uncompressed_buf = uncompressed_buf;

    uint32_t offset = 0;
    uint32_t data_num = SerializationUtil::read_ui32(uncompressed_buf);
    offset += sizeof(uint32_t);
    col->notnull_bitmap.resize((data_num + 7) / 8);
    for (size_t i = 0; i < col->notnull_bitmap.size(); i++) {
        col->notnull_bitmap[i] = *(uncompressed_buf + offset++);
    }

    char* value_buf = uncompressed_buf + offset;
    uint32_t value_buf_size = uncompressed_size - offset;
    common::ByteStream in;
    in.wrap_from(value_buf, value_buf_size);
    col->decoder->reset();

    auto dt = col->chunk_header.data_type_;
    int nonnull_total = count_non_null_prefix(col->notnull_bitmap,
                                              static_cast<int32_t>(data_num));
    int prefix_nonnull =
        count_non_null_prefix(col->notnull_bitmap, page_info.row_begin);
    col->predecoded_read_pos = prefix_nonnull;

    if (dt == common::STRING || dt == common::TEXT || dt == common::BLOB) {
        col->predecode_pa.init(512, common::MOD_TSFILE_READER);
        col->predecoded_strings.resize(nonnull_total);
        for (int i = 0; i < nonnull_total; i++) {
            if (RET_FAIL(col->decoder->read_String(col->predecoded_strings[i],
                                                   col->predecode_pa, in))) {
                return ret;
            }
        }
        col->predecoded_count = nonnull_total;
        col->predecoded = true;
        return E_OK;
    }

    if (nonnull_total == 0) {
        col->predecoded = true;
        return E_OK;
    }

    uint32_t elem_size = common::get_data_type_size(dt);
    col->predecoded_values.resize(static_cast<size_t>(nonnull_total) *
                                  elem_size);
    int actual = 0;
    switch (dt) {
        case common::BOOLEAN: {
            bool* out = reinterpret_cast<bool*>(col->predecoded_values.data());
            for (int i = 0; i < nonnull_total; i++) {
                if (RET_FAIL(col->decoder->read_boolean(out[i], in))) {
                    return ret;
                }
            }
            actual = nonnull_total;
            break;
        }
        case common::INT32:
        case common::DATE:
            if (RET_FAIL(col->decoder->read_batch_int32(
                    reinterpret_cast<int32_t*>(col->predecoded_values.data()),
                    nonnull_total, actual, in))) {
                return ret;
            }
            break;
        case common::INT64:
        case common::TIMESTAMP:
            if (RET_FAIL(col->decoder->read_batch_int64(
                    reinterpret_cast<int64_t*>(col->predecoded_values.data()),
                    nonnull_total, actual, in))) {
                return ret;
            }
            break;
        case common::FLOAT:
            if (RET_FAIL(col->decoder->read_batch_float(
                    reinterpret_cast<float*>(col->predecoded_values.data()),
                    nonnull_total, actual, in))) {
                return ret;
            }
            break;
        case common::DOUBLE:
            if (RET_FAIL(col->decoder->read_batch_double(
                    reinterpret_cast<double*>(col->predecoded_values.data()),
                    nonnull_total, actual, in))) {
                return ret;
            }
            break;
        default:
            return E_NOT_SUPPORT;
    }
    col->predecoded_count = actual;
    col->predecoded = true;
    return E_OK;
}

int AlignedChunkReader::load_current_planned_page() {
    if (current_page_plan_index_ >= chunk_pages_.size()) {
        return E_NO_MORE_DATA;
    }

    release_current_page_state();
    const ChunkPageInfo& page_info = chunk_pages_[current_page_plan_index_];
    int ret = decode_time_page_direct(page_info, page_all_times_);
    if (IS_FAIL(ret)) {
        return ret;
    }
    page_time_cursor_ = page_info.row_begin;
    page_time_count_ = page_info.row_end;
    time_predecoded_ = true;

#ifdef ENABLE_THREADS
    if (decode_pool_ != nullptr && value_columns_.size() > 1) {
        std::vector<int> col_rets(value_columns_.size(), E_OK);
        for (uint32_t c = 0; c < value_columns_.size(); c++) {
            decode_pool_->submit([&, c]() {
                col_rets[c] = predecode_value_page_for_plan(c, page_info);
            });
        }
        decode_pool_->wait_all();
        for (uint32_t c = 0; c < value_columns_.size(); c++) {
            if (IS_FAIL(col_rets[c])) {
                return col_rets[c];
            }
        }
    } else
#endif
    {
        for (uint32_t c = 0; c < value_columns_.size(); c++) {
            if (RET_FAIL(predecode_value_page_for_plan(c, page_info))) {
                return ret;
            }
        }
    }

    current_page_loaded_ = true;
    return E_OK;
}

int AlignedChunkReader::scatter_current_page(common::TsBlock* ret_tsblock,
                                             RowAppender& row_appender,
                                             common::PageArena* pa) {
    const uint32_t null_mask_base = 1 << 7;
    while (page_time_cursor_ < page_time_count_) {
        if (row_appender.remaining() == 0) {
            return E_OVERFLOW;
        }

        int64_t ts = page_all_times_[page_time_cursor_];
        if (UNLIKELY(!row_appender.add_row())) {
            return E_OVERFLOW;
        }
        row_appender.append(0, reinterpret_cast<char*>(&ts), sizeof(ts));

        for (uint32_t c = 0; c < value_columns_.size(); c++) {
            auto* col = value_columns_[c];
            bool is_null = true;
            if (!col->notnull_bitmap.empty()) {
                is_null = ((col->notnull_bitmap[page_time_cursor_ / 8] & 0xFF) &
                           (null_mask_base >> (page_time_cursor_ % 8))) == 0;
            }
            if (is_null) {
                row_appender.append_null(c + 1);
                continue;
            }

            if (col->chunk_header.data_type_ == common::STRING ||
                col->chunk_header.data_type_ == common::TEXT ||
                col->chunk_header.data_type_ == common::BLOB) {
                const common::String& value =
                    col->predecoded_strings[col->predecoded_read_pos++];
                row_appender.append(c + 1, value.buf_, value.len_);
            } else {
                uint32_t elem_size =
                    common::get_data_type_size(col->chunk_header.data_type_);
                row_appender.append(
                    c + 1,
                    col->predecoded_values.data() +
                        static_cast<size_t>(col->predecoded_read_pos++) *
                            elem_size,
                    elem_size);
            }
        }
        page_time_cursor_++;
    }

    current_page_plan_index_++;
    release_current_page_state();
    return E_OK;
}

int AlignedChunkReader::get_next_page_multi(TsBlock* ret_tsblock,
                                            Filter* oneshoot_filter,
                                            PageArena& pa) {
    int ret = E_OK;
    Filter* filter =
        (oneshoot_filter != nullptr ? oneshoot_filter : time_filter_);

    if (!page_plan_built_) {
        if (RET_FAIL(build_page_plan(filter))) {
            return ret;
        }
    }
    if (chunk_pages_.empty()) {
        return E_NO_MORE_DATA;
    }

    while (current_page_plan_index_ < chunk_pages_.size()) {
        if (!current_page_loaded_) {
            if (RET_FAIL(load_current_planned_page())) {
                return ret;
            }
        }
        RowAppender row_appender(ret_tsblock);
        ret = scatter_current_page(ret_tsblock, row_appender, &pa);
        if (ret == E_OVERFLOW) {
            return E_OK;
        }
        if (IS_FAIL(ret)) {
            return ret;
        }
    }
    return E_NO_MORE_DATA;
}

int AlignedChunkReader::get_next_page_multi_serial(TsBlock* ret_tsblock,
                                                   Filter* filter,
                                                   PageArena& pa) {
    int ret = E_OK;
    bool pt = prev_time_page_not_finish();
    bool pv = prev_any_value_page_not_finish_multi();
    if (pt && pv) {
        ret =
            decode_time_value_buf_into_tsblock_multi(ret_tsblock, filter, &pa);
        return ret;
    }
    if (!pt && !pv) {
        while (IS_SUCC(ret)) {
            if (RET_FAIL(get_cur_page_header(
                    time_chunk_meta_, time_in_stream_, cur_time_page_header_,
                    time_chunk_visit_offset_, time_chunk_header_))) {
                break;
            }
            for (size_t c = 0; c < value_columns_.size() && IS_SUCC(ret); c++) {
                auto* col = value_columns_[c];
                if (RET_FAIL(get_cur_page_header(
                        col->chunk_meta, col->in_stream, col->cur_page_header,
                        col->chunk_visit_offset, col->chunk_header,
                        &col->file_data_buf_size))) {
                }
            }
            if (IS_FAIL(ret)) break;
            if (cur_page_statisify_filter_multi(filter)) break;
            if (RET_FAIL(skip_cur_page_multi())) break;
            if (!has_more_data()) {
                ret = E_NO_MORE_DATA;
                break;
            }
        }
        if (IS_SUCC(ret)) {
            ret = decode_cur_time_page_data();
            if (IS_SUCC(ret)) ret = decode_cur_value_pages_multi();
        }
    }
    if (IS_SUCC(ret)) {
        ret =
            decode_time_value_buf_into_tsblock_multi(ret_tsblock, filter, &pa);
    }
    return ret;
}

bool AlignedChunkReader::cur_page_statisify_filter_multi(Filter* filter) {
    bool time_satisfy = filter == nullptr ||
                        cur_time_page_header_.statistic_ == nullptr ||
                        filter->satisfy(cur_time_page_header_.statistic_);
    return time_satisfy;
}

int AlignedChunkReader::skip_cur_page_multi() {
    time_chunk_visit_offset_ += cur_time_page_header_.compressed_size_;
    time_in_stream_.wrapped_buf_advance_read_pos(
        cur_time_page_header_.compressed_size_);
    for (auto* col : value_columns_) {
        col->chunk_visit_offset += col->cur_page_header.compressed_size_;
        col->in_stream.wrapped_buf_advance_read_pos(
            col->cur_page_header.compressed_size_);
    }
    return E_OK;
}

int AlignedChunkReader::decode_cur_value_pages_multi() {
    int ret = E_OK;
    // Phase 1: Serial IO — ensure each column's page data is in memory.
    for (size_t c = 0; c < value_columns_.size() && IS_SUCC(ret); c++) {
        ret = ensure_value_page_loaded(*value_columns_[c]);
    }
    if (IS_FAIL(ret)) return ret;

        // Phase 2: Parallel CPU — decompress + parse bitmap + reset decoder.
#ifdef ENABLE_THREADS
    if (value_columns_.size() > 1 && decode_pool_ != nullptr) {
        std::vector<int> col_rets(value_columns_.size(), E_OK);
        for (size_t c = 0; c < value_columns_.size(); c++) {
            auto* col = value_columns_[c];
            int* col_ret = &col_rets[c];
            decode_pool_->submit([col, col_ret] {
                *col_ret = decompress_and_parse_value_page(*col);
            });
        }
        decode_pool_->wait_all();
        for (size_t c = 0; c < col_rets.size(); c++) {
            if (IS_FAIL(col_rets[c])) return col_rets[c];
        }
    } else
#endif
    {
        for (size_t c = 0; c < value_columns_.size() && IS_SUCC(ret); c++) {
            ret = decompress_and_parse_value_page(*value_columns_[c]);
        }
    }
    return ret;
}

int AlignedChunkReader::decode_cur_value_page_data_for(ValueColumnState& col) {
    int ret = E_OK;

    // Step 1: ensure full page data is loaded
    if (col.in_stream.remaining_size() < col.cur_page_header.compressed_size_) {
        if (RET_FAIL(read_from_file_and_rewrap(
                col.in_stream, col.chunk_meta, col.chunk_visit_offset,
                col.file_data_buf_size,
                col.cur_page_header.compressed_size_))) {
            return ret;
        }
    }

    if (col.cur_page_header.compressed_size_ == 0) {
        col.in.wrap_from(nullptr, 0);
        return E_OK;
    }

    // Step 2: uncompress
    char* compressed_buf =
        col.in_stream.get_wrapped_buf() + col.in_stream.read_pos();
    uint32_t compressed_size = col.cur_page_header.compressed_size_;
    col.in_stream.wrapped_buf_advance_read_pos(compressed_size);
    col.chunk_visit_offset += compressed_size;

    char* uncompressed_buf = nullptr;
    uint32_t uncompressed_size = 0;
    if (RET_FAIL(col.compressor->reset(false))) {
        return ret;
    }
    if (RET_FAIL(col.compressor->uncompress(compressed_buf, compressed_size,
                                            uncompressed_buf,
                                            uncompressed_size))) {
        return ret;
    }
    col.uncompressed_buf = uncompressed_buf;

    if (uncompressed_size != col.cur_page_header.uncompressed_size_) {
        return E_TSFILE_CORRUPTED;
    }

    // Step 3: parse bitmap + value data
    uint32_t offset = 0;
    uint32_t data_num = SerializationUtil::read_ui32(uncompressed_buf);
    offset += sizeof(uint32_t);
    col.notnull_bitmap.resize((data_num + 7) / 8);
    for (size_t i = 0; i < col.notnull_bitmap.size(); i++) {
        col.notnull_bitmap[i] = *(uncompressed_buf + offset);
        offset++;
    }
    col.cur_value_index = -1;

    char* value_buf = uncompressed_buf + offset;
    uint32_t value_buf_size = uncompressed_size - offset;
    col.decoder->reset();
    col.in.wrap_from(value_buf, value_buf_size);
    return ret;
}

int AlignedChunkReader::ensure_value_page_loaded(ValueColumnState& col) {
    int ret = E_OK;
    if (col.in_stream.remaining_size() < col.cur_page_header.compressed_size_) {
        if (RET_FAIL(read_from_file_and_rewrap(
                col.in_stream, col.chunk_meta, col.chunk_visit_offset,
                col.file_data_buf_size,
                col.cur_page_header.compressed_size_))) {
            return ret;
        }
    }
    return ret;
}

int AlignedChunkReader::decompress_and_parse_value_page(ValueColumnState& col) {
    int ret = E_OK;

    if (col.cur_page_header.compressed_size_ == 0) {
        col.in.wrap_from(nullptr, 0);
        return E_OK;
    }

    // Decompress
    char* compressed_buf =
        col.in_stream.get_wrapped_buf() + col.in_stream.read_pos();
    uint32_t compressed_size = col.cur_page_header.compressed_size_;
    col.in_stream.wrapped_buf_advance_read_pos(compressed_size);
    col.chunk_visit_offset += compressed_size;

    char* uncompressed_buf = nullptr;
    uint32_t uncompressed_size = 0;
    if (RET_FAIL(col.compressor->reset(false))) {
        return ret;
    }
    if (RET_FAIL(col.compressor->uncompress(compressed_buf, compressed_size,
                                            uncompressed_buf,
                                            uncompressed_size))) {
        return ret;
    }
    col.uncompressed_buf = uncompressed_buf;

    if (uncompressed_size != col.cur_page_header.uncompressed_size_) {
        return E_TSFILE_CORRUPTED;
    }

    // Parse bitmap + value data
    uint32_t offset = 0;
    uint32_t data_num = SerializationUtil::read_ui32(uncompressed_buf);
    offset += sizeof(uint32_t);
    col.notnull_bitmap.resize((data_num + 7) / 8);
    for (size_t i = 0; i < col.notnull_bitmap.size(); i++) {
        col.notnull_bitmap[i] = *(uncompressed_buf + offset);
        offset++;
    }
    col.cur_value_index = -1;

    char* value_buf = uncompressed_buf + offset;
    uint32_t value_buf_size = uncompressed_size - offset;
    col.decoder->reset();
    col.in.wrap_from(value_buf, value_buf_size);
    return ret;
}

int AlignedChunkReader::decode_time_value_buf_into_tsblock_multi(
    TsBlock*& ret_tsblock, Filter* filter, PageArena* pa) {
    int ret = E_OK;
    RowAppender row_appender(ret_tsblock);
    ret = multi_DECODE_TV_BATCH(ret_tsblock, row_appender, filter, pa);

    // Release uncompressed buffers if pages are done
    if (ret != E_OVERFLOW) {
        if (time_uncompressed_buf_ != nullptr) {
            time_compressor_->after_uncompress(time_uncompressed_buf_);
            time_uncompressed_buf_ = nullptr;
        }
        for (auto* col : value_columns_) {
            if (col->uncompressed_buf != nullptr) {
                col->compressor->after_uncompress(col->uncompressed_buf);
                col->uncompressed_buf = nullptr;
            }
            if (!(col->decoder && col->decoder->has_remaining(col->in)) &&
                !col->in.has_remaining()) {
                col->in.reset();
            }
            col->notnull_bitmap.clear();
            col->notnull_bitmap.shrink_to_fit();
        }
        if (!prev_time_page_not_finish()) {
            time_in_.reset();
        }
    } else {
        ret = E_OK;
    }
    return ret;
}

int AlignedChunkReader::multi_DECODE_TV_BATCH(TsBlock* ret_tsblock,
                                              RowAppender& row_appender,
                                              Filter* filter, PageArena* pa) {
    int ret = E_OK;
    const int BATCH = 129;
    int64_t times[BATCH];
    const uint32_t null_mask_base = 1 << 7;
    const uint32_t num_cols = value_columns_.size();

    while (time_decoder_->has_remaining(time_in_)) {
        if (row_appender.remaining() < (uint32_t)BATCH) {
            ret = E_OVERFLOW;
            break;
        }

        // ── Phase 1: Decode a batch of timestamps ──
        int time_count = 0;
        if (RET_FAIL(time_decoder_->read_batch_int64(times, BATCH, time_count,
                                                     time_in_))) {
            break;
        }
        if (time_count == 0) break;

        // ── Phase 2: Apply time filter ──
        bool time_mask[BATCH];
        bool block_all_pass = (filter == nullptr);
        int pass_count = time_count;
        if (!block_all_pass) {
            pass_count =
                filter->satisfy_batch_time(times, time_count, time_mask);
        }

        // ── Phase 3: Per-column null check + value decode ──
        // For each column, compute null flags and decode non-null values.
        // We store decoded values in column-specific buffers.
        // Max 8 bytes per value, 129 values per batch.
        struct ColBatch {
            bool is_null[BATCH];
            int nonnull_count;
            // Value buffer — up to 129 * 8 bytes = 1032 bytes on stack
            char val_buf[BATCH * 8];
            int val_count;
        };
        // Allocate on heap if many columns, stack for small counts
        std::vector<ColBatch> col_batches(num_cols);

        for (uint32_t c = 0; c < num_cols; c++) {
            auto* col = value_columns_[c];
            auto& cb = col_batches[c];
            cb.nonnull_count = 0;
            cb.val_count = 0;
            for (int i = 0; i < time_count; i++) {
                int vi = col->cur_value_index + 1 + i;
                if (col->notnull_bitmap.empty() ||
                    ((col->notnull_bitmap[vi / 8] & 0xFF) &
                     (null_mask_base >> (vi % 8))) == 0) {
                    cb.is_null[i] = true;
                } else {
                    cb.is_null[i] = false;
                    cb.nonnull_count++;
                }
            }

            // Skip values if no rows pass time filter
            if (pass_count == 0 && cb.nonnull_count > 0) {
                switch (col->chunk_header.data_type_) {
                    case common::BOOLEAN: {
                        // Booleans are 1 byte each; skip by reading and
                        // discarding
                        for (int s = 0; s < cb.nonnull_count; s++) {
                            bool dummy;
                            col->decoder->read_boolean(dummy, col->in);
                        }
                        break;
                    }
                    case common::INT32:
                    case common::DATE: {
                        int sk = 0;
                        col->decoder->skip_int32(cb.nonnull_count, sk, col->in);
                        break;
                    }
                    case common::INT64:
                    case common::TIMESTAMP: {
                        int sk = 0;
                        col->decoder->skip_int64(cb.nonnull_count, sk, col->in);
                        break;
                    }
                    case common::FLOAT: {
                        int sk = 0;
                        col->decoder->skip_float(cb.nonnull_count, sk, col->in);
                        break;
                    }
                    case common::DOUBLE: {
                        int sk = 0;
                        col->decoder->skip_double(cb.nonnull_count, sk,
                                                  col->in);
                        break;
                    }
                    default:
                        // STRING etc - fall through to value decode
                        break;
                }
                cb.nonnull_count = 0;  // already skipped
            }

            // Decode non-null values
            if (cb.nonnull_count > 0) {
                switch (col->chunk_header.data_type_) {
                    case common::BOOLEAN: {
                        bool* out = reinterpret_cast<bool*>(cb.val_buf);
                        cb.val_count = 0;
                        for (int s = 0; s < cb.nonnull_count; s++) {
                            bool v;
                            if (col->decoder->read_boolean(v, col->in) !=
                                common::E_OK)
                                break;
                            out[cb.val_count++] = v;
                        }
                        break;
                    }
                    case common::INT32:
                    case common::DATE:
                        col->decoder->read_batch_int32(
                            reinterpret_cast<int32_t*>(cb.val_buf),
                            cb.nonnull_count, cb.val_count, col->in);
                        break;
                    case common::INT64:
                    case common::TIMESTAMP:
                        col->decoder->read_batch_int64(
                            reinterpret_cast<int64_t*>(cb.val_buf),
                            cb.nonnull_count, cb.val_count, col->in);
                        break;
                    case common::FLOAT:
                        col->decoder->read_batch_float(
                            reinterpret_cast<float*>(cb.val_buf),
                            cb.nonnull_count, cb.val_count, col->in);
                        break;
                    case common::DOUBLE:
                        col->decoder->read_batch_double(
                            reinterpret_cast<double*>(cb.val_buf),
                            cb.nonnull_count, cb.val_count, col->in);
                        break;
                    default:
                        // STRING handled below in scatter loop
                        break;
                }
            }
        }

        // ── Phase 4: Skip if no rows pass ──
        if (pass_count == 0) {
            for (uint32_t c = 0; c < num_cols; c++) {
                value_columns_[c]->cur_value_index += time_count;
            }
            continue;
        }

        // ── Phase 5: Scatter into TsBlock ──

        // Fast path: all rows pass filter AND all columns have no nulls
        // → batch memcpy directly into Vector buffers.
        if (pass_count == time_count) {
            bool all_nonnull = true;
            for (uint32_t c = 0; c < num_cols; c++) {
                if (col_batches[c].nonnull_count != time_count) {
                    all_nonnull = false;
                    break;
                }
            }
            if (all_nonnull) {
                // Batch append time column
                common::Vector* time_vec = ret_tsblock->get_vector(0);
                time_vec->get_value_data().append_fixed_value(
                    (const char*)times,
                    static_cast<uint32_t>(time_count) * sizeof(int64_t));
                // Batch append each value column
                for (uint32_t c = 0; c < num_cols; c++) {
                    auto& cb = col_batches[c];
                    auto* col = value_columns_[c];
                    uint32_t elem_size = common::get_data_type_size(
                        col->chunk_header.data_type_);
                    common::Vector* vec = ret_tsblock->get_vector(c + 1);
                    vec->get_value_data().append_fixed_value(
                        cb.val_buf,
                        static_cast<uint32_t>(cb.val_count) * elem_size);
                    col->cur_value_index += time_count;
                }
                row_appender.add_rows(static_cast<uint32_t>(time_count));
                continue;
            }
        }

        // Slow path: per-row scatter (has filter or has nulls)
        std::vector<int> val_idx(num_cols, 0);

        for (int i = 0; i < time_count; i++) {
            bool passes = block_all_pass || time_mask[i];

            if (!passes) {
                for (uint32_t c = 0; c < num_cols; c++) {
                    value_columns_[c]->cur_value_index++;
                    if (!col_batches[c].is_null[i]) val_idx[c]++;
                }
                continue;
            }

            if (UNLIKELY(!row_appender.add_row())) {
                ret = E_OVERFLOW;
                break;
            }

            row_appender.append(0, (char*)&times[i], sizeof(int64_t));

            for (uint32_t c = 0; c < num_cols; c++) {
                value_columns_[c]->cur_value_index++;
                auto& cb = col_batches[c];
                auto* col = value_columns_[c];

                if (cb.is_null[i]) {
                    row_appender.append_null(c + 1);
                } else {
                    uint32_t elem_size = common::get_data_type_size(
                        col->chunk_header.data_type_);
                    row_appender.append(
                        c + 1, cb.val_buf + val_idx[c] * elem_size, elem_size);
                    val_idx[c]++;
                }
            }
        }
        if (ret != E_OK) break;
    }
    return ret;
}

// ═══════════════════════════════════════════════════════════════════════════
// Chunk-level parallel decode
// ═══════════════════════════════════════════════════════════════════════════

void AlignedChunkReader::cleanup_chunk_decode() {
    for (size_t c = 0; c < chunk_cols_.size(); c++) {
        for (auto& cp : chunk_cols_[c]) {
            if (cp.uncompressed_buf) {
                common::mem_free(cp.uncompressed_buf);
                cp.uncompressed_buf = nullptr;
            }
        }
    }
    chunk_pages_.clear();
    chunk_times_.clear();
    chunk_cols_.clear();
    chunk_page_cursor_ = 0;
    chunk_level_active_ = false;
}

// Phase 1: Scan ALL page headers, classify by time filter, record file
// offsets for non-SKIP pages.  Advances ALL pages' visit offsets so the
// chunk is fully consumed after scan.
int AlignedChunkReader::scan_chunk_pages(Filter* filter) {
    int ret = E_OK;
    const uint32_t num_cols = value_columns_.size();
    chunk_pages_.clear();

    while (IS_SUCC(ret)) {
        if (time_chunk_visit_offset_ - time_chunk_header_.serialized_size_ >=
            time_chunk_header_.data_size_)
            break;

        // Read time page header.
        if (RET_FAIL(get_cur_page_header(
                time_chunk_meta_, time_in_stream_, cur_time_page_header_,
                time_chunk_visit_offset_, time_chunk_header_)))
            break;
        if (cur_time_page_header_.compressed_size_ == 0 &&
            cur_time_page_header_.uncompressed_size_ == 0)
            break;

        // Read value page headers (need sizes for offset tracking).
        for (size_t c = 0; c < num_cols && IS_SUCC(ret); c++) {
            auto* col = value_columns_[c];
            if (RET_FAIL(get_cur_page_header(
                    col->chunk_meta, col->in_stream, col->cur_page_header,
                    col->chunk_visit_offset, col->chunk_header,
                    &col->file_data_buf_size))) {
            }
        }
        if (IS_FAIL(ret)) break;

        // Classify by time statistics.
        Statistic* stat = cur_time_page_header_.statistic_;
        PagePassType pt;
        if (filter == nullptr || stat == nullptr) {
            pt = PagePassType::FULL_PASS;
        } else if (!filter->satisfy(stat)) {
            pt = PagePassType::SKIP;
        } else if (filter->contain_start_end_time(stat->start_time_,
                                                  stat->end_time_)) {
            pt = PagePassType::FULL_PASS;
        } else {
            pt = PagePassType::BOUNDARY;
        }

        // Record info for non-SKIP pages BEFORE advancing.
        if (pt != PagePassType::SKIP) {
            ChunkPageInfo info;
            info.pass_type = pt;
            info.time_file_offset = time_chunk_meta_->offset_of_chunk_header_ +
                                    time_chunk_visit_offset_;
            info.time_compressed_size = cur_time_page_header_.compressed_size_;
            info.time_uncompressed_size =
                cur_time_page_header_.uncompressed_size_;
            info.value_file_offsets.resize(num_cols);
            info.value_compressed_sizes.resize(num_cols);
            info.value_uncompressed_sizes.resize(num_cols);
            for (size_t c = 0; c < num_cols; c++) {
                auto* col = value_columns_[c];
                info.value_file_offsets[c] =
                    col->chunk_meta->offset_of_chunk_header_ +
                    col->chunk_visit_offset;
                info.value_compressed_sizes[c] =
                    col->cur_page_header.compressed_size_;
                info.value_uncompressed_sizes[c] =
                    col->cur_page_header.uncompressed_size_;
            }
            chunk_pages_.push_back(std::move(info));
        }

        // Advance ALL pages (SKIP and non-SKIP) to keep offsets aligned.
        time_chunk_visit_offset_ += cur_time_page_header_.compressed_size_;
        time_in_stream_.wrapped_buf_advance_read_pos(
            cur_time_page_header_.compressed_size_);
        for (size_t c = 0; c < num_cols; c++) {
            auto* col = value_columns_[c];
            col->chunk_visit_offset += col->cur_page_header.compressed_size_;
            col->in_stream.wrapped_buf_advance_read_pos(
                col->cur_page_header.compressed_size_);
        }
    }

    const size_t np = chunk_pages_.size();
    chunk_times_.resize(np);
    chunk_cols_.resize(num_cols);
    for (uint32_t c = 0; c < num_cols; c++) chunk_cols_[c].resize(np);
    chunk_page_cursor_ = 0;
    return ret;
}

// Phase 2: Decode non-SKIP pages.  Each column reads its own data from
// the recorded file offsets using pread — no shared buffers.
int AlignedChunkReader::decode_chunk_pages() {
    int ret = E_OK;
    const size_t np = chunk_pages_.size();
    const uint32_t num_cols = value_columns_.size();
    if (np == 0) return ret;

    // ── Helper: read compressed data from file offset into a local buf ──
    auto file_read_page = [&](int64_t offset, uint32_t size, char* stack,
                              uint32_t stack_sz, char*& out,
                              bool& heap) -> int {
        heap = size > stack_sz;
        out =
            heap ? (char*)common::mem_alloc(size, common::MOD_DEFAULT) : stack;
        if (!out) return common::E_OOM;
        int rlen = 0;
        return read_file_->read(offset, out, size, rlen);
    };

    // ── Time column (serial — single column, lightweight) ──
    for (size_t p = 0; p < np; p++) {
        auto& info = chunk_pages_[p];
        auto& td = chunk_times_[p];
        td.count = 0;
        td.cursor = 0;
        if (info.time_compressed_size == 0) continue;

        char stk[4096];
        char* cbuf;
        bool heap;
        if (RET_FAIL(file_read_page(info.time_file_offset,
                                    info.time_compressed_size, stk, sizeof(stk),
                                    cbuf, heap)))
            return ret;

        char* ub = nullptr;
        uint32_t us = 0;
        time_compressor_->reset(false);
        int r = time_compressor_->uncompress(cbuf, info.time_compressed_size,
                                             ub, us);
        if (heap && cbuf != ub) common::mem_free(cbuf);
        if (r != E_OK || us != info.time_uncompressed_size) {
            if (ub) time_compressor_->after_uncompress(ub);
            return E_TSFILE_CORRUPTED;
        }

        common::ByteStream ts_in;
        ts_in.wrap_from(ub, us);
        time_decoder_->reset();
        td.times.clear();
        const int BS = 1024;
        int64_t buf[BS];
        while (time_decoder_->has_remaining(ts_in)) {
            int actual = 0;
            time_decoder_->read_batch_int64(buf, BS, actual, ts_in);
            if (actual == 0) break;
            td.times.insert(td.times.end(), buf, buf + actual);
        }
        td.count = (int)td.times.size();
        time_compressor_->after_uncompress(ub);
    }

    // ── Value column decode lambda (one per column) ──
    auto decode_val_col = [&](uint32_t c) -> int {
        auto* col = value_columns_[c];
        for (size_t p = 0; p < np; p++) {
            auto& info = chunk_pages_[p];
            auto& cp = chunk_cols_[c][p];
            cp.data_num = 0;
            cp.nonnull_count = 0;
            cp.read_pos = 0;
            cp.uncompressed_buf = nullptr;
            uint32_t csz = info.value_compressed_sizes[c];
            if (csz == 0) continue;

            char stk[4096];
            char* cbuf;
            bool heap;
            int r = E_OK;
            {
                heap = csz > sizeof(stk);
                cbuf = heap ? (char*)common::mem_alloc(csz, common::MOD_DEFAULT)
                            : stk;
                if (!cbuf) return common::E_OOM;
                int rlen = 0;
                r = read_file_->read(info.value_file_offsets[c], cbuf, csz,
                                     rlen);
            }
            if (r != E_OK) {
                if (heap) common::mem_free(cbuf);
                return r;
            }

            char* ub = nullptr;
            uint32_t us = 0;
            col->compressor->reset(false);
            r = col->compressor->uncompress(cbuf, csz, ub, us);
            if (heap && cbuf != ub) common::mem_free(cbuf);
            if (r != E_OK || us != info.value_uncompressed_sizes[c]) {
                if (ub) col->compressor->after_uncompress(ub);
                return E_TSFILE_CORRUPTED;
            }
            cp.uncompressed_buf = ub;

            // Parse bitmap.
            uint32_t off = 0;
            uint32_t data_num = SerializationUtil::read_ui32(ub);
            off += sizeof(uint32_t);
            cp.data_num = data_num;
            cp.bitmap.resize((data_num + 7) / 8);
            for (size_t i = 0; i < cp.bitmap.size(); i++)
                cp.bitmap[i] = *(ub + off++);

            // Pre-decode fixed-length values.
            char* vbuf = ub + off;
            uint32_t vsize = us - off;
            col->decoder->reset();
            common::ByteStream vi;
            vi.wrap_from(vbuf, vsize);

            auto dt = col->chunk_header.data_type_;
            if (dt == common::STRING || dt == common::TEXT ||
                dt == common::BLOB) {
                cp.nonnull_count = 0;
                continue;
            }
            const uint32_t nmb = 1 << 7;
            int nn = 0;
            for (uint32_t i = 0; i < data_num; i++)
                if (!cp.bitmap.empty() &&
                    ((cp.bitmap[i / 8] & 0xFF) & (nmb >> (i % 8))) != 0)
                    nn++;
            if (nn == 0) {
                cp.nonnull_count = 0;
                continue;
            }
            uint32_t es = common::get_data_type_size(dt);
            cp.values.resize((size_t)nn * es);
            cp.nonnull_count = 0;
            switch (dt) {
                case common::BOOLEAN: {
                    bool* out = reinterpret_cast<bool*>(cp.values.data());
                    for (int s = 0; s < nn; s++) {
                        bool v;
                        if (col->decoder->read_boolean(v, vi) != E_OK) break;
                        out[cp.nonnull_count++] = v;
                    }
                    break;
                }
                case common::INT32:
                case common::DATE:
                    col->decoder->read_batch_int32(
                        reinterpret_cast<int32_t*>(cp.values.data()), nn,
                        cp.nonnull_count, vi);
                    break;
                case common::INT64:
                case common::TIMESTAMP:
                    col->decoder->read_batch_int64(
                        reinterpret_cast<int64_t*>(cp.values.data()), nn,
                        cp.nonnull_count, vi);
                    break;
                case common::FLOAT:
                    col->decoder->read_batch_float(
                        reinterpret_cast<float*>(cp.values.data()), nn,
                        cp.nonnull_count, vi);
                    break;
                case common::DOUBLE:
                    col->decoder->read_batch_double(
                        reinterpret_cast<double*>(cp.values.data()), nn,
                        cp.nonnull_count, vi);
                    break;
                default:
                    break;
            }
        }
        return E_OK;
    };

#ifdef ENABLE_THREADS
    if (decode_pool_ != nullptr) {
        std::vector<int> col_rets(num_cols, E_OK);
        for (uint32_t c = 0; c < num_cols; c++)
            decode_pool_->submit([&, c]() { col_rets[c] = decode_val_col(c); });
        decode_pool_->wait_all();
        for (uint32_t c = 0; c < num_cols; c++)
            if (col_rets[c] != E_OK) return col_rets[c];
        return ret;
    }
#endif
    for (uint32_t c = 0; c < num_cols && IS_SUCC(ret); c++)
        ret = decode_val_col(c);
    return ret;
}

// Phase 3: Scatter predecoded chunk data into TsBlock.
int AlignedChunkReader::scatter_chunk_pages(TsBlock* ret_tsblock,
                                            RowAppender& row_appender,
                                            Filter* filter, PageArena* pa) {
    int ret = E_OK;
    const uint32_t null_mask_base = 1 << 7;
    const uint32_t num_cols = value_columns_.size();
    const size_t np = chunk_pages_.size();

    while ((size_t)chunk_page_cursor_ < np) {
        auto& td = chunk_times_[chunk_page_cursor_];
        if (td.cursor >= td.count) {
            chunk_page_cursor_++;
            continue;
        }
        auto& info = chunk_pages_[chunk_page_cursor_];

        // Decide: fast bulk path or row-by-row.
        bool need_filter = (info.pass_type == PagePassType::BOUNDARY);
        bool can_bulk = !need_filter;
        if (can_bulk) {
            for (uint32_t c = 0; c < num_cols && can_bulk; c++) {
                auto& cp = chunk_cols_[c][chunk_page_cursor_];
                auto dt = value_columns_[c]->chunk_header.data_type_;
                if (dt == common::STRING || dt == common::TEXT ||
                    dt == common::BLOB)
                    can_bulk = false;
                else if (cp.data_num == 0)
                    can_bulk = false;
                else if (cp.nonnull_count != (int)cp.data_num)
                    can_bulk = false;
            }
        }

        if (can_bulk) {
            // ★ Bulk path: FULL_PASS, no nulls, all fixed-length.
            while (td.cursor < td.count) {
                int avail = (int)row_appender.remaining();
                if (avail <= 0) return E_OVERFLOW;
                int batch = std::min(td.count - td.cursor, avail);

                ret_tsblock->get_vector(0)->get_value_data().append_fixed_value(
                    (const char*)&td.times[td.cursor],
                    static_cast<uint32_t>(batch) * sizeof(int64_t));
                for (uint32_t c = 0; c < num_cols; c++) {
                    auto& cp = chunk_cols_[c][chunk_page_cursor_];
                    uint32_t es = common::get_data_type_size(
                        value_columns_[c]->chunk_header.data_type_);
                    ret_tsblock->get_vector(c + 1)
                        ->get_value_data()
                        .append_fixed_value(
                            cp.values.data() +
                                static_cast<size_t>(cp.read_pos) * es,
                            static_cast<uint32_t>(batch) * es);
                    cp.read_pos += batch;
                }
                row_appender.add_rows(static_cast<uint32_t>(batch));
                td.cursor += batch;
            }
        } else {
            // ★ Row-by-row path: handles filter, nulls, varlen.
            while (td.cursor < td.count) {
                if (row_appender.remaining() == 0) return E_OVERFLOW;
                int64_t t = td.times[td.cursor];

                if (need_filter && filter != nullptr &&
                    !filter->satisfy_start_end_time(t, t)) {
                    // Skip row — advance value cursors for non-null entries.
                    for (uint32_t c = 0; c < num_cols; c++) {
                        auto& cp = chunk_cols_[c][chunk_page_cursor_];
                        if (cp.data_num > 0 && !cp.bitmap.empty()) {
                            int vi = td.cursor;
                            if ((cp.bitmap[vi / 8] & 0xFF) &
                                (null_mask_base >> (vi % 8)))
                                cp.read_pos++;
                        }
                    }
                    td.cursor++;
                    continue;
                }

                if (UNLIKELY(!row_appender.add_row())) return E_OVERFLOW;
                row_appender.append(0, (char*)&t, sizeof(int64_t));

                for (uint32_t c = 0; c < num_cols; c++) {
                    auto& cp = chunk_cols_[c][chunk_page_cursor_];
                    int vi = td.cursor;
                    bool is_null = true;
                    if (cp.data_num > 0 && !cp.bitmap.empty()) {
                        is_null = ((cp.bitmap[vi / 8] & 0xFF) &
                                   (null_mask_base >> (vi % 8))) == 0;
                    }
                    if (is_null) {
                        row_appender.append_null(c + 1);
                    } else {
                        uint32_t es = common::get_data_type_size(
                            value_columns_[c]->chunk_header.data_type_);
                        row_appender.append(
                            c + 1,
                            cp.values.data() +
                                static_cast<size_t>(cp.read_pos) * es,
                            es);
                        cp.read_pos++;
                    }
                }
                td.cursor++;
            }
        }
        chunk_page_cursor_++;
    }
    return ret;
}

}  // end namespace storage
