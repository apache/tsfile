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
    time_compressor_ = nullptr;
    time_filter_ = time_filter;
    time_uncompressed_buf_ = nullptr;
    if (IS_NULL(time_decoder_)) {
        return E_OOM;
    }
    return E_OK;
}

void AlignedChunkReader::reset() {
    time_chunk_meta_ = nullptr;
    time_chunk_header_.reset();
    cur_time_page_header_.reset();

    char* file_data_buf = time_in_stream_.get_wrapped_buf();
    if (file_data_buf != nullptr) {
        mem_free(file_data_buf);
    }
    time_in_stream_.clear_wrapped_buf();
    time_in_stream_.reset();
    file_data_time_buf_size_ = 0;
    time_chunk_visit_offset_ = 0;

    // Free leftover uncompressed buffers from the previous chunk.
    if (time_uncompressed_buf_ != nullptr && time_compressor_ != nullptr) {
        time_compressor_->after_uncompress(time_uncompressed_buf_);
        time_uncompressed_buf_ = nullptr;
    }

    for (auto* col : value_columns_) {
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
    }
    cleanup_chunk_decode();
}

void AlignedChunkReader::destroy() {
    cleanup_chunk_decode();
    if (time_uncompressed_buf_ != nullptr && time_compressor_ != nullptr) {
        time_compressor_->after_uncompress(time_uncompressed_buf_);
        time_uncompressed_buf_ = nullptr;
    }
    if (time_decoder_ != nullptr) {
        time_decoder_->~Decoder();
        DecoderFactory::free(time_decoder_);
        time_decoder_ = nullptr;
    }
    if (time_compressor_ != nullptr) {
        time_compressor_->~Compressor();
        CompressorFactory::free(time_compressor_);
        time_compressor_ = nullptr;
    }
    char* buf = time_in_stream_.get_wrapped_buf();
    if (buf != nullptr) {
        mem_free(buf);
        time_in_stream_.clear_wrapped_buf();
    }
    cur_time_page_header_.reset();
    chunk_header_.~ChunkHeader();

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
        buf = col->in_stream.get_wrapped_buf();
        if (buf != nullptr) {
            mem_free(buf);
            col->in_stream.clear_wrapped_buf();
        }
        col->cur_page_header.reset();
        delete col;
    }
    value_columns_.clear();
#ifdef ENABLE_THREADS
    decode_pool_ = nullptr;  // borrowed, not owned
#endif
}

int AlignedChunkReader::load_by_aligned_meta(ChunkMeta* time_chunk_meta,
                                             ChunkMeta* value_chunk_meta) {
    std::vector<ChunkMeta*> value_metas = {value_chunk_meta};
    return load_by_aligned_meta_multi(time_chunk_meta, value_metas);
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
    return get_next_page_multi(ret_tsblock, oneshoot_filter, pa);
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
            int32_t& file_data_buf_size = override_buf_size != nullptr
                                              ? *override_buf_size
                                              : file_data_time_buf_size_;
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

int AlignedChunkReader::get_next_page(TsBlock* ret_tsblock,
                                      Filter* oneshoot_filter, PageArena& pa,
                                      int64_t min_time_hint, int& row_offset,
                                      int& row_limit) {
    if (row_limit == 0) {
        return E_NO_MORE_DATA;
    }
    return get_next_page_multi(ret_tsblock, oneshoot_filter, pa);
}

int AlignedChunkReader::load_by_aligned_meta_multi(
    ChunkMeta* time_chunk_meta, const std::vector<ChunkMeta*>& value_metas) {
    int ret = E_OK;
    time_chunk_meta_ = time_chunk_meta;

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

int AlignedChunkReader::get_next_page_multi(TsBlock* ret_tsblock,
                                            Filter* oneshoot_filter,
                                            PageArena& pa) {
    int ret = E_OK;
    Filter* filter =
        (oneshoot_filter != nullptr ? oneshoot_filter : time_filter_);

    // Resume chunk-level scatter from previous E_OVERFLOW.
    if (chunk_level_active_) {
        RowAppender row_appender(ret_tsblock);
        ret = scatter_chunk_pages(ret_tsblock, row_appender, filter, &pa);
        if (ret != E_OVERFLOW) {
            cleanup_chunk_decode();
        } else {
            ret = E_OK;
        }
        return ret;
    }

#ifdef ENABLE_THREADS
    // Chunk-level parallel path for multi-page compressed chunks.
    if (decode_pool_ != nullptr && value_columns_.size() > 1 &&
        !chunk_has_only_one_page(time_chunk_header_) &&
        time_chunk_header_.compression_type_ != common::UNCOMPRESSED) {
        ret = scan_chunk_pages(filter);
        if (IS_FAIL(ret)) return ret;
        if (chunk_pages_.empty()) return E_NO_MORE_DATA;

        ret = decode_chunk_pages();
        if (IS_FAIL(ret)) {
            cleanup_chunk_decode();
            return ret;
        }

        chunk_level_active_ = true;
        RowAppender row_appender(ret_tsblock);
        ret = scatter_chunk_pages(ret_tsblock, row_appender, filter, &pa);
        if (ret != E_OVERFLOW) {
            cleanup_chunk_decode();
        } else {
            ret = E_OK;
        }
        return ret;
    }
#endif

    // Serial fallback.
    return get_next_page_multi_serial(ret_tsblock, filter, pa);
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
        struct ColBatch {
            bool is_null[BATCH];
            int nonnull_count;
            char val_buf[BATCH * 8];
            int val_count;
        };
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
                        break;
                }
                cb.nonnull_count = 0;
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
        if (pass_count == time_count) {
            bool all_nonnull = true;
            for (uint32_t c = 0; c < num_cols; c++) {
                if (col_batches[c].nonnull_count != time_count) {
                    all_nonnull = false;
                    break;
                }
            }
            if (all_nonnull) {
                common::Vector* time_vec = ret_tsblock->get_vector(0);
                time_vec->get_value_data().append_fixed_value(
                    (const char*)times,
                    static_cast<uint32_t>(time_count) * sizeof(int64_t));
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

        // Slow path: per-row scatter
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

int AlignedChunkReader::scan_chunk_pages(Filter* filter) {
    int ret = E_OK;
    const uint32_t num_cols = value_columns_.size();
    chunk_pages_.clear();

    while (IS_SUCC(ret)) {
        if (time_chunk_visit_offset_ - time_chunk_header_.serialized_size_ >=
            time_chunk_header_.data_size_)
            break;

        if (RET_FAIL(get_cur_page_header(
                time_chunk_meta_, time_in_stream_, cur_time_page_header_,
                time_chunk_visit_offset_, time_chunk_header_)))
            break;
        if (cur_time_page_header_.compressed_size_ == 0 &&
            cur_time_page_header_.uncompressed_size_ == 0)
            break;

        for (size_t c = 0; c < num_cols && IS_SUCC(ret); c++) {
            auto* col = value_columns_[c];
            if (RET_FAIL(get_cur_page_header(
                    col->chunk_meta, col->in_stream, col->cur_page_header,
                    col->chunk_visit_offset, col->chunk_header,
                    &col->file_data_buf_size))) {
            }
        }
        if (IS_FAIL(ret)) break;

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

int AlignedChunkReader::decode_chunk_pages() {
    int ret = E_OK;
    const size_t np = chunk_pages_.size();
    const uint32_t num_cols = value_columns_.size();
    if (np == 0) return ret;

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

    // ── Time column (serial) ──
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

    // ── Value column decode lambda ──
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

            uint32_t off = 0;
            uint32_t data_num = SerializationUtil::read_ui32(ub);
            off += sizeof(uint32_t);
            cp.data_num = data_num;
            cp.bitmap.resize((data_num + 7) / 8);
            for (size_t i = 0; i < cp.bitmap.size(); i++)
                cp.bitmap[i] = *(ub + off++);

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
            while (td.cursor < td.count) {
                if (row_appender.remaining() == 0) return E_OVERFLOW;
                int64_t t = td.times[td.cursor];

                if (need_filter && filter != nullptr &&
                    !filter->satisfy_start_end_time(t, t)) {
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