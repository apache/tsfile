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
#include <type_traits>

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
    if (value_uncompressed_buf_ != nullptr && value_compressor_ != nullptr) {
        value_compressor_->after_uncompress(value_uncompressed_buf_);
        value_uncompressed_buf_ = nullptr;
    }
    time_in_.reset();
    value_in_.reset();

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
        for (auto& pps : col->per_page_state) {
            pps.predecode_pa.destroy();
        }
        col->per_page_state.clear();
        col->pending_decoded_values.clear();
        col->pending_decoded_count = 0;
        col->pending_decoded_cursor = 0;
        col->pending_decoded = false;
        // Note: decoder/compressor are NOT freed here — they are reused by
        // alloc_compressor_and_decoder() in load_by_aligned_meta_multi().
    }
    release_current_page_state();
    chunk_pages_.clear();
    per_page_times_.clear();
}

void AlignedChunkReader::destroy() {
    // .clear() leaves the vector's internal heap buffer allocated, which
    // mem_free can't reach because we placement-new the reader. swap with
    // an empty vector to actually release the backing storage so ASan's
    // LeakSanitizer doesn't flag the (rather large) ChunkPageInfo buffers.
    std::vector<ChunkPageInfo>{}.swap(chunk_pages_);
    std::vector<int64_t>{}.swap(page_all_times_);
    if (time_uncompressed_buf_ != nullptr && time_compressor_ != nullptr) {
        time_compressor_->after_uncompress(time_uncompressed_buf_);
        time_uncompressed_buf_ = nullptr;
    }
    if (value_uncompressed_buf_ != nullptr && value_compressor_ != nullptr) {
        value_compressor_->after_uncompress(value_uncompressed_buf_);
        value_uncompressed_buf_ = nullptr;
    }
    // Multi-value readers keep the current page's decompressed buffers in
    // ValueColumnState. Release them while the column compressors are still
    // alive; the columns are deleted below and cannot release them afterwards.
    release_current_page_state();
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
        for (auto& pps : col->per_page_state) {
            pps.predecode_pa.destroy();
        }
        col->per_page_state.clear();
        col->pending_decoded_values.clear();
        buf = col->in_stream.get_wrapped_buf();
        if (buf != nullptr) {
            mem_free(buf);
            col->in_stream.clear_wrapped_buf();
        }
        col->cur_page_header.reset();
        delete col;
    }
    // This reader is placement-new'd and torn down via destroy() + mem_free
    // without ever running ~AlignedChunkReader (see
    // TsFileSeriesScanIterator::destroy), so .clear() would leave these
    // vectors' backing buffers allocated and unreachable.  swap with an empty
    // vector to actually release the storage, matching the chunk_pages_ /
    // page_all_times_ handling above.
    std::vector<ValueColumnState*>().swap(value_columns_);
    std::vector<std::vector<int64_t>>().swap(per_page_times_);
#ifdef ENABLE_THREADS
    decode_pool_ = nullptr;  // borrowed, not owned
    for (auto* d : time_decoder_pool_) {
        if (d != nullptr) {
            d->~Decoder();
            DecoderFactory::free(d);
        }
    }
    std::vector<Decoder*>().swap(time_decoder_pool_);
    for (auto* c : time_compressor_pool_) {
        if (c != nullptr) {
            c->~Compressor();
            CompressorFactory::free(c);
        }
    }
    std::vector<Compressor*>().swap(time_compressor_pool_);
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
    if (!IS_SUCC(ret)) {
        mem_free(time_file_data_buf);
        return ret;
    }
    if (ret_read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
        ret = E_TSFILE_CORRUPTED;
        LOGE("file corrupted, ret=" << ret << ", offset="
                                    << time_chunk_meta_->offset_of_chunk_header_
                                    << "read_len=" << ret_read_len);
        mem_free(time_file_data_buf);
        return ret;
    }
    time_in_stream_.wrap_from(time_file_data_buf, ret_read_len);
    if (RET_FAIL(time_chunk_header_.deserialize_from(time_in_stream_))) {
        return ret;
    }
    time_chunk_visit_offset_ = time_in_stream_.read_pos();
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
    if (!IS_SUCC(ret)) {
        mem_free(value_file_data_buf);
        return ret;
    }
    if (ret_read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
        ret = E_TSFILE_CORRUPTED;
        LOGE("file corrupted, ret="
             << ret << ", offset=" << value_chunk_meta_->offset_of_chunk_header_
             << "read_len=" << ret_read_len);
        mem_free(value_file_data_buf);
        return ret;
    }
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
    return ret;
}

int AlignedChunkReader::alloc_compressor_and_decoder(
    storage::Decoder*& decoder, storage::Compressor*& compressor,
    TSEncoding encoding, TSDataType data_type, CompressionType compression) {
    // Invalid enum bytes indicate corrupted chunk metadata, not an allocator
    // failure.  Reject them before asking the factory for a decoder.
    if (encoding < common::PLAIN || encoding > common::CAMEL) {
        return E_TSFILE_CORRUPTED;
    }
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
        int row_offset = 0;
        return get_next_page_multi(ret_tsblock, oneshoot_filter, pa,
                                   row_offset);
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
            } else if (cur_page_may_satisfy_filter(filter)) {
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
        char* resized_buf = (char*)mem_realloc(file_data_buf, read_size);
        if (IS_NULL(resized_buf)) {
            return E_OOM;
        }
        file_data_buf = resized_buf;
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

// Page statistics provide two levels of certainty: "may satisfy" means the
// page cannot be ruled out, while "fully satisfies" means every row is known
// to match and the page count can safely be used for offset pushdown.
bool AlignedChunkReader::cur_page_may_satisfy_filter(Filter* filter) {
    bool value_satisfy = filter == nullptr ||
                         cur_value_page_header_.statistic_ == nullptr ||
                         filter->satisfy(cur_value_page_header_.statistic_);
    bool time_satisfy = filter == nullptr ||
                        cur_time_page_header_.statistic_ == nullptr ||
                        filter->satisfy(cur_time_page_header_.statistic_);
    return time_satisfy && value_satisfy;
}

bool AlignedChunkReader::cur_page_fully_satisfies_filter(Filter* filter) {
    Statistic* stat = cur_time_page_header_.statistic_;
    if (stat == nullptr) {
        stat = cur_value_page_header_.statistic_;
    }
    return filter == nullptr ||
           (stat != nullptr &&
            filter->contain_start_end_time(stat->start_time_, stat->end_time_));
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
                if (UNLIKELY(!row_appender.add_row())) {                       \
                    ret = E_OVERFLOW;                                          \
                    cur_value_index--;                                         \
                    break;                                                     \
                }                                                              \
                ret = time_decoder_->read_int64(time, time_in);                \
                if (ret != E_OK) {                                             \
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

namespace {
// Type-dispatched exact read / skip for decode_tv_batch<T>. Overload
// resolution on the value pointer selects the matching Decoder method.
FORCE_INLINE int read_value_exact_typed(Decoder* d, int32_t* out, int count,
                                        ByteStream& in) {
    return d->read_exact_int32(out, count, in);
}
FORCE_INLINE int read_value_exact_typed(Decoder* d, int64_t* out, int count,
                                        ByteStream& in) {
    return d->read_exact_int64(out, count, in);
}
FORCE_INLINE int read_value_exact_typed(Decoder* d, float* out, int count,
                                        ByteStream& in) {
    return d->read_exact_float(out, count, in);
}
FORCE_INLINE int read_value_exact_typed(Decoder* d, double* out, int count,
                                        ByteStream& in) {
    return d->read_exact_double(out, count, in);
}
FORCE_INLINE int skip_value_exact_typed(Decoder* d, int32_t*, int count,
                                        ByteStream& in) {
    return d->skip_exact_int32(count, in);
}
FORCE_INLINE int skip_value_exact_typed(Decoder* d, int64_t*, int count,
                                        ByteStream& in) {
    return d->skip_exact_int64(count, in);
}
FORCE_INLINE int skip_value_exact_typed(Decoder* d, float*, int count,
                                        ByteStream& in) {
    return d->skip_exact_float(count, in);
}
FORCE_INLINE int skip_value_exact_typed(Decoder* d, double*, int count,
                                        ByteStream& in) {
    return d->skip_exact_double(count, in);
}
}  // namespace

// Unified aligned time+value page decode for fixed-width value types
// (INT32/INT64/FLOAT/DOUBLE).  These differ only in the value array type, the
// typed read/skip calls (dispatched via the helpers above), and whether the
// per-value Filter::satisfy (which takes an int64 value) is applied — only
// integral value columns use it; float/double are filtered on time only.
template <typename T>
int AlignedChunkReader::decode_tv_batch(ByteStream& time_in,
                                        ByteStream& value_in,
                                        RowAppender& row_appender,
                                        Filter* filter) {
    int ret = E_OK;
    const int BATCH = 129;
    int64_t times[BATCH];
    T values[BATCH];
    const uint32_t null_mask_base = 1 << 7;

    while (time_decoder_->has_remaining(time_in)) {
        if (row_appender.remaining() < (uint32_t)BATCH) {
            ret = E_OVERFLOW;
            break;
        }

        // Block-level time filter check: skip entire block if out of range.
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
                        if (RET_FAIL(skip_value_exact_typed(
                                value_decoder_, values, nonnull, value_in))) {
                            break;
                        }
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
                if (RET_FAIL(skip_value_exact_typed(value_decoder_, values,
                                                    nonnull_count, value_in))) {
                    break;
                }
            }
            cur_value_index += time_count;
            continue;
        }

        if (nonnull_count > 0) {
            if (RET_FAIL(read_value_exact_typed(value_decoder_, values,
                                                nonnull_count, value_in))) {
                break;
            }
        }

        // Dense fixed-width batches are already laid out exactly as the two
        // destination vectors expect. Appending them row by row would issue
        // two tiny memcpy calls per row (time + value), plus virtual dispatch
        // and bookkeeping. Copy each column once instead. Integral value
        // filters still need the scalar satisfy(time, value) check unless the
        // decoder proved the whole block passes, so those batches retain the
        // fallback below.
        const bool needs_integral_value_filter =
            std::is_integral<T>::value && filter != nullptr && !block_all_pass;
        if (pass_count == time_count && nonnull_count == time_count &&
            !needs_integral_value_filter &&
            row_appender.can_bulk_append_fixed(0, sizeof(int64_t)) &&
            row_appender.can_bulk_append_fixed(1, sizeof(T))) {
            row_appender.bulk_append_fixed(0,
                                           reinterpret_cast<const char*>(times),
                                           static_cast<uint32_t>(time_count));
            row_appender.bulk_append_fixed(
                1, reinterpret_cast<const char*>(values),
                static_cast<uint32_t>(time_count));
            row_appender.add_rows(static_cast<uint32_t>(time_count));
            cur_value_index += time_count;
            continue;
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
                T val = values[val_idx++];
                // Per-value filter applies only to integral value columns;
                // Filter::satisfy takes an int64 value.  is_integral<T> is a
                // compile-time constant, so this branch is elided (and the
                // int64 cast never evaluated) for float/double.
                if (std::is_integral<T>::value && filter != nullptr &&
                    !block_all_pass &&
                    !filter->satisfy(times[i], static_cast<int64_t>(val))) {
                    continue;
                }
                if (UNLIKELY(!row_appender.add_row())) {
                    ret = E_OVERFLOW;
                    break;
                }
                row_appender.append(0, (char*)&times[i], sizeof(int64_t));
                row_appender.append(1, (char*)&val, sizeof(T));
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
            // Batch decode path: read_batch_int{32,64} consumes whole TS_2DIFF
            // blocks at once (and uses SIMD when ENABLE_SIMD); replaces a
            // per-value decode() loop that hot-dominated the read flame graph.
            ret = decode_tv_batch<int32_t>(time_in_, value_in_, row_appender,
                                           filter);
            break;
        case common::TIMESTAMP:
        case common::INT64:
            ret = decode_tv_batch<int64_t>(time_in_, value_in_, row_appender,
                                           filter);
            break;
        case common::FLOAT:
            ret = decode_tv_batch<float>(time_in_, value_in_, row_appender,
                                         filter);
            break;
        case common::DOUBLE:
            ret = decode_tv_batch<double>(time_in_, value_in_, row_appender,
                                          filter);
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
        if (row_limit == 0) {
            return E_NO_MORE_DATA;
        }
        // The multi-value path can consume offset through chunk/page count
        // statistics. Limit and min-time pushdown still need separate state
        // handling, so keep those combinations explicit instead of silently
        // returning a wider range.
        if (row_limit >= 0 ||
            min_time_hint != std::numeric_limits<int64_t>::min()) {
            return common::E_NOT_SUPPORT;
        }
        return get_next_page_multi(ret_tsblock, oneshoot_filter, pa,
                                   row_offset);
    }
    int ret = E_OK;
    Filter* filter =
        (oneshoot_filter != nullptr ? oneshoot_filter : time_filter_);

    if (row_limit == 0) {
        return E_NO_MORE_DATA;
    }

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
            } else if (!cur_page_may_satisfy_filter(filter)) {
                if (RET_FAIL(skip_cur_page())) {
                }
            } else if (should_skip_page_by_time(min_time_hint)) {
                if (RET_FAIL(skip_cur_page())) {
                }
            } else if (cur_page_fully_satisfies_filter(filter) &&
                       should_skip_page_by_offset(row_offset)) {
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
    if (!IS_SUCC(ret)) {
        mem_free(time_file_data_buf);
        return ret;
    }
    if (ret_read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
        ret = E_TSFILE_CORRUPTED;
        mem_free(time_file_data_buf);
        return ret;
    }
    time_in_stream_.wrap_from(time_file_data_buf, ret_read_len);
    if (RET_FAIL(time_chunk_header_.deserialize_from(time_in_stream_))) {
        return ret;
    }
    time_chunk_visit_offset_ = time_in_stream_.read_pos();

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
        if (!IS_SUCC(ret)) {
            mem_free(vbuf);
            return ret;
        }
        if (ret_read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
            ret = E_TSFILE_CORRUPTED;
            mem_free(vbuf);
            return ret;
        }
        col->in_stream.wrap_from(vbuf, ret_read_len);
        if (RET_FAIL(col->chunk_header.deserialize_from(col->in_stream))) {
            break;
        }
        col->chunk_visit_offset = col->in_stream.read_pos();
        if (RET_FAIL(alloc_compressor_and_decoder(
                col->decoder, col->compressor, col->chunk_header.encoding_type_,
                col->chunk_header.data_type_,
                col->chunk_header.compression_type_))) {
            break;
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
    return decode_time_page_with(page_info, out_times, time_decoder_,
                                 time_compressor_);
}

// Worker-safe variant: uses caller-provided decoder + compressor instead of
// the shared time_decoder_/time_compressor_ members.  Used by the parallel
// time-page decode dispatch in decode_all_planned_pages.
int AlignedChunkReader::decode_time_page_with(const ChunkPageInfo& page_info,
                                              std::vector<int64_t>& out_times,
                                              Decoder* decoder,
                                              Compressor* compressor) {
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
    // ReadFile::read() returns E_OK + short read_len on EOF; uncompressing
    // page_info.time_compressed_size from a buffer with uninitialised tail
    // bytes would feed garbage to the decompressor.
    if (read_len != static_cast<int32_t>(page_info.time_compressed_size)) {
        if (heap) common::mem_free(compressed_buf);
        return E_TSFILE_CORRUPTED;
    }

    char* uncompressed_buf = nullptr;
    uint32_t uncompressed_size = 0;
    if (RET_FAIL(compressor->reset(false))) {
        if (heap) common::mem_free(compressed_buf);
        return ret;
    }
    ret = compressor->uncompress(compressed_buf, page_info.time_compressed_size,
                                 uncompressed_buf, uncompressed_size);
    if (heap && compressed_buf != uncompressed_buf) {
        common::mem_free(compressed_buf);
    }
    if (IS_FAIL(ret) || uncompressed_size != page_info.time_uncompressed_size) {
        if (uncompressed_buf != nullptr) {
            compressor->after_uncompress(uncompressed_buf);
        }
        return E_TSFILE_CORRUPTED;
    }

    common::ByteStream in;
    in.wrap_from(uncompressed_buf, uncompressed_size);
    decoder->reset();
    const int batch_size = 1024;
    int64_t batch[batch_size];
    while (decoder->has_remaining(in)) {
        int actual = 0;
        if (RET_FAIL(
                decoder->read_batch_int64(batch, batch_size, actual, in))) {
            break;
        }
        if (actual == 0) {
            break;
        }
        out_times.insert(out_times.end(), batch, batch + actual);
    }
    compressor->after_uncompress(uncompressed_buf);
    return ret;
}

int AlignedChunkReader::build_page_plan(Filter* filter, int& row_offset) {
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
                    // Offset is defined on the filtered row stream. Consume
                    // matching rows only; holes inside a boundary page do not
                    // count toward it.
                    if (row_offset > 0) {
                        row_offset--;
                        continue;
                    }
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
            if (page_info.pass_type == PagePassType::FULL_PASS &&
                row_offset > 0) {
                const int32_t page_row_count =
                    page_info.row_end - page_info.row_begin;
                const int32_t rows_to_skip =
                    std::min(page_row_count, row_offset);
                page_info.row_begin += rows_to_skip;
                row_offset -= rows_to_skip;
                if (page_info.row_begin == page_info.row_end) {
                    page_info.pass_type = PagePassType::SKIP;
                }
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

    if (page_plan_built_) {
        per_page_times_.assign(chunk_pages_.size(), std::vector<int64_t>{});
        for (auto* col : value_columns_) {
            col->per_page_state.clear();
            col->per_page_state.resize(chunk_pages_.size());
        }
    }
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
        col->cur_value_index = -1;
        col->in.reset();
        for (auto& pps : col->per_page_state) {
            pps.predecode_pa.destroy();
        }
        col->per_page_state.clear();
        col->pending_decoded_values.clear();
        col->pending_decoded_count = 0;
        col->pending_decoded_cursor = 0;
        col->pending_decoded = false;
    }
    per_page_times_.clear();
    current_page_loaded_ = false;
}

int AlignedChunkReader::decode_value_page_for_slot(uint32_t col_idx,
                                                   size_t page_idx) {
    const ChunkPageInfo& page_info = chunk_pages_[page_idx];
    auto* col = value_columns_[col_idx];
    auto& pps = col->per_page_state[page_idx];

    pps.notnull_bitmap.clear();
    pps.predecoded_values.clear();
    pps.predecoded_strings.clear();
    pps.predecoded_read_pos = 0;
    pps.predecoded_count = 0;
    pps.predecode_pa.destroy();

    if (page_info.value_compressed_sizes[col_idx] == 0) {
        return E_OK;
    }

    char stack_buf[4096];
    char* compressed_buf = stack_buf;
    bool heap = page_info.value_compressed_sizes[col_idx] > sizeof(stack_buf);
    if (heap) {
        compressed_buf = static_cast<char*>(common::mem_alloc(
            page_info.value_compressed_sizes[col_idx], common::MOD_DEFAULT));
        if (compressed_buf == nullptr) return E_OOM;
    }

    int32_t read_len = 0;
    int ret =
        read_file_->read(page_info.value_file_offsets[col_idx], compressed_buf,
                         page_info.value_compressed_sizes[col_idx], read_len);
    if (IS_FAIL(ret)) {
        if (heap) common::mem_free(compressed_buf);
        return ret;
    }
    if (read_len !=
        static_cast<int32_t>(page_info.value_compressed_sizes[col_idx])) {
        if (heap) common::mem_free(compressed_buf);
        return E_TSFILE_CORRUPTED;
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
    // The value page begins with a uint32 data_num followed by a bitmap of
    // ceil(data_num/8) bytes; a corrupt or truncated page that doesn't even
    // hold the data_num header would let read_ui32() walk past the buffer.
    if (uncompressed_size < sizeof(uint32_t)) {
        col->compressor->after_uncompress(uncompressed_buf);
        return E_TSFILE_CORRUPTED;
    }

    uint32_t offset = 0;
    uint32_t data_num = SerializationUtil::read_ui32(uncompressed_buf);
    offset += sizeof(uint32_t);
    uint32_t bitmap_bytes = (data_num + 7) / 8;
    if (uncompressed_size - offset < bitmap_bytes) {
        col->compressor->after_uncompress(uncompressed_buf);
        return E_TSFILE_CORRUPTED;
    }
    pps.notnull_bitmap.resize(bitmap_bytes);
    memcpy(pps.notnull_bitmap.data(), uncompressed_buf + offset, bitmap_bytes);
    offset += bitmap_bytes;

    char* value_buf = uncompressed_buf + offset;
    uint32_t value_buf_size = uncompressed_size - offset;
    common::ByteStream in;
    in.wrap_from(value_buf, value_buf_size);
    col->decoder->reset();

    auto dt = col->chunk_header.data_type_;
    int nonnull_total = count_non_null_prefix(pps.notnull_bitmap,
                                              static_cast<int32_t>(data_num));
    int prefix_nonnull =
        count_non_null_prefix(pps.notnull_bitmap, page_info.row_begin);
    pps.predecoded_read_pos = prefix_nonnull;

    auto cleanup = [&]() {
        col->compressor->after_uncompress(uncompressed_buf);
    };

    if (dt == common::STRING || dt == common::TEXT || dt == common::BLOB) {
        pps.predecode_pa.init(512, common::MOD_TSFILE_READER);
        pps.predecoded_strings.resize(nonnull_total);
        for (int i = 0; i < nonnull_total; i++) {
            if (RET_FAIL(col->decoder->read_String(pps.predecoded_strings[i],
                                                   pps.predecode_pa, in))) {
                cleanup();
                return ret;
            }
        }
        pps.predecoded_count = nonnull_total;
        cleanup();
        return E_OK;
    }

    if (nonnull_total == 0) {
        cleanup();
        return E_OK;
    }

    uint32_t elem_size = common::get_data_type_size(dt);
    pps.predecoded_values.resize(static_cast<size_t>(nonnull_total) *
                                 elem_size);
    switch (dt) {
        case common::BOOLEAN: {
            bool* out = reinterpret_cast<bool*>(pps.predecoded_values.data());
            for (int i = 0; i < nonnull_total; i++) {
                if (RET_FAIL(col->decoder->read_boolean(out[i], in))) {
                    cleanup();
                    return ret;
                }
            }
            break;
        }
        case common::INT32:
        case common::DATE:
            if (RET_FAIL(col->decoder->read_exact_int32(
                    reinterpret_cast<int32_t*>(pps.predecoded_values.data()),
                    nonnull_total, in))) {
                cleanup();
                return ret;
            }
            break;
        case common::INT64:
        case common::TIMESTAMP:
            if (RET_FAIL(col->decoder->read_exact_int64(
                    reinterpret_cast<int64_t*>(pps.predecoded_values.data()),
                    nonnull_total, in))) {
                cleanup();
                return ret;
            }
            break;
        case common::FLOAT:
            if (RET_FAIL(col->decoder->read_exact_float(
                    reinterpret_cast<float*>(pps.predecoded_values.data()),
                    nonnull_total, in))) {
                cleanup();
                return ret;
            }
            break;
        case common::DOUBLE:
            if (RET_FAIL(col->decoder->read_exact_double(
                    reinterpret_cast<double*>(pps.predecoded_values.data()),
                    nonnull_total, in))) {
                cleanup();
                return ret;
            }
            break;
        default:
            cleanup();
            return E_NOT_SUPPORT;
    }
    pps.predecoded_count = nonnull_total;
    cleanup();
    return E_OK;
}

// Multi-thread path: one task per value column, each decoding all non-SKIP
// pages of that column serially.  Time pages dispatched as worker-bucketed
// strided tasks using per-worker decoder/compressor (filled from
// time_decoder_pool_ / time_compressor_pool_) so they don't contend on the
// shared time_decoder_/time_compressor_.
//
// Single-thread: do NOT pre-decode every page upfront — leave per_page_state
// empty so the scatter loop decodes on demand and releases after each page
// (see decode_page_lazy() / release_page_slot()).  Bounds memory to one page.
int AlignedChunkReader::decode_all_planned_pages() {
    if (chunk_pages_.empty()) return E_OK;

#ifdef ENABLE_THREADS
    if (decode_pool_ != nullptr && value_columns_.size() > 1) {
        // Lazily grow the per-worker time decoder/compressor pool.  Both
        // factories can return nullptr on OOM/unsupported config; without
        // checking, the worker task below dereferences null when calling
        // decode_time_page_with().
        size_t worker_count = decode_pool_->num_threads();
        if (time_decoder_pool_.size() < worker_count) {
            time_decoder_pool_.resize(worker_count, nullptr);
            time_compressor_pool_.resize(worker_count, nullptr);
            for (size_t w = 0; w < worker_count; w++) {
                if (time_decoder_pool_[w] == nullptr) {
                    time_decoder_pool_[w] =
                        DecoderFactory::alloc_time_decoder();
                    if (time_decoder_pool_[w] == nullptr) return E_OOM;
                }
                if (time_compressor_pool_[w] == nullptr) {
                    time_compressor_pool_[w] =
                        CompressorFactory::alloc_compressor(
                            time_chunk_header_.compression_type_);
                    if (time_compressor_pool_[w] == nullptr) return E_OOM;
                }
            }
        }

        std::vector<std::future<void>> futures;
        std::vector<int> col_rets(value_columns_.size(), E_OK);
        for (uint32_t c = 0; c < value_columns_.size(); c++) {
            int* col_ret = &col_rets[c];
            futures.push_back(decode_pool_->submit([this, c, col_ret]() {
                for (size_t p = 0; p < chunk_pages_.size(); p++) {
                    int r = decode_value_page_for_slot(c, p);
                    if (IS_FAIL(r)) {
                        *col_ret = r;
                        return;
                    }
                }
            }));
        }
        // Time pages dispatched in worker-sized chunks (one task per worker)
        // to amortize submit/wait overhead.  Stride for load balance.
        size_t time_task_count = std::min(worker_count, chunk_pages_.size());
        std::vector<int> time_rets(time_task_count, E_OK);
        for (size_t k = 0; k < time_task_count; k++) {
            int* tr = &time_rets[k];
            futures.push_back(decode_pool_->submit(
                [this, k, tr, time_task_count, worker_count]() {
                    size_t wid = common::ThreadPool::current_worker_id();
                    if (wid >= worker_count) wid = 0;
                    Decoder* dec = time_decoder_pool_[wid];
                    Compressor* comp = time_compressor_pool_[wid];
                    for (size_t p = k; p < chunk_pages_.size();
                         p += time_task_count) {
                        int r = decode_time_page_with(
                            chunk_pages_[p], per_page_times_[p], dec, comp);
                        if (IS_FAIL(r)) {
                            *tr = r;
                            return;
                        }
                    }
                }));
        }
        // Wait on each task's own future rather than draining the whole pool:
        // it is shared process-wide, so wait_all() would also block on
        // unrelated concurrent operations' tasks still in flight.
        for (auto& f : futures) f.get();
        for (auto r : time_rets) {
            if (IS_FAIL(r)) return r;
        }
        for (uint32_t c = 0; c < value_columns_.size(); c++) {
            if (IS_FAIL(col_rets[c])) return col_rets[c];
        }
        return E_OK;
    }
#endif
    // Single-thread: defer decode to scatter time.
    return E_OK;
}

// Decode time + all value columns for a single page slot on demand.
// Used by the single-thread path to keep memory bounded to one page.
int AlignedChunkReader::decode_page_lazy(size_t page_idx) {
    int ret = E_OK;
    if (RET_FAIL(decode_time_page_direct(chunk_pages_[page_idx],
                                         per_page_times_[page_idx]))) {
        return ret;
    }
    for (uint32_t c = 0; c < value_columns_.size(); c++) {
        if (RET_FAIL(decode_value_page_for_slot(c, page_idx))) {
            return ret;
        }
    }
    return E_OK;
}

// Release the decoded buffers of one page slot so they can be reused by the
// next page (keeps memory footprint bounded for the single-thread path).
void AlignedChunkReader::release_page_slot(size_t page_idx) {
    std::vector<int64_t>{}.swap(per_page_times_[page_idx]);
    for (auto* col : value_columns_) {
        if (page_idx >= col->per_page_state.size()) continue;
        auto& pps = col->per_page_state[page_idx];
        std::vector<uint8_t>{}.swap(pps.notnull_bitmap);
        std::vector<char>{}.swap(pps.predecoded_values);
        std::vector<common::String>{}.swap(pps.predecoded_strings);
        pps.predecode_pa.destroy();
        pps.predecoded_count = 0;
        pps.predecoded_read_pos = 0;
    }
}

int AlignedChunkReader::get_next_page_multi(TsBlock* ret_tsblock,
                                            Filter* oneshoot_filter,
                                            PageArena& pa, int& row_offset) {
    int ret = E_OK;
    Filter* filter =
        (oneshoot_filter != nullptr ? oneshoot_filter : time_filter_);

    // Dispatch:
    //   - Offset pushdown always uses the page plan so fully covered pages and
    //     the prefix of the first retained page can be removed before value
    //     decoding, even when no worker pool is available.
    //   - Multi-column with a thread pool also uses the page plan for
    //     chunk-level parallel pre-decode.
    //   - Otherwise decode the current page's columns inline.
#ifdef ENABLE_THREADS
    const bool use_parallel_page_plan =
        decode_pool_ != nullptr && value_columns_.size() > 1;
#else
    const bool use_parallel_page_plan = false;
#endif
    const bool serial_page_in_progress =
        !page_plan_built_ &&
        (prev_time_page_not_finish() || prev_any_value_page_not_finish_multi());
    const bool use_page_plan =
        page_plan_built_ || (!serial_page_in_progress &&
                             (row_offset > 0 || use_parallel_page_plan));
    if (!use_page_plan) {
        return get_next_page_multi_serial(ret_tsblock, filter, pa, row_offset);
    }

    if (!page_plan_built_) {
        if (RET_FAIL(build_page_plan(filter, row_offset))) {
            return ret;
        }
        if (RET_FAIL(decode_all_planned_pages())) {
            return ret;
        }
    }
    if (chunk_pages_.empty()) {
        return E_NO_MORE_DATA;
    }

    const uint32_t null_mask_base = 1 << 7;
    const uint32_t num_cols = value_columns_.size();
    RowAppender row_appender(ret_tsblock);
    // Detect single-thread lazy mode by whether decode_all_planned_pages left
    // per_page_times_ empty (it leaves slots empty when there's no pool).
    const bool single_thread_lazy = per_page_times_[0].empty();

    while (current_page_plan_index_ < chunk_pages_.size()) {
        const ChunkPageInfo& page_info = chunk_pages_[current_page_plan_index_];

        if (!current_page_loaded_) {
            if (single_thread_lazy) {
                if (RET_FAIL(decode_page_lazy(current_page_plan_index_))) {
                    return ret;
                }
            }
            page_time_cursor_ = page_info.row_begin;
            page_time_count_ = page_info.row_end;
            current_page_loaded_ = true;
        }
        const std::vector<int64_t>& times =
            per_page_times_[current_page_plan_index_];

        int32_t remaining_in_page = page_time_count_ - page_time_cursor_;
        uint32_t budget = row_appender.remaining();

        // Fast path: FULL_PASS page, no nulls in any value column, types
        // match destination, budget > 0.  Bulk-memcpys up to
        // min(budget, remaining_in_page) rows from page_time_cursor_; tail
        // pages of an SSI tsblock still take the memcpy path instead of
        // falling into the row-by-row scatter loop.
        bool can_bulk = page_info.pass_type == PagePassType::FULL_PASS &&
                        remaining_in_page > 0 && budget > 0;
        if (can_bulk) {
            for (uint32_t c = 0; c < num_cols; c++) {
                auto* col = value_columns_[c];
                auto& pps = col->per_page_state[current_page_plan_index_];
                auto dt = col->chunk_header.data_type_;
                if (dt == common::STRING || dt == common::TEXT ||
                    dt == common::BLOB ||
                    ret_tsblock->get_vector(c + 1)->get_vector_type() != dt ||
                    pps.predecoded_count != page_time_count_) {
                    can_bulk = false;
                    break;
                }
            }
        }

        if (can_bulk) {
            uint32_t bulk_count =
                std::min(budget, static_cast<uint32_t>(remaining_in_page));
            size_t time_byte_off =
                static_cast<size_t>(page_time_cursor_) * sizeof(int64_t);
            // Bulk-append both bytes AND row count for every Vector.
            // Skipping add_row_nums() would leave each Vector's row_num_
            // at 0 while the TsBlock-level row_count_ jumped to bulk_count;
            // fill_trailling_nulls() would then mark every just-written
            // row as null, and column iterators would report the wrong
            // length.
            common::Vector* time_vec = ret_tsblock->get_vector(0);
            time_vec->get_value_data().append_fixed_value(
                reinterpret_cast<const char*>(times.data()) + time_byte_off,
                bulk_count * sizeof(int64_t));
            time_vec->add_row_nums(bulk_count);
            for (uint32_t c = 0; c < num_cols; c++) {
                auto* col = value_columns_[c];
                auto& pps = col->per_page_state[current_page_plan_index_];
                uint32_t elem_size =
                    common::get_data_type_size(col->chunk_header.data_type_);
                common::Vector* vec = ret_tsblock->get_vector(c + 1);
                vec->get_value_data().append_fixed_value(
                    pps.predecoded_values.data() +
                        static_cast<size_t>(page_time_cursor_) * elem_size,
                    bulk_count * elem_size);
                vec->add_row_nums(bulk_count);
            }
            row_appender.add_rows(bulk_count);
            page_time_cursor_ += bulk_count;
            if (page_time_cursor_ >= page_time_count_) {
                if (single_thread_lazy) {
                    release_page_slot(current_page_plan_index_);
                }
                current_page_plan_index_++;
                current_page_loaded_ = false;
                continue;
            }
            // Budget exhausted mid-page; caller will drain and resume.
            return E_OK;
        }

        // Slow path: row-by-row.  Handles null bitmap, type promotion,
        // BOUNDARY pages, and partial-page E_OVERFLOW.
        // BOUNDARY pages: build_page_plan compressed the page to the
        // [first-hit, last-hit] range, but timestamps inside that range may
        // still fail the filter (e.g. TimeIn({2, 8}) leaves 3..7 unmatched).
        // Re-apply the filter per timestamp here, advancing predecoded
        // read positions for skipped non-null rows so the cursor stays
        // aligned with the page's value layout.
        const bool boundary_filter =
            page_info.pass_type == PagePassType::BOUNDARY && filter != nullptr;
        while (page_time_cursor_ < page_time_count_) {
            if (row_appender.remaining() == 0) {
                return E_OK;
            }
            int64_t ts = times[page_time_cursor_];
            if (boundary_filter && !filter->satisfy_start_end_time(ts, ts)) {
                for (uint32_t c = 0; c < num_cols; c++) {
                    auto* col = value_columns_[c];
                    auto& pps = col->per_page_state[current_page_plan_index_];
                    // An empty notnull_bitmap means this column carried no data
                    // for the page (a missing / sparse aligned measurement), so
                    // every row is null; otherwise consult the per-row bit.
                    bool is_null = true;
                    if (!pps.notnull_bitmap.empty()) {
                        is_null =
                            ((pps.notnull_bitmap[page_time_cursor_ / 8] &
                              0xFF) &
                             (null_mask_base >> (page_time_cursor_ % 8))) == 0;
                    }
                    if (!is_null) pps.predecoded_read_pos++;
                }
                page_time_cursor_++;
                continue;
            }
            if (UNLIKELY(!row_appender.add_row())) {
                return E_OK;
            }
            row_appender.append(0, reinterpret_cast<char*>(&ts), sizeof(ts));

            for (uint32_t c = 0; c < num_cols; c++) {
                auto* col = value_columns_[c];
                auto& pps = col->per_page_state[current_page_plan_index_];
                bool is_null = true;
                if (!pps.notnull_bitmap.empty()) {
                    is_null =
                        ((pps.notnull_bitmap[page_time_cursor_ / 8] & 0xFF) &
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
                        pps.predecoded_strings[pps.predecoded_read_pos++];
                    row_appender.append(c + 1, value.buf_, value.len_);
                } else {
                    uint32_t elem_size = common::get_data_type_size(
                        col->chunk_header.data_type_);
                    row_appender.append(
                        c + 1,
                        pps.predecoded_values.data() +
                            static_cast<size_t>(pps.predecoded_read_pos++) *
                                elem_size,
                        elem_size);
                }
            }
            page_time_cursor_++;
        }

        if (single_thread_lazy) {
            release_page_slot(current_page_plan_index_);
        }
        current_page_plan_index_++;
        current_page_loaded_ = false;
    }
    return E_NO_MORE_DATA;
}

int AlignedChunkReader::get_next_page_multi_serial(TsBlock* ret_tsblock,
                                                   Filter* filter,
                                                   PageArena& pa,
                                                   int& row_offset) {
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
            if (!cur_page_may_satisfy_filter_multi(filter)) {
                if (RET_FAIL(skip_cur_page_multi())) break;
            } else if (cur_page_fully_satisfies_filter_multi(filter) &&
                       should_skip_page_by_offset_multi(row_offset)) {
                if (RET_FAIL(skip_cur_page_multi())) break;
            } else {
                break;
            }
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

// Keep the same conservative distinction as the single-value path: a page
// that may satisfy the filter still needs row-level filtering unless its full
// time range is covered by the filter.
bool AlignedChunkReader::cur_page_may_satisfy_filter_multi(Filter* filter) {
    bool time_satisfy = filter == nullptr ||
                        cur_time_page_header_.statistic_ == nullptr ||
                        filter->satisfy(cur_time_page_header_.statistic_);
    return time_satisfy;
}

bool AlignedChunkReader::cur_page_fully_satisfies_filter_multi(Filter* filter) {
    Statistic* stat = cur_time_page_header_.statistic_;
    return filter == nullptr ||
           (stat != nullptr &&
            filter->contain_start_end_time(stat->start_time_, stat->end_time_));
}

bool AlignedChunkReader::should_skip_page_by_offset_multi(int& row_offset) {
    if (row_offset <= 0) {
        return false;
    }
    Statistic* stat = cur_time_page_header_.statistic_;
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

    // Phase 2: decompress + parse bitmap + reset decoder for each column's
    // current page, inline.  This serial path now only runs for single-column
    // reads or when no thread pool exists — multi-column reads with a pool take
    // the chunk-level path (decode_all_planned_pages), so there is no per-page
    // thread-pool fan-out here anymore.  predecode=false lets the scatter loop
    // (multi_DECODE_TV_BATCH) decode inline, which has better cache locality
    // when there is no parallelism to amortize an extra predecode buffer write.
    for (size_t c = 0; c < value_columns_.size() && IS_SUCC(ret); c++) {
        ret = decompress_and_parse_value_page(*value_columns_[c], false);
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
    if (uncompressed_size < sizeof(uint32_t)) return E_TSFILE_CORRUPTED;
    uint32_t offset = 0;
    uint32_t data_num = SerializationUtil::read_ui32(uncompressed_buf);
    offset += sizeof(uint32_t);
    uint32_t bitmap_bytes = (data_num + 7) / 8;
    if (uncompressed_size - offset < bitmap_bytes) return E_TSFILE_CORRUPTED;
    col.notnull_bitmap.resize(bitmap_bytes);
    memcpy(col.notnull_bitmap.data(), uncompressed_buf + offset, bitmap_bytes);
    offset += bitmap_bytes;
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

int AlignedChunkReader::decompress_and_parse_value_page(ValueColumnState& col,
                                                        bool predecode) {
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
    if (uncompressed_size < sizeof(uint32_t)) return E_TSFILE_CORRUPTED;
    uint32_t offset = 0;
    uint32_t data_num = SerializationUtil::read_ui32(uncompressed_buf);
    offset += sizeof(uint32_t);
    uint32_t bitmap_bytes = (data_num + 7) / 8;
    if (uncompressed_size - offset < bitmap_bytes) return E_TSFILE_CORRUPTED;
    col.notnull_bitmap.resize(bitmap_bytes);
    memcpy(col.notnull_bitmap.data(), uncompressed_buf + offset, bitmap_bytes);
    offset += bitmap_bytes;
    col.cur_value_index = -1;

    char* value_buf = uncompressed_buf + offset;
    uint32_t value_buf_size = uncompressed_size - offset;
    col.decoder->reset();
    col.in.wrap_from(value_buf, value_buf_size);

    // Pre-decode all non-null values into pending_decoded_values so the
    // scatter loop (multi_DECODE_TV_BATCH) just memcpys instead of calling
    // the decoder.  Moves the expensive int64/double decode into the worker
    // task so it runs in parallel.  Only handles fixed-length types — strings
    // stay on the inline-decode path.
    col.pending_decoded = false;
    col.pending_decoded_count = 0;
    col.pending_decoded_cursor = 0;
    auto dt = col.chunk_header.data_type_;
    if (predecode && dt != common::STRING && dt != common::TEXT &&
        dt != common::BLOB) {
        int nonnull_total = 0;
        for (uint32_t i = 0; i < data_num; i++) {
            if ((col.notnull_bitmap[i / 8] & (0x80 >> (i % 8))) != 0) {
                nonnull_total++;
            }
        }
        if (nonnull_total > 0) {
            uint32_t elem_size = common::get_data_type_size(dt);
            col.pending_decoded_values.resize(
                static_cast<size_t>(nonnull_total) * elem_size);
            int actual = 0;
            int rret = common::E_OK;
            switch (dt) {
                case common::BOOLEAN: {
                    bool* out = reinterpret_cast<bool*>(
                        col.pending_decoded_values.data());
                    for (int i = 0; i < nonnull_total; i++) {
                        bool v;
                        if (col.decoder->read_boolean(v, col.in) !=
                            common::E_OK) {
                            rret = common::E_OUT_OF_RANGE;
                            break;
                        }
                        out[i] = v;
                        actual++;
                    }
                    break;
                }
                case common::INT32:
                case common::DATE:
                    rret = col.decoder->read_exact_int32(
                        reinterpret_cast<int32_t*>(
                            col.pending_decoded_values.data()),
                        nonnull_total, col.in);
                    if (rret == common::E_OK) actual = nonnull_total;
                    break;
                case common::INT64:
                case common::TIMESTAMP:
                    rret = col.decoder->read_exact_int64(
                        reinterpret_cast<int64_t*>(
                            col.pending_decoded_values.data()),
                        nonnull_total, col.in);
                    if (rret == common::E_OK) actual = nonnull_total;
                    break;
                case common::FLOAT:
                    rret = col.decoder->read_exact_float(
                        reinterpret_cast<float*>(
                            col.pending_decoded_values.data()),
                        nonnull_total, col.in);
                    if (rret == common::E_OK) actual = nonnull_total;
                    break;
                case common::DOUBLE:
                    rret = col.decoder->read_exact_double(
                        reinterpret_cast<double*>(
                            col.pending_decoded_values.data()),
                        nonnull_total, col.in);
                    if (rret == common::E_OK) actual = nonnull_total;
                    break;
                default:
                    rret = common::E_OUT_OF_RANGE;
            }
            if (rret == common::E_OK && actual == nonnull_total) {
                col.pending_decoded_count = nonnull_total;
                col.pending_decoded = true;
            }
        } else {
            col.pending_decoded = true;  // empty page is trivially predecoded
        }
    }
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
            // The time stream and bitmap define the page's row/value count.
            // Once the page is fully processed, bytes left in an all-null
            // value stream are only encoder terminators or padding and must
            // not make has_more_data_multi() treat the page as unfinished.
            col->in.reset();
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
        // Cap each pass to what the appender can still hold; mirrors the fix
        // in ChunkReader's per-type batch loops.  A blanket "remaining < BATCH
        // → E_OVERFLOW" made progress impossible whenever the caller handed
        // us a TsBlock with capacity below BATCH (e.g. small per-block sizes
        // in multi-chunk queries).
        int eff_batch =
            std::min(BATCH, static_cast<int>(row_appender.remaining()));
        if (eff_batch <= 0) {
            ret = E_OVERFLOW;
            break;
        }

        // ── Phase 1: Decode a batch of timestamps ──
        int time_count = 0;
        if (RET_FAIL(time_decoder_->read_batch_int64(times, eff_batch,
                                                     time_count, time_in_))) {
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
            // Value buffer for fixed-width types — up to 129 * 8 bytes
            char val_buf[BATCH * 8];
            int val_count;
            // Variable-length values for STRING/TEXT/BLOB columns.  Only
            // populated when the column's data_type_ is variable; their
            // bufs are owned by the caller-provided PageArena.
            std::vector<common::String> str_vals;
        };
        // One ColBatch per value column.
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

            // Skip values if no rows pass time filter.  The aligned bitmap
            // supplies the exact count, including values equal to a codec's
            // in-band terminator.
            if (pass_count == 0 && cb.nonnull_count > 0) {
                int dret = common::E_OK;
                int sk = 0;
                switch (col->chunk_header.data_type_) {
                    case common::BOOLEAN: {
                        bool dummy;
                        for (sk = 0; sk < cb.nonnull_count; sk++) {
                            dret = col->decoder->read_boolean(dummy, col->in);
                            if (dret != common::E_OK) break;
                        }
                        break;
                    }
                    case common::INT32:
                    case common::DATE:
                        dret = skip_value_exact_typed(
                            col->decoder, static_cast<int32_t*>(nullptr),
                            cb.nonnull_count, col->in);
                        if (dret == common::E_OK) sk = cb.nonnull_count;
                        break;
                    case common::INT64:
                    case common::TIMESTAMP:
                        dret = skip_value_exact_typed(
                            col->decoder, static_cast<int64_t*>(nullptr),
                            cb.nonnull_count, col->in);
                        if (dret == common::E_OK) sk = cb.nonnull_count;
                        break;
                    case common::FLOAT:
                        dret = skip_value_exact_typed(
                            col->decoder, static_cast<float*>(nullptr),
                            cb.nonnull_count, col->in);
                        if (dret == common::E_OK) sk = cb.nonnull_count;
                        break;
                    case common::DOUBLE:
                        dret = skip_value_exact_typed(
                            col->decoder, static_cast<double*>(nullptr),
                            cb.nonnull_count, col->in);
                        if (dret == common::E_OK) sk = cb.nonnull_count;
                        break;
                    case common::STRING:
                    case common::TEXT:
                    case common::BLOB: {
                        // The decoder has no fast skip for var-length strings;
                        // reading + discarding is the only way to advance the
                        // input stream past the row's payload.
                        common::String tmp;
                        for (sk = 0; sk < cb.nonnull_count; sk++) {
                            dret = col->decoder->read_String(tmp, *pa, col->in);
                            if (dret != common::E_OK) break;
                        }
                        break;
                    }
                    default:
                        ret = E_TSFILE_CORRUPTED;
                        break;
                }
                if (ret != common::E_OK) break;
                if (dret != common::E_OK) {
                    ret = dret;
                    break;
                }
                if (sk != cb.nonnull_count) {
                    ret = E_TSFILE_CORRUPTED;
                    break;
                }
                cb.nonnull_count = 0;  // bytes consumed cleanly
            }

            // Decode non-null values.  Fast path: values were predecoded
            // into col->pending_decoded_values by the parallel worker — just
            // memcpy the slice for this batch.  Fallback: call the decoder
            // inline (used for STRING/TEXT/BLOB and when predecode was
            // skipped).
            if (cb.nonnull_count > 0) {
                if (col->pending_decoded) {
                    uint32_t elem_size = common::get_data_type_size(
                        col->chunk_header.data_type_);
                    memcpy(
                        cb.val_buf,
                        col->pending_decoded_values.data() +
                            static_cast<size_t>(col->pending_decoded_cursor) *
                                elem_size,
                        static_cast<size_t>(cb.nonnull_count) * elem_size);
                    col->pending_decoded_cursor += cb.nonnull_count;
                    cb.val_count = cb.nonnull_count;
                } else {
                    int dret = common::E_OK;
                    switch (col->chunk_header.data_type_) {
                        case common::BOOLEAN: {
                            bool* out = reinterpret_cast<bool*>(cb.val_buf);
                            cb.val_count = 0;
                            for (int s = 0; s < cb.nonnull_count; s++) {
                                bool v;
                                dret = col->decoder->read_boolean(v, col->in);
                                if (dret != common::E_OK) break;
                                out[cb.val_count++] = v;
                            }
                            break;
                        }
                        case common::INT32:
                        case common::DATE:
                            dret = col->decoder->read_exact_int32(
                                reinterpret_cast<int32_t*>(cb.val_buf),
                                cb.nonnull_count, col->in);
                            if (dret == common::E_OK)
                                cb.val_count = cb.nonnull_count;
                            break;
                        case common::INT64:
                        case common::TIMESTAMP:
                            dret = col->decoder->read_exact_int64(
                                reinterpret_cast<int64_t*>(cb.val_buf),
                                cb.nonnull_count, col->in);
                            if (dret == common::E_OK)
                                cb.val_count = cb.nonnull_count;
                            break;
                        case common::FLOAT:
                            dret = col->decoder->read_exact_float(
                                reinterpret_cast<float*>(cb.val_buf),
                                cb.nonnull_count, col->in);
                            if (dret == common::E_OK)
                                cb.val_count = cb.nonnull_count;
                            break;
                        case common::DOUBLE:
                            dret = col->decoder->read_exact_double(
                                reinterpret_cast<double*>(cb.val_buf),
                                cb.nonnull_count, col->in);
                            if (dret == common::E_OK)
                                cb.val_count = cb.nonnull_count;
                            break;
                        case common::STRING:
                        case common::TEXT:
                        case common::BLOB: {
                            // Variable-length payload doesn't fit in
                            // cb.val_buf; pull each value into str_vals and
                            // let the scatter loop index by val_count.
                            cb.str_vals.resize(cb.nonnull_count);
                            cb.val_count = 0;
                            for (int s = 0; s < cb.nonnull_count; s++) {
                                dret = col->decoder->read_String(cb.str_vals[s],
                                                                 *pa, col->in);
                                if (dret != common::E_OK) break;
                                cb.val_count++;
                            }
                            break;
                        }
                        default:
                            break;
                    }
                    // Any decoder error, or a short decode that produced
                    // fewer values than the bitmap promised, indicates a
                    // corrupt page; propagate immediately so the scatter
                    // loop doesn't read uninitialised cb.val_buf bytes.
                    if (dret != common::E_OK) {
                        ret = dret;
                        break;
                    }
                    if (col->chunk_header.data_type_ != common::STRING &&
                        col->chunk_header.data_type_ != common::TEXT &&
                        col->chunk_header.data_type_ != common::BLOB &&
                        cb.val_count != cb.nonnull_count) {
                        ret = E_TSFILE_CORRUPTED;
                        break;
                    }
                }
            }
        }
        if (ret != E_OK) break;

        // ── Phase 4: Skip if no rows pass ──
        if (pass_count == 0) {
            for (uint32_t c = 0; c < num_cols; c++) {
                value_columns_[c]->cur_value_index += time_count;
            }
            continue;
        }

        // ── Phase 5: Scatter into TsBlock ──

        // Fast path: all rows pass filter AND all columns have no nulls
        // → batch memcpy directly into Vector buffers.  STRING/TEXT/BLOB
        // columns have variable-width payload and live in cb.str_vals, not
        // cb.val_buf, so they must take the slow scatter path.
        if (pass_count == time_count) {
            bool all_nonnull_and_fixed_size = true;
            for (uint32_t c = 0; c < num_cols; c++) {
                auto dt = value_columns_[c]->chunk_header.data_type_;
                if (col_batches[c].nonnull_count != time_count ||
                    dt == common::STRING || dt == common::TEXT ||
                    dt == common::BLOB) {
                    all_nonnull_and_fixed_size = false;
                    break;
                }
            }
            if (all_nonnull_and_fixed_size) {
                // Batch append time column (bytes + row count); see the
                // chunk-level bulk path above for why add_row_nums() is
                // required alongside append_fixed_value().
                common::Vector* time_vec = ret_tsblock->get_vector(0);
                time_vec->get_value_data().append_fixed_value(
                    (const char*)times,
                    static_cast<uint32_t>(time_count) * sizeof(int64_t));
                time_vec->add_row_nums(static_cast<uint32_t>(time_count));
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
                    vec->add_row_nums(static_cast<uint32_t>(cb.val_count));
                    col->cur_value_index += time_count;
                }
                row_appender.add_rows(static_cast<uint32_t>(time_count));
                continue;
            }
        }

        // Slow path: per-row scatter (has filter or has nulls or strings)
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
                    auto dt = col->chunk_header.data_type_;
                    if (dt == common::STRING || dt == common::TEXT ||
                        dt == common::BLOB) {
                        const common::String& sv = cb.str_vals[val_idx[c]];
                        row_appender.append(c + 1, sv.buf_, sv.len_);
                    } else {
                        uint32_t elem_size = common::get_data_type_size(dt);
                        row_appender.append(c + 1,
                                            cb.val_buf + val_idx[c] * elem_size,
                                            elem_size);
                    }
                    val_idx[c]++;
                }
            }
        }
        if (ret != E_OK) break;
    }
    return ret;
}

}  // end namespace storage
