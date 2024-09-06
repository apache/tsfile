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

#include "compress/compressor_factory.h"
#include "encoding/decoder_factory.h"

using namespace common;
namespace storage {

int AlignedChunkReader::init(ReadFile *read_file, String m_name,
                             TSDataType data_type, Filter *time_filter) {
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

    char *file_data_buf = time_in_stream_.get_wrapped_buf();
    if (file_data_buf != nullptr) {
        mem_free(file_data_buf);
    }
    time_in_stream_.reset();
    file_data_buf = value_in_stream_.get_wrapped_buf();
    if (file_data_buf != nullptr) {
        mem_free(file_data_buf);
    }
    value_in_stream_.reset();
    file_data_time_buf_size_ = 0;
    file_data_value_buf_size_ = 0;
    time_chunk_visit_offset_ = 0;
    value_chunk_visit_offset_ = 0;
}

void AlignedChunkReader::destroy() {
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
    char *buf = time_in_stream_.get_wrapped_buf();
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
}

int AlignedChunkReader::load_by_aligned_meta(ChunkMeta *time_chunk_meta,
                                             ChunkMeta *value_chunk_meta) {
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
    char *time_file_data_buf =
        (char *)mem_alloc(file_data_time_buf_size_, MOD_CHUNK_READER);
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
    char *value_file_data_buf =
        (char *)mem_alloc(file_data_value_buf_size_, MOD_CHUNK_READER);
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
    storage::Decoder *&decoder, storage::Compressor *&compressor,
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

int AlignedChunkReader::get_next_page(TsBlock *ret_tsblock,
                                      Filter *oneshoot_filter) {
    int ret = E_OK;
    Filter *filter =
        (oneshoot_filter != nullptr ? oneshoot_filter : time_filter_);

    if (prev_time_page_not_finish() && prev_value_page_not_finish()) {
        ret = decode_time_value_buf_into_tsblock(ret_tsblock, oneshoot_filter);
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
            } else if (cur_page_statisify_filter(filter)) {
                break;
            } else if (RET_FAIL(skip_cur_page())) {
            }
        }
        if (IS_SUCC(ret)) {
            ret = decode_cur_time_page_data() || decode_cur_value_page_data();
        }
    }
    if (IS_SUCC(ret)) {
        ret = decode_time_value_buf_into_tsblock(ret_tsblock, oneshoot_filter);
    }
    return ret;
}

int AlignedChunkReader::get_cur_page_header(ChunkMeta *&chunk_meta,
                                            common::ByteStream &in_stream,
                                            PageHeader &cur_page_header,
                                            uint32_t &chunk_visit_offset,
                                            ChunkHeader &chunk_header) {
    int ret = E_OK;
    bool retry = true;
    int cur_page_header_serialized_size = 0;
    // TODO： configurable
    int retry_read_want_size = 1024;
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
            int32_t file_data_buf_size =
                chunk_header.data_type_ == common::VECTOR
                    ? file_data_time_buf_size_
                    : file_data_value_buf_size_;
            if (E_OK == read_from_file_and_rewrap(
                            in_stream, chunk_meta, chunk_visit_offset,
                            file_data_buf_size, retry_read_want_size)) {
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
    common::ByteStream &in_stream_, ChunkMeta *&chunk_meta,
    uint32_t &chunk_visit_offset, int32_t file_data_buf_size, int want_size) {
    int ret = E_OK;
    const int DEFAULT_READ_SIZE = 4096;  // may use page_size + page_header_size
    char *file_data_buf = in_stream_.get_wrapped_buf();
    int offset = chunk_meta->offset_of_chunk_header_ + chunk_visit_offset;
    int read_size =
        (want_size < DEFAULT_READ_SIZE ? DEFAULT_READ_SIZE : want_size);
    if (file_data_buf_size < read_size || read_size < file_data_buf_size / 10) {
        file_data_buf = (char *)mem_realloc(file_data_buf, read_size);
        if (IS_NULL(file_data_buf)) {
            return E_OOM;
        }
        file_data_buf_size = read_size;
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

bool AlignedChunkReader::cur_page_statisify_filter(Filter *filter) {
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
                cur_time_page_header_.compressed_size_,
                file_data_time_buf_size_))) {
        }
    }

    char *time_compressed_buf = nullptr;
    char *time_uncompressed_buf = nullptr;
    uint32_t time_compressed_buf_size = 0;
    uint32_t time_uncompressed_buf_size = 0;
    char *time_buf = nullptr;
    uint32_t time_buf_size = 0;

    // Step 2: do uncompress
    if (IS_SUCC(ret)) {
        time_compressed_buf =
            time_in_stream_.get_wrapped_buf() + time_in_stream_.read_pos();
#ifdef DEBUG_SE
        std::cout << "AlignedChunkReader::decode_cur_page_data,time_in_stream_."
                     "get_wrapped_buf="
                  << (void *)(time_in_stream_.get_wrapped_buf())
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

    // Step 3: get time_buf
    if (IS_SUCC(ret)) {
        int var_size = 0;
        if (RET_FAIL(SerializationUtil::read_var_uint(
                time_buf_size, time_uncompressed_buf,
                time_uncompressed_buf_size, &var_size))) {
        } else {
            time_buf = time_uncompressed_buf + var_size;
            if (time_uncompressed_buf_size < var_size + time_buf_size) {
                ret = E_TSFILE_CORRUPTED;
                ASSERT(false);
            }
        }
    }
    time_decoder_->reset();
#ifdef DEBUG_SE
    DEBUG_hex_dump_buf("AlignedChunkReader reader, time_buf = ", time_buf,
                       time_buf_size);
#endif
    time_in_.wrap_from(time_buf, time_buf_size);
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
                cur_value_page_header_.compressed_size_,
                file_data_value_buf_size_))) {
        }
    }

    char *value_compressed_buf = nullptr;
    char *value_uncompressed_buf = nullptr;
    uint32_t value_compressed_buf_size = 0;
    uint32_t value_uncompressed_buf_size = 0;
    char *value_buf = nullptr;
    uint32_t value_buf_size = 0;

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
        for (unsigned char &i : value_page_col_notnull_bitmap_) {
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
    TsBlock *&ret_tsblock, Filter *filter) {
    int ret = common::E_OK;
    ret = decode_tv_buf_into_tsblock_by_datatype(time_in_, value_in_,
                                                 ret_tsblock, filter);
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
        while ((time_decoder_->has_remaining() &&                              \
                value_decoder_->has_remaining()) ||                            \
               (time_in.has_remaining() && value_in.has_remaining())) {        \
            cur_value_index++;                                                 \
            if (((value_page_col_notnull_bitmap_[cur_value_index / 8] &        \
                  0xFF) &                                                      \
                 (mask >> (cur_value_index % 8))) == 0) {                      \
                RET_FAIL(time_decoder_->read_int64(time, time_in));            \
                if (ret != E_OK) {                                             \
                    break;                                                     \
                }                                                              \
            }                                                                  \
            if (UNLIKELY(!row_appender.add_row())) {                           \
                ret = E_OVERFLOW;                                              \
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
                row_appender.append(0, (char *)&time, sizeof(time));           \
                row_appender.append(1, (char *)&value, sizeof(value));         \
            }                                                                  \
        }                                                                      \
    } while (false)

int AlignedChunkReader::i32_DECODE_TYPED_TV_INTO_TSBLOCK(
    ByteStream &time_in, ByteStream &value_in, RowAppender &row_appender,
    Filter *filter) {
    int ret = E_OK;
    uint32_t mask = 1 << 7;
    do {
        int64_t time = 0;
        int32_t value;
        while ((time_decoder_->has_remaining() &&
                value_decoder_->has_remaining()) ||
               (time_in.has_remaining() && value_in.has_remaining())) {
            cur_value_index++;
            if (((value_page_col_notnull_bitmap_[cur_value_index / 8] & 0xFF) &
                 (mask >> (cur_value_index % 8))) == 0) {
                RET_FAIL(time_decoder_->read_int64(time, time_in));
                continue;
            }
            if (UNLIKELY(!row_appender.add_row())) {
                ret = E_OVERFLOW;
                break;
            } else if (RET_FAIL(time_decoder_->read_int64(time, time_in))) {
            }
            if (RET_FAIL(value_decoder_->read_int32(value, value_in))) {
            } else if (filter != nullptr && !filter->satisfy(time, value)) {
                row_appender.backoff_add_row();
                continue;
            } else {
                /*std::cout << "decoder: time=" << time << ", value=" << value
                 * << std::endl;*/
                row_appender.append(0, (char *)&time, sizeof(time));
                row_appender.append(1, (char *)&value, sizeof(value));
            }
        }
    } while (false);
    return ret;
}

int AlignedChunkReader::decode_tv_buf_into_tsblock_by_datatype(
    ByteStream &time_in, ByteStream &value_in, TsBlock *ret_tsblock,
    Filter *filter) {
    int ret = E_OK;
    RowAppender row_appender(ret_tsblock);
    switch (value_chunk_header_.data_type_) {
        case common::BOOLEAN:
            DECODE_TYPED_TV_INTO_TSBLOCK(bool, boolean, time_in_, value_in_,
                                         row_appender);
            break;
        case common::INT32:
            DECODE_TYPED_TV_INTO_TSBLOCK(int32_t, int32, time_in_, value_in_,
                                         row_appender);
            break;
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
        default:
            ret = E_NOT_SUPPORT;
            ASSERT(false);
    }
    if (ret_tsblock->get_row_count() == 0 && ret == E_OK) {
        ret = E_NO_MORE_DATA;
    }
    return ret;
}

}  // end namespace storage