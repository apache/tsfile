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

#include "chunk_reader.h"

#include "compress/compressor_factory.h"
#include "encoding/decoder_factory.h"

using namespace common;
namespace storage {

int ChunkReader::init(ReadFile *read_file, String m_name, TSDataType data_type,
                      Filter *time_filter) {
    read_file_ = read_file;
    measurement_name_.shallow_copy_from(m_name);
    time_decoder_ = DecoderFactory::alloc_time_decoder();
    value_decoder_ = nullptr;
    compressor_ = nullptr;
    time_filter_ = time_filter;
    uncompressed_buf_ = nullptr;
    if (IS_NULL(time_decoder_)) {
        return E_OOM;
    }
    return E_OK;
}

void ChunkReader::reset() {
    chunk_meta_ = nullptr;
    chunk_header_.reset();
    cur_page_header_.reset();

    char *file_data_buf = in_stream_.get_wrapped_buf();
    if (file_data_buf != nullptr) {
        mem_free(file_data_buf);
    }
    in_stream_.reset();
    file_data_buf_size_ = 0;
    chunk_visit_offset_ = 0;
}

void ChunkReader::destroy() {
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
    if (compressor_ != nullptr) {
        compressor_->~Compressor();
        CompressorFactory::free(compressor_);
        compressor_ = nullptr;
    }
    char *buf = in_stream_.get_wrapped_buf();
    if (buf != nullptr) {
        mem_free(buf);
        in_stream_.clear_wrapped_buf();
    }
    cur_page_header_.reset();
}

int ChunkReader::load_by_meta(ChunkMeta *meta) {
    int ret = E_OK;
    chunk_meta_ = meta;
#if DEBUG_SE
    std::cout << "ChunkReader::load_by_meta, meta=" << *meta << std::endl;
#endif
    /* ================ Step 1: reader data from file ================*/
    // at least, we can reader the chunk header and the first page header.
    // TODO configurable
    file_data_buf_size_ = 1024;
    int32_t ret_read_len = 0;
    char *file_data_buf =
        (char *)mem_alloc(file_data_buf_size_, MOD_CHUNK_READER);
    if (IS_NULL(file_data_buf)) {
        return E_OOM;
    }
    ret = read_file_->read(chunk_meta_->offset_of_chunk_header_, file_data_buf,
                           file_data_buf_size_, ret_read_len);
    if (IS_SUCC(ret) && ret_read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
        ret = E_TSFILE_CORRUPTED;
        LOGE("file corrupted, ret=" << ret << ", offset="
                                    << chunk_meta_->offset_of_chunk_header_
                                    << "read_len=" << ret_read_len);
        mem_free(file_data_buf);
    }

    /* ================ Step 2: deserialize chunk_header ================*/
    if (IS_SUCC(ret)) {
        in_stream_.wrap_from(file_data_buf, ret_read_len);
        // std::cout << "in_stream_.wrap_from " << (void*)file_data_buf <<
        // std::endl;
        if (RET_FAIL(chunk_header_.deserialize_from(in_stream_))) {
        } else if (RET_FAIL(alloc_compressor_and_value_decoder(
                       chunk_header_.encoding_type_, chunk_header_.data_type_,
                       chunk_header_.compression_type_))) {
        } else {
            chunk_visit_offset_ =
                in_stream_.read_pos();  // point to end of chunk_header_
#if DEBUG_SE
            std::cout << "ChunkReader::load_by_meta, chunk_header="
                      << chunk_header_ << std::endl;
#endif
        }
    }
    return ret;
}

int ChunkReader::alloc_compressor_and_value_decoder(
    TSEncoding encoding, TSDataType data_type, CompressionType compression) {
    if (value_decoder_ != nullptr) {
        value_decoder_->reset();
    } else {
        value_decoder_ = DecoderFactory::alloc_value_decoder(encoding, data_type);
        if (IS_NULL(value_decoder_)) {
            return E_OOM;
        }
    }

    if (compressor_ != nullptr) {
        compressor_->reset(/*for_compress*/ false);
    } else {
        compressor_ = CompressorFactory::alloc_compressor(compression);
        if (compressor_ == nullptr) {
            return E_OOM;
        }
    }
    return E_OK;
}

int ChunkReader::get_next_page(TsBlock *ret_tsblock, Filter *oneshoot_filter) {
    int ret = E_OK;
    Filter *filter =
        (oneshoot_filter != nullptr ? oneshoot_filter : time_filter_);

    if (prev_page_not_finish()) {
        ret = decode_tv_buf_into_tsblock_by_datatype(time_in_, value_in_,
                                                     ret_tsblock, filter);
        if (ret == E_OVERFLOW) {
            ret = E_OK;
        } else {
            if (uncompressed_buf_ != nullptr) {
                compressor_->after_uncompress(uncompressed_buf_);
                uncompressed_buf_ = nullptr;
            }
            time_in_.reset();
            value_in_.reset();
        }
        return ret;
    }

    while (IS_SUCC(ret)) {
        if (!has_more_data()) {
            return E_NO_MORE_DATA;
        }
        if (RET_FAIL(get_cur_page_header())) {
        } else if (cur_page_statisify_filter(filter)) {
            break;
        } else if (RET_FAIL(skip_cur_page())) {
        }
    }

    if (IS_SUCC(ret)) {
        ret = decode_cur_page_data(ret_tsblock, filter);
    }
    return ret;
}

int ChunkReader::get_cur_page_header() {
    int ret = E_OK;
    bool retry = true;
    int cur_page_header_serialized_size = 0;
    do {
        in_stream_.mark_read_pos();
        cur_page_header_.reset();
        ret = cur_page_header_.deserialize_from(
            in_stream_, !chunk_has_only_one_page(), chunk_header_.data_type_);
        cur_page_header_serialized_size = in_stream_.get_mark_len();
        if (deserialize_buf_not_enough(ret) && retry) {
            retry = false;
            ret = read_from_file_and_rewrap();
            if (E_OK == ret) {
                continue;
            }
        }
        break;
    } while (true);
    if (IS_SUCC(ret)) {
        // visit a header
        chunk_visit_offset_ += cur_page_header_serialized_size;
    }
#if DEBUG_SE
    std::cout << "get_cur_page_header, ret=" << ret << ", retry=" << retry
              << ", cur_page_header_=" << cur_page_header_
              << ", chunk_meta_->offset_of_chunk_header_="
              << chunk_meta_->offset_of_chunk_header_
              << ", cur_page_header_serialized_size="
              << cur_page_header_serialized_size << std::endl;
#endif
    return ret;
}

// reader at least @want_size bytes from file and wrap the buffer into
// @in_stream_
int ChunkReader::read_from_file_and_rewrap(int want_size) {
    int ret = E_OK;
    const int DEFAULT_READ_SIZE = 4096;  // may use page_size + page_header_size
    char *file_data_buf = in_stream_.get_wrapped_buf();
    int offset = chunk_meta_->offset_of_chunk_header_ + chunk_visit_offset_;
    int read_size =
        (want_size < DEFAULT_READ_SIZE ? DEFAULT_READ_SIZE : want_size);
    if (file_data_buf_size_ < read_size ||
        read_size < file_data_buf_size_ / 10) {
        file_data_buf = (char *)mem_realloc(file_data_buf, read_size);
        if (IS_NULL(file_data_buf)) {
            return E_OOM;
        }
        file_data_buf_size_ = read_size;
    }
    int ret_read_len = 0;
    if (RET_FAIL(read_file_->read(offset, file_data_buf, DEFAULT_READ_SIZE,
                                  ret_read_len))) {
    } else {
        in_stream_.wrap_from(file_data_buf, ret_read_len);
        // DEBUG_hex_dump_buf("wrapped buf = ", file_data_buf, 256);
    }
    return ret;
}

bool ChunkReader::cur_page_statisify_filter(Filter *filter) {
    return filter == nullptr || cur_page_header_.statistic_ == nullptr ||
           filter->satisfy(cur_page_header_.statistic_);
}

int ChunkReader::skip_cur_page() {
    int ret = E_OK;
    // visit a page tv data
    chunk_visit_offset_ += cur_page_header_.compressed_size_;
    in_stream_.wrapped_buf_advance_read_pos(cur_page_header_.compressed_size_);
    return ret;
}

int ChunkReader::decode_cur_page_data(TsBlock *&ret_tsblock, Filter *filter) {
    int ret = E_OK;

    // Step 1: make sure we load the whole page data in @in_stream_
    if (in_stream_.remaining_size() < cur_page_header_.compressed_size_) {
        // std::cout << "decode_cur_page_data. in_stream_.remaining_size="<<
        // in_stream_.remaining_size() << ", cur_page_header_.compressed_size_="
        // << cur_page_header_.compressed_size_ << std::endl;
        if (RET_FAIL(
                read_from_file_and_rewrap(cur_page_header_.compressed_size_))) {
        }
    }

    char *compressed_buf = nullptr;
    char *uncompressed_buf = nullptr;
    uint32_t compressed_buf_size = 0;  // cppcheck-suppress unreadVariable
    uint32_t uncompressed_buf_size = 0;
    char *time_buf = nullptr;
    char *value_buf = nullptr;
    uint32_t time_buf_size = 0;
    uint32_t value_buf_size = 0;

    // Step 2: do uncompress
    if (IS_SUCC(ret)) {
        compressed_buf = in_stream_.get_wrapped_buf() + in_stream_.read_pos();
        // std::cout << "ChunkReader::decode_cur_page_data,
        // in_stream_.get_wrapped_buf="
        // <<(void*)(in_stream_.get_wrapped_buf())<< ", in_stream_.read_pos=" <<
        // in_stream_.read_pos() << std::endl;
        compressed_buf_size = cur_page_header_.compressed_size_;
        in_stream_.wrapped_buf_advance_read_pos(compressed_buf_size);
        chunk_visit_offset_ += compressed_buf_size;
        if (RET_FAIL(compressor_->reset(false))) {
        } else if (RET_FAIL(compressor_->uncompress(
                       compressed_buf, compressed_buf_size, uncompressed_buf,
                       uncompressed_buf_size))) {
        } else {
            uncompressed_buf_ = uncompressed_buf;
        }
        // DEBUG_hex_dump_buf("ChunkReader reader, uncompressed buf = ",
        // uncompressed_buf, uncompressed_buf_size);
        if (ret != E_OK ||
            uncompressed_buf_size != cur_page_header_.uncompressed_size_) {
            ret = E_TSFILE_CORRUPTED;
            ASSERT(false);
        }
    }

    // Step 3: get time_buf & value_buf
    if (IS_SUCC(ret)) {
        int var_size = 0;
        if (RET_FAIL(SerializationUtil::read_var_uint(
                time_buf_size, uncompressed_buf, uncompressed_buf_size,
                &var_size))) {
        } else {
            time_buf = uncompressed_buf + var_size;
            value_buf = time_buf + time_buf_size;
            value_buf_size = uncompressed_buf_size - var_size - time_buf_size;
#if DEBUG_SE
            std::cout << "ChunkReader uncompress: compressed_buf_size="
                      << compressed_buf_size
                      << ", uncompressed_buf_size=" << uncompressed_buf_size
                      << ", var_size=" << var_size
                      << ", time_buf_size=" << time_buf_size << std::endl;
#endif
            if (uncompressed_buf_size <= var_size + time_buf_size) {
                ret = E_TSFILE_CORRUPTED;
                ASSERT(false);
            }
        }
    }

    // Step 4: decode time-value buffer into @ret_tsblock
    if (IS_SUCC(ret)) {
        time_decoder_->reset();
        value_decoder_->reset();
        time_in_.wrap_from(time_buf, time_buf_size);
        value_in_.wrap_from(value_buf, value_buf_size);
        // ret = decode_tv_buf_into_tsblock(time_buf, value_buf, time_buf_size,
        //                                  value_buf_size, ret_tsblock,
        //                                  filter);
        ret = decode_tv_buf_into_tsblock_by_datatype(time_in_, value_in_,
                                                     ret_tsblock, filter);
        // if we return during @decode_tv_buf_into_tsblock, we should keep
        // @uncompressed_buf_ valid until all TV pairs are decoded.
        if (ret != E_OVERFLOW) {
            if (uncompressed_buf_ != nullptr) {
                compressor_->after_uncompress(uncompressed_buf_);
                uncompressed_buf_ = nullptr;
            }
            time_in_.reset();
            value_in_.reset();
        } else {
            ret = E_OK;
        }
    }
    return ret;
}

#define DECODE_TYPED_TV_INTO_TSBLOCK(CppType, ReadType, time_in, value_in,     \
                                     row_appender)                             \
    do {                                                                       \
        int64_t time = 0;                                                      \
        CppType value;                                                         \
        while (time_decoder_->has_remaining() || time_in.has_remaining()) {    \
            ASSERT(value_decoder_->has_remaining() ||                          \
                   value_in.has_remaining());                                  \
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

int ChunkReader::i32_DECODE_TYPED_TV_INTO_TSBLOCK(ByteStream &time_in,
                                                  ByteStream &value_in,
                                                  RowAppender &row_appender,
                                                  Filter *filter) {
    int ret = E_OK;
    do {
        int64_t time = 0;
        int32_t value;
        while (time_decoder_->has_remaining() || time_in.has_remaining()) {
            ASSERT(value_decoder_->has_remaining() || value_in.has_remaining());
            if (UNLIKELY(!row_appender.add_row())) {
                ret = E_OVERFLOW;
                break;
            } else if (RET_FAIL(time_decoder_->read_int64(time, time_in))) {
            } else if (RET_FAIL(value_decoder_->read_int32(value, value_in))) {
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

int ChunkReader::decode_tv_buf_into_tsblock_by_datatype(ByteStream &time_in,
                                                        ByteStream &value_in,
                                                        TsBlock *ret_tsblock,
                                                        Filter *filter) {
    int ret = E_OK;
    RowAppender row_appender(ret_tsblock);
    switch (chunk_header_.data_type_) {
        case common::BOOLEAN:
            DECODE_TYPED_TV_INTO_TSBLOCK(bool, boolean, time_in_, value_in_,
                                         row_appender);
            break;
        case common::INT32:
            // DECODE_TYPED_TV_INTO_TSBLOCK(int32_t, int32, time_in_, value_in_,
            // row_appender);
            ret = i32_DECODE_TYPED_TV_INTO_TSBLOCK(time_in_, value_in_,
                                                   row_appender, filter);
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