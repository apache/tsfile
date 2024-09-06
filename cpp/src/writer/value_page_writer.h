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
#ifndef WRITER_PAGE_VALUE_WRITER_H
#define WRITER_PAGE_VALUE_WRITER_H

#include <vector>

#include "common/allocator/byte_stream.h"
#include "common/container/bit_map.h"
#include "common/statistic.h"
#include "compress/compressor.h"
#include "encoding/encoder.h"
#include "utils/db_utils.h"

namespace storage {

struct ValuePageData {
    uint32_t col_notnull_bitmap_buf_size_;
    uint32_t value_buf_size_;
    uint32_t uncompressed_size_;
    uint32_t compressed_size_;
    char *uncompressed_buf_;
    char *compressed_buf_;
    Compressor *compressor_;

    ValuePageData()
        : col_notnull_bitmap_buf_size_(0),
          value_buf_size_(0),
          uncompressed_size_(0),
          compressed_size_(0),
          uncompressed_buf_(nullptr),
          compressed_buf_(nullptr),
          compressor_(nullptr) {}
    int init(common::ByteStream &col_notnull_bitmap_bs,
             common::ByteStream &value_bs, Compressor *compressor,
             uint32_t size);
    void destroy() {
        // Be careful about the memory
        if (uncompressed_buf_ != nullptr) {
            common::mem_free(uncompressed_buf_);
            uncompressed_buf_ = nullptr;
        }
        if (compressed_buf_ != nullptr && compressor_ != nullptr) {
            compressor_->after_compress(compressed_buf_);
            compressed_buf_ = nullptr;
        }
    }
};

#define VPW_DO_WRITE_FOR_TYPE(TSDATATYPE, ISNULL)                         \
    {                                                                     \
        int ret = common::E_OK;                                           \
        if (UNLIKELY(data_type_ != TSDATATYPE)) {                         \
            ret = common::E_TYPE_NOT_MATCH;                               \
            return ret;                                                   \
        }                                                                 \
        if (!ISNULL) {                                                    \
            if ((size_ / 8) + 1 > col_notnull_bitmap_.size()) {           \
                col_notnull_bitmap_.push_back(0);                         \
            }                                                             \
            col_notnull_bitmap_[size_ / 8] |= (MASK >> (size_ % 8));      \
        }                                                                 \
        size_++;                                                          \
        if (ISNULL) {                                                     \
            return ret;                                                   \
        }                                                                 \
        if (RET_FAIL(value_encoder_->encode(value, value_out_stream_))) { \
        } else {                                                          \
            statistic_->update(timestamp, value);                         \
        }                                                                 \
        return ret;                                                       \
    }

class ValuePageWriter {
   public:
    ValuePageWriter()
        : data_type_(common::VECTOR),
          value_encoder_(nullptr),
          statistic_(nullptr),
          col_notnull_bitmap_out_stream_(OUT_STREAM_PAGE_SIZE,
                                         common::MOD_PAGE_WRITER_OUTPUT_STREAM),
          value_out_stream_(OUT_STREAM_PAGE_SIZE,
                            common::MOD_PAGE_WRITER_OUTPUT_STREAM),
          cur_page_data_(),
          compressor_(nullptr),
          is_inited_(false),
          col_notnull_bitmap_(),
          size_(0) {}
    ~ValuePageWriter() { destroy(); }
    int init(common::TSDataType data_type, common::TSEncoding encoding,
             common::CompressionType compression);
    void reset();
    void destroy();

    FORCE_INLINE int write(int64_t timestamp, bool value, bool isnull) {
        VPW_DO_WRITE_FOR_TYPE(common::BOOLEAN, isnull);
    }
    FORCE_INLINE int write(int64_t timestamp, int32_t value, bool isnull) {
        VPW_DO_WRITE_FOR_TYPE(common::INT32, isnull);
    }
    FORCE_INLINE int write(int64_t timestamp, int64_t value, bool isnull) {
        VPW_DO_WRITE_FOR_TYPE(common::INT64, isnull);
    }
    FORCE_INLINE int write(int64_t timestamp, float value, bool isnull) {
        VPW_DO_WRITE_FOR_TYPE(common::FLOAT, isnull);
    }
    FORCE_INLINE int write(int64_t timestamp, double value, bool isnull) {
        VPW_DO_WRITE_FOR_TYPE(common::DOUBLE, isnull);
    }

    FORCE_INLINE uint32_t get_point_numer() const { return statistic_->count_; }
    FORCE_INLINE uint32_t get_col_notnull_bitmap_out_stream_size() const {
        return col_notnull_bitmap_out_stream_.total_size();
    }
    FORCE_INLINE uint32_t get_page_memory_size() const {
        return col_notnull_bitmap_out_stream_.total_size() +
               value_out_stream_.total_size();
    }
    /**
     * calculate max possible memory size it occupies, including time
     * outputStream and value outputStream, because size outputStream is never
     * used until flushing.
     *
     * @return allocated size in time, value and outputStream
     */
    FORCE_INLINE uint32_t estimate_max_mem_size() const {
        return sizeof(int32_t) + 1 +
               col_notnull_bitmap_out_stream_.total_size() +
               value_out_stream_.total_size() +
               value_encoder_->get_max_byte_size();
    }
    int write_to_chunk(common::ByteStream &pages_data, bool write_header,
                       bool write_statistic, bool write_data_to_chunk_data);
    FORCE_INLINE common::ByteStream &get_col_notnull_bitmap_data() {
        return col_notnull_bitmap_out_stream_;
    }
    FORCE_INLINE common::ByteStream &get_value_data() {
        return value_out_stream_;
    }
    FORCE_INLINE Statistic *get_statistic() { return statistic_; }
    ValuePageData get_cur_page_data() { return cur_page_data_; }
    void destroy_page_data() { cur_page_data_.destroy(); }

   private:
    FORCE_INLINE int prepare_end_page() {
        int ret = common::E_OK;
        if (RET_FAIL(value_encoder_->flush(value_out_stream_))) {
        }
        for (auto col_notnull_bitmap_byte : col_notnull_bitmap_) {
            col_notnull_bitmap_out_stream_.write_buf(&col_notnull_bitmap_byte,
                                                     1);
        }
        return ret;
    }
    int copy_page_data_to(common::ByteStream &my_page_data,
                          common::ByteStream &pages_data);

   private:
    static const uint32_t OUT_STREAM_PAGE_SIZE = 1024;

   private:
    common::TSDataType data_type_;
    Encoder *value_encoder_;
    Statistic *statistic_;
    common::ByteStream col_notnull_bitmap_out_stream_;
    common::ByteStream value_out_stream_;
    ValuePageData cur_page_data_;
    Compressor *compressor_;
    bool is_inited_;
    std::vector<uint8_t> col_notnull_bitmap_;
    uint32_t size_;

    static uint32_t MASK;
};

}  // end namespace storage

#endif  // WRITER_PAGE_VALUE_WRITER_H
