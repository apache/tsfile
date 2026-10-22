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

#ifndef ENCODING_ALP_ENCODER_H
#define ENCODING_ALP_ENCODER_H

#include <stdint.h>

#include <vector>

#include "alp_scalar.h"
#include "common/db_common.h"
#include "encoding/encoder.h"
#include "utils/errno_define.h"

namespace storage {

template <typename T>
class AlpEncoderBase : public Encoder {
   public:
    AlpEncoderBase() {}
    ~AlpEncoderBase() override { destroy(); }

    void destroy() override {
        std::vector<T>().swap(values_);
        std::vector<uint8_t>().swap(page_buffer_);
    }

    void reset() override {
        values_.clear();
        page_buffer_.clear();
    }

    int encode(bool, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int encode(float value, common::ByteStream&) override {
        if (sizeof(T) != sizeof(float)) {
            return common::E_TYPE_NOT_MATCH;
        }
        return encode_value(static_cast<T>(value));
    }

    int encode(double value, common::ByteStream&) override {
        if (sizeof(T) != sizeof(double)) {
            return common::E_TYPE_NOT_MATCH;
        }
        return encode_value(static_cast<T>(value));
    }

    int encode_batch(const float* values, uint32_t count,
                     common::ByteStream&) override {
        if (sizeof(T) != sizeof(float) || (values == NULL && count != 0)) {
            return common::E_TYPE_NOT_MATCH;
        }
        return encode_batch_values(reinterpret_cast<const T*>(values), count);
    }

    int encode_batch(const double* values, uint32_t count,
                     common::ByteStream&) override {
        if (sizeof(T) != sizeof(double) || (values == NULL && count != 0)) {
            return common::E_TYPE_NOT_MATCH;
        }
        return encode_batch_values(reinterpret_cast<const T*>(values), count);
    }

    int flush(common::ByteStream& out_stream) override {
        if (values_.empty()) {
            return common::E_OK;
        }
        page_buffer_.reserve(values_.size() * sizeof(T) +
                             (values_.size() / alp::ALP_BLOCK_SIZE + 1) *
                                 alp::ALP_BLOCK_HEADER_SIZE);
        const alp::AlpStatus status = alp::AlpEncodePage(
            values_.empty() ? NULL : &values_[0],
            static_cast<uint32_t>(values_.size()), page_buffer_);
        values_.clear();
        if (status != alp::ALP_OK) {
            return common::E_ENCODE_ERR;
        }
        if (page_buffer_.empty()) {
            return common::E_OK;
        }
        const int ret =
            out_stream.write_buf(page_buffer_.empty() ? NULL : &page_buffer_[0],
                                 static_cast<uint32_t>(page_buffer_.size()));
        page_buffer_.clear();
        return ret;
    }

    int get_max_byte_size() override {
        return static_cast<int>(values_.capacity() * sizeof(T) +
                                alp::ALP_BLOCK_SIZE *
                                    (sizeof(T) + alp::ALP_BLOCK_HEADER_SIZE));
    }

   private:
    int encode_value(T value) {
        values_.push_back(value);
        return common::E_OK;
    }

    int encode_batch_values(const T* values, uint32_t count) {
        if (count == 0) {
            return common::E_OK;
        }
        values_.insert(values_.end(), values, values + count);
        return common::E_OK;
    }

    std::vector<T> values_;
    std::vector<uint8_t> page_buffer_;
};

class FloatAlpEncoder : public AlpEncoderBase<float> {
   public:
    FloatAlpEncoder() {}
};

class DoubleAlpEncoder : public AlpEncoderBase<double> {
   public:
    DoubleAlpEncoder() {}
};

}  // namespace storage

#endif  // ENCODING_ALP_ENCODER_H
