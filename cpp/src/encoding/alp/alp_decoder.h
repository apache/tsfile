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

#ifndef ENCODING_ALP_DECODER_H
#define ENCODING_ALP_DECODER_H

#include <stdint.h>

#include <algorithm>
#include <vector>

#include "alp_scalar.h"
#include "common/db_common.h"
#include "encoding/decoder.h"
#include "utils/errno_define.h"

namespace storage {

template <typename T>
class AlpDecoderBase : public Decoder {
   public:
    AlpDecoderBase() : initialized_(false), read_index_(0) {}
    ~AlpDecoderBase() override { destroy(); }

    void destroy() override {
        std::vector<T>().swap(values_);
        std::vector<uint8_t>().swap(page_buffer_);
        initialized_ = false;
        read_index_ = 0;
    }

    void reset() override {
        values_.clear();
        page_buffer_.clear();
        initialized_ = false;
        read_index_ = 0;
    }

    bool has_remaining(const common::ByteStream& in) override {
        if (initialized_) {
            return read_index_ < values_.size();
        }
        return in.has_remaining();
    }

    int read_boolean(bool&, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int32(int32_t&, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int64(int64_t&, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String&, common::PageArena&,
                    common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int read_float(float& ret_value, common::ByteStream& in) override {
        if (sizeof(T) != sizeof(float)) {
            return common::E_TYPE_NOT_MATCH;
        }
        const int ret = ensure_initialized(in);
        if (ret != common::E_OK) {
            return ret;
        }
        if (read_index_ >= values_.size()) {
            return common::E_NO_MORE_DATA;
        }
        ret_value = static_cast<float>(values_[read_index_++]);
        return common::E_OK;
    }

    int read_double(double& ret_value, common::ByteStream& in) override {
        if (sizeof(T) != sizeof(double)) {
            return common::E_TYPE_NOT_MATCH;
        }
        const int ret = ensure_initialized(in);
        if (ret != common::E_OK) {
            return ret;
        }
        if (read_index_ >= values_.size()) {
            return common::E_NO_MORE_DATA;
        }
        ret_value = static_cast<double>(values_[read_index_++]);
        return common::E_OK;
    }

    int read_batch_float(float* out, int capacity, int& actual,
                         common::ByteStream& in) override {
        if (sizeof(T) != sizeof(float)) {
            return common::E_TYPE_NOT_MATCH;
        }
        return read_batch_impl(out, capacity, actual, in);
    }

    int read_batch_double(double* out, int capacity, int& actual,
                          common::ByteStream& in) override {
        if (sizeof(T) != sizeof(double)) {
            return common::E_TYPE_NOT_MATCH;
        }
        return read_batch_impl(out, capacity, actual, in);
    }

    int skip_float(int count, int& skipped, common::ByteStream& in) override {
        if (sizeof(T) != sizeof(float)) {
            return common::E_TYPE_NOT_MATCH;
        }
        return skip_impl(count, skipped, in);
    }

    int skip_double(int count, int& skipped, common::ByteStream& in) override {
        if (sizeof(T) != sizeof(double)) {
            return common::E_TYPE_NOT_MATCH;
        }
        return skip_impl(count, skipped, in);
    }

   private:
    int ensure_initialized(common::ByteStream& in) {
        if (initialized_) {
            return common::E_OK;
        }
        initialized_ = true;
        const uint64_t remaining = in.remaining_size();
        if (remaining == 0) {
            return common::E_OK;
        }
        if (remaining > static_cast<uint64_t>(UINT32_MAX)) {
            return common::E_INVALID_ARG;
        }
        page_buffer_.resize(static_cast<uint32_t>(remaining));
        uint32_t read_len = 0;
        const int ret = in.read_buf(&page_buffer_[0],
                                    static_cast<uint32_t>(remaining), read_len);
        if (ret != common::E_OK && ret != common::E_PARTIAL_READ) {
            return ret;
        }
        if (read_len != remaining) {
            return common::E_PARTIAL_READ;
        }
        const alp::AlpStatus status = alp::AlpDecodePage(
            &page_buffer_[0], static_cast<uint32_t>(page_buffer_.size()),
            values_);
        if (status != alp::ALP_OK) {
            values_.clear();
            return common::E_DECODE_ERR;
        }
        return common::E_OK;
    }

    int read_batch_impl(float* out, int capacity, int& actual,
                        common::ByteStream& in) {
        return read_batch_generic(out, capacity, actual, in);
    }

    int read_batch_impl(double* out, int capacity, int& actual,
                        common::ByteStream& in) {
        return read_batch_generic(out, capacity, actual, in);
    }

    template <typename Out>
    int read_batch_generic(Out* out, int capacity, int& actual,
                           common::ByteStream& in) {
        actual = 0;
        if (capacity <= 0 || out == NULL) {
            return common::E_OK;
        }
        const int ret = ensure_initialized(in);
        if (ret != common::E_OK) {
            return ret;
        }
        const size_t remaining = values_.size() - read_index_;
        const size_t count =
            std::min(static_cast<size_t>(capacity), remaining);
        for (size_t i = 0; i < count; ++i) {
            out[i] = static_cast<Out>(values_[read_index_ + i]);
        }
        read_index_ += count;
        actual = static_cast<int>(count);
        return common::E_OK;
    }

    int skip_impl(int count, int& skipped, common::ByteStream& in) {
        skipped = 0;
        if (count <= 0) {
            return common::E_OK;
        }
        const int ret = ensure_initialized(in);
        if (ret != common::E_OK) {
            return ret;
        }
        const size_t remaining = values_.size() - read_index_;
        const size_t skipped_count =
            std::min(static_cast<size_t>(count), remaining);
        read_index_ += skipped_count;
        skipped = static_cast<int>(skipped_count);
        return common::E_OK;
    }

    std::vector<T> values_;
    std::vector<uint8_t> page_buffer_;
    bool initialized_;
    size_t read_index_;
};

class FloatAlpDecoder : public AlpDecoderBase<float> {
   public:
    FloatAlpDecoder() {}
};

class DoubleAlpDecoder : public AlpDecoderBase<double> {
   public:
    DoubleAlpDecoder() {}
};

}  // namespace storage

#endif  // ENCODING_ALP_DECODER_H
