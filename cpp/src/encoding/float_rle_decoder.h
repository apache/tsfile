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

#ifndef ENCODING_FLOAT_RLE_DECODER_H
#define ENCODING_FLOAT_RLE_DECODER_H

#include <cmath>
#include <cstdint>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "encoding/int32_rle_decoder.h"
#include "encoding/int64_rle_decoder.h"
#include "encoding/ts2diff_decoder.h"  // page metadata helpers

namespace storage {

// Decoder for the Java FloatEncoder RLE layout; see FloatRLEEncoder.
class FloatRLEDecoder : public Decoder {
   public:
    FloatRLEDecoder() { reset(); }
    ~FloatRLEDecoder() override = default;

    void reset() override {
        int_decoder_.reset();
        page_meta_parsed_ = false;
        max_point_value_ = 1.0;
        page_pos_ = 0;
        scaled_bm_.clear();
        raw_bm_.clear();
    }

    bool has_remaining(const common::ByteStream& buffer) override {
        return int_decoder_.has_remaining(buffer);
    }

    int read_boolean(bool& /*ret_value*/,
                     common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int32(int32_t& /*ret_value*/,
                   common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int64(int64_t& /*ret_value*/,
                   common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_float(float& ret_value, common::ByteStream& in) override {
        int ret = ensure_page_meta(in);
        if (RET_FAIL(ret)) {
            return ret;
        }
        int32_t value_int = 0;
        if (RET_FAIL(int_decoder_.read_int32(value_int, in))) {
            return ret;
        }
        ret_value = value_at(value_int);
        page_pos_++;
        return ret;
    }
    int read_double(double& /*ret_value*/,
                    common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String& /*ret_value*/, common::PageArena& /*pa*/,
                    common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

   private:
    int ensure_page_meta(common::ByteStream& in) {
        if (page_meta_parsed_) {
            return common::E_OK;
        }
        ts2diff_java_detail::PageMeta meta;
        int ret = ts2diff_java_detail::read_page_meta(in, meta);
        if (RET_FAIL(ret)) {
            return ret;
        }
        max_point_value_ =
            meta.max_point_number <= 0
                ? 1.0
                : std::pow(10.0, static_cast<double>(meta.max_point_number));
        scaled_bm_ = std::move(meta.scaled_bm);
        raw_bm_ = std::move(meta.raw_bm);
        page_pos_ = 0;
        page_meta_parsed_ = true;
        return common::E_OK;
    }

    float value_at(int32_t value_int) const {
        if (!raw_bm_.empty() &&
            ts2diff_java_detail::bitmap_marked(raw_bm_, page_pos_)) {
            return common::int_to_float(value_int);
        }
        const bool use_scaled =
            scaled_bm_.empty() ||
            ts2diff_java_detail::bitmap_marked(scaled_bm_, page_pos_);
        const double divisor = use_scaled ? max_point_value_ : 1.0;
        return static_cast<float>(static_cast<double>(value_int) / divisor);
    }

    Int32RleDecoder int_decoder_;
    double max_point_value_ = 1.0;
    int page_pos_ = 0;
    bool page_meta_parsed_ = false;
    std::vector<uint8_t> scaled_bm_;
    std::vector<uint8_t> raw_bm_;
};

class DoubleRLEDecoder : public Decoder {
   public:
    DoubleRLEDecoder() { reset(); }
    ~DoubleRLEDecoder() override = default;

    void reset() override {
        int_decoder_.reset();
        page_meta_parsed_ = false;
        max_point_value_ = 1.0;
        page_pos_ = 0;
        scaled_bm_.clear();
        raw_bm_.clear();
    }

    bool has_remaining(const common::ByteStream& buffer) override {
        return int_decoder_.has_remaining(buffer);
    }

    int read_boolean(bool& /*ret_value*/,
                     common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int32(int32_t& /*ret_value*/,
                   common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int64(int64_t& /*ret_value*/,
                   common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_float(float& /*ret_value*/,
                   common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_double(double& ret_value, common::ByteStream& in) override {
        int ret = ensure_page_meta(in);
        if (RET_FAIL(ret)) {
            return ret;
        }
        int64_t value_long = 0;
        if (RET_FAIL(int_decoder_.read_int64(value_long, in))) {
            return ret;
        }
        ret_value = value_at(value_long);
        page_pos_++;
        return ret;
    }
    int read_String(common::String& /*ret_value*/, common::PageArena& /*pa*/,
                    common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

   private:
    int ensure_page_meta(common::ByteStream& in) {
        if (page_meta_parsed_) {
            return common::E_OK;
        }
        ts2diff_java_detail::PageMeta meta;
        int ret = ts2diff_java_detail::read_page_meta(in, meta);
        if (RET_FAIL(ret)) {
            return ret;
        }
        max_point_value_ =
            meta.max_point_number <= 0
                ? 1.0
                : std::pow(10.0, static_cast<double>(meta.max_point_number));
        scaled_bm_ = std::move(meta.scaled_bm);
        raw_bm_ = std::move(meta.raw_bm);
        page_pos_ = 0;
        page_meta_parsed_ = true;
        return common::E_OK;
    }

    double value_at(int64_t value_long) const {
        if (!raw_bm_.empty() &&
            ts2diff_java_detail::bitmap_marked(raw_bm_, page_pos_)) {
            return common::long_to_double(value_long);
        }
        const bool use_scaled =
            scaled_bm_.empty() ||
            ts2diff_java_detail::bitmap_marked(scaled_bm_, page_pos_);
        const double divisor = use_scaled ? max_point_value_ : 1.0;
        return static_cast<double>(value_long) / divisor;
    }

    Int64RleDecoder int_decoder_;
    double max_point_value_ = 1.0;
    int page_pos_ = 0;
    bool page_meta_parsed_ = false;
    std::vector<uint8_t> scaled_bm_;
    std::vector<uint8_t> raw_bm_;
};

}  // namespace storage

#endif  // ENCODING_FLOAT_RLE_DECODER_H
