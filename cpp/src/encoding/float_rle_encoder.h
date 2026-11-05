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

#ifndef ENCODING_FLOAT_RLE_ENCODER_H
#define ENCODING_FLOAT_RLE_ENCODER_H

#include <cmath>
#include <cstdint>
#include <limits>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoder.h"
#include "encoding/int32_rle_encoder.h"
#include "encoding/int64_rle_encoder.h"
#include "encoding/ts2diff_encoder.h"      // java_round
#include "encoding/ts2diff_wire_format.h"  // overflow markers

namespace storage {

// FLOAT/DOUBLE RLE is the Java FloatEncoder wrapper: values are converted to
// scaled integers (with raw-bit fallbacks) and stored with the 32/64-bit RLE
// codec.  The page layout is the same as the TS_2DIFF float wrapper:
//   no overflow: [maxPointNumber varint][RLE stream]
//   overflow:    [marker varint][valueCount varint][scaled bitmap]
//                [raw bitmap?][maxPointNumber varint][RLE stream]
class FloatRLEEncoder : public Encoder {
   public:
    FloatRLEEncoder() { reset(); }
    ~FloatRLEEncoder() override = default;

    void reset() override {
        int_encoder_.reset();
        underflow_flags_.clear();
    }
    void destroy() override {}

    int encode(bool /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(float value, common::ByteStream& out_stream) override {
        return int_encoder_.encode(convert_float_to_int(value), out_stream);
    }
    int encode(double /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String /*value*/,
               common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int flush(common::ByteStream& out_stream) override {
        int ret = common::E_OK;
        if (underflow_flags_.empty()) {
            return ret;
        }
        common::ByteStream data(1024, common::MOD_ENCODER_OBJ, false);
        if (RET_FAIL(int_encoder_.flush(data))) {
            return ret;
        }
        const int num_values = static_cast<int>(underflow_flags_.size());
        if (has_overflow()) {
            std::vector<uint8_t> scaled_bitmap(
                static_cast<size_t>(num_values / 8 + 1), 0);
            std::vector<uint8_t> raw_bits_bitmap(
                static_cast<size_t>(num_values / 8 + 1), 0);
            bool has_raw_bits = false;
            for (int i = 0; i < num_values; i++) {
                int8_t f = underflow_flags_[static_cast<size_t>(i)];
                if (f == 1) {
                    scaled_bitmap[static_cast<size_t>(i / 8)] |=
                        static_cast<uint8_t>(1u << (i % 8));
                } else if (f == -1) {
                    has_raw_bits = true;
                    raw_bits_bitmap[static_cast<size_t>(i / 8)] |=
                        static_cast<uint8_t>(1u << (i % 8));
                }
            }
            if (RET_FAIL(common::SerializationUtil::write_var_uint(
                    has_raw_bits
                        ? ts2diff_java_detail::FLAG_ORIGINAL_VALUE_OVERFLOW
                        : ts2diff_java_detail::FLAG_SCALED_VALUE_OVERFLOW,
                    out_stream))) {
                return ret;
            }
            if (RET_FAIL(common::SerializationUtil::write_var_uint(
                    static_cast<uint32_t>(num_values), out_stream))) {
                return ret;
            }
            const uint32_t bm_len = static_cast<uint32_t>(num_values / 8 + 1);
            if (RET_FAIL(out_stream.write_buf(scaled_bitmap.data(), bm_len))) {
                return ret;
            }
            if (has_raw_bits && RET_FAIL(out_stream.write_buf(
                                    raw_bits_bitmap.data(), bm_len))) {
                return ret;
            }
        }
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                static_cast<uint32_t>(max_point_number_), out_stream))) {
            return ret;
        }
        if (RET_FAIL(common::merge_byte_stream(out_stream, data))) {
            return ret;
        }
        underflow_flags_.clear();
        return ret;
    }

    int get_max_byte_size() override {
        return int_encoder_.get_max_byte_size() + 16 +
               static_cast<int>(underflow_flags_.size() / 4 + 2);
    }

   private:
    int32_t convert_float_to_int(float value) {
        const double scaled = static_cast<double>(value) * max_point_value_;
        if (scaled > static_cast<double>(std::numeric_limits<int32_t>::max()) ||
            scaled < static_cast<double>(std::numeric_limits<int32_t>::min())) {
            if (std::isnan(value) ||
                value >
                    static_cast<float>(std::numeric_limits<int32_t>::max()) ||
                value <
                    static_cast<float>(std::numeric_limits<int32_t>::min())) {
                underflow_flags_.push_back(-1);
                return common::float_to_int(value);
            }
            underflow_flags_.push_back(0);
            return java_round<int32_t>(value);
        }
        if (std::isnan(value)) {
            underflow_flags_.push_back(-1);
            return common::float_to_int(value);
        }
        underflow_flags_.push_back(1);
        return java_round<int32_t>(scaled);
    }

    bool has_overflow() const {
        for (int8_t f : underflow_flags_) {
            if (f != 1) {
                return true;
            }
        }
        return false;
    }

    Int32RleEncoder int_encoder_;
    std::vector<int8_t> underflow_flags_;
    // Java Rle builder default: TSFileConfig.floatPrecision, currently 2.
    int max_point_number_ = 2;
    double max_point_value_ = 100.0;
};

class DoubleRLEEncoder : public Encoder {
   public:
    DoubleRLEEncoder() { reset(); }
    ~DoubleRLEEncoder() override = default;

    void reset() override {
        int_encoder_.reset();
        underflow_flags_.clear();
    }
    void destroy() override {}

    int encode(bool /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(float /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(double value, common::ByteStream& out_stream) override {
        return int_encoder_.encode(convert_double_to_long(value), out_stream);
    }
    int encode(common::String /*value*/,
               common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int flush(common::ByteStream& out_stream) override {
        int ret = common::E_OK;
        if (underflow_flags_.empty()) {
            return ret;
        }
        common::ByteStream data(1024, common::MOD_ENCODER_OBJ, false);
        if (RET_FAIL(int_encoder_.flush(data))) {
            return ret;
        }
        const int num_values = static_cast<int>(underflow_flags_.size());
        if (has_overflow()) {
            std::vector<uint8_t> scaled_bitmap(
                static_cast<size_t>(num_values / 8 + 1), 0);
            std::vector<uint8_t> raw_bits_bitmap(
                static_cast<size_t>(num_values / 8 + 1), 0);
            bool has_raw_bits = false;
            for (int i = 0; i < num_values; i++) {
                int8_t f = underflow_flags_[static_cast<size_t>(i)];
                if (f == 1) {
                    scaled_bitmap[static_cast<size_t>(i / 8)] |=
                        static_cast<uint8_t>(1u << (i % 8));
                } else if (f == -1) {
                    has_raw_bits = true;
                    raw_bits_bitmap[static_cast<size_t>(i / 8)] |=
                        static_cast<uint8_t>(1u << (i % 8));
                }
            }
            if (RET_FAIL(common::SerializationUtil::write_var_uint(
                    has_raw_bits
                        ? ts2diff_java_detail::FLAG_ORIGINAL_VALUE_OVERFLOW
                        : ts2diff_java_detail::FLAG_SCALED_VALUE_OVERFLOW,
                    out_stream))) {
                return ret;
            }
            if (RET_FAIL(common::SerializationUtil::write_var_uint(
                    static_cast<uint32_t>(num_values), out_stream))) {
                return ret;
            }
            const uint32_t bm_len = static_cast<uint32_t>(num_values / 8 + 1);
            if (RET_FAIL(out_stream.write_buf(scaled_bitmap.data(), bm_len))) {
                return ret;
            }
            if (has_raw_bits && RET_FAIL(out_stream.write_buf(
                                    raw_bits_bitmap.data(), bm_len))) {
                return ret;
            }
        }
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                static_cast<uint32_t>(max_point_number_), out_stream))) {
            return ret;
        }
        if (RET_FAIL(common::merge_byte_stream(out_stream, data))) {
            return ret;
        }
        underflow_flags_.clear();
        return ret;
    }

    int get_max_byte_size() override {
        return int_encoder_.get_max_byte_size() + 16 +
               static_cast<int>(underflow_flags_.size() / 4 + 2);
    }

   private:
    int64_t convert_double_to_long(double value) {
        const double scaled = value * max_point_value_;
        if (scaled > static_cast<double>(std::numeric_limits<int64_t>::max()) ||
            scaled < static_cast<double>(std::numeric_limits<int64_t>::min())) {
            if (std::isnan(value) ||
                value >
                    static_cast<double>(std::numeric_limits<int64_t>::max()) ||
                value <
                    static_cast<double>(std::numeric_limits<int64_t>::min())) {
                underflow_flags_.push_back(-1);
                return common::double_to_long(value);
            }
            underflow_flags_.push_back(0);
            return java_round<int64_t>(value);
        }
        if (std::isnan(value)) {
            underflow_flags_.push_back(-1);
            return common::double_to_long(value);
        }
        underflow_flags_.push_back(1);
        return java_round<int64_t>(scaled);
    }

    bool has_overflow() const {
        for (int8_t f : underflow_flags_) {
            if (f != 1) {
                return true;
            }
        }
        return false;
    }

    Int64RleEncoder int_encoder_;
    std::vector<int8_t> underflow_flags_;
    int max_point_number_ = 2;
    double max_point_value_ = 100.0;
};

}  // namespace storage

#endif  // ENCODING_FLOAT_RLE_ENCODER_H
