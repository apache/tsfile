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

#ifndef ENCODING_GORILLA_V1_ENCODER_H
#define ENCODING_GORILLA_V1_ENCODER_H

#include <cmath>
#include <cstdint>

#include "common/allocator/byte_stream.h"
#include "encoder.h"
#include "encoding/encode_utils.h"

namespace storage {

// The original Gorilla (GORILLA_V1) bit layout used by Java's
// SinglePrecisionEncoderV1/DoublePrecisionEncoderV1: the first value is
// stored raw little-endian, later values are an MSB-first bit stream of
// reuse-XOR ('0' / '10') or explicit-XOR ('11') encodings, terminated by a
// NaN sentinel written by flush().
class GorillaV1EncoderBase : public Encoder {
   public:
    void destroy() override {}

   protected:
    void reset_bits() {
        flag_ = false;
        leading_zero_num_ = 0;
        trailing_zero_num_ = 0;
        buffer_ = 0;
        bits_in_buffer_ = 0;
    }

    void write_bit(bool bit, common::ByteStream& out_stream) {
        buffer_ = static_cast<uint8_t>(buffer_ << 1);
        if (bit) {
            buffer_ |= 1u;
        }
        if (++bits_in_buffer_ == 8) {
            clear_buffer(out_stream);
        }
    }

    void write_bits(uint64_t num, common::ByteStream& out_stream, int start,
                    int end) {
        for (int i = start; i >= end; i--) {
            write_bit(((num >> i) & 1ULL) != 0, out_stream);
        }
    }

    void clear_buffer(common::ByteStream& out_stream) {
        if (bits_in_buffer_ == 0) {
            return;
        }
        buffer_ = static_cast<uint8_t>(buffer_ << (8 - bits_in_buffer_));
        out_stream.write_buf(&buffer_, 1);
        bits_in_buffer_ = 0;
        buffer_ = 0;
    }

    bool flag_ = false;
    int leading_zero_num_ = 0;
    int trailing_zero_num_ = 0;
    uint8_t buffer_ = 0;
    int bits_in_buffer_ = 0;
};

class FloatGorillaV1Encoder : public GorillaV1EncoderBase {
   public:
    void reset() override { reset_bits(); }

    int encode(float value, common::ByteStream& out_stream) override {
        uint32_t next_value = float_bits(value);
        if (!flag_) {
            flag_ = true;
            pre_value_ = next_value;
            leading_zero_num_ =
                number_of_leading_zeros(static_cast<int32_t>(pre_value_));
            trailing_zero_num_ =
                number_of_trailing_zeros(static_cast<int32_t>(pre_value_));
            uint8_t bytes[4];
            for (int i = 0; i < 4; i++) {
                bytes[i] = static_cast<uint8_t>(pre_value_ >> (i * 8));
            }
            return out_stream.write_buf(bytes, 4);
        }
        const uint32_t xor_value = next_value ^ pre_value_;
        if (xor_value == 0) {
            write_bit(false, out_stream);
        } else {
            const int leading =
                number_of_leading_zeros(static_cast<int32_t>(xor_value));
            const int trailing =
                number_of_trailing_zeros(static_cast<int32_t>(xor_value));
            if (leading >= leading_zero_num_ &&
                trailing >= trailing_zero_num_) {
                write_bit(true, out_stream);
                write_bit(false, out_stream);
                write_bits(xor_value, out_stream, 31 - leading_zero_num_,
                           trailing_zero_num_);
            } else {
                write_bit(true, out_stream);
                write_bit(true, out_stream);
                write_bits(static_cast<uint64_t>(leading), out_stream, 4, 0);
                write_bits(static_cast<uint64_t>(32 - leading - trailing),
                           out_stream, 5, 0);
                write_bits(xor_value, out_stream, 31 - leading, trailing);
            }
        }
        pre_value_ = next_value;
        leading_zero_num_ =
            number_of_leading_zeros(static_cast<int32_t>(pre_value_));
        trailing_zero_num_ =
            number_of_trailing_zeros(static_cast<int32_t>(pre_value_));
        return common::E_OK;
    }

    int encode(bool /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(double /*value*/, common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String /*value*/,
               common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int flush(common::ByteStream& out_stream) override {
        int ret = encode(std::nanf(""), out_stream);
        if (RET_FAIL(ret)) {
            return ret;
        }
        clear_buffer(out_stream);
        reset();
        return common::E_OK;
    }

    int get_max_byte_size() override { return 12; }

   private:
    static uint32_t float_bits(float value) {
        // Java uses Float.floatToIntBits, which canonicalizes every NaN.
        if (std::isnan(value)) {
            return 0x7fc00000U;
        }
        return static_cast<uint32_t>(common::float_to_int(value));
    }

    uint32_t pre_value_ = 0;
};

class DoubleGorillaV1Encoder : public GorillaV1EncoderBase {
   public:
    void reset() override { reset_bits(); }

    int encode(double value, common::ByteStream& out_stream) override {
        uint64_t next_value = double_bits(value);
        if (!flag_) {
            flag_ = true;
            pre_value_ = next_value;
            leading_zero_num_ =
                number_of_leading_zeros(static_cast<int64_t>(pre_value_));
            trailing_zero_num_ =
                number_of_trailing_zeros(static_cast<int64_t>(pre_value_));
            uint8_t bytes[8];
            for (int i = 0; i < 8; i++) {
                bytes[i] = static_cast<uint8_t>(pre_value_ >> (i * 8));
            }
            return out_stream.write_buf(bytes, 8);
        }
        const uint64_t xor_value = next_value ^ pre_value_;
        if (xor_value == 0) {
            write_bit(false, out_stream);
        } else {
            const int leading =
                number_of_leading_zeros(static_cast<int64_t>(xor_value));
            const int trailing =
                number_of_trailing_zeros(static_cast<int64_t>(xor_value));
            if (leading >= leading_zero_num_ &&
                trailing >= trailing_zero_num_) {
                write_bit(true, out_stream);
                write_bit(false, out_stream);
                write_bits(xor_value, out_stream, 63 - leading_zero_num_,
                           trailing_zero_num_);
            } else {
                write_bit(true, out_stream);
                write_bit(true, out_stream);
                write_bits(static_cast<uint64_t>(leading), out_stream, 5, 0);
                write_bits(static_cast<uint64_t>(64 - leading - trailing),
                           out_stream, 6, 0);
                write_bits(xor_value, out_stream, 63 - leading, trailing);
            }
        }
        pre_value_ = next_value;
        leading_zero_num_ =
            number_of_leading_zeros(static_cast<int64_t>(pre_value_));
        trailing_zero_num_ =
            number_of_trailing_zeros(static_cast<int64_t>(pre_value_));
        return common::E_OK;
    }

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
    int encode(common::String /*value*/,
               common::ByteStream& /*out_stream*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int flush(common::ByteStream& out_stream) override {
        int ret = encode(std::nan(""), out_stream);
        if (RET_FAIL(ret)) {
            return ret;
        }
        clear_buffer(out_stream);
        reset();
        return common::E_OK;
    }

    int get_max_byte_size() override { return 20; }

   private:
    static uint64_t double_bits(double value) {
        // Java uses Double.doubleToLongBits, which canonicalizes every NaN.
        if (std::isnan(value)) {
            return UINT64_C(0x7ff8000000000000);
        }
        return static_cast<uint64_t>(common::double_to_long(value));
    }

    uint64_t pre_value_ = 0;
};

}  // namespace storage

#endif  // ENCODING_GORILLA_V1_ENCODER_H
