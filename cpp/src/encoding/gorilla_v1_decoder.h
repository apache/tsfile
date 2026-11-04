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

#ifndef ENCODING_GORILLA_V1_DECODER_H
#define ENCODING_GORILLA_V1_DECODER_H

#include <cmath>
#include <cstdint>

#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "encoding/encode_utils.h"

namespace storage {

// Decoder for the original Gorilla (GORILLA_V1) layout written by Java's
// SinglePrecisionEncoderV1/DoublePrecisionEncoderV1.  Values are decoded one
// step ahead, exactly like the Java decoder, because the NaN sentinel that
// flush() appends is what marks the end of the stream.
class GorillaV1DecoderBase : public Decoder {
   public:
    void reset() override { reset_state(); }

   protected:
    void reset_state() {
        flag_ = false;
        leading_zero_num_ = 0;
        trailing_zero_num_ = 0;
        is_end_ = false;
        buffer_ = 0;
        bits_left_ = 0;
        read_error_ = common::E_OK;
    }

    bool read_bit(common::ByteStream& in) {
        if (bits_left_ == 0 && !is_end_) {
            fill_buffer(in);
        }
        if (buffer_ < 0) {
            return false;
        }
        --bits_left_;
        return ((buffer_ >> bits_left_) & 1) == 1;
    }

    void fill_buffer(common::ByteStream& in) {
        uint8_t byte = 0;
        uint32_t read_len = 0;
        if (in.read_buf(&byte, 1, read_len) == common::E_OK && read_len == 1) {
            buffer_ = byte;
            bits_left_ = 8;
        } else {
            // A stream truncated mid-value must not be decoded as a run of
            // zero bits; latch the error so read_float/read_double surface it.
            buffer_ = -1;
            bits_left_ = -1;
            read_error_ = common::E_BUF_NOT_ENOUGH;
        }
    }

    int read_bits_as_int(common::ByteStream& in, int len) {
        int num = 0;
        for (int i = 0; i < len; i++) {
            num = (num << 1) | (read_bit(in) ? 1 : 0);
        }
        return num;
    }

    bool flag_ = false;
    int leading_zero_num_ = 0;
    int trailing_zero_num_ = 0;
    bool is_end_ = false;
    int buffer_ = 0;
    int bits_left_ = 0;
    int read_error_ = common::E_OK;
};

class FloatGorillaV1Decoder : public GorillaV1DecoderBase {
   public:
    bool has_remaining(const common::ByteStream& buffer) override {
        return buffer.has_remaining();
    }

    int read_float(float& ret_value, common::ByteStream& in) override {
        if (!flag_) {
            flag_ = true;
            uint8_t bytes[4] = {0, 0, 0, 0};
            uint32_t read_len = 0;
            if (in.read_buf(bytes, 4, read_len) != common::E_OK ||
                read_len != 4) {
                return common::E_DECODE_ERR;
            }
            pre_value_ = static_cast<uint32_t>(bytes[0]) |
                         (static_cast<uint32_t>(bytes[1]) << 8) |
                         (static_cast<uint32_t>(bytes[2]) << 16) |
                         (static_cast<uint32_t>(bytes[3]) << 24);
            leading_zero_num_ =
                number_of_leading_zeros(static_cast<int32_t>(pre_value_));
            trailing_zero_num_ =
                number_of_trailing_zeros(static_cast<int32_t>(pre_value_));
            ret_value = common::int_to_float(static_cast<int32_t>(pre_value_));
            prefetch(in);
            return read_error_;
        }
        ret_value = common::int_to_float(static_cast<int32_t>(pre_value_));
        prefetch(in);
        return read_error_;
    }

    int read_boolean(bool& /*ret_value*/, common::ByteStream& /*in*/) override {
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
    int read_double(double& /*ret_value*/,
                    common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String& /*ret_value*/, common::PageArena& /*pa*/,
                    common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

   private:
    void prefetch(common::ByteStream& in) {
        const bool next_flag1 = read_bit(in);
        if (!next_flag1) {
            return;
        }
        const bool next_flag2 = read_bit(in);
        if (!next_flag2) {
            const int len = 32 - leading_zero_num_ - trailing_zero_num_;
            if (len < 0 || len > 32) {
                read_error_ = common::E_DECODE_ERR;
                return;
            }
            uint32_t xor_value = 0;
            for (int i = 0; i < len; i++) {
                xor_value = (xor_value << 1) | (read_bit(in) ? 1u : 0u);
            }
            xor_value <<= trailing_zero_num_;
            pre_value_ = xor_value ^ pre_value_;
        } else {
            const int leading = read_bits_as_int(in, 5);
            const int len = read_bits_as_int(in, 6);
            if (len < 0 || leading < 0 || leading + len > 32) {
                read_error_ = common::E_DECODE_ERR;
                return;
            }
            uint32_t xor_value = 0;
            for (int i = 0; i < len; i++) {
                xor_value = (xor_value << 1) | (read_bit(in) ? 1u : 0u);
            }
            xor_value <<= (32 - leading - len);
            pre_value_ = xor_value ^ pre_value_;
        }
        leading_zero_num_ =
            number_of_leading_zeros(static_cast<int32_t>(pre_value_));
        trailing_zero_num_ =
            number_of_trailing_zeros(static_cast<int32_t>(pre_value_));
        if (std::isnan(
                common::int_to_float(static_cast<int32_t>(pre_value_)))) {
            is_end_ = true;
        }
    }

    uint32_t pre_value_ = 0;
};

class DoubleGorillaV1Decoder : public GorillaV1DecoderBase {
   public:
    bool has_remaining(const common::ByteStream& buffer) override {
        return buffer.has_remaining();
    }

    int read_double(double& ret_value, common::ByteStream& in) override {
        if (!flag_) {
            flag_ = true;
            uint8_t bytes[8] = {0, 0, 0, 0, 0, 0, 0, 0};
            uint32_t read_len = 0;
            if (in.read_buf(bytes, 8, read_len) != common::E_OK ||
                read_len != 8) {
                return common::E_DECODE_ERR;
            }
            pre_value_ = 0;
            for (int i = 0; i < 8; i++) {
                pre_value_ |= static_cast<uint64_t>(bytes[i]) << (i * 8);
            }
            leading_zero_num_ =
                number_of_leading_zeros(static_cast<int64_t>(pre_value_));
            trailing_zero_num_ =
                number_of_trailing_zeros(static_cast<int64_t>(pre_value_));
            ret_value =
                common::long_to_double(static_cast<int64_t>(pre_value_));
            prefetch(in);
            return read_error_;
        }
        ret_value = common::long_to_double(static_cast<int64_t>(pre_value_));
        prefetch(in);
        return read_error_;
    }

    int read_boolean(bool& /*ret_value*/, common::ByteStream& /*in*/) override {
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
    int read_float(float& /*ret_value*/, common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String& /*ret_value*/, common::PageArena& /*pa*/,
                    common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

   private:
    void prefetch(common::ByteStream& in) {
        const bool next_flag1 = read_bit(in);
        if (!next_flag1) {
            return;
        }
        const bool next_flag2 = read_bit(in);
        if (!next_flag2) {
            const int len = 64 - leading_zero_num_ - trailing_zero_num_;
            if (len < 0 || len > 64) {
                read_error_ = common::E_DECODE_ERR;
                return;
            }
            uint64_t xor_value = 0;
            for (int i = 0; i < len; i++) {
                xor_value = (xor_value << 1) | (read_bit(in) ? 1ULL : 0ULL);
            }
            xor_value <<= trailing_zero_num_;
            pre_value_ = xor_value ^ pre_value_;
        } else {
            const int leading = read_bits_as_int(in, 6);
            const int len = read_bits_as_int(in, 7);
            if (len < 0 || leading < 0 || leading + len > 64) {
                read_error_ = common::E_DECODE_ERR;
                return;
            }
            uint64_t xor_value = 0;
            for (int i = 0; i < len; i++) {
                xor_value = (xor_value << 1) | (read_bit(in) ? 1ULL : 0ULL);
            }
            xor_value <<= (64 - leading - len);
            pre_value_ = xor_value ^ pre_value_;
        }
        leading_zero_num_ =
            number_of_leading_zeros(static_cast<int64_t>(pre_value_));
        trailing_zero_num_ =
            number_of_trailing_zeros(static_cast<int64_t>(pre_value_));
        if (std::isnan(
                common::long_to_double(static_cast<int64_t>(pre_value_)))) {
            is_end_ = true;
        }
    }

    uint64_t pre_value_ = 0;
};

}  // namespace storage

#endif  // ENCODING_GORILLA_V1_DECODER_H
