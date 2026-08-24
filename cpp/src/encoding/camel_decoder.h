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

#ifndef ENCODING_CAMEL_DECODER_H
#define ENCODING_CAMEL_DECODER_H

#include <stdint.h>

#include <limits>

#include "bit_packing_utils.h"
#include "camel_encoder.h"
#include "common/allocator/byte_stream.h"
#include "common/db_common.h"
#include "decoder.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

class CamelDecoder : public Decoder {
   public:
    CamelDecoder() { reset(); }
    ~CamelDecoder() override = default;

    void reset() override {
        type_ = common::CAMEL;
        is_first_ = true;
        stored_val_ = 0;
        previous_value_ = 0;
        scale_ = 1;
        leading_zeros_ = std::numeric_limits<int>::max();
        trailing_zeros_ = 0;
        bits_remaining_ = 0;
        bit_reader_.reset();
    }

    bool has_remaining(const common::ByteStream& buffer) override {
        return bits_remaining_ > 0 || buffer.has_remaining();
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
    int read_float(float&, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String&, common::PageArena&,
                    common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int read_double(double& ret_value, common::ByteStream& in) override {
        int ret = common::E_OK;
        if (bits_remaining_ == 0 && RET_FAIL(read_block_header(in))) {
            return ret;
        }
        if (is_first_) {
            uint64_t bits = 0;
            if (RET_FAIL(
                    read_bits(bits, camel_detail::BITS_FOR_FIRST_VALUE, in))) {
                return ret;
            }
            ret_value = camel_detail::long_bits_to_double(bits);
            stored_val_ = camel_detail::java_double_to_long(ret_value);
            is_first_ = false;
        } else if (RET_FAIL(read_next_value(ret_value, in))) {
            return ret;
        }
        previous_value_ = camel_detail::java_double_to_long_bits(ret_value);
        if (bits_remaining_ == 0) {
            bit_reader_.reset();
        }
        return common::E_OK;
    }

   private:
    int read_block_header(common::ByteStream& in) {
        int ret = common::E_OK;
        int32_t block_bits = 0;
        if (RET_FAIL(common::SerializationUtil::read_var_int(block_bits, in))) {
            return ret;
        }
        if (block_bits <= 0) {
            return common::E_TSFILE_CORRUPTED;
        }
        is_first_ = true;
        stored_val_ = 0;
        previous_value_ = 0;
        scale_ = 1;
        leading_zeros_ = std::numeric_limits<int>::max();
        trailing_zeros_ = 0;
        bits_remaining_ = block_bits;
        bit_reader_.reset();
        return common::E_OK;
    }

    int read_bit(bool& value, common::ByteStream& in) {
        if (bits_remaining_ < 1) {
            return common::E_BUF_NOT_ENOUGH;
        }
        int ret = bit_reader_.read_bit(value, in);
        if (ret == common::E_OK) {
            --bits_remaining_;
        }
        return ret;
    }

    int read_bits(uint64_t& value, int bits, common::ByteStream& in) {
        if (bits < 0 || bits_remaining_ < bits) {
            return common::E_BUF_NOT_ENOUGH;
        }
        int ret = bit_reader_.read_bits(value, bits, in);
        if (ret == common::E_OK) {
            bits_remaining_ -= bits;
        }
        return ret;
    }

    int read_int_bits(int& value, int bits, common::ByteStream& in) {
        uint64_t data = 0;
        int ret = read_bits(data, bits, in);
        if (ret == common::E_OK) {
            value = static_cast<int>(data);
        }
        return ret;
    }

    int read_next_value(double& value, common::ByteStream& in) {
        int ret = common::E_OK;
        int sign_bit = 0;
        int type_bit = 0;
        if (RET_FAIL(
                read_int_bits(sign_bit, camel_detail::BITS_FOR_SIGN, in)) ||
            RET_FAIL(
                read_int_bits(type_bit, camel_detail::BITS_FOR_TYPE, in))) {
            return ret;
        }
        double sign = sign_bit == 1 ? -1.0 : 1.0;
        if (type_bit == 1) {
            int64_t int_part = 0;
            double dec_part = 0;
            if (RET_FAIL(read_camel_long(int_part, in)) ||
                RET_FAIL(read_decimal(dec_part, in))) {
                return ret;
            }
            double abs_value =
                int_part >= 0
                    ? (static_cast<double>(int_part) * scale_ + dec_part) /
                          scale_
                    : -(static_cast<double>(int_part) * scale_ + dec_part) /
                          scale_;
            value = sign * abs_value;
            return common::E_OK;
        }

        double gorilla_value = 0;
        if (RET_FAIL(decode_gorilla(gorilla_value, in))) {
            return ret;
        }
        value = sign * gorilla_value;
        return common::E_OK;
    }

    int read_camel_long(int64_t& value, common::ByteStream& in) {
        int ret = common::E_OK;
        int64_t diff = 0;
        if (RET_FAIL(read_var_long(diff, in))) {
            return ret;
        }
        stored_val_ = static_cast<int64_t>(
            camel_detail::add_as_java_long(stored_val_, diff));
        value = stored_val_;
        return common::E_OK;
    }

    int read_decimal(double& value, common::ByteStream& in) {
        int ret = common::E_OK;
        int count = 0;
        if (RET_FAIL(read_int_bits(count, camel_detail::BITS_FOR_DECIMAL_COUNT,
                                   in))) {
            return ret;
        }
        ++count;
        if (count < 1 || count > camel_detail::DECODER_DECIMAL_MAX_COUNT) {
            return common::E_TSFILE_CORRUPTED;
        }

        bool has_xor = false;
        if (RET_FAIL(read_bit(has_xor, in))) {
            return ret;
        }
        uint64_t xor_value = 0;
        if (has_xor) {
            uint64_t bits = 0;
            if (RET_FAIL(read_bits(bits, count, in))) {
                return ret;
            }
            xor_value = bits << (camel_detail::DOUBLE_MANTISSA_BITS - count);
        }

        int64_t m_value = 0;
        if (RET_FAIL(read_var_long(m_value, in))) {
            return ret;
        }

        int64_t pow = camel_detail::POWERS_10[count - 1];
        double frac = 0;
        if (has_xor) {
            double base =
                static_cast<double>(m_value) / static_cast<double>(pow) + 1;
            uint64_t merged =
                xor_value ^ camel_detail::java_double_to_long_bits(base);
            frac = camel_detail::long_bits_to_double(merged) - 1;
        } else {
            frac = static_cast<double>(m_value) / static_cast<double>(pow);
        }

        scale_ = static_cast<double>(pow);
        value =
            static_cast<double>(camel_detail::java_math_round(frac * scale_));
        return common::E_OK;
    }

    int read_var_long(int64_t& value, common::ByteStream& in) {
        int ret = common::E_OK;
        uint64_t result = 0;
        int shift = 0;
        while (true) {
            uint64_t chunk = 0;
            bool has_next = false;
            if (RET_FAIL(read_bits(chunk, 7, in)) ||
                RET_FAIL(read_bit(has_next, in))) {
                return ret;
            }
            result |= chunk << shift;
            shift += 7;
            if (!has_next) {
                break;
            }
            if (shift >= 64) {
                return common::E_TSFILE_CORRUPTED;
            }
        }
        uint64_t decoded = (result >> 1) ^ (UINT64_C(0) - (result & 1U));
        value = static_cast<int64_t>(decoded);
        return common::E_OK;
    }

    int decode_gorilla(double& value, common::ByteStream& in) {
        int ret = common::E_OK;
        if (is_first_) {
            uint64_t bits = 0;
            if (RET_FAIL(
                    read_bits(bits, camel_detail::BITS_FOR_FIRST_VALUE, in))) {
                return ret;
            }
            previous_value_ = bits;
            is_first_ = false;
            value = camel_detail::long_bits_to_double(previous_value_);
            return common::E_OK;
        }

        bool control_bit = false;
        if (RET_FAIL(read_bit(control_bit, in))) {
            return ret;
        }
        if (!control_bit) {
            value = camel_detail::long_bits_to_double(previous_value_);
            return common::E_OK;
        }

        bool write_new_block = false;
        if (RET_FAIL(read_bit(write_new_block, in))) {
            return ret;
        }
        uint64_t xor_value = 0;
        if (!write_new_block) {
            int significant_bits = camel_detail::DOUBLE_TOTAL_BITS -
                                   leading_zeros_ - trailing_zeros_;
            if (significant_bits == 0) {
                value = camel_detail::long_bits_to_double(previous_value_);
                return common::E_OK;
            }
            if (RET_FAIL(read_bits(xor_value, significant_bits, in))) {
                return ret;
            }
            xor_value <<= trailing_zeros_;
        } else {
            int significant_bits = 0;
            if (RET_FAIL(read_int_bits(leading_zeros_,
                                       camel_detail::BITS_FOR_LEADING_ZEROS,
                                       in)) ||
                RET_FAIL(read_int_bits(significant_bits,
                                       camel_detail::BITS_FOR_SIGNIFICANT_BITS,
                                       in))) {
                return ret;
            }
            ++significant_bits;
            trailing_zeros_ = camel_detail::DOUBLE_TOTAL_BITS - leading_zeros_ -
                              significant_bits;
            if (trailing_zeros_ < 0) {
                return common::E_TSFILE_CORRUPTED;
            }
            if (RET_FAIL(read_bits(xor_value, significant_bits, in))) {
                return ret;
            }
            xor_value <<= trailing_zeros_;
        }

        previous_value_ ^= xor_value;
        value = camel_detail::long_bits_to_double(previous_value_);
        return common::E_OK;
    }

   private:
    common::TSEncoding type_;
    bool is_first_;
    int64_t stored_val_;
    uint64_t previous_value_;
    double scale_;
    int leading_zeros_;
    int trailing_zeros_;
    int bits_remaining_;
    MsbBitReader bit_reader_;
};

}  // namespace storage

#endif  // ENCODING_CAMEL_DECODER_H
