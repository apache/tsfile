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

#ifndef ENCODING_CAMEL_ENCODER_H
#define ENCODING_CAMEL_ENCODER_H

#include <math.h>
#include <stdint.h>

#include <limits>

#include "bit_packing_utils.h"
#include "common/allocator/byte_stream.h"
#include "common/db_common.h"
#include "encode_utils.h"
#include "encoder.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

namespace camel_detail {

static constexpr int BITS_FOR_SIGN = 1;
static constexpr int BITS_FOR_TYPE = 1;
static constexpr int BITS_FOR_FIRST_VALUE = 64;
static constexpr int BITS_FOR_LEADING_ZEROS = 6;
static constexpr int BITS_FOR_SIGNIFICANT_BITS = 6;
static constexpr int BITS_FOR_DECIMAL_COUNT = 4;
static constexpr int DOUBLE_TOTAL_BITS = 64;
static constexpr int DOUBLE_MANTISSA_BITS = 52;
static constexpr int ENCODER_DECIMAL_MAX_COUNT = 10;
static constexpr int DECODER_DECIMAL_MAX_COUNT = 15;

static constexpr uint64_t JAVA_CANONICAL_DOUBLE_NAN =
    UINT64_C(0x7ff8000000000000);

static constexpr int64_t POWERS_10[DECODER_DECIMAL_MAX_COUNT] = {
    INT64_C(10),
    INT64_C(100),
    INT64_C(1000),
    INT64_C(10000),
    INT64_C(100000),
    INT64_C(1000000),
    INT64_C(10000000),
    INT64_C(100000000),
    INT64_C(1000000000),
    INT64_C(10000000000),
    INT64_C(100000000000),
    INT64_C(1000000000000),
    INT64_C(10000000000000),
    INT64_C(100000000000000),
    INT64_C(1000000000000000),
};

static constexpr int64_t THRESHOLDS[DECODER_DECIMAL_MAX_COUNT] = {
    INT64_C(5),          INT64_C(25),         INT64_C(125),
    INT64_C(625),        INT64_C(3125),       INT64_C(15625),
    INT64_C(78125),      INT64_C(390625),     INT64_C(1953125),
    INT64_C(9765625),    INT64_C(48828125),   INT64_C(244140625),
    INT64_C(1220703125), INT64_C(6103515625), INT64_C(30517578125),
};

FORCE_INLINE uint64_t double_to_raw_bits(double value) {
    return static_cast<uint64_t>(common::double_to_long(value));
}

FORCE_INLINE uint64_t java_double_to_long_bits(double value) {
    if (value != value) {
        return JAVA_CANONICAL_DOUBLE_NAN;
    }
    return double_to_raw_bits(value);
}

FORCE_INLINE double long_bits_to_double(uint64_t bits) {
    return common::long_to_double(static_cast<int64_t>(bits));
}

FORCE_INLINE int64_t java_double_to_long(double value) {
    if (value != value) {
        return 0;
    }
    const double upper = ldexp(1.0, 63);
    const double lower = -ldexp(1.0, 63);
    if (value >= upper) {
        return std::numeric_limits<int64_t>::max();
    }
    if (value <= lower) {
        return std::numeric_limits<int64_t>::min();
    }
    return static_cast<int64_t>(value);
}

FORCE_INLINE int64_t java_math_round(double value) {
    if (value != value) {
        return 0;
    }
    return java_double_to_long(floor(value + 0.5));
}

FORCE_INLINE uint64_t add_as_java_long(int64_t left, int64_t right) {
    return static_cast<uint64_t>(left) + static_cast<uint64_t>(right);
}

FORCE_INLINE uint64_t sub_as_java_long(int64_t left, int64_t right) {
    return static_cast<uint64_t>(left) - static_cast<uint64_t>(right);
}

}  // namespace camel_detail

class CamelEncoder : public Encoder {
   public:
    CamelEncoder()
        : bit_buffer_(common::ByteStream::DEFAULT_PAGE_SIZE,
                      common::MOD_ENCODER_OBJ) {
        reset();
    }
    ~CamelEncoder() override = default;

    void destroy() override { bit_buffer_.destroy(); }

    void reset() override {
        type_ = common::CAMEL;
        bit_buffer_.reset();
        bit_writer_.reset();
        reset_state();
    }

    int flush(common::ByteStream& out) override {
        int ret = common::E_OK;
        if (!has_pending_) {
            return common::E_OK;
        }
        if (RET_FAIL(bit_writer_.flush_partial(bit_buffer_))) {
            return ret;
        }
        if (RET_FAIL(common::SerializationUtil::write_var_int(
                bit_writer_.bits_written(), out))) {
            return ret;
        }
        if (RET_FAIL(common::merge_byte_stream(out, bit_buffer_))) {
            return ret;
        }
        bit_buffer_.reset();
        bit_writer_.reset();
        reset_state();
        return common::E_OK;
    }

    int get_max_byte_size() override {
        return static_cast<int>(bit_buffer_.allocated_bytes() + 32);
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
    int encode(float, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int encode(double value, common::ByteStream&) override {
        int ret = add_value(value);
        if (ret == common::E_OK) {
            has_pending_ = true;
        }
        return ret;
    }

   private:
    void reset_state() {
        is_first_ = true;
        has_pending_ = false;
        stored_val_ = 0;
        previous_value_ = 0;
        leading_zeros_ = std::numeric_limits<int>::max();
        trailing_zeros_ = 0;
    }

    int add_value(double value) {
        int ret = common::E_OK;
        if (is_first_) {
            if (RET_FAIL(
                    write_first(camel_detail::double_to_raw_bits(value)))) {
                return ret;
            }
        } else if (RET_FAIL(compress_value(value))) {
            return ret;
        }
        previous_value_ = camel_detail::java_double_to_long_bits(value);
        return common::E_OK;
    }

    int write_first(uint64_t value) {
        is_first_ = false;
        stored_val_ = camel_detail::java_double_to_long(
            camel_detail::long_bits_to_double(value));
        return bit_writer_.write_bits(value, camel_detail::BITS_FOR_FIRST_VALUE,
                                      bit_buffer_);
    }

    int compress_value(double value) {
        int ret = common::E_OK;
        int sign_bit =
            static_cast<int>((camel_detail::java_double_to_long_bits(value) >>
                              (camel_detail::DOUBLE_TOTAL_BITS - 1)) &
                             1U);
        if (RET_FAIL(bit_writer_.write_bits(static_cast<uint64_t>(sign_bit),
                                            camel_detail::BITS_FOR_SIGN,
                                            bit_buffer_))) {
            return ret;
        }

        value = fabs(value);
        if (value > static_cast<double>(std::numeric_limits<int64_t>::max()) ||
            value == 0 ||
            fabs(floor(log10(value))) >
                camel_detail::ENCODER_DECIMAL_MAX_COUNT) {
            if (RET_FAIL(bit_writer_.write_bits(0, camel_detail::BITS_FOR_TYPE,
                                                bit_buffer_))) {
                return ret;
            }
            return encode_gorilla(value);
        }

        int64_t integer_part = camel_detail::java_double_to_long(value);
        int num_digits = 1;
        int64_t abs_int = integer_part < 0 ? -integer_part : integer_part;
        while (abs_int >= 10) {
            abs_int /= 10;
            ++num_digits;
        }

        double factor = 1;
        int decimal_count = 0;
        while (fabs(value * factor -
                    static_cast<double>(
                        camel_detail::java_math_round(value * factor))) > 0) {
            factor *= 10.0;
            ++decimal_count;
            if (num_digits + decimal_count >
                camel_detail::ENCODER_DECIMAL_MAX_COUNT) {
                break;
            }
        }
        if (decimal_count < 1) {
            decimal_count = 1;
        }

        if (decimal_count + num_digits <=
            camel_detail::ENCODER_DECIMAL_MAX_COUNT) {
            int64_t pow = camel_detail::POWERS_10[decimal_count - 1];
            int64_t decimal_value = camel_detail::java_math_round(
                                        value * static_cast<double>(pow)) %
                                    pow;
            if (RET_FAIL(bit_writer_.write_bits(1, camel_detail::BITS_FOR_TYPE,
                                                bit_buffer_))) {
                return ret;
            }
            if (RET_FAIL(compress_integer_value(integer_part)) ||
                RET_FAIL(
                    compress_decimal_value(decimal_value, decimal_count))) {
                return ret;
            }
            return common::E_OK;
        }

        if (RET_FAIL(bit_writer_.write_bits(0, camel_detail::BITS_FOR_TYPE,
                                            bit_buffer_))) {
            return ret;
        }
        return encode_gorilla(value);
    }

    int compress_integer_value(int64_t value) {
        int64_t diff = static_cast<int64_t>(
            camel_detail::sub_as_java_long(value, stored_val_));
        stored_val_ = value;
        return write_var_long(diff);
    }

    int compress_decimal_value(int64_t decimal_value, int decimal_count) {
        int ret = common::E_OK;
        if (RET_FAIL(bit_writer_.write_bits(
                static_cast<uint64_t>(decimal_count - 1),
                camel_detail::BITS_FOR_DECIMAL_COUNT, bit_buffer_))) {
            return ret;
        }

        int64_t threshold = camel_detail::THRESHOLDS[decimal_count - 1];
        int64_t m = static_cast<int64_t>(
            static_cast<int32_t>(static_cast<uint32_t>(decimal_value)));

        if (decimal_value >= threshold) {
            if (RET_FAIL(bit_writer_.write_bit(true, bit_buffer_))) {
                return ret;
            }
            m = static_cast<int64_t>(static_cast<int32_t>(
                static_cast<uint32_t>(decimal_value % threshold)));

            int64_t pow = camel_detail::POWERS_10[decimal_count - 1];
            uint64_t xor_value =
                camel_detail::java_double_to_long_bits(
                    static_cast<double>(decimal_value) /
                        static_cast<double>(pow) +
                    1) ^
                camel_detail::java_double_to_long_bits(
                    static_cast<double>(m) / static_cast<double>(pow) + 1);
            if (RET_FAIL(bit_writer_.write_bits(
                    xor_value >>
                        (camel_detail::DOUBLE_MANTISSA_BITS - decimal_count),
                    decimal_count, bit_buffer_))) {
                return ret;
            }
        } else if (RET_FAIL(bit_writer_.write_bit(false, bit_buffer_))) {
            return ret;
        }

        return write_var_long(m);
    }

    int write_var_long(int64_t value) {
        int ret = common::E_OK;
        uint64_t u_value = (static_cast<uint64_t>(value) << 1) ^
                           (value < 0 ? UINT64_MAX : UINT64_C(0));
        while ((u_value & ~UINT64_C(0x7F)) != 0) {
            if (RET_FAIL(bit_writer_.write_bits(u_value & UINT64_C(0x7F), 7,
                                                bit_buffer_)) ||
                RET_FAIL(bit_writer_.write_bit(true, bit_buffer_))) {
                return ret;
            }
            u_value >>= 7;
        }
        if (RET_FAIL(bit_writer_.write_bits(u_value & UINT64_C(0x7F), 7,
                                            bit_buffer_)) ||
            RET_FAIL(bit_writer_.write_bit(false, bit_buffer_))) {
            return ret;
        }
        return common::E_OK;
    }

    int encode_gorilla(double value) {
        int ret = common::E_OK;
        uint64_t curr = camel_detail::java_double_to_long_bits(value);
        if (is_first_) {
            if (RET_FAIL(bit_writer_.write_bits(
                    curr, camel_detail::BITS_FOR_FIRST_VALUE, bit_buffer_))) {
                return ret;
            }
            previous_value_ = curr;
            is_first_ = false;
            return common::E_OK;
        }

        uint64_t xor_value = curr ^ previous_value_;
        if (xor_value == 0) {
            if (RET_FAIL(bit_writer_.write_bit(false, bit_buffer_))) {
                return ret;
            }
        } else {
            if (RET_FAIL(bit_writer_.write_bit(true, bit_buffer_))) {
                return ret;
            }
            int leading =
                number_of_leading_zeros(static_cast<int64_t>(xor_value));
            int trailing =
                number_of_trailing_zeros(static_cast<int64_t>(xor_value));
            if (leading >= leading_zeros_ && trailing >= trailing_zeros_) {
                if (RET_FAIL(bit_writer_.write_bit(false, bit_buffer_))) {
                    return ret;
                }
                int significant_bits = camel_detail::DOUBLE_TOTAL_BITS -
                                       leading_zeros_ - trailing_zeros_;
                if (RET_FAIL(bit_writer_.write_bits(
                        xor_value >> trailing_zeros_, significant_bits,
                        bit_buffer_))) {
                    return ret;
                }
            } else {
                if (RET_FAIL(bit_writer_.write_bit(true, bit_buffer_)) ||
                    RET_FAIL(bit_writer_.write_bits(
                        static_cast<uint64_t>(leading),
                        camel_detail::BITS_FOR_LEADING_ZEROS, bit_buffer_))) {
                    return ret;
                }
                int significant_bits =
                    camel_detail::DOUBLE_TOTAL_BITS - leading - trailing;
                if (RET_FAIL(bit_writer_.write_bits(
                        static_cast<uint64_t>(significant_bits - 1),
                        camel_detail::BITS_FOR_SIGNIFICANT_BITS,
                        bit_buffer_)) ||
                    RET_FAIL(bit_writer_.write_bits(xor_value >> trailing,
                                                    significant_bits,
                                                    bit_buffer_))) {
                    return ret;
                }
                leading_zeros_ = leading;
                trailing_zeros_ = trailing;
            }
        }

        previous_value_ = curr;
        return common::E_OK;
    }

   private:
    common::TSEncoding type_;
    bool is_first_;
    bool has_pending_;
    int64_t stored_val_;
    uint64_t previous_value_;
    int leading_zeros_;
    int trailing_zeros_;
    MsbBitWriter bit_writer_;
    common::ByteStream bit_buffer_;
};

}  // namespace storage

#endif  // ENCODING_CAMEL_ENCODER_H
