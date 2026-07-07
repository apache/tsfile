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

#ifndef ENCODING_CHIMP_ENCODER_H
#define ENCODING_CHIMP_ENCODER_H

#include <stdint.h>

#include <climits>

#include "bit_packing_utils.h"
#include "common/allocator/byte_stream.h"
#include "common/db_common.h"
#include "encode_utils.h"
#include "encoder.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

static constexpr int8_t CHIMP_LEADING_REPRESENTATION[64] = {
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5,
    6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7};

static constexpr int8_t CHIMP_LEADING_ROUND[64] = {
    0,  0,  0,  0,  0,  0,  0,  0,  8,  8,  8,  8,  12, 12, 12, 12,
    16, 16, 18, 18, 20, 20, 22, 22, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24,
    24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24, 24};

static constexpr int32_t CHIMP_ENCODING_ENDING_INTEGER = INT32_MIN;
static constexpr int64_t CHIMP_ENCODING_ENDING_LONG = INT64_MIN;
static constexpr int32_t CHIMP_ENCODING_ENDING_FLOAT = 0x7fc00000;
static constexpr int64_t CHIMP_ENCODING_ENDING_DOUBLE =
    static_cast<int64_t>(0x7ff8000000000000ULL);

template <typename T>
struct ChimpTraits;

template <>
struct ChimpTraits<int32_t> {
    using Unsigned = uint32_t;
    static constexpr int VALUE_BITS = 32;
    static constexpr int PREVIOUS_VALUES = 64;
    static constexpr int PREVIOUS_VALUES_LOG2 = 6;
    static constexpr int THRESHOLD = 5 + PREVIOUS_VALUES_LOG2;
    static constexpr int SET_LSB = (1 << (THRESHOLD + 1)) - 1;
    static constexpr int CASE_ZERO_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 2;
    static constexpr int CASE_ONE_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 10;
    static constexpr int32_t ENDING = CHIMP_ENCODING_ENDING_INTEGER;
};

template <>
struct ChimpTraits<int64_t> {
    using Unsigned = uint64_t;
    static constexpr int VALUE_BITS = 64;
    static constexpr int PREVIOUS_VALUES = 128;
    static constexpr int PREVIOUS_VALUES_LOG2 = 7;
    static constexpr int THRESHOLD = 6 + PREVIOUS_VALUES_LOG2;
    static constexpr int SET_LSB = (1 << (THRESHOLD + 1)) - 1;
    static constexpr int CASE_ZERO_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 2;
    static constexpr int CASE_ONE_METADATA_LENGTH = PREVIOUS_VALUES_LOG2 + 11;
    static constexpr int64_t ENDING = CHIMP_ENCODING_ENDING_LONG;
};

template <typename T>
class ChimpEncoder : public Encoder {
   public:
    using Traits = ChimpTraits<T>;
    using Unsigned = typename Traits::Unsigned;

    ChimpEncoder() { reset(); }
    ~ChimpEncoder() override = default;

    void destroy() override {}

    void reset() override {
        type_ = common::CHIMP;
        first_value_was_written_ = false;
        stored_leading_zeros_ = INT32_MAX;
        writer_.reset();
        index_ = 0;
        current_ = 0;
        for (int i = 0; i < Traits::PREVIOUS_VALUES; ++i) {
            stored_values_[i] = 0;
        }
        for (int i = 0; i < (1 << (Traits::THRESHOLD + 1)); ++i) {
            indices_[i] = 0;
        }
    }

    int flush(common::ByteStream& out) override {
        int ret = encode_value(Traits::ENDING, out);
        if (RET_FAIL(ret)) {
            return ret;
        }
        ret = writer_.flush_forced(out);
        reset();
        return ret;
    }

    int get_max_byte_size() override {
        return (2 + (Traits::VALUE_BITS == 32 ? 5 : 6) +
                (Traits::VALUE_BITS == 32 ? 5 : 6) + Traits::VALUE_BITS) /
                   8 +
               1;
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
    int encode(double, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }

   protected:
    int encode_value(T value, common::ByteStream& out) {
        if (LIKELY(first_value_was_written_)) {
            return compress_value(value, out);
        }
        int ret = write_first(value, out);
        if (ret == common::E_OK) {
            first_value_was_written_ = true;
        }
        return ret;
    }

   private:
    int key_of(T value) const {
        return static_cast<int>(static_cast<uint32_t>(value) & Traits::SET_LSB);
    }

    int write_first(T value, common::ByteStream& out) {
        stored_values_[current_] = value;
        indices_[key_of(value)] = index_;
        return writer_.write_bits(static_cast<Unsigned>(value),
                                  Traits::VALUE_BITS, out);
    }

    int compress_value(T value, common::ByteStream& out) {
        int ret = common::E_OK;
        int key = key_of(value);
        T xor_value = 0;
        int previous_index = 0;
        int trailing_zeros = 0;
        int curr_index = indices_[key];

        if ((index_ - curr_index) < Traits::PREVIOUS_VALUES) {
            T temp_xor =
                value ^ stored_values_[curr_index % Traits::PREVIOUS_VALUES];
            trailing_zeros = number_of_trailing_zeros(temp_xor);
            if (trailing_zeros > Traits::THRESHOLD) {
                previous_index = curr_index % Traits::PREVIOUS_VALUES;
                xor_value = temp_xor;
            } else {
                previous_index = index_ % Traits::PREVIOUS_VALUES;
                xor_value = stored_values_[previous_index] ^ value;
            }
        } else {
            previous_index = index_ % Traits::PREVIOUS_VALUES;
            xor_value = stored_values_[previous_index] ^ value;
        }

        if (xor_value == 0) {
            ret = writer_.write_bits(static_cast<uint64_t>(previous_index),
                                     Traits::CASE_ZERO_METADATA_LENGTH, out);
            stored_leading_zeros_ = Traits::VALUE_BITS + 1;
        } else {
            int leading_zeros =
                CHIMP_LEADING_ROUND[number_of_leading_zeros(xor_value)];
            if (trailing_zeros > Traits::THRESHOLD) {
                int significant_bits =
                    Traits::VALUE_BITS - leading_zeros - trailing_zeros;
                uint64_t metadata =
                    (Traits::VALUE_BITS == 32
                         ? 256ULL * (Traits::PREVIOUS_VALUES + previous_index)
                         : 512ULL *
                               (Traits::PREVIOUS_VALUES + previous_index)) +
                    (Traits::VALUE_BITS == 32
                         ? 32ULL * CHIMP_LEADING_REPRESENTATION[leading_zeros]
                         : 64ULL *
                               CHIMP_LEADING_REPRESENTATION[leading_zeros]) +
                    static_cast<uint64_t>(significant_bits);
                if (RET_FAIL(writer_.write_bits(
                        metadata, Traits::CASE_ONE_METADATA_LENGTH, out))) {
                    return ret;
                }
                Unsigned meaningful =
                    static_cast<Unsigned>(xor_value) >> trailing_zeros;
                ret = writer_.write_bits(meaningful, significant_bits, out);
                stored_leading_zeros_ = Traits::VALUE_BITS + 1;
            } else if (leading_zeros == stored_leading_zeros_) {
                if (RET_FAIL(writer_.write_bit(true, out))) {
                    return ret;
                }
                if (RET_FAIL(writer_.write_bit(false, out))) {
                    return ret;
                }
                int significant_bits = Traits::VALUE_BITS - leading_zeros;
                ret = writer_.write_bits(static_cast<Unsigned>(xor_value),
                                         significant_bits, out);
            } else {
                stored_leading_zeros_ = leading_zeros;
                int significant_bits = Traits::VALUE_BITS - leading_zeros;
                if (RET_FAIL(writer_.write_bits(
                        24ULL + CHIMP_LEADING_REPRESENTATION[leading_zeros], 5,
                        out))) {
                    return ret;
                }
                ret = writer_.write_bits(static_cast<Unsigned>(xor_value),
                                         significant_bits, out);
            }
        }

        if (ret == common::E_OK) {
            current_ = (current_ + 1) % Traits::PREVIOUS_VALUES;
            stored_values_[current_] = value;
            ++index_;
            indices_[key] = index_;
        }
        return ret;
    }

   protected:
    common::TSEncoding type_;
    bool first_value_was_written_;
    int stored_leading_zeros_;
    MsbBitWriter writer_;
    T stored_values_[Traits::PREVIOUS_VALUES];
    int indices_[1 << (Traits::THRESHOLD + 1)];
    int index_;
    int current_;
};

class IntChimpEncoder : public ChimpEncoder<int32_t> {
   public:
    int encode(int32_t value, common::ByteStream& out) override {
        return encode_value(value, out);
    }
};

class LongChimpEncoder : public ChimpEncoder<int64_t> {
   public:
    int encode(int64_t value, common::ByteStream& out) override {
        return encode_value(value, out);
    }
};

class FloatChimpEncoder : public ChimpEncoder<int32_t> {
   public:
    int encode(float value, common::ByteStream& out) override {
        return encode_value(common::float_to_int(value), out);
    }

    int flush(common::ByteStream& out) override {
        int ret = encode_value(CHIMP_ENCODING_ENDING_FLOAT, out);
        if (RET_FAIL(ret)) {
            return ret;
        }
        ret = writer_.flush_forced(out);
        reset();
        return ret;
    }
};

class DoubleChimpEncoder : public ChimpEncoder<int64_t> {
   public:
    int encode(double value, common::ByteStream& out) override {
        return encode_value(common::double_to_long(value), out);
    }

    int flush(common::ByteStream& out) override {
        int ret = encode_value(CHIMP_ENCODING_ENDING_DOUBLE, out);
        if (RET_FAIL(ret)) {
            return ret;
        }
        ret = writer_.flush_forced(out);
        reset();
        return ret;
    }
};

}  // namespace storage

#endif  // ENCODING_CHIMP_ENCODER_H
