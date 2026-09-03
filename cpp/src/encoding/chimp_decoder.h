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

#ifndef ENCODING_CHIMP_DECODER_H
#define ENCODING_CHIMP_DECODER_H

#include <stdint.h>

#include <climits>

#include "bit_packing_utils.h"
#include "chimp_encoder.h"
#include "decoder.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

static constexpr int8_t CHIMP_LEADING_DECODING[8] = {0,  8,  12, 16,
                                                     18, 20, 22, 24};

template <typename T>
class ChimpDecoder : public Decoder {
   public:
    using Traits = ChimpTraits<T>;
    using Unsigned = typename Traits::Unsigned;

    ChimpDecoder() { reset(); }
    ~ChimpDecoder() override = default;

    void reset() override {
        type_ = common::CHIMP;
        first_value_was_read_ = false;
        has_next_ = true;
        stored_value_ = 0;
        stored_leading_zeros_ = INT32_MAX;
        stored_trailing_zeros_ = 0;
        current_ = 0;
        reader_.reset();
        for (int i = 0; i < Traits::PREVIOUS_VALUES; ++i) {
            stored_values_[i] = 0;
        }
    }

    bool has_remaining(const common::ByteStream& buffer) override {
        return has_next_ && (first_value_was_read_ || buffer.has_remaining() ||
                             reader_.has_cached_bits());
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
    int read_double(double&, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String&, common::PageArena&,
                    common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }

   protected:
    int read_value(T& ret_value, T ending, common::ByteStream& in) {
        int ret = common::E_OK;
        ret_value = stored_value_;
        if (UNLIKELY(!first_value_was_read_)) {
            uint64_t first = 0;
            if (RET_FAIL(reader_.read_bits(first, Traits::VALUE_BITS, in))) {
                return ret;
            }
            stored_value_ = static_cast<T>(static_cast<Unsigned>(first));
            stored_values_[current_] = stored_value_;
            first_value_was_read_ = true;
            ret_value = stored_value_;
        }
        if (RET_FAIL(cache_next(ending, in))) {
            return ret;
        }
        return common::E_OK;
    }

   private:
    int read_control_bits(uint8_t& value, common::ByteStream& in) {
        int ret = common::E_OK;
        value = 0;
        for (int i = 0; i < 2; ++i) {
            bool bit = false;
            if (RET_FAIL(reader_.read_bit(bit, in))) {
                return ret;
            }
            value <<= 1;
            if (bit) {
                value |= 1;
            }
        }
        return common::E_OK;
    }

    int read_next(common::ByteStream& in) {
        int ret = common::E_OK;
        uint8_t control_bits = 0;
        if (RET_FAIL(read_control_bits(control_bits, in))) {
            return ret;
        }

        uint64_t bits = 0;
        switch (control_bits) {
            case 3: {
                if (RET_FAIL(reader_.read_bits(bits, 3, in))) {
                    return ret;
                }
                stored_leading_zeros_ = CHIMP_LEADING_DECODING[bits & 0x07];
                int meaningful_bits =
                    Traits::VALUE_BITS - stored_leading_zeros_;
                if (RET_FAIL(reader_.read_bits(bits, meaningful_bits, in))) {
                    return ret;
                }
                xor_with(static_cast<Unsigned>(bits));
                store_current();
                return common::E_OK;
            }
            case 2: {
                int meaningful_bits =
                    Traits::VALUE_BITS - stored_leading_zeros_;
                if (RET_FAIL(reader_.read_bits(bits, meaningful_bits, in))) {
                    return ret;
                }
                xor_with(static_cast<Unsigned>(bits));
                store_current();
                return common::E_OK;
            }
            case 1: {
                constexpr int METADATA_BITS =
                    Traits::CASE_ONE_METADATA_LENGTH - 2;
                if (RET_FAIL(reader_.read_bits(bits, METADATA_BITS, in))) {
                    return ret;
                }
                int fill = METADATA_BITS;
                int index = static_cast<int>(
                    (bits >> (fill -= Traits::PREVIOUS_VALUES_LOG2)) &
                    ((1 << Traits::PREVIOUS_VALUES_LOG2) - 1));
                stored_leading_zeros_ =
                    CHIMP_LEADING_DECODING[(bits >> (fill -= 3)) & 0x07];
                int significant_bits_width = Traits::VALUE_BITS == 32 ? 5 : 6;
                int significant_bits = static_cast<int>(
                    (bits >> (fill -= significant_bits_width)) &
                    ((1 << significant_bits_width) - 1));
                stored_value_ = stored_values_[index];
                if (significant_bits == 0) {
                    significant_bits = Traits::VALUE_BITS;
                }
                stored_trailing_zeros_ = Traits::VALUE_BITS - significant_bits -
                                         stored_leading_zeros_;
                if (RET_FAIL(reader_.read_bits(bits, significant_bits, in))) {
                    return ret;
                }
                xor_with(static_cast<Unsigned>(bits) << stored_trailing_zeros_);
                store_current();
                return common::E_OK;
            }
            default: {
                if (RET_FAIL(reader_.read_bits(
                        bits, Traits::PREVIOUS_VALUES_LOG2, in))) {
                    return ret;
                }
                stored_value_ = stored_values_[bits];
                store_current();
                return common::E_OK;
            }
        }
    }

    int cache_next(T ending, common::ByteStream& in) {
        int ret = read_next(in);
        if (RET_FAIL(ret)) {
            return ret;
        }
        if (stored_values_[current_] == ending) {
            has_next_ = false;
        }
        return common::E_OK;
    }

    void xor_with(Unsigned xor_bits) {
        Unsigned stored = static_cast<Unsigned>(stored_value_);
        stored_value_ = static_cast<T>(stored ^ xor_bits);
    }

    void store_current() {
        current_ = (current_ + 1) % Traits::PREVIOUS_VALUES;
        stored_values_[current_] = stored_value_;
    }

   protected:
    common::TSEncoding type_;
    bool first_value_was_read_;
    bool has_next_;
    T stored_value_;
    T stored_values_[Traits::PREVIOUS_VALUES];
    int stored_leading_zeros_;
    int stored_trailing_zeros_;
    int current_;
    MsbBitReader reader_;
};

class IntChimpDecoder : public ChimpDecoder<int32_t> {
   public:
    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        return read_value(ret_value, CHIMP_ENCODING_ENDING_INTEGER, in);
    }
};

class LongChimpDecoder : public ChimpDecoder<int64_t> {
   public:
    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        return read_value(ret_value, CHIMP_ENCODING_ENDING_LONG, in);
    }
};

class FloatChimpDecoder : public ChimpDecoder<int32_t> {
   public:
    int read_float(float& ret_value, common::ByteStream& in) override {
        int32_t bits = 0;
        int ret = read_value(bits, CHIMP_ENCODING_ENDING_FLOAT, in);
        if (ret == common::E_OK) {
            ret_value = common::int_to_float(bits);
        }
        return ret;
    }
};

class DoubleChimpDecoder : public ChimpDecoder<int64_t> {
   public:
    int read_double(double& ret_value, common::ByteStream& in) override {
        int64_t bits = 0;
        int ret = read_value(bits, CHIMP_ENCODING_ENDING_DOUBLE, in);
        if (ret == common::E_OK) {
            ret_value = common::long_to_double(bits);
        }
        return ret;
    }
};

}  // namespace storage

#endif  // ENCODING_CHIMP_DECODER_H
