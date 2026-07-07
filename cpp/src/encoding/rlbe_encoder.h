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

#ifndef ENCODING_RLBE_ENCODER_H
#define ENCODING_RLBE_ENCODER_H

#include <stdint.h>

#include "bit_packing_utils.h"
#include "common/allocator/byte_stream.h"
#include "common/db_common.h"
#include "encode_utils.h"
#include "encoder.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

static constexpr int RLBE_BLOCK_DEFAULT_SIZE = 10000;

template <typename T>
struct RLBETraits;

template <>
struct RLBETraits<int32_t> {
    using Unsigned = uint32_t;
    static constexpr int VALUE_BITS = 32;
    static constexpr int LENGTH_CODE_BITS = 6;
};

template <>
struct RLBETraits<int64_t> {
    using Unsigned = uint64_t;
    static constexpr int VALUE_BITS = 64;
    static constexpr int LENGTH_CODE_BITS = 7;
};

template <typename T>
class RLBEEncoder : public Encoder {
   public:
    using Traits = RLBETraits<T>;
    using Unsigned = typename Traits::Unsigned;

    RLBEEncoder() { reset(); }
    ~RLBEEncoder() override = default;

    void destroy() override {}

    void reset() override {
        type_ = common::RLBE;
        write_index_ = -1;
        previous_value_ = 0;
        byte_writer_.reset();
        for (int i = 0; i <= RLBE_BLOCK_DEFAULT_SIZE; ++i) {
            diff_value_[i] = 0;
            length_code_[i] = 0;
            length_rle_[i] = 0;
        }
    }

    int flush(common::ByteStream& out) override { return flush_block(out); }

    int get_max_byte_size() override {
        return 5 * 4 * RLBE_BLOCK_DEFAULT_SIZE *
               (Traits::VALUE_BITS == 64 ? 2 : 1);
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
        if (write_index_ == -1) {
            diff_value_[++write_index_] = value;
            length_code_[write_index_] = binary_length(value);
            previous_value_ = value;
            return common::E_OK;
        }

        diff_value_[++write_index_] =
            static_cast<T>(static_cast<Unsigned>(value) -
                           static_cast<Unsigned>(previous_value_));
        length_code_[write_index_] = binary_length(diff_value_[write_index_]);
        previous_value_ = value;
        if (write_index_ == RLBE_BLOCK_DEFAULT_SIZE - 1) {
            return flush(out);
        }
        return common::E_OK;
    }

   private:
    int binary_length(T value) const {
        if (value == 0) {
            return 1;
        }
        return Traits::VALUE_BITS - number_of_leading_zeros(value);
    }

    uint64_t calc_fibonacci(uint64_t value) const {
        uint64_t fib[RLBE_BLOCK_DEFAULT_SIZE * 2 + 1] = {0};
        fib[0] = 1;
        fib[1] = 1;
        int i = 0;
        for (i = 2; fib[i - 1] <= value; ++i) {
            fib[i] = fib[i - 1] + fib[i - 2];
        }
        --i;

        uint64_t encoded = 0;
        while (value > 0) {
            while (fib[i] > value && i >= 1) {
                --i;
            }
            encoded |= (1ULL << (i - 1));
            value -= fib[i];
        }
        return encoded;
    }

    void rle_on_length_code() {
        int i = 0;
        while (i <= write_index_) {
            int j = i;
            int repeat = 0;
            while (j <= write_index_ && length_code_[j] == length_code_[i]) {
                ++j;
                ++repeat;
            }
            length_rle_[i] = repeat;
            i = j;
        }
    }

    int flush_block(common::ByteStream& out) {
        int ret = common::E_OK;
        if (write_index_ == -1) {
            return common::E_OK;
        }
        if (RET_FAIL(byte_writer_.write_bits(
                static_cast<uint32_t>(write_index_ + 1), 32, out))) {
            return ret;
        }
        rle_on_length_code();
        for (int i = 0; i <= write_index_; ++i) {
            if (length_rle_[i] > 0 && RET_FAIL(flush_segment(i, out))) {
                return ret;
            }
        }
        if (RET_FAIL(byte_writer_.flush_partial(out))) {
            return ret;
        }
        reset();
        return common::E_OK;
    }

    int flush_segment(int first, common::ByteStream& out) {
        int ret = common::E_OK;
        if (RET_FAIL(byte_writer_.write_bits(
                static_cast<uint64_t>(length_code_[first]),
                Traits::LENGTH_CODE_BITS, out))) {
            return ret;
        }

        uint64_t fib = calc_fibonacci(length_rle_[first]);
        int fib_len = binary_length(static_cast<T>(fib));
        for (int i = 0; i < fib_len; ++i) {
            if (RET_FAIL(
                    byte_writer_.write_bit(((fib >> i) & 1ULL) != 0, out))) {
                return ret;
            }
        }
        if (RET_FAIL(byte_writer_.write_bit(true, out))) {
            return ret;
        }

        int j = first;
        do {
            int diff_len = binary_length(diff_value_[j]);
            if (RET_FAIL(byte_writer_.write_bits(
                    static_cast<Unsigned>(diff_value_[j]), diff_len, out))) {
                return ret;
            }
            ++j;
        } while (j <= write_index_ && length_rle_[j] == 0);
        return common::E_OK;
    }

   protected:
    common::TSEncoding type_;
    T diff_value_[RLBE_BLOCK_DEFAULT_SIZE + 1];
    int length_code_[RLBE_BLOCK_DEFAULT_SIZE + 1];
    int length_rle_[RLBE_BLOCK_DEFAULT_SIZE + 1];
    T previous_value_;
    int write_index_;
    MsbBitWriter byte_writer_;
};

class IntRLBEEncoder : public RLBEEncoder<int32_t> {
   public:
    int encode(int32_t value, common::ByteStream& out) override {
        return encode_value(value, out);
    }
};

class LongRLBEEncoder : public RLBEEncoder<int64_t> {
   public:
    int encode(int64_t value, common::ByteStream& out) override {
        return encode_value(value, out);
    }
};

class FloatRLBEEncoder : public RLBEEncoder<int32_t> {
   public:
    int encode(float value, common::ByteStream& out) override {
        return encode_value(common::float_to_int(value), out);
    }
};

class DoubleRLBEEncoder : public RLBEEncoder<int64_t> {
   public:
    int encode(double value, common::ByteStream& out) override {
        return encode_value(common::double_to_long(value), out);
    }
};

}  // namespace storage

#endif  // ENCODING_RLBE_ENCODER_H
