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

#ifndef ENCODING_RLBE_DECODER_H
#define ENCODING_RLBE_DECODER_H

#include <stdint.h>

#include "bit_packing_utils.h"
#include "decoder.h"
#include "rlbe_encoder.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

template <typename T>
class RLBEDecoder : public Decoder {
   public:
    using Traits = RLBETraits<T>;
    using Unsigned = typename Traits::Unsigned;

    RLBEDecoder() { reset(); }
    ~RLBEDecoder() override = default;

    void reset() override {
        type_ = common::RLBE;
        block_size_ = 0;
        write_index_ = -1;
        read_index_ = -1;
        bit_reader_.reset();
        for (int i = 0; i <= RLBE_BLOCK_DEFAULT_SIZE * 2; ++i) {
            data_[i] = 0;
            fibonacci_[i] = 0;
        }
        fibonacci_[0] = 1;
        fibonacci_[1] = 1;
    }

    bool has_remaining(const common::ByteStream& buffer) override {
        return read_index_ < write_index_ || buffer.has_remaining();
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
    int read_value(T& ret_value, common::ByteStream& in) {
        int ret = common::E_OK;
        if (read_index_ >= write_index_) {
            if (RET_FAIL(read_block(in))) {
                return ret;
            }
        }
        ret_value = data_[++read_index_];
        return common::E_OK;
    }

   private:
    int read_bit_as_int(int& bit, common::ByteStream& in) {
        bool b = false;
        int ret = bit_reader_.read_bit(b, in);
        bit = b ? 1 : 0;
        return ret;
    }

    int read_head(common::ByteStream& in) {
        int ret = common::E_OK;
        for (int i = 0; i <= write_index_; ++i) {
            data_[i] = 0;
        }
        write_index_ = -1;
        read_index_ = -1;
        if (RET_FAIL(bit_reader_.align_to_byte(in))) {
            return ret;
        }
        uint64_t bits = 0;
        if (RET_FAIL(bit_reader_.read_bits(bits, 32, in))) {
            return ret;
        }
        block_size_ = static_cast<int>(bits);
        if (block_size_ <= 0 || block_size_ > RLBE_BLOCK_DEFAULT_SIZE) {
            return common::E_DECODE_ERR;
        }
        for (int i = 0; i < block_size_ * 2; ++i) {
            data_[i] = 0;
            fibonacci_[i] = 0;
        }
        fibonacci_[0] = 1;
        fibonacci_[1] = 1;
        return common::E_OK;
    }

    int read_block(common::ByteStream& in) {
        int ret = common::E_OK;
        if (RET_FAIL(read_head(in))) {
            return ret;
        }
        while (write_index_ < block_size_ - 1) {
            uint64_t bits = 0;
            if (RET_FAIL(bit_reader_.read_bits(bits, Traits::LENGTH_CODE_BITS,
                                               in))) {
                return ret;
            }
            int segment_length = static_cast<int>(bits);
            if (segment_length < 1 || segment_length > Traits::VALUE_BITS) {
                return common::E_DECODE_ERR;
            }

            int now = 0;
            int next = 0;
            if (RET_FAIL(read_bit_as_int(now, in)) ||
                RET_FAIL(read_bit_as_int(next, in))) {
                return ret;
            }

            uint64_t run_length = 0;
            int j = 1;
            while (true) {
                if (j >= static_cast<int>(sizeof(fibonacci_) /
                                          sizeof(fibonacci_[0]))) {
                    return common::E_DECODE_ERR;
                }
                if (j > 1) {
                    fibonacci_[j] = fibonacci_[j - 1] + fibonacci_[j - 2];
                    if (fibonacci_[j] <= fibonacci_[j - 1]) {
                        return common::E_DECODE_ERR;
                    }
                }
                if (now == 1) {
                    const uint64_t remaining =
                        static_cast<uint64_t>(block_size_ - write_index_ - 1);
                    if (fibonacci_[j] > remaining ||
                        run_length > remaining - fibonacci_[j]) {
                        return common::E_DECODE_ERR;
                    }
                    run_length += fibonacci_[j];
                }
                if (now == 1 && next == 1) {
                    break;
                }
                ++j;
                now = next;
                if (RET_FAIL(read_bit_as_int(next, in))) {
                    return ret;
                }
            }

            for (uint64_t i = 0; i < run_length; ++i) {
                if (RET_FAIL(bit_reader_.read_bits(bits, segment_length, in))) {
                    return ret;
                }
                Unsigned delta_bits = static_cast<Unsigned>(bits);
                if (segment_length == Traits::VALUE_BITS) {
                    delta_bits |= static_cast<Unsigned>(1)
                                  << (Traits::VALUE_BITS - 1);
                }
                T delta = static_cast<T>(delta_bits);
                if (write_index_ == -1) {
                    data_[++write_index_] = delta;
                } else {
                    Unsigned next_value =
                        static_cast<Unsigned>(data_[write_index_]) +
                        static_cast<Unsigned>(delta);
                    data_[++write_index_] = static_cast<T>(next_value);
                }
            }
        }
        return common::E_OK;
    }

   protected:
    common::TSEncoding type_;
    int block_size_;
    T data_[RLBE_BLOCK_DEFAULT_SIZE * 2 + 1];
    uint64_t fibonacci_[RLBE_BLOCK_DEFAULT_SIZE * 2 + 1];
    int write_index_;
    int read_index_;
    MsbBitReader bit_reader_;
};

class IntRLBEDecoder : public RLBEDecoder<int32_t> {
   public:
    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        return read_value(ret_value, in);
    }
};

class LongRLBEDecoder : public RLBEDecoder<int64_t> {
   public:
    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        return read_value(ret_value, in);
    }
};

class FloatRLBEDecoder : public RLBEDecoder<int32_t> {
   public:
    int read_float(float& ret_value, common::ByteStream& in) override {
        int32_t bits = 0;
        int ret = read_value(bits, in);
        if (ret == common::E_OK) {
            ret_value = common::int_to_float(bits);
        }
        return ret;
    }
};

class DoubleRLBEDecoder : public RLBEDecoder<int64_t> {
   public:
    int read_double(double& ret_value, common::ByteStream& in) override {
        int64_t bits = 0;
        int ret = read_value(bits, in);
        if (ret == common::E_OK) {
            ret_value = common::long_to_double(bits);
        }
        return ret;
    }
};

}  // namespace storage

#endif  // ENCODING_RLBE_DECODER_H
