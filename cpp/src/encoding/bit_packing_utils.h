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

#ifndef ENCODING_BIT_PACKING_UTILS_H
#define ENCODING_BIT_PACKING_UTILS_H

#include <stdint.h>

#include "common/allocator/byte_stream.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

class MsbBitWriter {
   public:
    MsbBitWriter() { reset(); }

    void reset() {
        buffer_ = 0;
        bits_left_ = 8;
        bits_written_ = 0;
    }

    int write_bit(bool value, common::ByteStream& out) {
        if (value) {
            buffer_ |= static_cast<uint8_t>(1U << (bits_left_ - 1));
        }
        --bits_left_;
        ++bits_written_;
        return flush_byte_if_full(out);
    }

    int write_bits(uint64_t value, int bits, common::ByteStream& out) {
        int ret = common::E_OK;
        while (bits > 0) {
            int shift = bits - bits_left_;
            if (shift >= 0) {
                uint8_t mask = static_cast<uint8_t>((1U << bits_left_) - 1U);
                buffer_ |= static_cast<uint8_t>((value >> shift) & mask);
                bits -= bits_left_;
                bits_written_ += bits_left_;
                bits_left_ = 0;
            } else {
                shift = bits_left_ - bits;
                uint64_t mask = bits == 64 ? UINT64_MAX : ((1ULL << bits) - 1);
                buffer_ |= static_cast<uint8_t>((value & mask) << shift);
                bits_written_ += bits;
                bits_left_ -= bits;
                bits = 0;
            }
            if (RET_FAIL(flush_byte_if_full(out))) {
                return ret;
            }
        }
        return ret;
    }

    int flush_partial(common::ByteStream& out) {
        if (bits_left_ == 8) {
            return common::E_OK;
        }
        uint8_t byte = buffer_;
        int ret = out.write_buf(&byte, 1);
        buffer_ = 0;
        bits_left_ = 8;
        return ret;
    }

    int flush_forced(common::ByteStream& out) {
        bits_left_ = 0;
        return flush_byte_if_full(out);
    }

    int bits_written() const { return bits_written_; }

   private:
    int flush_byte_if_full(common::ByteStream& out) {
        if (bits_left_ != 0) {
            return common::E_OK;
        }
        uint8_t byte = buffer_;
        int ret = out.write_buf(&byte, 1);
        buffer_ = 0;
        bits_left_ = 8;
        return ret;
    }

    uint8_t buffer_;
    int bits_left_;
    int bits_written_;
};

class MsbBitReader {
   public:
    MsbBitReader() { reset(); }

    void reset() {
        buffer_ = 0;
        bits_left_ = 0;
    }

    int read_bit(bool& value, common::ByteStream& in) {
        int ret = common::E_OK;
        if (bits_left_ == 0 && RET_FAIL(load_byte(in))) {
            return ret;
        }
        value = ((buffer_ >> (bits_left_ - 1)) & 1U) != 0;
        --bits_left_;
        return common::E_OK;
    }

    int read_bits(uint64_t& value, int bits, common::ByteStream& in) {
        int ret = common::E_OK;
        value = 0;
        while (bits > 0) {
            if (bits_left_ == 0 && RET_FAIL(load_byte(in))) {
                return ret;
            }
            if (bits > bits_left_ || bits == 8) {
                uint8_t mask = static_cast<uint8_t>((1U << bits_left_) - 1U);
                value = (value << bits_left_) + (buffer_ & mask);
                bits -= bits_left_;
                bits_left_ = 0;
            } else {
                uint8_t mask = static_cast<uint8_t>((1U << bits) - 1U);
                uint8_t data = static_cast<uint8_t>(
                    (buffer_ >> (bits_left_ - bits)) & mask);
                value = (value << bits) + data;
                bits_left_ -= bits;
                bits = 0;
            }
        }
        return ret;
    }

    int align_to_byte(common::ByteStream& in) {
        bool ignored = false;
        int ret = common::E_OK;
        while (bits_left_ > 0) {
            if (RET_FAIL(read_bit(ignored, in))) {
                return ret;
            }
        }
        return ret;
    }

    bool has_cached_bits() const { return bits_left_ > 0; }

   private:
    int load_byte(common::ByteStream& in) {
        uint32_t read_len = 0;
        int ret = in.read_buf(&buffer_, 1, read_len);
        if (ret != common::E_OK || read_len != 1) {
            return common::E_BUF_NOT_ENOUGH;
        }
        bits_left_ = 8;
        return common::E_OK;
    }

    uint8_t buffer_;
    int bits_left_;
};

}  // namespace storage

#endif  // ENCODING_BIT_PACKING_UTILS_H
