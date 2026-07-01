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

#ifndef ENCODING_INT64PACKER_ENCODER_H
#define ENCODING_INT64PACKER_ENCODER_H

#define NUM_OF_INTS 8

#include "encoder.h"

namespace storage {

class Int64Packer {
   private:
    int width_;

   public:
    Int64Packer(int width_) { this->width_ = width_; }
    ~Int64Packer() { destroy(); }

    void destroy() { /* do nothing for IntPacker */
    }
    void reset() { /* do thing for IntPacker */
    }

    void pack_8values(const int64_t values[], int offset, unsigned char buf[]) {
        int buf_idx = 0;
        int value_idx = offset;
        // remaining bits for the current unfinished Integer
        int left_bit = 0;

        while (value_idx < NUM_OF_INTS + offset) {
            // buffer is used for saving 64 bits as a part of result
            int64_t buffer = 0;
            // remaining size of bits in the 'buffer'
            int left_size = 64;

            // encode the left bits of current Integer to 'buffer'
            if (left_bit > 0) {
                buffer |= (static_cast<uint64_t>(values[value_idx])
                           << (64 - left_bit));
                left_size -= left_bit;
                left_bit = 0;
                value_idx++;
            }

            while (left_size >= width_ && value_idx < NUM_OF_INTS + offset) {
                // encode one Integer to the 'buffer'
                buffer |= (static_cast<uint64_t>(values[value_idx])
                           << (left_size - width_));
                left_size -= width_;
                value_idx++;
            }
            // If the remaining space of the buffer can not save the bits for
            // one Integer,
            if (left_size > 0 && value_idx < NUM_OF_INTS + offset) {
                // put the first 'left_size' bits of the Integer into remaining
                // space of the buffer
                buffer |= ((static_cast<uint64_t>(values[value_idx])) >>
                           (width_ - left_size));
                left_bit = width_ - left_size;
            }

            // put the buffer into the final result
            for (int j = 0; j < 8; j++) {
                buf[buf_idx] =
                    (unsigned char)(((uint64_t)buffer >> ((8 - j - 1) * 8)) &
                                    0xFF);
                buf_idx++;
                // width_ is the bit num of each value, but here is means the
                // max byte num
                if (buf_idx >= width_ * 8 / 8) {
                    return;
                }
            }
        }
    }

    /**
     * decode Integers from byte array.
     *
     * @param buf - array where bytes are in.
     * @param offset - offset of first byte to be decoded in buf
     * @param values - decoded result , the length of 'values' should be @{link
     * IntPacker#NUM_OF_INTS}
     */
    void unpack_8values(const unsigned char buf[], int offset,
                        int64_t values[]) {
        int byte_idx = offset;
        int value_idx = 0;
        int left_bits = 8;
        int total_bits = 0;

        while (value_idx < 8) {
            values[value_idx] = 0;
            total_bits = 0;

            while (total_bits < width_) {
                if (width_ - total_bits >= left_bits) {
                    values[value_idx] <<= left_bits;
                    values[value_idx] |= static_cast<int64_t>(
                        buf[byte_idx] & ((1 << left_bits) - 1));
                    total_bits += left_bits;
                    byte_idx++;
                    left_bits = 8;
                } else {
                    int t = width_ - total_bits;
                    values[value_idx] <<= t;
                    values[value_idx] |= static_cast<int64_t>(
                        (buf[byte_idx] & ((1 << left_bits) - 1)) >>
                        (left_bits - t));
                    left_bits -= t;
                    total_bits += t;
                }
            }

            value_idx++;
        }
    }

    /**
     * decode all values from 'buf' with specified offset and length decoded
     * result will be saved in the array named 'values'.
     *
     * @param buf array where all bytes are in.
     * @param length length of bytes to be decoded in buf.
     * @param values decoded result.
     */
    void unpack_all_values(const unsigned char buf[], int length,
                           int64_t values[]) {
        int idx = 0;
        int k = 0;
        while (idx < length) {
            int64_t tv[8];
            // decode 8 values one time, current result will be saved in the
            // array named 'tv'
            unpack_8values(buf, idx, tv);
            // System.arraycopy(tv, 0, values, k, 8);
            std::memmove(values + k, tv, 8 * sizeof(int64_t));
            idx += width_;
            k += 8;
        }
    }

    void set_width(int width_) { this->width_ = width_; }
};

}  // end namespace storage
#endif  // ENCODING_IntPacker_ENCODER_H
