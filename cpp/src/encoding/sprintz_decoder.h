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

#ifndef SPRINTZ_DECODER_H
#define SPRINTZ_DECODER_H

#include <cstdint>
#include <cstring>
#include <iostream>
#include <istream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "decoder.h"

namespace storage {

class SprintzDecoder : public Decoder {
   public:
    ~SprintzDecoder() override = default;

    // Reset decoder state
    void reset() override {
        is_block_read_ = false;
        current_count_ = 0;
    }

    // Decode a compressed block (to be implemented by subclasses)
    virtual int decode_block(common::ByteStream& in) = 0;

    // Update predictor based on decoded data (to be implemented by subclasses)
    virtual int recalculate() = 0;

   protected:
    SprintzDecoder()
        : bit_width_(0),
          block_size_(8),
          is_block_read_(false),
          current_count_(0),
          decode_size_(0) {}

   protected:
    // Copy already-reconstructed values into `out`, decoding further blocks as
    // needed. The state machine is the per-value one, so a batch emits exactly
    // the values (and leaves exactly the residual block state) that the same
    // number of read_*() calls would: no value is emitted that the per-value
    // has_remaining() gate would have stopped at, and a partially consumed
    // block is left pending.
    //
    // `buffer` is the subclass's decoded-block buffer. A full block holds
    // block_size_ + 1 values: buffer[0] is the block's first value, stored
    // verbatim, and buffer[1 .. block_size_] are the reconstructed rest. A
    // short block holds decode_size_ values.
    //
    // `min_block_bytes` is how much unread data the subclass's has_remaining()
    // needs before a new block may be started. `gate_tail` mirrors the other
    // half of that check: INT32/INT64 has_remaining() reports a pending block
    // as readable only while current_count_ < block_size_, so a full block's
    // last value is returned only when the stream still has data; FLOAT/DOUBLE
    // compare against decode_size_ and return it unconditionally.
    template <typename T>
    int batch_read(T* out, int capacity, int& actual, T* buffer,
                   common::ByteStream& in, bool gate_tail,
                   uint64_t min_block_bytes) {
        actual = 0;
        if (capacity <= 0) {
            return common::E_OK;
        }
        if (out == nullptr) {
            return common::E_INVALID_ARG;
        }
        int ret = common::E_OK;
        while (actual < capacity) {
            if (!is_block_read_) {
                if (in.remaining_size() < min_block_bytes) {
                    break;
                }
                if (RET_FAIL(decode_block(in))) {
                    return ret;
                }
            }
            int usable = decode_size_ - current_count_;
            if (usable > 0 && current_count_ + usable > block_size_) {
                // The full block's last value (index block_size_) is the one
                // the per-value gate only returns while more data follows.
                usable = block_size_ - current_count_;
                if (!gate_tail || in.remaining_size() >= min_block_bytes) {
                    ++usable;
                }
            }
            if (usable <= 0) {
                break;
            }
            const int n =
                usable < capacity - actual ? usable : capacity - actual;
            std::memcpy(out + actual, buffer + current_count_,
                        static_cast<size_t>(n) * sizeof(T));
            actual += n;
            current_count_ += n;
            if (current_count_ == decode_size_) {
                is_block_read_ = false;
                current_count_ = 0;
            }
        }
        return common::E_OK;
    }

   protected:
    int bit_width_;       // Current bit width being used
    int block_size_;      // Default is 8
    bool is_block_read_;  // Whether current block has been read
    int current_count_;   // Current decoding position
    int decode_size_;     // Number of valid data items in current decoded block
};

}  // namespace storage

#endif  // SPRINTZ_DECODER_H