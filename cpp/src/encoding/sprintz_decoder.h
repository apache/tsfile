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
    int bit_width_;       // Current bit width being used
    int block_size_;      // Default is 8
    bool is_block_read_;  // Whether current block has been read
    int current_count_;   // Current decoding position
    int decode_size_;     // Number of valid data items in current decoded block
};

}  // namespace storage

#endif  // SPRINTZ_DECODER_H