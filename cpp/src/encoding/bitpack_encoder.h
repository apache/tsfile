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

#ifndef ENCODING_BITPACK_ENCODER_H
#define ENCODING_BITPACK_ENCODER_H

#include <vector>

#include "common/allocator/alloc_base.h"
#include "encoder.h"
#include "encoding/encode_utils.h"
#include "encoding/intpacker.h"
#include "utils/errno_define.h"

namespace storage {

class BitPackEncoder {
   private:
    int bitpacked_group_count_;
    int num_buffered_values_;
    int bit_width_;
    IntPacker *packer_;
    common::ByteStream byte_cache_;
    std::vector<int64_t> values_;  // all data tobe encoded
    int64_t buffered_values_[8];   // encode each 8 values
    std::vector<unsigned char> bytes_buffer_;

   public:
    // BitPackEncoder() :byte_cache_(1024,common::MOD_ENCODER_OBJ){}
    BitPackEncoder()
        : bitpacked_group_count_(0),
          num_buffered_values_(0),
          bit_width_(0),
          packer_(nullptr),
          byte_cache_(1024, common::MOD_ENCODER_OBJ) {}
    ~BitPackEncoder() { destroy(); }

    void init() {
        bitpacked_group_count_ = 0;
        num_buffered_values_ = 0;
        bit_width_ = 0;
        packer_ = nullptr;
    }

    void destroy() { delete (packer_); }

    void reset() {
        num_buffered_values_ = 0;
        bitpacked_group_count_ = 0;
        bit_width_ = 0;
        bytes_buffer_.clear();
        byte_cache_.reset();
        values_.clear();
        delete (packer_);
        packer_ = nullptr;
    }

    FORCE_INLINE void encode(int64_t value, common::ByteStream &out) {
        values_.push_back(value);
        int current_bit_width = 64 - number_of_leading_zeros(value);
        if (current_bit_width > bit_width_) {
            bit_width_ = current_bit_width;
        }
    }

    void encode_flush(common::ByteStream &out) {
        ASSERT(packer_ == nullptr);
        packer_ = new IntPacker(bit_width_);
        common::SerializationUtil::write_i8(bit_width_, byte_cache_);
        for (size_t i = 0; i < values_.size(); i++) {
            // encodeValue(value);
            buffered_values_[num_buffered_values_] = values_[i];
            num_buffered_values_++;
            if (num_buffered_values_ == 8) {
                write_or_append_bitpacked_run();
            }
        }
        flush(out);
    }

    void write_or_append_bitpacked_run() {
        if (bitpacked_group_count_ >= 63) {
            // we've packed as many values as we can for this run,
            // end it and start a new one
            end_previous_bitpacked_run(8);
        }
        convert_buffer();
        num_buffered_values_ = 0;
        ++bitpacked_group_count_;
    }

    void convert_buffer() {
        // TODO: put the bytes on the stack instead on the heap
        unsigned char *bytes = (unsigned char *)common::mem_alloc(
            bit_width_, common::MOD_BITENCODE_OBJ);
        int64_t tmp_buffer[8];
        for (int i = 0; i < 8; i++) {
            tmp_buffer[i] = (int64_t)buffered_values_[i];
        }
        packer_->pack_8values(tmp_buffer, 0, bytes);
        // we'll not writer bit-packing group to OutputStream immediately
        // we buffer them in list
        for (int i = 0; i < bit_width_; i++) {
            bytes_buffer_.push_back(bytes[i]);
        }
        common::mem_free(bytes);
    }

    void flush(common::ByteStream &out) {
        int last_bitpacked_num = num_buffered_values_;
        if (num_buffered_values_ > 0) {
            clear_buffer();
            write_or_append_bitpacked_run();
            end_previous_bitpacked_run(last_bitpacked_num);
        } else {
            end_previous_bitpacked_run(8);
        }
        uint32_t b_length = byte_cache_.total_size();
        common::SerializationUtil::write_var_uint(b_length, out);
        merge_byte_stream(out, byte_cache_);
        reset();
    }

    void clear_buffer() {
        for (int i = num_buffered_values_; i < 8; i++) {
            buffered_values_[i] = 0;
        }
    }

    void end_previous_bitpacked_run(int last_bitpacked_num) {
        unsigned char bitPackHeader =
            (unsigned char)((bitpacked_group_count_ << 1) | 1);
        common::SerializationUtil::write_ui8(bitPackHeader, byte_cache_);
        common::SerializationUtil::write_ui8((uint8_t)last_bitpacked_num,
                                             byte_cache_);
        for (size_t i = 0; i < bytes_buffer_.size(); i++) {
            common::SerializationUtil::write_ui8(bytes_buffer_[i], byte_cache_);
        }
        bytes_buffer_.clear();
        bitpacked_group_count_ = 0;
    }

    int get_max_byte_size() {
        if (values_.empty()) {
            return 0;
        }
        int totalValues = values_.size();
        int fullGroups = totalValues / 8;
        int remainingValues = totalValues % 8;
        int bytesPerGroup = (bit_width_ * 8 + 7) / 8;
        int maxSize = 0;
        maxSize += fullGroups * bytesPerGroup;
        if (remainingValues > 0) {
            maxSize += bytesPerGroup;
        }

        // Add additional bytes, because each bitpack group has a header of 1
        // byte and a tail of 1 byte.
        maxSize += fullGroups * (1 + 1) + (remainingValues > 0 ? (1 + 1) : 0);
        return maxSize;
    }
};

}  // end namespace storage
#endif  // ENCODING_BITPACK_ENCODER_H
