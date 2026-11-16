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

#ifndef INT32_SPRINTZ_DECODER_H
#define INT32_SPRINTZ_DECODER_H

#include <cstring>
#include <iostream>
#include <istream>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "encoding/fire.h"
#include "encoding/int32_rle_decoder.h"
#include "int32_packer.h"
#include "sprintz_decoder.h"

namespace storage {

class Int32SprintzDecoder : public SprintzDecoder {
   public:
    Int32SprintzDecoder()
        : packer_(0),
          fire_pred_(2),
          pre_value_(0),
          current_value_(0),
          current_buffer_(block_size_ + 1),
          pack_buf_(block_size_ * sizeof(int32_t) + 8, 0),
          predict_scheme_("fire") {
        SprintzDecoder::reset();
        current_value_ = 0;
        pre_value_ = 0;
        current_count_ = 0;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0);
    }

    ~Int32SprintzDecoder() override = default;

    void set_predict_method(const std::string& method) {
        predict_scheme_ = method;
    }

    bool has_remaining(const common::ByteStream& in) override {
        uint32_t min_len = sizeof(int32_t) + 1;
        return (is_block_read_ && current_count_ < block_size_) ||
               in.remaining_size() >= min_len;
    }

    int read_boolean(bool& ret_value, common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_float(float& ret_value, common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_double(double& ret_value, common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String& ret_value, common::PageArena& pa,
                    common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        int ret = common::E_OK;
        if (!is_block_read_) {
            if (RET_FAIL(decode_block(in))) {
                return ret;
            }
        }
        ret_value = current_buffer_[current_count_++];
        if (current_count_ == decode_size_) {
            is_block_read_ = false;
            current_count_ = 0;
        }
        return ret;
    }

    int read_batch_int32(int32_t* out, int capacity, int& actual,
                         common::ByteStream& in) override {
        return batch_read(out, capacity, actual, current_buffer_.data(), in,
                          true, sizeof(int32_t) + 1);
    }

    void reset() override {
        SprintzDecoder::reset();
        current_value_ = 0;
        pre_value_ = 0;
        current_count_ = 0;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0);
    }

    bool has_next(common::ByteStream& input) {
        uint32_t min_length = sizeof(int32_t) + 1;
        return (is_block_read_ && current_count_ < block_size_) ||
               input.remaining_size() >= min_length;
    }

   protected:
    int decode_block(common::ByteStream& input) override {
        // read header bitWidth
        int ret = common::E_OK;
        uint8_t byte;
        uint32_t bit_width = 0, read_len = 0;
        ret = input.read_buf(&byte, 1, read_len);
        if (ret != common::E_OK || read_len != 1) {
            return common::E_DECODE_ERR;
        }
        bit_width |= static_cast<uint32_t>(byte);
        bit_width_ = static_cast<int32_t>(bit_width);

        if ((bit_width_ & (1 << 7)) != 0) {
            decode_size_ = bit_width_ & ~(1 << 7);
            Int32RleDecoder decoder;
            for (int i = 0; i < decode_size_; ++i) {
                if (RET_FAIL(decoder.read_int(current_buffer_[i], input))) {
                    return ret;
                }
            }
        } else {
            if (bit_width_ > 32) {
                // Int32Packer supports at most 32 bits per value; the header
                // byte is not otherwise validated.
                return common::E_DECODE_ERR;
            }
            decode_size_ = block_size_ + 1;
            uint32_t tmp_prev_value;
            common::SerializationUtil::read_var_uint(tmp_prev_value, input);
            pre_value_ = tmp_prev_value;
            current_buffer_[0] = pre_value_;

            uint32_t read_len = 0;
            input.read_buf(reinterpret_cast<char*>(pack_buf_.data()),
                           bit_width_, read_len);
            // The SIMD unpack gathers 8 bytes per value and may read up to 7
            // bytes past the packed group. Zero everything the read did not
            // fill so a reused buffer never carries a previous block's bytes
            // into the padding or into a short read's tail.
            if (read_len < pack_buf_.size()) {
                std::memset(pack_buf_.data() + read_len, 0,
                            pack_buf_.size() - read_len);
            }

            packer_.set_width(bit_width_);
            packer_.unpack_8values(pack_buf_.data(), 0,
                                   current_buffer_.data() + 1);

            ret = recalculate();
        }
        is_block_read_ = true;
        return ret;
    }

    int recalculate() override {
        int ret = common::E_OK;
        for (int i = 1; i <= block_size_; ++i) {
            if (current_buffer_[i] % 2 == 0) {
                current_buffer_[i] = -current_buffer_[i] / 2;
            } else {
                current_buffer_[i] = (current_buffer_[i] + 1) / 2;
            }
        }

        if (predict_scheme_ == "delta") {
            for (size_t i = 1; i < current_buffer_.size(); ++i) {
                current_buffer_[i] += current_buffer_[i - 1];
            }
        } else if (predict_scheme_ == "fire") {
            fire_pred_.reset();
            for (int i = 1; i <= block_size_; ++i) {
                int32_t pred = fire_pred_.predict(current_buffer_[i - 1]);
                int32_t err = current_buffer_[i];
                current_buffer_[i] = pred + err;
                fire_pred_.train(current_buffer_[i - 1], current_buffer_[i],
                                 err);
            }
        } else {
            ret = common::E_DECODE_ERR;
        }
        return ret;
    }

   private:
    Int32Packer packer_;
    IntFire fire_pred_;
    int32_t pre_value_;
    int32_t current_value_;
    std::vector<int32_t> current_buffer_;
    std::vector<uint8_t> pack_buf_;
    std::string predict_scheme_;
};

}  // namespace storage

#endif  // INT32_SPRINTZ_DECODER_H
