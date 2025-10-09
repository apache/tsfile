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

#ifndef INT64_SPRINTZ_DECODER_H
#define INT64_SPRINTZ_DECODER_H

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/fire.h"
#include "encoding/int64_packer.h"
#include "encoding/int64_rle_decoder.h"
#include "sprintz_decoder.h"

namespace storage {

class Int64SprintzDecoder : public SprintzDecoder {
   public:
    Int64SprintzDecoder()
        : current_value_(0),
          pre_value_(0),
          current_buffer_(block_size_ + 1),
          fire_pred_(3),
          predict_scheme_("fire") {
        SprintzDecoder::reset();
        current_count_ = 0;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0);
    }

    ~Int64SprintzDecoder() override = default;

    void set_predict_method(const std::string& method) {
        predict_scheme_ = method;
    }

    void reset() override {
        SprintzDecoder::reset();
        current_value_ = 0;
        pre_value_ = 0;
        current_count_ = 0;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0);
    }

    bool has_remaining(const common::ByteStream& in) {
        return (is_block_read_ && current_count_ < block_size_) ||
               in.has_remaining();
    }

    bool has_next(common::ByteStream& input) {
        return (is_block_read_ && current_count_ < block_size_) ||
               input.remaining_size() > 0;
    }

    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int read_boolean(bool& ret_value, common::ByteStream& in) override {
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

    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
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
            Int64RleDecoder decoder;
            for (int i = 0; i < decode_size_; ++i) {
                current_buffer_[i] = decoder.read_int(input);
            }
        } else {
            decode_size_ = block_size_ + 1;

            common::SerializationUtil::read_i64(pre_value_, input);
            current_buffer_[0] = pre_value_;

            // Read packed buffer
            std::vector<uint8_t> pack_buf(bit_width_);
            uint32_t read_len = 0;
            input.read_buf(reinterpret_cast<char*>(pack_buf.data()), bit_width_,
                           read_len);

            std::vector<int64_t> tmp_buffer(8);
            packer_ = std::make_shared<Int64Packer>(bit_width_);
            packer_->unpack_8values(pack_buf.data(), 0, tmp_buffer.data());

            for (int i = 0; i < 8; ++i) {
                current_buffer_[i + 1] = tmp_buffer[i];
            }

            ret = recalculate();
        }

        is_block_read_ = true;
        return ret;
    }

    int recalculate() override {
        int ret = common::E_OK;
        for (int i = 1; i <= block_size_; ++i) {
            if ((current_buffer_[i] & 1) == 0) {
                current_buffer_[i] = -current_buffer_[i] / 2;
            } else {
                current_buffer_[i] = (current_buffer_[i] + 1) / 2;
            }
        }

        if (predict_scheme_ == "delta") {
            for (int i = 1; i <= block_size_; ++i) {
                current_buffer_[i] += current_buffer_[i - 1];
            }
        } else if (predict_scheme_ == "fire") {
            fire_pred_.reset();
            for (int i = 1; i <= block_size_; ++i) {
                int64_t pred = fire_pred_.predict(current_buffer_[i - 1]);
                int64_t err = current_buffer_[i];
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
    std::shared_ptr<Int64Packer> packer_;
    LongFire fire_pred_;
    int64_t pre_value_;
    int64_t current_value_;
    std::vector<int64_t> current_buffer_;
    std::string predict_scheme_;
};

}  // namespace storage

#endif  // INT64_SPRINTZ_DECODER_H
