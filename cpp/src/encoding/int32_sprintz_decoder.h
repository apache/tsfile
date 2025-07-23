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
        : current_value_(0),
          pre_value_(0),
          current_buffer_(block_size_ + 1),
          fire_pred_(2),
          predict_scheme_("fire") {
        SprintzDecoder::reset();
        current_value_ = 0;
        pre_value_ = 0;
        current_count_ = 0;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0);
    }

    ~Int32SprintzDecoder() override = default;

    void set_predict_method(const std::string &method) {
        predict_scheme_ = method;
    }

    bool has_remaining(const common::ByteStream &in) {
        int min_len = sizeof(int32_t) + 1;
        return (is_block_read_ && current_count_ < block_size_) ||
               in.remaining_size() >= min_len;
    }

    int read_boolean(bool &ret_value, common::ByteStream &in) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int read_int64(int64_t &ret_value, common::ByteStream &in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_float(float &ret_value, common::ByteStream &in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_double(double &ret_value, common::ByteStream &in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String &ret_value, common::PageArena &pa,
                    common::ByteStream &in) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int read_int32(int32_t &ret_value, common::ByteStream &in) {
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

    void reset() override {
        SprintzDecoder::reset();
        current_value_ = 0;
        pre_value_ = 0;
        current_count_ = 0;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0);
    }

    bool has_next(common::ByteStream &input) {
        int min_lenth = sizeof(int32_t) + 1;
        return (is_block_read_ && current_count_ < block_size_) ||
               input.remaining_size() >= min_lenth;
    }

   protected:
    int decode_block(common::ByteStream &input) override {
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
                current_buffer_[i] = decoder.read_int(input);
            }
        } else {
            decode_size_ = block_size_ + 1;
            uint32_t tmp_prev_value;
            common::SerializationUtil::read_var_uint(tmp_prev_value, input);
            pre_value_ = tmp_prev_value;
            current_buffer_[0] = pre_value_;

            std::vector<uint8_t> pack_buf(bit_width_);
            uint32_t read_len = 0;
            input.read_buf(reinterpret_cast<char *>(pack_buf.data()),
                           bit_width_, read_len);

            std::vector<int32_t> tmp_buffer(8);
            packer_ = std::make_shared<Int32Packer>(bit_width_);
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
    std::shared_ptr<Int32Packer> packer_;
    IntFire fire_pred_;
    int32_t pre_value_;
    int32_t current_value_;
    std::vector<int32_t> current_buffer_;
    std::string predict_scheme_;
};

}  // namespace storage

#endif  // INT32_SPRINTZ_DECODER_H
