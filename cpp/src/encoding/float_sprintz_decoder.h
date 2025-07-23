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

#ifndef FLOAT_SPRINTZ_DECODER_H
#define FLOAT_SPRINTZ_DECODER_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/fire.h"
#include "gorilla_decoder.h"
#include "int32_packer.h"
#include "sprintz_decoder.h"

namespace storage {

class FloatSprintzDecoder : public SprintzDecoder {
   public:
    FloatSprintzDecoder() : fire_pred_(2), predict_scheme_("fire") {
        SprintzDecoder::reset();
        current_buffer_.resize(block_size_ + 1);
        convert_buffer_.resize(block_size_);
        pre_value_ = 0;
        current_value_ = 0.0f;
        current_count_ = 0;
        decode_size_ = 0;
        is_block_read_ = false;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0.0f);
        std::fill(convert_buffer_.begin(), convert_buffer_.end(), 0);
        fire_pred_.reset();
    }

    ~FloatSprintzDecoder() override = default;

    void set_predict_method(const std::string &method) {
        predict_scheme_ = method;
    }

    int read_boolean(bool &ret_value, common::ByteStream &in) {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int32(int32_t &ret_value, common::ByteStream &in) {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int64(int64_t &ret_value, common::ByteStream &in) {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_double(double &ret_value, common::ByteStream &in) {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String &ret_value, common::PageArena &pa,
                    common::ByteStream &in) {
        return common::E_TYPE_NOT_MATCH;
    }

    void reset() override {
        SprintzDecoder::reset();
        pre_value_ = 0;
        current_value_ = 0.0f;
        current_count_ = 0;
        decode_size_ = 0;
        is_block_read_ = false;
        std::fill(current_buffer_.begin(), current_buffer_.end(), 0.0f);
        std::fill(convert_buffer_.begin(), convert_buffer_.end(), 0);
        fire_pred_.reset();
    }

    bool has_remaining(const common::ByteStream &input) override {
        int min_length = sizeof(uint32_t) + 1;
        return (is_block_read_ && current_count_ < decode_size_) ||
               input.remaining_size() >= min_length;
    }

    int read_float(float &ret_value, common::ByteStream &input) override {
        int ret = common::E_OK;
        if (!is_block_read_) {
            if (RET_FAIL(decode_block(input))) {
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
            FloatGorillaDecoder decoder;
            for (int i = 0; i < decode_size_; ++i) {
                if (RET_FAIL(decoder.read_float(current_buffer_[i], input))) {
                    return ret;
                }
            }
        } else {
            // packed block
            decode_size_ = block_size_ + 1;
            common::SerializationUtil::read_float(pre_value_, input);
            current_buffer_[0] = pre_value_;
            // read packed data
            std::vector<uint8_t> pack_buf(bit_width_);
            uint32_t read_len = 0;
            input.read_buf(reinterpret_cast<char *>(pack_buf.data()),
                           bit_width_, read_len);
            packer_ = std::make_shared<Int32Packer>(bit_width_);
            std::vector<int32_t> tmp_buffer(block_size_);
            packer_->unpack_8values(pack_buf.data(), 0, tmp_buffer.data());
            // move into convert_buffer_
            for (int i = 0; i < block_size_; ++i) {
                convert_buffer_[i] = tmp_buffer[i];
            }
            ret = recalculate();
        }
        is_block_read_ = true;
        return ret;
    }

    int recalculate() override {
        int ret = common::E_OK;
        for (int i = 0; i < block_size_; ++i) {
            int32_t v = convert_buffer_[i];
            convert_buffer_[i] = (v % 2 == 0) ? -v / 2 : (v + 1) / 2;
        }

        if (predict_scheme_ == "delta") {
            uint32_t prev_bits;
            std::memcpy(&prev_bits, &current_buffer_[0], sizeof(prev_bits));
            // Java: convertBuffer[0] = convertBuffer[0] +
            // Float.floatToIntBits(preValue);
            int32_t corrected0 =
                convert_buffer_[0] + static_cast<int32_t>(prev_bits);
            convert_buffer_[0] = corrected0;
            // Java: currentBuffer[1] = Float.intBitsToFloat(convertBuffer[0]);
            float f0;
            std::memcpy(&f0, &corrected0, sizeof(corrected0));
            current_buffer_[1] = f0;

            for (int i = 1; i < block_size_; ++i) {
                // Java: convertBuffer[i] += convertBuffer[i - 1];
                convert_buffer_[i] += convert_buffer_[i - 1];
                int32_t bits = convert_buffer_[i];
                float fi;
                std::memcpy(&fi, &bits, sizeof(bits));
                current_buffer_[i + 1] = fi;
            }

        } else if (predict_scheme_ == "fire") {
            fire_pred_.reset();
            uint32_t prev_bits;
            std::memcpy(&prev_bits, &current_buffer_[0], sizeof(prev_bits));
            // Java: int p = firePred.predict(Float.floatToIntBits(preValue));
            int32_t p = fire_pred_.predict(prev_bits);
            int32_t e0 = convert_buffer_[0];
            int32_t corrected0 = p + e0;
            convert_buffer_[0] = corrected0;
            float f0;
            std::memcpy(&f0, &corrected0, sizeof(corrected0));
            current_buffer_[1] = f0;
            // Java: firePred.train(Float.floatToIntBits(preValue),
            // convertBuffer[0], e);
            fire_pred_.train(prev_bits, corrected0, e0);

            for (int i = 1; i < block_size_; ++i) {
                uint32_t prev_bits_i;
                std::memcpy(&prev_bits_i, &current_buffer_[i],
                            sizeof(prev_bits_i));
                int32_t err = convert_buffer_[i];
                int32_t pred = fire_pred_.predict(prev_bits_i);
                int32_t corrected = pred + err;
                convert_buffer_[i] = corrected;
                float fi;
                std::memcpy(&fi, &corrected, sizeof(corrected));
                current_buffer_[i + 1] = fi;
                // Java: firePred.train(convertBuffer[i - 1], convertBuffer[i],
                // err);
                fire_pred_.train(prev_bits_i, corrected, err);
            }

        } else {
            ret = common::E_DECODE_ERR;
        }
        return ret;
    }

   private:
    float pre_value_;
    float current_value_;
    size_t current_count_;
    int decode_size_;
    bool is_block_read_ = false;

    std::vector<float> current_buffer_;
    std::vector<int32_t> convert_buffer_;
    std::shared_ptr<Int32Packer> packer_;
    IntFire fire_pred_;
    std::string predict_scheme_;
};

}  // namespace storage

#endif  // FLOAT_SPRINTZ_DECODER_H