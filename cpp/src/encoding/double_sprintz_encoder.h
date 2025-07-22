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

#ifndef DOUBLE_SPRINTZ_ENCODER_H
#define DOUBLE_SPRINTZ_ENCODER_H

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/fire.h"
#include "encoding/int64_packer.h"
#include "gorilla_encoder.h"
#include "sprintz_encoder.h"

namespace storage {

class DoubleSprintzEncoder : public SprintzEncoder {
   public:
    DoubleSprintzEncoder() : fire_pred_(3) {
        convert_buffer_.resize(block_size_);
    }

    ~DoubleSprintzEncoder() override = default;

    void reset() override {
        SprintzEncoder::reset();
        values_.clear();
    }

    void destroy() override {}

    int get_one_item_max_size() override {
        return 1 + (1 + block_size_) * static_cast<int>(sizeof(int64_t));
    }

    int get_max_byte_size() override {
        return 1 + (static_cast<int>(values_.size()) + 1) *
                       static_cast<int>(sizeof(int64_t));
    }

    int encode(bool, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(float, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(double value, common::ByteStream& out_stream) override {
        int ret = common::E_OK;
        if (!is_first_cached_) {
            values_.push_back(value);
            is_first_cached_ = true;
            return ret;
        }
        values_.push_back(value);

        if (values_.size() == block_size_ + 1) {
            fire_pred_.reset();
            for (int i = 1; i <= block_size_; ++i) {
                convert_buffer_[i - 1] = predict(values_[i], values_[i - 1]);
            }
            bit_pack();
            is_first_cached_ = false;
            values_.clear();
            group_num_++;
            if (group_num_ == group_max_) {
                if (RET_FAIL(flush(out_stream))) return ret;
            }
        }
        return ret;
    }
    int encode(const common::String, common::ByteStream&) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int flush(common::ByteStream& out_stream) override {
        int ret = common::E_OK;
        if (byte_cache_.total_size() > 0) {
            if (RET_FAIL(common::SerializationUtil::chunk_read_all_data(
                    byte_cache_, out_stream))) {
                return ret;
            }
        }

        if (!values_.empty()) {
            int size = static_cast<int>(values_.size());
            size |= (1 << 7);
            common::SerializationUtil::
                write_int_little_endian_padded_on_bit_width(size, out_stream,
                                                            1);
            DoubleGorillaEncoder encoder;
            for (double val : values_) {
                encoder.encode(val, out_stream);
            }
            encoder.flush(out_stream);
        }

        reset();
        return ret;
    }

   protected:
    void bit_pack() override {
        // extract and remove first value
        double pre_value = values_[0];
        values_.erase(values_.begin());

        // compute bit width and init packer
        bit_width_ = get_int64_max_bit_width(convert_buffer_);
        packer_ = std::make_shared<Int64Packer>(bit_width_);

        std::vector<uint8_t> bytes(bit_width_);
        packer_->pack_8values(convert_buffer_.data(), 0, bytes.data());

        // write bit_width and first value
        common::SerializationUtil::write_int_little_endian_padded_on_bit_width(
            bit_width_, byte_cache_, 1);
        uint8_t buf[8];
        common::double_to_bytes(pre_value, buf);
        byte_cache_.write_buf(reinterpret_cast<const char*>(buf), 8);
        byte_cache_.write_buf(reinterpret_cast<const char*>(bytes.data()),
                              bytes.size());
    }

    int64_t predict(double value, double prev) {
        int64_t curr_bits = common::double_to_long(value);
        int64_t prev_bits = common::double_to_long(prev);
        int64_t raw_pred;
        if (predict_method_ == "delta") {
            raw_pred = curr_bits - prev_bits;
        } else if (predict_method_ == "fire") {
            int64_t pred = fire_pred_.predict(prev_bits);
            int64_t err = curr_bits - pred;
            fire_pred_.train(prev_bits, curr_bits, err);
            raw_pred = err;
        } else {
            ASSERT(false);
        }
        return (raw_pred <= 0) ? -2 * raw_pred : 2 * raw_pred - 1;
    }

   private:
    std::vector<double> values_;
    std::vector<int64_t> convert_buffer_;
    std::shared_ptr<Int64Packer> packer_;
    LongFire fire_pred_;
};

}  // namespace storage

#endif  // DOUBLE_SPRINTZ_ENCODER_H
