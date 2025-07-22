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

#ifndef INT64_SPRINTZ_ENCODER_H
#define INT64_SPRINTZ_ENCODER_H

#include <cstdint>
#include <memory>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/encode_utils.h"
#include "encoding/fire.h"
#include "encoding/int64_packer.h"
#include "encoding/int64_rle_encoder.h"
#include "sprintz_encoder.h"

namespace storage {

class Int64SprintzEncoder : public SprintzEncoder {
   public:
    Int64SprintzEncoder() : SprintzEncoder(), fire_pred_(3) {}

    ~Int64SprintzEncoder() override = default;

    void reset() override {
        SprintzEncoder::reset();
        values_.clear();
    }

    void destroy() override {}

    int encode(int32_t value, common::ByteStream& out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int encode(float value, common::ByteStream& out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int encode(double value, common::ByteStream& out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int encode(bool value, common::ByteStream& out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int encode(common::String value, common::ByteStream& out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int encode(int64_t value, common::ByteStream& out_stream) override {
        int ret = common::E_OK;
        if (!is_first_cached_) {
            values_.push_back(value);
            is_first_cached_ = true;
            return ret;
        }

        values_.push_back(value);

        if (values_.size() == block_size_ + 1) {
            int64_t prev = values_[0];
            fire_pred_.reset();
            for (int i = 1; i <= block_size_; ++i) {
                int64_t temp = values_[i];
                values_[i] = predict(values_[i], prev);
                prev = temp;
            }

            bit_pack();
            is_first_cached_ = false;
            values_.clear();
            group_num_++;

            if (group_num_ == group_max_) {
                if (RET_FAIL(flush(out_stream))) {
                    return ret;
                }
            }
        }

        return ret;
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

            Int64RleEncoder encoder;
            for (int64_t val : values_) {
                encoder.encode(val, out_stream);
            }
            encoder.flush(out_stream);
        }

        reset();
        return ret;
    }

    int get_one_item_max_size() override {
        return 1 + (1 + block_size_) * sizeof(int64_t);
    }

    int get_max_byte_size() override {
        return 1 + (values_.size() + 1) * sizeof(int64_t);
    }

   protected:
    void bit_pack() override {
        int64_t pre_value = values_[0];
        values_.erase(values_.begin());

        bit_width_ = get_int64_max_bit_width(values_);
        packer_ = std::make_shared<Int64Packer>(bit_width_);

        std::vector<uint8_t> bytes(bit_width_);
        std::vector<int64_t> tmp_buffer(values_.begin(),
                                        values_.begin() + block_size_);
        packer_->pack_8values(tmp_buffer.data(), 0, bytes.data());

        common::SerializationUtil::write_int_little_endian_padded_on_bit_width(
            bit_width_, byte_cache_, 1);
        common::SerializationUtil::write_i64(pre_value, byte_cache_);
        byte_cache_.write_buf(reinterpret_cast<const char*>(bytes.data()),
                              bytes.size());
    }

    int64_t predict(int64_t value, int64_t prev) {
        int64_t pred = 0;
        if (predict_method_ == "delta") {
            pred = delta(value, prev);
        } else if (predict_method_ == "fire") {
            pred = fire(value, prev);
        } else {
            ASSERT(false);
        }

        return (pred <= 0) ? -2 * pred : 2 * pred - 1;
    }

    int64_t delta(int64_t value, int64_t prev) { return value - prev; }

    int64_t fire(int64_t value, int64_t prev) {
        int64_t pred = fire_pred_.predict(prev);
        int64_t err = value - pred;
        fire_pred_.train(prev, value, err);
        return err;
    }

   private:
    std::vector<int64_t> values_;
    std::shared_ptr<Int64Packer> packer_;
    LongFire fire_pred_;
};

}  // namespace storage

#endif  // INT64_SPRINTZ_ENCODER_H
