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

#ifndef ENCODING_INT32RLE_DECODER_H
#define ENCODING_INT32RLE_DECODER_H

#include <vector>

#include "common/allocator/alloc_base.h"
#include "decoder.h"
#include "encoder.h"
#include "encoding/encode_utils.h"
#include "encoding/int32_packer.h"

namespace storage {

class Int32RleDecoder : public Decoder {
   private:
    uint32_t length_;
    uint32_t bit_width_;
    int bitpacking_num_;
    bool is_length_and_bitwidth_readed_;
    int current_count_;
    bool is_rle_run_;
    int32_t rle_value_;
    common::ByteStream byte_cache_{common::MOD_DECODER_OBJ};
    int32_t* current_buffer_;
    Int32Packer* packer_;
    uint8_t* tmp_buf_;

   public:
    Int32RleDecoder()
        : length_(0),
          bit_width_(0),
          bitpacking_num_(0),
          is_length_and_bitwidth_readed_(false),
          current_count_(0),
          is_rle_run_(false),
          rle_value_(0),
          byte_cache_(1024, common::MOD_DECODER_OBJ),
          current_buffer_(nullptr),
          packer_(nullptr),
          tmp_buf_(nullptr) {}
    ~Int32RleDecoder() override { destroy(); }

    bool has_remaining(const common::ByteStream& buffer) override {
        return buffer.has_remaining() || has_next_package();
    }
    int read_boolean(bool& ret_value, common::ByteStream& in) override {
        int32_t bool_value;
        int ret = read_int32(bool_value, in);
        if (ret == common::E_OK) {
            ret_value = bool_value != 0;
        }
        return ret;
    }
    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        return read_int(ret_value, in);
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

    void init() {
        packer_ = nullptr;
        is_length_and_bitwidth_readed_ = false;
        length_ = 0;
        bit_width_ = 0;
        bitpacking_num_ = 0;
        current_count_ = 0;
        is_rle_run_ = false;
        rle_value_ = 0;
    }

    bool has_next(common::ByteStream& buffer) {
        if (current_count_ > 0 || buffer.remaining_size() > 0 ||
            has_next_package()) {
            return true;
        }
        return false;
    }

    bool has_next_package() {
        return current_count_ > 0 || byte_cache_.remaining_size() > 0;
    }

    int read_int(int32_t& result, common::ByteStream& buffer) {
        int ret = common::E_OK;
        if (!is_length_and_bitwidth_readed_) {
            // start to reader a new rle+bit-packing pattern
            if (RET_FAIL(read_length_and_bitwidth(buffer))) {
                return ret;
            }
        }
        if (current_count_ == 0) {
            // The header is encoded as an unsigned varint where:
            //   low bit = 0  => RLE run:      header_value >> 1 is the run
            //   count low bit = 1  => bit-packing:  header_value >> 1 is the
            //   group count
            uint32_t header_value = 0;
            if (RET_FAIL(common::SerializationUtil::read_var_uint(
                    header_value, byte_cache_))) {
                return ret;
            }
            if (header_value & 1) {
                if (RET_FAIL(call_read_bit_packing_buffer(header_value))) {
                    return ret;
                }
            } else {
                if (RET_FAIL(call_read_rle_run(header_value))) {
                    return ret;
                }
            }
        }
        --current_count_;
        result = is_rle_run_
                     ? rle_value_
                     : current_buffer_[bitpacking_num_ - current_count_ - 1];
        if (!has_next_package()) {
            is_length_and_bitwidth_readed_ = false;
        }
        return ret;
    }

    int call_read_rle_run(uint32_t header_value) {
        int ret = common::E_OK;
        int run_length = (int)(header_value >> 1);
        if (run_length <= 0) {
            return common::E_DECODE_ERR;
        }
        int byte_width = (bit_width_ + 7) / 8;
        // Read the repeated value (stored as byte_width bytes, little-endian)
        int32_t value = 0;
        for (int i = 0; i < byte_width; i++) {
            uint8_t b;
            if (RET_FAIL(common::SerializationUtil::read_ui8(b, byte_cache_))) {
                return ret;
            }
            value |= ((int32_t)b) << (i * 8);
        }
        rle_value_ = value;
        is_rle_run_ = true;
        current_count_ = run_length;
        bitpacking_num_ = run_length;
        return ret;
    }

    int call_read_bit_packing_buffer(uint32_t header_value) {
        int bit_packed_group_count = (int)(header_value >> 1);
        // in last bit-packing group, there may be some useless value,
        // lastBitPackedNum indicates how many values is useful
        uint8_t last_bit_packed_num;
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_ui8(last_bit_packed_num,
                                                         byte_cache_))) {
            return ret;
        }
        if (bit_packed_group_count > 0) {
            current_count_ =
                (bit_packed_group_count - 1) * 8 + last_bit_packed_num;
            bitpacking_num_ = current_count_;
            is_rle_run_ = false;
        } else {
            return common::E_DECODE_ERR;
        }
        ret = read_bit_packing_buffer(bit_packed_group_count,
                                      last_bit_packed_num);
        return ret;
    }

    int read_bit_packing_buffer(int bit_packed_group_count,
                                int last_bit_packed_num) {
        int ret = common::E_OK;
        if (current_buffer_ != nullptr) {
            common::mem_free(current_buffer_);
        }
        current_buffer_ = static_cast<int32_t*>(
            common::mem_alloc(sizeof(int32_t) * bit_packed_group_count * 8,
                              common::MOD_DECODER_OBJ));
        if (IS_NULL(current_buffer_)) {
            return common::E_OOM;
        }
        int bytes_to_read = bit_packed_group_count * bit_width_;
        if (bytes_to_read > (int)byte_cache_.remaining_size()) {
            bytes_to_read = byte_cache_.remaining_size();
        }
        std::vector<unsigned char> bytes(bytes_to_read);

        for (int i = 0; i < bytes_to_read; i++) {
            if (RET_FAIL(common::SerializationUtil::read_ui8(bytes[i],
                                                             byte_cache_))) {
                return ret;
            }
        }

        // save all int values in currentBuffer
        packer_->unpack_all_values(
            bytes.data(), bytes_to_read,
            current_buffer_);  // decode from bytes, save in currentBuffer
        return ret;
    }

    int read_length_and_bitwidth(common::ByteStream& buffer) {
        int ret = common::E_OK;
        if (RET_FAIL(
                common::SerializationUtil::read_var_uint(length_, buffer))) {
            return common::E_PARTIAL_READ;
        } else {
            if (tmp_buf_) {
                common::mem_free(tmp_buf_);
            }
            tmp_buf_ =
                (uint8_t*)common::mem_alloc(length_, common::MOD_DECODER_OBJ);
            if (tmp_buf_ == nullptr) {
                return common::E_OOM;
            }
            uint32_t ret_read_len = 0;
            if (RET_FAIL(buffer.read_buf((uint8_t*)tmp_buf_, length_,
                                         ret_read_len))) {
                return ret;
            } else if (length_ != ret_read_len) {
                ret = common::E_PARTIAL_READ;
            }
            byte_cache_.wrap_from((char*)tmp_buf_, length_);
            is_length_and_bitwidth_readed_ = true;
            uint8_t tmp_bit_width;
            common::SerializationUtil::read_ui8(tmp_bit_width, byte_cache_);
            bit_width_ = tmp_bit_width;
            if (packer_ != nullptr) {
                delete packer_;
            }
            init_packer();
        }
        return ret;
    }

    void init_packer() { packer_ = new Int32Packer(bit_width_); }

    void destroy() { /* do nothing for BitpackEncoder */
        if (packer_) {
            delete (packer_);
            packer_ = nullptr;
        }
        if (current_buffer_) {
            common::mem_free(current_buffer_);
            current_buffer_ = nullptr;
        }
        if (tmp_buf_) {
            common::mem_free(tmp_buf_);
            tmp_buf_ = nullptr;
        }
    }

    void reset() override {
        length_ = 0;
        bit_width_ = 0;
        bitpacking_num_ = 0;
        is_length_and_bitwidth_readed_ = false;
        current_count_ = 0;
        is_rle_run_ = false;
        rle_value_ = 0;
        if (current_buffer_) {
            common::mem_free(current_buffer_);
            current_buffer_ = nullptr;
        }
        if (packer_) {
            delete (packer_);
            packer_ = nullptr;
        }
        if (tmp_buf_) {
            common::mem_free(tmp_buf_);
            tmp_buf_ = nullptr;
        }
    }
};

}  // end namespace storage
#endif  // ENCODING_BITPACK_ENCODER_H
