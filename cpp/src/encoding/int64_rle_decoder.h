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

#ifndef ENCODING_INT64RLE_DECODER_H
#define ENCODING_INT64RLE_DECODER_H

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "decoder.h"
#include "encoder.h"
#include "encoding/encode_utils.h"
#include "encoding/int64_packer.h"

namespace storage {

class Int64RleDecoder : public Decoder {
   private:
    uint32_t length_;
    uint32_t bit_width_;
    int bitpacking_num_;
    bool is_length_and_bitwidth_readed_;
    int current_count_;
    bool is_rle_run_;
    int64_t rle_value_;
    common::ByteStream byte_cache_{common::MOD_DECODER_OBJ};
    int64_t* current_buffer_;
    Int64Packer* packer_;
    uint8_t* tmp_buf_;

   public:
    Int64RleDecoder()
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
    ~Int64RleDecoder() override { destroy(); }

    bool has_remaining(const common::ByteStream& buffer) override {
        return buffer.has_remaining() || has_next_package();
    }
    int read_boolean(bool& ret_value, common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        return read_int(ret_value, in);
    }
    int read_batch_int64(int64_t* out, int capacity, int& actual,
                         common::ByteStream& in) override {
        actual = 0;
        int ret = common::E_OK;
        while (actual < capacity && has_remaining(in)) {
            if (!is_length_and_bitwidth_readed_) {
                if (RET_FAIL(read_length_and_bitwidth(in))) {
                    return ret;
                }
            }
            if (current_count_ == 0) {
                // Low bit = 0 => RLE run, = 1 => bit-packing group (same
                // header dispatch as read_int).
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
            if (current_count_ <= 0) {
                // A validated header always yields at least one value; a
                // non-positive count here means the pattern was corrupted.
                return common::E_TSFILE_CORRUPTED;
            }
            const int n = std::min(current_count_, capacity - actual);
            if (is_rle_run_) {
                // The whole run holds the same value; a broadcast fill is
                // auto-vectorized by the compiler.
                std::fill(out + actual, out + actual + n, rle_value_);
            } else {
                // Bit-packing groups are already unpacked into current_buffer_
                // by the packer, so the remaining values are a straight copy.
                const int64_t* src =
                    current_buffer_ + (bitpacking_num_ - current_count_);
                std::memcpy(out + actual, src,
                            static_cast<size_t>(n) * sizeof(int64_t));
            }
            actual += n;
            current_count_ -= n;
            if (!has_next_package()) {
                is_length_and_bitwidth_readed_ = false;
            }
        }
        return ret;
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

    void init() { reset(); }

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

    int read_int(int64_t& result, common::ByteStream& buffer) {
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
        // The repeated value is stored big-endian (most significant byte
        // first), matching the Java/IoTDB LongRleEncoder
        // (BytesUtils.longToBytes(value, byteWidth)). This differs from the
        // INT32 RLE path, which stores the repeated value little-endian.
        int64_t value = 0;
        for (int i = 0; i < byte_width; i++) {
            uint8_t b;
            if (RET_FAIL(common::SerializationUtil::read_ui8(b, byte_cache_))) {
                return ret;
            }
            value =
                static_cast<int64_t>((static_cast<uint64_t>(value) << 8) | b);
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
        if (bit_packed_group_count <= 0) {
            return common::E_DECODE_ERR;
        }
        if (last_bit_packed_num == 0 || last_bit_packed_num > NUM_OF_INTS) {
            // Encoders write 1..8. Anything else makes current_count_ disagree
            // with the number of values actually packed, which the batch
            // memcpy would then use as an unbounded length.
            return common::E_TSFILE_CORRUPTED;
        }
        const int64_t value_count =
            (static_cast<int64_t>(bit_packed_group_count) - 1) * NUM_OF_INTS +
            last_bit_packed_num;
        if (value_count > INT32_MAX) {
            return common::E_TSFILE_CORRUPTED;
        }
        current_count_ = static_cast<int>(value_count);
        bitpacking_num_ = current_count_;
        is_rle_run_ = false;
        ret = read_bit_packing_buffer(bit_packed_group_count,
                                      last_bit_packed_num);
        if (ret != common::E_OK) {
            // Do not leave a value count pointing at an unfilled buffer.
            current_count_ = 0;
            bitpacking_num_ = 0;
        }
        return ret;
    }

    int read_bit_packing_buffer(int bit_packed_group_count,
                                int last_bit_packed_num) {
        (void)last_bit_packed_num;
        int ret = common::E_OK;
        // bit_width_ comes from the file. Validate it before using it to size
        // the bulk read: a 32-bit product of a corrupt group count and width
        // can wrap to a negative byte count and make the read below write past
        // the bytes vector.
        if (bit_width_ == 0 || bit_width_ > 64) {
            return common::E_TSFILE_CORRUPTED;
        }
        const uint64_t group_bytes =
            static_cast<uint64_t>(bit_packed_group_count) * bit_width_;
        if (group_bytes > byte_cache_.remaining_size() ||
            group_bytes > static_cast<uint64_t>(INT32_MAX)) {
            // The declared groups do not fit in the pattern.
            return common::E_PARTIAL_READ;
        }
        const uint64_t value_count =
            static_cast<uint64_t>(bit_packed_group_count) * NUM_OF_INTS;
        if (value_count > UINT32_MAX / sizeof(int64_t)) {
            // The value buffer size must fit mem_alloc's uint32_t argument.
            return common::E_TSFILE_CORRUPTED;
        }

        if (current_buffer_ != nullptr) {
            common::mem_free(current_buffer_);
            current_buffer_ = nullptr;
        }
        current_buffer_ = static_cast<int64_t*>(common::mem_alloc(
            static_cast<uint32_t>(value_count * sizeof(int64_t)),
            common::MOD_DECODER_OBJ));
        if (IS_NULL(current_buffer_)) {
            return common::E_OOM;
        }
        const int bytes_to_read = static_cast<int>(group_bytes);
        std::vector<unsigned char> bytes(bytes_to_read, 0);

        uint32_t got = 0;
        if (RET_FAIL(byte_cache_.read_buf(
                bytes.data(), static_cast<uint32_t>(bytes_to_read), got))) {
            return ret;
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
        }
        if (length_ > static_cast<uint32_t>(INT32_MAX)) {
            return common::E_TSFILE_CORRUPTED;
        }
        // Prefer a read-only view over the caller's buffer instead of copying
        // the whole pattern into tmp_buf_. `buffer` is the current value
        // page's ByteStream and outlives the pattern decode; byte_cache_ only
        // reads from it and is cleared by reset()/destroy(). Fall back to the
        // copy when the stream is paged or shorter than the pattern.
        if (buffer.is_wrapped() && buffer.remaining_size() >= length_) {
            byte_cache_.wrap_from(buffer.get_wrapped_buf() + buffer.read_pos(),
                                  static_cast<int32_t>(length_));
            buffer.wrapped_buf_advance_read_pos(length_);
        } else {
            if (tmp_buf_ != nullptr) {
                common::mem_free(tmp_buf_);
                tmp_buf_ = nullptr;
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
            }
            byte_cache_.wrap_from((char*)tmp_buf_, length_);
        }
        uint8_t tmp_bit_width = 0;
        if (RET_FAIL(common::SerializationUtil::read_ui8(tmp_bit_width,
                                                         byte_cache_))) {
            return ret;
        }
        if (tmp_bit_width == 0 || tmp_bit_width > 64) {
            return common::E_TSFILE_CORRUPTED;
        }
        bit_width_ = tmp_bit_width;
        if (packer_ != nullptr) {
            delete packer_;
        }
        init_packer();
        is_length_and_bitwidth_readed_ = true;
        return ret;
    }

    void init_packer() { packer_ = new Int64Packer(bit_width_); }

    void destroy() {
        // Drop the borrowed pattern view before freeing tmp_buf_ so no stale
        // pointer into caller memory (or into tmp_buf_) survives.
        byte_cache_.reset();
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
        byte_cache_.reset();
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
