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
#ifndef ENCODING_ZIGZAG_DECODER_H
#define ENCODING_ZIGZAG_DECODER_H

#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "utils/db_utils.h"
#include "utils/util_define.h"

namespace storage {

template <typename T>
class ZigzagDecoder : public Decoder {
   public:
    // A zigzag-encoded int32 value is at most 5 groups of 7 bits, and an
    // int64 value at most 10.
    static const int DECODE_ARR_CAPACITY = sizeof(T) == 8 ? 10 : 5;

    ZigzagDecoder() { init(); }
    ~ZigzagDecoder() override { destroy(); }

    void init() {
        type_ = common::ZIGZAG;
        bits_left_ = 0;
        buffer_ = 0;
        stored_value_ = 0;
        first_bit_of_byte_ = 0;
        num_of_sorts_of_zigzag_ = 0;
        first_read_ = true;
        zigzag_decode_arr_ = nullptr;
    }

    bool has_remaining(const common::ByteStream& buffer) override {
        return buffer.has_remaining() || !list_transit_in_zd_.empty();
    }
    int read_boolean(bool& ret_value, common::ByteStream& in) override {
        return common::E_TYPE_NOT_MATCH;
    }
    inline int read_int32(int32_t& ret_value, common::ByteStream& in) override {
        uint64_t stored_value = 0;
        int ret = decode(in, stored_value);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = static_cast<int32_t>(zigzag_decoder(stored_value));
        return common::E_OK;
    }
    inline int read_int64(int64_t& ret_value, common::ByteStream& in) override {
        uint64_t stored_value = 0;
        int ret = decode(in, stored_value);
        if (ret != common::E_OK) {
            return ret;
        }
        ret_value = static_cast<int64_t>(zigzag_decoder(stored_value));
        return common::E_OK;
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

    void each_encode_reset() {
        bits_left_ = 0;
        buffer_ = 0;
        stored_value_ = 0;
        first_bit_of_byte_ = 0;
        num_of_sorts_of_zigzag_ = 0;
    }

    void reset() override {
        type_ = common::ZIGZAG;
        bits_left_ = 0;
        buffer_ = 0;
        stored_value_ = 0;
        first_bit_of_byte_ = 0;
        num_of_sorts_of_zigzag_ = 0;
        first_read_ = true;
        destroy();
    }

    void destroy() {
        if (zigzag_decode_arr_ != nullptr) {
            common::mem_free(zigzag_decode_arr_);
            zigzag_decode_arr_ = nullptr;
        }
    }

    // Reads an unsigned 32-bit var-int with at most five bytes and rejects a
    // continuation past the fifth byte and payload bits above bit 31.
    int read_bounded_var_uint(common::ByteStream& in, uint32_t& value) {
        value = 0;
        for (int i = 0; i < 5; ++i) {
            uint8_t byte = 0;
            uint32_t read_len = 0;
            int ret = in.read_buf(&byte, 1, read_len);
            if (UNLIKELY(ret != common::E_OK || read_len != 1)) {
                return common::E_PARTIAL_READ;
            }
            // The fifth byte may carry only the four bits that still fit.
            if (UNLIKELY(i == 4 && (byte & 0xF0) != 0)) {
                return common::E_TSFILE_CORRUPTED;
            }
            value |= static_cast<uint32_t>(byte & 0x7F) << (7 * i);
            if ((byte & 0x80) == 0) {
                return common::E_OK;
            }
        }
        return common::E_TSFILE_CORRUPTED;
    }

    int read_header(common::ByteStream& in) {
        int ret = read_bounded_var_uint(in, zigzag_length_);
        if (ret != common::E_OK) {
            return ret;
        }
        return read_bounded_var_uint(in, int_length_);
    }

    void read_byte_from_list() {
        buffer_ = (uint8_t)(list_transit_in_zd_[0]);
        list_transit_in_zd_.erase(list_transit_in_zd_.begin());
    }

    bool fill_in_arr() {
        if (UNLIKELY(num_of_sorts_of_zigzag_ >= DECODE_ARR_CAPACITY)) {
            // More continuation bytes than any valid value can carry; writing
            // one more group would run past zigzag_decode_arr_.
            return false;
        }
        buffer_ &= ~(1 << 7);
        zigzag_decode_arr_[num_of_sorts_of_zigzag_] = buffer_;
        num_of_sorts_of_zigzag_ += 1;

        buffer_ = 0;
        bits_left_ = 0;
        return true;
    }

    void read_the_first_bit_of_byte_() {
        first_bit_of_byte_ = (int32_t)((buffer_ >> 7) & 0x1);
    }

    void splice_bytes_in_arr() {
        stored_value_ = 0;
        if (num_of_sorts_of_zigzag_ == 1) {
            stored_value_ = (uint64_t)(zigzag_decode_arr_[0]);
        } else {
            stored_value_ = (uint64_t)(zigzag_decode_arr_[0]);
            for (int i = 0; i < num_of_sorts_of_zigzag_; i++) {
                uint64_t value_shift = (uint64_t)(zigzag_decode_arr_[i])
                                       << (int)(7 * i);
                stored_value_ = value_shift | stored_value_;
            }
        }
    }

    static uint64_t zigzag_decoder(uint64_t stored_value) {
        return (stored_value >> 1) ^ (0 - (stored_value & 1));
    }

    int decode(common::ByteStream& in, uint64_t& ret_stored_value);

   public:
    common::TSEncoding type_;
    uint8_t* zigzag_decode_arr_;
    uint64_t stored_value_;
    int bits_left_;
    uint8_t buffer_;
    int first_bit_of_byte_;
    int num_of_sorts_of_zigzag_;
    bool first_read_;
    uint32_t zigzag_length_;
    uint32_t int_length_;
    std::vector<uint8_t> list_transit_in_zd_;
};

template <typename T>
inline int ZigzagDecoder<T>::decode(common::ByteStream& in,
                                    uint64_t& ret_stored_value) {
    int ret = common::E_OK;
    if (UNLIKELY(first_read_ == true)) {
        // zigzag_length_ is the byte count of every encoded value in the page,
        // so it cannot exceed the remaining input. Bounding it here also bounds
        // list_transit_in_zd_: a crafted var-int used to make the decoder push
        // that many bytes one at a time into memory.
        if (RET_FAIL(read_header(in))) {
            return common::E_TSFILE_CORRUPTED;
        }
        if (static_cast<uint64_t>(zigzag_length_) > in.remaining_size()) {
            return common::E_TSFILE_CORRUPTED;
        }
        if (zigzag_decode_arr_ == nullptr) {
            zigzag_decode_arr_ = (uint8_t*)common::mem_alloc(
                DECODE_ARR_CAPACITY, common::MOD_ZIGZAG_OBJ);
            if (UNLIKELY(zigzag_decode_arr_ == nullptr)) {
                return common::E_OOM;
            }
        }
        buffer_ = 0;
        first_read_ = false;
        list_transit_in_zd_.clear();
        for (uint32_t i = 0; i < zigzag_length_; i++) {
            uint32_t read_len = 0;
            ret = in.read_buf(&buffer_, 1, read_len);
            if (UNLIKELY(ret != common::E_OK || read_len != 1)) {
                return common::E_PARTIAL_READ;
            }
            list_transit_in_zd_.push_back(buffer_);
            buffer_ = 0;
            bits_left_ = 0;
        }
    }

    if (UNLIKELY(list_transit_in_zd_.empty())) {
        // A value whose continuation bit asks for another byte, after the
        // page's declared bytes are exhausted.
        return common::E_TSFILE_CORRUPTED;
    }
    read_byte_from_list();
    read_the_first_bit_of_byte_();
    while (first_bit_of_byte_ == 1) {
        if (UNLIKELY(!fill_in_arr())) {
            return common::E_TSFILE_CORRUPTED;
        }
        if (UNLIKELY(list_transit_in_zd_.empty())) {
            return common::E_TSFILE_CORRUPTED;
        }
        read_byte_from_list();
        read_the_first_bit_of_byte_();
    }
    if (UNLIKELY(!fill_in_arr())) {
        return common::E_TSFILE_CORRUPTED;
    }
    if (UNLIKELY(num_of_sorts_of_zigzag_ == DECODE_ARR_CAPACITY)) {
        // The last group may carry only the bits that still fit into T: four
        // bits for the fifth int32 group, one bit for the tenth int64 group.
        // Anything above would be silently truncated by the final cast.
        const uint8_t top = zigzag_decode_arr_[DECODE_ARR_CAPACITY - 1];
        const uint8_t excess_mask = sizeof(T) == 4 ? 0x70 : 0x7E;
        if (UNLIKELY((top & excess_mask) != 0)) {
            return common::E_TSFILE_CORRUPTED;
        }
    }
    splice_bytes_in_arr();

    ret_stored_value = stored_value_;
    each_encode_reset();
    return common::E_OK;
}

typedef ZigzagDecoder<int32_t> IntZigzagDecoder;
typedef ZigzagDecoder<int64_t> LongZigzagDecoder;

}  // end namespace storage
#endif  // ENCODING_zigzag_DECODER_H