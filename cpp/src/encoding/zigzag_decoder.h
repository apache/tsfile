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
class ZigzagDecoder {
   public:
    ZigzagDecoder() { init(); }
    ~ZigzagDecoder() { destroy(); }

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

    void reset() {
        bits_left_ = 0;
        buffer_ = 0;
        stored_value_ = 0;
        first_bit_of_byte_ = 0;
        num_of_sorts_of_zigzag_ = 0;
    }

    void destroy() {
        if (zigzag_decode_arr_ != nullptr) {
            common::mem_free(zigzag_decode_arr_);
            zigzag_decode_arr_ = nullptr;
        }
    }

    void read_header(common::ByteStream &in) {
        common::SerializationUtil::read_var_uint(zigzag_length_, in);
        common::SerializationUtil::read_var_uint(int_length_, in);
    }

    void flush_byte_if_empty(common::ByteStream &in) {
        if (bits_left_ == 0) {
            uint32_t read_len = 0;
            in.read_buf(&buffer_, 1, read_len);
            bits_left_ = 8;
        }
    }

    void read_byte_from_list() {
        buffer_ = (uint8_t)(list_transit_in_zd_[0]);
        list_transit_in_zd_.erase(list_transit_in_zd_.begin());
    }

    void fill_in_arr() {
        buffer_ &= ~(1 << 7);
        zigzag_decode_arr_[num_of_sorts_of_zigzag_] = buffer_;
        num_of_sorts_of_zigzag_ += 1;

        buffer_ = 0;
        bits_left_ = 0;
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

    int64_t zigzag_decoder(int64_t stored_value_) {
        stored_value_ = ((uint64_t)stored_value_ >> 1) ^ -(stored_value_ & 1);
        return stored_value_;
    }

    T decode(common::ByteStream &in);

   public:
    common::TSEncoding type_;
    uint8_t *zigzag_decode_arr_;
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

template <>
int32_t ZigzagDecoder<int32_t>::decode(common::ByteStream &in) {
    if (UNLIKELY(first_read_ == true)) {
        read_header(in);
        zigzag_decode_arr_ =
            (uint8_t *)common::mem_alloc(10, common::MOD_ZIGZAG_OBJ);
        buffer_ = 0;
        first_read_ = false;
        list_transit_in_zd_.clear();
        for (uint32_t i = 0; i < zigzag_length_; i++) {
            flush_byte_if_empty(in);
            list_transit_in_zd_.push_back(buffer_);
            buffer_ = 0;
            bits_left_ = 0;
        }
    }

    read_byte_from_list();
    read_the_first_bit_of_byte_();
    while (first_bit_of_byte_ == 1) {
        fill_in_arr();
        read_byte_from_list();
        read_the_first_bit_of_byte_();
    }
    fill_in_arr();
    splice_bytes_in_arr();

    int32_t ret_value = (int32_t)(stored_value_);
    ret_value = (int32_t)(zigzag_decoder(stored_value_));
    reset();
    return ret_value;
}

template <>
int64_t ZigzagDecoder<int64_t>::decode(common::ByteStream &in) {
    if (UNLIKELY(first_read_ == true)) {
        read_header(in);
        zigzag_decode_arr_ =
            (uint8_t *)common::mem_alloc(10, common::MOD_ZIGZAG_OBJ);
        buffer_ = 0;
        first_read_ = false;
        list_transit_in_zd_.clear();
        for (uint32_t i = 0; i < zigzag_length_; i++) {
            flush_byte_if_empty(in);
            list_transit_in_zd_.push_back(buffer_);
            buffer_ = 0;
            bits_left_ = 0;
        }
    }

    read_byte_from_list();
    read_the_first_bit_of_byte_();
    while (first_bit_of_byte_ == 1) {
        fill_in_arr();
        read_byte_from_list();
        read_the_first_bit_of_byte_();
    }
    fill_in_arr();
    splice_bytes_in_arr();

    int64_t ret_value = (int64_t)(stored_value_);
    ret_value = (int64_t)(zigzag_decoder(stored_value_));
    reset();
    return ret_value;
}

typedef ZigzagDecoder<int32_t> IntZigzagDecoder;
typedef ZigzagDecoder<int64_t> LongZigzagDecoder;

}  // end namespace storage
#endif  // ENCODING_zigzag_DECODER_H