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
#ifndef ENCODING_ZIGZAG_ENCODER_H
#define ENCODING_ZIGZAG_ENCODER_H

#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoder.h"
#include "utils/db_utils.h"
#include "utils/util_define.h"

namespace storage {

template <typename T>
class ZigzagEncoder {
   public:
    ZigzagEncoder() { init(); }

    ~ZigzagEncoder() {}

    void destroy() {}

    void init() {
        type_ = common::ZIGZAG;
        buffer_ = 0;
        length_of_input_bytestream_ = 0;
        length_of_encode_bytestream_ = 0;
        first_read_ = true;
    }

    void reset() {
        type_ = common::ZIGZAG;
        buffer_ = 0;
        length_of_input_bytestream_ = 0;
        length_of_encode_bytestream_ = 0;
        list_transit_in_ze_.clear();
        first_read_ = true;
    }

    void flush_byte(common::ByteStream &out) {
        out.write_buf(&buffer_, 1);
        buffer_ = 0;
    }

    void add_byte_to_trans() {
        list_transit_in_ze_.push_back(buffer_);
        length_of_encode_bytestream_ += 1;
        buffer_ = 0;
    }

    void write_byte_with_subsequence(T value_zigzag) {
        buffer_ = (uint8_t)((value_zigzag | 0x80) & 0xFF);
        add_byte_to_trans();
    }

    void write_byte_without_subsequence(T value_zigzag) {
        buffer_ = (uint8_t)(value_zigzag & 0x7F);
        add_byte_to_trans();
    }

    int encode(T value);
    int flush(common::ByteStream &out);

   public:
    common::TSEncoding type_;
    uint8_t buffer_;
    int length_of_input_bytestream_;
    int length_of_encode_bytestream_;
    std::vector<uint8_t> list_transit_in_ze_;
    bool first_read_;
};

template <>
int ZigzagEncoder<int32_t>::encode(int32_t value) {
    if (UNLIKELY(first_read_ == true)) {
        reset();
        first_read_ = false;
    }
    length_of_input_bytestream_ += 1;
    int32_t value_zigzag = (value << 1) ^ (value >> 31);
    if ((value_zigzag & ~0x7F) != 0) {
        write_byte_with_subsequence(value_zigzag);
        value_zigzag = (uint32_t)value_zigzag >> 7;
        while ((value_zigzag & ~0x7F) != 0) {
            write_byte_with_subsequence(value_zigzag);
            value_zigzag = (uint32_t)value_zigzag >> 7;
        }
    }

    write_byte_without_subsequence(value_zigzag);
    value_zigzag = (uint32_t)value_zigzag >> 7;

    return common::E_OK;
}

template <>
int ZigzagEncoder<int64_t>::encode(int64_t value) {
    if (UNLIKELY(first_read_ == true)) {
        reset();
        first_read_ = false;
    }
    length_of_input_bytestream_ += 1;
    int64_t value_zigzag = (value << 1) ^ (value >> 63);
    if ((value_zigzag & ~0x7F) != 0) {
        write_byte_with_subsequence(value_zigzag);
        value_zigzag = (uint64_t)value_zigzag >> 7;
        while ((value_zigzag & ~0x7F) != 0) {
            write_byte_with_subsequence(value_zigzag);
            value_zigzag = (uint64_t)value_zigzag >> 7;
        }
    }

    write_byte_without_subsequence(value_zigzag);
    value_zigzag = (uint64_t)value_zigzag >> 7;

    return common::E_OK;
}

template <>
int ZigzagEncoder<int32_t>::flush(common::ByteStream &out) {
    buffer_ = (uint8_t)(length_of_encode_bytestream_);
    flush_byte(out);

    buffer_ = (uint8_t)(length_of_input_bytestream_);
    flush_byte(out);

    for (int i = 0; i < length_of_encode_bytestream_; i++) {
        buffer_ = (uint8_t)(list_transit_in_ze_[i]);
        flush_byte(out);
    }
    reset();
    return common::E_OK;
}

template <>
int ZigzagEncoder<int64_t>::flush(common::ByteStream &out) {
    buffer_ = (uint8_t)(length_of_encode_bytestream_);
    flush_byte(out);

    buffer_ = (uint8_t)(length_of_input_bytestream_);
    flush_byte(out);

    for (int i = 0; i < length_of_encode_bytestream_; i++) {
        buffer_ = (uint8_t)(list_transit_in_ze_[i]);
        flush_byte(out);
    }
    reset();
    return common::E_OK;
}

typedef ZigzagEncoder<int32_t> IntZigzagEncoder;
typedef ZigzagEncoder<int64_t> LongZigzagEncoder;

}  // end namespace storage
#endif  // ENCODING_ZIGZAG_ENCODER_H