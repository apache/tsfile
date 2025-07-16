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
class ZigzagEncoder : public Encoder {
   public:
    ZigzagEncoder() { init(); }

    ~ZigzagEncoder() override = default;

    void destroy() override {}

    // int init(common::TSDataType data_type) = 0;
    int encode(bool value, common::ByteStream &out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t value, common::ByteStream &out_stream) override;
    int encode(int64_t value, common::ByteStream &out_stream) override;
    int encode(float value, common::ByteStream &out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(double value, common::ByteStream &out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String value, common::ByteStream &out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }

    int get_max_byte_size() override {
        if (list_transit_in_ze_.empty()) {
            return 0;
        }
        return 8 + list_transit_in_ze_.size();
    }

    void init() {
        type_ = common::ZIGZAG;
        buffer_ = 0;
        length_of_input_bytestream_ = 0;
        length_of_encode_bytestream_ = 0;
        first_read_ = true;
    }

    void reset() override {
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

    inline int encode(T value);

    inline int flush(common::ByteStream &out) override;

   public:
    common::TSEncoding type_;
    uint8_t buffer_ = 0;
    int length_of_input_bytestream_ = 0;
    int length_of_encode_bytestream_ = 0;
    std::vector<uint8_t> list_transit_in_ze_;
    bool first_read_{};
};

template <typename T>
inline int ZigzagEncoder<T>::encode(int32_t /*value*/,
                                    common::ByteStream & /*out*/) {
    return common::E_TYPE_NOT_MATCH;
}

template <typename T>
inline int ZigzagEncoder<T>::encode(int64_t /*value*/,
                                    common::ByteStream & /*out*/) {
    return common::E_TYPE_NOT_MATCH;
}

template <>
inline int ZigzagEncoder<int32_t>::encode(int32_t value) {
    if (UNLIKELY(first_read_ == true)) {
        reset();
        first_read_ = false;
    }
    length_of_input_bytestream_ += 1;
    int32_t value_zigzag =
        static_cast<int32_t>((static_cast<uint32_t>(value) << 1) ^
                             static_cast<uint32_t>(value >> 31));

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
inline int ZigzagEncoder<int32_t>::encode(int32_t value,
                                          common::ByteStream &out_stream) {
    return encode(value);
}

template <>
inline int ZigzagEncoder<int64_t>::encode(int64_t value) {
    if (UNLIKELY(first_read_ == true)) {
        reset();
        first_read_ = false;
    }
    length_of_input_bytestream_ += 1;
    int64_t value_zigzag =
        static_cast<int64_t>((static_cast<uint64_t>(value) << 1) ^
                             static_cast<uint64_t>(value >> 63));

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
inline int ZigzagEncoder<int64_t>::encode(int64_t value,
                                          common::ByteStream &out_stream) {
    return encode(value);
}

template <>
inline int ZigzagEncoder<int32_t>::flush(common::ByteStream &out) {
    common::SerializationUtil::write_var_uint(length_of_encode_bytestream_,
                                              out);
    common::SerializationUtil::write_var_uint(length_of_input_bytestream_, out);

    for (int i = 0; i < length_of_encode_bytestream_; i++) {
        buffer_ = (uint8_t)(list_transit_in_ze_[i]);
        flush_byte(out);
    }
    reset();
    return common::E_OK;
}

template <>
inline int ZigzagEncoder<int64_t>::flush(common::ByteStream &out) {
    common::SerializationUtil::write_var_uint(length_of_encode_bytestream_,
                                              out);
    common::SerializationUtil::write_var_uint(length_of_input_bytestream_, out);

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