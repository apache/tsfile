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

#ifndef ENCODING_TS2DIFF_DECODER_H
#define ENCODING_TS2DIFF_DECODER_H

#include <sys/types.h>

#include <cstddef>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "utils/util_define.h"

namespace storage {
template <typename T>
class TS2DIFFDecoder : public Decoder {
   public:
    TS2DIFFDecoder() { reset(); }
    ~TS2DIFFDecoder() {}

    void reset() {
        write_index_ = -1;
        bits_left_ = 0;
        stored_value_ = 0;
        buffer_ = 0;
        delta_min_ = 0;
        first_value_ = 0;
        previous_value_ = 0;
        bit_width_ = 0;
        current_index_ = 0;
    }

    FORCE_INLINE bool has_remaining() {
        // std::cout << "has_remaining, current_index_=" << current_index_ << ",
        // write_index_=" << write_index_ << std::endl;
        return bits_left_ != 0 || (current_index_ <= write_index_ &&
                                   write_index_ != -1 && current_index_ != 0);
    }

    void read_header(common::ByteStream &in) {
        common::SerializationUtil::read_i32(write_index_, in);
        common::SerializationUtil::read_i32(bit_width_, in);
    }

    // If empty, cache 8 bits from in_stream to 'buffer_'.
    void read_byte_if_empty(common::ByteStream &in) {
        if (bits_left_ == 0) {
            uint32_t read_len = 0;
            in.read_buf(&buffer_, 1, read_len);
            if (read_len != 0) {
                bits_left_ = 8;
            }
        }
    }

    int64_t read_long(int bits, common::ByteStream &in) {
        int64_t value = 0;
        while (bits > 0) {
            if (bits > bits_left_ || bits == 8) {
                // Take only the bits_left_ "least significant" bits.
                uint8_t d = (uint8_t)(buffer_ & ((1 << bits_left_) - 1));
                value = (value << bits_left_) + (d & 0xFF);
                bits -= bits_left_;
                bits_left_ = 0;
            } else {
                // Shift to correct position and take only least significant
                // bits.
                uint8_t d =
                    (uint8_t)((((uint8_t)buffer_) >> (bits_left_ - bits)) &
                              ((1 << bits) - 1));
                value = (value << bits) + (d & 0xFF);
                bits_left_ -= bits;
                bits = 0;
            }
            if (bits <= 0 && current_index_ == 0) {
                break;
            }
            read_byte_if_empty(in);
        }
        return value;
    }

    T decode(common::ByteStream &in);
    int read_boolean(bool &ret_value, common::ByteStream &in);
    int read_int32(int32_t &ret_value, common::ByteStream &in);
    int read_int64(int64_t &ret_value, common::ByteStream &in);
    int read_float(float &ret_value, common::ByteStream &in);
    int read_double(double &ret_value, common::ByteStream &in);

   public:
    T first_value_;
    T previous_value_;
    T stored_value_;
    T delta_min_;
    uint8_t buffer_;
    int bits_left_;
    int bit_width_;
    int write_index_;
    int current_index_;
};

template <>
inline int32_t TS2DIFFDecoder<int32_t>::decode(common::ByteStream &in) {
    int32_t ret_value = stored_value_;
    if (UNLIKELY(current_index_ == 0)) {
        read_header(in);
        common::SerializationUtil::read_i32(delta_min_, in);
        common::SerializationUtil::read_i32(first_value_, in);
        ret_value = first_value_;
        bits_left_ = 0;
        buffer_ = 0;
        read_byte_if_empty(in);
        current_index_ = 1;
        return ret_value;
    }
    if (current_index_++ >= write_index_) {
        current_index_ = 0;
    }
    stored_value_ = (int32_t)read_long(bit_width_, in);
    ret_value = stored_value_ + first_value_ + delta_min_;
    first_value_ = ret_value;

    return ret_value;
}

template <>
inline int64_t TS2DIFFDecoder<int64_t>::decode(common::ByteStream &in) {
    int64_t ret_value = stored_value_;
    if (UNLIKELY(current_index_ == 0)) {
        read_header(in);
        common::SerializationUtil::read_i64(delta_min_, in);
        common::SerializationUtil::read_i64(first_value_, in);
        ret_value = first_value_;
        read_byte_if_empty(in);
        current_index_ = 1;
        return ret_value;
    }
    if (current_index_++ >= write_index_) {
        current_index_ = 0;
    }
    stored_value_ = (int64_t)read_long(bit_width_, in);
    ret_value = stored_value_ + first_value_ + delta_min_;
    first_value_ = ret_value;

    // std::cout << "decode, current_index_=" << current_index_ << ",
    // write_index_=" << write_index_ << ", ret_value=" << ret_value <<
    // std::endl;

    return ret_value;
}

class FloatTS2DIFFDecoder : public TS2DIFFDecoder<int32_t> {
   public:
    float decode(common::ByteStream &in) {
        int32_t value_int = TS2DIFFDecoder<int32_t>::decode(in);
        return common::int_to_float(value_int);
    }

    int read_boolean(bool &ret_value, common::ByteStream &in);
    int read_int32(int32_t &ret_value, common::ByteStream &in);
    int read_int64(int64_t &ret_value, common::ByteStream &in);
    int read_float(float &ret_value, common::ByteStream &in);
    int read_double(double &ret_value, common::ByteStream &in);
};

class DoubleTS2DIFFDecoder : public TS2DIFFDecoder<int64_t> {
   public:
    double decode(common::ByteStream &in) {
        int64_t value_long = TS2DIFFDecoder<int64_t>::decode(in);
        return common::long_to_double(value_long);
    }

    int read_boolean(bool &ret_value, common::ByteStream &in);
    int read_int32(int32_t &ret_value, common::ByteStream &in);
    int read_int64(int64_t &ret_value, common::ByteStream &in);
    int read_float(float &ret_value, common::ByteStream &in);
    int read_double(double &ret_value, common::ByteStream &in);
};

typedef TS2DIFFDecoder<int32_t> IntTS2DIFFDecoder;
typedef TS2DIFFDecoder<int64_t> LongTS2DIFFDecoder;

// wrap as Decoder interface
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_boolean(bool &ret_value,
                                                 common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_int32(int32_t &ret_value,
                                               common::ByteStream &in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_int64(int64_t &ret_value,
                                               common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_float(float &ret_value,
                                               common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_double(double &ret_value,
                                                common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_boolean(bool &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_int32(int32_t &ret_value,
                                                common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_int64(int64_t &ret_value,
                                                common::ByteStream &in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_float(float &ret_value,
                                                common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_double(double &ret_value,
                                                 common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_boolean(bool &ret_value,
                                                   common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_int32(int32_t &ret_value,
                                                 common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_int64(int64_t &ret_value,
                                                 common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_float(float &ret_value,
                                                 common::ByteStream &in) {
    ret_value = decode(in);
    return common::E_OK;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_double(double &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_boolean(bool &ret_value,
                                                    common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_int32(int32_t &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_int64(int64_t &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_float(float &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_double(double &ret_value,
                                                   common::ByteStream &in) {
    ret_value = decode(in);
    return common::E_OK;
}

}  // end namespace storage
#endif  // ENCODING_TS2DIFF_DECODER_H
