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
#ifndef ENCODING_GORILLA_DECODER_H
#define ENCODING_GORILLA_DECODER_H

#include <climits>

#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "encode_utils.h"
#include "gorilla_encoder.h"
#include "utils/db_utils.h"
#include "utils/util_define.h"

namespace storage {

template <typename T>
class GorillaDecoder : public Decoder {
   public:
    GorillaDecoder() { reset(); }

    ~GorillaDecoder() {}

    void reset() {
        type_ = common::GORILLA;
        stored_value_ = 0;
        stored_leading_zeros_ = INT32_MAX;
        stored_trailing_zeros_ = 0;
        bits_left_ = 0;
        first_value_was_read_ = false;
        has_next_ = true;
        buffer_ = 0;
    }

    FORCE_INLINE bool has_next() { return has_next_; }
    FORCE_INLINE bool has_remaining() { return has_next(); }

    // If empty, cache 8 bits from in_stream to 'buffer_'.
    void flush_byte_if_empty(common::ByteStream &in) {
        if (bits_left_ == 0) {
            uint32_t read_len = 0;
            in.read_buf(&buffer_, 1, read_len);
            bits_left_ = 8;
        }
    }

    // Reads the next bit and returns true if the next bit is 1, otherwise 0.
    bool read_bit(common::ByteStream &in) {
        bool bit = ((buffer_ >> (bits_left_ - 1)) & 1) == 1;
        bits_left_--;
        flush_byte_if_empty(in);
        return bit;
    }

    /*
     * Reads a long from the next X bits that represent the least significant
     * bits in the long value.
     * @bits: How many next bits are reader from the stream
     * return: long value that was reader from the stream
     */
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
            flush_byte_if_empty(in);
        }
        return value;
    }

    // Read the control bits
    uint8_t read_next_control_bit(int max_bits, common::ByteStream &in) {
        uint8_t value = 0x00;
        for (int i = 0; i < max_bits; i++) {
            value <<= 1;
            if (read_bit(in)) {
                value |= 0x01;
            } else {
                break;
            }
        }
        return value;
    }

    T read_next(common::ByteStream &in);
    virtual T cache_next(common::ByteStream &in);
    T decode(common::ByteStream &in);

    // interface from Decoder
    int read_boolean(bool &ret_value, common::ByteStream &in);
    int read_int32(int32_t &ret_value, common::ByteStream &in);
    int read_int64(int64_t &ret_value, common::ByteStream &in);
    int read_float(float &ret_value, common::ByteStream &in);
    int read_double(double &ret_value, common::ByteStream &in);

   public:
    common::TSEncoding type_;
    T stored_value_;
    int stored_leading_zeros_;
    int stored_trailing_zeros_;
    int bits_left_;
    bool first_value_was_read_;
    bool has_next_;
    uint8_t buffer_;
};

template <>
FORCE_INLINE int32_t
GorillaDecoder<int32_t>::read_next(common::ByteStream &in) {
    uint8_t control_bits = read_next_control_bit(2, in);
    uint8_t significant_bits = 0;
    int32_t xor_value = 0;
    switch (control_bits) {
        case 3:  // case '11': use new leading and trailing zeros
            stored_leading_zeros_ =
                (int)read_long(LEADING_ZERO_BITS_LENGTH_32BIT,
                               in);  // todo: int or int32_t?
            significant_bits =
                (uint8_t)read_long(MEANINGFUL_XOR_BITS_LENGTH_32BIT, in);
            significant_bits++;
            stored_trailing_zeros_ = VALUE_BITS_LENGTH_32BIT -
                                     significant_bits - stored_leading_zeros_;
            // missing break is intentional, we want to overflow to next one
        case 2:  // case '10': use stored leading and trailing zeros
            xor_value = (int32_t)read_long(VALUE_BITS_LENGTH_32BIT -
                                               stored_leading_zeros_ -
                                               stored_trailing_zeros_,
                                           in);
            xor_value <<= stored_trailing_zeros_;
            stored_value_ ^= xor_value;
            // missing break is intentional, we want to overflow to next one
        default:  // case '0': use stored value
            return stored_value_;
    }
    return stored_value_;
}

template <>
FORCE_INLINE int64_t
GorillaDecoder<int64_t>::read_next(common::ByteStream &in) {
    uint8_t control_bits = read_next_control_bit(2, in);

    uint8_t significant_bits = 0;
    int64_t xor_value = 0;
    switch (control_bits) {
        case 3: {  // case '11': use new leading and trailing zeros
            stored_leading_zeros_ =
                (int)read_long(LEADING_ZERO_BITS_LENGTH_64BIT,
                               in);  // todo: int or int32_t?
            significant_bits =
                (uint8_t)read_long(MEANINGFUL_XOR_BITS_LENGTH_64BIT, in);
            significant_bits++;
            stored_trailing_zeros_ = VALUE_BITS_LENGTH_64BIT -
                                     significant_bits - stored_leading_zeros_;
            // missing break is intentional, we want to overflow to next one
        }
        case 2: {  // case '10': use stored leading and trailing zeros
            xor_value =
                read_long(VALUE_BITS_LENGTH_64BIT - stored_leading_zeros_ -
                              stored_trailing_zeros_,
                          in);
            xor_value <<= stored_trailing_zeros_;
            stored_value_ ^= xor_value;
            // missing break is intentional, we want to overflow to next one
        }
        default: {  // case '0': use stored value
            return stored_value_;
        }
    }
    return stored_value_;
}

template <>
FORCE_INLINE int32_t
GorillaDecoder<int32_t>::cache_next(common::ByteStream &in) {
    read_next(in);
    if (stored_value_ == GORILLA_ENCODING_ENDING_INTEGER) {
        has_next_ = false;
    }
    return stored_value_;
}

template <>
FORCE_INLINE int64_t
GorillaDecoder<int64_t>::cache_next(common::ByteStream &in) {
    read_next(in);
    if (stored_value_ == GORILLA_ENCODING_ENDING_LONG) {
        has_next_ = false;
    }
    return stored_value_;
}

template <>
FORCE_INLINE int32_t GorillaDecoder<int32_t>::decode(common::ByteStream &in) {
    int32_t ret_value = stored_value_;
    if (UNLIKELY(!first_value_was_read_)) {
        flush_byte_if_empty(in);
        stored_value_ = (int32_t)read_long(VALUE_BITS_LENGTH_32BIT, in);
        first_value_was_read_ = true;
        ret_value = stored_value_;
    }
    cache_next(in);
    return ret_value;
}

template <>
FORCE_INLINE int64_t GorillaDecoder<int64_t>::decode(common::ByteStream &in) {
    int64_t ret_value = stored_value_;
    if (UNLIKELY(!first_value_was_read_)) {
        flush_byte_if_empty(in);
        stored_value_ = read_long(VALUE_BITS_LENGTH_64BIT, in);
        first_value_was_read_ = true;
        ret_value = stored_value_;
    }
    cache_next(in);
    return ret_value;
}

class FloatGorillaDecoder : public GorillaDecoder<int32_t> {
   public:
    int read_boolean(bool &ret_value, common::ByteStream &in);
    int read_int32(int32_t &ret_value, common::ByteStream &in);
    int read_int64(int64_t &ret_value, common::ByteStream &in);
    int read_float(float &ret_value, common::ByteStream &in);
    int read_double(double &ret_value, common::ByteStream &in);

    float decode(common::ByteStream &in) {
        int32_t value_int = GorillaDecoder<int32_t>::decode(in);
        return common::int_to_float(value_int);
    }

    int32_t cache_next(common::ByteStream &in) {
        read_next(in);
        if (stored_value_ ==
            common::float_to_int(GORILLA_ENCODING_ENDING_FLOAT)) {
            has_next_ = false;
        }
        return stored_value_;
    }
};

class DoubleGorillaDecoder : public GorillaDecoder<int64_t> {
   public:
    int read_boolean(bool &ret_value, common::ByteStream &in);
    int read_int32(int32_t &ret_value, common::ByteStream &in);
    int read_int64(int64_t &ret_value, common::ByteStream &in);
    int read_float(float &ret_value, common::ByteStream &in);
    int read_double(double &ret_value, common::ByteStream &in);

    double decode(common::ByteStream &in) {
        int64_t value_long = GorillaDecoder<int64_t>::decode(in);
        return common::long_to_double(value_long);
    }

    int64_t cache_next(common::ByteStream &in) {
        read_next(in);
        if (stored_value_ ==
            common::double_to_long(GORILLA_ENCODING_ENDING_DOUBLE)) {
            has_next_ = false;
        }
        return stored_value_;
    }
};

typedef GorillaDecoder<int32_t> IntGorillaDecoder;
typedef GorillaDecoder<int64_t> LongGorillaDecoder;

// wrap as Decoder interface
template <>
FORCE_INLINE int IntGorillaDecoder::read_boolean(bool &ret_value,
                                                 common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntGorillaDecoder::read_int32(int32_t &ret_value,
                                               common::ByteStream &in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int IntGorillaDecoder::read_int64(int64_t &ret_value,
                                               common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntGorillaDecoder::read_float(float &ret_value,
                                               common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntGorillaDecoder::read_double(double &ret_value,
                                                common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_boolean(bool &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_int32(int32_t &ret_value,
                                                common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_int64(int64_t &ret_value,
                                                common::ByteStream &in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_float(float &ret_value,
                                                common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_double(double &ret_value,
                                                 common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatGorillaDecoder::read_boolean(bool &ret_value,
                                                   common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatGorillaDecoder::read_int32(int32_t &ret_value,
                                                 common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatGorillaDecoder::read_int64(int64_t &ret_value,
                                                 common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatGorillaDecoder::read_float(float &ret_value,
                                                 common::ByteStream &in) {
    ret_value = decode(in);
    return common::E_OK;
}
FORCE_INLINE int FloatGorillaDecoder::read_double(double &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_boolean(bool &ret_value,
                                                    common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_int32(int32_t &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_int64(int64_t &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_float(float &ret_value,
                                                  common::ByteStream &in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_double(double &ret_value,
                                                   common::ByteStream &in) {
    ret_value = decode(in);
    return common::E_OK;
}

}  // end namespace storage
#endif  // ENCODING_GORILLA_DECODER_H
