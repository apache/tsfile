#ifndef ENCODING_GORILLA_ENCODER_H
#define ENCODING_GORILLA_ENCODER_H

#include <climits>
#include <cmath>

#include "common/allocator/byte_stream.h"
#include "encode_utils.h"
#include "encoder.h"
#include "utils/db_utils.h"
#include "utils/util_define.h"

#define VALUE_BITS_LENGTH_32BIT 32
#define LEADING_ZERO_BITS_LENGTH_32BIT 5
#define MEANINGFUL_XOR_BITS_LENGTH_32BIT 5

#define VALUE_BITS_LENGTH_64BIT 64
#define LEADING_ZERO_BITS_LENGTH_64BIT 6
#define MEANINGFUL_XOR_BITS_LENGTH_64BIT 6

#define INT32_ONE_ITEM_MAX_SIZE                                              \
    (2 + LEADING_ZERO_BITS_LENGTH_32BIT + MEANINGFUL_XOR_BITS_LENGTH_32BIT + \
     VALUE_BITS_LENGTH_32BIT) /                                              \
            8 +                                                              \
        1

#define INT64_ONE_ITEM_MAX_SIZE                                              \
    (2 + LEADING_ZERO_BITS_LENGTH_64BIT + MEANINGFUL_XOR_BITS_LENGTH_64BIT + \
     VALUE_BITS_LENGTH_64BIT) /                                              \
            8 +                                                              \
        1

#define GORILLA_ENCODING_ENDING_INTEGER INT32_MIN
#define GORILLA_ENCODING_ENDING_LONG INT64_MIN
#define GORILLA_ENCODING_ENDING_FLOAT nanf("")
#define GORILLA_ENCODING_ENDING_DOUBLE nan("")

namespace storage {

template <typename T>
class GorillaEncoder : public Encoder {
   public:
    GorillaEncoder() { reset(); }
    ~GorillaEncoder() {}
    void destroy() {}

    void reset() {
        type_ = common::GORILLA;
        stored_leading_zeros_ = INT32_MAX;
        stored_trailing_zeros_ = 0;
        bits_left_ = 8;
        first_value_was_written_ = false;
        buffer_ = 0;
        stored_value_ = 0;
    }

    // If full, flush bits cached in 'buffer_' to out_stream.
    void flush_byte_if_full(common::ByteStream &out) {
        if (bits_left_ == 0) {
            out.write_buf(&buffer_, 1);
            buffer_ = 0;
            bits_left_ = 8;
        }
    }

    // Stores a 0 in 'buffer_' and increases the count of bits by 1.
    FORCE_INLINE void write_bit_zero(common::ByteStream &out) {
        bits_left_--;
        flush_byte_if_full(out);
    }

    // Stores a 1 in 'buffer_' and increases the count of bits by 1.
    FORCE_INLINE void write_bit_one(common::ByteStream &out) {
        buffer_ |= (1 << (bits_left_ - 1));
        bits_left_--;
        flush_byte_if_full(out);
    }

    /*
     * Writes the given long value using the defined amount of least significant
     * bits.
     *
     * value: The long value to be written
     * bits: How many bits are stored to the stream
     */
    void write_bits(int64_t value, int bits, common::ByteStream &out) {
        while (bits > 0) {
            int shift = bits - bits_left_;
            if (shift >= 0) {
                buffer_ |=
                    (uint8_t)((value >> shift) & ((1 << bits_left_) - 1));
                bits -= bits_left_;
                bits_left_ = 0;
            } else {
                shift = bits_left_ - bits;
                buffer_ |= (uint8_t)(value << shift);
                bits_left_ -= bits;
                bits = 0;
            }
            flush_byte_if_full(out);
        }
    }

    int get_one_item_max_size();
    void write_first(T value, common::ByteStream &out);
    void write_existing_leading(T xor_value, common::ByteStream &out);
    void write_new_leading(T xor_value, int leading_zeros, int trailing_zeros,
                           common::ByteStream &out);
    void compress_value(T value, common::ByteStream &out);
    int do_encode(T value, common::ByteStream &out);
    int flush(common::ByteStream &out);

    int encode(bool value, common::ByteStream &out_stream);
    int encode(int32_t value, common::ByteStream &out_stream);
    int encode(int64_t value, common::ByteStream &out_stream);
    int encode(float value, common::ByteStream &out_stream);
    int encode(double value, common::ByteStream &out_stream);

   public:
    common::TSEncoding type_;  // for debug
    T stored_value_;
    int stored_leading_zeros_;
    int stored_trailing_zeros_;
    int bits_left_;
    bool first_value_was_written_;
    uint8_t buffer_;
};

template <>
FORCE_INLINE int GorillaEncoder<int32_t>::get_one_item_max_size() {
    return INT32_ONE_ITEM_MAX_SIZE;
}
template <>
FORCE_INLINE int GorillaEncoder<int64_t>::get_one_item_max_size() {
    return INT64_ONE_ITEM_MAX_SIZE;
}

template <>
FORCE_INLINE void GorillaEncoder<int32_t>::write_first(
    int32_t value, common::ByteStream &out) {
    stored_value_ = value;
    write_bits(value, VALUE_BITS_LENGTH_32BIT, out);
}

template <>
FORCE_INLINE void GorillaEncoder<int64_t>::write_first(
    int64_t value, common::ByteStream &out) {
    stored_value_ = value;
    write_bits(value, VALUE_BITS_LENGTH_64BIT, out);
}

/*
 * If there at least as many leading zeros and as many trailing
 * zeros as previous value, control bit = 0.
 * just store the meaningful XORed value
 */
template <>
FORCE_INLINE void GorillaEncoder<int32_t>::write_existing_leading(
    int32_t xor_value, common::ByteStream &out) {
    write_bit_zero(out);
    int significant_bits = VALUE_BITS_LENGTH_32BIT - stored_leading_zeros_ -
                           stored_trailing_zeros_;
    write_bits(((uint32_t)xor_value) >> stored_trailing_zeros_,
               significant_bits, out);
}

template <>
FORCE_INLINE void GorillaEncoder<int64_t>::write_existing_leading(
    int64_t xor_value, common::ByteStream &out) {
    write_bit_zero(out);
    int significant_bits = VALUE_BITS_LENGTH_64BIT - stored_leading_zeros_ -
                           stored_trailing_zeros_;
    write_bits(((uint64_t)xor_value) >> stored_trailing_zeros_,
               significant_bits, out);
}

/*
 * Stores the length of the number of leading zeros in the next 5 bits
 * Stores the length of the meaningful XORed value in the next 5 bits
 * Stores the meaningful bits of the XORed value
 */
template <>
FORCE_INLINE void GorillaEncoder<int32_t>::write_new_leading(
    int32_t xor_value, int leading_zeros, int trailing_zeros,
    common::ByteStream &out) {
    write_bit_one(out);

    int significant_bits =
        VALUE_BITS_LENGTH_32BIT - leading_zeros - trailing_zeros;
    write_bits(leading_zeros, LEADING_ZERO_BITS_LENGTH_32BIT, out);
    write_bits((long)significant_bits - 1, MEANINGFUL_XOR_BITS_LENGTH_32BIT,
               out);
    write_bits(((uint32_t)xor_value) >> trailing_zeros, significant_bits, out);

    stored_leading_zeros_ = leading_zeros;
    stored_trailing_zeros_ = trailing_zeros;
}

template <>
FORCE_INLINE void GorillaEncoder<int64_t>::write_new_leading(
    int64_t xor_value, int leading_zeros, int trailing_zeros,
    common::ByteStream &out) {
    write_bit_one(out);

    int significant_bits =
        VALUE_BITS_LENGTH_64BIT - leading_zeros - trailing_zeros;
    write_bits(leading_zeros, LEADING_ZERO_BITS_LENGTH_64BIT, out);
    write_bits((long)significant_bits - 1, MEANINGFUL_XOR_BITS_LENGTH_64BIT,
               out);
    write_bits(((uint64_t)xor_value) >> trailing_zeros, significant_bits, out);

    stored_leading_zeros_ = leading_zeros;
    stored_trailing_zeros_ = trailing_zeros;
}

template <typename T>
FORCE_INLINE void GorillaEncoder<T>::compress_value(T value,
                                                    common::ByteStream &out) {
    T xor_value = stored_value_ ^ value;
    stored_value_ = value;
    if (xor_value == 0) {
        write_bit_zero(out);
    } else {
        write_bit_one(out);
        int leading_zeros = number_of_leading_zeros(xor_value);
        int trailing_zeros = number_of_trailing_zeros(xor_value);
        if (leading_zeros >= stored_leading_zeros_ &&
            trailing_zeros >= stored_trailing_zeros_) {
            write_existing_leading(xor_value, out);
        } else {
            write_new_leading(xor_value, leading_zeros, trailing_zeros, out);
        }
    }
}

template <typename T>
FORCE_INLINE int GorillaEncoder<T>::do_encode(T value,
                                              common::ByteStream &out) {
    if (LIKELY(first_value_was_written_)) {
        compress_value(value, out);
    } else {
        write_first(value, out);
        first_value_was_written_ = true;
    }
    return common::E_OK;
}

template <>
FORCE_INLINE int GorillaEncoder<int32_t>::flush(common::ByteStream &out) {
    // ending stream
    do_encode(GORILLA_ENCODING_ENDING_INTEGER, out);

    // flip the byte no matter it is empty or not
    // the empty ending byte is necessary when decoding
    bits_left_ = 0;
    flush_byte_if_full(out);

    // the encoder may be reused, so let us reset it
    reset();
    return common::E_OK;
}

template <>
FORCE_INLINE int GorillaEncoder<int64_t>::flush(common::ByteStream &out) {
    // ending stream
    do_encode(GORILLA_ENCODING_ENDING_LONG, out);

    // flip the byte no matter it is empty or not
    // the empty ending byte is necessary when decoding
    bits_left_ = 0;
    flush_byte_if_full(out);

    // the encoder may be reused, so let us reset it
    reset();
    return common::E_OK;
}

class FloatGorillaEncoder : public GorillaEncoder<int32_t> {
   public:
    int do_encode(float value, common::ByteStream &out) {
        int32_t value_int = common::float_to_int(value);
        return GorillaEncoder<int32_t>::do_encode(value_int, out);
    }

    int flush(common::ByteStream &out) {
        // ending stream
        float f = GORILLA_ENCODING_ENDING_FLOAT;
        do_encode(f, out);

        // flip the byte no matter it is empty or not
        // the empty ending byte is necessary when decoding
        bits_left_ = 0;
        flush_byte_if_full(out);

        // the encoder may be reused, so let us reset it
        reset();
        return common::E_OK;
    }

    int encode(bool value, common::ByteStream &out_stream);
    int encode(int32_t value, common::ByteStream &out_stream);
    int encode(int64_t value, common::ByteStream &out_stream);
    int encode(float value, common::ByteStream &out_stream);
    int encode(double value, common::ByteStream &out_stream);
};

class DoubleGorillaEncoder : public GorillaEncoder<int64_t> {
   public:
    int do_encode(double value, common::ByteStream &out) {
        int64_t value_long = common::double_to_long(value);
        return GorillaEncoder<int64_t>::do_encode(value_long, out);
    }

    int flush(common::ByteStream &out) {
        // ending stream
        double d = GORILLA_ENCODING_ENDING_DOUBLE;
        do_encode(d, out);

        // flip the byte no matter it is empty or not
        // the empty ending byte is necessary when decoding
        bits_left_ = 0;
        flush_byte_if_full(out);

        // the encoder may be reused, so let us reset it
        reset();
        return common::E_OK;
    }

    int encode(bool value, common::ByteStream &out_stream);
    int encode(int32_t value, common::ByteStream &out_stream);
    int encode(int64_t value, common::ByteStream &out_stream);
    int encode(float value, common::ByteStream &out_stream);
    int encode(double value, common::ByteStream &out_stream);
};

typedef GorillaEncoder<int32_t> IntGorillaEncoder;
typedef GorillaEncoder<int64_t> LongGorillaEncoder;

template <>
FORCE_INLINE int IntGorillaEncoder::encode(bool value,
                                           common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int IntGorillaEncoder::encode(int32_t value,
                                           common::ByteStream &out_stream) {
    return do_encode(value, out_stream);
}
template <>
FORCE_INLINE int IntGorillaEncoder::encode(int64_t value,
                                           common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int IntGorillaEncoder::encode(float value,
                                           common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int IntGorillaEncoder::encode(double value,
                                           common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}

template <>
FORCE_INLINE int LongGorillaEncoder::encode(bool value,
                                            common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int LongGorillaEncoder::encode(int32_t value,
                                            common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int LongGorillaEncoder::encode(int64_t value,
                                            common::ByteStream &out_stream) {
    return do_encode(value, out_stream);
}
template <>
FORCE_INLINE int LongGorillaEncoder::encode(float value,
                                            common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int LongGorillaEncoder::encode(double value,
                                            common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}

FORCE_INLINE int FloatGorillaEncoder::encode(bool value,
                                             common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int FloatGorillaEncoder::encode(int32_t value,
                                             common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int FloatGorillaEncoder::encode(int64_t value,
                                             common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int FloatGorillaEncoder::encode(float value,
                                             common::ByteStream &out_stream) {
    return do_encode(value, out_stream);
}
FORCE_INLINE int FloatGorillaEncoder::encode(double value,
                                             common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}

FORCE_INLINE int DoubleGorillaEncoder::encode(bool value,
                                              common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int DoubleGorillaEncoder::encode(int32_t value,
                                              common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int DoubleGorillaEncoder::encode(int64_t value,
                                              common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int DoubleGorillaEncoder::encode(float value,
                                              common::ByteStream &out_stream) {
    ASSERT(false);
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int DoubleGorillaEncoder::encode(double value,
                                              common::ByteStream &out_stream) {
    return do_encode(value, out_stream);
}

}  // end namespace storage
#endif  // ENCODING_GORILLA_ENCODER_H
