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

#ifndef ENCODING_TS2DIFF_ENCODER_H
#define ENCODING_TS2DIFF_ENCODER_H

#include <sys/types.h>

#include <cmath>
#include <limits>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "encoder.h"
#if defined(__SSE4_2__)
#include <smmintrin.h>
#define USE_SSE 1
#elif defined(__AVX2__)
#include <immintrin.h>
#define USE_AVX2 1
#endif

namespace storage {

template <typename T>
struct SIMDOps;

template <>
struct SIMDOps<int32_t> {
#ifdef USE_SSE
    static void rebase(int32_t* arr, int32_t min_val, size_t size) {
        const __m128i min_vec = _mm_set1_epi32(min_val);
        size_t i = 0;
        for (; i + 3 < size; i += 4) {
            __m128i vec =
                _mm_loadu_si128(reinterpret_cast<const __m128i*>(arr + i));
            vec = _mm_sub_epi32(vec, min_vec);
            _mm_storeu_si128(reinterpret_cast<__m128i*>(arr + i), vec);
        }
        for (; i < size; ++i) {
            arr[i] -= min_val;
        }
    }
#else
    static void rebase(int32_t* arr, int32_t min_val, size_t size) {
        for (size_t i = 0; i < size; ++i) {
            arr[i] -= min_val;
        }
    }
#endif
};

template <>
struct SIMDOps<int64_t> {
#ifdef USE_AVX2
    static void rebase(int64_t* arr, int64_t min_val, size_t size) {
        const __m256i min_vec = _mm256_set1_epi64x(min_val);
        size_t i = 0;
        for (; i + 3 < size; i += 4) {
            __m256i vec =
                _mm256_loadu_si256(reinterpret_cast<const __m256i*>(arr + i));
            vec = _mm256_sub_epi64(vec, min_vec);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(arr + i), vec);
        }
        for (; i < size; ++i) {
            arr[i] -= min_val;
        }
    }
#else
    static void rebase(int64_t* arr, int64_t min_val, size_t size) {
        for (size_t i = 0; i < size; ++i) {
            arr[i] -= min_val;
        }
    }
#endif
};

template <typename T>
class TS2DIFFEncoder : public Encoder {
   public:
    TS2DIFFEncoder() { init(); }

    ~TS2DIFFEncoder() { destroy(); }

    void reset() { write_index_ = -1; }

    void init() {
        block_size_ = 128;
        // block_size_ = 16;
        delta_arr_ = (T*)common::mem_alloc(sizeof(T) * block_size_,
                                           common::MOD_TS2DIFF_OBJ);
        write_index_ = -1;
        bits_left_ = 8;
        buffer_ = 0;
        delta_arr_min_ = 0;
        delta_arr_max_ = 0;
        first_value_ = 0;
        previous_value_ = 0;
    }

    void destroy() {
        if (delta_arr_ != nullptr) {
            common::mem_free(delta_arr_);
            delta_arr_ = nullptr;
        }
    }

    void write_bits(int64_t value, int bits, common::ByteStream& out_stream) {
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
            flush_byte_if_full(out_stream);
        }
    }

    void flush_remaining(common::ByteStream& out_stream) {
        // FIXME bits_left_ != 0 does not means something to be flushed. (=8)
        if (bits_left_ != 0 && bits_left_ != 8) {
            bits_left_ = 0;
            flush_byte_if_full(out_stream);
        }
    }

    void flush_byte_if_full(common::ByteStream& out_stream) {
        if (bits_left_ == 0) {
            out_stream.write_buf(&buffer_, 1);
            buffer_ = 0;
            bits_left_ = 8;
        }
    }

    void rebase_arr(int i) { delta_arr_[i] = delta_arr_[i] - delta_arr_min_; }

    int cal_bit_width(T n) {
        int bit_width = 0;
        while (n > 0) {
            bit_width++;
            n >>= 1;
        }
        return bit_width;
    }

    int do_encode(T value, common::ByteStream& out_stream);
    int encode(bool value, common::ByteStream& out_stream);
    int encode(int32_t value, common::ByteStream& out_stream);
    int encode(int64_t value, common::ByteStream& out_stream);
    int encode(float value, common::ByteStream& out_stream);
    int encode(double value, common::ByteStream& out_stream);
    int encode(common::String value, common::ByteStream& out_stream);

    int flush(common::ByteStream& out_stream);

    int get_max_byte_size() {
        // The meaning of 24 is: index(4)+width(4)+minDeltaBase(8)+firstValue(8)
        return 24 + write_index_ * 8;
    }

   public:
    int block_size_;
    T* delta_arr_;
    T first_value_;
    T previous_value_;
    T delta_arr_min_;
    T delta_arr_max_;
    uint8_t buffer_;
    int bits_left_;
    int write_index_;
};

template <typename T>
int TS2DIFFEncoder<T>::do_encode(T value, common::ByteStream& out_stream) {
    if (write_index_ == -1) {
        first_value_ = value;
        previous_value_ = first_value_;
        write_index_++;
        return common::E_OK;
    }
    // Calculate the delta between the current value and the previous_value_
    T delta = value - previous_value_;
    previous_value_ = value;
    if (write_index_ == 0) {
        delta_arr_max_ = delta;
        delta_arr_min_ = delta;
    }
    if (delta > delta_arr_max_) {
        delta_arr_max_ = delta;
    }
    if (delta < delta_arr_min_) {
        delta_arr_min_ = delta;
    }

    delta_arr_[write_index_] = delta;
    write_index_++;

    if (write_index_ >= block_size_) {
        return flush(out_stream);
    }
    return common::E_OK;
}

template <>
inline int TS2DIFFEncoder<int32_t>::flush(common::ByteStream& out_stream) {
    int ret = common::E_OK;
    if (write_index_ == -1) {
        return common::E_OK;
    }
    // Subtract the minimum value for each delta_arr_ item
    SIMDOps<int32_t>::rebase(delta_arr_, delta_arr_min_, write_index_);
    // Calculate the bit length of each value to writer
    int bit_width = cal_bit_width(delta_arr_max_ - delta_arr_min_);
    // writer header
    common::SerializationUtil::write_ui32(write_index_, out_stream);
    common::SerializationUtil::write_ui32(bit_width, out_stream);
    common::SerializationUtil::write_ui32(delta_arr_min_, out_stream);
    common::SerializationUtil::write_ui32(first_value_, out_stream);
    // writer data
    for (int i = 0; i < write_index_; i++) {
        write_bits(delta_arr_[i], bit_width, out_stream);
    }
    flush_remaining(out_stream);
    reset();
    return ret;
}

template <>
inline int TS2DIFFEncoder<int64_t>::flush(common::ByteStream& out_stream) {
    int ret = common::E_OK;
    if (write_index_ == -1) {
        return common::E_OK;
    }
    // Subtract the minimum value for each delta_arr_ item
    SIMDOps<int64_t>::rebase(delta_arr_, delta_arr_min_, write_index_);
    // Calculate the bit length of each value to writer
    int bit_width = cal_bit_width(delta_arr_max_ - delta_arr_min_);
    // writer header
    common::SerializationUtil::write_i32(write_index_, out_stream);
    common::SerializationUtil::write_i32(bit_width, out_stream);
    common::SerializationUtil::write_i64(delta_arr_min_, out_stream);
    common::SerializationUtil::write_i64(first_value_, out_stream);
    // writer data
    for (int i = 0; i < write_index_; i++) {
        write_bits(delta_arr_[i], bit_width, out_stream);
    }
    flush_remaining(out_stream);
    reset();  // 语义，writeIndex=-1;
    return ret;
}

class FloatTS2DIFFEncoder : public TS2DIFFEncoder<int32_t> {
   public:
    FloatTS2DIFFEncoder() : max_point_number_(2), max_point_value_(100.0) {}
    int do_encode(float value, common::ByteStream& out_stream) {
        int32_t value_int = convert_float_to_int(value);
        return TS2DIFFEncoder<int32_t>::do_encode(value_int, out_stream);
    }
    int flush(common::ByteStream& out_stream) override;
    int encode(bool value, common::ByteStream& out_stream);
    int encode(int32_t value, common::ByteStream& out_stream);
    int encode(int64_t value, common::ByteStream& out_stream);
    int encode(float value, common::ByteStream& out_stream);
    int encode(double value, common::ByteStream& out_stream);

   private:
    int32_t convert_float_to_int(float value) {
        const double scaled = static_cast<double>(value) * max_point_value_;
        if (scaled > static_cast<double>(std::numeric_limits<int32_t>::max()) ||
            scaled < static_cast<double>(std::numeric_limits<int32_t>::min())) {
            if (std::isnan(value) ||
                value >
                    static_cast<float>(std::numeric_limits<int32_t>::max()) ||
                value <
                    static_cast<float>(std::numeric_limits<int32_t>::min())) {
                underflow_flags_.push_back(-1);
                return common::float_to_int(value);
            }
            underflow_flags_.push_back(0);
            return static_cast<int32_t>(std::lround(value));
        }
        if (std::isnan(value)) {
            underflow_flags_.push_back(-1);
            return common::float_to_int(value);
        }
        underflow_flags_.push_back(1);
        return static_cast<int32_t>(std::lround(scaled));
    }
    bool has_overflow() const {
        for (int8_t f : underflow_flags_) {
            if (f != 1) {
                return true;
            }
        }
        return false;
    }

   private:
    int max_point_number_;
    double max_point_value_;
    std::vector<int8_t> underflow_flags_;
};

class DoubleTS2DIFFEncoder : public TS2DIFFEncoder<int64_t> {
   public:
    DoubleTS2DIFFEncoder() : max_point_number_(2), max_point_value_(100.0) {}
    int do_encode(double value, common::ByteStream& out_stream) {
        int64_t value_long = convert_double_to_long(value);
        return TS2DIFFEncoder<int64_t>::do_encode(value_long, out_stream);
    }
    int flush(common::ByteStream& out_stream) override;
    int encode(bool value, common::ByteStream& out_stream);
    int encode(int32_t value, common::ByteStream& out_stream);
    int encode(int64_t value, common::ByteStream& out_stream);
    int encode(float value, common::ByteStream& out_stream);
    int encode(double value, common::ByteStream& out_stream);

   private:
    int64_t convert_double_to_long(double value) {
        const double scaled = value * max_point_value_;
        if (scaled > static_cast<double>(std::numeric_limits<int64_t>::max()) ||
            scaled < static_cast<double>(std::numeric_limits<int64_t>::min())) {
            if (std::isnan(value) ||
                value >
                    static_cast<double>(std::numeric_limits<int64_t>::max()) ||
                value <
                    static_cast<double>(std::numeric_limits<int64_t>::min())) {
                underflow_flags_.push_back(-1);
                return common::double_to_long(value);
            }
            underflow_flags_.push_back(0);
            return static_cast<int64_t>(std::llround(value));
        }
        if (std::isnan(value)) {
            underflow_flags_.push_back(-1);
            return common::double_to_long(value);
        }
        underflow_flags_.push_back(1);
        return static_cast<int64_t>(std::llround(scaled));
    }
    bool has_overflow() const {
        for (int8_t f : underflow_flags_) {
            if (f != 1) {
                return true;
            }
        }
        return false;
    }

   private:
    int max_point_number_;
    double max_point_value_;
    std::vector<int8_t> underflow_flags_;
};

typedef TS2DIFFEncoder<int32_t> IntTS2DIFFEncoder;
typedef TS2DIFFEncoder<int64_t> LongTS2DIFFEncoder;

// wrap as Encoder
template <>
FORCE_INLINE int IntTS2DIFFEncoder::encode(bool value,
                                           common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int IntTS2DIFFEncoder::encode(int32_t value,
                                           common::ByteStream& out) {
    return do_encode(value, out);
}
template <>
FORCE_INLINE int IntTS2DIFFEncoder::encode(int64_t value,
                                           common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int IntTS2DIFFEncoder::encode(float value,
                                           common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int IntTS2DIFFEncoder::encode(double value,
                                           common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int IntTS2DIFFEncoder::encode(common::String value,
                                           common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}

template <>
FORCE_INLINE int LongTS2DIFFEncoder::encode(bool value,
                                            common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int LongTS2DIFFEncoder::encode(int32_t value,
                                            common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int LongTS2DIFFEncoder::encode(int64_t value,
                                            common::ByteStream& out) {
    return do_encode(value, out);
}
template <>
FORCE_INLINE int LongTS2DIFFEncoder::encode(float value,
                                            common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int LongTS2DIFFEncoder::encode(double value,
                                            common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
template <>
FORCE_INLINE int LongTS2DIFFEncoder::encode(common::String value,
                                            common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int FloatTS2DIFFEncoder::encode(bool value,
                                             common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int FloatTS2DIFFEncoder::encode(int32_t value,
                                             common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int FloatTS2DIFFEncoder::encode(int64_t value,
                                             common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int FloatTS2DIFFEncoder::encode(float value,
                                             common::ByteStream& out) {
    return do_encode(value, out);
}
FORCE_INLINE int FloatTS2DIFFEncoder::encode(double value,
                                             common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}

FORCE_INLINE int DoubleTS2DIFFEncoder::encode(bool value,
                                              common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int DoubleTS2DIFFEncoder::encode(int32_t value,
                                              common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int DoubleTS2DIFFEncoder::encode(int64_t value,
                                              common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int DoubleTS2DIFFEncoder::encode(float value,
                                              common::ByteStream& out) {
    return common::E_TYPE_NOT_MATCH;
}
FORCE_INLINE int DoubleTS2DIFFEncoder::encode(double value,
                                              common::ByteStream& out) {
    return do_encode(value, out);
}

// Keep float/double TS_2DIFF page layout compatible with Java.
FORCE_INLINE int FloatTS2DIFFEncoder::flush(common::ByteStream& out_stream) {
    int ret = common::E_OK;
    if (write_index_ == -1) {
        return common::E_OK;
    }
    const int num_values = write_index_ + 1;
    common::ByteStream inner(1024, common::MOD_TS2DIFF_OBJ, false);
    if (RET_FAIL(common::SerializationUtil::write_var_uint(
            static_cast<uint32_t>(max_point_number_), inner))) {
        return ret;
    }
    SIMDOps<int32_t>::rebase(delta_arr_, delta_arr_min_, write_index_);
    int bit_width = cal_bit_width(delta_arr_max_ - delta_arr_min_);
    if (RET_FAIL(common::SerializationUtil::write_ui32(
            static_cast<uint32_t>(write_index_), inner))) {
        return ret;
    }
    if (RET_FAIL(common::SerializationUtil::write_ui32(
            static_cast<uint32_t>(bit_width), inner))) {
        return ret;
    }
    if (RET_FAIL(common::SerializationUtil::write_ui32(
            static_cast<uint32_t>(delta_arr_min_), inner))) {
        return ret;
    }
    if (RET_FAIL(common::SerializationUtil::write_ui32(
            static_cast<uint32_t>(first_value_), inner))) {
        return ret;
    }
    for (int i = 0; i < write_index_; i++) {
        write_bits(delta_arr_[i], bit_width, inner);
    }
    flush_remaining(inner);
    reset();

    const bool overflow = has_overflow();
    if (overflow) {
        std::vector<uint8_t> underflow_bitmap(
            static_cast<size_t>(num_values / 8 + 1), 0);
        std::vector<uint8_t> overflow_bitmap(
            static_cast<size_t>(num_values / 8 + 1), 0);
        bool has_original_value_overflow = false;
        for (int i = 0; i < num_values; i++) {
            int8_t f = underflow_flags_[static_cast<size_t>(i)];
            if (f == 1) {
                underflow_bitmap[static_cast<size_t>(i / 8)] |=
                    static_cast<uint8_t>(1u << (i % 8));
            } else if (f == -1) {
                has_original_value_overflow = true;
                overflow_bitmap[static_cast<size_t>(i / 8)] |=
                    static_cast<uint8_t>(1u << (i % 8));
            }
        }
        constexpr uint32_t FLAG_SCALED_VALUE_OVERFLOW =
            2147483647u;  // Integer.MAX_VALUE
        constexpr uint32_t FLAG_ORIGINAL_VALUE_OVERFLOW =
            2147483646u;  // Integer.MAX_VALUE - 1
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                has_original_value_overflow ? FLAG_ORIGINAL_VALUE_OVERFLOW
                                            : FLAG_SCALED_VALUE_OVERFLOW,
                out_stream))) {
            return ret;
        }
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                static_cast<uint32_t>(num_values), out_stream))) {
            return ret;
        }
        const uint32_t bm_len = static_cast<uint32_t>(num_values / 8 + 1);
        if (RET_FAIL(out_stream.write_buf(underflow_bitmap.data(), bm_len))) {
            return ret;
        }
        if (has_original_value_overflow &&
            RET_FAIL(out_stream.write_buf(overflow_bitmap.data(), bm_len))) {
            return ret;
        }
    }
    if (RET_FAIL(merge_byte_stream(out_stream, inner, true))) {
        return ret;
    }
    underflow_flags_.clear();
    return ret;
}

FORCE_INLINE int DoubleTS2DIFFEncoder::flush(common::ByteStream& out_stream) {
    int ret = common::E_OK;
    if (write_index_ == -1) {
        return common::E_OK;
    }
    const int num_values = write_index_ + 1;
    common::ByteStream inner(1024, common::MOD_TS2DIFF_OBJ, false);
    if (RET_FAIL(common::SerializationUtil::write_var_uint(
            static_cast<uint32_t>(max_point_number_), inner))) {
        return ret;
    }
    SIMDOps<int64_t>::rebase(delta_arr_, delta_arr_min_, write_index_);
    int bit_width = cal_bit_width(delta_arr_max_ - delta_arr_min_);
    if (RET_FAIL(common::SerializationUtil::write_i32(write_index_, inner))) {
        return ret;
    }
    if (RET_FAIL(common::SerializationUtil::write_i32(bit_width, inner))) {
        return ret;
    }
    if (RET_FAIL(common::SerializationUtil::write_i64(delta_arr_min_, inner))) {
        return ret;
    }
    if (RET_FAIL(common::SerializationUtil::write_i64(first_value_, inner))) {
        return ret;
    }
    for (int i = 0; i < write_index_; i++) {
        write_bits(delta_arr_[i], bit_width, inner);
    }
    flush_remaining(inner);
    reset();

    const bool overflow = has_overflow();
    if (overflow) {
        std::vector<uint8_t> underflow_bitmap(
            static_cast<size_t>(num_values / 8 + 1), 0);
        std::vector<uint8_t> overflow_bitmap(
            static_cast<size_t>(num_values / 8 + 1), 0);
        bool has_original_value_overflow = false;
        for (int i = 0; i < num_values; i++) {
            int8_t f = underflow_flags_[static_cast<size_t>(i)];
            if (f == 1) {
                underflow_bitmap[static_cast<size_t>(i / 8)] |=
                    static_cast<uint8_t>(1u << (i % 8));
            } else if (f == -1) {
                has_original_value_overflow = true;
                overflow_bitmap[static_cast<size_t>(i / 8)] |=
                    static_cast<uint8_t>(1u << (i % 8));
            }
        }
        constexpr uint32_t FLAG_SCALED_VALUE_OVERFLOW =
            2147483647u;  // Integer.MAX_VALUE
        constexpr uint32_t FLAG_ORIGINAL_VALUE_OVERFLOW =
            2147483646u;  // Integer.MAX_VALUE - 1
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                has_original_value_overflow ? FLAG_ORIGINAL_VALUE_OVERFLOW
                                            : FLAG_SCALED_VALUE_OVERFLOW,
                out_stream))) {
            return ret;
        }
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                static_cast<uint32_t>(num_values), out_stream))) {
            return ret;
        }
        const uint32_t bm_len = static_cast<uint32_t>(num_values / 8 + 1);
        if (RET_FAIL(out_stream.write_buf(underflow_bitmap.data(), bm_len))) {
            return ret;
        }
        if (has_original_value_overflow &&
            RET_FAIL(out_stream.write_buf(overflow_bitmap.data(), bm_len))) {
            return ret;
        }
    }
    if (RET_FAIL(merge_byte_stream(out_stream, inner, true))) {
        return ret;
    }
    underflow_flags_.clear();
    return ret;
}

}  // end namespace storage
#endif  // ENCODING_TS2DIFF_ENCODER_H
