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

#include <cmath>
#include <cstddef>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "utils/util_define.h"

namespace storage {

namespace ts2diff_java_detail {

// Java float/double TS_2DIFF overflow page markers.
constexpr uint32_t FLAG_ORIGINAL_VALUE_OVERFLOW =
    2147483646u;  // Integer.MAX_VALUE - 1
constexpr uint32_t FLAG_SCALED_VALUE_OVERFLOW =
    2147483647u;  // Integer.MAX_VALUE

inline bool bitmap_marked(const std::vector<uint8_t>& bm, int idx) {
    if (bm.empty()) {
        return false;
    }
    size_t byte_idx = static_cast<size_t>(idx / 8);
    if (byte_idx >= bm.size()) {
        return false;
    }
    return (bm[byte_idx] & static_cast<uint8_t>(1u << (idx % 8))) != 0;
}

inline bool looks_like_ts2diff_header(common::ByteStream& in) {
    int ret = common::E_OK;
    uint32_t probe_mark = in.read_pos();
    int32_t write_index = 0;
    int32_t bit_width = 0;
    if (RET_FAIL(common::SerializationUtil::read_i32(write_index, in)) ||
        RET_FAIL(common::SerializationUtil::read_i32(bit_width, in))) {
        in.set_read_pos(probe_mark);
        return false;
    }
    in.set_read_pos(probe_mark);
    if (write_index < 0 || write_index > 128) {
        return false;
    }
    if (bit_width < 0 || bit_width > 64) {
        return false;
    }
    return true;
}

inline int consume_float_double_ts2diff_prefix(
    common::ByteStream& in, bool& is_legacy_raw, int& max_point_number,
    std::vector<uint8_t>& underflow_bm, std::vector<uint8_t>& overflow_bm,
    int& segment_size) {
    int ret = common::E_OK;
    is_legacy_raw = false;
    max_point_number = 0;
    underflow_bm.clear();
    overflow_bm.clear();
    segment_size = 0;
    uint32_t mark = in.read_pos();
    uint32_t tag = 0;
    if (RET_FAIL(common::SerializationUtil::read_var_uint(tag, in))) {
        return ret;
    }
    if (tag == FLAG_ORIGINAL_VALUE_OVERFLOW ||
        tag == FLAG_SCALED_VALUE_OVERFLOW) {
        uint32_t n = 0;
        if (RET_FAIL(common::SerializationUtil::read_var_uint(n, in))) {
            return ret;
        }
        segment_size = static_cast<int>(n);
        int bm_len = segment_size / 8 + 1;
        underflow_bm.resize(static_cast<size_t>(bm_len), 0);
        uint32_t read_len = 0;
        if (RET_FAIL(in.read_buf(underflow_bm.data(),
                                 static_cast<uint32_t>(bm_len), read_len)) ||
            read_len != static_cast<uint32_t>(bm_len)) {
            return ret;
        }
        if (tag == FLAG_ORIGINAL_VALUE_OVERFLOW) {
            overflow_bm.resize(static_cast<size_t>(bm_len), 0);
            if (RET_FAIL(in.read_buf(overflow_bm.data(),
                                     static_cast<uint32_t>(bm_len),
                                     read_len)) ||
                read_len != static_cast<uint32_t>(bm_len)) {
                return ret;
            }
        }
        uint32_t mpn = 0;
        if (RET_FAIL(common::SerializationUtil::read_var_uint(mpn, in))) {
            return ret;
        }
        max_point_number = static_cast<int>(mpn);
        return common::E_OK;
    }

    // Distinguish Java maxPointNumber prefix from legacy raw C++ block.
    max_point_number = static_cast<int>(tag);
    if (!looks_like_ts2diff_header(in)) {
        in.set_read_pos(mark);
        is_legacy_raw = true;
    } else {
        segment_size = 0;
    }
    return common::E_OK;
}

}  // namespace ts2diff_java_detail

template <typename T>
class TS2DIFFDecoder : public Decoder {
   public:
    TS2DIFFDecoder() { reset(); }
    ~TS2DIFFDecoder() override {}

    void reset() override {
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

    FORCE_INLINE bool has_remaining(const common::ByteStream& buffer) override {
        if (buffer.has_remaining()) return true;
        return bits_left_ != 0 || (current_index_ <= write_index_ &&
                                   write_index_ != -1 && current_index_ != 0);
    }

    void read_header(common::ByteStream& in) {
        common::SerializationUtil::read_i32(write_index_, in);
        common::SerializationUtil::read_i32(bit_width_, in);
    }

    // If empty, cache 8 bits from in_stream to 'buffer_'.
    void read_byte_if_empty(common::ByteStream& in) {
        if (bits_left_ == 0) {
            uint32_t read_len = 0;
            in.read_buf(&buffer_, 1, read_len);
            if (read_len != 0) {
                bits_left_ = 8;
            }
        }
    }

    int64_t read_long(int bits, common::ByteStream& in) {
        int64_t value = 0;
        while (bits > 0) {
            read_byte_if_empty(in);
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
        }
        return value;
    }

    T decode(common::ByteStream& in);
    int read_boolean(bool& ret_value, common::ByteStream& in) override;
    int read_int32(int32_t& ret_value, common::ByteStream& in) override;
    int read_int64(int64_t& ret_value, common::ByteStream& in) override;
    int read_float(float& ret_value, common::ByteStream& in) override;
    int read_double(double& ret_value, common::ByteStream& in) override;
    int read_String(common::String& ret_value, common::PageArena& pa,
                    common::ByteStream& in) override;

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
inline int32_t TS2DIFFDecoder<int32_t>::decode(common::ByteStream& in) {
    int32_t ret_value = stored_value_;
    if (UNLIKELY(current_index_ == 0)) {
        read_header(in);
        common::SerializationUtil::read_i32(delta_min_, in);
        common::SerializationUtil::read_i32(first_value_, in);
        ret_value = first_value_;
        bits_left_ = 0;
        buffer_ = 0;
        if (write_index_ == 0) {
            current_index_ = 0;
        } else {
            current_index_ = 1;
        }
        return ret_value;
    }
    // although it seems we are reading an int64, bit_width_ guarantees
    // that it does not overflow int32
    stored_value_ = read_long(bit_width_, in);
    ret_value = stored_value_ + first_value_ + delta_min_;
    if (current_index_++ >= write_index_) {
        current_index_ = 0;
        bits_left_ = 0;
    }
    first_value_ = ret_value;
    return ret_value;
}

template <>
inline int64_t TS2DIFFDecoder<int64_t>::decode(common::ByteStream& in) {
    int64_t ret_value = stored_value_;
    if (UNLIKELY(current_index_ == 0)) {
        read_header(in);
        common::SerializationUtil::read_i64(delta_min_, in);
        common::SerializationUtil::read_i64(first_value_, in);
        ret_value = first_value_;
        if (write_index_ == 0) {
            current_index_ = 0;
        } else {
            current_index_ = 1;
        }
        return ret_value;
    }
    stored_value_ = (int64_t)read_long(bit_width_, in);
    ret_value = stored_value_ + first_value_ + delta_min_;
    first_value_ = ret_value;
    if (current_index_++ >= write_index_) {
        current_index_ = 0;
        bits_left_ = 0;
    }
    return ret_value;
}

class FloatTS2DIFFDecoder : public TS2DIFFDecoder<int32_t> {
   public:
    FloatTS2DIFFDecoder() = default;
    float decode(common::ByteStream& in) {
        int32_t value_int = TS2DIFFDecoder<int32_t>::decode(in);
        return common::int_to_float(value_int);
    }

    int read_boolean(bool& ret_value, common::ByteStream& in);
    int read_int32(int32_t& ret_value, common::ByteStream& in);
    int read_int64(int64_t& ret_value, common::ByteStream& in);
    int read_float(float& ret_value, common::ByteStream& in);
    int read_double(double& ret_value, common::ByteStream& in);

   private:
    bool is_legacy_raw_{false};
    int max_point_number_{0};
    double max_point_value_{1.0};
    int segment_pos_{0};
    int segment_size_{0};
    std::vector<uint8_t> underflow_bm_;
    std::vector<uint8_t> overflow_bm_;
};

class DoubleTS2DIFFDecoder : public TS2DIFFDecoder<int64_t> {
   public:
    DoubleTS2DIFFDecoder() = default;
    double decode(common::ByteStream& in) {
        int64_t value_long = TS2DIFFDecoder<int64_t>::decode(in);
        return common::long_to_double(value_long);
    }

    int read_boolean(bool& ret_value, common::ByteStream& in);
    int read_int32(int32_t& ret_value, common::ByteStream& in);
    int read_int64(int64_t& ret_value, common::ByteStream& in);
    int read_float(float& ret_value, common::ByteStream& in);
    int read_double(double& ret_value, common::ByteStream& in);

   private:
    bool is_legacy_raw_{false};
    int max_point_number_{0};
    double max_point_value_{1.0};
    int segment_pos_{0};
    int segment_size_{0};
    std::vector<uint8_t> underflow_bm_;
    std::vector<uint8_t> overflow_bm_;
};

typedef TS2DIFFDecoder<int32_t> IntTS2DIFFDecoder;
typedef TS2DIFFDecoder<int64_t> LongTS2DIFFDecoder;

// wrap as Decoder interface
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_boolean(bool& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_int32(int32_t& ret_value,
                                               common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_int64(int64_t& ret_value,
                                               common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_float(float& ret_value,
                                               common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_double(double& ret_value,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntTS2DIFFDecoder::read_String(common::String& ret_value,
                                                common::PageArena& pa,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_boolean(bool& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_int32(int32_t& ret_value,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_int64(int64_t& ret_value,
                                                common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_float(float& ret_value,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_double(double& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongTS2DIFFDecoder::read_String(common::String& ret_value,
                                                 common::PageArena& pa,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_boolean(bool& ret_value,
                                                   common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_int32(int32_t& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_int64(int64_t& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_float(float& ret_value,
                                                 common::ByteStream& in) {
    int ret = common::E_OK;
    if (current_index_ == 0) {
        if (RET_FAIL(ts2diff_java_detail::consume_float_double_ts2diff_prefix(
                in, is_legacy_raw_, max_point_number_, underflow_bm_,
                overflow_bm_, segment_size_))) {
            return ret;
        }
        max_point_value_ =
            max_point_number_ <= 0
                ? 1.0
                : std::pow(10.0, static_cast<double>(max_point_number_));
        segment_pos_ = 0;
    }
    if (is_legacy_raw_) {
        ret_value = decode(in);
        return common::E_OK;
    }
    int32_t value_int = TS2DIFFDecoder<int32_t>::decode(in);
    if (!overflow_bm_.empty() &&
        ts2diff_java_detail::bitmap_marked(overflow_bm_, segment_pos_)) {
        ret_value = common::int_to_float(value_int);
    } else {
        bool use_scaled = true;
        if (!underflow_bm_.empty()) {
            use_scaled =
                ts2diff_java_detail::bitmap_marked(underflow_bm_, segment_pos_);
        }
        const double divisor = use_scaled ? max_point_value_ : 1.0;
        ret_value =
            static_cast<float>(static_cast<double>(value_int) / divisor);
    }
    segment_pos_++;
    return common::E_OK;
}
FORCE_INLINE int FloatTS2DIFFDecoder::read_double(double& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_boolean(bool& ret_value,
                                                    common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_int32(int32_t& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_int64(int64_t& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_float(float& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleTS2DIFFDecoder::read_double(double& ret_value,
                                                   common::ByteStream& in) {
    int ret = common::E_OK;
    if (current_index_ == 0) {
        if (RET_FAIL(ts2diff_java_detail::consume_float_double_ts2diff_prefix(
                in, is_legacy_raw_, max_point_number_, underflow_bm_,
                overflow_bm_, segment_size_))) {
            return ret;
        }
        max_point_value_ =
            max_point_number_ <= 0
                ? 1.0
                : std::pow(10.0, static_cast<double>(max_point_number_));
        segment_pos_ = 0;
    }
    if (is_legacy_raw_) {
        ret_value = decode(in);
        return common::E_OK;
    }
    int64_t value_long = TS2DIFFDecoder<int64_t>::decode(in);
    if (!overflow_bm_.empty() &&
        ts2diff_java_detail::bitmap_marked(overflow_bm_, segment_pos_)) {
        ret_value = common::long_to_double(value_long);
    } else {
        bool use_scaled = true;
        if (!underflow_bm_.empty()) {
            use_scaled =
                ts2diff_java_detail::bitmap_marked(underflow_bm_, segment_pos_);
        }
        const double divisor = use_scaled ? max_point_value_ : 1.0;
        ret_value = static_cast<double>(value_long) / divisor;
    }
    segment_pos_++;
    return common::E_OK;
}

}  // end namespace storage
#endif  // ENCODING_TS2DIFF_DECODER_H
