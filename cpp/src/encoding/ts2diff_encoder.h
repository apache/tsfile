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
#include <type_traits>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "encoder.h"
#include "ts2diff_wire_format.h"

#ifdef ENABLE_SIMD
#include "simde/x86/avx2.h"
#endif

namespace storage {

template <typename T>
struct SIMDOps;

template <>
struct SIMDOps<int32_t> {
#ifdef ENABLE_SIMD
    static void rebase(int32_t* arr, int32_t min_val, size_t size) {
        const simde__m128i min_vec = simde_mm_set1_epi32(min_val);
        size_t i = 0;
        for (; i + 3 < size; i += 4) {
            simde__m128i vec = simde_mm_loadu_si128(
                reinterpret_cast<const simde__m128i*>(arr + i));
            vec = simde_mm_sub_epi32(vec, min_vec);
            simde_mm_storeu_si128(reinterpret_cast<simde__m128i*>(arr + i),
                                  vec);
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
#ifdef ENABLE_SIMD
    static void rebase(int64_t* arr, int64_t min_val, size_t size) {
        const simde__m256i min_vec = simde_mm256_set1_epi64x(min_val);
        size_t i = 0;
        for (; i + 3 < size; i += 4) {
            simde__m256i vec = simde_mm256_loadu_si256(
                reinterpret_cast<const simde__m256i*>(arr + i));
            vec = simde_mm256_sub_epi64(vec, min_vec);
            simde_mm256_storeu_si256(reinterpret_cast<simde__m256i*>(arr + i),
                                     vec);
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

    void reset() override { write_index_ = -1; }

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

    void destroy() override {
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

    // Java-compatible bit width: the maximum width over the *rebased*
    // deltas, not the width of (max - min).  When the raw deltas wrap
    // (e.g. two adjacent raw IEEE bit patterns whose difference
    // overflows the integer type), (max - min) itself wraps negative and
    // its width would be 0, silently discarding every delta in the
    // block.  Mirrors Java calculateBitWidthsForDeltaBlockBuffer, which
    // widens each rebased delta individually.
    int cal_bit_width_rebased() {
        typedef typename std::make_unsigned<T>::type UT;
        UT max_rebased = 0;
        for (int i = 0; i < write_index_; i++) {
            UT rebased = static_cast<UT>(delta_arr_[i]) -
                         static_cast<UT>(delta_arr_min_);
            if (rebased > max_rebased) {
                max_rebased = rebased;
            }
        }
        int bit_width = 0;
        while (max_rebased > 0) {
            bit_width++;
            max_rebased >>= 1;
        }
        return bit_width;
    }

    // Batch bit-pack `count` values (each `bit_width` bits, MSB-first within
    // byte) into a single contiguous buffer and write it to out_stream in one
    // call. Avoids the per-byte write_buf overhead of the scalar write_bits
    // loop.
    //
    // Result codes:
    //   E_OK  → written successfully.
    //   -1    → caller must fall back to write_bits + flush_remaining because
    //           bit_width exceeds the safe accumulator width.
    //   any other non-zero value → real write_buf error; the caller must
    //           propagate it instead of treating the flush as successful.
    template <typename U>
    static int pack_bits_msb(const U* values, int count, int bit_width,
                             common::ByteStream& out_stream) {
        if (count <= 0 || bit_width <= 0) return common::E_OK;
        if (bit_width > 56) return -1;  // fall back

        size_t total_bytes = ((size_t)count * (size_t)bit_width + 7) / 8;
        std::vector<uint8_t> buf(total_bytes, 0);

        uint64_t accum = 0;
        int bits_in_accum = 0;
        size_t pos = 0;
        const uint64_t mask = (1ULL << bit_width) - 1;

        for (int i = 0; i < count; i++) {
            uint64_t v = static_cast<uint64_t>(values[i]) & mask;
            accum = (accum << bit_width) | v;
            bits_in_accum += bit_width;
            while (bits_in_accum >= 8) {
                buf[pos++] = static_cast<uint8_t>(accum >> (bits_in_accum - 8));
                bits_in_accum -= 8;
            }
            if (bits_in_accum > 0) {
                accum &= ((1ULL << bits_in_accum) - 1);
            } else {
                accum = 0;
            }
        }
        if (bits_in_accum > 0) {
            buf[pos++] = static_cast<uint8_t>(accum << (8 - bits_in_accum));
        }
        // The write status must reach the caller: a dropped return code
        // here would let flush() report E_OK for a page whose delta block
        // never made it to disk.
        return out_stream.write_buf(buf.data(), pos);
    }

    int do_encode(T value, common::ByteStream& out_stream);
    int encode(bool value, common::ByteStream& out_stream) override;
    int encode(int32_t value, common::ByteStream& out_stream) override;
    int encode(int64_t value, common::ByteStream& out_stream) override;
    int encode(float value, common::ByteStream& out_stream) override;
    int encode(double value, common::ByteStream& out_stream) override;
    int encode(common::String value, common::ByteStream& out_stream) override;

    int encode_batch(const int32_t* values, uint32_t count,
                     common::ByteStream& out_stream) override;
    int encode_batch(const int64_t* values, uint32_t count,
                     common::ByteStream& out_stream) override;

    int flush(common::ByteStream& out_stream) override;

    int get_max_byte_size() override {
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
    // Bit width over the raw deltas rebased by min (computed before the
    // array itself is rebased, so cal_bit_width_rebased subtracts min
    // exactly once).
    int bit_width = cal_bit_width_rebased();
    // Subtract the minimum value for each delta_arr_ item
    SIMDOps<int32_t>::rebase(delta_arr_, delta_arr_min_, write_index_);
    // Header writes can fail too (back-pressure / OOM on the underlying
    // stream); a half-written header followed by reset() leaves the page
    // corrupted but the caller thinking the data was flushed.
    if (RET_FAIL(
            common::SerializationUtil::write_ui32(write_index_, out_stream))) {
        return ret;
    }
    if (RET_FAIL(
            common::SerializationUtil::write_ui32(bit_width, out_stream))) {
        return ret;
    }
    if (RET_FAIL(common::SerializationUtil::write_ui32(delta_arr_min_,
                                                       out_stream))) {
        return ret;
    }
    if (RET_FAIL(
            common::SerializationUtil::write_ui32(first_value_, out_stream))) {
        return ret;
    }
    // writer data — batched bit-pack + single write_buf for the common case;
    // fall back to per-bit path for the rare wide bit_width.
    const int pack_ret =
        pack_bits_msb(delta_arr_, write_index_, bit_width, out_stream);
    if (pack_ret == -1) {
        for (int i = 0; i < write_index_; i++) {
            write_bits(delta_arr_[i], bit_width, out_stream);
        }
        flush_remaining(out_stream);
    } else if (pack_ret != common::E_OK) {
        // Real write failure — don't clear encoder state so the higher
        // layer can detect the page is poisoned.
        return pack_ret;
    }
    // Base reset only (write_index_ = -1).  This is a virtual call so a
    // float wrapper encoder can keep its page-scoped state (overflow
    // flags, buffered blocks) alive across the per-block flush.
    TS2DIFFEncoder<int32_t>::reset();
    return ret;
}

template <>
inline int TS2DIFFEncoder<int64_t>::flush(common::ByteStream& out_stream) {
    int ret = common::E_OK;
    if (write_index_ == -1) {
        return common::E_OK;
    }
    // Bit width over the raw deltas rebased by min; see the int32
    // specialization for ordering rationale.
    int bit_width = cal_bit_width_rebased();
    // Subtract the minimum value for each delta_arr_ item
    SIMDOps<int64_t>::rebase(delta_arr_, delta_arr_min_, write_index_);
    // Header writes can fail too — see int32 specialization for rationale.
    if (RET_FAIL(
            common::SerializationUtil::write_i32(write_index_, out_stream))) {
        return ret;
    }
    if (RET_FAIL(common::SerializationUtil::write_i32(bit_width, out_stream))) {
        return ret;
    }
    if (RET_FAIL(
            common::SerializationUtil::write_i64(delta_arr_min_, out_stream))) {
        return ret;
    }
    if (RET_FAIL(
            common::SerializationUtil::write_i64(first_value_, out_stream))) {
        return ret;
    }
    // writer data — batched bit-pack + single write_buf for the common case;
    // fall back to per-bit path for the rare wide bit_width (>56).
    const int pack_ret =
        pack_bits_msb(delta_arr_, write_index_, bit_width, out_stream);
    if (pack_ret == -1) {
        for (int i = 0; i < write_index_; i++) {
            write_bits(delta_arr_[i], bit_width, out_stream);
        }
        flush_remaining(out_stream);
    } else if (pack_ret != common::E_OK) {
        return pack_ret;
    }
    // Base reset only — see the int32 specialization for the virtual-call
    // rationale.
    TS2DIFFEncoder<int64_t>::reset();
    return ret;
}

// ============================================================================
// Batch encode: INT32
// Adjacent-difference removes sequential dependency; SIMD for delta + min/max.
// ============================================================================

template <>
inline int TS2DIFFEncoder<int32_t>::encode_batch(
    const int32_t* values, uint32_t count, common::ByteStream& out_stream) {
    int ret = common::E_OK;
    uint32_t offset = 0;

    while (offset < count) {
        // Start of new block: store first_value
        if (write_index_ == -1) {
            first_value_ = values[offset];
            previous_value_ = first_value_;
            write_index_ = 0;
            offset++;
            continue;
        }

        // How many deltas fit in current block
        uint32_t space = static_cast<uint32_t>(block_size_) - write_index_;
        uint32_t batch = std::min(count - offset, space);

        // ── Adjacent difference: delta[i] = values[i] - values[i-1] ──
        // First delta uses previous_value_
        delta_arr_[write_index_] = values[offset] - previous_value_;

        uint32_t i = 1;
#ifdef ENABLE_SIMD
        // SIMD: 4 adjacent differences at a time
        for (; i + 3 < batch; i += 4) {
            simde__m128i cur = simde_mm_loadu_si128(
                reinterpret_cast<const simde__m128i*>(values + offset + i));
            simde__m128i prv = simde_mm_loadu_si128(
                reinterpret_cast<const simde__m128i*>(values + offset + i - 1));
            simde__m128i diff = simde_mm_sub_epi32(cur, prv);
            simde_mm_storeu_si128(
                reinterpret_cast<simde__m128i*>(delta_arr_ + write_index_ + i),
                diff);
        }
#endif
        for (; i < batch; i++) {
            delta_arr_[write_index_ + i] =
                values[offset + i] - values[offset + i - 1];
        }
        previous_value_ = values[offset + batch - 1];

        // ── Min/max of new deltas ──
        int32_t local_min = delta_arr_[write_index_];
        int32_t local_max = delta_arr_[write_index_];

        uint32_t j = 1;
#ifdef ENABLE_SIMD
        if (batch >= 5) {
            simde__m128i vmin = simde_mm_set1_epi32(local_min);
            simde__m128i vmax = vmin;
            for (; j + 3 < batch; j += 4) {
                simde__m128i v =
                    simde_mm_loadu_si128(reinterpret_cast<const simde__m128i*>(
                        delta_arr_ + write_index_ + j));
                vmin = simde_mm_min_epi32(vmin, v);
                vmax = simde_mm_max_epi32(vmax, v);
            }
            // Horizontal reduce
            int32_t tmp[4];
            simde_mm_storeu_si128(reinterpret_cast<simde__m128i*>(tmp), vmin);
            for (int k = 0; k < 4; k++)
                if (tmp[k] < local_min) local_min = tmp[k];
            simde_mm_storeu_si128(reinterpret_cast<simde__m128i*>(tmp), vmax);
            for (int k = 0; k < 4; k++)
                if (tmp[k] > local_max) local_max = tmp[k];
        }
#endif
        for (; j < batch; j++) {
            int32_t d = delta_arr_[write_index_ + j];
            if (d < local_min) local_min = d;
            if (d > local_max) local_max = d;
        }

        // Merge with block min/max
        if (write_index_ == 0) {
            delta_arr_min_ = local_min;
            delta_arr_max_ = local_max;
        } else {
            if (local_min < delta_arr_min_) delta_arr_min_ = local_min;
            if (local_max > delta_arr_max_) delta_arr_max_ = local_max;
        }

        write_index_ += batch;
        offset += batch;

        if (write_index_ >= block_size_) {
            if (RET_FAIL(flush(out_stream))) return ret;
        }
    }
    return ret;
}

// ============================================================================
// Batch encode: INT64
// ============================================================================

template <>
inline int TS2DIFFEncoder<int64_t>::encode_batch(
    const int64_t* values, uint32_t count, common::ByteStream& out_stream) {
    int ret = common::E_OK;
    uint32_t offset = 0;

    while (offset < count) {
        if (write_index_ == -1) {
            first_value_ = values[offset];
            previous_value_ = first_value_;
            write_index_ = 0;
            offset++;
            continue;
        }

        uint32_t space = static_cast<uint32_t>(block_size_) - write_index_;
        uint32_t batch = std::min(count - offset, space);

        // Adjacent difference
        delta_arr_[write_index_] = values[offset] - previous_value_;

        uint32_t i = 1;
#ifdef ENABLE_SIMD
        // SIMD: 2 adjacent differences at a time (128-bit, native NEON)
        for (; i + 1 < batch; i += 2) {
            simde__m128i cur = simde_mm_loadu_si128(
                reinterpret_cast<const simde__m128i*>(values + offset + i));
            simde__m128i prv = simde_mm_loadu_si128(
                reinterpret_cast<const simde__m128i*>(values + offset + i - 1));
            simde__m128i diff = simde_mm_sub_epi64(cur, prv);
            simde_mm_storeu_si128(
                reinterpret_cast<simde__m128i*>(delta_arr_ + write_index_ + i),
                diff);
        }
#endif
        for (; i < batch; i++) {
            delta_arr_[write_index_ + i] =
                values[offset + i] - values[offset + i - 1];
        }
        previous_value_ = values[offset + batch - 1];

        // Min/max (scalar — no efficient 64-bit SIMD min/max before AVX-512)
        int64_t local_min = delta_arr_[write_index_];
        int64_t local_max = delta_arr_[write_index_];
        for (uint32_t j = 1; j < batch; j++) {
            int64_t d = delta_arr_[write_index_ + j];
            if (d < local_min) local_min = d;
            if (d > local_max) local_max = d;
        }

        if (write_index_ == 0) {
            delta_arr_min_ = local_min;
            delta_arr_max_ = local_max;
        } else {
            if (local_min < delta_arr_min_) delta_arr_min_ = local_min;
            if (local_max > delta_arr_max_) delta_arr_max_ = local_max;
        }

        write_index_ += batch;
        offset += batch;

        if (write_index_ >= block_size_) {
            if (RET_FAIL(flush(out_stream))) return ret;
        }
    }
    return ret;
}

// Default: unsupported types fall back to base class loop
template <typename T>
int TS2DIFFEncoder<T>::encode_batch(const int32_t* values, uint32_t count,
                                    common::ByteStream& out) {
    return Encoder::encode_batch(values, count, out);
}
template <typename T>
int TS2DIFFEncoder<T>::encode_batch(const int64_t* values, uint32_t count,
                                    common::ByteStream& out) {
    return Encoder::encode_batch(values, count, out);
}

// Java Math.round(x) == floor(x + 0.5) (ties go towards +infinity),
// unlike std::lround's ties-away-from-zero (-2.5 -> -2, not -3).
// Math.round saturates at the integer limits; a plain static_cast of the
// boundary value (e.g. INT64_MAX rounding up from below, or 2^63 passing
// the <= (double)INT64_MAX gate) would be undefined behaviour, so the
// conversion clamps first.  The 64-bit upper bound is compared against
// 2^63 rather than (double)INT64_MAX, which rounds up to 2^63 and would
// let exactly 2^63 through; the lower limits are exactly representable.
template <typename Int>
inline Int java_round(double x) {
    const double r = std::floor(x + 0.5);
    if (sizeof(Int) < 8) {
        if (r > static_cast<double>(std::numeric_limits<Int>::max())) {
            return std::numeric_limits<Int>::max();
        }
    } else if (r >= 9223372036854775808.0) {  // 2^63
        return std::numeric_limits<Int>::max();
    }
    if (r < static_cast<double>(std::numeric_limits<Int>::min())) {
        return std::numeric_limits<Int>::min();
    }
    return static_cast<Int>(r);
}

class FloatTS2DIFFEncoder : public TS2DIFFEncoder<int32_t> {
   public:
    FloatTS2DIFFEncoder()
        : max_point_number_(2),  // Java Ts2Diff builder default via
                                 // initFromProps -> TSFileConfig
                                 // floatPrecision
          max_point_value_(100.0),
          page_blocks_(1024, common::MOD_TS2DIFF_OBJ, false) {}
    int do_encode(float value, common::ByteStream& out_stream) {
        int32_t value_int = convert_float_to_int(value);
        return TS2DIFFEncoder<int32_t>::do_encode(value_int, out_stream);
    }
    // The wire value is self-describing; tests use this to exercise
    // maxPointNumber = 0 pages (Form 1 leading 0x00 byte).
    void set_max_point_number(int mpn) {
        max_point_number_ = mpn;
        max_point_value_ = mpn <= 0 ? 1.0 : std::pow(10.0, mpn);
    }
    // PageWriter resets the encoder between pages without going through a
    // successful flush() (e.g. when the prior page was aborted).  The base
    // reset() only clears write_index_; underflow_flags_ and the buffered
    // complete blocks would otherwise leak into the next page.
    void reset() override {
        TS2DIFFEncoder<int32_t>::reset();
        underflow_flags_.clear();
        page_blocks_.reset();
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
            return java_round<int32_t>(value);
        }
        if (std::isnan(value)) {
            underflow_flags_.push_back(-1);
            return common::float_to_int(value);
        }
        underflow_flags_.push_back(1);
        return java_round<int32_t>(scaled);
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
    // Java FloatEncoder emits the page metadata (maxPointNumber and, when
    // needed, the page-wide overflow bitmaps) once per page, followed by a
    // continuous sequence of integer TS_2DIFF blocks.  The integer encoder
    // flushes a complete 129-value block on its own whenever it fills, so
    // those blocks are buffered here and emitted, metadata first, when the
    // page is sealed.
    common::ByteStream page_blocks_;
};

class DoubleTS2DIFFEncoder : public TS2DIFFEncoder<int64_t> {
   public:
    DoubleTS2DIFFEncoder()
        : max_point_number_(2),  // Java Ts2Diff builder default via
                                 // initFromProps -> TSFileConfig
                                 // floatPrecision
          max_point_value_(100.0),
          page_blocks_(1024, common::MOD_TS2DIFF_OBJ, false) {}
    int do_encode(double value, common::ByteStream& out_stream) {
        int64_t value_long = convert_double_to_long(value);
        return TS2DIFFEncoder<int64_t>::do_encode(value_long, out_stream);
    }
    // See FloatTS2DIFFEncoder::set_max_point_number.
    void set_max_point_number(int mpn) {
        max_point_number_ = mpn;
        max_point_value_ = mpn <= 0 ? 1.0 : std::pow(10.0, mpn);
    }
    // See FloatTS2DIFFEncoder::reset for rationale — the prior page's
    // overflow markers and buffered blocks must not bleed into the next.
    void reset() override {
        TS2DIFFEncoder<int64_t>::reset();
        underflow_flags_.clear();
        page_blocks_.reset();
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
            return java_round<int64_t>(value);
        }
        if (std::isnan(value)) {
            underflow_flags_.push_back(-1);
            return common::double_to_long(value);
        }
        underflow_flags_.push_back(1);
        return java_round<int64_t>(scaled);
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
    // See FloatTS2DIFFEncoder::page_blocks_ — buffered complete integer
    // blocks, emitted with the page metadata when the page is sealed.
    common::ByteStream page_blocks_;
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

// Java FloatEncoder page layout:
//   no overflow:   [maxPointNumber varint][block 1][block 2]...
//   overflow:      [overflow marker varint][pageValueCount varint]
//                  [page-wide bitmap(s)][maxPointNumber varint][blocks...]
// The integer encoder triggers flush() whenever a 129-value block fills
// (write_index_ == block_size_, reachable only from do_encode); those
// blocks are buffered in page_blocks_ without any float metadata.  When
// the page is sealed (write_index_ < block_size_), the trailing block is
// appended and the page metadata plus all buffered blocks are emitted.
FORCE_INLINE int FloatTS2DIFFEncoder::flush(common::ByteStream& out_stream) {
    int ret = common::E_OK;
    const bool block_flush = (write_index_ == block_size_);
    if (block_flush) {
        // Complete-block flush from do_encode: plain integer block, no
        // float wrapper metadata, overflow flags must survive for the
        // page-wide bitmap.
        return TS2DIFFEncoder<int32_t>::flush(page_blocks_);
    }
    if (write_index_ != -1) {
        // Page-seal flush: append the trailing (partial) integer block.
        if (RET_FAIL(TS2DIFFEncoder<int32_t>::flush(page_blocks_))) {
            return ret;
        }
    }
    if (underflow_flags_.empty()) {
        // Empty page (nothing encoded, no blocks buffered).
        return common::E_OK;
    }
    const int num_values = static_cast<int>(underflow_flags_.size());
    if (has_overflow()) {
        std::vector<uint8_t> scaled_bitmap(
            static_cast<size_t>(num_values / 8 + 1), 0);
        std::vector<uint8_t> raw_bits_bitmap(
            static_cast<size_t>(num_values / 8 + 1), 0);
        bool has_raw_bits = false;
        for (int i = 0; i < num_values; i++) {
            int8_t f = underflow_flags_[static_cast<size_t>(i)];
            if (f == 1) {
                scaled_bitmap[static_cast<size_t>(i / 8)] |=
                    static_cast<uint8_t>(1u << (i % 8));
            } else if (f == -1) {
                has_raw_bits = true;
                raw_bits_bitmap[static_cast<size_t>(i / 8)] |=
                    static_cast<uint8_t>(1u << (i % 8));
            }
        }
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                has_raw_bits ? ts2diff_java_detail::FLAG_ORIGINAL_VALUE_OVERFLOW
                             : ts2diff_java_detail::FLAG_SCALED_VALUE_OVERFLOW,
                out_stream))) {
            return ret;
        }
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                static_cast<uint32_t>(num_values), out_stream))) {
            return ret;
        }
        const uint32_t bm_len = static_cast<uint32_t>(num_values / 8 + 1);
        if (RET_FAIL(out_stream.write_buf(scaled_bitmap.data(), bm_len))) {
            return ret;
        }
        if (has_raw_bits &&
            RET_FAIL(out_stream.write_buf(raw_bits_bitmap.data(), bm_len))) {
            return ret;
        }
    }
    // maxPointNumber sits right before the first integer block in every
    // page layout.
    if (RET_FAIL(common::SerializationUtil::write_var_uint(
            static_cast<uint32_t>(max_point_number_), out_stream))) {
        return ret;
    }
    if (RET_FAIL(common::merge_byte_stream(out_stream, page_blocks_))) {
        return ret;
    }
    // The encoder state is wiped only after every write into out_stream has
    // committed: clearing it earlier would leave write_index_ at -1 after a
    // mid-flush failure, so the retrying flush() would short-circuit at the
    // top and drop the data.
    underflow_flags_.clear();
    page_blocks_.reset();
    return ret;
}

// See FloatTS2DIFFEncoder::flush for the page layout rationale.
FORCE_INLINE int DoubleTS2DIFFEncoder::flush(common::ByteStream& out_stream) {
    int ret = common::E_OK;
    const bool block_flush = (write_index_ == block_size_);
    if (block_flush) {
        return TS2DIFFEncoder<int64_t>::flush(page_blocks_);
    }
    if (write_index_ != -1) {
        if (RET_FAIL(TS2DIFFEncoder<int64_t>::flush(page_blocks_))) {
            return ret;
        }
    }
    if (underflow_flags_.empty()) {
        return common::E_OK;
    }
    const int num_values = static_cast<int>(underflow_flags_.size());
    if (has_overflow()) {
        std::vector<uint8_t> scaled_bitmap(
            static_cast<size_t>(num_values / 8 + 1), 0);
        std::vector<uint8_t> raw_bits_bitmap(
            static_cast<size_t>(num_values / 8 + 1), 0);
        bool has_raw_bits = false;
        for (int i = 0; i < num_values; i++) {
            int8_t f = underflow_flags_[static_cast<size_t>(i)];
            if (f == 1) {
                scaled_bitmap[static_cast<size_t>(i / 8)] |=
                    static_cast<uint8_t>(1u << (i % 8));
            } else if (f == -1) {
                has_raw_bits = true;
                raw_bits_bitmap[static_cast<size_t>(i / 8)] |=
                    static_cast<uint8_t>(1u << (i % 8));
            }
        }
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                has_raw_bits ? ts2diff_java_detail::FLAG_ORIGINAL_VALUE_OVERFLOW
                             : ts2diff_java_detail::FLAG_SCALED_VALUE_OVERFLOW,
                out_stream))) {
            return ret;
        }
        if (RET_FAIL(common::SerializationUtil::write_var_uint(
                static_cast<uint32_t>(num_values), out_stream))) {
            return ret;
        }
        const uint32_t bm_len = static_cast<uint32_t>(num_values / 8 + 1);
        if (RET_FAIL(out_stream.write_buf(scaled_bitmap.data(), bm_len))) {
            return ret;
        }
        if (has_raw_bits &&
            RET_FAIL(out_stream.write_buf(raw_bits_bitmap.data(), bm_len))) {
            return ret;
        }
    }
    if (RET_FAIL(common::SerializationUtil::write_var_uint(
            static_cast<uint32_t>(max_point_number_), out_stream))) {
        return ret;
    }
    if (RET_FAIL(common::merge_byte_stream(out_stream, page_blocks_))) {
        return ret;
    }
    // Same deferred-reset rationale as FloatTS2DIFFEncoder::flush — keeping
    // write_index_ live until every committed write succeeds avoids the
    // "next flush returns E_OK on lost data" pattern.
    underflow_flags_.clear();
    page_blocks_.reset();
    return ret;
}

}  // end namespace storage
#endif  // ENCODING_TS2DIFF_ENCODER_H
