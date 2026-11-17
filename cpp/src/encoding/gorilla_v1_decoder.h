/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#ifndef ENCODING_GORILLA_V1_DECODER_H
#define ENCODING_GORILLA_V1_DECODER_H

#include <algorithm>
#include <cmath>
#include <cstdint>

#include "common/allocator/byte_stream.h"
#include "decoder.h"
#include "encoding/encode_utils.h"
#include "gorilla_decoder.h"  // GorillaBitReader

namespace storage {

namespace gorilla_v1_detail {

// Type-dependent parts of the GORILLA_V1 bit layout.  Stored is the raw
// little-endian bit pattern of a value (uint32_t for FLOAT, uint64_t for
// DOUBLE), matching the types used by the per-value decoders below.
template <typename Stored>
struct GorillaV1RawTraits;

template <>
struct GorillaV1RawTraits<uint32_t> {
    typedef float Value;
    static constexpr int VALUE_BITS = 32;
    static constexpr int LEADING_BITS = 5;
    static constexpr int LEN_BITS = 6;

    static FORCE_INLINE float to_value(uint32_t bits) {
        return common::int_to_float(static_cast<int32_t>(bits));
    }
    static FORCE_INLINE int leading_zeros(uint32_t bits) {
        return number_of_leading_zeros(static_cast<int32_t>(bits));
    }
    static FORCE_INLINE int trailing_zeros(uint32_t bits) {
        return number_of_trailing_zeros(static_cast<int32_t>(bits));
    }
};

template <>
struct GorillaV1RawTraits<uint64_t> {
    typedef double Value;
    static constexpr int VALUE_BITS = 64;
    static constexpr int LEADING_BITS = 6;
    static constexpr int LEN_BITS = 7;

    static FORCE_INLINE double to_value(uint64_t bits) {
        return common::long_to_double(static_cast<int64_t>(bits));
    }
    static FORCE_INLINE int leading_zeros(uint64_t bits) {
        return number_of_leading_zeros(static_cast<int64_t>(bits));
    }
    static FORCE_INLINE int trailing_zeros(uint64_t bits) {
        return number_of_trailing_zeros(static_cast<int64_t>(bits));
    }
};

// Raw-pointer equivalent of the per-value prefetch(): decodes the encoding of
// the value that follows the one just returned and stores it in @pre_value.
// A NaN marks the encoder's end-of-stream sentinel and is never returned as a
// value.
template <typename Stored>
FORCE_INLINE void prefetch_raw(GorillaBitReader& r, Stored& pre_value,
                               int& leading_zero_num, int& trailing_zero_num,
                               bool& is_end, int& read_error) {
    typedef GorillaV1RawTraits<Stored> traits;
    const bool next_flag1 = r.read_bit();
    if (UNLIKELY(r.exhausted)) {
        read_error = common::E_BUF_NOT_ENOUGH;
        return;
    }
    if (!next_flag1) {
        return;
    }
    const bool next_flag2 = r.read_bit();
    if (UNLIKELY(r.exhausted)) {
        read_error = common::E_BUF_NOT_ENOUGH;
        return;
    }
    if (!next_flag2) {
        const int len =
            traits::VALUE_BITS - leading_zero_num - trailing_zero_num;
        if (UNLIKELY(len < 0 || len > traits::VALUE_BITS)) {
            read_error = common::E_DECODE_ERR;
            return;
        }
        uint64_t xor_value = r.read_long(len);
        if (UNLIKELY(r.exhausted)) {
            read_error = common::E_BUF_NOT_ENOUGH;
            return;
        }
        xor_value <<= trailing_zero_num;
        pre_value ^= static_cast<Stored>(xor_value);
    } else {
        const int leading = static_cast<int>(r.read_long(traits::LEADING_BITS));
        if (UNLIKELY(r.exhausted)) {
            read_error = common::E_BUF_NOT_ENOUGH;
            return;
        }
        const int len = static_cast<int>(r.read_long(traits::LEN_BITS));
        if (UNLIKELY(r.exhausted)) {
            read_error = common::E_BUF_NOT_ENOUGH;
            return;
        }
        if (UNLIKELY(len < 0 || leading < 0 ||
                     leading + len > traits::VALUE_BITS)) {
            read_error = common::E_DECODE_ERR;
            return;
        }
        uint64_t xor_value = r.read_long(len);
        if (UNLIKELY(r.exhausted)) {
            read_error = common::E_BUF_NOT_ENOUGH;
            return;
        }
        // len == 0 carries no xor bits, so the value is zero; skipping the
        // shift also avoids shifting by the full type width on a corrupt
        // stream whose leading/len fields both decode to zero.
        if (len > 0) {
            xor_value <<= (traits::VALUE_BITS - leading - len);
        }
        pre_value ^= static_cast<Stored>(xor_value);
    }
    leading_zero_num = traits::leading_zeros(pre_value);
    trailing_zero_num = traits::trailing_zeros(pre_value);
    if (std::isnan(traits::to_value(pre_value))) {
        is_end = true;
    }
}

}  // namespace gorilla_v1_detail

// Decoder for the original Gorilla (GORILLA_V1) layout written by Java's
// SinglePrecisionEncoderV1/DoublePrecisionEncoderV1.  Values are decoded one
// step ahead, exactly like the Java decoder, because the NaN sentinel that
// flush() appends is what marks the end of the stream.
class GorillaV1DecoderBase : public Decoder {
   public:
    void reset() override { reset_state(); }

   protected:
    void reset_state() {
        flag_ = false;
        leading_zero_num_ = 0;
        trailing_zero_num_ = 0;
        is_end_ = false;
        buffer_ = 0;
        bits_left_ = 0;
        read_error_ = common::E_OK;
    }

    bool read_bit(common::ByteStream& in) {
        if (bits_left_ == 0 && !is_end_) {
            fill_buffer(in);
        }
        if (buffer_ < 0) {
            return false;
        }
        --bits_left_;
        return ((buffer_ >> bits_left_) & 1) == 1;
    }

    void fill_buffer(common::ByteStream& in) {
        uint8_t byte = 0;
        uint32_t read_len = 0;
        if (in.read_buf(&byte, 1, read_len) == common::E_OK && read_len == 1) {
            buffer_ = byte;
            bits_left_ = 8;
        } else {
            // A stream truncated mid-value must not be decoded as a run of
            // zero bits; latch the error so read_float/read_double surface it.
            buffer_ = -1;
            bits_left_ = -1;
            read_error_ = common::E_BUF_NOT_ENOUGH;
        }
    }

    int read_bits_as_int(common::ByteStream& in, int len) {
        int num = 0;
        for (int i = 0; i < len; i++) {
            num = (num << 1) | (read_bit(in) ? 1 : 0);
        }
        return num;
    }

    // Batch decode over a wrapped ByteStream.  Walks the same read sequence as
    // the per-value read_float/read_double (lookahead included, so a batch must
    // never return the NaN sentinel), but reads bits from a 64-bit reservoir
    // over the wrapped buffer instead of calling ByteStream::read_buf per byte.
    // All decoder state is preserved so a batch may stop mid-stream and the
    // next read_batch_* / read_float / read_double resumes correctly.
    template <typename Stored>
    int batch_decode_raw(
        typename gorilla_v1_detail::GorillaV1RawTraits<Stored>::Value* out,
        int capacity, int& actual, Stored& pre_value, common::ByteStream& in) {
        typedef gorilla_v1_detail::GorillaV1RawTraits<Stored> traits;
        actual = 0;
        if (capacity <= 0) {
            return common::E_OK;
        }

        const uint64_t cursor = in.read_pos();
        const uint64_t end_pos = cursor + in.remaining_size();
        // Anchor at the byte holding unread bits (if any) so the batch resumes
        // exactly where a per-value read stopped: those bits are the low
        // bits_left_ bits of buffer_, a byte already consumed by the stream.
        const bool has_partial = bits_left_ > 0;
        const uint64_t base_off = has_partial ? cursor - 1 : cursor;
        const uint64_t data_len64 = end_pos - base_off;
        // Pages are far smaller than 4 GiB, so saturating keeps the 32-bit
        // cursor of GorillaBitReader safe.
        const uint32_t data_len = data_len64 > UINT32_MAX
                                      ? UINT32_MAX
                                      : static_cast<uint32_t>(data_len64);

        GorillaBitReader r;
        r.data =
            reinterpret_cast<const uint8_t*>(in.get_wrapped_buf()) + base_off;
        r.pos = has_partial ? 1 : 0;
        r.data_len = data_len;
        r.bits = has_partial ? bits_left_ : 0;
        r.buffer = has_partial
                       ? static_cast<uint64_t>(static_cast<uint8_t>(buffer_))
                       : 0;

        int ret = common::E_OK;
        const uint32_t value_bytes = traits::VALUE_BITS / 8;
        while (actual < capacity) {
            // The lookahead that returns the last value also decodes the NaN
            // sentinel and latches is_end_; the sentinel is never a value. In
            // a well-formed page that lookahead also exhausts the bytes, so
            // the guard below would stop here anyway; checking is_end_ first
            // keeps a batch from decoding past an embedded NaN (the per-value
            // decoder stops fetching bytes once is_end_ is set).
            if (UNLIKELY(is_end_)) {
                break;
            }
            // Mirror the per-value batch guard (has_remaining()): stop once
            // every byte of the stream has been fetched, even if some low bits
            // of the last byte are still buffered.
            const uint64_t consumed_bits = static_cast<uint64_t>(r.pos) * 8 -
                                           static_cast<uint64_t>(r.bits);
            if (UNLIKELY(data_len == 0 ||
                         consumed_bits >
                             static_cast<uint64_t>(data_len) * 8 - 8)) {
                break;
            }
            if (UNLIKELY(read_error_ != common::E_OK)) {
                ret = read_error_;
                break;
            }

            typename traits::Value value;
            if (UNLIKELY(!flag_)) {
                flag_ = true;
                if (UNLIKELY(r.bits != 0 || r.pos + value_bytes > r.data_len)) {
                    // Truncated before the first value finished. read_buf
                    // consumes the remaining bytes before reporting the error.
                    r.pos = r.data_len;
                    ret = common::E_DECODE_ERR;
                    break;
                }
                Stored raw = 0;
                for (uint32_t i = 0; i < value_bytes; i++) {
                    raw |= static_cast<Stored>(r.data[r.pos + i]) << (i * 8);
                }
                r.pos += value_bytes;
                pre_value = raw;
                leading_zero_num_ = traits::leading_zeros(raw);
                trailing_zero_num_ = traits::trailing_zeros(raw);
                value = traits::to_value(raw);
            } else {
                value = traits::to_value(pre_value);
            }
            gorilla_v1_detail::prefetch_raw<Stored>(
                r, pre_value, leading_zero_num_, trailing_zero_num_, is_end_,
                read_error_);
            if (UNLIKELY(read_error_ != common::E_OK)) {
                // The per-value read returns the error together with the value
                // whose lookahead failed; the batch loop discards that value.
                ret = read_error_;
                break;
            }
            out[actual++] = value;
        }

        // Sync the raw cursor back. The unread bits are the low `bits` bits of
        // the reservoir, so the byte cursor follows from the consumed bit
        // count; restore the byte-oriented scalar state from that.
        const uint64_t consumed_bits =
            static_cast<uint64_t>(r.pos) * 8 - static_cast<uint64_t>(r.bits);
        const uint64_t consumed_bytes = consumed_bits / 8;
        const int partial_bits = static_cast<int>(consumed_bits % 8);
        if (partial_bits == 0) {
            buffer_ = 0;
            bits_left_ = 0;
            in.wrapped_buf_advance_read_pos(base_off + consumed_bytes - cursor);
        } else {
            buffer_ = r.data[consumed_bytes];
            bits_left_ = 8 - partial_bits;
            in.wrapped_buf_advance_read_pos(base_off + consumed_bytes + 1 -
                                            cursor);
        }
        return ret;
    }

    bool flag_ = false;
    int leading_zero_num_ = 0;
    int trailing_zero_num_ = 0;
    bool is_end_ = false;
    int buffer_ = 0;
    int bits_left_ = 0;
    int read_error_ = common::E_OK;
};

class FloatGorillaV1Decoder : public GorillaV1DecoderBase {
   public:
    bool has_remaining(const common::ByteStream& buffer) override {
        return buffer.has_remaining();
    }

    int read_float(float& ret_value, common::ByteStream& in) override {
        if (!flag_) {
            flag_ = true;
            uint8_t bytes[4] = {0, 0, 0, 0};
            uint32_t read_len = 0;
            if (in.read_buf(bytes, 4, read_len) != common::E_OK ||
                read_len != 4) {
                return common::E_DECODE_ERR;
            }
            pre_value_ = static_cast<uint32_t>(bytes[0]) |
                         (static_cast<uint32_t>(bytes[1]) << 8) |
                         (static_cast<uint32_t>(bytes[2]) << 16) |
                         (static_cast<uint32_t>(bytes[3]) << 24);
            leading_zero_num_ =
                number_of_leading_zeros(static_cast<int32_t>(pre_value_));
            trailing_zero_num_ =
                number_of_trailing_zeros(static_cast<int32_t>(pre_value_));
            ret_value = common::int_to_float(static_cast<int32_t>(pre_value_));
            prefetch(in);
            return read_error_;
        }
        ret_value = common::int_to_float(static_cast<int32_t>(pre_value_));
        prefetch(in);
        return read_error_;
    }

    int read_boolean(bool& /*ret_value*/, common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int32(int32_t& /*ret_value*/,
                   common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int64(int64_t& /*ret_value*/,
                   common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_batch_float(float* out, int capacity, int& actual,
                         common::ByteStream& in) override {
        // The raw path needs a contiguous wrapped buffer, and it anchors on the
        // byte holding unread bits; with no consumed byte to anchor on (a
        // caller re-wrapped the stream without resetting) only the per-value
        // loop can make sense of the buffered bits.
        if (UNLIKELY(!in.is_wrapped() ||
                     (bits_left_ > 0 && in.read_pos() == 0))) {
            return Decoder::read_batch_float(out, capacity, actual, in);
        }
        return batch_decode_raw<uint32_t>(out, capacity, actual, pre_value_,
                                          in);
    }
    int skip_float(int count, int& skipped, common::ByteStream& in) override {
        skipped = 0;
        // No cheaper way to skip exists: the encoding is stateful, so decode
        // through the batch path and discard.
        float scratch[64];
        while (skipped < count) {
            const int chunk = std::min(count - skipped, 64);
            int got = 0;
            const int ret = read_batch_float(scratch, chunk, got, in);
            if (ret != common::E_OK) {
                return ret;
            }
            skipped += got;
            if (got < chunk) {
                break;
            }
        }
        return common::E_OK;
    }
    int read_double(double& /*ret_value*/,
                    common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_String(common::String& /*ret_value*/, common::PageArena& /*pa*/,
                    common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

   private:
    void prefetch(common::ByteStream& in) {
        const bool next_flag1 = read_bit(in);
        if (!next_flag1) {
            return;
        }
        const bool next_flag2 = read_bit(in);
        if (!next_flag2) {
            const int len = 32 - leading_zero_num_ - trailing_zero_num_;
            if (len < 0 || len > 32) {
                read_error_ = common::E_DECODE_ERR;
                return;
            }
            uint32_t xor_value = 0;
            for (int i = 0; i < len; i++) {
                xor_value = (xor_value << 1) | (read_bit(in) ? 1u : 0u);
            }
            xor_value <<= trailing_zero_num_;
            pre_value_ = xor_value ^ pre_value_;
        } else {
            const int leading = read_bits_as_int(in, 5);
            const int len = read_bits_as_int(in, 6);
            if (len < 0 || leading < 0 || leading + len > 32) {
                read_error_ = common::E_DECODE_ERR;
                return;
            }
            uint32_t xor_value = 0;
            for (int i = 0; i < len; i++) {
                xor_value = (xor_value << 1) | (read_bit(in) ? 1u : 0u);
            }
            // len == 0 carries no xor bits, so the value is zero; skipping the
            // shift also avoids shifting by the full type width on a corrupt
            // stream whose leading/len fields both decode to zero.
            if (len > 0) {
                xor_value <<= (32 - leading - len);
            }
            pre_value_ = xor_value ^ pre_value_;
        }
        leading_zero_num_ =
            number_of_leading_zeros(static_cast<int32_t>(pre_value_));
        trailing_zero_num_ =
            number_of_trailing_zeros(static_cast<int32_t>(pre_value_));
        if (std::isnan(
                common::int_to_float(static_cast<int32_t>(pre_value_)))) {
            is_end_ = true;
        }
    }

    uint32_t pre_value_ = 0;
};

class DoubleGorillaV1Decoder : public GorillaV1DecoderBase {
   public:
    bool has_remaining(const common::ByteStream& buffer) override {
        return buffer.has_remaining();
    }

    int read_double(double& ret_value, common::ByteStream& in) override {
        if (!flag_) {
            flag_ = true;
            uint8_t bytes[8] = {0, 0, 0, 0, 0, 0, 0, 0};
            uint32_t read_len = 0;
            if (in.read_buf(bytes, 8, read_len) != common::E_OK ||
                read_len != 8) {
                return common::E_DECODE_ERR;
            }
            pre_value_ = 0;
            for (int i = 0; i < 8; i++) {
                pre_value_ |= static_cast<uint64_t>(bytes[i]) << (i * 8);
            }
            leading_zero_num_ =
                number_of_leading_zeros(static_cast<int64_t>(pre_value_));
            trailing_zero_num_ =
                number_of_trailing_zeros(static_cast<int64_t>(pre_value_));
            ret_value =
                common::long_to_double(static_cast<int64_t>(pre_value_));
            prefetch(in);
            return read_error_;
        }
        ret_value = common::long_to_double(static_cast<int64_t>(pre_value_));
        prefetch(in);
        return read_error_;
    }

    int read_boolean(bool& /*ret_value*/, common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int32(int32_t& /*ret_value*/,
                   common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_int64(int64_t& /*ret_value*/,
                   common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_float(float& /*ret_value*/, common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int read_batch_double(double* out, int capacity, int& actual,
                          common::ByteStream& in) override {
        // See read_batch_float: the raw path needs a wrapped buffer and a
        // consumed byte to anchor on.
        if (UNLIKELY(!in.is_wrapped() ||
                     (bits_left_ > 0 && in.read_pos() == 0))) {
            return Decoder::read_batch_double(out, capacity, actual, in);
        }
        return batch_decode_raw<uint64_t>(out, capacity, actual, pre_value_,
                                          in);
    }
    int skip_double(int count, int& skipped, common::ByteStream& in) override {
        skipped = 0;
        // No cheaper way to skip exists: the encoding is stateful, so decode
        // through the batch path and discard.
        double scratch[64];
        while (skipped < count) {
            const int chunk = std::min(count - skipped, 64);
            int got = 0;
            const int ret = read_batch_double(scratch, chunk, got, in);
            if (ret != common::E_OK) {
                return ret;
            }
            skipped += got;
            if (got < chunk) {
                break;
            }
        }
        return common::E_OK;
    }
    int read_String(common::String& /*ret_value*/, common::PageArena& /*pa*/,
                    common::ByteStream& /*in*/) override {
        return common::E_TYPE_NOT_MATCH;
    }

   private:
    void prefetch(common::ByteStream& in) {
        const bool next_flag1 = read_bit(in);
        if (!next_flag1) {
            return;
        }
        const bool next_flag2 = read_bit(in);
        if (!next_flag2) {
            const int len = 64 - leading_zero_num_ - trailing_zero_num_;
            if (len < 0 || len > 64) {
                read_error_ = common::E_DECODE_ERR;
                return;
            }
            uint64_t xor_value = 0;
            for (int i = 0; i < len; i++) {
                xor_value = (xor_value << 1) | (read_bit(in) ? 1ULL : 0ULL);
            }
            xor_value <<= trailing_zero_num_;
            pre_value_ = xor_value ^ pre_value_;
        } else {
            const int leading = read_bits_as_int(in, 6);
            const int len = read_bits_as_int(in, 7);
            if (len < 0 || leading < 0 || leading + len > 64) {
                read_error_ = common::E_DECODE_ERR;
                return;
            }
            uint64_t xor_value = 0;
            for (int i = 0; i < len; i++) {
                xor_value = (xor_value << 1) | (read_bit(in) ? 1ULL : 0ULL);
            }
            // len == 0 carries no xor bits, so the value is zero; skipping the
            // shift also avoids shifting by the full type width on a corrupt
            // stream whose leading/len fields both decode to zero.
            if (len > 0) {
                xor_value <<= (64 - leading - len);
            }
            pre_value_ = xor_value ^ pre_value_;
        }
        leading_zero_num_ =
            number_of_leading_zeros(static_cast<int64_t>(pre_value_));
        trailing_zero_num_ =
            number_of_trailing_zeros(static_cast<int64_t>(pre_value_));
        if (std::isnan(
                common::long_to_double(static_cast<int64_t>(pre_value_)))) {
            is_end_ = true;
        }
    }

    uint64_t pre_value_ = 0;
};

}  // namespace storage

#endif  // ENCODING_GORILLA_V1_DECODER_H
