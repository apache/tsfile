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

// ── Raw-pointer bit reader ────────────────────────────────────────────────
// Operates directly on a contiguous byte array, bypassing ByteStream's
// per-byte read_buf() overhead (atomic loads, page boundary checks, memcpy).

struct GorillaBitReader {
    const uint8_t* data;
    uint32_t pos;       // next byte index to load
    uint32_t data_len;  // total bytes
    int bits;           // remaining bits in cur_byte (0..8)
    uint8_t cur_byte;

    FORCE_INLINE void load_byte_if_empty() {
        if (bits == 0 && pos < data_len) {
            cur_byte = data[pos++];
            bits = 8;
        }
    }

    FORCE_INLINE bool read_bit() {
        bool bit = ((cur_byte >> (bits - 1)) & 1) == 1;
        bits--;
        load_byte_if_empty();
        return bit;
    }

    FORCE_INLINE int64_t read_long(int n) {
        int64_t value = 0;
        while (n > 0) {
            if (n > bits || n == 8) {
                value = (value << bits) + (cur_byte & ((1 << bits) - 1));
                n -= bits;
                bits = 0;
            } else {
                value = (value << n) +
                        ((cur_byte >> (bits - n)) & ((1 << n) - 1));
                bits -= n;
                n = 0;
            }
            load_byte_if_empty();
        }
        return value;
    }

    FORCE_INLINE uint8_t read_control_bits(int max_bits) {
        uint8_t value = 0x00;
        for (int i = 0; i < max_bits; i++) {
            value <<= 1;
            if (read_bit()) {
                value |= 0x01;
            } else {
                break;
            }
        }
        return value;
    }
};

// ── Templated raw-pointer decode helpers ──────────────────────────────────

template <typename T>
struct GorillaRawOps {
    static FORCE_INLINE T read_next(GorillaBitReader& r,
                                     T& stored_value,
                                     int& stored_leading_zeros,
                                     int& stored_trailing_zeros);
};

template <>
struct GorillaRawOps<int32_t> {
    static constexpr int VALUE_BITS = VALUE_BITS_LENGTH_32BIT;

    static FORCE_INLINE int32_t read_next(GorillaBitReader& r,
                                           int32_t& stored_value,
                                           int& stored_leading_zeros,
                                           int& stored_trailing_zeros) {
        uint8_t ctrl = r.read_control_bits(2);
        switch (ctrl) {
            case 3: {
                stored_leading_zeros =
                    (int)r.read_long(LEADING_ZERO_BITS_LENGTH_32BIT);
                uint8_t sig =
                    (uint8_t)r.read_long(MEANINGFUL_XOR_BITS_LENGTH_32BIT);
                sig++;
                stored_trailing_zeros =
                    VALUE_BITS - sig - stored_leading_zeros;
            }
            // fallthrough
            case 2: {
                int32_t xor_value = (int32_t)r.read_long(
                    VALUE_BITS - stored_leading_zeros - stored_trailing_zeros);
                xor_value = static_cast<uint32_t>(xor_value)
                            << stored_trailing_zeros;
                stored_value ^= xor_value;
            }
            // fallthrough
            default:
                return stored_value;
        }
        return stored_value;
    }
};

template <>
struct GorillaRawOps<int64_t> {
    static constexpr int VALUE_BITS = VALUE_BITS_LENGTH_64BIT;

    static FORCE_INLINE int64_t read_next(GorillaBitReader& r,
                                           int64_t& stored_value,
                                           int& stored_leading_zeros,
                                           int& stored_trailing_zeros) {
        uint8_t ctrl = r.read_control_bits(2);
        switch (ctrl) {
            case 3: {
                stored_leading_zeros =
                    (int)r.read_long(LEADING_ZERO_BITS_LENGTH_64BIT);
                uint8_t sig =
                    (uint8_t)r.read_long(MEANINGFUL_XOR_BITS_LENGTH_64BIT);
                sig++;
                stored_trailing_zeros =
                    VALUE_BITS - sig - stored_leading_zeros;
            }
            // fallthrough
            case 2: {
                int64_t xor_value = r.read_long(
                    VALUE_BITS - stored_leading_zeros - stored_trailing_zeros);
                xor_value = static_cast<uint64_t>(xor_value)
                            << stored_trailing_zeros;
                stored_value ^= xor_value;
            }
            // fallthrough
            default:
                return stored_value;
        }
        return stored_value;
    }
};

// ──────────────────────────────────────────────────────────────────────────

template <typename T>
class GorillaDecoder : public Decoder {
   public:
    GorillaDecoder() { reset(); }

    ~GorillaDecoder() override = default;

    void reset() override {
        type_ = common::GORILLA;
        stored_value_ = 0;
        stored_leading_zeros_ = INT32_MAX;
        stored_trailing_zeros_ = 0;
        bits_left_ = 0;
        first_value_was_read_ = false;
        has_next_ = false;
        buffer_ = 0;
    }

    FORCE_INLINE bool has_next() { return has_next_; }
    FORCE_INLINE bool has_remaining(const common::ByteStream& buffer) override {
        return buffer.has_remaining() || has_next();
    }

    // If empty, cache 8 bits from in_stream to 'buffer_'.
    void flush_byte_if_empty(common::ByteStream& in) {
        if (bits_left_ == 0) {
            uint32_t read_len = 0;
            in.read_buf(&buffer_, 1, read_len);
            bits_left_ = 8;
        }
    }

    // Reads the next bit and returns true if the next bit is 1, otherwise 0.
    bool read_bit(common::ByteStream& in) {
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
    int64_t read_long(int bits, common::ByteStream& in) {
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
    uint8_t read_next_control_bit(int max_bits, common::ByteStream& in) {
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

    T read_next(common::ByteStream& in);
    virtual T cache_next(common::ByteStream& in);
    T decode(common::ByteStream& in);

    // interface from Decoder
    int read_boolean(bool& ret_value, common::ByteStream& in) override;
    int read_int32(int32_t& ret_value, common::ByteStream& in) override;
    int read_int64(int64_t& ret_value, common::ByteStream& in) override;
    int read_float(float& ret_value, common::ByteStream& in) override;
    int read_double(double& ret_value, common::ByteStream& in) override;
    int read_String(common::String& ret_value, common::PageArena& pa,
                    common::ByteStream& in) override;

    // Batch overrides — declared here, defined after template specializations
    int read_batch_int32(int32_t* out, int capacity, int& actual,
                         common::ByteStream& in) override;
    int read_batch_int64(int64_t* out, int capacity, int& actual,
                         common::ByteStream& in) override;
    int skip_int32(int count, int& skipped,
                   common::ByteStream& in) override;
    int skip_int64(int count, int& skipped,
                   common::ByteStream& in) override;

   protected:
    // ── Batch decode using raw pointer (bypasses ByteStream) ─────────────
    // The decode() contract:
    //   stored_value_ holds the "next" value to be returned.
    //   decode() returns stored_value_, then advances via cache_next().
    //   has_next_==false means the ending sentinel was hit.
    //
    // batch_decode_raw replicates this logic using GorillaBitReader on the
    // wrapped contiguous buffer, then syncs state back to ByteStream.
    int batch_decode_raw(T* out, int capacity, int& actual, T ending,
                         common::ByteStream& in) {
        if (!in.is_wrapped()) {
            return batch_decode_fallback(out, capacity, actual, ending, in);
        }

        const uint8_t* base =
            (const uint8_t*)in.get_wrapped_buf() + in.read_pos();
        uint32_t remain = in.remaining_size();

        GorillaBitReader r;
        r.data = base;
        r.pos = 0;
        r.data_len = remain;
        r.bits = bits_left_;
        r.cur_byte = buffer_;

        actual = 0;

        // Bootstrap first value if needed (mirrors decode()'s first-call path)
        if (UNLIKELY(!first_value_was_read_)) {
            if (r.bits == 0 && r.pos >= r.data_len) goto done;
            r.load_byte_if_empty();
            stored_value_ =
                (T)r.read_long(GorillaRawOps<T>::VALUE_BITS);
            first_value_was_read_ = true;
            // Save the first value before cache_next mutates stored_value_
            T first_value = stored_value_;
            // cache_next: read_next then check ending
            GorillaRawOps<T>::read_next(r, stored_value_,
                                        stored_leading_zeros_,
                                        stored_trailing_zeros_);
            if (stored_value_ == ending) {
                has_next_ = false;
            } else {
                has_next_ = true;
            }
            // Output the first value
            out[actual++] = first_value;
            if (!has_next_ || actual >= capacity) goto done;
        }

        // Main batch loop
        while (actual < capacity && has_next_) {
            out[actual++] = stored_value_;
            GorillaRawOps<T>::read_next(r, stored_value_,
                                        stored_leading_zeros_,
                                        stored_trailing_zeros_);
            if (stored_value_ == ending) {
                has_next_ = false;
            }
        }

    done:
        // Sync bit-reader state back
        buffer_ = r.cur_byte;
        bits_left_ = r.bits;
        in.wrapped_buf_advance_read_pos(r.pos);
        return common::E_OK;
    }

    int batch_skip_raw(int count, int& skipped, T ending,
                       common::ByteStream& in) {
        if (!in.is_wrapped()) {
            return batch_skip_fallback(count, skipped, ending, in);
        }

        const uint8_t* base =
            (const uint8_t*)in.get_wrapped_buf() + in.read_pos();
        uint32_t remain = in.remaining_size();

        GorillaBitReader r;
        r.data = base;
        r.pos = 0;
        r.data_len = remain;
        r.bits = bits_left_;
        r.cur_byte = buffer_;

        skipped = 0;

        if (UNLIKELY(!first_value_was_read_)) {
            if (r.bits == 0 && r.pos >= r.data_len) goto done;
            r.load_byte_if_empty();
            stored_value_ =
                (T)r.read_long(GorillaRawOps<T>::VALUE_BITS);
            first_value_was_read_ = true;
            GorillaRawOps<T>::read_next(r, stored_value_,
                                        stored_leading_zeros_,
                                        stored_trailing_zeros_);
            if (stored_value_ == ending) {
                has_next_ = false;
            } else {
                has_next_ = true;
            }
            // The first value counts as one skip
            skipped++;
            if (!has_next_ || skipped >= count) goto done;
        }

        while (skipped < count && has_next_) {
            skipped++;
            GorillaRawOps<T>::read_next(r, stored_value_,
                                        stored_leading_zeros_,
                                        stored_trailing_zeros_);
            if (stored_value_ == ending) {
                has_next_ = false;
            }
        }

    done:
        buffer_ = r.cur_byte;
        bits_left_ = r.bits;
        in.wrapped_buf_advance_read_pos(r.pos);
        return common::E_OK;
    }

    int batch_decode_fallback(T* out, int capacity, int& actual, T ending,
                              common::ByteStream& in) {
        actual = 0;
        while (actual < capacity && has_remaining(in)) {
            out[actual++] = decode(in);
        }
        return common::E_OK;
    }

    int batch_skip_fallback(int count, int& skipped, T ending,
                            common::ByteStream& in) {
        skipped = 0;
        while (skipped < count && has_remaining(in)) {
            decode(in);
            skipped++;
        }
        return common::E_OK;
    }

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
GorillaDecoder<int32_t>::read_next(common::ByteStream& in) {
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
            xor_value = static_cast<uint32_t>(xor_value)
                        << stored_trailing_zeros_;
            stored_value_ ^= xor_value;
            // missing break is intentional, we want to overflow to next one
        default:  // case '0': use stored value
            return stored_value_;
    }
    return stored_value_;
}

template <>
FORCE_INLINE int64_t
GorillaDecoder<int64_t>::read_next(common::ByteStream& in) {
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
            xor_value = static_cast<uint64_t>(xor_value)
                        << stored_trailing_zeros_;
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
GorillaDecoder<int32_t>::cache_next(common::ByteStream& in) {
    read_next(in);
    if (stored_value_ == GORILLA_ENCODING_ENDING_INTEGER) {
        has_next_ = false;
    }
    return stored_value_;
}

template <>
FORCE_INLINE int64_t
GorillaDecoder<int64_t>::cache_next(common::ByteStream& in) {
    read_next(in);
    if (stored_value_ == GORILLA_ENCODING_ENDING_LONG) {
        has_next_ = false;
    }
    return stored_value_;
}

template <>
FORCE_INLINE int32_t GorillaDecoder<int32_t>::decode(common::ByteStream& in) {
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
FORCE_INLINE int64_t GorillaDecoder<int64_t>::decode(common::ByteStream& in) {
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
    int read_boolean(bool& ret_value, common::ByteStream& in) override;
    int read_int32(int32_t& ret_value, common::ByteStream& in) override;
    int read_int64(int64_t& ret_value, common::ByteStream& in) override;
    int read_float(float& ret_value, common::ByteStream& in) override;
    int read_double(double& ret_value, common::ByteStream& in) override;

    float decode(common::ByteStream& in) {
        int32_t value_int = GorillaDecoder<int32_t>::decode(in);
        return common::int_to_float(value_int);
    }

    int32_t cache_next(common::ByteStream& in) override {
        read_next(in);
        if (stored_value_ ==
            common::float_to_int(GORILLA_ENCODING_ENDING_FLOAT)) {
            has_next_ = false;
        }
        return stored_value_;
    }

    int read_batch_float(float* out, int capacity, int& actual,
                         common::ByteStream& in) override {
        int32_t ending = common::float_to_int(GORILLA_ENCODING_ENDING_FLOAT);
        actual = 0;
        while (actual < capacity && has_remaining(in)) {
            int32_t buf[129];
            int batch = std::min(129, capacity - actual);
            int buf_actual = 0;
            int ret = batch_decode_raw(buf, batch, buf_actual, ending, in);
            if (ret != common::E_OK) return ret;
            if (buf_actual == 0) break;
            for (int i = 0; i < buf_actual; i++) {
                out[actual + i] = common::int_to_float(buf[i]);
            }
            actual += buf_actual;
        }
        return common::E_OK;
    }

    int skip_float(int count, int& skipped,
                   common::ByteStream& in) override {
        int32_t ending = common::float_to_int(GORILLA_ENCODING_ENDING_FLOAT);
        return batch_skip_raw(count, skipped, ending, in);
    }
};

class DoubleGorillaDecoder : public GorillaDecoder<int64_t> {
   public:
    int read_boolean(bool& ret_value, common::ByteStream& in) override;
    int read_int32(int32_t& ret_value, common::ByteStream& in) override;
    int read_int64(int64_t& ret_value, common::ByteStream& in) override;
    int read_float(float& ret_value, common::ByteStream& in) override;
    int read_double(double& ret_value, common::ByteStream& in) override;

    double decode(common::ByteStream& in) {
        int64_t value_long = GorillaDecoder<int64_t>::decode(in);
        return common::long_to_double(value_long);
    }

    int64_t cache_next(common::ByteStream& in) override {
        read_next(in);
        if (stored_value_ ==
            common::double_to_long(GORILLA_ENCODING_ENDING_DOUBLE)) {
            has_next_ = false;
        }
        return stored_value_;
    }

    int read_batch_double(double* out, int capacity, int& actual,
                          common::ByteStream& in) override {
        int64_t ending = common::double_to_long(GORILLA_ENCODING_ENDING_DOUBLE);
        actual = 0;
        while (actual < capacity && has_remaining(in)) {
            int64_t buf[129];
            int batch = std::min(129, capacity - actual);
            int buf_actual = 0;
            int ret = batch_decode_raw(buf, batch, buf_actual, ending, in);
            if (ret != common::E_OK) return ret;
            if (buf_actual == 0) break;
            for (int i = 0; i < buf_actual; i++) {
                out[actual + i] = common::long_to_double(buf[i]);
            }
            actual += buf_actual;
        }
        return common::E_OK;
    }

    int skip_double(int count, int& skipped,
                    common::ByteStream& in) override {
        int64_t ending = common::double_to_long(GORILLA_ENCODING_ENDING_DOUBLE);
        return batch_skip_raw(count, skipped, ending, in);
    }
};

typedef GorillaDecoder<int32_t> IntGorillaDecoder;
typedef GorillaDecoder<int64_t> LongGorillaDecoder;

// ── IntGorillaDecoder batch/skip overrides ─────────────────────────────────
template <>
inline int GorillaDecoder<int32_t>::read_batch_int32(
    int32_t* out, int capacity, int& actual, common::ByteStream& in) {
    return batch_decode_raw(out, capacity, actual,
                            GORILLA_ENCODING_ENDING_INTEGER, in);
}
template <>
inline int GorillaDecoder<int32_t>::read_batch_int64(
    int64_t*, int, int& actual, common::ByteStream&) {
    actual = 0;
    return common::E_NOT_SUPPORT;
}
template <>
inline int GorillaDecoder<int32_t>::skip_int32(int count, int& skipped,
                                                common::ByteStream& in) {
    return batch_skip_raw(count, skipped,
                          GORILLA_ENCODING_ENDING_INTEGER, in);
}
template <>
inline int GorillaDecoder<int32_t>::skip_int64(int, int& skipped,
                                                common::ByteStream&) {
    skipped = 0;
    return common::E_NOT_SUPPORT;
}

// ── LongGorillaDecoder batch/skip overrides ───────────────────────────────
template <>
inline int GorillaDecoder<int64_t>::read_batch_int32(
    int32_t*, int, int& actual, common::ByteStream&) {
    actual = 0;
    return common::E_NOT_SUPPORT;
}
template <>
inline int GorillaDecoder<int64_t>::read_batch_int64(
    int64_t* out, int capacity, int& actual, common::ByteStream& in) {
    return batch_decode_raw(out, capacity, actual,
                            GORILLA_ENCODING_ENDING_LONG, in);
}
template <>
inline int GorillaDecoder<int64_t>::skip_int32(int, int& skipped,
                                                common::ByteStream&) {
    skipped = 0;
    return common::E_NOT_SUPPORT;
}
template <>
inline int GorillaDecoder<int64_t>::skip_int64(int count, int& skipped,
                                                common::ByteStream& in) {
    return batch_skip_raw(count, skipped,
                          GORILLA_ENCODING_ENDING_LONG, in);
}

// ── Scalar Decoder interface wrappers (unchanged) ─────────────────────────
template <>
FORCE_INLINE int IntGorillaDecoder::read_boolean(bool& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntGorillaDecoder::read_int32(int32_t& ret_value,
                                               common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int IntGorillaDecoder::read_int64(int64_t& ret_value,
                                               common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntGorillaDecoder::read_float(float& ret_value,
                                               common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntGorillaDecoder::read_double(double& ret_value,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int IntGorillaDecoder::read_String(common::String& ret_value,
                                                common::PageArena& pa,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_boolean(bool& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_int32(int32_t& ret_value,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_int64(int64_t& ret_value,
                                                common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_float(float& ret_value,
                                                common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_double(double& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
template <>
FORCE_INLINE int LongGorillaDecoder::read_String(common::String& ret_value,
                                                 common::PageArena& pa,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatGorillaDecoder::read_boolean(bool& ret_value,
                                                   common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatGorillaDecoder::read_int32(int32_t& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatGorillaDecoder::read_int64(int64_t& ret_value,
                                                 common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int FloatGorillaDecoder::read_float(float& ret_value,
                                                 common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}
FORCE_INLINE int FloatGorillaDecoder::read_double(double& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_boolean(bool& ret_value,
                                                    common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_int32(int32_t& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_int64(int64_t& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_float(float& ret_value,
                                                  common::ByteStream& in) {
    ASSERT(false);
    return common::E_NOT_SUPPORT;
}
FORCE_INLINE int DoubleGorillaDecoder::read_double(double& ret_value,
                                                   common::ByteStream& in) {
    ret_value = decode(in);
    return common::E_OK;
}

}  // end namespace storage
#endif  // ENCODING_GORILLA_DECODER_H
