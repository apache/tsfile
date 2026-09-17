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

#ifndef ENCODING_ALP_SCALAR_H
#define ENCODING_ALP_SCALAR_H

#include <math.h>
#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <limits>
#include <vector>

#include "alp_format.h"
#include "alp_traits.h"
#ifdef ENABLE_SIMD
#include "alp_simde.h"
#endif

namespace storage {
namespace alp {

template <typename Unsigned>
inline uint8_t AlpComputeBitWidth(Unsigned value) {
    uint8_t width = 0;
    while (value > 0) {
        ++width;
        value >>= 1;
    }
    return width;
}

template <typename Unsigned>
inline void AlpPackBits(const Unsigned* values, uint32_t count,
                        uint8_t bit_width, std::vector<uint8_t>& body) {
    body.clear();
    if (bit_width == 0 || count == 0) {
        return;
    }
    const uint32_t raw_bytes =
        static_cast<uint32_t>((static_cast<uint64_t>(count) * bit_width + 7) /
                              8);
    body.assign(AlpAlignUp(raw_bytes + 16, ALP_BODY_ALIGNMENT), 0);

    if (bit_width == sizeof(Unsigned) * 8) {
        for (uint32_t i = 0; i < count; ++i) {
            const Unsigned value = values[i];
            for (uint32_t b = 0; b < sizeof(Unsigned); ++b) {
                body[i * sizeof(Unsigned) + b] =
                    static_cast<uint8_t>((value >> (8 * b)) & 0xFFu);
            }
        }
        return;
    }

    // Word-based packing: build a 128-bit window for each value and OR it
    // into two adjacent 64-bit words. This removes the per-bit loop while
    // keeping the byte layout identical to the scalar reference.
    const uint64_t mask =
        bit_width >= 64 ? ~static_cast<uint64_t>(0)
                        : ((static_cast<uint64_t>(1) << bit_width) - 1);
    uint64_t bit_pos = 0;
    for (uint32_t i = 0; i < count; ++i) {
        const uint64_t value = static_cast<uint64_t>(values[i]) & mask;
        const uint32_t byte_pos = static_cast<uint32_t>(bit_pos >> 3);
        const uint32_t bit_offset = static_cast<uint32_t>(bit_pos & 7u);
        const uint64_t low = value << bit_offset;
        const uint64_t high =
            bit_offset == 0 ? 0 : (value >> (64 - bit_offset));
        uint64_t dst = 0;
        std::memcpy(&dst, body.data() + byte_pos, sizeof(dst));
        dst |= low;
        std::memcpy(body.data() + byte_pos, &dst, sizeof(dst));
        if (high != 0) {
            std::memcpy(&dst, body.data() + byte_pos + sizeof(dst), sizeof(dst));
            dst |= high;
            std::memcpy(body.data() + byte_pos + sizeof(dst), &dst, sizeof(dst));
        }
        bit_pos += bit_width;
    }
}

template <typename Unsigned>
inline bool AlpUnpackBits(const uint8_t* body, uint32_t body_bytes,
                          uint32_t count, uint8_t bit_width,
                          std::vector<Unsigned>& out) {
    out.assign(count, 0);
    if (bit_width == 0 || count == 0) {
        return true;
    }
    const uint32_t raw_bytes =
        static_cast<uint32_t>((static_cast<uint64_t>(count) * bit_width + 7) /
                              8);
    if (body_bytes < raw_bytes) {
        return false;
    }

    if (bit_width == sizeof(Unsigned) * 8) {
        for (uint32_t i = 0; i < count; ++i) {
            Unsigned value = 0;
            for (uint32_t b = 0; b < sizeof(Unsigned); ++b) {
                value |= static_cast<Unsigned>(body[i * sizeof(Unsigned) + b])
                         << (8 * b);
            }
            out[i] = value;
        }
        return true;
    }

    // The body is padded with at least 16 zero bytes past the logical end.
    // The SIMD path handles groups of four; the scalar tail uses two
    // overlapping 64-bit loads and remains portable C++11.
    uint32_t i = 0;
#ifdef ENABLE_SIMD
    const uint32_t simd_count = count & ~static_cast<uint32_t>(3);
    if (simd_count > 0 &&
        !AlpSimdUnpackBits(body, simd_count, bit_width, &out[0])) {
        return false;
    }
    i = simd_count;
#endif
    const uint64_t mask =
        bit_width >= 64 ? ~static_cast<uint64_t>(0)
                        : ((static_cast<uint64_t>(1) << bit_width) - 1);
    uint64_t bit_pos = static_cast<uint64_t>(i) * bit_width;
    for (; i < count; ++i) {
        const uint32_t byte_pos = static_cast<uint32_t>(bit_pos >> 3);
        const uint32_t bit_offset = static_cast<uint32_t>(bit_pos & 7u);
        uint64_t low = 0;
        uint64_t high = 0;
        std::memcpy(&low, body + byte_pos, sizeof(low));
        std::memcpy(&high, body + byte_pos + sizeof(low), sizeof(high));
        uint64_t value = low >> bit_offset;
        if (bit_offset != 0) {
            value |= high << (64 - bit_offset);
        }
        out[i] = static_cast<Unsigned>(value & mask);
        bit_pos += bit_width;
    }
    return true;
}

template <typename T>
inline uint64_t AlpEstimatedBodyBytes(uint32_t count, uint8_t bit_width) {
    if (bit_width == 0) {
        return 0;
    }
    const uint64_t raw_bytes =
        (static_cast<uint64_t>(count) * bit_width + 7) / 8;
    return AlpAlignUp(static_cast<uint32_t>(raw_bytes + 16), ALP_BODY_ALIGNMENT);
}


template <typename T>
inline AlpStatus AlpEncodeValuesScalar(
    const T* values, uint32_t count, uint8_t factor, uint8_t exponent,
    typename AlpTypeTraits<T>::Encoded* encoded, uint8_t* bitmap, T* exceptions,
    uint32_t* exception_count) {
    typedef AlpTypeTraits<T> Traits;
    typedef typename Traits::Encoded Encoded;
    uint32_t ex_count = 0;
    for (uint32_t i = 0; i < count; ++i) {
        Encoded value = 0;
        if (!Traits::EncodeValue(values[i], factor, exponent, &value) ||
            !Traits::BitwiseEqual(Traits::DecodeValue(value, factor, exponent),
                                  values[i])) {
            bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            exceptions[ex_count++] = values[i];
            encoded[i] = 0;
        } else {
            encoded[i] = value;
        }
    }
    *exception_count = ex_count;
    return ALP_OK;
}

template <typename T>
inline AlpStatus AlpDecodeValuesScalar(
    const typename AlpTypeTraits<T>::Encoded* encoded, uint32_t count,
    uint8_t factor, uint8_t exponent, const uint8_t* bitmap,
    const T* exceptions, uint32_t exception_count, T* out) {
    typedef AlpTypeTraits<T> Traits;
    uint32_t exception_index = 0;
    for (uint32_t i = 0; i < count; ++i) {
        T value = Traits::DecodeValue(encoded[i], factor, exponent);
        if (bitmap != NULL &&
            (bitmap[i >> 3] & static_cast<uint8_t>(1u << (i & 7u))) != 0) {
            if (exception_index >= exception_count) {
                return ALP_MALFORMED;
            }
            value = exceptions[exception_index++];
        }
        out[i] = value;
    }
    return exception_index == exception_count ? ALP_OK : ALP_MALFORMED;
}

template <typename T>
inline AlpStatus AlpEncodeValues(
    const T* values, uint32_t count, uint8_t factor, uint8_t exponent,
    typename AlpTypeTraits<T>::Encoded* encoded, uint8_t* bitmap, T* exceptions,
    uint32_t* exception_count) {
#ifdef ENABLE_SIMD
    return AlpSimdEncodeValues(values, count, factor, exponent, encoded, bitmap,
                               exceptions, exception_count);
#else
    return AlpEncodeValuesScalar(values, count, factor, exponent, encoded,
                                 bitmap, exceptions, exception_count);
#endif
}

template <typename T>
inline AlpStatus AlpDecodeValues(
    const typename AlpTypeTraits<T>::Encoded* encoded, uint32_t count,
    uint8_t factor, uint8_t exponent, const uint8_t* bitmap,
    const T* exceptions, uint32_t exception_count, T* out) {
#ifdef ENABLE_SIMD
    return AlpSimdDecodeValues(encoded, count, factor, exponent, bitmap,
                               exceptions, exception_count, out);
#else
    return AlpDecodeValuesScalar(encoded, count, factor, exponent, bitmap,
                                 exceptions, exception_count, out);
#endif
}

template <typename T>
inline uint64_t AlpEstimateCandidate(const T* samples, uint32_t sample_count,
                                     uint32_t block_count, uint8_t factor,
                                     uint8_t exponent, bool verify) {
    typedef AlpTypeTraits<T> Traits;
    typedef typename Traits::Encoded Encoded;
    typedef typename Traits::Unsigned Unsigned;

    uint32_t sample_exceptions = 0;
    bool has_value = false;
    Encoded min_value = 0;
    Encoded max_value = 0;
    for (uint32_t i = 0; i < sample_count; ++i) {
        Encoded encoded = 0;
        if (!Traits::EncodeValue(samples[i], factor, exponent, &encoded)) {
            ++sample_exceptions;
            continue;
        }
        if (verify &&
            !Traits::BitwiseEqual(Traits::DecodeValue(encoded, factor, exponent),
                                  samples[i])) {
            ++sample_exceptions;
            continue;
        }
        if (!has_value) {
            min_value = encoded;
            max_value = encoded;
            has_value = true;
        } else {
            min_value = std::min(min_value, encoded);
            max_value = std::max(max_value, encoded);
        }
    }

    const uint64_t exception_estimate =
        sample_count == 0
            ? block_count
            : static_cast<uint64_t>(sample_exceptions) * block_count /
                  sample_count;
    uint8_t bit_width = 0;
    if (has_value) {
        const Unsigned adjusted_max =
            static_cast<Unsigned>(max_value) - static_cast<Unsigned>(min_value);
        bit_width = AlpComputeBitWidth(adjusted_max);
    }
    const uint64_t body_bytes = AlpEstimatedBodyBytes<T>(block_count, bit_width);
    const uint64_t bitmap_bytes = (static_cast<uint64_t>(block_count) + 7) / 8;
    return body_bytes + bitmap_bytes +
           exception_estimate * Traits::ValueSize();
}

template <typename T>
inline bool AlpChooseFactorExponent(const T* values, uint32_t count,
                                    uint8_t& best_factor,
                                    uint8_t& best_exponent) {
    typedef AlpTypeTraits<T> Traits;

    const uint32_t sample_count = std::min(count, ALP_SAMPLE_SIZE);
    std::vector<T> samples(sample_count);
    for (uint32_t i = 0; i < sample_count; ++i) {
        const uint32_t index =
            sample_count == count ? i : (i * count) / sample_count;
        samples[i] = values[index];
    }

    // Stage 1: cheap screen over a small prefix of the sample. The decode
    // round-trip check is skipped here; stage 2 performs the exact check on
    // the shortlisted candidates.
    static const uint32_t kStage1Samples = 8;
    static const uint32_t kShortlist = 8;
    const uint32_t stage1_count = std::min(sample_count, kStage1Samples);
    uint64_t top_estimate[kShortlist];
    uint8_t top_factor[kShortlist];
    uint8_t top_exponent[kShortlist];
    uint32_t top_count = 0;

    for (uint8_t e = 0; e <= Traits::MaxExponent(); ++e) {
        for (uint8_t f = 0; f <= e; ++f) {
            const uint64_t estimate = AlpEstimateCandidate(
                samples.empty() ? NULL : &samples[0], stage1_count, count, f,
                e, true);
            int pos = static_cast<int>(top_count);
            for (uint32_t i = 0; i < top_count; ++i) {
                if (estimate < top_estimate[i] ||
                    (estimate == top_estimate[i] &&
                     (e > top_exponent[i] ||
                      (e == top_exponent[i] && f > top_factor[i])))) {
                    pos = static_cast<int>(i);
                    break;
                }
            }
            if (pos < static_cast<int>(kShortlist)) {
                const uint32_t last =
                    std::min(top_count, kShortlist - 1);
                for (uint32_t j = last; j > static_cast<uint32_t>(pos); --j) {
                    top_estimate[j] = top_estimate[j - 1];
                    top_factor[j] = top_factor[j - 1];
                    top_exponent[j] = top_exponent[j - 1];
                }
                top_estimate[pos] = estimate;
                top_factor[pos] = f;
                top_exponent[pos] = e;
                if (top_count < kShortlist) {
                    ++top_count;
                }
            }
        }
    }

    if (top_count == 0) {
        return false;
    }

    // Stage 2: exact round-trip check on the shortlisted candidates using the
    // full sample.
    uint64_t best_size = std::numeric_limits<uint64_t>::max();
    bool found = false;
    for (uint32_t i = 0; i < top_count; ++i) {
        const uint8_t f = top_factor[i];
        const uint8_t e = top_exponent[i];
        const uint64_t estimate = AlpEstimateCandidate(
            samples.empty() ? NULL : &samples[0], sample_count, count, f, e,
            true);
        if (!found || estimate < best_size ||
            (estimate == best_size &&
             (e > best_exponent ||
              (e == best_exponent && f > best_factor)))) {
            found = true;
            best_size = estimate;
            best_factor = f;
            best_exponent = e;
        }
    }
    return found;
}

template <typename T>
inline AlpStatus AlpEncodeBlock(const T* values, uint32_t count,
                                std::vector<uint8_t>& out) {
    typedef AlpTypeTraits<T> Traits;
    typedef typename Traits::Encoded Encoded;
    typedef typename Traits::Unsigned Unsigned;

    if (values == NULL || count == 0 || count > ALP_BLOCK_SIZE) {
        return ALP_INVALID_ARGUMENT;
    }

    uint8_t factor = 0;
    uint8_t exponent = 0;
    if (!AlpChooseFactorExponent(values, count, factor, exponent)) {
        return ALP_MALFORMED;
    }

    std::vector<Encoded> encoded(count, 0);
    std::vector<uint8_t> bitmap((count + 7) / 8, 0);
    std::vector<T> exceptions(count);
    uint32_t exception_count = 0;
    AlpStatus encode_status =
        AlpEncodeValues(values, count, factor, exponent, &encoded[0],
                        &bitmap[0], &exceptions[0], &exception_count);
    if (encode_status != ALP_OK) {
        return encode_status;
    }
    exceptions.resize(exception_count);

    bool has_value = false;
    Encoded min_value = 0;
    Encoded max_value = 0;
    for (uint32_t i = 0; i < count; ++i) {
        if ((bitmap[i >> 3] & static_cast<uint8_t>(1u << (i & 7u))) != 0) {
            continue;
        }
        const Encoded value = encoded[i];
        if (!has_value) {
            min_value = value;
            max_value = value;
            has_value = true;
        } else {
            min_value = std::min(min_value, value);
            max_value = std::max(max_value, value);
        }
    }

    const Encoded for_base = has_value ? min_value : 0;
    const Unsigned adjusted_max =
        has_value ? static_cast<Unsigned>(max_value) -
                        static_cast<Unsigned>(min_value)
                  : 0;
    const uint8_t bit_width = AlpComputeBitWidth(adjusted_max);

    std::vector<Unsigned> adjusted(count, 0);
    for (uint32_t i = 0; i < count; ++i) {
        if ((bitmap[i >> 3] & static_cast<uint8_t>(1u << (i & 7u))) == 0) {
            adjusted[i] = static_cast<Unsigned>(encoded[i]) -
                          static_cast<Unsigned>(for_base);
        }
    }

    std::vector<uint8_t> body;
    AlpPackBits(adjusted.empty() ? NULL : &adjusted[0], count, bit_width, body);

    const uint64_t bitmap_bytes = exception_count == 0 ? 0 : bitmap.size();
    const uint64_t alp_payload_bytes = bitmap_bytes +
                                       static_cast<uint64_t>(exceptions.size()) *
                                           Traits::ValueSize() +
                                       body.size();
    const uint64_t plain_payload_bytes =
        static_cast<uint64_t>(count) * Traits::ValueSize();
    const bool use_plain = !has_value || alp_payload_bytes >= plain_payload_bytes;

    AlpBlockHeader header;
    memset(&header, 0, sizeof(header));
    header.value_count = count;
    header.factor = factor;
    header.exponent = exponent;
    header.bit_width = bit_width;
    header.for_base = static_cast<int64_t>(for_base);

    if (use_plain) {
        header.scheme = ALP_SCHEME_PLAIN;
        header.exception_count = 0;
        header.body_bytes = static_cast<uint32_t>(plain_payload_bytes);
        header.bit_width = 0;
        header.for_base = 0;
        AlpWriteBlockHeader(out, header);
        for (uint32_t i = 0; i < count; ++i) {
            Traits::AppendValue(out, values[i]);
        }
        return ALP_OK;
    }

    header.scheme = ALP_SCHEME_ALP;
    header.flags = exception_count == 0 ? 0 : 1;
    header.exception_count = exception_count;
    header.body_bytes = static_cast<uint32_t>(body.size());
    AlpWriteBlockHeader(out, header);

    if (exception_count > 0) {
        out.insert(out.end(), bitmap.begin(), bitmap.end());
        for (uint32_t i = 0; i < exceptions.size(); ++i) {
            Traits::AppendValue(out, exceptions[i]);
        }
    }
    out.insert(out.end(), body.begin(), body.end());
    return ALP_OK;
}

template <typename T>
inline AlpStatus AlpDecodeBlock(const uint8_t* data, uint32_t size,
                                uint32_t& pos, std::vector<T>& out) {
    typedef AlpTypeTraits<T> Traits;
    typedef typename Traits::Encoded Encoded;
    typedef typename Traits::Unsigned Unsigned;

    AlpBlockHeader header;
    if (!AlpParseBlockHeader(data, size, pos, header)) {
        return ALP_MALFORMED;
    }
    if (header.value_count == 0 || header.value_count > ALP_BLOCK_SIZE ||
        header.exception_count > header.value_count ||
        header.bit_width > Traits::MaxBitWidth()) {
        return ALP_MALFORMED;
    }

    if (header.scheme == ALP_SCHEME_PLAIN) {
        if (header.body_bytes !=
            static_cast<uint32_t>(header.value_count * Traits::ValueSize())) {
            return ALP_MALFORMED;
        }
        for (uint32_t i = 0; i < header.value_count; ++i) {
            T value;
            if (!Traits::ReadValue(data, size, pos, &value)) {
                return ALP_BUFFER_TOO_SMALL;
            }
            out.push_back(value);
        }
        return ALP_OK;
    }

    if (header.scheme != ALP_SCHEME_ALP) {
        return ALP_MALFORMED;
    }

    std::vector<uint8_t> bitmap;
    if (header.exception_count > 0) {
        const uint32_t bitmap_bytes = (header.value_count + 7) / 8;
        if (pos + bitmap_bytes > size) {
            return ALP_BUFFER_TOO_SMALL;
        }
        bitmap.assign(data + pos, data + pos + bitmap_bytes);
        pos += bitmap_bytes;
    }

    std::vector<T> exceptions;
    exceptions.reserve(header.exception_count);
    for (uint32_t i = 0; i < header.exception_count; ++i) {
        T value;
        if (!Traits::ReadValue(data, size, pos, &value)) {
            return ALP_BUFFER_TOO_SMALL;
        }
        exceptions.push_back(value);
    }

    const uint32_t expected_raw_bytes =
        header.bit_width == 0
            ? 0
            : static_cast<uint32_t>(
                  (static_cast<uint64_t>(header.value_count) *
                       header.bit_width +
                   7) /
                  8);
    const uint32_t expected_body_bytes =
        header.bit_width == 0
            ? 0
            : AlpAlignUp(expected_raw_bytes + 16, ALP_BODY_ALIGNMENT);
    if (header.body_bytes != expected_body_bytes || pos + header.body_bytes > size) {
        return ALP_MALFORMED;
    }

    std::vector<Unsigned> adjusted;
    if (!AlpUnpackBits(data + pos, header.body_bytes, header.value_count,
                       header.bit_width, adjusted)) {
        return ALP_MALFORMED;
    }
    pos += header.body_bytes;

    std::vector<Encoded> encoded(header.value_count);
    for (uint32_t i = 0; i < header.value_count; ++i) {
        const Unsigned raw =
            adjusted[i] + static_cast<Unsigned>(header.for_base);
        std::memcpy(&encoded[i], &raw, sizeof(encoded[i]));
    }

    std::vector<T> decoded(header.value_count);
    const uint8_t* bitmap_ptr =
        header.exception_count > 0 ? (bitmap.empty() ? NULL : &bitmap[0])
                                   : NULL;
    const T* exception_ptr =
        header.exception_count > 0
            ? (exceptions.empty() ? NULL : &exceptions[0])
            : NULL;
    const AlpStatus decode_status = AlpDecodeValues(
        &encoded[0], header.value_count, header.factor, header.exponent,
        bitmap_ptr, exception_ptr, header.exception_count, &decoded[0]);
    if (decode_status != ALP_OK) {
        return decode_status;
    }
    out.insert(out.end(), decoded.begin(), decoded.end());
    return ALP_OK;
}

template <typename T>
inline AlpStatus AlpEncodePage(const T* values, uint32_t count,
                               std::vector<uint8_t>& out) {
    out.clear();
    if (values == NULL && count != 0) {
        return ALP_INVALID_ARGUMENT;
    }
    const uint32_t block_count =
        count == 0 ? 0 : (count + ALP_BLOCK_SIZE - 1) / ALP_BLOCK_SIZE;

    AlpPageHeader page;
    page.version = ALP_FORMAT_VERSION;
    page.data_type = AlpTypeTraits<T>::DataType();
    page.reserved = 0;
    page.value_count = count;
    page.block_count = block_count;
    page.reserved2 = 0;
    AlpWritePageHeader(out, page);

    uint32_t offset = 0;
    for (uint32_t b = 0; b < block_count; ++b) {
        const uint32_t n = std::min(ALP_BLOCK_SIZE, count - offset);
        const AlpStatus status = AlpEncodeBlock(values + offset, n, out);
        if (status != ALP_OK) {
            return status;
        }
        offset += n;
    }
    return ALP_OK;
}

template <typename T>
inline AlpStatus AlpDecodePage(const uint8_t* data, uint32_t size,
                               std::vector<T>& out) {
    out.clear();
    if (data == NULL && size != 0) {
        return ALP_INVALID_ARGUMENT;
    }
    uint32_t pos = 0;
    AlpPageHeader page;
    if (!AlpParsePageHeader(data, size, pos, page)) {
        return ALP_MALFORMED;
    }
    if (page.version != ALP_FORMAT_VERSION ||
        page.data_type != AlpTypeTraits<T>::DataType() || page.reserved != 0 ||
        page.reserved2 != 0) {
        return ALP_MALFORMED;
    }

    out.reserve(page.value_count);
    for (uint32_t b = 0; b < page.block_count; ++b) {
        const AlpStatus status = AlpDecodeBlock(data, size, pos, out);
        if (status != ALP_OK) {
            return status;
        }
    }
    if (out.size() != page.value_count || pos != size) {
        return ALP_MALFORMED;
    }
    return ALP_OK;
}

}  // namespace alp
}  // namespace storage

#endif  // ENCODING_ALP_SCALAR_H
