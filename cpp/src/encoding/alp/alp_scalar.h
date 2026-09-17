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
    body.assign(AlpAlignUp(raw_bytes, ALP_BODY_ALIGNMENT), 0);

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

    uint64_t bit_pos = 0;
    for (uint32_t i = 0; i < count; ++i) {
        const Unsigned value = values[i];
        for (uint8_t b = 0; b < bit_width; ++b) {
            if ((value >> b) & static_cast<Unsigned>(1)) {
                body[static_cast<uint32_t>(bit_pos >> 3)] |= static_cast<uint8_t>(
                    1u << (bit_pos & 7u));
            }
            ++bit_pos;
        }
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

    uint64_t bit_pos = 0;
    for (uint32_t i = 0; i < count; ++i) {
        Unsigned value = 0;
        for (uint8_t b = 0; b < bit_width; ++b) {
            if (body[static_cast<uint32_t>(bit_pos >> 3)] &
                static_cast<uint8_t>(1u << (bit_pos & 7u))) {
                value |= static_cast<Unsigned>(1) << b;
            }
            ++bit_pos;
        }
        out[i] = value;
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
    return AlpAlignUp(static_cast<uint32_t>(raw_bytes), ALP_BODY_ALIGNMENT);
}

template <typename T>
inline bool AlpChooseFactorExponent(const T* values, uint32_t count,
                                    uint8_t& best_factor,
                                    uint8_t& best_exponent) {
    typedef AlpTypeTraits<T> Traits;
    typedef typename Traits::Encoded Encoded;
    typedef typename Traits::Unsigned Unsigned;

    const uint32_t sample_count = std::min(count, ALP_SAMPLE_SIZE);
    std::vector<T> samples(sample_count);
    for (uint32_t i = 0; i < sample_count; ++i) {
        const uint32_t index =
            sample_count == count ? i : (i * count) / sample_count;
        samples[i] = values[index];
    }

    uint64_t best_size = std::numeric_limits<uint64_t>::max();
    bool found = false;
    for (uint8_t e = 0; e <= Traits::MaxExponent(); ++e) {
        for (uint8_t f = 0; f <= e; ++f) {
            uint32_t sample_exceptions = 0;
            bool has_value = false;
            Encoded min_value = 0;
            Encoded max_value = 0;
            for (uint32_t i = 0; i < sample_count; ++i) {
                Encoded encoded = 0;
                if (!Traits::EncodeValue(samples[i], f, e, &encoded) ||
                    !Traits::BitwiseEqual(
                        Traits::DecodeValue(encoded, f, e), samples[i])) {
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

            uint64_t exception_estimate =
                static_cast<uint64_t>(sample_exceptions) * count /
                sample_count;
            uint8_t bit_width = 0;
            if (has_value) {
                const Unsigned adjusted_max =
                    static_cast<Unsigned>(max_value) -
                    static_cast<Unsigned>(min_value);
                bit_width = AlpComputeBitWidth(adjusted_max);
            }
            const uint64_t body_bytes = AlpEstimatedBodyBytes<T>(count, bit_width);
            const uint64_t bitmap_bytes = (static_cast<uint64_t>(count) + 7) / 8;
            const uint64_t estimate =
                body_bytes + bitmap_bytes +
                exception_estimate * Traits::ValueSize();
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
    std::vector<T> exceptions;
    exceptions.reserve(count / 8 + 1);

    bool has_value = false;
    Encoded min_value = 0;
    Encoded max_value = 0;
    uint32_t exception_count = 0;
    for (uint32_t i = 0; i < count; ++i) {
        Encoded value = 0;
        if (!Traits::EncodeValue(values[i], factor, exponent, &value) ||
            !Traits::BitwiseEqual(Traits::DecodeValue(value, factor, exponent),
                                  values[i])) {
            bitmap[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            exceptions.push_back(values[i]);
            ++exception_count;
            continue;
        }
        encoded[i] = value;
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
            : AlpAlignUp(expected_raw_bytes, ALP_BODY_ALIGNMENT);
    if (header.body_bytes != expected_body_bytes || pos + header.body_bytes > size) {
        return ALP_MALFORMED;
    }

    std::vector<Unsigned> adjusted;
    if (!AlpUnpackBits(data + pos, header.body_bytes, header.value_count,
                       header.bit_width, adjusted)) {
        return ALP_MALFORMED;
    }
    pos += header.body_bytes;

    uint32_t exception_index = 0;
    for (uint32_t i = 0; i < header.value_count; ++i) {
        const Unsigned raw =
            adjusted[i] + static_cast<Unsigned>(header.for_base);
        Encoded encoded = 0;
        std::memcpy(&encoded, &raw, sizeof(encoded));
        T value = Traits::DecodeValue(encoded, header.factor, header.exponent);
        if (header.exception_count > 0 &&
            (bitmap[i >> 3] & static_cast<uint8_t>(1u << (i & 7u))) != 0) {
            if (exception_index >= exceptions.size()) {
                return ALP_MALFORMED;
            }
            value = exceptions[exception_index++];
        }
        out.push_back(value);
    }
    if (exception_index != header.exception_count) {
        return ALP_MALFORMED;
    }
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
