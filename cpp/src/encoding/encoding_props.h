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

#ifndef ENCODING_ENCODING_PROPS_H
#define ENCODING_ENCODING_PROPS_H

#include <cstdint>
#include <map>
#include <string>

namespace storage {
namespace detail {

// Integer.parseInt uses Character.digit(char, 10): BMP decimal digits are
// accepted, but supplementary digits (UTF-16 surrogate pairs) are not.
inline int precision_digit(const std::string& text, size_t& offset) {
    const auto first = static_cast<unsigned char>(text[offset++]);
    if (first < 0x80) {
        return first >= '0' && first <= '9' ? first - '0' : -1;
    }
    int trailing;
    uint32_t code;
    if (first >= 0xC2 && first <= 0xDF) {
        trailing = 1;
        code = first & 0x1F;
    } else if (first >= 0xE0 && first <= 0xEF) {
        trailing = 2;
        code = first & 0x0F;
    } else {
        return -1;
    }
    if (offset + trailing > text.size()) return -1;
    for (int i = 0; i < trailing; ++i) {
        const auto next = static_cast<unsigned char>(text[offset++]);
        if ((next & 0xC0) != 0x80) return -1;
        code = (code << 6) | (next & 0x3F);
    }
    if ((trailing == 2 && code < 0x800) || (code >= 0xD800 && code <= 0xDFFF))
        return -1;
    static const uint16_t zeroes[] = {
        0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6, 0x0B66, 0x0BE6,
        0x0C66, 0x0CE6, 0x0D66, 0x0DE6, 0x0E50, 0x0ED0, 0x0F20, 0x1040, 0x1090,
        0x17E0, 0x1810, 0x1946, 0x19D0, 0x1A80, 0x1A90, 0x1B50, 0x1BB0, 0x1C40,
        0x1C50, 0xA620, 0xA8D0, 0xA900, 0xA9D0, 0xA9F0, 0xAA50, 0xABF0, 0xFF10};
    for (uint16_t zero : zeroes) {
        if (code >= zero && code <= zero + 9U) return code - zero;
    }
    return -1;
}

inline int max_point_number(const std::map<std::string, std::string>& props) {
    const auto property = props.find("max_point_number");
    if (property == props.end()) return 2;
    const auto& text = property->second;
    size_t offset = 0;
    bool negative = false;
    if (!text.empty() && (text[0] == '-' || text[0] == '+')) {
        negative = text[0] == '-';
        ++offset;
    }
    // A fresh Java Ts2Diff builder retains zero after NumberFormatException.
    if (offset == text.size()) return 0;
    const uint32_t limit = negative ? 2147483648U : 2147483647U;
    uint32_t value = 0;
    while (offset < text.size()) {
        const int digit = precision_digit(text, offset);
        if (digit < 0 || value > (limit - digit) / 10) return 0;
        value = value * 10 + digit;
    }
    // Negative precision falls back to Java's default floatPrecision (2).
    return negative && value != 0 ? 2 : static_cast<int>(value);
}

}  // namespace detail
}  // namespace storage

#endif  // ENCODING_ENCODING_PROPS_H
