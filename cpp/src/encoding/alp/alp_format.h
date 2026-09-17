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

#ifndef ENCODING_ALP_FORMAT_H
#define ENCODING_ALP_FORMAT_H

#include <stdint.h>
#include <string.h>

#include <limits>
#include <vector>

#include "alp_constants.h"

namespace storage {
namespace alp {

enum AlpStatus : int {
    ALP_OK = 0,
    ALP_INVALID_ARGUMENT = -1,
    ALP_MALFORMED = -2,
    ALP_BUFFER_TOO_SMALL = -3,
    ALP_TYPE_MISMATCH = -4
};

struct AlpPageHeader {
    uint8_t version;
    uint8_t data_type;
    uint16_t reserved;
    uint32_t value_count;
    uint32_t block_count;
    uint32_t reserved2;
};

struct AlpBlockHeader {
    uint8_t scheme;
    uint8_t factor;
    uint8_t exponent;
    uint8_t flags;
    uint32_t value_count;
    uint32_t exception_count;
    uint32_t body_bytes;
    int64_t for_base;
    uint8_t bit_width;
    uint8_t reserved[3];
};

static const uint32_t ALP_PAGE_HEADER_SIZE = 16;
static const uint32_t ALP_BLOCK_HEADER_SIZE = 32;

inline uint32_t AlpAlignUp(uint32_t value, uint32_t alignment) {
    if (alignment == 0) {
        return value;
    }
    const uint32_t remainder = value % alignment;
    return remainder == 0 ? value : value + (alignment - remainder);
}

inline void AlpAppendU8(std::vector<uint8_t>& out, uint8_t v) {
    out.push_back(v);
}

inline void AlpAppendU16(std::vector<uint8_t>& out, uint16_t v) {
    out.push_back(static_cast<uint8_t>(v & 0xFFu));
    out.push_back(static_cast<uint8_t>((v >> 8) & 0xFFu));
}

inline void AlpAppendU32(std::vector<uint8_t>& out, uint32_t v) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFFu));
    }
}

inline void AlpAppendU64(std::vector<uint8_t>& out, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<uint8_t>((v >> (8 * i)) & 0xFFu));
    }
}

inline bool AlpReadU8(const uint8_t* data, uint32_t size, uint32_t& pos,
                      uint8_t& out) {
    if (pos + 1 > size) {
        return false;
    }
    out = data[pos++];
    return true;
}

inline bool AlpReadU16(const uint8_t* data, uint32_t size, uint32_t& pos,
                       uint16_t& out) {
    if (pos + 2 > size) {
        return false;
    }
    out = static_cast<uint16_t>(data[pos]) |
          (static_cast<uint16_t>(data[pos + 1]) << 8);
    pos += 2;
    return true;
}

inline bool AlpReadU32(const uint8_t* data, uint32_t size, uint32_t& pos,
                       uint32_t& out) {
    if (pos + 4 > size) {
        return false;
    }
    out = static_cast<uint32_t>(data[pos]) |
          (static_cast<uint32_t>(data[pos + 1]) << 8) |
          (static_cast<uint32_t>(data[pos + 2]) << 16) |
          (static_cast<uint32_t>(data[pos + 3]) << 24);
    pos += 4;
    return true;
}

inline bool AlpReadU64(const uint8_t* data, uint32_t size, uint32_t& pos,
                       uint64_t& out) {
    if (pos + 8 > size) {
        return false;
    }
    out = 0;
    for (int i = 0; i < 8; ++i) {
        out |= static_cast<uint64_t>(data[pos + i]) << (8 * i);
    }
    pos += 8;
    return true;
}

inline void AlpWritePageHeader(std::vector<uint8_t>& out,
                               const AlpPageHeader& header) {
    AlpAppendU8(out, header.version);
    AlpAppendU8(out, header.data_type);
    AlpAppendU16(out, header.reserved);
    AlpAppendU32(out, header.value_count);
    AlpAppendU32(out, header.block_count);
    AlpAppendU32(out, header.reserved2);
}

inline bool AlpParsePageHeader(const uint8_t* data, uint32_t size,
                               uint32_t& pos, AlpPageHeader& header) {
    return AlpReadU8(data, size, pos, header.version) &&
           AlpReadU8(data, size, pos, header.data_type) &&
           AlpReadU16(data, size, pos, header.reserved) &&
           AlpReadU32(data, size, pos, header.value_count) &&
           AlpReadU32(data, size, pos, header.block_count) &&
           AlpReadU32(data, size, pos, header.reserved2);
}

inline void AlpWriteBlockHeader(std::vector<uint8_t>& out,
                                const AlpBlockHeader& header) {
    AlpAppendU8(out, header.scheme);
    AlpAppendU8(out, header.factor);
    AlpAppendU8(out, header.exponent);
    AlpAppendU8(out, header.flags);
    AlpAppendU32(out, header.value_count);
    AlpAppendU32(out, header.exception_count);
    AlpAppendU32(out, header.body_bytes);
    AlpAppendU64(out, static_cast<uint64_t>(header.for_base));
    AlpAppendU8(out, header.bit_width);
    AlpAppendU8(out, header.reserved[0]);
    AlpAppendU8(out, header.reserved[1]);
    AlpAppendU8(out, header.reserved[2]);
}

inline bool AlpParseBlockHeader(const uint8_t* data, uint32_t size,
                                uint32_t& pos, AlpBlockHeader& header) {
    uint64_t base = 0;
    if (!AlpReadU8(data, size, pos, header.scheme) ||
        !AlpReadU8(data, size, pos, header.factor) ||
        !AlpReadU8(data, size, pos, header.exponent) ||
        !AlpReadU8(data, size, pos, header.flags) ||
        !AlpReadU32(data, size, pos, header.value_count) ||
        !AlpReadU32(data, size, pos, header.exception_count) ||
        !AlpReadU32(data, size, pos, header.body_bytes) ||
        !AlpReadU64(data, size, pos, base) ||
        !AlpReadU8(data, size, pos, header.bit_width) ||
        !AlpReadU8(data, size, pos, header.reserved[0]) ||
        !AlpReadU8(data, size, pos, header.reserved[1]) ||
        !AlpReadU8(data, size, pos, header.reserved[2])) {
        return false;
    }
    header.for_base = static_cast<int64_t>(base);
    return true;
}

}  // namespace alp
}  // namespace storage

#endif  // ENCODING_ALP_FORMAT_H
