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

#ifndef ENCODING_ALP_TRAITS_H
#define ENCODING_ALP_TRAITS_H

#include <math.h>
#include <stdint.h>

#include <cstring>

#include "alp_format.h"

namespace storage {
namespace alp {

template <typename T>
struct AlpTypeTraits;

template <>
struct AlpTypeTraits<float> {
    typedef int32_t Encoded;
    typedef uint32_t Unsigned;

    static uint8_t DataType() { return ALP_FLOAT; }
    static uint8_t MaxExponent() { return ALP_FLOAT_MAX_EXPONENT; }
    static uint8_t MaxBitWidth() { return 32; }
    static uint32_t ValueSize() { return 4; }

    static bool EncodeValue(float value, uint8_t factor, uint8_t exponent,
                            int32_t* out) {
        if (!std::isfinite(value) || (value == 0.0f && std::signbit(value))) {
            return false;
        }
        const float scaled =
            value * ALP_FLOAT_EXP[exponent] * ALP_FLOAT_FRAC[factor];
        if (!std::isfinite(scaled) || scaled < ALP_FLOAT_ENCODING_LOWER_LIMIT ||
            scaled > ALP_FLOAT_ENCODING_UPPER_LIMIT) {
            return false;
        }
        const float rounded = scaled + ALP_FLOAT_MAGIC - ALP_FLOAT_MAGIC;
        if (!std::isfinite(rounded) ||
            rounded < ALP_FLOAT_ENCODING_LOWER_LIMIT ||
            rounded > ALP_FLOAT_ENCODING_UPPER_LIMIT) {
            return false;
        }
        *out = static_cast<int32_t>(rounded);
        return true;
    }

    static float DecodeValue(int32_t encoded, uint8_t factor,
                             uint8_t exponent) {
        const float factor_value = static_cast<float>(ALP_FACT[factor]);
        return static_cast<float>(encoded) * factor_value *
               ALP_FLOAT_FRAC[exponent];
    }

    static void AppendValue(std::vector<uint8_t>& out, float value) {
        uint32_t bits = 0;
        std::memcpy(&bits, &value, sizeof(bits));
        AlpAppendU32(out, bits);
    }

    static bool ReadValue(const uint8_t* data, uint32_t size, uint32_t& pos,
                          float* value) {
        uint32_t bits = 0;
        if (!AlpReadU32(data, size, pos, bits)) {
            return false;
        }
        std::memcpy(value, &bits, sizeof(bits));
        return true;
    }

    static bool BitwiseEqual(float lhs, float rhs) {
        return std::memcmp(&lhs, &rhs, sizeof(float)) == 0;
    }
};

template <>
struct AlpTypeTraits<double> {
    typedef int64_t Encoded;
    typedef uint64_t Unsigned;

    static uint8_t DataType() { return ALP_DOUBLE; }
    static uint8_t MaxExponent() { return ALP_DOUBLE_MAX_EXPONENT; }
    static uint8_t MaxBitWidth() { return 64; }
    static uint32_t ValueSize() { return 8; }

    static bool EncodeValue(double value, uint8_t factor, uint8_t exponent,
                            int64_t* out) {
        if (!std::isfinite(value) || (value == 0.0 && std::signbit(value))) {
            return false;
        }
        const double scaled =
            value * ALP_DOUBLE_EXP[exponent] * ALP_DOUBLE_FRAC[factor];
        if (!std::isfinite(scaled) ||
            scaled < ALP_DOUBLE_ENCODING_LOWER_LIMIT ||
            scaled > ALP_DOUBLE_ENCODING_UPPER_LIMIT) {
            return false;
        }
        const double rounded = scaled + ALP_DOUBLE_MAGIC - ALP_DOUBLE_MAGIC;
        if (!std::isfinite(rounded) ||
            rounded < ALP_DOUBLE_ENCODING_LOWER_LIMIT ||
            rounded > ALP_DOUBLE_ENCODING_UPPER_LIMIT) {
            return false;
        }
        *out = static_cast<int64_t>(rounded);
        return true;
    }

    static double DecodeValue(int64_t encoded, uint8_t factor,
                              uint8_t exponent) {
        const double factor_value = static_cast<double>(ALP_FACT[factor]);
        return static_cast<double>(encoded) * factor_value *
               ALP_DOUBLE_FRAC[exponent];
    }

    static void AppendValue(std::vector<uint8_t>& out, double value) {
        uint64_t bits = 0;
        std::memcpy(&bits, &value, sizeof(bits));
        AlpAppendU64(out, bits);
    }

    static bool ReadValue(const uint8_t* data, uint32_t size, uint32_t& pos,
                          double* value) {
        uint64_t bits = 0;
        if (!AlpReadU64(data, size, pos, bits)) {
            return false;
        }
        std::memcpy(value, &bits, sizeof(bits));
        return true;
    }

    static bool BitwiseEqual(double lhs, double rhs) {
        return std::memcmp(&lhs, &rhs, sizeof(double)) == 0;
    }
};

}  // namespace alp
}  // namespace storage

#endif  // ENCODING_ALP_TRAITS_H
