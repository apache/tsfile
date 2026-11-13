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

#ifndef ENCODING_ALP_CONSTANTS_H
#define ENCODING_ALP_CONSTANTS_H

#include <stdint.h>

namespace storage {
namespace alp {

enum AlpDataType : uint8_t { ALP_FLOAT = 0, ALP_DOUBLE = 1 };

enum AlpScheme : uint8_t {
    ALP_SCHEME_ALP = 0,
    ALP_SCHEME_PLAIN = 1,
    ALP_SCHEME_ALP_RD = 2  // reserved for a later format revision
};

static const uint8_t ALP_FORMAT_VERSION = 1;
static const uint32_t ALP_BLOCK_SIZE = 1024;
static const uint32_t ALP_SAMPLE_SIZE = 32;
static const uint32_t ALP_BODY_ALIGNMENT = 32;
static const uint8_t ALP_FLOAT_MAX_EXPONENT = 10;
static const uint8_t ALP_DOUBLE_MAX_EXPONENT = 18;

static const float ALP_FLOAT_MAGIC = 12582912.0f;           // 1.5 * 2^23
static const double ALP_DOUBLE_MAGIC = 6755399441055744.0;  // 1.5 * 2^52

// Values outside these bounds cannot be cast to the corresponding integer type
// without undefined behaviour.  The bounds intentionally leave a small margin
// so the magic-number rounding itself cannot overflow.
static const float ALP_FLOAT_ENCODING_LOWER_LIMIT = -2147483520.0f;
static const float ALP_FLOAT_ENCODING_UPPER_LIMIT = 2147483520.0f;
static const double ALP_DOUBLE_ENCODING_LOWER_LIMIT = -9223372036854774784.0;
static const double ALP_DOUBLE_ENCODING_UPPER_LIMIT = 9223372036854774784.0;

static const float ALP_FLOAT_FRAC[ALP_FLOAT_MAX_EXPONENT + 1] = {
    1.0f,      0.1f,       0.01f,       0.001f,       0.0001f,      0.00001f,
    0.000001f, 0.0000001f, 0.00000001f, 0.000000001f, 0.0000000001f};

static const float ALP_FLOAT_EXP[ALP_FLOAT_MAX_EXPONENT + 1] = {
    1.0f,         10.0f,         100.0f,        1000.0f,
    10000.0f,     100000.0f,     1000000.0f,    10000000.0f,
    100000000.0f, 1000000000.0f, 10000000000.0f};

static const double ALP_DOUBLE_FRAC[ALP_DOUBLE_MAX_EXPONENT + 1] = {
    1.0,
    0.1,
    0.01,
    0.001,
    0.0001,
    0.00001,
    0.000001,
    0.0000001,
    0.00000001,
    0.000000001,
    0.0000000001,
    0.00000000001,
    0.000000000001,
    0.0000000000001,
    0.00000000000001,
    0.000000000000001,
    0.0000000000000001,
    0.00000000000000001,
    0.000000000000000001};

static const double ALP_DOUBLE_EXP[ALP_DOUBLE_MAX_EXPONENT + 1] = {
    1.0,
    10.0,
    100.0,
    1000.0,
    10000.0,
    100000.0,
    1000000.0,
    10000000.0,
    100000000.0,
    1000000000.0,
    10000000000.0,
    100000000000.0,
    1000000000000.0,
    10000000000000.0,
    100000000000000.0,
    1000000000000000.0,
    10000000000000000.0,
    100000000000000000.0,
    1000000000000000000.0};

// Exact integer powers of ten used by the decoder.  Up to 10^18 they are
// exactly representable as double; the float values are exactly representable
// up to 10^10.
static const int64_t ALP_FACT[ALP_DOUBLE_MAX_EXPONENT + 1] = {
    1LL,
    10LL,
    100LL,
    1000LL,
    10000LL,
    100000LL,
    1000000LL,
    10000000LL,
    100000000LL,
    1000000000LL,
    10000000000LL,
    100000000000LL,
    1000000000000LL,
    10000000000000LL,
    100000000000000LL,
    1000000000000000LL,
    10000000000000000LL,
    100000000000000000LL,
    1000000000000000000LL};

}  // namespace alp
}  // namespace storage

#endif  // ENCODING_ALP_CONSTANTS_H
