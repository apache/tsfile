/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include <gtest/gtest.h>

#include <cfloat>
#include <climits>
#include <cmath>
#include <limits>

#include "common/allocator/byte_stream.h"
#include "encoding/double_sprintz_decoder.h"
#include "encoding/double_sprintz_encoder.h"
#include "encoding/float_sprintz_decoder.h"
#include "encoding/float_sprintz_encoder.h"
#include "encoding/int32_sprintz_decoder.h"
#include "encoding/int32_sprintz_encoder.h"
#include "encoding/int64_sprintz_decoder.h"
#include "encoding/int64_sprintz_encoder.h"

using namespace storage;
using namespace common;

namespace {

constexpr int float_max_point_value = 10000;
constexpr int64_t double_max_point_value = 1000000000000000LL;

std::vector<int32_t> int_list;
std::vector<int64_t> long_list;
std::vector<float> float_list;
std::vector<double> double_list;
std::vector<int> iterations = {1, 3, 8, 16, 1000, 10000};

void PrepareHybridData() {
    int hybrid_count = 11;
    int hybrid_num = 50;
    int hybrid_start = 2000;
    for (int i = 0; i < hybrid_num; i++) {
        for (int j = 0; j < hybrid_count; j++) {
            float_list.push_back(static_cast<float>(hybrid_start) /
                                 float_max_point_value);
            double_list.push_back(static_cast<double>(hybrid_start) /
                                  double_max_point_value);
            int_list.push_back(hybrid_start);
            long_list.push_back(hybrid_start);
        }
        for (int j = 0; j < hybrid_count; j++) {
            float_list.push_back(static_cast<float>(hybrid_start) /
                                 float_max_point_value);
            double_list.push_back(static_cast<double>(hybrid_start) /
                                  double_max_point_value);
            int_list.push_back(hybrid_start);
            long_list.push_back(hybrid_start);
            hybrid_start += 3;
        }
        hybrid_count += 2;
    }
}

class SprintzCodecTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (int_list.empty()) PrepareHybridData();
    }
};

// Produced by Java Sprintz encoders with FIRE: one packed block, then
// extreme transitions requiring the existing raw fallback representation.
const char* java_sprintz_golden[] = {
    "0100ff882320030800000000800000007fffffffffffffff00000001c00000003fffffff00"
    "00002a810401030100",
    "010000000000000000ff8843400308000000000000000080000000000000007fffffffffff"
    "ffffffffffffffffffff0000000000000001c0000000000000003fffffffffffffff000000"
    "000000002a810401030100",
    "1f00000000fdfffffe03fffffc03fffff807fffff007ffffe00fffffc01fffff803fffff88"
    "00000000c00e0fffbfffffd000000017fbfffff5000000015fc00000d00000001602000000"
    "81422a0000c4cf7a80",
    "3f0000000000000000ffbffffffffffffe007ffffffffffffc007ffffffffffff800ffffff"
    "fffffff000ffffffffffffe001ffffffffffffc003ffffffffffff8007ffffffffffff8800"
    "00000000000000c00381ffff7ffffffffffffd00000000000000017ff7ffffffffffff5000"
    "00000000000015ff8000000000000d00000000000000016004000000000000008140454000"
    "00000000c23ffbd4",
};

std::string StreamHex(ByteStream& stream) {
    static const char digits[] = "0123456789abcdef";
    std::string hex;
    std::vector<uint8_t> bytes;
    uint8_t byte;
    uint32_t read_len;
    while (stream.has_remaining()) {
        if (stream.read_buf(&byte, 1, read_len) != E_OK) break;
        bytes.push_back(byte);
        hex += digits[byte >> 4];
        hex += digits[byte & 15];
    }
    stream.reset();
    stream.write_buf(bytes.data(), bytes.size());
    return hex;
}

TEST_F(SprintzCodecTest, FireMatchesJavaWrappingArithmetic) {
    IntFire int_fire(2);
    LongFire long_fire(3);
    const int32_t ints[] = {0,   INT32_MIN, 123456789, INT32_MAX,
                            -42, 1 << 30,   23};
    const int64_t longs[] = {0,
                             int64_t(1) << 40,
                             (int64_t(1) << 40) + (int64_t(1) << 30),
                             int64_t(1) << 55,
                             INT64_MAX,
                             -42,
                             23};
    const int32_t java_int_predictions[] = {
        0, INT32_MIN, 129748245, 2145006038, 6291414, 1077936126, 7340055};
    const int64_t java_long_predictions[] = {0,
                                             1099511627776LL,
                                             1100585369600LL,
                                             36026597995708416LL,
                                             INT64_MAX,
                                             -140737488355370LL,
                                             22};
    for (int i = 0; i < 7; ++i) {
        ASSERT_EQ(int_fire.predict(ints[i]), java_int_predictions[i]);
        ASSERT_EQ(long_fire.predict(longs[i]), java_long_predictions[i]);
        int_fire.train(ints[i], ints[(i + 1) % 7], 1);
        long_fire.train(longs[i], longs[(i + 1) % 7], 1);
    }
}

TEST_F(SprintzCodecTest, Int32FullBlocksAndExtremeTransitions) {
    const std::vector<int32_t> edge_values = {
        0, INT32_MIN, INT32_MAX, -1, 1, INT32_MIN / 2, INT32_MAX / 2, 42, 0};
    for (const std::string method : {"fire", "delta"}) {
        for (int count : {1, 8, 9, 10, 16, 18, 32, 143, 144, 145, 4096}) {
            SCOPED_TRACE(method + ":" + std::to_string(count));
            Int32SprintzEncoder encoder;
            encoder.set_predict_method(method);
            for (int round = 0; round < 2; ++round) {
                ByteStream stream(128, MOD_ENCODER_OBJ);
                for (int i = 0; i < count; ++i) {
                    // Also exercise a safe packed block before raw fallbacks.
                    int32_t value = i < 9 ? static_cast<int32_t>(i)
                                          : edge_values[i % edge_values.size()];
                    ASSERT_EQ(encoder.encode(value, stream), E_OK);
                }
                ASSERT_EQ(encoder.flush(stream), E_OK);
                if (method == "fire" && count == 18) {
                    ASSERT_EQ(StreamHex(stream), java_sprintz_golden[0]);
                }
                Int32SprintzDecoder decoder;
                decoder.set_predict_method(method);
                for (int i = 0; i < count; ++i) {
                    SCOPED_TRACE(i);
                    ASSERT_TRUE(decoder.has_remaining(stream));
                    int32_t actual;
                    ASSERT_EQ(decoder.read_int32(actual, stream), E_OK);
                    int32_t expected =
                        i < 9 ? static_cast<int32_t>(i)
                              : edge_values[i % edge_values.size()];
                    ASSERT_EQ(actual, expected);
                }
                ASSERT_FALSE(decoder.has_remaining(stream));
            }
        }
    }
}

TEST_F(SprintzCodecTest, Int64FullBlocksAndExtremeTransitions) {
    const std::vector<int64_t> edge_values = {
        0, INT64_MIN, INT64_MAX, -1, 1, INT64_MIN / 2, INT64_MAX / 2, 42, 0};
    for (const std::string method : {"fire", "delta"}) {
        for (int count : {1, 8, 9, 10, 16, 18, 32, 143, 144, 145, 4096}) {
            SCOPED_TRACE(method + ":" + std::to_string(count));
            Int64SprintzEncoder encoder;
            encoder.set_predict_method(method);
            for (int round = 0; round < 2; ++round) {
                ByteStream stream(128, MOD_ENCODER_OBJ);
                for (int i = 0; i < count; ++i) {
                    // Also exercise a safe packed block before raw fallbacks.
                    int64_t value = i < 9 ? static_cast<int64_t>(i)
                                          : edge_values[i % edge_values.size()];
                    ASSERT_EQ(encoder.encode(value, stream), E_OK);
                }
                ASSERT_EQ(encoder.flush(stream), E_OK);
                if (method == "fire" && count == 18) {
                    ASSERT_EQ(StreamHex(stream), java_sprintz_golden[1]);
                }
                Int64SprintzDecoder decoder;
                decoder.set_predict_method(method);
                for (int i = 0; i < count; ++i) {
                    SCOPED_TRACE(i);
                    ASSERT_TRUE(decoder.has_remaining(stream));
                    int64_t actual;
                    ASSERT_EQ(decoder.read_int64(actual, stream), E_OK);
                    int64_t expected =
                        i < 9 ? static_cast<int64_t>(i)
                              : edge_values[i % edge_values.size()];
                    ASSERT_EQ(actual, expected);
                }
                ASSERT_FALSE(decoder.has_remaining(stream));
            }
        }
    }
}

TEST_F(SprintzCodecTest, FloatFullBlocksAndExtremeTransitions) {
    const std::vector<float> edge_values = {
        0.0f,
        -0.0f,
        FLT_MAX,
        -FLT_MAX,
        std::numeric_limits<float>::denorm_min(),
        -std::numeric_limits<float>::denorm_min(),
        1.0f,
        -1.0f,
        42.5f};
    for (const std::string method : {"fire", "delta"}) {
        for (int count : {1, 8, 9, 10, 16, 18, 32, 143, 144, 145, 4096}) {
            SCOPED_TRACE(method + ":" + std::to_string(count));
            FloatSprintzEncoder encoder;
            encoder.set_predict_method(method);
            for (int round = 0; round < 2; ++round) {
                ByteStream stream(128, MOD_ENCODER_OBJ);
                for (int i = 0; i < count; ++i) {
                    // Also exercise a safe packed block before raw fallbacks.
                    float value = i < 9 ? static_cast<float>(i)
                                        : edge_values[i % edge_values.size()];
                    ASSERT_EQ(encoder.encode(value, stream), E_OK);
                }
                ASSERT_EQ(encoder.flush(stream), E_OK);
                if (method == "fire" && count == 18) {
                    ASSERT_EQ(StreamHex(stream), java_sprintz_golden[2]);
                }
                FloatSprintzDecoder decoder;
                decoder.set_predict_method(method);
                for (int i = 0; i < count; ++i) {
                    SCOPED_TRACE(i);
                    ASSERT_TRUE(decoder.has_remaining(stream));
                    float actual;
                    ASSERT_EQ(decoder.read_float(actual, stream), E_OK);
                    float expected = i < 9
                                         ? static_cast<float>(i)
                                         : edge_values[i % edge_values.size()];
                    ASSERT_EQ(common::float_to_int(actual),
                              common::float_to_int(expected));
                }
                ASSERT_FALSE(decoder.has_remaining(stream));
            }
        }
    }
}

TEST_F(SprintzCodecTest, DoubleFullBlocksAndExtremeTransitions) {
    const std::vector<double> edge_values = {
        0.0,
        -0.0,
        DBL_MAX,
        -DBL_MAX,
        std::numeric_limits<double>::denorm_min(),
        -std::numeric_limits<double>::denorm_min(),
        1.0,
        -1.0,
        42.5};
    for (const std::string method : {"fire", "delta"}) {
        for (int count : {1, 8, 9, 10, 16, 18, 32, 143, 144, 145, 4096}) {
            SCOPED_TRACE(method + ":" + std::to_string(count));
            DoubleSprintzEncoder encoder;
            encoder.set_predict_method(method);
            for (int round = 0; round < 2; ++round) {
                ByteStream stream(128, MOD_ENCODER_OBJ);
                for (int i = 0; i < count; ++i) {
                    // Also exercise a safe packed block before raw fallbacks.
                    double value = i < 9 ? static_cast<double>(i)
                                         : edge_values[i % edge_values.size()];
                    ASSERT_EQ(encoder.encode(value, stream), E_OK);
                }
                ASSERT_EQ(encoder.flush(stream), E_OK);
                if (method == "fire" && count == 18) {
                    ASSERT_EQ(StreamHex(stream), java_sprintz_golden[3]);
                }
                DoubleSprintzDecoder decoder;
                decoder.set_predict_method(method);
                for (int i = 0; i < count; ++i) {
                    SCOPED_TRACE(i);
                    ASSERT_TRUE(decoder.has_remaining(stream));
                    double actual;
                    ASSERT_EQ(decoder.read_double(actual, stream), E_OK);
                    double expected = i < 9
                                          ? static_cast<double>(i)
                                          : edge_values[i % edge_values.size()];
                    ASSERT_EQ(common::double_to_long(actual),
                              common::double_to_long(expected));
                }
                ASSERT_FALSE(decoder.has_remaining(stream));
            }
        }
    }
}

TEST_F(SprintzCodecTest, Int32SingleValue) {
    Int32SprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    ASSERT_EQ(encoder.encode(777, stream), E_OK);
    ASSERT_EQ(encoder.flush(stream), E_OK);

    Int32SprintzDecoder decoder;
    int32_t val;
    ASSERT_TRUE(decoder.has_remaining(stream));
    ASSERT_EQ(decoder.read_int32(val, stream), E_OK);
    ASSERT_EQ(val, 777);
    ASSERT_FALSE(decoder.has_remaining(stream));
}

TEST_F(SprintzCodecTest, Int64SingleValue) {
    Int64SprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    int64_t value = static_cast<int64_t>(INT32_MAX) + 10;
    ASSERT_EQ(encoder.encode(value, stream), E_OK);
    ASSERT_EQ(encoder.flush(stream), E_OK);

    Int64SprintzDecoder decoder;
    int64_t actual;
    ASSERT_TRUE(decoder.has_remaining(stream));
    ASSERT_EQ(decoder.read_int64(actual, stream), E_OK);
    ASSERT_EQ(actual, value);
    ASSERT_FALSE(decoder.has_remaining(stream));
}

TEST_F(SprintzCodecTest, Int32EdgeValues) {
    std::vector<int32_t> values = {INT32_MIN, -1, 0, 1, INT32_MAX};

    Int32SprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    for (auto v : values) {
        encoder.encode(v, stream);
    }
    encoder.flush(stream);

    Int32SprintzDecoder decoder;
    for (auto expected : values) {
        int32_t actual;
        ASSERT_TRUE(decoder.has_remaining(stream));
        ASSERT_EQ(decoder.read_int32(actual, stream), E_OK);
        ASSERT_EQ(actual, expected);
    }
    ASSERT_FALSE(decoder.has_remaining(stream));
}

TEST_F(SprintzCodecTest, Int64EdgeValues) {
    std::vector<int64_t> values = {INT64_MIN, -1, 0, 1, INT64_MAX};

    Int64SprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    for (auto v : values) {
        encoder.encode(v, stream);
    }
    encoder.flush(stream);

    Int64SprintzDecoder decoder;
    for (auto expected : values) {
        int64_t actual;
        ASSERT_TRUE(decoder.has_remaining(stream));
        ASSERT_EQ(decoder.read_int64(actual, stream), E_OK);
        ASSERT_EQ(actual, expected);
    }
    ASSERT_FALSE(decoder.has_remaining(stream));
}

TEST_F(SprintzCodecTest, Int32ZeroNumber) {
    Int32SprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    for (int i = 0; i < 3; ++i) encoder.encode(0, stream);
    encoder.flush(stream);
    for (int i = 0; i < 3; ++i) encoder.encode(0, stream);
    encoder.flush(stream);

    for (int round = 0; round < 2; ++round) {
        Int32SprintzDecoder decoder;
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            int32_t actual;
            ASSERT_EQ(decoder.read_int32(actual, stream), E_OK);
            ASSERT_EQ(actual, 0);
        }
    }
}

TEST_F(SprintzCodecTest, Int64ZeroNumber) {
    Int64SprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    for (int i = 0; i < 3; ++i) encoder.encode(static_cast<int64_t>(0), stream);
    encoder.flush(stream);
    for (int i = 0; i < 3; ++i) encoder.encode(static_cast<int64_t>(0), stream);
    encoder.flush(stream);

    for (int round = 0; round < 2; ++round) {
        Int64SprintzDecoder decoder;
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            int64_t actual;
            ASSERT_EQ(decoder.read_int64(actual, stream), E_OK);
            ASSERT_EQ(actual, 0);
        }
    }
}

TEST_F(SprintzCodecTest, Int32Increasing) {
    for (int num : iterations) {
        Int32SprintzEncoder encoder;
        ByteStream stream(1024, MOD_ENCODER_OBJ);
        for (int i = 0; i < num; ++i) encoder.encode(7 + 2 * i, stream);
        encoder.flush(stream);

        Int32SprintzDecoder decoder;
        for (int i = 0; i < num; ++i) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            int32_t actual;
            ASSERT_EQ(decoder.read_int32(actual, stream), E_OK);
            ASSERT_EQ(actual, 7 + 2 * i);
        }
        ASSERT_FALSE(decoder.has_remaining(stream));
    }
}

TEST_F(SprintzCodecTest, Int64Increasing) {
    for (int num : iterations) {
        Int64SprintzEncoder encoder;
        ByteStream stream(1024, MOD_ENCODER_OBJ);
        for (int i = 0; i < num; ++i)
            encoder.encode(static_cast<int64_t>(7) + 2 * i, stream);
        encoder.flush(stream);

        Int64SprintzDecoder decoder;
        for (int i = 0; i < num; ++i) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            int64_t actual;
            ASSERT_EQ(decoder.read_int64(actual, stream), E_OK);
            ASSERT_EQ(actual, 7 + 2 * i);
        }
        ASSERT_FALSE(decoder.has_remaining(stream));
    }
}

TEST_F(SprintzCodecTest, FloatSingleValue) {
    FloatSprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    ASSERT_EQ(encoder.encode(FLT_MAX, stream), E_OK);
    ASSERT_EQ(encoder.flush(stream), E_OK);

    FloatSprintzDecoder decoder;
    float actual;
    ASSERT_TRUE(decoder.has_remaining(stream));
    ASSERT_EQ(decoder.read_float(actual, stream), E_OK);
    ASSERT_EQ(actual, FLT_MAX);
    ASSERT_FALSE(decoder.has_remaining(stream));
}

TEST_F(SprintzCodecTest, DoubleSingleValue) {
    DoubleSprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    ASSERT_EQ(encoder.encode(DBL_MAX, stream), E_OK);
    ASSERT_EQ(encoder.flush(stream), E_OK);

    DoubleSprintzDecoder decoder;
    double actual;
    ASSERT_TRUE(decoder.has_remaining(stream));
    ASSERT_EQ(decoder.read_double(actual, stream), E_OK);
    ASSERT_EQ(actual, DBL_MAX);
    ASSERT_FALSE(decoder.has_remaining(stream));
}

TEST_F(SprintzCodecTest, FloatZeroNumber) {
    FloatSprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    float value = 0.0f;
    for (int i = 0; i < 3; ++i) encoder.encode(value, stream);
    encoder.flush(stream);
    for (int i = 0; i < 3; ++i) encoder.encode(value, stream);
    encoder.flush(stream);

    for (int round = 0; round < 2; ++round) {
        FloatSprintzDecoder decoder;
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            float actual;
            ASSERT_EQ(decoder.read_float(actual, stream), E_OK);
            ASSERT_EQ(actual, value);
        }
    }
}

TEST_F(SprintzCodecTest, DoubleZeroNumber) {
    DoubleSprintzEncoder encoder;
    ByteStream stream(128, MOD_ENCODER_OBJ);
    double value = 0.0;
    for (int i = 0; i < 3; ++i) encoder.encode(value, stream);
    encoder.flush(stream);
    for (int i = 0; i < 3; ++i) encoder.encode(value, stream);
    encoder.flush(stream);

    for (int round = 0; round < 2; ++round) {
        DoubleSprintzDecoder decoder;
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            double actual;
            ASSERT_EQ(decoder.read_double(actual, stream), E_OK);
            ASSERT_EQ(actual, value);
        }
    }
}

TEST_F(SprintzCodecTest, FloatIncreasing) {
    for (int num : iterations) {
        FloatSprintzEncoder encoder;
        ByteStream stream(1024, MOD_ENCODER_OBJ);
        float value = 7.101f;
        for (int i = 0; i < num; ++i) encoder.encode(value + 2.0f * i, stream);
        encoder.flush(stream);

        FloatSprintzDecoder decoder;
        for (int i = 0; i < num; ++i) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            float actual;
            ASSERT_EQ(decoder.read_float(actual, stream), E_OK);
            ASSERT_FLOAT_EQ(actual, value + 2 * i);
        }
        ASSERT_FALSE(decoder.has_remaining(stream));
    }
}

TEST_F(SprintzCodecTest, DoubleIncreasing) {
    for (int num : iterations) {
        DoubleSprintzEncoder encoder;
        ByteStream stream(1024, MOD_ENCODER_OBJ);
        float f = 7.101f;
        double value = static_cast<double>(f);
        for (int i = 0; i < num; ++i) {
            double input_val = value + 2.0 * i;
            encoder.encode(input_val, stream);
        }

        encoder.flush(stream);

        DoubleSprintzDecoder decoder;
        for (int i = 0; i < num; ++i) {
            ASSERT_TRUE(decoder.has_remaining(stream));
            double actual;
            ASSERT_EQ(decoder.read_double(actual, stream), E_OK);
            ASSERT_DOUBLE_EQ(actual, value + 2 * i);
        }
        ASSERT_FALSE(decoder.has_remaining(stream));
    }
}

TEST_F(SprintzCodecTest, FloatExtremeValues) {
    std::vector<float> test_vals = {FLT_MIN, FLT_MAX, -FLT_MIN,      -FLT_MAX,
                                    -0.0f,   0.0f,    std::nanf("1")};

    FloatSprintzEncoder encoder;
    ByteStream stream(256, MOD_ENCODER_OBJ);
    for (auto v : test_vals) {
        encoder.encode(v, stream);
    }
    encoder.flush(stream);

    FloatSprintzDecoder decoder;
    for (auto expected : test_vals) {
        float actual;
        ASSERT_TRUE(decoder.has_remaining(stream));
        ASSERT_EQ(decoder.read_float(actual, stream), E_OK);
        if (std::isnan(expected)) {
            ASSERT_TRUE(std::isnan(actual));
        } else {
            ASSERT_FLOAT_EQ(actual, expected);
        }
    }
    ASSERT_FALSE(decoder.has_remaining(stream));
}

TEST_F(SprintzCodecTest, DoubleExtremeValues) {
    std::vector<double> test_vals = {DBL_MIN, DBL_MAX, -DBL_MIN,    -DBL_MAX,
                                     -0.0,    0.0,     std::nan("")};

    DoubleSprintzEncoder encoder;
    ByteStream stream(256, MOD_ENCODER_OBJ);
    for (auto v : test_vals) {
        encoder.encode(v, stream);
    }
    encoder.flush(stream);

    DoubleSprintzDecoder decoder;
    for (auto expected : test_vals) {
        double actual;
        ASSERT_TRUE(decoder.has_remaining(stream));
        ASSERT_EQ(decoder.read_double(actual, stream), E_OK);
        if (std::isnan(expected)) {
            ASSERT_TRUE(std::isnan(actual));
        } else {
            ASSERT_DOUBLE_EQ(actual, expected);
        }
    }
    ASSERT_FALSE(decoder.has_remaining(stream));
}

}  // namespace
