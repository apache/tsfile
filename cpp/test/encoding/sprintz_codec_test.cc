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

#include <algorithm>
#include <cfloat>
#include <climits>
#include <cmath>
#include <cstring>
#include <random>
#include <vector>

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

// ── Batch decode vs per-value decode ─────────────────────────────────────
//
// A SPRINTZ decoder reconstructs one block at a time into a buffer and serves
// values out of it. The batch overrides copy whole runs out of that buffer,
// so they must reproduce the per-value state machine exactly: same values,
// same stop points, same residual block state. These tests pin the two
// implementations to each other on the same byte stream.

int ReadOne(Int32SprintzDecoder& decoder, int32_t& value, ByteStream& in) {
    return decoder.read_int32(value, in);
}

int ReadOne(Int64SprintzDecoder& decoder, int64_t& value, ByteStream& in) {
    return decoder.read_int64(value, in);
}

int ReadOne(FloatSprintzDecoder& decoder, float& value, ByteStream& in) {
    return decoder.read_float(value, in);
}

int ReadOne(DoubleSprintzDecoder& decoder, double& value, ByteStream& in) {
    return decoder.read_double(value, in);
}

int ReadBatch(Int32SprintzDecoder& decoder, int32_t* out, int capacity,
              int& actual, ByteStream& in) {
    return decoder.read_batch_int32(out, capacity, actual, in);
}

int ReadBatch(Int64SprintzDecoder& decoder, int64_t* out, int capacity,
              int& actual, ByteStream& in) {
    return decoder.read_batch_int64(out, capacity, actual, in);
}

int ReadBatch(FloatSprintzDecoder& decoder, float* out, int capacity,
              int& actual, ByteStream& in) {
    return decoder.read_batch_float(out, capacity, actual, in);
}

int ReadBatch(DoubleSprintzDecoder& decoder, double* out, int capacity,
              int& actual, ByteStream& in) {
    return decoder.read_batch_double(out, capacity, actual, in);
}

// The loop Decoder::read_batch_* ran before the SPRINTZ overrides existed.
// This is the reference behaviour the overrides must reproduce.
template <typename T, typename DecoderT>
int ReferenceBatch(DecoderT& decoder, T* out, int capacity, int& actual,
                   ByteStream& in) {
    actual = 0;
    T value;
    while (actual < capacity && decoder.has_remaining(in)) {
        int ret = ReadOne(decoder, value, in);
        if (ret != E_OK) {
            return ret;
        }
        out[actual++] = value;
    }
    return E_OK;
}

template <typename T>
struct SprintzTypes;

template <>
struct SprintzTypes<int32_t> {
    typedef Int32SprintzEncoder Encoder;
    typedef Int32SprintzDecoder Decoder;
};

template <>
struct SprintzTypes<int64_t> {
    typedef Int64SprintzEncoder Encoder;
    typedef Int64SprintzDecoder Decoder;
};

template <>
struct SprintzTypes<float> {
    typedef FloatSprintzEncoder Encoder;
    typedef FloatSprintzDecoder Decoder;
};

template <>
struct SprintzTypes<double> {
    typedef DoubleSprintzEncoder Encoder;
    typedef DoubleSprintzDecoder Decoder;
};

template <typename T>
void EncodeAll(const std::vector<T>& values, ByteStream& out) {
    typename SprintzTypes<T>::Encoder encoder;
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(E_OK, encoder.encode(values[i], out));
    }
    ASSERT_EQ(E_OK, encoder.flush(out));
}

// Values are float/double with possible NaNs, so compare bit patterns.
template <typename T>
void ExpectSameBits(const T& expected, const T& actual, const char* what) {
    ASSERT_EQ(0, std::memcmp(&expected, &actual, sizeof(T))) << what;
}

int ReadExact(Int32SprintzDecoder& decoder, int32_t* out, int count,
              ByteStream& in) {
    return decoder.read_exact_int32(out, count, in);
}

int ReadExact(Int64SprintzDecoder& decoder, int64_t* out, int count,
              ByteStream& in) {
    return decoder.read_exact_int64(out, count, in);
}

int ReadExact(FloatSprintzDecoder& decoder, float* out, int count,
              ByteStream& in) {
    return decoder.read_exact_float(out, count, in);
}

int ReadExact(DoubleSprintzDecoder& decoder, double* out, int count,
              ByteStream& in) {
    return decoder.read_exact_double(out, count, in);
}

std::vector<int> RepeatScript(const std::vector<int>& base, int rounds) {
    std::vector<int> script;
    script.reserve(static_cast<size_t>(rounds));
    for (int i = 0; i < rounds; ++i) {
        script.push_back(base[static_cast<size_t>(i) % base.size()]);
    }
    return script;
}

// Compare batch reads against the per-value loop, round by round on identical
// streams, including a per-value read between rounds so a batch that stopped
// at a gated block tail is resumed on both paths.
template <typename T>
void CheckBatchParity(const std::vector<T>& values,
                      const std::vector<int>& capacities,
                      uint32_t page_size = 1024) {
    typedef typename SprintzTypes<T>::Decoder Decoder;

    ByteStream ref_in(page_size, MOD_ENCODER_OBJ);
    ByteStream bat_in(page_size, MOD_ENCODER_OBJ);
    EncodeAll(values, ref_in);
    EncodeAll(values, bat_in);
    ASSERT_EQ(ref_in.total_size(), bat_in.total_size());

    Decoder ref_dec;
    Decoder bat_dec;
    std::vector<T> emitted;
    for (size_t round = 0; round < capacities.size(); ++round) {
        const int capacity = capacities[round];
        const size_t buf_len = static_cast<size_t>(capacity > 0 ? capacity : 1);
        std::vector<T> ref_buf(buf_len);
        std::vector<T> bat_buf(buf_len);
        int ref_actual = -1;
        int bat_actual = -1;
        int ref_ret = ReferenceBatch(ref_dec, ref_buf.data(), capacity,
                                     ref_actual, ref_in);
        int bat_ret =
            ReadBatch(bat_dec, bat_buf.data(), capacity, bat_actual, bat_in);

        SCOPED_TRACE(::testing::Message()
                     << "round " << round << " capacity " << capacity);
        ASSERT_EQ(ref_ret, bat_ret);
        ASSERT_EQ(ref_actual, bat_actual);
        for (int i = 0; i < bat_actual; ++i) {
            ExpectSameBits(ref_buf[i], bat_buf[i], "batch value");
            emitted.push_back(bat_buf[i]);
        }
        ASSERT_EQ(ref_in.remaining_size(), bat_in.remaining_size());
        ASSERT_EQ(ref_dec.has_remaining(ref_in), bat_dec.has_remaining(bat_in));

        if (ref_dec.has_remaining(ref_in)) {
            T ref_value = T();
            T bat_value = T();
            int ref_one = ReadOne(ref_dec, ref_value, ref_in);
            int bat_one = ReadOne(bat_dec, bat_value, bat_in);
            ASSERT_EQ(ref_one, bat_one);
            ASSERT_EQ(E_OK, ref_one);
            ExpectSameBits(ref_value, bat_value, "handoff value");
            emitted.push_back(bat_value);
        }
    }

    // Drain the rest per value; the residuals must still agree.
    while (ref_dec.has_remaining(ref_in)) {
        ASSERT_TRUE(bat_dec.has_remaining(bat_in));
        T ref_value = T();
        T bat_value = T();
        ASSERT_EQ(ReadOne(ref_dec, ref_value, ref_in),
                  ReadOne(bat_dec, bat_value, bat_in));
        ExpectSameBits(ref_value, bat_value, "drained value");
        emitted.push_back(bat_value);
    }
    ASSERT_FALSE(bat_dec.has_remaining(bat_in));
    ASSERT_EQ(ref_in.remaining_size(), bat_in.remaining_size());

    // What both paths produced is a prefix of the encoded sequence (a stream
    // that ends on a full block leaves its last value unreadable per value).
    ASSERT_LE(emitted.size(), values.size());
    for (size_t i = 0; i < emitted.size(); ++i) {
        ExpectSameBits(values[i], emitted[i], "emitted value");
    }
}

template <typename T>
struct ValueSets {
    std::vector<T> smooth;
    std::vector<T> random;
    std::vector<T> constant;
    std::vector<T> jumps;
};

template <typename T>
ValueSets<T> MakeValueSets(int count, uint32_t seed);

template <>
ValueSets<int32_t> MakeValueSets<int32_t>(int count, uint32_t seed) {
    std::mt19937 rng(seed);
    // Delta magnitudes stay below 2^31 so the encoder's zigzag step (2 * d)
    // does not overflow.
    std::uniform_int_distribution<int32_t> wide(-250000000, 250000000);
    ValueSets<int32_t> sets;
    int32_t smooth = 1000;
    for (int i = 0; i < count; ++i) {
        smooth += 1 + static_cast<int32_t>(rng() % 7);
        sets.smooth.push_back(smooth);
        sets.random.push_back(wide(rng));
        sets.constant.push_back(-7);
        sets.jumps.push_back((i % 2 == 0) ? 500000000 : -500000000);
    }
    return sets;
}

template <>
ValueSets<int64_t> MakeValueSets<int64_t>(int count, uint32_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int64_t> wide(-1000000000000000LL,
                                                1000000000000000LL);
    ValueSets<int64_t> sets;
    int64_t smooth = 1600000000000LL;
    for (int i = 0; i < count; ++i) {
        smooth += 1 + static_cast<int64_t>(rng() % 97);
        sets.smooth.push_back(smooth);
        sets.random.push_back(wide(rng));
        sets.constant.push_back(INT64_MIN);
        sets.jumps.push_back((i % 2 == 0) ? 1000000000000000LL
                                          : -1000000000000000LL);
    }
    return sets;
}

template <>
ValueSets<float> MakeValueSets<float>(int count, uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> wide(1.0f, 8.0f);
    ValueSets<float> sets;
    float smooth = 100.0f;
    for (int i = 0; i < count; ++i) {
        smooth += 0.25f * static_cast<float>(1 + rng() % 4) +
                  0.001f * static_cast<float>(rng() % 1000);
        sets.smooth.push_back(smooth);
        sets.random.push_back(wide(rng));
        sets.constant.push_back(12.5f);
        sets.jumps.push_back((i % 2 == 0) ? 1000.0f : 1.0f);
    }
    return sets;
}

template <>
ValueSets<double> MakeValueSets<double>(int count, uint32_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> wide(1.0, 8.0);
    ValueSets<double> sets;
    double smooth = 100.0;
    for (int i = 0; i < count; ++i) {
        smooth += 0.25 * static_cast<double>(1 + rng() % 4) +
                  0.000001 * static_cast<double>(rng() % 1000000);
        sets.smooth.push_back(smooth);
        sets.random.push_back(wide(rng));
        sets.constant.push_back(-0.0);
        sets.jumps.push_back((i % 2 == 0) ? 1000000.0 : 1.0);
    }
    return sets;
}

std::vector<int> BoundaryCapacities() {
    const int caps[] = {1, 2, 3, 5, 7, 8, 9, 10, 17, 129};
    return std::vector<int>(caps, caps + sizeof(caps) / sizeof(caps[0]));
}

// A batch script that also splits blocks at arbitrary points.
std::vector<int> RandomCapacities(int rounds, uint32_t seed) {
    std::mt19937 rng(seed);
    std::vector<int> script;
    script.reserve(static_cast<size_t>(rounds));
    for (int i = 0; i < rounds; ++i) {
        script.push_back(static_cast<int>(rng() % 23));
    }
    return script;
}

// The reference loop reads a huge capacity in one call; the batch override
// must agree with it for every count, including ones that end mid-block,
// exactly on a block boundary and inside the final short block.
template <typename T>
void CheckBlockBoundaryCounts() {
    typedef typename SprintzTypes<T>::Decoder Decoder;
    ValueSets<T> sets = MakeValueSets<T>(64, 7);
    for (int count = 0; count <= 40; ++count) {
        std::vector<T> values(sets.smooth.begin(), sets.smooth.begin() + count);
        ByteStream ref_in(1024, MOD_ENCODER_OBJ);
        ByteStream bat_in(1024, MOD_ENCODER_OBJ);
        EncodeAll(values, ref_in);
        EncodeAll(values, bat_in);

        Decoder ref_dec;
        Decoder bat_dec;
        const int capacity = count + 16;
        std::vector<T> ref_buf(static_cast<size_t>(capacity));
        std::vector<T> bat_buf(static_cast<size_t>(capacity));
        int ref_actual = -1;
        int bat_actual = -1;
        SCOPED_TRACE(::testing::Message() << "count " << count);
        ASSERT_EQ(
            ReferenceBatch(ref_dec, ref_buf.data(), capacity, ref_actual,
                           ref_in),
            ReadBatch(bat_dec, bat_buf.data(), capacity, bat_actual, bat_in));
        ASSERT_EQ(ref_actual, bat_actual);
        for (int i = 0; i < bat_actual; ++i) {
            ExpectSameBits(ref_buf[i], bat_buf[i], "batch value");
        }
        ASSERT_EQ(ref_in.remaining_size(), bat_in.remaining_size());
        ASSERT_EQ(ref_dec.has_remaining(ref_in), bat_dec.has_remaining(bat_in));

        // read_exact recovers the full input, so it also exercises the
        // batch/scalar handoff at a block tail.
        ByteStream exact_in(1024, MOD_ENCODER_OBJ);
        EncodeAll(values, exact_in);
        Decoder exact_dec;
        std::vector<T> exact(static_cast<size_t>(count > 0 ? count : 1));
        ASSERT_EQ(E_OK, ReadExact(exact_dec, exact.data(), count, exact_in));
        for (int i = 0; i < count; ++i) {
            ExpectSameBits(values[static_cast<size_t>(i)],
                           exact[static_cast<size_t>(i)], "exact value");
        }
        // FLOAT/DOUBLE short blocks are Gorilla-encoded and can leave the
        // final bytes of the end marker buffered (the per-value path does the
        // same), so only bound the residue here.
        ASSERT_LE(exact_in.remaining_size(), 8u);
    }
}

template <typename T>
void CheckTruncatedStreams() {
    typedef typename SprintzTypes<T>::Decoder Decoder;
    ValueSets<T> sets = MakeValueSets<T>(40, 11);
    ByteStream full(1024, MOD_ENCODER_OBJ);
    EncodeAll(sets.smooth, full);

    std::vector<uint8_t> bytes;
    bytes.reserve(static_cast<size_t>(full.total_size()));
    ByteStream::BufferIterator iter = full.init_buffer_iterator();
    while (true) {
        ByteStream::Buffer buffer = iter.get_next_buf();
        if (buffer.buf_ == nullptr || buffer.len_ == 0) {
            break;
        }
        const uint8_t* begin = reinterpret_cast<const uint8_t*>(buffer.buf_);
        bytes.insert(bytes.end(), begin, begin + buffer.len_);
    }
    ASSERT_EQ(bytes.size(), full.total_size());

    for (size_t len = 0; len <= bytes.size(); ++len) {
        ByteStream ref_in(1024, MOD_ENCODER_OBJ);
        ByteStream bat_in(1024, MOD_ENCODER_OBJ);
        if (len > 0) {
            ref_in.write_buf(reinterpret_cast<const char*>(bytes.data()),
                             static_cast<uint32_t>(len));
            bat_in.write_buf(reinterpret_cast<const char*>(bytes.data()),
                             static_cast<uint32_t>(len));
        }
        Decoder ref_dec;
        Decoder bat_dec;
        const int capacity = 1000;
        std::vector<T> ref_buf(static_cast<size_t>(capacity));
        std::vector<T> bat_buf(static_cast<size_t>(capacity));
        int ref_actual = -1;
        int bat_actual = -1;
        int ref_ret = ReferenceBatch(ref_dec, ref_buf.data(), capacity,
                                     ref_actual, ref_in);
        int bat_ret =
            ReadBatch(bat_dec, bat_buf.data(), capacity, bat_actual, bat_in);
        SCOPED_TRACE(::testing::Message()
                     << "prefix " << len << " of " << bytes.size());
        ASSERT_EQ(ref_ret, bat_ret);
        ASSERT_EQ(ref_actual, bat_actual);
        for (int i = 0; i < bat_actual; ++i) {
            ExpectSameBits(ref_buf[i], bat_buf[i], "truncated value");
        }
        ASSERT_EQ(ref_in.remaining_size(), bat_in.remaining_size());
        ASSERT_EQ(ref_dec.has_remaining(ref_in), bat_dec.has_remaining(bat_in));
    }
}

template <typename T>
void CheckZeroCapacity() {
    typedef typename SprintzTypes<T>::Decoder Decoder;
    ValueSets<T> sets = MakeValueSets<T>(32, 5);
    ByteStream in(1024, MOD_ENCODER_OBJ);
    EncodeAll(sets.smooth, in);
    const uint64_t remaining = in.remaining_size();

    Decoder decoder;
    T value = T();
    int actual = -1;
    ASSERT_EQ(E_OK, ReadBatch(decoder, &value, 0, actual, in));
    ASSERT_EQ(0, actual);
    ASSERT_EQ(E_OK, ReadBatch(decoder, &value, -3, actual, in));
    ASSERT_EQ(0, actual);
    ASSERT_EQ(remaining, in.remaining_size());

    // No state was consumed: the first batch starts at the first value.
    std::vector<T> out(4);
    ASSERT_EQ(E_OK, ReadBatch(decoder, out.data(), 4, actual, in));
    ASSERT_EQ(4, actual);
    for (int i = 0; i < actual; ++i) {
        ExpectSameBits(sets.smooth[static_cast<size_t>(i)], out[i],
                       "value after zero capacity");
    }
}

template <typename T>
void CheckEndOfStream() {
    typedef typename SprintzTypes<T>::Decoder Decoder;
    Decoder empty_decoder;
    ByteStream empty_in(64, MOD_ENCODER_OBJ);
    T value = T();
    int actual = -1;
    ASSERT_EQ(E_OK, ReadBatch(empty_decoder, &value, 8, actual, empty_in));
    ASSERT_EQ(0, actual);
    ASSERT_FALSE(empty_decoder.has_remaining(empty_in));

    ValueSets<T> sets = MakeValueSets<T>(50, 9);
    ByteStream in(1024, MOD_ENCODER_OBJ);
    EncodeAll(sets.smooth, in);
    Decoder decoder;
    std::vector<T> out(1024);
    uint64_t guard = 0;
    while (decoder.has_remaining(in)) {
        ASSERT_EQ(E_OK, ReadBatch(decoder, out.data(), 1024, actual, in));
        if (actual == 0) {
            break;
        }
        guard += static_cast<uint64_t>(actual);
        ASSERT_LE(guard, sets.smooth.size());
    }
    ASSERT_FALSE(decoder.has_remaining(in));
    ASSERT_EQ(0u, in.remaining_size());
    // Once drained, further batch calls report no values and no error.
    ASSERT_EQ(E_OK, ReadBatch(decoder, out.data(), 1024, actual, in));
    ASSERT_EQ(0, actual);
}

// read_exact must hand whole pages through the batch path, including the
// per-page tail the batch deliberately leaves for a scalar read.
template <typename T>
void CheckReadExactChunks(int total, int chunk) {
    typedef typename SprintzTypes<T>::Decoder Decoder;
    ValueSets<T> sets = MakeValueSets<T>(total, 21);
    ByteStream in(1024, MOD_ENCODER_OBJ);
    EncodeAll(sets.smooth, in);
    Decoder decoder;
    std::vector<T> out(static_cast<size_t>(total));
    for (int done = 0; done < total; done += chunk) {
        const int count = std::min(chunk, total - done);
        ASSERT_EQ(E_OK, ReadExact(decoder, out.data() + done, count, in));
    }
    for (int i = 0; i < total; ++i) {
        ExpectSameBits(sets.smooth[static_cast<size_t>(i)],
                       out[static_cast<size_t>(i)], "chunked exact value");
    }
    // FLOAT/DOUBLE short blocks are Gorilla-encoded and can leave the final
    // bytes of the end marker buffered, exactly as the per-value path does.
    ASSERT_LE(in.remaining_size(), 8u);
}

TEST_F(SprintzCodecTest, Int32BatchParitySmooth) {
    CheckBatchParity(MakeValueSets<int32_t>(3000, 1).smooth,
                     RepeatScript(BoundaryCapacities(), 300));
}

TEST_F(SprintzCodecTest, Int64BatchParitySmooth) {
    CheckBatchParity(MakeValueSets<int64_t>(3000, 2).smooth,
                     RepeatScript(BoundaryCapacities(), 300));
}

TEST_F(SprintzCodecTest, FloatBatchParitySmooth) {
    CheckBatchParity(MakeValueSets<float>(3000, 3).smooth,
                     RepeatScript(BoundaryCapacities(), 300));
}

TEST_F(SprintzCodecTest, DoubleBatchParitySmooth) {
    CheckBatchParity(MakeValueSets<double>(3000, 4).smooth,
                     RepeatScript(BoundaryCapacities(), 300));
}

TEST_F(SprintzCodecTest, Int32BatchParityRandom) {
    CheckBatchParity(MakeValueSets<int32_t>(3000, 5).random,
                     RandomCapacities(400, 6));
}

TEST_F(SprintzCodecTest, Int64BatchParityRandom) {
    CheckBatchParity(MakeValueSets<int64_t>(3000, 7).random,
                     RandomCapacities(400, 8));
}

TEST_F(SprintzCodecTest, FloatBatchParityRandom) {
    CheckBatchParity(MakeValueSets<float>(3000, 9).random,
                     RandomCapacities(400, 10));
}

TEST_F(SprintzCodecTest, DoubleBatchParityRandom) {
    CheckBatchParity(MakeValueSets<double>(3000, 11).random,
                     RandomCapacities(400, 12));
}

TEST_F(SprintzCodecTest, Int32BatchParityConstant) {
    CheckBatchParity(MakeValueSets<int32_t>(1000, 13).constant,
                     RepeatScript(BoundaryCapacities(), 200));
}

TEST_F(SprintzCodecTest, Int64BatchParityConstant) {
    CheckBatchParity(MakeValueSets<int64_t>(1000, 14).constant,
                     RepeatScript(BoundaryCapacities(), 200));
}

TEST_F(SprintzCodecTest, FloatBatchParityConstant) {
    CheckBatchParity(MakeValueSets<float>(1000, 15).constant,
                     RepeatScript(BoundaryCapacities(), 200));
}

TEST_F(SprintzCodecTest, DoubleBatchParityConstant) {
    CheckBatchParity(MakeValueSets<double>(1000, 16).constant,
                     RepeatScript(BoundaryCapacities(), 200));
}

TEST_F(SprintzCodecTest, Int32BatchParityJumps) {
    CheckBatchParity(MakeValueSets<int32_t>(1000, 17).jumps,
                     RepeatScript(BoundaryCapacities(), 200));
}

TEST_F(SprintzCodecTest, Int64BatchParityJumps) {
    CheckBatchParity(MakeValueSets<int64_t>(1000, 18).jumps,
                     RepeatScript(BoundaryCapacities(), 200));
}

TEST_F(SprintzCodecTest, FloatBatchParityJumps) {
    CheckBatchParity(MakeValueSets<float>(1000, 19).jumps,
                     RepeatScript(BoundaryCapacities(), 200));
}

TEST_F(SprintzCodecTest, DoubleBatchParityJumps) {
    CheckBatchParity(MakeValueSets<double>(1000, 20).jumps,
                     RepeatScript(BoundaryCapacities(), 200));
}

// A small ByteStream page size splits block payload reads across pages.
TEST_F(SprintzCodecTest, Int32BatchParityAcrossStreamPages) {
    CheckBatchParity(MakeValueSets<int32_t>(2000, 22).smooth,
                     RepeatScript(BoundaryCapacities(), 300), 16);
}

TEST_F(SprintzCodecTest, Int64BatchParityAcrossStreamPages) {
    CheckBatchParity(MakeValueSets<int64_t>(2000, 23).smooth,
                     RepeatScript(BoundaryCapacities(), 300), 16);
}

TEST_F(SprintzCodecTest, FloatBatchParityAcrossStreamPages) {
    CheckBatchParity(MakeValueSets<float>(2000, 24).smooth,
                     RepeatScript(BoundaryCapacities(), 300), 16);
}

TEST_F(SprintzCodecTest, DoubleBatchParityAcrossStreamPages) {
    CheckBatchParity(MakeValueSets<double>(2000, 25).smooth,
                     RepeatScript(BoundaryCapacities(), 300), 16);
}

TEST_F(SprintzCodecTest, Int32BatchBlockBoundaryCounts) {
    CheckBlockBoundaryCounts<int32_t>();
}

TEST_F(SprintzCodecTest, Int64BatchBlockBoundaryCounts) {
    CheckBlockBoundaryCounts<int64_t>();
}

TEST_F(SprintzCodecTest, FloatBatchBlockBoundaryCounts) {
    CheckBlockBoundaryCounts<float>();
}

TEST_F(SprintzCodecTest, DoubleBatchBlockBoundaryCounts) {
    CheckBlockBoundaryCounts<double>();
}

TEST_F(SprintzCodecTest, Int32BatchZeroCapacity) {
    CheckZeroCapacity<int32_t>();
}

TEST_F(SprintzCodecTest, Int64BatchZeroCapacity) {
    CheckZeroCapacity<int64_t>();
}

TEST_F(SprintzCodecTest, FloatBatchZeroCapacity) { CheckZeroCapacity<float>(); }

TEST_F(SprintzCodecTest, DoubleBatchZeroCapacity) {
    CheckZeroCapacity<double>();
}

TEST_F(SprintzCodecTest, Int32BatchEndOfStream) { CheckEndOfStream<int32_t>(); }

TEST_F(SprintzCodecTest, Int64BatchEndOfStream) { CheckEndOfStream<int64_t>(); }

TEST_F(SprintzCodecTest, FloatBatchEndOfStream) { CheckEndOfStream<float>(); }

TEST_F(SprintzCodecTest, DoubleBatchEndOfStream) { CheckEndOfStream<double>(); }

TEST_F(SprintzCodecTest, Int32BatchTruncatedStream) {
    CheckTruncatedStreams<int32_t>();
}

TEST_F(SprintzCodecTest, Int64BatchTruncatedStream) {
    CheckTruncatedStreams<int64_t>();
}

TEST_F(SprintzCodecTest, FloatBatchTruncatedStream) {
    CheckTruncatedStreams<float>();
}

TEST_F(SprintzCodecTest, DoubleBatchTruncatedStream) {
    CheckTruncatedStreams<double>();
}

TEST_F(SprintzCodecTest, Int32BatchReadExactChunks) {
    CheckReadExactChunks<int32_t>(1000, 37);
}

TEST_F(SprintzCodecTest, Int64BatchReadExactChunks) {
    CheckReadExactChunks<int64_t>(1000, 129);
}

TEST_F(SprintzCodecTest, FloatBatchReadExactChunks) {
    CheckReadExactChunks<float>(1000, 8);
}

TEST_F(SprintzCodecTest, DoubleBatchReadExactChunks) {
    CheckReadExactChunks<double>(1000, 1);
}

}  // namespace
