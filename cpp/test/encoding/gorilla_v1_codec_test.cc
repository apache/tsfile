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

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/gorilla_v1_decoder.h"
#include "encoding/gorilla_v1_encoder.h"

namespace storage {

namespace {

// Type-dispatched access to the GORILLA_V1 entry points, so the same test body
// covers FLOAT (single precision) and DOUBLE.
template <typename T>
struct V1Codec;

template <>
struct V1Codec<float> {
    typedef FloatGorillaV1Encoder Encoder;
    typedef FloatGorillaV1Decoder Decoder;

    static int read_one(Decoder& d, float& value, common::ByteStream& in) {
        return d.read_float(value, in);
    }
    static int read_batch(Decoder& d, float* out, int capacity, int& actual,
                          common::ByteStream& in) {
        return d.read_batch_float(out, capacity, actual, in);
    }
    static int read_exact(Decoder& d, float* out, int count,
                          common::ByteStream& in) {
        return d.read_exact_float(out, count, in);
    }
    static int skip(Decoder& d, int count, int& skipped,
                    common::ByteStream& in) {
        return d.skip_float(count, skipped, in);
    }
    static uint64_t bits(float value) {
        return static_cast<uint64_t>(
            static_cast<uint32_t>(common::float_to_int(value)));
    }
    static float from_bits(uint64_t bits) {
        uint32_t narrow = static_cast<uint32_t>(bits);
        float value = 0.0f;
        std::memcpy(&value, &narrow, sizeof(value));
        return value;
    }
};

template <>
struct V1Codec<double> {
    typedef DoubleGorillaV1Encoder Encoder;
    typedef DoubleGorillaV1Decoder Decoder;

    static int read_one(Decoder& d, double& value, common::ByteStream& in) {
        return d.read_double(value, in);
    }
    static int read_batch(Decoder& d, double* out, int capacity, int& actual,
                          common::ByteStream& in) {
        return d.read_batch_double(out, capacity, actual, in);
    }
    static int read_exact(Decoder& d, double* out, int count,
                          common::ByteStream& in) {
        return d.read_exact_double(out, count, in);
    }
    static int skip(Decoder& d, int count, int& skipped,
                    common::ByteStream& in) {
        return d.skip_double(count, skipped, in);
    }
    static uint64_t bits(double value) {
        return static_cast<uint64_t>(common::double_to_long(value));
    }
    static double from_bits(uint64_t bits) {
        int64_t narrow = static_cast<int64_t>(bits);
        double value = 0.0;
        std::memcpy(&value, &narrow, sizeof(value));
        return value;
    }
};

template <typename T>
std::vector<uint8_t> EncodeV1(const std::vector<T>& values) {
    typename V1Codec<T>::Encoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (size_t i = 0; i < values.size(); i++) {
        if (encoder.encode(values[i], stream) != common::E_OK) {
            return std::vector<uint8_t>();
        }
    }
    if (encoder.flush(stream) != common::E_OK) {
        return std::vector<uint8_t>();
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(stream.total_size()));
    if (!bytes.empty()) {
        uint32_t read_len = 0;
        stream.read_buf(bytes.data(), static_cast<uint32_t>(bytes.size()),
                        read_len);
        if (read_len != bytes.size()) {
            return std::vector<uint8_t>();
        }
    }
    return bytes;
}

std::vector<uint8_t> Slice(const std::vector<uint8_t>& bytes, size_t len) {
    return std::vector<uint8_t>(bytes.begin(), bytes.begin() + len);
}

void WrapStream(const std::vector<uint8_t>& bytes, common::ByteStream& in) {
    in.wrap_from(
        reinterpret_cast<const char*>(bytes.empty() ? nullptr : bytes.data()),
        static_cast<int32_t>(bytes.size()));
}

template <typename T>
struct Decoded {
    Decoded() : ret(common::E_OK) {}
    std::vector<T> values;
    int ret;
};

// Reference decoder: the per-value loop that Decoder::read_batch_* performs.
template <typename T>
Decoded<T> DecodePerValue(const std::vector<uint8_t>& bytes, int limit) {
    typename V1Codec<T>::Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    WrapStream(bytes, in);
    Decoded<T> out;
    while (static_cast<int>(out.values.size()) < limit &&
           decoder.has_remaining(in)) {
        T value;
        const int ret = V1Codec<T>::read_one(decoder, value, in);
        if (ret != common::E_OK) {
            out.ret = ret;
            return out;
        }
        out.values.push_back(value);
    }
    return out;
}

// Batch decoder: repeatedly calls read_batch_* with the given capacity cycle
// until the decoder reports that no further value can be produced.
template <typename T>
Decoded<T> DecodeBatched(const std::vector<uint8_t>& bytes, int limit,
                         const std::vector<int>& capacity_cycle) {
    typename V1Codec<T>::Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    WrapStream(bytes, in);
    Decoded<T> out;
    std::vector<T> buffer;
    size_t cycle_pos = 0;
    while (static_cast<int>(out.values.size()) < limit) {
        int capacity = capacity_cycle[cycle_pos % capacity_cycle.size()];
        cycle_pos++;
        if (capacity <= 0) {
            continue;
        }
        if (capacity > limit - static_cast<int>(out.values.size())) {
            capacity = limit - static_cast<int>(out.values.size());
        }
        buffer.resize(static_cast<size_t>(capacity));
        int actual = 0;
        const int ret = V1Codec<T>::read_batch(decoder, buffer.data(), capacity,
                                               actual, in);
        // A failing call still reports the values it decoded before the error,
        // exactly like Decoder::read_batch_*'s per-value loop.
        out.values.insert(out.values.end(), buffer.begin(),
                          buffer.begin() + actual);
        if (ret != common::E_OK) {
            out.ret = ret;
            return out;
        }
        if (actual < capacity) {
            break;
        }
    }
    return out;
}

// Reference skip: the per-value loop that Decoder::skip_* performs.
template <typename T>
int SkipPerValue(typename V1Codec<T>::Decoder& decoder, int count, int& skipped,
                 common::ByteStream& in) {
    skipped = 0;
    while (skipped < count && decoder.has_remaining(in)) {
        T value;
        const int ret = V1Codec<T>::read_one(decoder, value, in);
        if (ret != common::E_OK) {
            return ret;
        }
        ++skipped;
    }
    return common::E_OK;
}

template <typename T>
void ExpectSameValues(const std::vector<T>& expected,
                      const std::vector<T>& actual,
                      const std::string& context) {
    ASSERT_EQ(expected.size(), actual.size()) << context;
    for (size_t i = 0; i < expected.size(); i++) {
        EXPECT_EQ(V1Codec<T>::bits(expected[i]), V1Codec<T>::bits(actual[i]))
            << context << " index=" << i;
    }
}

template <typename T>
std::vector<T> MakeRandomValues(int count, uint64_t seed) {
    std::vector<T> values;
    values.reserve(static_cast<size_t>(count));
    uint64_t state = seed;
    for (int i = 0; i < count; i++) {
        state = state * UINT64_C(6364136223846793005) +
                UINT64_C(1442695040888963407);
        T value = V1Codec<T>::from_bits(state ^ (state >> 29));
        if (std::isnan(value)) {
            value = static_cast<T>(i % 977) * static_cast<T>(0.25);
        }
        values.push_back(value);
    }
    return values;
}

template <typename T>
std::vector<T> MakeSmoothValues(int count) {
    std::vector<T> values;
    values.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; i++) {
        const double x = static_cast<double>(i) / 100.0;
        values.push_back(static_cast<T>(std::sin(x) * 1000.0 +
                                        std::cos(x * 0.37) * 0.125 +
                                        static_cast<double>(i) * 0.001));
    }
    return values;
}

template <typename T>
std::vector<T> MakeRepeatedValues() {
    std::vector<T> values;
    for (int i = 0; i < 517; i++) {
        values.push_back(static_cast<T>(17.25));
    }
    for (int i = 0; i < 600; i++) {
        values.push_back(static_cast<T>(-3.5));
    }
    for (int i = 0; i < 300; i++) {
        values.push_back(static_cast<T>(i) * static_cast<T>(0.125));
    }
    return values;
}

template <typename T>
std::vector<T> MakeSpecialValues() {
    const T inf = std::numeric_limits<T>::infinity();
    std::vector<T> values;
    values.push_back(static_cast<T>(0));
    values.push_back(-static_cast<T>(0));
    values.push_back(static_cast<T>(1));
    values.push_back(static_cast<T>(-1));
    values.push_back(inf);
    values.push_back(-inf);
    values.push_back(std::numeric_limits<T>::denorm_min());
    values.push_back(std::numeric_limits<T>::max());
    values.push_back(-std::numeric_limits<T>::max());
    values.push_back(std::numeric_limits<T>::min());
    values.push_back(std::numeric_limits<T>::epsilon());
    values.push_back(static_cast<T>(1e-30));
    values.push_back(static_cast<T>(1e30));
    values.push_back(static_cast<T>(-1e-30));
    // Largest subnormal: adjacent bit pattern to the smallest normal.
    values.push_back(V1Codec<T>::from_bits(
        V1Codec<T>::bits(std::numeric_limits<T>::min()) - 1));
    values.push_back(static_cast<T>(0));
    values.push_back(inf);
    values.push_back(std::numeric_limits<T>::denorm_min());
    return values;
}

// Encodes @values, then checks that the per-value decoder, the batch decoder
// under several capacity cycles, and the input values all agree bit for bit.
template <typename T>
void RunParityCase(const std::vector<T>& values, const std::string& context) {
    ASSERT_FALSE(values.empty()) << context;
    const std::vector<uint8_t> bytes = EncodeV1(values);
    ASSERT_FALSE(bytes.empty()) << context;
    const int count = static_cast<int>(values.size());

    const Decoded<T> reference = DecodePerValue<T>(bytes, count + 8);
    ASSERT_EQ(reference.ret, common::E_OK) << context;
    ExpectSameValues(values, reference.values, context + " per-value");

    const std::vector<std::vector<int>> cycles = {
        std::vector<int>(1, 1),       std::vector<int>(1, 2),
        std::vector<int>(1, 3),       std::vector<int>(1, 7),
        std::vector<int>(1, 64),      std::vector<int>(1, 129),
        std::vector<int>(1, 1000000), std::vector<int>{5, 1, 9, 2, 17, 33},
        std::vector<int>{257, 1},
    };
    for (size_t c = 0; c < cycles.size(); c++) {
        const Decoded<T> batched =
            DecodeBatched<T>(bytes, count + 8, cycles[c]);
        ASSERT_EQ(batched.ret, common::E_OK) << context << " cycle=" << c;
        ExpectSameValues(values, batched.values,
                         context + " batch cycle=" + std::to_string(c));
    }
}

// Truncates @values' encoding at every prefix length and checks that the batch
// decoder reports the same outcome as the per-value loop.
template <typename T>
void RunTruncationParity(const std::vector<T>& values,
                         const std::string& context) {
    const std::vector<uint8_t> bytes = EncodeV1(values);
    ASSERT_FALSE(bytes.empty()) << context;
    const int limit = static_cast<int>(values.size()) + 8;
    for (size_t len = 0; len <= bytes.size(); len++) {
        const std::vector<uint8_t> prefix = Slice(bytes, len);
        const std::string label = context + " len=" + std::to_string(len);
        const Decoded<T> reference = DecodePerValue<T>(prefix, limit);
        const std::vector<int> cycle{3, 1, 5};
        const Decoded<T> batched = DecodeBatched<T>(prefix, limit, cycle);
        EXPECT_EQ(batched.ret, reference.ret) << label;
        ExpectSameValues(reference.values, batched.values, label);
    }
}

// Decodes an unwrapped (owning) ByteStream: the batch override must fall back
// to the per-value loop there.
template <typename T>
void RunUnwrappedFallback(const std::vector<T>& values,
                          const std::string& context) {
    typename V1Codec<T>::Encoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(encoder.encode(values[i], stream), common::E_OK) << context;
    }
    ASSERT_EQ(encoder.flush(stream), common::E_OK) << context;
    ASSERT_FALSE(stream.is_wrapped()) << context;

    typename V1Codec<T>::Decoder decoder;
    std::vector<T> decoded;
    std::vector<T> buffer(7);
    while (static_cast<int>(decoded.size()) < static_cast<int>(values.size())) {
        int actual = 0;
        ASSERT_EQ(V1Codec<T>::read_batch(decoder, buffer.data(),
                                         static_cast<int>(buffer.size()),
                                         actual, stream),
                  common::E_OK)
            << context;
        ASSERT_GT(actual, 0) << context;
        decoded.insert(decoded.end(), buffer.begin(), buffer.begin() + actual);
    }
    ExpectSameValues(values, decoded, context);
}

// Switches between batch and per-value reads mid-stream, in both orders.
template <typename T>
void RunHandoff(const std::vector<T>& values, int handoff,
                const std::string& context) {
    const std::vector<uint8_t> bytes = EncodeV1(values);
    ASSERT_FALSE(bytes.empty()) << context;
    const int count = static_cast<int>(values.size());
    ASSERT_LE(handoff, count) << context;

    {
        typename V1Codec<T>::Decoder decoder;
        common::ByteStream in(common::MOD_DEFAULT);
        WrapStream(bytes, in);
        std::vector<T> decoded(static_cast<size_t>(handoff));
        int actual = 0;
        ASSERT_EQ(V1Codec<T>::read_batch(decoder, decoded.data(), handoff,
                                         actual, in),
                  common::E_OK)
            << context;
        ASSERT_EQ(actual, handoff) << context;
        while (static_cast<int>(decoded.size()) < count &&
               decoder.has_remaining(in)) {
            T value;
            ASSERT_EQ(V1Codec<T>::read_one(decoder, value, in), common::E_OK)
                << context;
            decoded.push_back(value);
        }
        ExpectSameValues(values, decoded, context + " batch->per-value");
    }
    {
        typename V1Codec<T>::Decoder decoder;
        common::ByteStream in(common::MOD_DEFAULT);
        WrapStream(bytes, in);
        std::vector<T> decoded;
        while (static_cast<int>(decoded.size()) < handoff) {
            T value;
            ASSERT_EQ(V1Codec<T>::read_one(decoder, value, in), common::E_OK)
                << context;
            decoded.push_back(value);
        }
        std::vector<T> buffer(static_cast<size_t>(count - handoff));
        int actual = 0;
        ASSERT_EQ(
            V1Codec<T>::read_batch(decoder, buffer.data(),
                                   static_cast<int>(buffer.size()), actual, in),
            common::E_OK)
            << context;
        decoded.insert(decoded.end(), buffer.begin(), buffer.begin() + actual);
        ExpectSameValues(values, decoded, context + " per-value->batch");
    }
}

template <typename T>
void RunSkipCase(const std::vector<T>& values, int skip_count,
                 const std::string& context) {
    const std::vector<uint8_t> bytes = EncodeV1(values);
    ASSERT_FALSE(bytes.empty()) << context;
    const int count = static_cast<int>(values.size());
    ASSERT_GE(skip_count, 0) << context;
    ASSERT_LE(skip_count, count) << context;

    const int expected_skipped = skip_count;
    typename V1Codec<T>::Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    WrapStream(bytes, in);
    int skipped = -1;
    ASSERT_EQ(V1Codec<T>::skip(decoder, skip_count, skipped, in), common::E_OK)
        << context;
    ASSERT_EQ(skipped, expected_skipped) << context;

    std::vector<T> rest(static_cast<size_t>(count - skip_count));
    if (!rest.empty()) {
        ASSERT_EQ(V1Codec<T>::read_exact(decoder, rest.data(),
                                         static_cast<int>(rest.size()), in),
                  common::E_OK)
            << context;
        std::vector<T> expected(values.begin() + skip_count, values.end());
        ExpectSameValues(expected, rest, context);
    }
}

// Batches @values and checks skipping more values than the stream holds stops
// at the end marker without error.
template <typename T>
void RunSkipPastEnd(const std::vector<T>& values, const std::string& context) {
    const std::vector<uint8_t> bytes = EncodeV1(values);
    typename V1Codec<T>::Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    WrapStream(bytes, in);
    int skipped = 0;
    ASSERT_EQ(V1Codec<T>::skip(decoder, static_cast<int>(values.size()) + 10,
                               skipped, in),
              common::E_OK)
        << context;
    EXPECT_EQ(skipped, static_cast<int>(values.size())) << context;
    EXPECT_FALSE(decoder.has_remaining(in)) << context;
}

class GorillaV1CodecTest : public ::testing::Test {};

}  // namespace

// ── Java wire-format golden streams ────────────────────────────────────────
// Byte streams generated with the Java implementation's
// SinglePrecisionEncoderV1 / DoublePrecisionEncoderV1 so the C++ encoder and
// both C++ decode paths are checked against the Java wire format.

TEST_F(GorillaV1CodecTest, JavaGoldenFloatStreams) {
    const std::vector<uint8_t> one_two = {0x00, 0x00, 0x80, 0x3F, 0xC2,
                                          0x47, 0xFE, 0x22, 0x3F, 0xC0};
    const std::vector<float> one_two_values = {1.0f, 2.0f};
    EXPECT_EQ(EncodeV1(one_two_values), one_two);
    EXPECT_EQ(DecodePerValue<float>(one_two, 10).ret, common::E_OK);
    ExpectSameValues(one_two_values, DecodePerValue<float>(one_two, 10).values,
                     "java [1.0, 2.0] per-value");
    ExpectSameValues(
        one_two_values,
        DecodeBatched<float>(one_two, 10, std::vector<int>(1, 2)).values,
        "java [1.0, 2.0] batch");

    const std::vector<uint8_t> one_two_three = {0x00, 0x00, 0x80, 0x3F, 0xC2,
                                                0x47, 0xFE, 0x90, 0x73, 0xF8};
    const std::vector<float> three_values = {1.0f, 2.0f, 3.0f};
    EXPECT_EQ(EncodeV1(three_values), one_two_three);
    ExpectSameValues(
        three_values,
        DecodeBatched<float>(one_two_three, 10, std::vector<int>(1, 1)).values,
        "java [1.0, 2.0, 3.0] batch");

    const std::vector<uint8_t> single = {0x00, 0x00, 0xF0, 0x40, 0x9F, 0x98};
    const std::vector<float> single_value = {7.5f};
    EXPECT_EQ(EncodeV1(single_value), single);
    const Decoded<float> decoded =
        DecodeBatched<float>(single, 10, std::vector<int>(1, 8));
    ASSERT_EQ(decoded.ret, common::E_OK);
    ExpectSameValues(single_value, decoded.values, "java [7.5] batch");

    const std::vector<uint8_t> empty = {0x00, 0x00, 0xC0, 0x7F};
    EXPECT_EQ(EncodeV1(std::vector<float>()), empty);
    // The encoder writes only the raw NaN sentinel; both paths must report
    // E_BUF_NOT_ENOUGH and produce no value.
    EXPECT_EQ(DecodePerValue<float>(empty, 10).ret, common::E_BUF_NOT_ENOUGH);
    const Decoded<float> empty_batched =
        DecodeBatched<float>(empty, 10, std::vector<int>(1, 3));
    EXPECT_EQ(empty_batched.ret, common::E_BUF_NOT_ENOUGH);
    EXPECT_TRUE(empty_batched.values.empty());
}

TEST_F(GorillaV1CodecTest, JavaGoldenDoubleStreams) {
    const std::vector<uint8_t> one_two = {0x00, 0x00, 0x00, 0x00, 0x00,
                                          0x00, 0xF0, 0x3F, 0xC1, 0x17,
                                          0xFF, 0xF0, 0x85, 0xFF, 0xF0};
    const std::vector<double> one_two_values = {1.0, 2.0};
    EXPECT_EQ(EncodeV1(one_two_values), one_two);
    ExpectSameValues(one_two_values, DecodePerValue<double>(one_two, 10).values,
                     "java double [1.0, 2.0] per-value");
    ExpectSameValues(
        one_two_values,
        DecodeBatched<double>(one_two, 10, std::vector<int>(1, 2)).values,
        "java double [1.0, 2.0] batch");

    const std::vector<uint8_t> mixed = {
        0x18, 0x2D, 0x44, 0x54, 0xFB, 0x21, 0x09, 0x40, 0xC0, 0x7B, 0x80, 0x12,
        0x43, 0xF6, 0xA8, 0x88, 0x5A, 0x3C, 0x07, 0xDF, 0xC6, 0xFC, 0x87, 0x91,
        0x00, 0x0E, 0xB3, 0xC0, 0x73, 0xF9, 0x0F, 0x22, 0x00, 0x1D, 0x67};
    const std::vector<double> mixed_values = {3.141592653589793, -0.0, 1e300};
    EXPECT_EQ(EncodeV1(mixed_values), mixed);
    ExpectSameValues(
        mixed_values,
        DecodeBatched<double>(mixed, 10, std::vector<int>(1, 1)).values,
        "java double pi/-0/1e300 batch");

    const std::vector<uint8_t> extremes = {
        0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xEF, 0x7F, 0xBF,
        0xF7, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x60, 0xBF,
        0xFF, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02};
    const std::vector<double> extreme_values = {
        std::numeric_limits<double>::max(),
        std::numeric_limits<double>::denorm_min()};
    EXPECT_EQ(EncodeV1(extreme_values), extremes);
    ExpectSameValues(
        extreme_values,
        DecodeBatched<double>(extremes, 10, std::vector<int>(1, 5)).values,
        "java double max/denorm_min batch");

    const std::vector<uint8_t> neg_zero = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                                           0x00, 0x80, 0xC0, 0x1B, 0xFF, 0xF0};
    const std::vector<double> neg_zero_values = {-0.0};
    EXPECT_EQ(EncodeV1(neg_zero_values), neg_zero);
    ExpectSameValues(
        neg_zero_values,
        DecodeBatched<double>(neg_zero, 10, std::vector<int>(1, 4)).values,
        "java double -0.0 batch");

    const std::vector<uint8_t> empty = {0x00, 0x00, 0x00, 0x00,
                                        0x00, 0x00, 0xF8, 0x7F};
    EXPECT_EQ(EncodeV1(std::vector<double>()), empty);
    EXPECT_EQ(DecodePerValue<double>(empty, 10).ret, common::E_BUF_NOT_ENOUGH);
    EXPECT_EQ(DecodeBatched<double>(empty, 10, std::vector<int>(1, 2)).ret,
              common::E_BUF_NOT_ENOUGH);
}

TEST_F(GorillaV1CodecTest, JavaGoldenFirstValueNaN) {
    // Java encodes a leading NaN as the raw first value; the following value
    // is still decodable because only the lookahead treats NaN as the marker.
    const std::vector<uint8_t> bytes = {0x00, 0x00, 0xC0, 0x7F,
                                        0xA0, 0x18, 0x41, 0x80};
    const std::vector<float> values = {std::numeric_limits<float>::quiet_NaN(),
                                       1.5f};
    EXPECT_EQ(EncodeV1(values), bytes);
    const Decoded<float> reference = DecodePerValue<float>(bytes, 10);
    ASSERT_EQ(reference.ret, common::E_OK);
    ASSERT_EQ(reference.values.size(), 2u);
    ExpectSameValues(values, reference.values, "java [NaN, 1.5] per-value");
    const Decoded<float> batched =
        DecodeBatched<float>(bytes, 10, std::vector<int>(1, 1));
    ASSERT_EQ(batched.ret, common::E_OK);
    ExpectSameValues(values, batched.values, "java [NaN, 1.5] batch");
}

// ── Batch/per-value parity ─────────────────────────────────────────────────

TEST_F(GorillaV1CodecTest, FloatParityRandomValues) {
    RunParityCase(MakeRandomValues<float>(5000, UINT64_C(0x9e3779b97f4a7c15)),
                  "float random");
}

TEST_F(GorillaV1CodecTest, DoubleParityRandomValues) {
    RunParityCase(MakeRandomValues<double>(5000, UINT64_C(0x2545F4914F6CDD1D)),
                  "double random");
}

TEST_F(GorillaV1CodecTest, FloatParitySmoothValues) {
    RunParityCase(MakeSmoothValues<float>(4000), "float smooth");
}

TEST_F(GorillaV1CodecTest, DoubleParitySmoothValues) {
    RunParityCase(MakeSmoothValues<double>(4000), "double smooth");
}

TEST_F(GorillaV1CodecTest, FloatParityRepeatedRuns) {
    RunParityCase(MakeRepeatedValues<float>(), "float repeated");
}

TEST_F(GorillaV1CodecTest, DoubleParityRepeatedRuns) {
    RunParityCase(MakeRepeatedValues<double>(), "double repeated");
}

TEST_F(GorillaV1CodecTest, FloatParityConstant) {
    RunParityCase(std::vector<float>(10000, 3.141592653589793f),
                  "float constant");
    RunParityCase(std::vector<float>(513, 0.0f), "float zeros");
}

TEST_F(GorillaV1CodecTest, DoubleParityConstant) {
    RunParityCase(std::vector<double>(10000, 3.141592653589793),
                  "double constant");
    RunParityCase(std::vector<double>(513, -0.0), "double neg zeros");
}

TEST_F(GorillaV1CodecTest, FloatParitySpecialValues) {
    RunParityCase(MakeSpecialValues<float>(), "float special");
}

TEST_F(GorillaV1CodecTest, DoubleParitySpecialValues) {
    RunParityCase(MakeSpecialValues<double>(), "double special");
}

TEST_F(GorillaV1CodecTest, FloatParitySingleValue) {
    RunParityCase(std::vector<float>(1, 42.25f), "float single");
    RunParityCase(std::vector<float>(1, -0.0f), "float single -0");
    RunParityCase(std::vector<float>(1, std::numeric_limits<float>::infinity()),
                  "float single inf");
}

TEST_F(GorillaV1CodecTest, DoubleParitySingleValue) {
    RunParityCase(std::vector<double>(1, -1234.5), "double single");
    RunParityCase(std::vector<double>(1, std::numeric_limits<double>::min()),
                  "double single min normal");
}

TEST_F(GorillaV1CodecTest, ParityTwoValuesAndBoundaryLengths) {
    for (int count = 2; count <= 40; count++) {
        std::vector<float> values;
        for (int i = 0; i < count; i++) {
            values.push_back(static_cast<float>(i) * 0.75f - 5.0f);
        }
        RunParityCase(values, "float len=" + std::to_string(count));
    }
}

TEST_F(GorillaV1CodecTest, FloatParityLargeMagnitudeJumps) {
    std::vector<float> values;
    for (int i = 0; i < 2000; i++) {
        switch (i % 4) {
            case 0:
                values.push_back(static_cast<float>(i) * 1e30f);
                break;
            case 1:
                values.push_back(static_cast<float>(i) * 1e-30f);
                break;
            case 2:
                values.push_back(-static_cast<float>(i) * 1.0f);
                break;
            default:
                values.push_back(static_cast<float>(i % 7));
                break;
        }
    }
    RunParityCase(values, "float magnitude jumps");
}

// ── Truncated streams ──────────────────────────────────────────────────────

TEST_F(GorillaV1CodecTest, FloatTruncationParity) {
    RunTruncationParity(std::vector<float>{1.5f, 2.5f, 3.25f, -8.0f},
                        "float truncation");
    RunTruncationParity(MakeRepeatedValues<float>(), "float truncation long");
}

TEST_F(GorillaV1CodecTest, DoubleTruncationParity) {
    RunTruncationParity(std::vector<double>{1.5, 2.5, 3.25, -8.0},
                        "double truncation");
    RunTruncationParity(MakeSmoothValues<double>(200),
                        "double truncation long");
}

// ── Capacity and stream-boundary edges ─────────────────────────────────────

TEST_F(GorillaV1CodecTest, ZeroCapacityLeavesStateUntouched) {
    const std::vector<float> values = MakeSmoothValues<float>(50);
    const std::vector<uint8_t> bytes = EncodeV1(values);
    FloatGorillaV1Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    WrapStream(bytes, in);
    int actual = -1;
    EXPECT_EQ(decoder.read_batch_float(nullptr, 0, actual, in), common::E_OK);
    EXPECT_EQ(actual, 0);
    EXPECT_EQ(in.read_pos(), 0u);
    EXPECT_TRUE(decoder.has_remaining(in));

    std::vector<float> decoded(values.size());
    int got = 0;
    ASSERT_EQ(decoder.read_batch_float(
                  decoded.data(), static_cast<int>(decoded.size()), got, in),
              common::E_OK);
    ExpectSameValues(values, decoded, "float after zero capacity");

    const std::vector<double> dvalues = MakeSmoothValues<double>(50);
    const std::vector<uint8_t> dbytes = EncodeV1(dvalues);
    DoubleGorillaV1Decoder ddecoder;
    common::ByteStream din(common::MOD_DEFAULT);
    WrapStream(dbytes, din);
    EXPECT_EQ(ddecoder.read_batch_double(nullptr, 0, actual, din),
              common::E_OK);
    EXPECT_EQ(actual, 0);
}

TEST_F(GorillaV1CodecTest, CapacityBeyondRemainingValues) {
    const std::vector<float> values = MakeSmoothValues<float>(37);
    const std::vector<uint8_t> bytes = EncodeV1(values);
    FloatGorillaV1Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    WrapStream(bytes, in);

    std::vector<float> decoded(values.size() + 5);
    int actual = 0;
    ASSERT_EQ(decoder.read_batch_float(
                  decoded.data(), static_cast<int>(decoded.size()), actual, in),
              common::E_OK);
    ASSERT_EQ(actual, static_cast<int>(values.size()));
    decoded.resize(static_cast<size_t>(actual));
    ExpectSameValues(values, decoded, "float capacity beyond");

    // Further calls must report no values and keep returning E_OK.
    for (int i = 0; i < 3; i++) {
        actual = -1;
        EXPECT_EQ(decoder.read_batch_float(decoded.data(), 8, actual, in),
                  common::E_OK);
        EXPECT_EQ(actual, 0);
    }
}

TEST_F(GorillaV1CodecTest, ReadExactAcrossMultipleCalls) {
    const std::vector<double> values = MakeSmoothValues<double>(1000);
    const std::vector<uint8_t> bytes = EncodeV1(values);
    DoubleGorillaV1Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    WrapStream(bytes, in);
    std::vector<double> decoded(values.size());
    ASSERT_EQ(decoder.read_exact_double(decoded.data(), 300, in), common::E_OK);
    ASSERT_EQ(
        decoder.read_exact_double(decoded.data() + 300,
                                  static_cast<int>(decoded.size()) - 300, in),
        common::E_OK);
    ExpectSameValues(values, decoded, "double read_exact chunks");
}

// The readers decode each page with a reset decoder over a freshly wrapped
// page buffer; make sure a decoder can be reused that way after the NaN
// sentinel of the previous page has been consumed.
TEST_F(GorillaV1CodecTest, ResetBetweenPages) {
    const std::vector<double> page1 = MakeSmoothValues<double>(300);
    const std::vector<double> page2 = MakeRepeatedValues<double>();
    const std::vector<uint8_t> bytes1 = EncodeV1(page1);
    const std::vector<uint8_t> bytes2 = EncodeV1(page2);
    DoubleGorillaV1Decoder decoder;

    std::vector<double> decoded1(page1.size());
    {
        common::ByteStream in(common::MOD_DEFAULT);
        WrapStream(bytes1, in);
        ASSERT_EQ(decoder.read_exact_double(
                      decoded1.data(), static_cast<int>(decoded1.size()), in),
                  common::E_OK);
        ExpectSameValues(page1, decoded1, "page 1");
    }

    decoder.reset();
    std::vector<double> decoded2(page2.size());
    {
        common::ByteStream in(common::MOD_DEFAULT);
        WrapStream(bytes2, in);
        ASSERT_EQ(decoder.read_exact_double(
                      decoded2.data(), static_cast<int>(decoded2.size()), in),
                  common::E_OK);
        ExpectSameValues(page2, decoded2, "page 2");
    }

    const std::vector<float> fpage1 = MakeSmoothValues<float>(300);
    const std::vector<float> fpage2 = MakeRepeatedValues<float>();
    const std::vector<uint8_t> fbytes1 = EncodeV1(fpage1);
    const std::vector<uint8_t> fbytes2 = EncodeV1(fpage2);
    FloatGorillaV1Decoder fdecoder;
    std::vector<float> fdecoded1(fpage1.size());
    {
        common::ByteStream in(common::MOD_DEFAULT);
        WrapStream(fbytes1, in);
        ASSERT_EQ(fdecoder.read_exact_float(
                      fdecoded1.data(), static_cast<int>(fdecoded1.size()), in),
                  common::E_OK);
        ExpectSameValues(fpage1, fdecoded1, "float page 1");
    }
    fdecoder.reset();
    std::vector<float> fdecoded2(fpage2.size());
    {
        common::ByteStream in(common::MOD_DEFAULT);
        WrapStream(fbytes2, in);
        ASSERT_EQ(fdecoder.read_exact_float(
                      fdecoded2.data(), static_cast<int>(fdecoded2.size()), in),
                  common::E_OK);
        ExpectSameValues(fpage2, fdecoded2, "float page 2");
    }
}

// ── Unwrapped streams fall back to the per-value loop ──────────────────────

TEST_F(GorillaV1CodecTest, FloatUnwrappedFallback) {
    RunUnwrappedFallback(MakeSmoothValues<float>(300), "float unwrapped");
}

TEST_F(GorillaV1CodecTest, DoubleUnwrappedFallback) {
    RunUnwrappedFallback(MakeRepeatedValues<double>(), "double unwrapped");
}

// ── Mixed per-value / batch reads ──────────────────────────────────────────

TEST_F(GorillaV1CodecTest, BatchPerValueHandoff) {
    const std::vector<float> values = MakeSmoothValues<float>(777);
    RunHandoff(values, 1, "float handoff 1");
    RunHandoff(values, 256, "float handoff 256");
    RunHandoff(values, 776, "float handoff 776");

    const std::vector<double> dvalues = MakeSmoothValues<double>(777);
    RunHandoff(dvalues, 1, "double handoff 1");
    RunHandoff(dvalues, 256, "double handoff 256");
    RunHandoff(dvalues, 776, "double handoff 776");
}

// ── Skip ──────────────────────────────────────────────────────────────────

TEST_F(GorillaV1CodecTest, FloatSkipParity) {
    const std::vector<float> values = MakeSmoothValues<float>(1500);
    RunSkipCase(values, 0, "float skip 0");
    RunSkipCase(values, 1, "float skip 1");
    RunSkipCase(values, 129, "float skip 129");
    RunSkipCase(values, 1499, "float skip last");
    RunSkipCase(values, 1500, "float skip all");
    RunSkipPastEnd(values, "float skip past end");
}

TEST_F(GorillaV1CodecTest, DoubleSkipParity) {
    const std::vector<double> values = MakeRepeatedValues<double>();
    RunSkipCase(values, 0, "double skip 0");
    RunSkipCase(values, 517, "double skip 517");
    RunSkipCase(values, 777, "double skip 777");
    RunSkipCase(values, 1416, "double skip last");
    RunSkipPastEnd(values, "double skip past end");

    // Skip must consume the same values as the per-value skip loop.
    const std::vector<uint8_t> bytes = EncodeV1(values);
    DoubleGorillaV1Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    WrapStream(bytes, in);
    int skipped = 0;
    ASSERT_EQ(decoder.skip_double(600, skipped, in), common::E_OK);
    ASSERT_EQ(skipped, 600);

    // Same skip through the per-value reference loop: both must stop at the
    // same stream position.
    const std::vector<uint8_t> bytes2 = EncodeV1(values);
    DoubleGorillaV1Decoder ref_decoder;
    common::ByteStream ref_in(common::MOD_DEFAULT);
    WrapStream(bytes2, ref_in);
    int ref_skipped = 0;
    ASSERT_EQ(SkipPerValue<double>(ref_decoder, 600, ref_skipped, ref_in),
              common::E_OK);
    EXPECT_EQ(ref_skipped, skipped);
    EXPECT_EQ(ref_in.read_pos(), in.read_pos());

    std::vector<double> rest(values.size() - 600);
    ASSERT_EQ(decoder.read_exact_double(rest.data(),
                                        static_cast<int>(rest.size()), in),
              common::E_OK);
    std::vector<double> expected(values.begin() + 600, values.end());
    ExpectSameValues(expected, rest, "double skip then read");
}

// ── NaN handling ──────────────────────────────────────────────────────────

TEST_F(GorillaV1CodecTest, BatchStopsAtEmbeddedNaN) {
    // GORILLA_V1 cannot carry a mid-stream NaN: the encoder writes it with the
    // same canonical bit pattern as the flush sentinel, so the decoder treats
    // it as the end of the stream. The per-value loop then stops fetching
    // bytes; a batch must stop at the marker instead of emitting it or
    // decoding whatever follows.
    std::vector<float> values = {1.5f, std::numeric_limits<float>::quiet_NaN(),
                                 2.5f, 3.5f};
    const std::vector<uint8_t> bytes = EncodeV1(values);
    FloatGorillaV1Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    WrapStream(bytes, in);

    std::vector<float> decoded(16);
    int actual = 0;
    ASSERT_EQ(decoder.read_batch_float(decoded.data(), 16, actual, in),
              common::E_OK);
    ASSERT_EQ(actual, 1);
    EXPECT_EQ(V1Codec<float>::bits(decoded[0]), V1Codec<float>::bits(1.5f));
    actual = -1;
    EXPECT_EQ(decoder.read_batch_float(decoded.data(), 16, actual, in),
              common::E_OK);
    EXPECT_EQ(actual, 0);

    std::vector<double> dvalues = {
        1.5, std::numeric_limits<double>::quiet_NaN(), 2.5};
    const std::vector<uint8_t> dbytes = EncodeV1(dvalues);
    DoubleGorillaV1Decoder ddecoder;
    common::ByteStream din(common::MOD_DEFAULT);
    WrapStream(dbytes, din);
    std::vector<double> ddecoded(16);
    ASSERT_EQ(ddecoder.read_batch_double(ddecoded.data(), 16, actual, din),
              common::E_OK);
    ASSERT_EQ(actual, 1);
    EXPECT_EQ(V1Codec<double>::bits(ddecoded[0]), V1Codec<double>::bits(1.5));
}

}  // namespace storage
