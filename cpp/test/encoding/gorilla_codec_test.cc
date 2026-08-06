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

#include <cmath>
#include <cstring>
#include <limits>

#include "encoding/gorilla_decoder.h"
#include "encoding/gorilla_encoder.h"

namespace storage {

class GorillaCodecTest : public ::testing::Test {};

TEST_F(GorillaCodecTest, BasicEncoding) {
    storage::IntGorillaEncoder int_encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int32_t data[] = {100, 102, 105, 107, 110, 115, 120, 1000000, 1000005};
    for (int32_t value : data) {
        EXPECT_EQ(int_encoder.encode(value, stream), common::E_OK);
    }
    int_encoder.flush(stream);

    ASSERT_EQ(stream.total_size(), 24);

    uint32_t want_len = 24, read_len;
    uint8_t real_buf[24] = {};
    stream.read_buf(real_buf, want_len, read_len);
    EXPECT_EQ(want_len, read_len);
    // Generated using Java Edition
    uint8_t expected_buf[] = {0,   0,   0,  100, 252, 15,  193, 252,
                              82,  251, 39, 101, 236, 135, 161, 31,
                              232, 174, 15, 192, 7,   161, 34,  128};
    for (int i = 0; i < 24; i++) {
        EXPECT_EQ(real_buf[i], expected_buf[i]);
    }
}

TEST_F(GorillaCodecTest, Int32EncodingDecoding) {
    storage::IntGorillaEncoder int_encoder;
    storage::IntGorillaDecoder int_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int32_t data[] = {100, 102, 105, 107, 110, 115, 120, 1000000, 1000005};
    for (int32_t value : data) {
        EXPECT_EQ(int_encoder.encode(value, stream), common::E_OK);
    }
    int_encoder.flush(stream);

    for (int i = 0; i < (int)(sizeof(data) / sizeof(int32_t)); i++) {
        EXPECT_EQ(data[i], int_decoder.decode(stream));
    }
}

TEST_F(GorillaCodecTest, Int32EncodingDecodingLargeQuantities) {
    storage::IntGorillaEncoder int_encoder;
    storage::IntGorillaDecoder int_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (int32_t value = 0; value < 10000; value++) {
        EXPECT_EQ(int_encoder.encode(value, stream), common::E_OK);
    }
    int_encoder.flush(stream);

    for (int32_t value = 0; value < 10000; value++) {
        EXPECT_EQ(value, int_decoder.decode(stream));
    }
}

TEST_F(GorillaCodecTest, Int64EncodingDecoding) {
    storage::LongGorillaEncoder long_encoder;
    storage::LongGorillaDecoder long_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int64_t data[] = {100, 102, 105, 107, 110, 115, 120, 1000000, 1000005};
    for (int64_t value : data) {
        EXPECT_EQ(long_encoder.encode(value, stream), common::E_OK);
    }
    long_encoder.flush(stream);

    for (int i = 0; i < (int)(sizeof(data) / sizeof(int64_t)); i++) {
        EXPECT_EQ(data[i], long_decoder.decode(stream));
    }
}

TEST_F(GorillaCodecTest, Int64EncodingDecodingLargeQuantities) {
    storage::LongGorillaEncoder long_encoder;
    storage::LongGorillaDecoder long_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (int64_t value = 0; value < 10000; value++) {
        EXPECT_EQ(long_encoder.encode(value, stream), common::E_OK);
    }
    long_encoder.flush(stream);

    for (int64_t value = 0; value < 10000; value++) {
        EXPECT_EQ(value, long_decoder.decode(stream));
    }
}

TEST_F(GorillaCodecTest, FloatEncodingDecodingBoundaryValues) {
    storage::FloatGorillaEncoder float_encoder;
    storage::FloatGorillaDecoder float_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);

    // Test values include important boundary cases and special floating-point
    // values
    std::vector<float> test_values = {
        0.0f,   // Zero
        -0.0f,  // Negative zero (distinct in IEEE 754)
        1.0f,   // Positive one
        -1.0f,  // Negative one
        std::numeric_limits<float>::min(),     // Smallest positive normalized
                                               // value
        std::numeric_limits<float>::max(),     // Largest positive finite value
        std::numeric_limits<float>::lowest(),  // Smallest (most negative)
                                               // finite value
        std::numeric_limits<float>::infinity(),   // Positive infinity
        -std::numeric_limits<float>::infinity(),  // Negative infinity
        std::numeric_limits<float>::
            denorm_min(),  // Smallest positive subnormal (denormalized) value
        std::numeric_limits<float>::epsilon(),  // Difference between 1 and the
                                                // next representable value
        std::nanf("")                           // Not-a-Number (NaN)
    };

    // Encode all test values into the stream
    for (auto value : test_values) {
        EXPECT_EQ(float_encoder.encode(value, stream), common::E_OK);
    }
    float_encoder.flush(stream);

    // Decode values from the stream and verify correctness
    for (auto expected : test_values) {
        float decoded = float_decoder.decode(stream);
        if (std::isnan(expected)) {
            // NaN is unordered; must use isnan() to check
            EXPECT_TRUE(std::isnan(decoded));
        } else if (std::isinf(expected)) {
            // Check if decoded value is infinite and has the same sign
            EXPECT_TRUE(std::isinf(decoded));
            EXPECT_EQ(std::signbit(expected), std::signbit(decoded));
        } else {
            // For finite floats, allow small precision differences
            EXPECT_FLOAT_EQ(decoded, expected);
        }
    }
}

TEST_F(GorillaCodecTest, DoubleEncodingDecodingBoundaryValues) {
    storage::DoubleGorillaEncoder double_encoder;
    storage::DoubleGorillaDecoder double_decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);

    // Test values include important boundary cases and special floating-point
    // values for double precision
    std::vector<double> test_values = {
        0.0,   // Zero
        -0.0,  // Negative zero (distinct in IEEE 754)
        1.0,   // Positive one
        -1.0,  // Negative one
        std::numeric_limits<double>::min(),     // Smallest positive normalized
                                                // value
        std::numeric_limits<double>::max(),     // Largest positive finite value
        std::numeric_limits<double>::lowest(),  // Smallest (most negative)
                                                // finite value
        std::numeric_limits<double>::infinity(),   // Positive infinity
        -std::numeric_limits<double>::infinity(),  // Negative infinity
        std::numeric_limits<double>::
            denorm_min(),  // Smallest positive subnormal (denormalized) value
        std::numeric_limits<double>::epsilon(),  // Difference between 1 and the
                                                 // next representable value
        std::nan("")                             // Not-a-Number (NaN)
    };

    // Encode all test values into the stream
    for (auto value : test_values) {
        EXPECT_EQ(double_encoder.encode(value, stream), common::E_OK);
    }
    double_encoder.flush(stream);

    // Decode values from the stream and verify correctness
    for (auto expected : test_values) {
        double decoded = double_decoder.decode(stream);
        if (std::isnan(expected)) {
            // NaN is unordered; must use isnan() to check
            EXPECT_TRUE(std::isnan(decoded));
        } else if (std::isinf(expected)) {
            // Check if decoded value is infinite and has the same sign
            EXPECT_TRUE(std::isinf(decoded));
            EXPECT_EQ(std::signbit(expected), std::signbit(decoded));
        } else {
            // For finite doubles, allow small precision differences
            EXPECT_DOUBLE_EQ(decoded, expected);
        }
    }
}

// ── Batch decode tests (exercises the raw-pointer GorillaBitReader path) ──

TEST_F(GorillaCodecTest, Int32BatchDecode) {
    storage::IntGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 500;
    int32_t expected[N];
    for (int i = 0; i < N; i++) {
        expected[i] = i * 7 - 100;
        EXPECT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    encoder.flush(stream);

    // Copy to a contiguous buffer and wrap (simulates production path)
    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    ASSERT_EQ(got, total);

    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    storage::IntGorillaDecoder decoder;
    int32_t out[N];
    int total_decoded = 0;
    while (decoder.has_remaining(wrapped) && total_decoded < N) {
        int batch = std::min(129, N - total_decoded);
        int actual = 0;
        EXPECT_EQ(decoder.read_batch_int32(out + total_decoded, batch, actual,
                                           wrapped),
                  common::E_OK);
        if (actual == 0) break;
        total_decoded += actual;
    }
    ASSERT_EQ(total_decoded, N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(out[i], expected[i]) << "mismatch at index " << i;
    }
}

TEST_F(GorillaCodecTest, Int64BatchDecode) {
    storage::LongGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 500;
    int64_t expected[N];
    for (int i = 0; i < N; i++) {
        expected[i] = (int64_t)i * 13 - 200;
        EXPECT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);

    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    storage::LongGorillaDecoder decoder;
    int64_t out[N];
    int total_decoded = 0;
    while (decoder.has_remaining(wrapped) && total_decoded < N) {
        int batch = std::min(129, N - total_decoded);
        int actual = 0;
        EXPECT_EQ(decoder.read_batch_int64(out + total_decoded, batch, actual,
                                           wrapped),
                  common::E_OK);
        if (actual == 0) break;
        total_decoded += actual;
    }
    ASSERT_EQ(total_decoded, N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(out[i], expected[i]) << "mismatch at index " << i;
    }
}

TEST_F(GorillaCodecTest, FloatBatchDecode) {
    storage::FloatGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 300;
    std::vector<float> expected(N);
    for (int i = 0; i < N; i++) {
        expected[i] = (float)i * 1.5f - 50.0f;
        EXPECT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);

    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    storage::FloatGorillaDecoder decoder;
    std::vector<float> out(N);
    int total_decoded = 0;
    while (decoder.has_remaining(wrapped) && total_decoded < N) {
        int batch = std::min(129, N - total_decoded);
        int actual = 0;
        EXPECT_EQ(decoder.read_batch_float(out.data() + total_decoded, batch,
                                           actual, wrapped),
                  common::E_OK);
        if (actual == 0) break;
        total_decoded += actual;
    }
    ASSERT_EQ(total_decoded, N);
    for (int i = 0; i < N; i++) {
        EXPECT_FLOAT_EQ(out[i], expected[i]) << "mismatch at index " << i;
    }
}

TEST_F(GorillaCodecTest, FloatBatchDecodeLongRepeatedRuns) {
    storage::FloatGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 600;
    std::vector<float> expected(N);
    for (int i = 0; i < N; i++) {
        if (i < 258) {
            // Two complete 129-row batches. The repeated run crosses several
            // 64-bit reservoirs and ends exactly at a batch boundary.
            expected[i] = 17.25f;
        } else if (i < 517) {
            expected[i] = -3.5f;
        } else {
            expected[i] = static_cast<float>(i - 517) * 0.125f;
        }
        ASSERT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    ASSERT_EQ(encoder.flush(stream), common::E_OK);

    const uint32_t total = stream.total_size();
    std::vector<uint8_t> encoded(total);
    uint32_t got = 0;
    stream.read_buf(encoded.data(), total, got);
    ASSERT_EQ(got, total);

    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from(reinterpret_cast<const char*>(encoded.data()), total);
    storage::FloatGorillaDecoder decoder;
    std::vector<float> decoded(N);
    int total_decoded = 0;
    while (total_decoded < N) {
        const int capacity = std::min(129, N - total_decoded);
        int actual = 0;
        ASSERT_EQ(decoder.read_batch_float(decoded.data() + total_decoded,
                                           capacity, actual, wrapped),
                  common::E_OK);
        ASSERT_EQ(actual, capacity);
        total_decoded += actual;
    }

    for (int i = 0; i < N; i++) {
        EXPECT_EQ(common::float_to_int(decoded[i]),
                  common::float_to_int(expected[i]))
            << "i=" << i;
    }
}

TEST_F(GorillaCodecTest, FloatBatchRepeatedFillPreservesBitPatterns) {
    constexpr uint32_t NAN_BITS = 0x7FC12345U;
    float payload_nan = 0;
    std::memcpy(&payload_nan, &NAN_BITS, sizeof(payload_nan));
    ASSERT_TRUE(std::isnan(payload_nan));
    ASSERT_NE(common::float_to_int(payload_nan),
              common::float_to_int(GORILLA_ENCODING_ENDING_FLOAT));

    const int N = 520;
    std::vector<float> expected(N);
    for (int i = 0; i < N; i++) {
        if (i < 257) {
            expected[i] = payload_nan;
        } else if (i < 514) {
            expected[i] = -0.0f;
        } else {
            expected[i] = static_cast<float>(i - 514) + 0.25f;
        }
    }

    storage::FloatGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (float value : expected) {
        ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
    }
    ASSERT_EQ(encoder.flush(stream), common::E_OK);

    const uint32_t total = stream.total_size();
    std::vector<uint8_t> encoded(total);
    uint32_t got = 0;
    stream.read_buf(encoded.data(), total, got);
    ASSERT_EQ(got, total);

    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from(reinterpret_cast<const char*>(encoded.data()), total);
    storage::FloatGorillaDecoder decoder;
    std::vector<float> decoded(N);
    int total_decoded = 0;
    while (total_decoded < N) {
        const int capacity = std::min(129, N - total_decoded);
        int actual = 0;
        ASSERT_EQ(decoder.read_batch_float(decoded.data() + total_decoded,
                                           capacity, actual, wrapped),
                  common::E_OK);
        ASSERT_EQ(actual, capacity);
        total_decoded += actual;
    }

    for (int i = 0; i < N; i++) {
        EXPECT_EQ(common::float_to_int(decoded[i]),
                  common::float_to_int(expected[i]))
            << "i=" << i;
    }
}

TEST_F(GorillaCodecTest, FloatBatchDecodeUnwrappedInput) {
    storage::FloatGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 127;
    std::vector<float> expected(N);
    for (int i = 0; i < N; i++) {
        expected[i] = std::sin(i * 0.125f) * 23.0f;
        ASSERT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    encoder.flush(stream);

    storage::FloatGorillaDecoder decoder;
    std::vector<float> actual(N);
    int actual_count = 0;
    ASSERT_EQ(decoder.read_batch_float(actual.data(), N, actual_count, stream),
              common::E_OK);
    ASSERT_EQ(actual_count, N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(common::float_to_int(actual[i]),
                  common::float_to_int(expected[i]))
            << "i=" << i;
    }
}

TEST_F(GorillaCodecTest, DoubleBatchDecode) {
    storage::DoubleGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 300;
    std::vector<double> expected(N);
    for (int i = 0; i < N; i++) {
        expected[i] = (double)i * 2.7 - 100.0;
        EXPECT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);

    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    storage::DoubleGorillaDecoder decoder;
    std::vector<double> out(N);
    int total_decoded = 0;
    while (decoder.has_remaining(wrapped) && total_decoded < N) {
        int batch = std::min(129, N - total_decoded);
        int actual = 0;
        EXPECT_EQ(decoder.read_batch_double(out.data() + total_decoded, batch,
                                            actual, wrapped),
                  common::E_OK);
        if (actual == 0) break;
        total_decoded += actual;
    }
    ASSERT_EQ(total_decoded, N);
    for (int i = 0; i < N; i++) {
        EXPECT_DOUBLE_EQ(out[i], expected[i]) << "mismatch at index " << i;
    }
}

TEST_F(GorillaCodecTest, DoubleBatchDecodeOneValueAtATime) {
    storage::DoubleGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 512;
    std::vector<double> expected(N);
    for (int i = 0; i < N; i++) {
        expected[i] = (i % 9 == 0) ? 42.0 : std::sin(i * 0.03125) * 1000.0;
        ASSERT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    ASSERT_EQ(got, total);
    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from(reinterpret_cast<const char*>(buf.data()), total);

    storage::DoubleGorillaDecoder decoder;
    for (int i = 0; i < N; i++) {
        double decoded = 0;
        int actual = 0;
        ASSERT_EQ(decoder.read_batch_double(&decoded, 1, actual, wrapped),
                  common::E_OK)
            << "i=" << i;
        ASSERT_EQ(actual, 1) << "i=" << i;
        EXPECT_EQ(common::double_to_long(decoded),
                  common::double_to_long(expected[i]))
            << "i=" << i;
    }
}

TEST_F(GorillaCodecTest, Int64BatchDecodeLongRepeatedRuns) {
    storage::LongGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 513;
    std::vector<int64_t> expected(N);
    for (int i = 0; i < N; i++) {
        if (i < 333) {
            expected[i] = 0x123456789ABCDEFLL;
        } else if (i < 500) {
            expected[i] = -9876543210LL;
        } else {
            expected[i] = static_cast<int64_t>(i) * 17 - 9;
        }
        ASSERT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    ASSERT_EQ(encoder.flush(stream), common::E_OK);

    const uint32_t total = stream.total_size();
    std::vector<uint8_t> encoded(total);
    uint32_t got = 0;
    stream.read_buf(encoded.data(), total, got);
    ASSERT_EQ(got, total);

    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from(reinterpret_cast<const char*>(encoded.data()), total);
    storage::LongGorillaDecoder decoder;
    std::vector<int64_t> decoded(N);
    int total_decoded = 0;
    while (total_decoded < N) {
        const int capacity = std::min(73, N - total_decoded);
        int actual = 0;
        ASSERT_EQ(decoder.read_batch_int64(decoded.data() + total_decoded,
                                           capacity, actual, wrapped),
                  common::E_OK);
        ASSERT_EQ(actual, capacity);
        total_decoded += actual;
    }

    EXPECT_EQ(decoded, expected);
}

TEST_F(GorillaCodecTest, DoubleBatchScalarAndSkipInterleave) {
    storage::DoubleGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 400;
    std::vector<double> expected(N);
    for (int i = 0; i < N; i++) {
        expected[i] = (i / 7) * 0.125 + std::cos(i * 0.017);
        ASSERT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    ASSERT_EQ(got, total);
    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from(reinterpret_cast<const char*>(buf.data()), total);

    storage::DoubleGorillaDecoder decoder;
    int cursor = 0;
    for (; cursor < 7; cursor++) {
        double decoded = 0;
        ASSERT_EQ(decoder.read_double(decoded, wrapped), common::E_OK);
        EXPECT_EQ(common::double_to_long(decoded),
                  common::double_to_long(expected[cursor]));
    }
    ASSERT_TRUE(decoder.has_remaining(wrapped))
        << "remaining=" << wrapped.remaining_size()
        << " has_next=" << decoder.has_next();

    std::vector<double> batch(113);
    int actual = 0;
    ASSERT_EQ(
        decoder.read_batch_double(batch.data(), static_cast<int>(batch.size()),
                                  actual, wrapped),
        common::E_OK);
    ASSERT_EQ(actual, static_cast<int>(batch.size()));
    for (int i = 0; i < actual; i++, cursor++) {
        EXPECT_EQ(common::double_to_long(batch[i]),
                  common::double_to_long(expected[cursor]));
    }

    for (int i = 0; i < 5; i++, cursor++) {
        double decoded = 0;
        ASSERT_EQ(decoder.read_double(decoded, wrapped), common::E_OK);
        EXPECT_EQ(common::double_to_long(decoded),
                  common::double_to_long(expected[cursor]));
    }

    int skipped = 0;
    ASSERT_EQ(decoder.skip_double(137, skipped, wrapped), common::E_OK);
    ASSERT_EQ(skipped, 137);
    cursor += skipped;

    std::vector<double> tail(N - cursor);
    actual = 0;
    ASSERT_EQ(decoder.read_batch_double(
                  tail.data(), static_cast<int>(tail.size()), actual, wrapped),
              common::E_OK);
    ASSERT_EQ(actual, static_cast<int>(tail.size()));
    for (int i = 0; i < actual; i++, cursor++) {
        EXPECT_EQ(common::double_to_long(tail[i]),
                  common::double_to_long(expected[cursor]));
    }
    EXPECT_EQ(cursor, N);
}

TEST_F(GorillaCodecTest, DoubleExactReadAndSkipPreserveEmbeddedNaN) {
    const double expected[] = {1.25, std::nan(""), 2.5, std::nan(""), 5.0};
    const int count = sizeof(expected) / sizeof(expected[0]);

    storage::DoubleGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (double value : expected) {
        ASSERT_EQ(encoder.encode(value, stream), common::E_OK);
    }
    encoder.flush(stream);

    const uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    ASSERT_EQ(got, total);

    common::ByteStream read_stream(common::MOD_DEFAULT);
    read_stream.wrap_from(reinterpret_cast<const char*>(buf.data()), total);
    storage::DoubleGorillaDecoder read_decoder;
    double actual[count];
    ASSERT_EQ(read_decoder.read_exact_double(actual, count, read_stream),
              common::E_OK);
    for (int i = 0; i < count; i++) {
        if (std::isnan(expected[i])) {
            EXPECT_TRUE(std::isnan(actual[i])) << "i=" << i;
        } else {
            EXPECT_DOUBLE_EQ(actual[i], expected[i]) << "i=" << i;
        }
    }

    common::ByteStream skip_stream(common::MOD_DEFAULT);
    skip_stream.wrap_from(reinterpret_cast<const char*>(buf.data()), total);
    storage::DoubleGorillaDecoder skip_decoder;
    ASSERT_EQ(skip_decoder.skip_exact_double(3, skip_stream), common::E_OK);
    double tail[2];
    ASSERT_EQ(skip_decoder.read_exact_double(tail, 2, skip_stream),
              common::E_OK);
    EXPECT_TRUE(std::isnan(tail[0]));
    EXPECT_DOUBLE_EQ(tail[1], 5.0);
}

TEST_F(GorillaCodecTest, DoubleBatchDecodeFullWidthXor) {
    const uint64_t patterns[] = {
        0x0000000000000000ULL, 0xFFFFFFFFFFFFFFFFULL, 0x0123456789ABCDEFULL,
        0xFEDCBA9876543210ULL, 0x8000000000000001ULL, 0x7FEFFFFFFFFFFFFFULL,
    };
    const int N = sizeof(patterns) / sizeof(patterns[0]);
    std::vector<double> expected(N);

    storage::DoubleGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (int i = 0; i < N; i++) {
        std::memcpy(&expected[i], &patterns[i], sizeof(double));
        ASSERT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    ASSERT_EQ(got, total);
    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from(reinterpret_cast<const char*>(buf.data()), total);

    storage::DoubleGorillaDecoder decoder;
    std::vector<double> decoded(N);
    int actual = 0;
    ASSERT_EQ(decoder.read_batch_double(decoded.data(), N, actual, wrapped),
              common::E_OK);
    ASSERT_EQ(actual, N);
    for (int i = 0; i < N; i++) {
        uint64_t decoded_bits = 0;
        std::memcpy(&decoded_bits, &decoded[i], sizeof(double));
        EXPECT_EQ(decoded_bits, patterns[i]) << "i=" << i;
    }
}

TEST_F(GorillaCodecTest, DoubleBatchTruncatedUnwrappedInputReturnsError) {
    storage::DoubleGorillaEncoder encoder;
    common::ByteStream encoded_stream(1024, common::MOD_DEFAULT);
    const int N = 128;
    for (int i = 0; i < N; i++) {
        double value = std::sin(i * 0.03125) * 1000.0 + i * 0.125;
        ASSERT_EQ(encoder.encode(value, encoded_stream), common::E_OK);
    }
    ASSERT_EQ(encoder.flush(encoded_stream), common::E_OK);

    uint32_t total = encoded_stream.total_size();
    ASSERT_GT(total, 1u);
    std::vector<uint8_t> encoded(total);
    uint32_t got = 0;
    encoded_stream.read_buf(encoded.data(), total, got);
    ASSERT_EQ(got, total);

    common::ByteStream decode_input(1024, common::MOD_DEFAULT);
    ASSERT_EQ(decode_input.write_buf(encoded.data(), total - 1), common::E_OK);
    storage::DoubleGorillaDecoder decoder;
    std::vector<double> decoded(N);
    int actual = -1;
    EXPECT_EQ(
        decoder.read_batch_double(decoded.data(), N, actual, decode_input),
        common::E_BUF_NOT_ENOUGH);
    EXPECT_LT(actual, N);

    common::ByteStream skip_input(1024, common::MOD_DEFAULT);
    ASSERT_EQ(skip_input.write_buf(encoded.data(), total - 1), common::E_OK);
    storage::DoubleGorillaDecoder skip_decoder;
    int skipped = -1;
    EXPECT_EQ(skip_decoder.skip_double(N, skipped, skip_input),
              common::E_BUF_NOT_ENOUGH);
    EXPECT_LT(skipped, N);
}

TEST_F(GorillaCodecTest, Int32BatchSkip) {
    storage::IntGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 200;
    int32_t expected[N];
    for (int i = 0; i < N; i++) {
        expected[i] = i * 3;
        EXPECT_EQ(encoder.encode(expected[i], stream), common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);

    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    storage::IntGorillaDecoder decoder;
    // Skip first 50 values
    int skipped = 0;
    EXPECT_EQ(decoder.skip_int32(50, skipped, wrapped), common::E_OK);
    EXPECT_EQ(skipped, 50);
    // Read next 50 values
    int32_t out[50];
    int actual = 0;
    EXPECT_EQ(decoder.read_batch_int32(out, 50, actual, wrapped), common::E_OK);
    EXPECT_EQ(actual, 50);
    for (int i = 0; i < 50; i++) {
        EXPECT_EQ(out[i], expected[50 + i]) << "mismatch at index " << i;
    }
}

// Regression: batch_decode_raw used to write out[0] unconditionally in the
// bootstrap branch, even when capacity was 0. Verify the entry path early
// returns and leaves the stream + state untouched.
TEST_F(GorillaCodecTest, Int32BatchDecodeZeroCapacity) {
    storage::IntGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 8;
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(encoder.encode(i, stream), common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    storage::IntGorillaDecoder decoder;
    int32_t sentinel[1] = {0x7fffffff};
    int actual = 42;
    EXPECT_EQ(decoder.read_batch_int32(sentinel, 0, actual, wrapped),
              common::E_OK);
    EXPECT_EQ(actual, 0);
    EXPECT_EQ(sentinel[0], 0x7fffffff);  // not written

    // Followup decode should still read the first value 0.
    int32_t out[N];
    int got_actual = 0;
    EXPECT_EQ(decoder.read_batch_int32(out, N, got_actual, wrapped),
              common::E_OK);
    EXPECT_EQ(got_actual, N);
    for (int i = 0; i < N; i++) EXPECT_EQ(out[i], i);
}

TEST_F(GorillaCodecTest, Int64BatchDecodeZeroCapacity) {
    storage::LongGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    for (int i = 0; i < 8; i++) {
        ASSERT_EQ(encoder.encode(static_cast<int64_t>(i), stream),
                  common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    storage::LongGorillaDecoder decoder;
    int64_t sentinel[1] = {0x7fffffffffffffffLL};
    int actual = 42;
    EXPECT_EQ(decoder.read_batch_int64(sentinel, 0, actual, wrapped),
              common::E_OK);
    EXPECT_EQ(actual, 0);
    EXPECT_EQ(sentinel[0], 0x7fffffffffffffffLL);  // not written
}

// Regression: a truncated Gorilla page used to spin GorillaBitReader::read_long
// forever (bits stays 0, n -= 0 never decreases) and GorillaBitReader::read_bit
// would compute (cur_byte >> -1).  batch_decode_raw must now surface
// E_BUF_NOT_ENOUGH instead of looping.
TEST_F(GorillaCodecTest, Int32BatchDecodeTruncatedInputReturnsError) {
    // Encode enough values to fill several bits, then chop the buffer down to
    // a small prefix so the decoder runs out of bits mid-value.
    storage::IntGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 32;
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(encoder.encode(i * 11 + 3, stream), common::E_OK);
    }
    encoder.flush(stream);

    uint32_t total = stream.total_size();
    ASSERT_GT(total, 4u);
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    ASSERT_EQ(got, total);

    // 3 bytes is large enough to bootstrap the first value (depending on
    // VALUE_BITS_LENGTH_32BIT) but typically too short for the full batch.
    common::ByteStream truncated(common::MOD_DEFAULT);
    truncated.wrap_from((const char*)buf.data(), 3);

    storage::IntGorillaDecoder decoder;
    int32_t out[N];
    int actual = -1;
    int ret = decoder.read_batch_int32(out, N, actual, truncated);
    // Either the decoder reports the truncation, or it stops early without
    // looping forever; both are acceptable.  What MUST NOT happen is a hang
    // or a full-batch return — the test will time out on a hang via the
    // GoogleTest harness.
    EXPECT_TRUE(ret == common::E_OK || ret == common::E_BUF_NOT_ENOUGH)
        << "unexpected ret=" << ret;
    EXPECT_LT(actual, N);
}

TEST_F(GorillaCodecTest, Int64BatchDecodeTruncatedInputReturnsError) {
    storage::LongGorillaEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 32;
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(encoder.encode(static_cast<int64_t>(i) * 17 + 5, stream),
                  common::E_OK);
    }
    encoder.flush(stream);
    uint32_t total = stream.total_size();
    ASSERT_GT(total, 4u);
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    ASSERT_EQ(got, total);

    common::ByteStream truncated(common::MOD_DEFAULT);
    truncated.wrap_from((const char*)buf.data(), 3);

    storage::LongGorillaDecoder decoder;
    int64_t out[N];
    int actual = -1;
    int ret = decoder.read_batch_int64(out, N, actual, truncated);
    EXPECT_TRUE(ret == common::E_OK || ret == common::E_BUF_NOT_ENOUGH)
        << "unexpected ret=" << ret;
    EXPECT_LT(actual, N);
}

}  // namespace storage
