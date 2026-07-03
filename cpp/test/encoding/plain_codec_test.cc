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

#include "encoding/plain_decoder.h"
#include "encoding/plain_encoder.h"

namespace storage {

TEST(PlainEncoderDecoderTest, EncodeDecodeBool) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    bool original = true;
    bool decoded = false;

    encoder.encode(original, stream);
    decoder.read_boolean(decoded, stream);

    EXPECT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeInt32) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int32_t original = 12345;
    int32_t decoded = 0;

    encoder.encode(original, stream);
    decoder.read_int32(decoded, stream);

    EXPECT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeMinusInt32) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int32_t original = -12345333;
    int32_t decoded = 0;
    encoder.encode(original, stream);
    decoder.read_int32(decoded, stream);
    EXPECT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeMinusInt64) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int64_t original = -12345333;
    int64_t decoded = 0;
    encoder.encode(original, stream);
    decoder.read_int64(decoded, stream);
    EXPECT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeInt64) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int64_t original = 123456789012345LL;
    int64_t decoded = 0;

    encoder.encode(original, stream);
    decoder.read_int64(decoded, stream);

    EXPECT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeFloat) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    float original = 123.45f;
    float decoded = 0.0f;

    encoder.encode(original, stream);
    decoder.read_float(decoded, stream);

    EXPECT_FLOAT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeDouble) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    double original = 123.456789;
    double decoded = 0.0;

    encoder.encode(original, stream);
    decoder.read_double(decoded, stream);

    EXPECT_DOUBLE_EQ(original, decoded);
}

// Regression: read_batch_int64/float/double used to dereference
// in.get_wrapped_buf() unconditionally, which is null for a normal paged
// ByteStream. Verify the fallback path produces correct results.
TEST(PlainEncoderDecoderTest, ReadBatchInt64PagedStream) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    // Tiny page size forces multi-page write so the stream is paged, not
    // wrapped.
    common::ByteStream stream(16, common::MOD_DEFAULT);
    const int N = 32;
    int64_t values[N];
    for (int i = 0; i < N; i++) {
        values[i] = static_cast<int64_t>(i) * 7 - 3;
        encoder.encode(values[i], stream);
    }
    int64_t out[N];
    int actual = 0;
    EXPECT_EQ(decoder.read_batch_int64(out, N, actual, stream), common::E_OK);
    EXPECT_EQ(actual, N);
    for (int i = 0; i < N; i++) {
        EXPECT_EQ(out[i], values[i]) << "mismatch at " << i;
    }
}

TEST(PlainEncoderDecoderTest, ReadBatchFloatPagedStream) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(16, common::MOD_DEFAULT);
    const int N = 32;
    float values[N];
    for (int i = 0; i < N; i++) {
        values[i] = static_cast<float>(i) * 0.5f - 1.25f;
        encoder.encode(values[i], stream);
    }
    float out[N];
    int actual = 0;
    EXPECT_EQ(decoder.read_batch_float(out, N, actual, stream), common::E_OK);
    EXPECT_EQ(actual, N);
    for (int i = 0; i < N; i++) {
        EXPECT_FLOAT_EQ(out[i], values[i]);
    }
}

// Regression: encode_batch(const double*) used to reinterpret_cast to
// int64_t* and dispatch into the int64 path, which read the doubles through
// an int64_t pointer — a strict-aliasing violation under -O.  The dedicated
// double path now memcpys per element; verify a full round-trip through it.
TEST(PlainEncoderDecoderTest, EncodeBatchDoubleRoundTrip) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const uint32_t N = 64;
    double values[N];
    for (uint32_t i = 0; i < N; i++) {
        values[i] = static_cast<double>(i) * 0.125 - 3.14;
    }
    ASSERT_EQ(encoder.encode_batch(values, N, stream), common::E_OK);

    double out[N];
    int actual = 0;
    EXPECT_EQ(decoder.read_batch_double(out, N, actual, stream), common::E_OK);
    EXPECT_EQ(actual, static_cast<int>(N));
    for (uint32_t i = 0; i < N; i++) {
        EXPECT_DOUBLE_EQ(out[i], values[i]) << "mismatch at " << i;
    }
}

TEST(PlainEncoderDecoderTest, ReadBatchDoublePagedStream) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(16, common::MOD_DEFAULT);
    const int N = 32;
    double values[N];
    for (int i = 0; i < N; i++) {
        values[i] = static_cast<double>(i) * 1.25 + 3.14;
        encoder.encode(values[i], stream);
    }
    double out[N];
    int actual = 0;
    EXPECT_EQ(decoder.read_batch_double(out, N, actual, stream), common::E_OK);
    EXPECT_EQ(actual, N);
    for (int i = 0; i < N; i++) {
        EXPECT_DOUBLE_EQ(out[i], values[i]);
    }
}

}  // end namespace storage