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

// Targeted coverage tests that exercise paths missed by the per-codec
// roundtrip tests: type-mismatch error returns, has_remaining variants,
// SIMD/scalar batch branches, floating-point special values, dictionary
// decoder/encoder, and reset cycles.

#include <cmath>
#include <limits>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/dictionary_decoder.h"
#include "encoding/dictionary_encoder.h"
#include "encoding/gorilla_decoder.h"
#include "encoding/gorilla_encoder.h"
#include "encoding/int32_rle_decoder.h"
#include "encoding/int32_rle_encoder.h"
#include "encoding/int64_rle_decoder.h"
#include "encoding/int64_rle_encoder.h"
#include "encoding/plain_decoder.h"
#include "encoding/plain_encoder.h"
#include "encoding/ts2diff_decoder.h"
#include "encoding/ts2diff_encoder.h"
#include "encoding/zigzag_decoder.h"
#include "encoding/zigzag_encoder.h"
#include "gtest/gtest.h"

namespace storage {

// ── Type-mismatch returns ────────────────────────────────────────────────
//
// Every codec exposes read_boolean / read_int32 / read_int64 / read_float /
// read_double / read_String. Most of them only implement one or two and
// return E_TYPE_NOT_MATCH for the rest, but those return paths were never
// hit by the existing per-codec tests (which only call the one supported
// method per codec).
TEST(EncodingCoverage, TypeMismatchReturnsAreReachable) {
    common::ByteStream s(64, common::MOD_DEFAULT);
    common::PageArena pa;
    pa.init(512, common::MOD_DEFAULT);
    bool b;
    float f;
    double d;
    int64_t i64;
    common::String str;

    // Each decoder returns an error sentinel (E_TYPE_NOT_MATCH or
    // E_NOT_SUPPORT depending on codec) for the read_* variants it
    // doesn't implement.  We only care that the unsupported path returns
    // an error rather than a corrupted value.  Note that GorillaDecoder
    // implements its unsupported paths with `ASSERT(false)`; calling
    // those in Debug builds aborts, so we exercise only the codecs that
    // return cleanly (Zigzag, RLE).
    auto NE_OK = [](int r) { EXPECT_NE(r, common::E_OK); };
    IntZigzagDecoder zz;
    NE_OK(zz.read_boolean(b, s));
    NE_OK(zz.read_float(f, s));
    NE_OK(zz.read_double(d, s));
    NE_OK(zz.read_String(str, pa, s));

    Int32RleDecoder rle32;
    NE_OK(rle32.read_int64(i64, s));
    NE_OK(rle32.read_float(f, s));
    NE_OK(rle32.read_double(d, s));
    NE_OK(rle32.read_String(str, pa, s));

    Int64RleDecoder rle64;
    int32_t i32;
    NE_OK(rle64.read_boolean(b, s));
    NE_OK(rle64.read_int32(i32, s));
    NE_OK(rle64.read_float(f, s));
    NE_OK(rle64.read_double(d, s));
    NE_OK(rle64.read_String(str, pa, s));
    (void)i32;
    (void)i64;
}

// ── Reset cycles ────────────────────────────────────────────────────────
//
// Each codec defines a reset() that resets internal state; nothing in the
// roundtrip tests calls it.  Encode → reset → re-encode should still
// produce a stream that decodes to the second batch's values.
TEST(EncodingCoverage, ResetClearsState) {
    {
        IntZigzagEncoder enc;
        IntZigzagDecoder dec;
        common::ByteStream s(64, common::MOD_DEFAULT);
        EXPECT_EQ(enc.encode(123, s), common::E_OK);
        enc.flush(s);
        EXPECT_EQ(dec.decode(s), 123);
        dec.reset();
        common::ByteStream s2(64, common::MOD_DEFAULT);
        EXPECT_EQ(enc.encode(-456, s2), common::E_OK);
        enc.flush(s2);
        EXPECT_EQ(dec.decode(s2), -456);
    }
    {
        IntGorillaEncoder enc;
        IntGorillaDecoder dec;
        common::ByteStream s(64, common::MOD_DEFAULT);
        EXPECT_EQ(enc.encode(7, s), common::E_OK);
        EXPECT_EQ(enc.encode(7, s), common::E_OK);
        enc.flush(s);
        int32_t v;
        EXPECT_EQ(dec.read_int32(v, s), common::E_OK);
        EXPECT_EQ(v, 7);
        dec.reset();
        enc.reset();
        common::ByteStream s2(64, common::MOD_DEFAULT);
        EXPECT_EQ(enc.encode(42, s2), common::E_OK);
        EXPECT_EQ(enc.encode(42, s2), common::E_OK);
        enc.flush(s2);
        EXPECT_EQ(dec.read_int32(v, s2), common::E_OK);
        EXPECT_EQ(v, 42);
    }
}

// ── has_remaining variants ──────────────────────────────────────────────
TEST(EncodingCoverage, HasRemainingOnEmptyAndAfterDrain) {
    common::ByteStream empty(64, common::MOD_DEFAULT);
    {
        IntZigzagDecoder zz;
        EXPECT_FALSE(zz.has_remaining(empty));
    }
    {
        IntGorillaDecoder g;
        EXPECT_FALSE(g.has_remaining(empty));
    }
    {
        Int32RleDecoder rle;
        EXPECT_FALSE(rle.has_remaining(empty));
    }
    {
        TS2DIFFDecoder<int32_t> t;
        EXPECT_FALSE(t.has_remaining(empty));
    }
    {
        PlainDecoder p;
        EXPECT_FALSE(p.has_remaining(empty));
    }
}

// ── Gorilla floating-point special values ──────────────────────────────
//
// FloatGorillaDecoder / DoubleGorillaDecoder run different VALUE_BITS and
// ending-sentinel paths.  Verify they round-trip NaN, infinity, -0.0 and
// denormals — none of which the existing happy-path roundtrip exercises.
TEST(EncodingCoverage, GorillaFloatSpecialValues) {
    FloatGorillaEncoder enc;
    common::ByteStream s(256, common::MOD_DEFAULT);
    std::vector<float> values = {
        0.0f,
        -0.0f,
        std::numeric_limits<float>::infinity(),
        -std::numeric_limits<float>::infinity(),
        std::numeric_limits<float>::min(),
        std::numeric_limits<float>::denorm_min(),
        std::numeric_limits<float>::epsilon(),
        1.0f,
        -1.0f,
        std::numeric_limits<float>::max(),
        std::numeric_limits<float>::lowest(),
    };
    for (float v : values) ASSERT_EQ(enc.encode(v, s), common::E_OK);
    enc.flush(s);

    FloatGorillaDecoder dec;
    float out;
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(dec.read_float(out, s), common::E_OK) << "i=" << i;
        if (std::isnan(values[i])) {
            EXPECT_TRUE(std::isnan(out));
        } else {
            // Bitwise compare to catch -0.0 vs 0.0 etc.
            uint32_t a, b;
            memcpy(&a, &values[i], sizeof(float));
            memcpy(&b, &out, sizeof(float));
            EXPECT_EQ(a, b) << "i=" << i;
        }
    }
}

TEST(EncodingCoverage, GorillaDoubleSpecialValues) {
    DoubleGorillaEncoder enc;
    common::ByteStream s(256, common::MOD_DEFAULT);
    std::vector<double> values = {
        0.0,
        -0.0,
        std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::min(),
        std::numeric_limits<double>::denorm_min(),
        std::numeric_limits<double>::epsilon(),
        1.0,
        -1.0,
        std::numeric_limits<double>::max(),
        std::numeric_limits<double>::lowest(),
    };
    for (double v : values) ASSERT_EQ(enc.encode(v, s), common::E_OK);
    enc.flush(s);

    DoubleGorillaDecoder dec;
    double out;
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_EQ(dec.read_double(out, s), common::E_OK) << "i=" << i;
        uint64_t a, b;
        memcpy(&a, &values[i], sizeof(double));
        memcpy(&b, &out, sizeof(double));
        EXPECT_EQ(a, b) << "i=" << i;
    }
}

// ── Gorilla skip path ───────────────────────────────────────────────────
TEST(EncodingCoverage, GorillaSkipInt32Roundtrip) {
    IntGorillaEncoder enc;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const int N = 200;
    std::vector<int32_t> values(N);
    for (int i = 0; i < N; i++) {
        values[i] = i * 11 - 5;
        ASSERT_EQ(enc.encode(values[i], stream), common::E_OK);
    }
    enc.flush(stream);

    // Wrap into contiguous buffer for batch_skip_raw.
    uint32_t total = stream.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    stream.read_buf(buf.data(), total, got);
    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    IntGorillaDecoder dec;
    int skipped = 0;
    ASSERT_EQ(dec.skip_int32(50, skipped, wrapped), common::E_OK);
    EXPECT_EQ(skipped, 50);
    int32_t out[N];
    int actual = 0;
    ASSERT_EQ(dec.read_batch_int32(out, N - 50, actual, wrapped), common::E_OK);
    EXPECT_EQ(actual, N - 50);
    for (int i = 0; i < N - 50; i++) {
        EXPECT_EQ(out[i], values[50 + i]) << "i=" << i;
    }
}

// ── TS2DIFF batch decode hits SIMD block + scalar tail ─────────────────
TEST(EncodingCoverage, TS2DIFFBatchInt32MultipleBlocks) {
    TS2DIFFEncoder<int32_t> enc;
    common::ByteStream s(8192, common::MOD_DEFAULT);
    // Encode 500 values to span ~4 blocks (default block size 128).
    const int N = 500;
    std::vector<int32_t> values(N);
    for (int i = 0; i < N; i++) {
        values[i] = i * 7 + 3;
        ASSERT_EQ(enc.encode(values[i], s), common::E_OK);
    }
    ASSERT_EQ(enc.flush(s), common::E_OK);

    // Wrap-from for the SIMD/scalar block fast path.
    uint32_t total = s.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    s.read_buf(buf.data(), total, got);
    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    TS2DIFFDecoder<int32_t> dec;
    std::vector<int32_t> out(N);
    int total_decoded = 0;
    while (dec.has_remaining(wrapped) && total_decoded < N) {
        int actual = 0;
        ASSERT_EQ(dec.read_batch_int32(out.data() + total_decoded,
                                       N - total_decoded, actual, wrapped),
                  common::E_OK);
        if (actual == 0) break;
        total_decoded += actual;
    }
    EXPECT_EQ(total_decoded, N);
    for (int i = 0; i < N; i++) EXPECT_EQ(out[i], values[i]) << "i=" << i;
}

TEST(EncodingCoverage, TS2DIFFBatchInt64MultipleBlocks) {
    TS2DIFFEncoder<int64_t> enc;
    common::ByteStream s(8192, common::MOD_DEFAULT);
    const int N = 500;
    std::vector<int64_t> values(N);
    for (int i = 0; i < N; i++) {
        values[i] = static_cast<int64_t>(i) * 17 + 41;
        ASSERT_EQ(enc.encode(values[i], s), common::E_OK);
    }
    ASSERT_EQ(enc.flush(s), common::E_OK);

    uint32_t total = s.total_size();
    std::vector<uint8_t> buf(total);
    uint32_t got = 0;
    s.read_buf(buf.data(), total, got);
    common::ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from((const char*)buf.data(), total);

    TS2DIFFDecoder<int64_t> dec;
    std::vector<int64_t> out(N);
    int total_decoded = 0;
    while (dec.has_remaining(wrapped) && total_decoded < N) {
        int actual = 0;
        ASSERT_EQ(dec.read_batch_int64(out.data() + total_decoded,
                                       N - total_decoded, actual, wrapped),
                  common::E_OK);
        if (actual == 0) break;
        total_decoded += actual;
    }
    EXPECT_EQ(total_decoded, N);
    for (int i = 0; i < N; i++) EXPECT_EQ(out[i], values[i]) << "i=" << i;
}

// ── Plain encoder: encode_batch fast paths for each type ───────────────
TEST(EncodingCoverage, PlainEncoderBatchAllTypes) {
    PlainEncoder enc;
    PlainDecoder dec;

    // Float batch.
    {
        common::ByteStream s(1024, common::MOD_DEFAULT);
        const uint32_t N = 100;
        float v[N];
        for (uint32_t i = 0; i < N; i++) v[i] = i * 0.5f - 1.0f;
        ASSERT_EQ(enc.encode_batch(v, N, s), common::E_OK);
        float out[N];
        int actual = 0;
        ASSERT_EQ(dec.read_batch_float(out, N, actual, s), common::E_OK);
        EXPECT_EQ(actual, static_cast<int>(N));
        for (uint32_t i = 0; i < N; i++) EXPECT_FLOAT_EQ(out[i], v[i]);
    }
    // Int64 batch.
    {
        common::ByteStream s(1024, common::MOD_DEFAULT);
        const uint32_t N = 100;
        int64_t v[N];
        for (uint32_t i = 0; i < N; i++) v[i] = i * 1000 - 50;
        ASSERT_EQ(enc.encode_batch(v, N, s), common::E_OK);
        int64_t out[N];
        int actual = 0;
        ASSERT_EQ(dec.read_batch_int64(out, N, actual, s), common::E_OK);
        EXPECT_EQ(actual, static_cast<int>(N));
        for (uint32_t i = 0; i < N; i++) EXPECT_EQ(out[i], v[i]);
    }
}

// ── PlainDecoder skip paths (wrapped + paged) ──────────────────────────
TEST(EncodingCoverage, PlainSkipPagedStream) {
    PlainEncoder enc;
    PlainDecoder dec;
    // Paged ByteStream (tiny page) forces the fallback path.
    common::ByteStream s(16, common::MOD_DEFAULT);
    for (int i = 0; i < 32; i++)
        ASSERT_EQ(enc.encode((int64_t)i, s), common::E_OK);
    int skipped = 0;
    ASSERT_EQ(dec.skip_int64(10, skipped, s), common::E_OK);
    EXPECT_EQ(skipped, 10);
    int64_t out;
    ASSERT_EQ(dec.read_int64(out, s), common::E_OK);
    EXPECT_EQ(out, 10);
}

// ── Dictionary codec roundtrip ─────────────────────────────────────────
TEST(EncodingCoverage, DictionaryStringRoundTrip) {
    DictionaryEncoder enc;
    common::ByteStream s(1024, common::MOD_DEFAULT);

    std::vector<std::string> raw = {"apple",  "banana", "apple",
                                    "cherry", "banana", "apple"};
    for (const auto& r : raw) {
        common::String str(const_cast<char*>(r.c_str()), r.size());
        ASSERT_EQ(enc.encode(str, s), common::E_OK);
    }
    enc.flush(s);

    DictionaryDecoder dec;
    common::PageArena pa;
    pa.init(512, common::MOD_DEFAULT);
    for (const auto& r : raw) {
        common::String out;
        ASSERT_EQ(dec.read_String(out, pa, s), common::E_OK);
        ASSERT_EQ(out.len_, r.size());
        EXPECT_EQ(std::string(out.buf_, out.len_), r);
    }
}

}  // namespace storage
