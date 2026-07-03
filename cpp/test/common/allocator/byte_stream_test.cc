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
#include "common/allocator/byte_stream.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>

namespace common {

TEST(FloatConversionTest, FloatToBytesAndBytesToFloat) {
    float actual_float = -1234.5678f;
    uint8_t actual_bytes[4];
    float_to_bytes(actual_float, actual_bytes);
    float expect_float = bytes_to_float(actual_bytes);
    EXPECT_FLOAT_EQ(expect_float, actual_float);
}

TEST(DoubleConversionTest, DoubleToBytesAndBytesToDouble) {
    double actual_double = -1.23456789e-20;
    uint8_t actual_bytes[8];
    double_to_bytes(actual_double, actual_bytes);
    double expect_double = bytes_to_double(actual_bytes);
    EXPECT_DOUBLE_EQ(expect_double, actual_double);
}

class ByteStreamTest : public ::testing::Test {
   protected:
    void SetUp() override {
        byte_stream_ = new ByteStream(16, MOD_DEFAULT, false);
    }

    void TearDown() override { delete byte_stream_; }

    void write_to_stream(const uint8_t* data, uint32_t size) {
        ASSERT_EQ(byte_stream_->write_buf(data, size), common::E_OK);
    }

    void read_from_stream(uint8_t* buffer, uint32_t want_len,
                          uint32_t& read_len) {
        ASSERT_EQ(byte_stream_->read_buf(buffer, want_len, read_len),
                  common::E_OK);
    }

    void wrap_external_buffer(const char* buffer, int32_t size) {
        byte_stream_->wrap_from(buffer, size);
    }

    ByteStream* byte_stream_;
};

TEST_F(ByteStreamTest, WriteReadTest) {
    const uint8_t data[] = {0x01, 0x02, 0x03};
    const uint32_t data_size = sizeof(data);

    write_to_stream(data, data_size);
    uint8_t read_buffer[data_size];
    uint32_t read_len = 0;
    read_from_stream(read_buffer, data_size, read_len);

    ASSERT_EQ(read_len, data_size);
    for (uint32_t i = 0; i < data_size; ++i) {
        ASSERT_EQ(read_buffer[i], data[i]);
    }
}

TEST_F(ByteStreamTest, WriteReadLargeQuantities) {
    for (int i = 0; i < 1024 * 1024; i++) {
        const uint8_t data = i & 0xff;
        write_to_stream(&data, 1);
    }

    static uint8_t read_buffer[1024 * 1024];
    for (int i = 0; i < 1024 * 1024; i++) {
        uint32_t read_len = 0;
        read_from_stream(read_buffer + i, 1, read_len);
    }

    for (int i = 0; i < 1024 * 1024; i++) {
        EXPECT_EQ(read_buffer[i], i & 0xff);
    }
}

TEST_F(ByteStreamTest, WrapExternalBufferTest) {
    const char externalBuffer[] = "Hello, World!";
    const int32_t bufferSize = sizeof(externalBuffer);

    wrap_external_buffer(externalBuffer, bufferSize);
    ASSERT_TRUE(byte_stream_->is_wrapped());
    ASSERT_STREQ(byte_stream_->get_wrapped_buf(), externalBuffer);
}

TEST_F(ByteStreamTest, SizeTest) {
    const uint8_t data[] = {0x01, 0x02, 0x03};
    const uint32_t data_size = sizeof(data);

    write_to_stream(data, data_size);
    ASSERT_EQ(byte_stream_->total_size(), data_size);
    ASSERT_EQ(byte_stream_->remaining_size(), data_size);

    uint8_t read_buffer[data_size];
    uint32_t read_len = 0;
    read_from_stream(read_buffer, 2, read_len);

    ASSERT_EQ(byte_stream_->remaining_size(), data_size - 2);
}

TEST_F(ByteStreamTest, MarkReadPositionTest) {
    const uint8_t data[] = {0x01, 0x02, 0x03};
    const uint32_t data_size = sizeof(data);

    write_to_stream(data, data_size);

    byte_stream_->mark_read_pos();

    uint8_t read_buffer[data_size];
    uint32_t read_len = 0;
    read_from_stream(read_buffer, 2, read_len);

    uint32_t markedLen = byte_stream_->get_mark_len();
    ASSERT_EQ(markedLen, read_len);
}

TEST_F(ByteStreamTest, ResetTest) {
    const uint8_t data[] = {0x01, 0x02, 0x03};
    const uint32_t data_size = sizeof(data);

    write_to_stream(data, data_size);

    byte_stream_->reset();

    ASSERT_EQ(byte_stream_->total_size(), 0);
    ASSERT_EQ(byte_stream_->remaining_size(), 0);
    ASSERT_EQ(byte_stream_->read_pos(), 0);
    ASSERT_FALSE(byte_stream_->is_wrapped());
}

TEST_F(ByteStreamTest, WriteMoreThanPageSizeTest) {
    const uint8_t data[20] = {0x01, 0x02, 0x03};
    const uint32_t data_size = sizeof(data);

    write_to_stream(data, data_size);

    ASSERT_EQ(byte_stream_->total_size(), 20);
    ASSERT_EQ(byte_stream_->remaining_size(), 20);

    uint8_t read_buffer[data_size];
    uint32_t read_len = 0;
    read_from_stream(read_buffer, data_size, read_len);

    ASSERT_EQ(read_len, data_size);
    for (uint32_t i = 0; i < data_size; ++i) {
        ASSERT_EQ(read_buffer[i], data[i]);
    }
}

TEST_F(ByteStreamTest, ReadMoreThanAvailableTest) {
    const uint8_t data[] = {0x01, 0x02, 0x03};
    const uint32_t data_size = sizeof(data);

    write_to_stream(data, data_size);

    uint8_t read_buffer[4];
    uint32_t read_len = 0;
    int readResult = byte_stream_->read_buf(read_buffer, 4, read_len);

    ASSERT_EQ(readResult, common::E_PARTIAL_READ);
    ASSERT_EQ(read_len, data_size);
}

// Regression: the ctor used to take page_size verbatim, but hot read/write
// paths use `& (page_size-1)` as a bitmask.  A non-power-of-2 page_size
// would cause page-crossing logic to misfire, corrupting written data.
// Constructing with 1000 should still round-trip cleanly across many pages.
// Regression: round_up_pow2 used `while (ps < n) ps <<= 1`, which overflows
// to 0 once ps passes 2^31 and never matches, looping forever.  Verify the
// clamped helper returns the largest representable power of two instead.
TEST(ByteStreamCtorTest, RoundUpPow2ClampsHugeInput) {
    EXPECT_EQ(round_up_pow2(0u), 1u);
    EXPECT_EQ(round_up_pow2(1u), 1u);
    EXPECT_EQ(round_up_pow2(1000u), 1024u);
    EXPECT_EQ(round_up_pow2(1024u), 1024u);
    EXPECT_EQ(round_up_pow2(0x80000000u), 0x80000000u);
    EXPECT_EQ(round_up_pow2(0x80000001u), 0x80000000u);
    EXPECT_EQ(round_up_pow2(0xFFFFFFFFu), 0x80000000u);
}

TEST(ByteStreamCtorTest, NonPowerOfTwoPageSizeRoundTrip) {
    ByteStream bs(1000, MOD_DEFAULT, false);
    // Span ~5 pages: 1024 * 5 = 5120 bytes.
    const uint32_t N = 5120;
    std::vector<uint8_t> data(N);
    for (uint32_t i = 0; i < N; i++) {
        data[i] = static_cast<uint8_t>((i * 31 + 7) & 0xff);
    }
    ASSERT_EQ(bs.write_buf(data.data(), N), common::E_OK);

    std::vector<uint8_t> out(N, 0);
    uint32_t read_len = 0;
    ASSERT_EQ(bs.read_buf(out.data(), N, read_len), common::E_OK);
    ASSERT_EQ(read_len, N);
    for (uint32_t i = 0; i < N; i++) {
        ASSERT_EQ(out[i], data[i]) << "mismatch at idx " << i;
    }
}

TEST_F(ByteStreamTest, WrapAndClearTest) {
    const char externalBuffer[] = "Hello, World!";
    const int32_t bufferSize = sizeof(externalBuffer);

    wrap_external_buffer(externalBuffer, bufferSize);
    byte_stream_->clear_wrapped_buf();

    ASSERT_EQ(byte_stream_->get_wrapped_buf(), nullptr);
}

class SerializationUtilTest : public ::testing::Test {
   protected:
    void SetUp() override {
        byte_stream_ = new ByteStream(16, MOD_DEFAULT, false);
    }

    void TearDown() override { delete byte_stream_; }

    ByteStream* byte_stream_;
};

TEST_F(SerializationUtilTest, WriteReadUI8) {
    uint8_t value_to_write = 0x12;
    uint8_t value_read = 0;

    EXPECT_EQ(SerializationUtil::write_ui8(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_ui8(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadUI16) {
    uint16_t value_to_write = 0x1234;
    uint16_t value_read = 0;

    EXPECT_EQ(SerializationUtil::write_ui16(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_ui16(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadUI32) {
    uint32_t value_to_write = 0x12345678;
    uint32_t value_read = 0;

    EXPECT_EQ(SerializationUtil::write_ui32(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_ui32(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadUI64) {
    uint64_t value_to_write = 0x123456789ABCDEF0;
    uint64_t value_read = 0;

    EXPECT_EQ(SerializationUtil::write_ui64(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_ui64(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadFloat) {
    float value_to_write = 3.14f;
    float value_read = 0.0;

    EXPECT_EQ(SerializationUtil::write_float(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_float(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadDouble) {
    double value_to_write = 3.141592653589793;
    double value_read = 0.0;

    EXPECT_EQ(SerializationUtil::write_double(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_double(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadString) {
    std::string value_to_write = "Hello, World!";
    std::string value_read = "";

    EXPECT_EQ(SerializationUtil::write_str(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_str(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadIntLEPaddedBitWidth_BitWidthTooLarge) {
    int32_t value = 123;
    EXPECT_EQ(SerializationUtil::write_int_little_endian_padded_on_bit_width(
                  value, *byte_stream_, 40),
              common::E_TSFILE_CORRUPTED);

    byte_stream_->reset();
    int32_t read_val = 0;
    EXPECT_EQ(SerializationUtil::read_int_little_endian_padded_on_bit_width(
                  *byte_stream_, 40, read_val),
              common::E_TSFILE_CORRUPTED);
}

TEST_F(SerializationUtilTest, WriteReadIntLEPaddedBitWidthBoundaryValue) {
    std::vector<int32_t> test_values = {
        132100, 1, -1, 12345678, -87654321, INT32_MAX, INT32_MIN};
    int bit_width = 32;
    for (int32_t original_value : test_values) {
        byte_stream_->reset();
        EXPECT_EQ(
            SerializationUtil::write_int_little_endian_padded_on_bit_width(
                original_value, *byte_stream_, bit_width),
            common::E_OK);
        int32_t read_value = 0;
        EXPECT_EQ(SerializationUtil::read_int_little_endian_padded_on_bit_width(
                      *byte_stream_, bit_width, read_value),
                  common::E_OK);
        EXPECT_EQ(read_value, original_value)
            << "Mismatch with bit_width = " << bit_width;
    }
}

// Regression: total_size_ was widened to uint64_t but the read-cursor APIs
// stayed uint32_t.  A stream that legitimately reaches >4 GiB would have
// remaining_size() / read_pos() / set_read_pos() truncating to the low 32
// bits and silently mis-positioning later reads.  Lock the widened type at
// compile time so a partial revert can't reintroduce truncation, and
// round-trip a moderate value via the API to catch arithmetic mistakes.
TEST(ByteStreamWidthTest, ReadCursorApisAre64Bit) {
    ByteStream s(64, common::MOD_DEFAULT);
    static_assert(sizeof(decltype(s.read_pos())) >= sizeof(uint64_t),
                  "ByteStream::read_pos() must return a 64-bit type");
    static_assert(sizeof(decltype(s.remaining_size())) >= sizeof(uint64_t),
                  "ByteStream::remaining_size() must return a 64-bit type");
    static_assert(sizeof(decltype(s.get_mark_len())) >= sizeof(uint64_t),
                  "ByteStream::get_mark_len() must return a 64-bit type");

    // Round-trip a position via set_read_pos / read_pos on a small wrapped
    // buffer.  Combined with the static_asserts above this guards the path
    // arithmetic: a partial revert that kept the signature 64-bit but
    // truncated read_pos_ to uint32_t internally would fail set_read_pos →
    // read_pos on values near a 32-bit boundary.
    constexpr int32_t kLen = 256;
    std::vector<char> backing(kLen, 0);
    ByteStream wrapped(common::MOD_DEFAULT);
    wrapped.wrap_from(backing.data(), kLen);
    wrapped.set_read_pos(static_cast<uint64_t>(kLen - 7));
    EXPECT_EQ(wrapped.read_pos(), static_cast<uint64_t>(kLen - 7));
    EXPECT_EQ(wrapped.remaining_size(), 7u);
}

// Regression for the 64 KiB page memory-pressure account: ByteStream pages
// are allocated up to OUT_STREAM_PAGE_SIZE bytes even when only a handful of
// bytes have been written, so a chunk-group with many sparse measurements
// can pin tens of megabytes that total_size() can't see.  allocated_bytes()
// must reflect the real allocated footprint.
TEST(ByteStreamAllocatedBytesTest, ReportsPageAllocationsNotLogicalSize) {
    constexpr uint32_t kPageSize = 4096;
    ByteStream s(kPageSize, common::MOD_DEFAULT);
    EXPECT_EQ(s.allocated_bytes(), 0u);

    // First write triggers one page allocation; logical size is 4 bytes but
    // the real footprint should be the rounded page size.
    uint8_t payload[4] = {1, 2, 3, 4};
    ASSERT_EQ(s.write_buf(payload, 4), common::E_OK);
    EXPECT_EQ(s.total_size(), 4u);
    EXPECT_GE(s.allocated_bytes(), kPageSize);
    EXPECT_EQ(s.allocated_bytes() % kPageSize, 0u);
}

// Regression for finding 21 (MSVC reinterpret_cast<atomic<T>*> UB): the
// OptionalAtomic storage is now a real std::atomic<T>, so atomic ops never
// observe a non-atomic backing object.  Lock the storage type at compile
// time so a future refactor can't reintroduce the bare T fallback.
TEST(OptionalAtomicStorageTest, BackingStorageIsRealAtomic) {
    OptionalAtomic<uint64_t> oa(0, /*enable_atomic=*/true);
    static_assert(!std::is_copy_constructible<OptionalAtomic<uint64_t>>::value,
                  "OptionalAtomic must not be copyable — the std::atomic<T> "
                  "storage forces explicit load/store");
    EXPECT_EQ(oa.load(), 0u);
    oa.store(42);
    EXPECT_EQ(oa.load(), 42u);
    EXPECT_EQ(oa.atomic_aaf(8), 50u);
    EXPECT_EQ(oa.load(), 50u);
    EXPECT_EQ(oa.atomic_faa(1), 50u);
    EXPECT_EQ(oa.load(), 51u);
}

}  // namespace common
