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

    uint8_t read_buffer[1024 * 1024];
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
    uint8_t value_read;

    EXPECT_EQ(SerializationUtil::write_ui8(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_ui8(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadUI16) {
    uint16_t value_to_write = 0x1234;
    uint16_t value_read;

    EXPECT_EQ(SerializationUtil::write_ui16(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_ui16(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadUI32) {
    uint32_t value_to_write = 0x12345678;
    uint32_t value_read;

    EXPECT_EQ(SerializationUtil::write_ui32(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_ui32(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadUI64) {
    uint64_t value_to_write = 0x123456789ABCDEF0;
    uint64_t value_read;

    EXPECT_EQ(SerializationUtil::write_ui64(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_ui64(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadFloat) {
    float value_to_write = 3.14f;
    float value_read;

    EXPECT_EQ(SerializationUtil::write_float(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_float(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadDouble) {
    double value_to_write = 3.141592653589793;
    double value_read;

    EXPECT_EQ(SerializationUtil::write_double(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_double(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

TEST_F(SerializationUtilTest, WriteReadString) {
    std::string value_to_write = "Hello, World!";
    std::string value_read;

    EXPECT_EQ(SerializationUtil::write_str(value_to_write, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(SerializationUtil::read_str(value_read, *byte_stream_),
              common::E_OK);
    EXPECT_EQ(value_to_write, value_read);
}

}  // namespace common