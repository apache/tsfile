#include "common/container/byte_buffer.h"

#include <gtest/gtest.h>

namespace {

class ByteBufferTest : public ::testing::Test {
   protected:
    common::ByteBuffer buffer;

    void SetUp() override { buffer.init(128); }

    void TearDown() override {}
};

TEST_F(ByteBufferTest, Initialization) {
    EXPECT_NE(buffer.get_data(), nullptr);
}

TEST_F(ByteBufferTest, AppendFixedValue) {
    const char* value = "Hello";
    uint32_t len = strlen(value);
    buffer.append_fixed_value(value, len);
    EXPECT_STREQ(buffer.read(0, len), value);
}

TEST_F(ByteBufferTest, AppendVariableValue) {
    const char* value = "World";
    uint32_t len = strlen(value);

    buffer.append_variable_value(value, len);

    uint32_t read_len;
    char* read_value = buffer.read(0, &read_len);

    EXPECT_EQ(read_len, len);
    EXPECT_STREQ(read_value, value);
}

TEST_F(ByteBufferTest, ExtendMemory) {
    common::ByteBuffer byte_buffer;
    byte_buffer.init(5);
    const char* value = "Extended";
    uint32_t len = strlen(value);
    buffer.append_fixed_value(value, len);
    EXPECT_STREQ(buffer.read(0, len), value);
}

}  // namespace
