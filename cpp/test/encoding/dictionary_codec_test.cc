#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "encoding/dictionary_decoder.h"
#include "encoding/dictionary_encoder.h"

namespace storage {

class DictionaryTest : public ::testing::Test {};

TEST_F(DictionaryTest, DictionaryEncoder) {
    DictionaryEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DICENCODE_OBJ);
    encoder.init();
    encoder.encode("apple", stream);
    encoder.encode("banana", stream);
    encoder.encode("cherry", stream);
    encoder.encode("apple", stream);
    encoder.flush(stream);

    uint8_t buf[1024] = {0};
    uint32_t want_len, read_len;
    want_len = stream.total_size();
    stream.read_buf(buf, want_len, read_len);
    // Generated using Java Edition
    uint8_t expected_buf[] = {6,   10,  97,  112, 112, 108, 101, 12,  98,
                              97,  110, 97,  110, 97,  12,  99,  104, 101,
                              114, 114, 121, 5,   2,   3,   4,   24,  0};
    EXPECT_EQ(read_len, sizeof(expected_buf));

    for (int i = 0; i < sizeof(expected_buf); i++) {
        EXPECT_EQ(expected_buf[i], buf[i]);
    }
}

TEST_F(DictionaryTest, DictionaryEncoderAndDecoder) {
    DictionaryEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DICENCODE_OBJ);
    encoder.init();

    encoder.encode("apple", stream);
    encoder.encode("banana", stream);
    encoder.encode("cherry", stream);
    encoder.encode("apple", stream);
    encoder.flush(stream);

    DictionaryDecoder decoder;
    decoder.init();

    ASSERT_TRUE(decoder.has_next(stream));
    ASSERT_EQ(decoder.read_string(stream), "apple");

    ASSERT_TRUE(decoder.has_next(stream));
    ASSERT_EQ(decoder.read_string(stream), "banana");

    ASSERT_TRUE(decoder.has_next(stream));
    ASSERT_EQ(decoder.read_string(stream), "cherry");

    ASSERT_TRUE(decoder.has_next(stream));
    ASSERT_EQ(decoder.read_string(stream), "apple");
}

TEST_F(DictionaryTest, DictionaryEncoderAndDecoderLargeQuantities) {
    DictionaryEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DICENCODE_OBJ);
    encoder.init();

    for (int64_t value = 1; value < 10000; value++) {
        encoder.encode(std::to_string(value), stream);
    }
    encoder.flush(stream);

    DictionaryDecoder decoder;
    decoder.init();

    for (int64_t value = 1; value < 10000; value++) {
        ASSERT_EQ(decoder.read_string(stream), std::to_string(value));
    }
}

}  // namespace storage