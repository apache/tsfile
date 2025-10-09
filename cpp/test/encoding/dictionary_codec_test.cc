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

#include <random>
#include <string>
#include <unordered_set>
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

    for (size_t i = 0; i < (size_t)sizeof(expected_buf); i++) {
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

TEST_F(DictionaryTest, DictionaryEncoderAndDecoderOneItem) {
    DictionaryEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DICENCODE_OBJ);
    encoder.init();

    encoder.encode("apple", stream);
    encoder.flush(stream);

    DictionaryDecoder decoder;
    decoder.init();

    ASSERT_TRUE(decoder.has_next(stream));
    ASSERT_EQ(decoder.read_string(stream), "apple");

    ASSERT_FALSE(decoder.has_next(stream));
}

TEST_F(DictionaryTest, DictionaryEncoderAndDecoderRepeatedItems) {
    DictionaryEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DICENCODE_OBJ);
    encoder.init();

    for (char c = 'a'; c <= 'z'; c++) {
        for (int i = 0; i < 100; i++) {
            encoder.encode(std::string(c, 3), stream);
        }
    }
    encoder.flush(stream);

    DictionaryDecoder decoder;
    decoder.init();

    for (char c = 'a'; c <= 'z'; c++) {
        for (int i = 0; i < 100; i++) {
            ASSERT_EQ(decoder.read_string(stream), std::string(c, 3));
        }
    }
}

TEST_F(DictionaryTest,
       DictionaryEncoderAndDecoderLargeQuantitiesWithRandomStrings) {
    DictionaryEncoder encoder;
    common::ByteStream stream(1024, common::MOD_DICENCODE_OBJ);
    encoder.init();

    // Prepare random string generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> length_dist(5, 20);  // String length range
    std::uniform_int_distribution<> char_dist(33,
                                              126);  // Printable ASCII range

    // Generate 10000 random strings
    const int num_strings = 10000;
    std::vector<std::string> test_strings;
    std::unordered_set<std::string> string_set;  // For ensuring uniqueness

    while (test_strings.size() < num_strings) {
        int length = length_dist(gen);
        std::string str;
        str.reserve(length);

        for (int i = 0; i < length; ++i) {
            str.push_back(static_cast<char>(char_dist(gen)));
        }

        // Ensure string uniqueness
        if (string_set.insert(str).second) {
            test_strings.push_back(str);
        }
    }

    // Encode all strings
    for (const auto& str : test_strings) {
        encoder.encode(str, stream);
    }
    encoder.flush(stream);

    DictionaryDecoder decoder;
    decoder.init();

    // Decode and verify all strings
    for (const auto& expected_str : test_strings) {
        std::string decoded_str = decoder.read_string(stream);
        ASSERT_EQ(decoded_str, expected_str);
    }
}

}  // namespace storage