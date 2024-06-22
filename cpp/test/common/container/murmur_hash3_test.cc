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
#include "common/container/murmur_hash3.h"

#include <gtest/gtest.h>

namespace common {

class Murmur128HashTest : public ::testing::Test {
   protected:
    struct TestData {
        std::string string_val;
        uint32_t seed;
        int32_t expected_string_hash;

        TestData(std::string stringVal, uint32_t seed,
                 int32_t expectedStringHash)
            : string_val(std::move(stringVal)),
              seed(seed),
              expected_string_hash(expectedStringHash) {}
    };

    // Generated using Java Murmur128Hash
    TestData test_data_[3] = {
        TestData("testString", 12345, -1444917689),
        TestData("Special characters: @#$%^&*()_+", 24680, 580454411),
        TestData(
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
            "eiusmod tempor incididunt ut labore et dolore magna aliqua.",
            54321, -207787044)};
};

TEST_F(Murmur128HashTest, HashString) {
    for (const auto& data : test_data_) {
        std::string str(data.string_val);
        int32_t hash_result = Murmur128Hash::hash(str, data.seed);
        EXPECT_EQ(hash_result, data.expected_string_hash);
    }
}

}  // namespace common
