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
#include "common/allocator/my_string.h"

#include <gtest/gtest.h>

namespace {

class StringTest : public ::testing::Test {
   public:
    common::PageArena arena_;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StringTest, DefaultConstructorAndIsNull) {
    common::String s;
    EXPECT_TRUE(s.is_null());
    EXPECT_EQ(s.len_, 0u);
    EXPECT_EQ(s.buf_, nullptr);
}

TEST_F(StringTest, DupFromString) {
    std::string input = (char*)"Hello";
    common::String s;
    int result = s.dup_from(input, arena_);

    EXPECT_EQ(result, common::E_OK);
    EXPECT_EQ(s.len_, input.size());
    EXPECT_NE(s.buf_, nullptr);
    EXPECT_EQ(memcmp(s.buf_, input.c_str(), s.len_), 0);
}

TEST_F(StringTest, DupFromStringObject) {
    common::String s1((char*)"World", 5);
    common::String s2;
    int result = s2.dup_from(s1, arena_);

    EXPECT_EQ(result, common::E_OK);
    EXPECT_EQ(s2.len_, s1.len_);
    EXPECT_NE(s2.buf_, nullptr);
    EXPECT_EQ(memcmp(s2.buf_, s1.buf_, s2.len_), 0);
}

TEST_F(StringTest, BuildFromStringObjects) {
    common::String s1((char*)"Hello", 5);
    common::String s2((char*)"World", 5);
    common::String result;
    int build_result = result.build_from(s1, s2, arena_);

    EXPECT_EQ(build_result, common::E_OK);
    EXPECT_EQ(result.len_, s1.len_ + s2.len_);
    EXPECT_NE(result.buf_, nullptr);
    EXPECT_EQ(memcmp(result.buf_, s1.buf_, s1.len_), 0);
    EXPECT_EQ(memcmp(result.buf_ + s1.len_, s2.buf_, s2.len_), 0);
}

TEST_F(StringTest, EqualToStringObjects) {
    common::String s1((char*)"Hello", 5);
    common::String s2((char*)"Hello", 5);
    common::String s3((char*)"World", 5);

    EXPECT_TRUE(s1.equal_to(s2));
    EXPECT_FALSE(s1.equal_to(s3));
}

TEST_F(StringTest, LessThanStringObjects) {
    common::String s1((char*)"Hello", 5);
    common::String s2((char*)"World", 5);
    common::String s3((char*)"Hell", 4);

    EXPECT_TRUE(s1.less_than(s2));
    EXPECT_FALSE(s2.less_than(s1));
    EXPECT_TRUE(s3.less_than(s1));
}

TEST_F(StringTest, CompareStringObjects) {
    common::String s1((char*)"Hello", 5);
    common::String s2((char*)"World", 5);
    common::String s3((char*)"Hello", 5);
    common::String s4((char*)"Hell", 4);

    EXPECT_EQ(s1.compare(s3), 0);
    EXPECT_GT(s2.compare(s1), 0);
    EXPECT_LT(s4.compare(s1), 0);
    EXPECT_EQ(s1.compare(s3), 0);
}

}  // namespace
