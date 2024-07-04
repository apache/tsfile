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
#include "common/container/slice.h"

#include <gtest/gtest.h>

namespace common {

TEST(SliceTest, DefaultConstructor) {
    Slice slice;
    EXPECT_EQ(slice.size(), 0);
    EXPECT_EQ(slice.data(), nullptr);
    EXPECT_TRUE(slice.empty());
}

TEST(SliceTest, StringConstructor) {
    std::string str = "test";
    Slice slice(str);
    EXPECT_EQ(slice.size(), str.size());
    EXPECT_FALSE(slice.empty());
}

TEST(SliceTest, CStrConstructor) {
    const char* cstr = "test";
    Slice slice(cstr);
    EXPECT_EQ(slice.size(), strlen(cstr));
    EXPECT_FALSE(slice.empty());
}

TEST(SliceTest, CopyConstructor) {
    std::string str = "test";
    Slice slice1(str);
    Slice slice2(slice1);
    EXPECT_EQ(slice1.size(), slice2.size());
}

TEST(SliceTest, AssignmentOperator) {
    std::string str1 = "test1";
    std::string str2 = "test2";
    Slice slice1(str1);
    Slice slice2(str2);
    slice2 = slice1;
    EXPECT_EQ(slice1.size(), slice2.size());
}

TEST(SliceTest, DataAccess) {
    std::string str = "test";
    Slice slice(str);
    EXPECT_EQ(slice[0], 't');
    EXPECT_EQ(slice[1], 'e');
    EXPECT_EQ(slice[2], 's');
    EXPECT_EQ(slice[3], 't');
}

TEST(SliceTest, ToString) {
    std::string str = "test";
    Slice slice(str);
    EXPECT_EQ(slice.to_string(), str);
}

TEST(SliceTest, ComparisonOperators) {
    Slice slice1("test");
    Slice slice2("test");
    Slice slice3("different");

    EXPECT_EQ(slice1, slice2);
    EXPECT_NE(slice1, slice3);
}

TEST(SliceTest, CompareFunction) {
    Slice slice1("abc");
    Slice slice2("abcd");
    Slice slice3("abc");

    EXPECT_LT(slice1.compare(slice2), 0);
    EXPECT_GT(slice2.compare(slice1), 0);
    EXPECT_EQ(slice1.compare(slice3), 0);
}

}  // namespace common
