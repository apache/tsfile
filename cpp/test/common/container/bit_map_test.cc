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
#include "common/container/bit_map.h"

#include <gtest/gtest.h>

class BitMapTest : public ::testing::Test {
   protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(BitMapTest, Initialization) {
    common::BitMap bitmap;
    EXPECT_EQ(bitmap.init(100), common::E_OK);
    EXPECT_EQ(bitmap.get_size(), 13);
    EXPECT_TRUE(bitmap.get_bitmap() != nullptr);
}

TEST_F(BitMapTest, Reset) {
    common::BitMap bitmap;
    bitmap.init(100);
    bitmap.set(10);
    bitmap.set(20);
    bitmap.reset();
    for (int i = 0; i < 100; ++i) {
        EXPECT_FALSE(bitmap.test(i));
    }
}

TEST_F(BitMapTest, SetAnd) {
    common::BitMap bitmap;
    bitmap.init(100);
    bitmap.set(10);
    bitmap.set(20);
    EXPECT_TRUE(bitmap.test(10));
    EXPECT_TRUE(bitmap.test(20));
    EXPECT_FALSE(bitmap.test(30));
}

TEST_F(BitMapTest, Clear) {
    common::BitMap bitmap;
    bitmap.init(100);
    bitmap.set(10);
    bitmap.set(20);
    bitmap.clear(10);
    EXPECT_FALSE(bitmap.test(10));
    EXPECT_TRUE(bitmap.test(20));
}

TEST_F(BitMapTest, ClearMultipleBits) {
    common::BitMap bitmap;
    bitmap.init(100);
    bitmap.set(5);
    bitmap.set(15);
    bitmap.set(25);

    ASSERT_TRUE(bitmap.test(5));
    ASSERT_TRUE(bitmap.test(15));
    ASSERT_TRUE(bitmap.test(25));

    bitmap.clear(5);
    bitmap.clear(15);
    bitmap.clear(25);

    ASSERT_FALSE(bitmap.test(5));
    ASSERT_FALSE(bitmap.test(15));
    ASSERT_FALSE(bitmap.test(25));
}

TEST_F(BitMapTest, InitializeAsOne) {
    common::BitMap bitmap;
    bitmap.init(100, false);
    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(bitmap.test(i));
    }
}
