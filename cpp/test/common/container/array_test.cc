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
#include "common/container/array.h"

#include <gtest/gtest.h>

namespace common {

class ArrayTest : public ::testing::Test {
   protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ArrayTest, Initialization) {
    common::Array<int> arr;
    EXPECT_EQ(arr.size(), 0);
    EXPECT_EQ(arr.capacity(), ARRAY_INIT_CAPACITY);
    EXPECT_TRUE(arr.empty());
}

TEST_F(ArrayTest, CustomInitialization) {
    size_t custom_capacity = 500;
    common::Array<int> arr(custom_capacity);
    EXPECT_EQ(arr.size(), 0);
    EXPECT_EQ(arr.capacity(), custom_capacity);
}

TEST_F(ArrayTest, InitAndDestroy) {
    common::Array<int> arr;
    EXPECT_EQ(arr.init(), E_OK);

    arr.destroy();
    EXPECT_EQ(arr.capacity(), 0);
    EXPECT_EQ(arr.size(), 0);
}

TEST_F(ArrayTest, Append) {
    common::Array<int> arr;
    arr.init();
    EXPECT_EQ(arr.append(1), E_OK);
    EXPECT_EQ(arr.size(), 1);
    EXPECT_EQ(arr[0], 1);
}

TEST_F(ArrayTest, Insert) {
    common::Array<int> arr;
    arr.init();
    arr.append(1);
    arr.append(2);
    arr.append(3);

    EXPECT_EQ(arr.insert(1, 5), E_OK);
    EXPECT_EQ(arr.size(), 4);
    EXPECT_EQ(arr[1], 5);
    EXPECT_EQ(arr[2], 2);
}

TEST_F(ArrayTest, Remove) {
    common::Array<int> arr;
    arr.init();
    arr.append(1);
    arr.append(2);
    arr.append(3);

    int removed = arr.remove(1);
    EXPECT_EQ(removed, 2);
    EXPECT_EQ(arr.size(), 2);
    EXPECT_EQ(arr[1], 3);
}

TEST_F(ArrayTest, RemoveValue) {
    common::Array<int> arr;
    arr.init();
    arr.append(1);
    arr.append(2);
    arr.append(3);
    arr.append(2);

    EXPECT_EQ(arr.remove_value(2), E_OK);
    EXPECT_EQ(arr.size(), 2);
    EXPECT_FALSE(arr.contain(2));
}

TEST_F(ArrayTest, Find) {
    common::Array<int> arr;
    arr.init();
    arr.append(1);
    arr.append(2);
    arr.append(3);

    bool found;
    size_t idx = arr.find(2, found);
    EXPECT_TRUE(found);
    EXPECT_EQ(idx, 1);

    idx = arr.find(4, found);
    EXPECT_FALSE(found);
    EXPECT_EQ(idx, 0);
}

TEST_F(ArrayTest, AtAndOperator) {
    common::Array<int> arr;
    arr.init();
    arr.append(1);
    arr.append(2);
    arr.append(3);

    EXPECT_EQ(arr.at(0), 1);
    EXPECT_EQ(arr[1], 2);
}

TEST_F(ArrayTest, Clear) {
    common::Array<int> arr;
    arr.init();
    arr.append(1);
    arr.append(2);
    arr.append(3);

    arr.clear();
    EXPECT_EQ(arr.size(), 0);
}

TEST_F(ArrayTest, CapacityExtend) {
    common::Array<int> arr;
    arr.init();

    for (int i = 0; i < ARRAY_INIT_CAPACITY + 1; ++i) {
        arr.append(i);
    }

    EXPECT_GT(arr.capacity(), ARRAY_INIT_CAPACITY);
    EXPECT_EQ(arr.size(), ARRAY_INIT_CAPACITY + 1);
}

TEST_F(ArrayTest, CapacityShrink) {
    common::Array<int> arr;
    arr.init();

    for (int i = 0; i < ARRAY_INIT_CAPACITY; ++i) {
        arr.append(i);
    }

    for (int i = 0; i < ARRAY_INIT_CAPACITY; ++i) {
        arr.remove(0);
    }

    EXPECT_LT(arr.capacity(), ARRAY_INIT_CAPACITY);
    EXPECT_EQ(arr.size(), 0);
}

}  // namespace common