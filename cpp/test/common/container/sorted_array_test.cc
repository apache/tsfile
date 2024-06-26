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
#include "common/container/sorted_array.h"

#include <gtest/gtest.h>

namespace common {

TEST(SortedArrayTest, Initialization) {
    SortedArray<int> array;
    array.init();
    EXPECT_TRUE(array.empty());
    EXPECT_EQ(array.size(), 0);
    EXPECT_EQ(array.capacity(), SORTED_ARRAY_INIT_CAPACITY);
    array.destroy();
}

TEST(SortedArrayTest, InsertAndSize) {
    SortedArray<int> array;
    array.init();
    array.insert(5);
    EXPECT_EQ(array.size(), 1);
    EXPECT_EQ(array[0], 5);
    array.destroy();
}

TEST(SortedArrayTest, InsertMultiple) {
    SortedArray<int> array;
    array.init();
    array.insert(5);
    array.insert(1);
    array.insert(3);
    EXPECT_EQ(array.size(), 3);
    EXPECT_EQ(array[0], 1);
    EXPECT_EQ(array[1], 3);
    EXPECT_EQ(array[2], 5);
    array.destroy();
}

TEST(SortedArrayTest, InsertDuplicate) {
    SortedArray<int> array;
    array.init();
    array.insert(5);
    int ret = array.insert(5);
    EXPECT_EQ(ret, E_ALREADY_EXIST);
    EXPECT_EQ(array.size(), 1);
    array.destroy();
}

TEST(SortedArrayTest, FindValue) {
    SortedArray<int> array;
    array.init();
    array.insert(5);
    array.insert(1);
    array.insert(3);
    bool found = false;
    size_t pos = array.find(3, found);
    EXPECT_TRUE(found);
    EXPECT_EQ(pos, 1);
    pos = array.find(4, found);
    EXPECT_FALSE(found);
    EXPECT_EQ(pos, 2);
    array.destroy();
}

TEST(SortedArrayTest, RemoveValue) {
    SortedArray<int> array;
    array.init();
    array.insert(5);
    array.insert(1);
    array.insert(3);
    array.remove_value(3);
    EXPECT_EQ(array.size(), 2);
    EXPECT_EQ(array[0], 1);
    EXPECT_EQ(array[1], 5);
    array.destroy();
}

TEST(SortedArrayTest, RemoveByIndex) {
    SortedArray<int> array;
    array.init();
    array.insert(5);
    array.insert(1);
    array.insert(3);
    int removed = array.remove(1);
    EXPECT_EQ(removed, 3);
    EXPECT_EQ(array.size(), 2);
    EXPECT_EQ(array[0], 1);
    EXPECT_EQ(array[1], 5);
    array.destroy();
}

TEST(SortedArrayTest, GetMinMax) {
    SortedArray<int> array;
    array.init();
    array.insert(5);
    array.insert(1);
    array.insert(3);
    EXPECT_EQ(array.get_min(), 1);
    EXPECT_EQ(array.get_max(), 5);
    array.destroy();
}

TEST(SortedArrayTest, ClearArray) {
    SortedArray<int> array;
    array.init();
    array.insert(5);
    array.insert(1);
    array.insert(3);
    array.clear();
    EXPECT_TRUE(array.empty());
    EXPECT_EQ(array.size(), 0);
    array.destroy();
}

TEST(SortedArrayTest, ExtendArray) {
    SortedArray<int> array;
    array.init();
    for (int i = 0; i < SORTED_ARRAY_INIT_CAPACITY + 1; ++i) {
        array.insert(i);
    }
    EXPECT_EQ(array.size(), SORTED_ARRAY_INIT_CAPACITY + 1);
    EXPECT_GT(array.capacity(), SORTED_ARRAY_INIT_CAPACITY);
    array.destroy();
}

TEST(SortedArrayTest, ShrinkArray) {
    SortedArray<int> array;
    array.init();
    for (int i = 0; i < SORTED_ARRAY_INIT_CAPACITY + 1; ++i) {
        array.insert(i);
    }
    int size = array.size();
    for (int i = 0; i < size; ++i) {
        array.remove(0);
    }
    EXPECT_LT(array.capacity(), SORTED_ARRAY_INIT_CAPACITY);
    array.destroy();
}

}  // namespace common
