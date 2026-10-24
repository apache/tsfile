/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

#include "common/cache/lru_cache.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace common {

TEST(LruCacheTest, StrictCapacityPromotesAndEvicts) {
    Cache<int, std::string> cache(2, 0);
    cache.insert(1, "one");
    cache.insert(2, "two");

    const std::string* one = cache.getPtr(1);
    ASSERT_NE(one, nullptr);
    EXPECT_EQ(*one, "one");

    cache.insert(3, "three");
    EXPECT_TRUE(cache.contains(1));
    EXPECT_FALSE(cache.contains(2));
    EXPECT_TRUE(cache.contains(3));
    EXPECT_EQ(cache.size(), 2u);
}

TEST(LruCacheTest, UpdateReplacesValueAndPromotes) {
    Cache<int, std::string> cache(2, 0);
    cache.insert(1, "one");
    cache.insert(2, "two");
    cache.insert(1, "updated");
    cache.insert(3, "three");

    EXPECT_EQ(cache.getRef(1), "updated");
    EXPECT_FALSE(cache.contains(2));
}

TEST(LruCacheTest, ElasticityPrunesAfterHardLimitIsExceeded) {
    Cache<int, int> cache(2, 2);
    for (int i = 1; i <= 4; ++i) {
        cache.insert(i, i);
    }
    EXPECT_EQ(cache.size(), 4u);
    EXPECT_EQ(cache.getMaxAllowedSize(), 4u);

    cache.insert(5, 5);
    EXPECT_EQ(cache.size(), 2u);
    EXPECT_TRUE(cache.contains(5));
    EXPECT_TRUE(cache.contains(4));
    EXPECT_FALSE(cache.contains(3));
}

TEST(LruCacheTest, CopyAndReferenceLookupsAreExplicit) {
    Cache<int, std::string> cache(2, 0);
    cache.insert(1, "one");

    std::string copied;
    EXPECT_TRUE(cache.tryGetCopy(1, copied));
    EXPECT_EQ(copied, "one");

    const std::string* ref = nullptr;
    EXPECT_TRUE(cache.tryGetRef(1, ref));
    ASSERT_NE(ref, nullptr);
    EXPECT_EQ(*ref, "one");
    EXPECT_EQ(cache.getCopy(1), "one");
    EXPECT_THROW(cache.getRef(99), std::out_of_range);
}

TEST(LruCacheTest, NonCopyingLookupSupportsMoveOnlyValues) {
    Cache<int, std::unique_ptr<int>> cache(1, 0);
    cache.insert(1, std::unique_ptr<int>(new int(7)));

    const std::unique_ptr<int>* value = nullptr;
    ASSERT_TRUE(cache.tryGetRef(1, value));
    ASSERT_NE(value, nullptr);
    ASSERT_NE(value->get(), nullptr);
    EXPECT_EQ(**value, 7);

    cache.insert(1, std::unique_ptr<int>(new int(9)));
    value = cache.getPtr(1);
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(**value, 9);
}

TEST(LruCacheTest, RemoveClearAndUnboundedMode) {
    Cache<int, int> bounded(2, 0);
    bounded.insert(1, 1);
    EXPECT_TRUE(bounded.remove(1));
    EXPECT_FALSE(bounded.remove(1));
    bounded.insert(2, 2);
    bounded.clear();
    EXPECT_TRUE(bounded.empty());

    Cache<int, int> unbounded(0, 100);
    for (int i = 0; i < 200; ++i) {
        unbounded.insert(i, i);
    }
    EXPECT_EQ(unbounded.size(), 200u);
    EXPECT_EQ(unbounded.getMaxAllowedSize(), 0u);
}

}  // namespace common
