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
#include "common/container/list.h"

#include <gtest/gtest.h>

namespace common {

class SimpleListTest : public ::testing::Test {
   protected:
    void SetUp() override { page_arena_.init(1024, MOD_DEFAULT); }
    PageArena page_arena_;
};

TEST_F(SimpleListTest, PushBackAndFront) {
    SimpleList<int> list(&page_arena_);

    EXPECT_EQ(list.size(), 0);

    list.push_back(10);
    EXPECT_EQ(list.size(), 1);
    EXPECT_EQ(list.front(), 10);

    list.push_back(20);
    EXPECT_EQ(list.size(), 2);
    EXPECT_EQ(list.front(), 10);
}

TEST_F(SimpleListTest, Iterator) {
    SimpleList<int> list(&page_arena_);
    list.push_back(10);
    list.push_back(20);
    list.push_back(30);

    SimpleList<int>::Iterator it = list.begin();
    EXPECT_TRUE(it.is_inited());
    EXPECT_EQ(it.get(), 10);
    it++;
    EXPECT_EQ(it.get(), 20);
    it++;
    EXPECT_EQ(it.get(), 30);
    it++;

    SimpleList<int>::Iterator end_it = list.end();
    EXPECT_EQ(it, end_it);
}

TEST_F(SimpleListTest, Remove) {
    SimpleList<int> list(&page_arena_);
    list.push_back(10);
    list.push_back(20);
    list.push_back(30);

    EXPECT_EQ(list.remove(20), common::E_OK);
    EXPECT_EQ(list.size(), 2);

    SimpleList<int>::Iterator it = list.begin();
    EXPECT_EQ(it.get(), 10);
    it++;
    EXPECT_EQ(it.get(), 30);
    it++;
    SimpleList<int>::Iterator end_it = list.end();
    EXPECT_EQ(it, end_it);

    EXPECT_EQ(list.remove(40), common::E_NOT_EXIST);
}

TEST_F(SimpleListTest, Clear) {
    SimpleList<int> list(&page_arena_);
    list.push_back(10);
    list.push_back(20);
    list.push_back(30);

    list.clear();
    EXPECT_EQ(list.size(), 0);
    EXPECT_EQ(list.begin(), list.end());
}

}  // namespace common