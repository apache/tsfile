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
#include "common/container/hash_table.h"

#include <gtest/gtest.h>

namespace common {

class HashTableTest : public ::testing::Test {
   protected:
    void SetUp() override {}
    void TearDown() override {}
    common::HashTable<std::string, std::string, SliceHashFunc> hash_table_;
};

TEST_F(HashTableTest, Initialize) {
    EXPECT_EQ(hash_table_.init(), E_OK);
    hash_table_.destroy();
}

TEST_F(HashTableTest, PutAndGetSingleElement) {
    EXPECT_EQ(hash_table_.init(), E_OK);
    EXPECT_EQ(hash_table_.put("key1", "value1"), E_OK);

    std::string value;
    EXPECT_EQ(hash_table_.get("key1", value), E_OK);
    EXPECT_EQ(value, "value1");
}

TEST_F(HashTableTest, PutAndUpdateElement) {
    EXPECT_EQ(hash_table_.init(), E_OK);
    EXPECT_EQ(hash_table_.put("key1", "value1"), E_OK);
    EXPECT_EQ(hash_table_.put("key1", "value2"), E_OK);

    std::string value;
    EXPECT_EQ(hash_table_.get("key1", value), E_OK);
    EXPECT_EQ(value, "value2");
}

TEST_F(HashTableTest, GetNonExistentElement) {
    EXPECT_EQ(hash_table_.init(), E_OK);

    std::string value;
    EXPECT_EQ(hash_table_.get("key1", value), E_NOT_EXIST);
}

TEST_F(HashTableTest, RemoveElement) {
    EXPECT_EQ(hash_table_.init(), E_OK);
    EXPECT_EQ(hash_table_.put("key1", "value1"), E_OK);
    EXPECT_EQ(hash_table_.remove("key1"), E_OK);

    std::string value;
    EXPECT_EQ(hash_table_.get("key1", value), E_NOT_EXIST);
}

TEST_F(HashTableTest, HandleCollisions) {
    EXPECT_EQ(hash_table_.init(), E_OK);

    // Assuming hash function will cause a collision for keys 1 and 2
    EXPECT_EQ(hash_table_.put("key1", "value1"), E_OK);
    EXPECT_EQ(hash_table_.put("key2", "value2"), E_OK);

    std::string value;
    EXPECT_EQ(hash_table_.get("key1", value), E_OK);
    EXPECT_EQ(value, "value1");
    EXPECT_EQ(hash_table_.get("key2", value), E_OK);
    EXPECT_EQ(value, "value2");
}

TEST_F(HashTableTest, RehashDuringExtend) {
    EXPECT_EQ(hash_table_.init(), E_OK);

    for (int i = 0; i < TABLE_CAPACITY * SEGMENT_CAPACITY; ++i) {
        EXPECT_EQ(hash_table_.put(std::to_string(i), "value"), E_OK);
    }

    std::string value;
    for (int i = 0; i < TABLE_CAPACITY * SEGMENT_CAPACITY; ++i) {
        EXPECT_EQ(hash_table_.get(std::to_string(i), value), E_OK);
        EXPECT_EQ(value, "value");
    }
}

}  // namespace common