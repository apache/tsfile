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
#include <unordered_set>

#include <gtest/gtest.h>

#include "reader/bloom_filter.h"
using namespace storage;
TEST(BloomfilterTest, BloomFilter) {
    BloomFilter filter;
    filter.init(0.1, 10);
    common::PageArena arena;
    common::String device1 = common::String("test_table.test1.test", arena);
    common::String sensor = common::String();
    filter.add_path_entry(device1, sensor);
    common::String sensor1 = common::String("value", arena);
    filter.add_path_entry(device1, sensor1);
    common::ByteStream out(1024, common::MOD_DEFAULT);
    uint8_t *filter_data_bytes = nullptr;
    int32_t filter_data_bytes_len = 0;
    filter.get_bit_set()->to_bytes(filter_data_bytes, filter_data_bytes_len);
    std::unordered_set<uint8_t> data;
    for (int i = 0; i < filter_data_bytes_len; i++) {
        data.insert(static_cast<int>(filter_data_bytes[i]));
        std::cout << static_cast<int>(filter_data_bytes[i]) << " ";
    }
    filter.serialize_to(out);

    BloomFilter filter2;
    filter2.deserialize_from(out);
    // ASSERT_EQ(filter, filter2);
    uint8_t *filter_data_bytes2 = nullptr;
    filter2.get_bit_set()->to_bytes(filter_data_bytes2, filter_data_bytes_len);
    for (int i = 0; i < filter_data_bytes_len; i++) {
        ASSERT_TRUE(data.find(static_cast<int>(filter_data_bytes2[i])) != data.end());
    }
}
