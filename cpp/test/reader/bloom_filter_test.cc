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
#include "reader/bloom_filter.h"

#include <gtest/gtest.h>

#include <unordered_set>
using namespace storage;
TEST(BloomfilterTest, BloomFilter) {
    BloomFilter filter;

    std::unordered_set<uint8_t> my_set = {0, 0, 0,   0,  0, 0, 0, 0, 2,
                                          0, 2, 128, 32, 0, 0, 1, 0, 4,
                                          0, 0, 0,   16, 0, 0, 0, 0, 32};

    filter.init(0.1, 10);
    common::PageArena arena;
    common::String device1 = common::String("test_table.test1.test", arena);
    common::String sensor = common::String();
    filter.add_path_entry(device1, sensor);
    common::String sensor1 = common::String("value", arena);
    filter.add_path_entry(device1, sensor1);
    common::ByteStream out(1024, common::MOD_DEFAULT);
    uint8_t* filter_data_bytes = nullptr;
    int32_t filter_data_bytes_len = 0;
    filter.get_bit_set()->to_bytes(filter_data_bytes, filter_data_bytes_len);
    std::unordered_set<uint8_t> data;
    for (int i = 0; i < filter_data_bytes_len; i++) {
        data.insert(static_cast<int>(filter_data_bytes[i]));
        ASSERT_TRUE(my_set.find(static_cast<int>(filter_data_bytes[i])) !=
                    my_set.end());
    }
    filter.serialize_to(out);

    BloomFilter filter2;
    filter2.deserialize_from(out);
    // ASSERT_EQ(filter, filter2);
    uint8_t* filter_data_bytes2 = nullptr;
    int32_t filter_data_bytes_len2 = 0;
    filter2.get_bit_set()->to_bytes(filter_data_bytes2, filter_data_bytes_len2);
    ASSERT_EQ(filter_data_bytes_len, filter_data_bytes_len2);
    for (int i = 0; i < filter_data_bytes_len2; i++) {
        ASSERT_TRUE(data.find(static_cast<int>(filter_data_bytes2[i])) !=
                    data.end());
        ASSERT_TRUE(my_set.find(static_cast<int>(filter_data_bytes2[i])) !=
                    my_set.end());
    }
    common::mem_free(filter_data_bytes);
    common::mem_free(filter_data_bytes2);
}

TEST(BloomfilterTest, EmptyFilterUsesJavaCompatibleEncoding) {
    BloomFilter filter;
    ASSERT_EQ(common::E_OK, filter.init(0.1, 0));

    common::ByteStream out(1024, common::MOD_DEFAULT);
    ASSERT_EQ(common::E_OK, filter.serialize_to(out));
    ASSERT_EQ(1U, out.total_size());

    BloomFilter deserialized;
    ASSERT_EQ(common::E_OK, deserialized.deserialize_from(out));
    EXPECT_TRUE(deserialized.is_empty());
    EXPECT_EQ(0U, out.remaining_size());
}

TEST(BloomfilterTest, RejectsInvalidHashFunctionCount) {
    common::ByteStream out(1024, common::MOD_DEFAULT);
    const uint8_t filter_byte = 1;
    ASSERT_EQ(common::E_OK, common::SerializationUtil::write_var_uint(1, out));
    ASSERT_EQ(common::E_OK, out.write_buf(&filter_byte, 1));
    ASSERT_EQ(common::E_OK,
              common::SerializationUtil::write_var_uint(256, out));
    ASSERT_EQ(common::E_OK, common::SerializationUtil::write_var_uint(
                                BloomFilter::MAX_HASH_FUNC_COUNT + 1, out));

    BloomFilter filter;
    EXPECT_EQ(common::E_TSFILE_CORRUPTED, filter.deserialize_from(out));
}

TEST(BloomfilterTest, RejectsFilterLengthBeyondRemainingInput) {
    common::ByteStream out(1024, common::MOD_DEFAULT);
    ASSERT_EQ(common::E_OK,
              common::SerializationUtil::write_var_uint(1024, out));

    BloomFilter filter;
    EXPECT_EQ(common::E_TSFILE_CORRUPTED, filter.deserialize_from(out));
}
