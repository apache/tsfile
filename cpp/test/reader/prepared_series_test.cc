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

#include "reader/prepared_series.h"

#include <gtest/gtest.h>

#include <thread>
#include <vector>

namespace storage {
namespace {

PageLocator page(uint64_t begin, uint64_t end, uint64_t offset) {
    PageLocator result;
    result.row_begin = begin;
    result.row_end = end;
    result.page_data_offset = offset;
    return result;
}

TEST(PagePositionIndexTest, PublishesOnlyCompleteGapFreePrefixes) {
    PagePositionIndex index;
    EXPECT_FALSE(index.append_complete(page(1, 5, 100)));
    EXPECT_EQ(0U, index.covered_rows());
    EXPECT_TRUE(index.append_complete(page(0, 5, 100)));
    EXPECT_TRUE(index.append_complete(page(5, 9, 200)));
    EXPECT_FALSE(index.append_complete(page(8, 12, 300)));
    EXPECT_EQ(9U, index.covered_rows());
    EXPECT_EQ(2U, index.size());

    PageLocator found;
    EXPECT_TRUE(index.find(0, found));
    EXPECT_EQ(100U, found.page_data_offset);
    EXPECT_TRUE(index.find(8, found));
    EXPECT_EQ(200U, found.page_data_offset);
    EXPECT_FALSE(index.find(9, found));
}

TEST(PagePositionIndexTest, ConcurrentWritersPublishOneGapFreePrefix) {
    PagePositionIndex index;
    constexpr uint64_t page_count = 64;
    std::vector<std::thread> writers;
    writers.reserve(page_count);
    for (uint64_t row = 0; row < page_count; ++row) {
        writers.emplace_back([row, &index]() {
            PageLocator locator = page(row, row + 1, 1000 + row);
            while (index.covered_rows() <= row &&
                   !index.append_complete(locator)) {
                std::this_thread::yield();
            }
        });
    }
    for (std::thread& writer : writers) {
        writer.join();
    }

    EXPECT_EQ(page_count, index.covered_rows());
    EXPECT_EQ(page_count, index.size());
    for (uint64_t row = 0; row < page_count; ++row) {
        PageLocator found;
        ASSERT_TRUE(index.find(row, found));
        EXPECT_EQ(row, found.row_begin);
        EXPECT_EQ(1000 + row, found.page_data_offset);
    }
}

}  // namespace
}  // namespace storage
