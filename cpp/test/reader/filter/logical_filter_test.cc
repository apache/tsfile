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

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/statistic.h"
#include "reader/filter/and_filter.h"
#include "reader/filter/or_filter.h"
#include "reader/filter/time_operator.h"

namespace storage {
namespace {

using Range = std::pair<int64_t, int64_t>;

Statistic make_stat(int64_t start_time, int64_t end_time) {
    Statistic stat;
    stat.start_time_ = start_time;
    stat.end_time_ = end_time;
    return stat;
}

std::vector<Range> collect_time_ranges(Filter& filter) {
    std::unique_ptr<std::vector<TimeRange*>> raw(filter.get_time_ranges());
    std::vector<Range> ranges;
    for (TimeRange* range : *raw) {
        ranges.emplace_back(range->start_time_, range->end_time_);
        delete range;
    }
    return ranges;
}

std::vector<bool> run_batch(Filter& filter, const std::vector<int64_t>& times) {
    std::unique_ptr<bool[]> mask =
        times.empty() ? nullptr
                      : std::unique_ptr<bool[]>(new bool[times.size()]);
    const int pass = filter.satisfy_batch_time(
        times.data(), static_cast<int>(times.size()), mask.get());
    std::vector<bool> result(times.size());
    int counted = 0;
    for (size_t i = 0; i < times.size(); ++i) {
        result[i] = mask[i];
        if (mask[i]) {
            ++counted;
        }
    }
    EXPECT_EQ(pass, counted);
    return result;
}

TEST(LogicalFilterTest, AndCombinesPruningAndRowPredicates) {
    TimeBetween between(2, 4, /*not_between=*/false);
    TimeEq eq(3);
    AndFilter filter(&between, &eq);

    Statistic spanning = make_stat(2, 5);
    Statistic without_point = make_stat(4, 6);
    EXPECT_TRUE(filter.satisfy(&spanning));
    EXPECT_FALSE(filter.satisfy(&without_point));

    EXPECT_TRUE(filter.satisfy(3, 0));
    EXPECT_FALSE(filter.satisfy(4, 0));

    EXPECT_TRUE(filter.satisfy_start_end_time(2, 5));
    EXPECT_FALSE(filter.satisfy_start_end_time(4, 6));
    EXPECT_TRUE(filter.contain_start_end_time(3, 3));
    EXPECT_FALSE(filter.contain_start_end_time(2, 4));
    EXPECT_FALSE(filter.contain_start_end_time(2, 2));

    EXPECT_EQ(collect_time_ranges(filter), (std::vector<Range>{{3, 3}}));
    EXPECT_EQ(run_batch(filter, {1, 2, 3, 4, 5, 6}),
              (std::vector<bool>{false, false, true, false, false, false}));
}

TEST(LogicalFilterTest, OrUnionsPruningAndRowPredicates) {
    TimeBetween between(2, 4, /*not_between=*/false);
    TimeGt gt(5);
    OrFilter filter(&between, &gt);

    Statistic low = make_stat(1, 2);
    Statistic high = make_stat(6, 8);
    Statistic miss = make_stat(5, 5);
    EXPECT_TRUE(filter.satisfy(&low));
    EXPECT_TRUE(filter.satisfy(&high));
    EXPECT_FALSE(filter.satisfy(&miss));

    EXPECT_TRUE(filter.satisfy(3, 0));
    EXPECT_TRUE(filter.satisfy(6, 0));
    EXPECT_FALSE(filter.satisfy(1, 0));

    EXPECT_TRUE(filter.satisfy_start_end_time(1, 2));
    EXPECT_TRUE(filter.satisfy_start_end_time(6, 9));
    EXPECT_FALSE(filter.satisfy_start_end_time(5, 5));
    EXPECT_TRUE(filter.contain_start_end_time(2, 4));
    EXPECT_FALSE(filter.contain_start_end_time(1, 9));

    EXPECT_EQ(
        collect_time_ranges(filter),
        (std::vector<Range>{{2, 4}, {6, std::numeric_limits<int64_t>::max()}}));
    EXPECT_EQ(run_batch(filter, {1, 2, 3, 4, 5, 6}),
              (std::vector<bool>{false, true, true, true, false, true}));
}

TEST(LogicalFilterTest, NestedLogicalFiltersPropagate) {
    TimeBetween between(1, 3, /*not_between=*/false);
    TimeGt gt(6);
    OrFilter outer_or(&between, &gt);
    TimeEq eq(2);
    AndFilter nested(&outer_or, &eq);

    EXPECT_TRUE(nested.satisfy(2, 0));
    EXPECT_FALSE(nested.satisfy(3, 0));
    EXPECT_FALSE(nested.satisfy(7, 0));
    EXPECT_EQ(collect_time_ranges(nested), (std::vector<Range>{{2, 2}}));
    EXPECT_EQ(run_batch(nested, {1, 2, 3, 7}),
              (std::vector<bool>{false, true, false, false}));
}

}  // namespace
}  // namespace storage
