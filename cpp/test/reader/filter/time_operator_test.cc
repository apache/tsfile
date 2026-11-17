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

#include "reader/filter/time_operator.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/statistic.h"

namespace storage {
namespace {

using Range = std::pair<int64_t, int64_t>;

Statistic make_stat(int64_t start_time, int64_t end_time) {
    Statistic stat;
    stat.start_time_ = start_time;
    stat.end_time_ = end_time;
    return stat;
}

bool satisfy_stat(Filter& filter, int64_t start_time, int64_t end_time) {
    Statistic stat = make_stat(start_time, end_time);
    return filter.satisfy(&stat);
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

// Six and five elements exercise both the vector width and the scalar tail on
// every platform (NEON processes pairs, the SSE/AVX2 kernel processes quads).
const std::vector<int64_t> kSixTimes = {1, 2, 3, 4, 5, 6};

TEST(TimeOperatorTest, TimeBetweenBoundarySemantics) {
    TimeBetween between(3, 5, /*not_between=*/false);
    Statistic overlap = make_stat(4, 8);
    Statistic below = make_stat(1, 2);
    Statistic above = make_stat(6, 9);
    EXPECT_TRUE(between.satisfy(&overlap));
    EXPECT_FALSE(between.satisfy(&below));
    EXPECT_FALSE(between.satisfy(&above));

    EXPECT_TRUE(between.satisfy(3, 0));
    EXPECT_TRUE(between.satisfy(5, 0));
    EXPECT_FALSE(between.satisfy(2, 0));
    EXPECT_FALSE(between.satisfy(6, 0));
    EXPECT_TRUE(between.satisfy(4, common::String("ignored")));
    EXPECT_FALSE(between.satisfy(6, common::String("ignored")));

    EXPECT_TRUE(between.satisfy_start_end_time(2, 4));
    EXPECT_FALSE(between.satisfy_start_end_time(6, 8));
    EXPECT_TRUE(between.contain_start_end_time(3, 5));
    EXPECT_FALSE(between.contain_start_end_time(2, 5));

    EXPECT_EQ(collect_time_ranges(between), (std::vector<Range>{{3, 5}}));
    EXPECT_FALSE(between.is_unbounded_time_filter());
    EXPECT_TRUE(between.is_contiguous_time_filter());
    EXPECT_EQ(between.get_filter_type(), TIME_FILTER);

    EXPECT_EQ(run_batch(between, kSixTimes),
              (std::vector<bool>{false, false, true, true, true, false}));
}

TEST(TimeOperatorTest, TimeBetweenNotBetweenSemantics) {
    TimeBetween not_between(3, 5, /*not_between=*/true);
    Statistic inside = make_stat(4, 4);
    Statistic outside = make_stat(6, 9);
    EXPECT_FALSE(not_between.satisfy(&inside));
    EXPECT_TRUE(not_between.satisfy(&outside));

    EXPECT_FALSE(not_between.satisfy(4, 0));
    EXPECT_TRUE(not_between.satisfy(6, 0));
    EXPECT_TRUE(not_between.satisfy(2, common::String("ignored")));

    EXPECT_TRUE(not_between.satisfy_start_end_time(0, 2));
    EXPECT_TRUE(not_between.contain_start_end_time(6, 8));
    EXPECT_FALSE(not_between.contain_start_end_time(4, 5));

    EXPECT_EQ(collect_time_ranges(not_between),
              (std::vector<Range>{{std::numeric_limits<int64_t>::min(), 2},
                                  {6, std::numeric_limits<int64_t>::max()}}));
    EXPECT_FALSE(not_between.is_contiguous_time_filter());
    EXPECT_EQ(run_batch(not_between, kSixTimes),
              (std::vector<bool>{true, true, false, false, false, true}));
}

TEST(TimeOperatorTest, TimeBetweenInvertedRange) {
    TimeBetween inverted(5, 3, /*not_between=*/false);
    Statistic any = make_stat(0, 100);
    EXPECT_FALSE(inverted.satisfy(&any));
    EXPECT_FALSE(inverted.satisfy(4, 0));
    EXPECT_FALSE(inverted.satisfy_start_end_time(0, 100));
    EXPECT_FALSE(inverted.contain_start_end_time(0, 100));
    EXPECT_TRUE(collect_time_ranges(inverted).empty());

    TimeBetween inverted_not(5, 3, /*not_between=*/true);
    EXPECT_TRUE(inverted_not.satisfy(&any));
    EXPECT_TRUE(inverted_not.satisfy(4, 0));
    EXPECT_TRUE(inverted_not.satisfy_start_end_time(0, 100));
    EXPECT_EQ(collect_time_ranges(inverted_not),
              (std::vector<Range>{{std::numeric_limits<int64_t>::min(),
                                   std::numeric_limits<int64_t>::max()}}));

    TimeBetween unbounded(std::numeric_limits<int64_t>::min(),
                          std::numeric_limits<int64_t>::max(),
                          /*not_between=*/false);
    EXPECT_TRUE(unbounded.is_unbounded_time_filter());
}

TEST(TimeOperatorTest, TimeInSemantics) {
    TimeIn in({2, 8}, /*not_in=*/false);
    Statistic any = make_stat(10, 20);
    EXPECT_TRUE(in.satisfy(&any));
    EXPECT_TRUE(in.satisfy(2, 0));
    EXPECT_FALSE(in.satisfy(5, 0));
    EXPECT_TRUE(in.satisfy(8, common::String("ignored")));
    EXPECT_FALSE(in.satisfy(9, common::String("ignored")));
    EXPECT_FALSE(in.satisfy_start_end_time(3, 7));
    EXPECT_EQ(collect_time_ranges(in), (std::vector<Range>{{2, 2}, {8, 8}}));
    EXPECT_EQ(run_batch(in, kSixTimes),
              (std::vector<bool>{false, true, false, false, false, false}));

    TimeIn not_in({2, 8}, /*not_in=*/true);
    EXPECT_FALSE(not_in.satisfy(2, 0));
    EXPECT_TRUE(not_in.satisfy(5, 0));
    EXPECT_FALSE(not_in.satisfy(8, common::String("ignored")));
    EXPECT_EQ(run_batch(not_in, kSixTimes),
              (std::vector<bool>{true, false, true, true, true, true}));
}

TEST(TimeOperatorTest, TimeEqSemantics) {
    TimeEq eq(3);
    Statistic containing = make_stat(1, 5);
    Statistic excluding = make_stat(4, 6);
    EXPECT_TRUE(eq.satisfy(&containing));
    EXPECT_FALSE(eq.satisfy(&excluding));
    EXPECT_TRUE(eq.satisfy(3, 0));
    EXPECT_FALSE(eq.satisfy(4, 0));
    EXPECT_TRUE(eq.satisfy(3, common::String("ignored")));
    EXPECT_FALSE(eq.satisfy(2, common::String("ignored")));
    EXPECT_TRUE(eq.satisfy_start_end_time(2, 4));
    EXPECT_FALSE(eq.satisfy_start_end_time(4, 6));
    EXPECT_TRUE(eq.contain_start_end_time(3, 3));
    EXPECT_FALSE(eq.contain_start_end_time(2, 3));
    EXPECT_EQ(collect_time_ranges(eq), (std::vector<Range>{{3, 3}}));
    EXPECT_EQ(run_batch(eq, kSixTimes),
              (std::vector<bool>{false, false, true, false, false, false}));
}

TEST(TimeOperatorTest, TimeNotEqSemantics) {
    TimeNotEq neq(3);
    Statistic single = make_stat(3, 3);
    Statistic range = make_stat(3, 4);
    EXPECT_FALSE(neq.satisfy(&single));
    EXPECT_TRUE(neq.satisfy(&range));
    EXPECT_FALSE(neq.satisfy(3, 0));
    EXPECT_TRUE(neq.satisfy(4, 0));
    EXPECT_FALSE(neq.satisfy(3, common::String("ignored")));
    EXPECT_TRUE(neq.satisfy_start_end_time(1, 2));
    EXPECT_FALSE(neq.satisfy_start_end_time(3, 5));
    EXPECT_FALSE(neq.satisfy_start_end_time(1, 3));
    EXPECT_TRUE(neq.contain_start_end_time(4, 5));
    EXPECT_TRUE(neq.contain_start_end_time(1, 2));
    EXPECT_FALSE(neq.contain_start_end_time(1, 4));
    EXPECT_EQ(collect_time_ranges(neq),
              (std::vector<Range>{{std::numeric_limits<int64_t>::min(), 2},
                                  {4, std::numeric_limits<int64_t>::max()}}));
    EXPECT_EQ(run_batch(neq, kSixTimes),
              (std::vector<bool>{true, true, false, true, true, true}));

    TimeNotEq not_min(std::numeric_limits<int64_t>::min());
    EXPECT_EQ(collect_time_ranges(not_min),
              (std::vector<Range>{{std::numeric_limits<int64_t>::min() + 1,
                                   std::numeric_limits<int64_t>::max()}}));
    TimeNotEq not_max(std::numeric_limits<int64_t>::max());
    EXPECT_EQ(collect_time_ranges(not_max),
              (std::vector<Range>{{std::numeric_limits<int64_t>::min(),
                                   std::numeric_limits<int64_t>::max() - 1}}));
}

TEST(TimeOperatorTest, TimeGtSemantics) {
    TimeGt gt(2);
    EXPECT_TRUE(satisfy_stat(gt, 1, 3));
    EXPECT_FALSE(satisfy_stat(gt, 1, 2));
    EXPECT_TRUE(gt.satisfy(3, 0));
    EXPECT_FALSE(gt.satisfy(2, 0));
    EXPECT_TRUE(gt.satisfy(3, common::String("ignored")));
    EXPECT_FALSE(gt.satisfy(1, common::String("ignored")));
    EXPECT_TRUE(gt.satisfy_start_end_time(1, 3));
    EXPECT_FALSE(gt.satisfy_start_end_time(1, 2));
    EXPECT_TRUE(gt.contain_start_end_time(3, 5));
    EXPECT_FALSE(gt.contain_start_end_time(1, 5));
    EXPECT_EQ(collect_time_ranges(gt),
              (std::vector<Range>{{3, std::numeric_limits<int64_t>::max()}}));
    EXPECT_EQ(run_batch(gt, kSixTimes),
              (std::vector<bool>{false, false, true, true, true, true}));

    TimeGt gt_max(std::numeric_limits<int64_t>::max());
    EXPECT_TRUE(collect_time_ranges(gt_max).empty());
}

TEST(TimeOperatorTest, TimeGtEqSemantics) {
    TimeGtEq gt_eq(2);
    EXPECT_TRUE(satisfy_stat(gt_eq, 2, 3));
    EXPECT_TRUE(satisfy_stat(gt_eq, 1, 2));
    EXPECT_FALSE(satisfy_stat(gt_eq, 1, 1));
    EXPECT_TRUE(gt_eq.satisfy(2, 0));
    EXPECT_FALSE(gt_eq.satisfy(1, 0));
    EXPECT_TRUE(gt_eq.satisfy(2, common::String("ignored")));
    EXPECT_FALSE(gt_eq.satisfy(0, common::String("ignored")));
    EXPECT_TRUE(gt_eq.satisfy_start_end_time(1, 2));
    EXPECT_FALSE(gt_eq.satisfy_start_end_time(1, 1));
    EXPECT_TRUE(gt_eq.contain_start_end_time(2, 5));
    EXPECT_TRUE(gt_eq.contain_start_end_time(3, 5));
    EXPECT_FALSE(gt_eq.contain_start_end_time(1, 5));
    EXPECT_EQ(collect_time_ranges(gt_eq),
              (std::vector<Range>{{2, std::numeric_limits<int64_t>::max()}}));
    gt_eq.reset_value(4);
    EXPECT_EQ(run_batch(gt_eq, kSixTimes),
              (std::vector<bool>{false, false, false, true, true, true}));
}

TEST(TimeOperatorTest, TimeLtSemantics) {
    TimeLt lt(5);
    EXPECT_TRUE(satisfy_stat(lt, 4, 9));
    EXPECT_FALSE(satisfy_stat(lt, 5, 9));
    EXPECT_TRUE(lt.satisfy(4, 0));
    EXPECT_FALSE(lt.satisfy(5, 0));
    EXPECT_TRUE(lt.satisfy(4, common::String("ignored")));
    EXPECT_FALSE(lt.satisfy(6, common::String("ignored")));
    EXPECT_TRUE(lt.satisfy_start_end_time(4, 9));
    EXPECT_FALSE(lt.satisfy_start_end_time(5, 9));
    EXPECT_TRUE(lt.contain_start_end_time(0, 4));
    EXPECT_FALSE(lt.contain_start_end_time(0, 6));
    EXPECT_EQ(collect_time_ranges(lt),
              (std::vector<Range>{{std::numeric_limits<int64_t>::min(), 4}}));
    EXPECT_EQ(run_batch(lt, kSixTimes),
              (std::vector<bool>{true, true, true, true, false, false}));

    TimeLt lt_min(std::numeric_limits<int64_t>::min());
    EXPECT_TRUE(collect_time_ranges(lt_min).empty());
}

TEST(TimeOperatorTest, TimeLtEqSemantics) {
    TimeLtEq lt_eq(5);
    EXPECT_TRUE(satisfy_stat(lt_eq, 5, 9));
    EXPECT_FALSE(satisfy_stat(lt_eq, 6, 9));
    EXPECT_TRUE(lt_eq.satisfy(5, 0));
    EXPECT_FALSE(lt_eq.satisfy(6, 0));
    EXPECT_TRUE(lt_eq.satisfy(5, common::String("ignored")));
    EXPECT_FALSE(lt_eq.satisfy(7, common::String("ignored")));
    EXPECT_TRUE(lt_eq.satisfy_start_end_time(5, 9));
    EXPECT_FALSE(lt_eq.satisfy_start_end_time(6, 9));
    EXPECT_TRUE(lt_eq.contain_start_end_time(0, 4));
    EXPECT_TRUE(lt_eq.contain_start_end_time(0, 5));
    EXPECT_FALSE(lt_eq.contain_start_end_time(0, 6));
    EXPECT_EQ(collect_time_ranges(lt_eq),
              (std::vector<Range>{{std::numeric_limits<int64_t>::min(), 5}}));
    EXPECT_EQ(run_batch(lt_eq, kSixTimes),
              (std::vector<bool>{true, true, true, true, true, false}));
}

TEST(TimeOperatorTest, BatchHandlesVectorWidthAndTail) {
    TimeGt gt(3);
    const std::vector<int64_t> five_times = {1, 3, 4, 5, 9};
    EXPECT_EQ(run_batch(gt, five_times),
              (std::vector<bool>{false, false, true, true, true}));
    EXPECT_TRUE(run_batch(gt, {}).empty());

    TimeBetween between(2, 4, /*not_between=*/false);
    EXPECT_EQ(run_batch(between, five_times),
              (std::vector<bool>{false, true, true, false, false}));
}

}  // namespace
}  // namespace storage
