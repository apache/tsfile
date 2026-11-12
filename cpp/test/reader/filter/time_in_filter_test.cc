/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#include "reader/filter/time_operator.h"

using namespace storage;

// Regression: TimeIn::satisfy_start_end_time / contain_start_end_time used to
// return true unconditionally.  In the aligned batch/multi paths the
// contain_start_end_time=true branch flips block_all_pass on, the per-row
// satisfy_batch_time check is skipped, and the reader emits every row in the
// block — making `WHERE time IN (2, 8)` look identical to "no time filter"
// whenever the block's time range overlapped the IN list at all.

TEST(TimeInFilterTest, ContainStartEndTimeIsFalseForSparseRange) {
    TimeIn in({2, 8}, /*not_in=*/false);
    // Range [0,10] contains many times not in {2,8}; the block cannot
    // unconditionally pass.
    EXPECT_FALSE(in.contain_start_end_time(0, 10));
    // Range that is a single matching point passes.
    EXPECT_TRUE(in.contain_start_end_time(2, 2));
    // Single non-matching point: doesn't pass.
    EXPECT_FALSE(in.contain_start_end_time(5, 5));
}

TEST(TimeInFilterTest, SatisfyStartEndTimeTracksOverlap) {
    TimeIn in({2, 8}, /*not_in=*/false);
    // Some value in range → block may have matching rows.
    EXPECT_TRUE(in.satisfy_start_end_time(0, 10));
    EXPECT_TRUE(in.satisfy_start_end_time(2, 2));
    EXPECT_TRUE(in.satisfy_start_end_time(8, 8));
    // No value in range → block can be skipped.
    EXPECT_FALSE(in.satisfy_start_end_time(3, 7));
    EXPECT_FALSE(in.satisfy_start_end_time(9, 100));
}

TEST(TimeInFilterTest, NotInContainSemantics) {
    TimeIn not_in({2, 8}, /*not_in=*/true);
    // Range [3,7] has no excluded value → every row passes NOT IN.
    EXPECT_TRUE(not_in.contain_start_end_time(3, 7));
    // Range [0,10] includes 2 and 8 → cannot blanket-pass.
    EXPECT_FALSE(not_in.contain_start_end_time(0, 10));
}

TEST(TimeInFilterTest, NotInSatisfyStartEndTimeSemantics) {
    TimeIn not_in({2, 8}, /*not_in=*/true);
    // Single excluded point: filter rejects it.
    EXPECT_FALSE(not_in.satisfy_start_end_time(2, 2));
    // Single non-excluded point: filter accepts it.
    EXPECT_TRUE(not_in.satisfy_start_end_time(5, 5));
    // A wider range always has at least one non-excluded time.
    EXPECT_TRUE(not_in.satisfy_start_end_time(0, 10));
}

TEST(TimeInFilterTest, BatchTimeFallbackUsesScalarSemantics) {
    TimeIn in({2, 8}, /*not_in=*/false);
    int64_t times[] = {1, 2, 3, 7, 8, 9};
    bool mask[6];
    int pass = in.satisfy_batch_time(times, 6, mask);
    EXPECT_EQ(pass, 2);
    EXPECT_FALSE(mask[0]);
    EXPECT_TRUE(mask[1]);
    EXPECT_FALSE(mask[2]);
    EXPECT_FALSE(mask[3]);
    EXPECT_TRUE(mask[4]);
    EXPECT_FALSE(mask[5]);
}

// The aligned chunk reader uses this predicate to skip the per-row time check
// and to take the bulk path; it must only be true for a plain TimeBetween that
// accepts every int64 value.
TEST(TimeBetweenFilterTest, IsUnboundedTimeFilterOnlyForFullRangeBetween) {
    const int64_t min_time = std::numeric_limits<int64_t>::min();
    const int64_t max_time = std::numeric_limits<int64_t>::max();

    TimeBetween full(min_time, max_time, /*not_between=*/false);
    EXPECT_TRUE(full.is_unbounded_time_filter());

    // Negated full range accepts nothing, so it must not take the shortcut.
    TimeBetween negated(min_time, max_time, /*not_between=*/true);
    EXPECT_FALSE(negated.is_unbounded_time_filter());

    // Inverted bounds are not "everything".
    TimeBetween inverted(max_time, min_time, /*not_between=*/false);
    EXPECT_FALSE(inverted.is_unbounded_time_filter());

    // One missing bound is a real restriction.
    TimeBetween half_open(min_time + 1, max_time, /*not_between=*/false);
    EXPECT_FALSE(half_open.is_unbounded_time_filter());
    TimeBetween narrow(0, 100, /*not_between=*/false);
    EXPECT_FALSE(narrow.is_unbounded_time_filter());

    // Other filter types never claim to be unbounded.
    TimeIn time_in({1, 2}, /*not_in=*/false);
    EXPECT_FALSE(time_in.is_unbounded_time_filter());
}
