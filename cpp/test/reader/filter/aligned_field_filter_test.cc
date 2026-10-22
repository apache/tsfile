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

#include "reader/filter/aligned_field_filter.h"

#include <gtest/gtest.h>

#include "common/statistic.h"
#include "reader/filter/selection_mask.h"

namespace storage {
namespace {

TEST(SelectionMaskTest, BooleanOperationsAndTrailingBits) {
    SelectionMask left(70);
    SelectionMask right(70);
    for (uint32_t row : {0U, 2U, 63U, 64U, 69U}) left.set_one(row);
    for (uint32_t row : {1U, 2U, 64U}) right.set_one(row);

    left.and_with(right);
    EXPECT_EQ(left.count_ones(), 2U);
    EXPECT_TRUE(left.test(2));
    EXPECT_TRUE(left.test(64));

    left.or_with(right);
    EXPECT_EQ(left.count_ones(), 3U);
    left.invert();
    EXPECT_EQ(left.count_ones(), 67U);
    EXPECT_FALSE(left.test(2));
    EXPECT_TRUE(left.test(69));
}

TEST(SelectionMaskTest, NextSetBitAndClearFrom) {
    SelectionMask mask(130);
    for (uint32_t row : {0U, 63U, 64U, 129U}) mask.set_one(row);
    uint32_t value = 0;
    EXPECT_TRUE(mask.next_set_bit(0, value));
    EXPECT_EQ(value, 0U);
    EXPECT_TRUE(mask.next_set_bit(1, value));
    EXPECT_EQ(value, 63U);
    EXPECT_TRUE(mask.next_set_bit(64, value));
    EXPECT_EQ(value, 64U);
    EXPECT_TRUE(mask.next_set_bit(65, value));
    EXPECT_EQ(value, 129U);
    mask.clear_from(64);
    EXPECT_EQ(mask.count_ones(), 2U);
    EXPECT_FALSE(mask.test(64));
    EXPECT_FALSE(mask.test(129));
}

TEST(AlignedFieldFilterTest, PageStatisticsAreConservative) {
    Int64Statistic statistic;
    statistic.count_ = 5;
    statistic.min_value_ = 10;
    statistic.max_value_ = 20;
    const std::map<std::string, Statistic*> stats{{"a", &statistic}};

    auto below =
        AlignedFieldFilter::compare("a", AlignedFieldFilter::CompareOp::LT,
                                    AlignedFieldFilter::Literal::int64(5));
    EXPECT_FALSE(below->may_match(stats, 5));

    auto overlap =
        AlignedFieldFilter::compare("a", AlignedFieldFilter::CompareOp::LE,
                                    AlignedFieldFilter::Literal::int64(10));
    EXPECT_TRUE(overlap->may_match(stats, 5));

    auto is_null = AlignedFieldFilter::is_null("a");
    EXPECT_FALSE(is_null->may_match(stats, 5));
    statistic.count_ = 4;
    EXPECT_TRUE(is_null->may_match(stats, 5));

    auto missing = AlignedFieldFilter::compare(
        "missing", AlignedFieldFilter::CompareOp::EQ,
        AlignedFieldFilter::Literal::int64(1));
    EXPECT_TRUE(missing->may_match(stats, 5));
}

TEST(AlignedFieldFilterTest, PageStatisticsAllMatchIsConservative) {
    Int64Statistic statistic;
    statistic.count_ = 5;
    statistic.min_value_ = 10;
    statistic.max_value_ = 20;
    const std::map<std::string, Statistic*> stats{{"a", &statistic}};

    auto ge =
        AlignedFieldFilter::compare("a", AlignedFieldFilter::CompareOp::GE,
                                    AlignedFieldFilter::Literal::int64(10));
    EXPECT_TRUE(ge->all_match(stats, 5));

    auto lt =
        AlignedFieldFilter::compare("a", AlignedFieldFilter::CompareOp::LT,
                                    AlignedFieldFilter::Literal::int64(21));
    EXPECT_TRUE(lt->all_match(stats, 5));

    auto strict_lt =
        AlignedFieldFilter::compare("a", AlignedFieldFilter::CompareOp::LT,
                                    AlignedFieldFilter::Literal::int64(20));
    EXPECT_FALSE(strict_lt->all_match(stats, 5));

    auto not_null = AlignedFieldFilter::is_not_null("a");
    EXPECT_TRUE(not_null->all_match(stats, 5));
    statistic.count_ = 4;
    EXPECT_FALSE(not_null->all_match(stats, 5));
    auto is_null = AlignedFieldFilter::is_null("a");
    statistic.count_ = 0;
    EXPECT_TRUE(is_null->all_match(stats, 5));

    statistic.count_ = 5;
    auto combined = AlignedFieldFilter::and_filter(
        AlignedFieldFilter::compare("a", AlignedFieldFilter::CompareOp::GE,
                                    AlignedFieldFilter::Literal::int64(10)),
        AlignedFieldFilter::is_not_null("a"));
    EXPECT_TRUE(combined->all_match(stats, 5));
}

TEST(AlignedFieldFilterTest, CollectsReferencedColumnsOnce) {
    auto left =
        AlignedFieldFilter::compare("a", AlignedFieldFilter::CompareOp::EQ,
                                    AlignedFieldFilter::Literal::int32(1));
    auto right = AlignedFieldFilter::or_filter(
        AlignedFieldFilter::is_null("b"),
        AlignedFieldFilter::compare("a", AlignedFieldFilter::CompareOp::GT,
                                    AlignedFieldFilter::Literal::int32(3)));
    auto root =
        AlignedFieldFilter::and_filter(std::move(left), std::move(right));
    EXPECT_EQ(root->referenced_columns(), (std::vector<std::string>{"a", "b"}));
}

}  // namespace
}  // namespace storage
