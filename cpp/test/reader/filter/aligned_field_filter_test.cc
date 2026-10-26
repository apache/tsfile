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

#include <cmath>
#include <limits>
#include <random>
#include <tuple>

#include "common/schema.h"
#include "common/statistic.h"
#include "reader/filter/selection_mask.h"

namespace storage {
namespace {

TEST(AlignedFieldFilterTest, MissingNotOperandsStayInvalidAfterRewriting) {
    using namespace common;
    using A = AlignedFieldFilter;
    TableSchema schema(
        "t", {{"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}});
    for (int shape = 0; shape < 5; ++shape) {
        A::Ptr filter;
        if (shape > 0) {
            auto left = shape % 2 == 0 ? A::Ptr() : A::is_null("v");
            auto right = shape % 2 == 0 ? A::is_null("v") : A::Ptr();
            filter = shape <= 2
                         ? A::and_filter(std::move(left), std::move(right))
                         : A::or_filter(std::move(left), std::move(right));
        }
        for (int depth = 1; depth <= 8; ++depth) {
            SCOPED_TRACE(::testing::Message() << shape << ":" << depth);
            filter = A::not_filter(std::move(filter));
            ASSERT_NE(filter, nullptr);
            EXPECT_FALSE(filter->is_valid());
            EXPECT_EQ(filter->bind(schema), E_INVALID_ARG);
            SelectionMask mask;
            EXPECT_EQ(filter->evaluate_columns({}, 0, mask), E_INVALID_ARG);
        }
    }
}

TEST(AlignedFieldFilterTest, NumericBindingMatchesJavaNumberConversions) {
    using namespace common;
    using A = AlignedFieldFilter;
    using L = A::Literal;
    using Case = std::tuple<TSDataType, L, L>;
    const double infinity = std::numeric_limits<double>::infinity();
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const float maximum = std::numeric_limits<float>::max();
    const double overflow = double(maximum) + std::ldexp(1.0, 103);
    // Golden results verified against Java ValueFilterApi.eq, including
    // narrowing, saturation, rounding, NaN, signed zero, and date aliases.
    const std::vector<Case> cases{
        {INT64, L::int32(10), L::int64(10)},
        {INT32, L::int64(4294967297LL), L::int32(1)},
        {INT32, L::int64(INT64_MAX), L::int32(-1)},
        {INT32, L::int64(INT64_MIN), L::int32(0)},
        {INT32, L::float64(1.9), L::int32(1)},
        {INT32, L::float32(-1.9F), L::int32(-1)},
        {INT32, L::float64(infinity), L::int32(INT32_MAX)},
        {INT32, L::float64(-infinity), L::int32(INT32_MIN)},
        {INT64, L::float64(nan), L::int64(0)},
        {INT64, L::float64(infinity), L::int64(INT64_MAX)},
        {INT64, L::float32(-infinity), L::int64(INT64_MIN)},
        {INT64, L::float64(std::ldexp(1.0, 63)), L::int64(INT64_MAX)},
        {INT64, L::float64(std::nextafter(std::ldexp(1.0, 63), 0.0)),
         L::int64(9223372036854774784LL)},
        {DATE, L::int64(4294967297LL), L::int32(1)},
        {TIMESTAMP, L::int32(10), L::int64(10)},
        {FLOAT, L::int64(16777217), L::float32(16777216)},
        {DOUBLE, L::int64(9007199254740993LL), L::float64(9007199254740992.0)},
        {FLOAT, L::float64(std::nextafter(double(maximum), infinity)),
         L::float32(maximum)},
        {FLOAT, L::float64(std::nextafter(overflow, 0.0)), L::float32(maximum)},
        {FLOAT, L::float64(overflow), L::float32(infinity)},
        {FLOAT, L::float64(std::nextafter(-overflow, 0.0)),
         L::float32(-maximum)},
        {FLOAT, L::float64(-overflow), L::float32(-infinity)},
        {FLOAT, L::float64(nan), L::float32(nan)},
        {FLOAT, L::float64(-0.0), L::float32(-0.0F)},
        {DOUBLE, L::float32(1.9F), L::float64(1.89999997615814208984375)}};
    for (const auto& item : cases) {
        const TSDataType type = std::get<0>(item);
        const L& input = std::get<1>(item);
        const L& expected = std::get<2>(item);
        SCOPED_TRACE(::testing::Message()
                     << int(type) << ":" << int(input.type));
        TableSchema schema(
            "t", {{"v", type, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}});
        auto filter = A::compare("v", A::CompareOp::EQ, input);
        ASSERT_EQ(filter->bind(schema), E_OK);
        const L& actual = filter->literal();
        ASSERT_EQ(actual.type, expected.type);
        if (actual.type == INT32) {
            EXPECT_EQ(actual.int32_value, expected.int32_value);
        } else if (actual.type == INT64) {
            EXPECT_EQ(actual.int64_value, expected.int64_value);
        } else if (actual.type == FLOAT) {
            if (std::isnan(expected.float_value)) {
                EXPECT_TRUE(std::isnan(actual.float_value));
            } else {
                EXPECT_EQ(actual.float_value, expected.float_value);
                EXPECT_EQ(std::signbit(actual.float_value),
                          std::signbit(expected.float_value));
            }
        } else {
            if (std::isnan(expected.double_value)) {
                EXPECT_TRUE(std::isnan(actual.double_value));
            } else {
                EXPECT_EQ(actual.double_value, expected.double_value);
                EXPECT_EQ(std::signbit(actual.double_value),
                          std::signbit(expected.double_value));
            }
        }
    }
}

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

TEST(SelectionMaskTest, SelectedRunsCrossWordBoundariesAndStopAtRowCount) {
    SelectionMask mask(137);
    for (uint32_t row = 3; row < 130; ++row) mask.set_one(row);
    mask.set_one(136);
    uint32_t begin = 0, end = 0;
    ASSERT_TRUE(mask.next_selected_run(0, begin, end));
    EXPECT_EQ(begin, 3U);
    EXPECT_EQ(end, 130U);
    ASSERT_TRUE(mask.next_selected_run(63, begin, end));
    EXPECT_EQ(begin, 63U);
    EXPECT_EQ(end, 130U);
    ASSERT_TRUE(mask.next_selected_run(end, begin, end));
    EXPECT_EQ(begin, 136U);
    EXPECT_EQ(end, 137U);
    EXPECT_FALSE(mask.next_selected_run(end, begin, end));
    mask.set_all();
    ASSERT_TRUE(mask.next_selected_run(0, begin, end));
    EXPECT_EQ(begin, 0U);
    EXPECT_EQ(end, 137U);
    ASSERT_TRUE(mask.next_selected_run(64, begin, end));
    EXPECT_EQ(begin, 64U);
    EXPECT_EQ(end, 137U);
    mask.invert();
    EXPECT_FALSE(mask.next_selected_run(0, begin, end));
    EXPECT_FALSE(SelectionMask(0).next_selected_run(0, begin, end));
}

TEST(SelectionMaskTest, SelectedRunsMatchScalarOracle) {
    std::mt19937 random(1026);
    for (uint32_t size : {1U, 63U, 64U, 65U, 127U, 130U, 257U, 1025U}) {
        for (int iteration = 0; iteration < 8; ++iteration) {
            SelectionMask mask(size);
            for (uint32_t row = 0; row < size; ++row) {
                if ((random() & 7U) < static_cast<uint32_t>(iteration))
                    mask.set_one(row);
            }
            for (uint32_t from = 0; from <= size; ++from) {
                uint32_t expected_begin = from;
                while (expected_begin < size && !mask.test(expected_begin))
                    ++expected_begin;
                uint32_t expected_end = expected_begin;
                while (expected_end < size && mask.test(expected_end))
                    ++expected_end;
                uint32_t begin = 0, end = 0;
                const bool found = mask.next_selected_run(from, begin, end);
                ASSERT_EQ(found, expected_begin < size);
                if (found) {
                    EXPECT_EQ(begin, expected_begin);
                    EXPECT_EQ(end, expected_end);
                }
            }
        }
    }
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

TEST(AlignedFieldFilterTest, NaNStatisticsCannotProvePageMatchesOrExclusion) {
    const auto check = [](Statistic& stat,
                          const AlignedFieldFilter::Literal& literal) {
        const std::map<std::string, Statistic*> stats{{"v", &stat}};
        for (auto op : {AlignedFieldFilter::CompareOp::EQ,
                        AlignedFieldFilter::CompareOp::NE,
                        AlignedFieldFilter::CompareOp::LT,
                        AlignedFieldFilter::CompareOp::LE,
                        AlignedFieldFilter::CompareOp::GT,
                        AlignedFieldFilter::CompareOp::GE}) {
            auto filter = AlignedFieldFilter::compare("v", op, literal);
            EXPECT_TRUE(filter->may_match(stats, 3));
            EXPECT_FALSE(filter->all_match(stats, 3));
            auto negated = AlignedFieldFilter::not_filter(
                AlignedFieldFilter::compare("v", op, literal));
            EXPECT_TRUE(negated->may_match(stats, 3));
            EXPECT_FALSE(negated->all_match(stats, 3));
        }
    };
    FloatStatistic floats;
    DoubleStatistic doubles;
    for (int r = 0; r < 3; ++r) {
        floats.update(r,
                      r == 1 ? std::numeric_limits<float>::quiet_NaN() : 1.0f);
        doubles.update(r,
                       r == 1 ? std::numeric_limits<double>::quiet_NaN() : 1.0);
    }
    check(floats, AlignedFieldFilter::Literal::float32(1));
    check(doubles, AlignedFieldFilter::Literal::float64(1));
}

TEST(AlignedFieldFilterTest, FiniteFloatingStatisticsRetainFastPaths) {
    FloatStatistic floats;
    DoubleStatistic doubles;
    for (int r = 0; r < 3; ++r) {
        floats.update(r, 1.0f);
        doubles.update(r, 1.0);
    }
    auto f =
        AlignedFieldFilter::compare("v", AlignedFieldFilter::CompareOp::EQ,
                                    AlignedFieldFilter::Literal::float32(1));
    auto d =
        AlignedFieldFilter::compare("v", AlignedFieldFilter::CompareOp::EQ,
                                    AlignedFieldFilter::Literal::float64(1));
    EXPECT_TRUE(f->all_match({{"v", &floats}}, 3));
    EXPECT_TRUE(d->all_match({{"v", &doubles}}, 3));
    auto below =
        AlignedFieldFilter::compare("v", AlignedFieldFilter::CompareOp::LT,
                                    AlignedFieldFilter::Literal::float32(0));
    EXPECT_FALSE(below->may_match({{"v", &floats}}, 3));
}

TEST(AlignedFieldFilterTest, NegatedFloatingStatisticsPreserveSafeShortcuts) {
    using A = AlignedFieldFilter;
    FloatStatistic floats;
    DoubleStatistic doubles;
    for (int value : {10, 15, 20}) {
        floats.update(value, static_cast<float>(value));
        doubles.update(value, static_cast<double>(value));
    }
    for (bool is_float : {true, false}) {
        const std::map<std::string, Statistic*> stats{
            {"v", is_float ? static_cast<Statistic*>(&floats)
                           : static_cast<Statistic*>(&doubles)}};
        for (auto op : {A::CompareOp::LT, A::CompareOp::LE, A::CompareOp::GT,
                        A::CompareOp::GE}) {
            for (double threshold :
                 {-std::numeric_limits<double>::infinity(), 0.0, 10.0, 15.0,
                  20.0, 30.0, std::numeric_limits<double>::infinity()}) {
                SCOPED_TRACE(::testing::Message()
                             << is_float << ":" << int(op) << ":" << threshold);
                int matches = 0;
                for (double value : {10.0, 15.0, 20.0}) {
                    bool comparison = false;
                    switch (op) {
                        case A::CompareOp::LT:
                            comparison = value < threshold;
                            break;
                        case A::CompareOp::LE:
                            comparison = value <= threshold;
                            break;
                        case A::CompareOp::GT:
                            comparison = value > threshold;
                            break;
                        case A::CompareOp::GE:
                            comparison = value >= threshold;
                            break;
                        default:
                            FAIL() << "unexpected ordered comparison";
                    }
                    matches += !comparison;
                }
                const auto literal =
                    is_float
                        ? A::Literal::float32(static_cast<float>(threshold))
                        : A::Literal::float64(threshold);
                auto filter = A::not_filter(A::compare("v", op, literal));
                EXPECT_EQ(filter->may_match(stats, 3), matches != 0);
                EXPECT_EQ(filter->all_match(stats, 3), matches == 3);
                // The same non-NULL statistics can describe a page with
                // another NULL row, which must never match a comparison.
                EXPECT_EQ(filter->may_match(stats, 4), matches != 0);
                EXPECT_FALSE(filter->all_match(stats, 4));
                auto original = A::compare("v", op, literal);
                auto twice =
                    A::not_filter(A::not_filter(A::compare("v", op, literal)));
                EXPECT_EQ(twice->may_match(stats, 3),
                          original->may_match(stats, 3));
                EXPECT_EQ(twice->all_match(stats, 3),
                          original->all_match(stats, 3));
            }
            const auto nan =
                is_float ? A::Literal::float32(
                               std::numeric_limits<float>::quiet_NaN())
                         : A::Literal::float64(
                               std::numeric_limits<double>::quiet_NaN());
            auto filter = A::not_filter(A::compare("v", op, nan));
            EXPECT_TRUE(filter->may_match(stats, 3));
            EXPECT_FALSE(filter->all_match(stats, 3));
        }
        auto mismatched = A::not_filter(
            A::compare("v", A::CompareOp::LT, A::Literal::int32(15)));
        EXPECT_TRUE(mismatched->may_match(stats, 3));
        EXPECT_FALSE(mismatched->all_match(stats, 3));
    }
}

}  // namespace
}  // namespace storage
