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

#include "reader/expression.h"

#include <gtest/gtest.h>

#include <vector>

#include "reader/filter/time_operator.h"

namespace storage {

namespace {

// A filter without clone(): pushing it onto several owners must fail instead
// of sharing a single instance (and double-freeing it later).
class StatelessFilter : public Filter {
   public:
    bool satisfy(Statistic*) override { return true; }
    bool satisfy(int64_t, int64_t) override { return true; }
};

}  // namespace

TEST(QueryExpressionTest, OptimizeFailsWhenAndCannotCloneTimeFilter) {
    QueryExpression query_expression;
    std::vector<Path> paths{Path("root.sg.d1.s1")};
    Expression* expression = new Expression(
        AND_EXPR, new Expression(GLOBALTIME_EXPR, new StatelessFilter()),
        new Expression(SERIES_EXPR, paths.at(0), new TimeGtEq(0)));

    EXPECT_EQ(query_expression.optimize(expression, paths), nullptr);
}

TEST(QueryExpressionTest, OptimizeFailsWhenOrCannotCloneTimeFilter) {
    QueryExpression query_expression;
    std::vector<Path> paths{Path("root.sg.d1.s1")};
    Expression* expression = new Expression(
        OR_EXPR, new Expression(GLOBALTIME_EXPR, new StatelessFilter()),
        new Expression(SERIES_EXPR, paths.at(0), new TimeGtEq(0)));

    EXPECT_EQ(query_expression.optimize(expression, paths), nullptr);
}

TEST(QueryExpressionTest, EmptySelectedSeriesReturnsNull) {
    QueryExpression query_expression;
    std::vector<Path> selected_series;
    // optimize() consumes the input tree, including on failure: the empty
    // selected-series list makes the global time filter impossible to push.
    Expression* expression = new Expression(
        OR_EXPR, new Expression(GLOBALTIME_EXPR, static_cast<Filter*>(nullptr)),
        new Expression(SERIES_EXPR, Path(), static_cast<Filter*>(nullptr)));

    EXPECT_EQ(query_expression.optimize(expression, selected_series), nullptr);
}

}  // namespace storage
