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

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <cstdio>
#include <string>
#include <vector>

#include "common/global.h"
#include "common/record.h"
#include "common/schema.h"
#include "reader/expression.h"
#include "reader/filter/time_filter.h"
#include "reader/filter/time_operator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace storage {
namespace {

using namespace common;

std::string unique_global_filter_file() {
    static std::atomic<uint64_t> counter{0};
#ifdef _WIN32
    const auto pid = static_cast<uint64_t>(_getpid());
#else
    const auto pid = static_cast<uint64_t>(getpid());
#endif
    return "global_time_filter_" + std::to_string(pid) + "_" +
           std::to_string(counter.fetch_add(1)) + ".tsfile";
}

Path series_path(const std::string& device, const std::string& measurement) {
    return Path(device + "." + measurement);
}

Expression* series_expr(const std::string& device,
                        const std::string& measurement, Filter* filter) {
    return new Expression(SERIES_EXPR, series_path(device, measurement),
                          filter);
}

Expression* global_expr(Filter* filter) {
    return new Expression(GLOBALTIME_EXPR, filter);
}

std::vector<int64_t> collect_timestamps(ResultSet* result) {
    std::vector<int64_t> timestamps;
    bool has_next = false;
    while (true) {
        EXPECT_EQ(result->next(has_next), E_OK);
        if (!has_next) {
            break;
        }
        timestamps.push_back(result->get_row_record()->get_timestamp());
    }
    return timestamps;
}

class GlobalTimeFilterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(libtsfile_init(), E_OK);
        file_ = unique_global_filter_file();
        std::remove(file_.c_str());
        write_file();
        ASSERT_EQ(reader_.open(file_), E_OK);
    }

    void TearDown() override {
        reader_.close();
        std::remove(file_.c_str());
        libtsfile_destroy();
    }

    // s1: [1, 8] -> 10 * t; s2: {2, 4, 6} -> 100 * t.
    void write_file() {
        TsFileWriter writer;
        ASSERT_EQ(writer.open(file_), E_OK);
        MeasurementSchema s1_schema("s1", INT64);
        MeasurementSchema s2_schema("s2", INT64);
        ASSERT_EQ(writer.register_timeseries(device_, s1_schema), E_OK);
        ASSERT_EQ(writer.register_timeseries(device_, s2_schema), E_OK);
        for (int64_t t = 1; t <= 8; ++t) {
            TsRecord record(t, device_);
            record.add_point("s1", static_cast<int64_t>(10 * t));
            ASSERT_EQ(writer.write_record(record), E_OK);
        }
        for (int64_t t : {2, 4, 6}) {
            TsRecord record(t, device_);
            record.add_point("s2", static_cast<int64_t>(100 * t));
            ASSERT_EQ(writer.write_record(record), E_OK);
        }
        ASSERT_EQ(writer.flush(), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
    }

    ResultSet* query(Expression* expression,
                     const std::vector<std::string>& measurements) {
        std::vector<Path> paths;
        for (const auto& measurement : measurements) {
            paths.push_back(series_path(device_, measurement));
        }
        ResultSet* result = nullptr;
        EXPECT_EQ(
            reader_.query(QueryExpression::create(paths, expression), result),
            E_OK);
        return result;
    }

    const std::string device_ = "root.global_time_filter";
    std::string file_;
    TsFileReader reader_;
};

TEST_F(GlobalTimeFilterTest, OrGlobalTimeWithSeriesFilter) {
    Expression* expression =
        new Expression(OR_EXPR, global_expr(new TimeBetween(3, 5, false)),
                       series_expr(device_, "s1", new TimeGt(6)));
    ResultSet* result = query(expression, {"s1"});
    ASSERT_NE(result, nullptr);

    EXPECT_EQ(collect_timestamps(result),
              (std::vector<int64_t>{3, 4, 5, 7, 8}));
    reader_.destroy_query_data_set(result);
}

TEST_F(GlobalTimeFilterTest, AndGlobalTimeWithSeriesFilter) {
    Expression* expression =
        new Expression(AND_EXPR, global_expr(new TimeBetween(3, 6, false)),
                       series_expr(device_, "s1", new TimeGt(4)));
    ResultSet* result = query(expression, {"s1"});
    ASSERT_NE(result, nullptr);

    EXPECT_EQ(collect_timestamps(result), (std::vector<int64_t>{5, 6}));
    reader_.destroy_query_data_set(result);
}

TEST_F(GlobalTimeFilterTest, OrOfTwoGlobalFilters) {
    Expression* expression =
        new Expression(OR_EXPR, global_expr(new TimeBetween(1, 2, false)),
                       global_expr(new TimeGt(7)));
    ResultSet* result = query(expression, {"s1"});
    ASSERT_NE(result, nullptr);

    EXPECT_EQ(collect_timestamps(result), (std::vector<int64_t>{1, 2, 8}));
    reader_.destroy_query_data_set(result);
}

TEST_F(GlobalTimeFilterTest, AndOfTwoGlobalFilters) {
    Expression* expression =
        new Expression(AND_EXPR, global_expr(new TimeBetween(2, 6, false)),
                       global_expr(new TimeGt(4)));
    ResultSet* result = query(expression, {"s1"});
    ASSERT_NE(result, nullptr);

    EXPECT_EQ(collect_timestamps(result), (std::vector<int64_t>{5, 6}));
    reader_.destroy_query_data_set(result);
}

TEST_F(GlobalTimeFilterTest, OrGlobalTimeWithMultiSeriesExpression) {
    Expression* series_tree =
        new Expression(OR_EXPR, series_expr(device_, "s1", new TimeGt(7)),
                       series_expr(device_, "s2", new TimeEq(6)));
    Expression* expression = new Expression(
        OR_EXPR, global_expr(new TimeBetween(2, 3, false)), series_tree);
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    // s1: {2, 3} from the global filter and {8} from its own filter;
    // s2: {2} from the global filter and {6} from its own filter.
    EXPECT_EQ(collect_timestamps(result), (std::vector<int64_t>{2, 3, 6, 8}));
    reader_.destroy_query_data_set(result);
}

TEST_F(GlobalTimeFilterTest, AndGlobalTimeWithAndOfTwoSeries) {
    Expression* series_tree =
        new Expression(AND_EXPR, series_expr(device_, "s1", new TimeGtEq(3)),
                       series_expr(device_, "s2", new TimeGtEq(0)));
    Expression* expression = new Expression(
        AND_EXPR, global_expr(new TimeBetween(2, 7, false)), series_tree);
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    EXPECT_EQ(collect_timestamps(result), (std::vector<int64_t>{4, 6}));
    reader_.destroy_query_data_set(result);
}

TEST_F(GlobalTimeFilterTest, AndGlobalTimeWithEmptySeriesBranch) {
    Expression* series_tree =
        new Expression(OR_EXPR, series_expr(device_, "s1", new TimeEq(1)),
                       series_expr(device_, "s2", new TimeEq(6)));
    Expression* expression = new Expression(
        AND_EXPR, global_expr(new TimeBetween(2, 5, false)), series_tree);
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    EXPECT_TRUE(collect_timestamps(result).empty());
    reader_.destroy_query_data_set(result);
}

}  // namespace
}  // namespace storage
