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

#include "reader/qds_with_timegenerator.h"

#include <gtest/gtest.h>

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

#if defined(_WIN32)
#include <process.h>
#define QDS_GETPID _getpid
#else
#include <unistd.h>
#define QDS_GETPID getpid
#endif

namespace storage {
namespace {

using namespace common;

std::string unique_file_name(const char* tag) {
    static std::atomic<uint64_t> counter{0};
    return std::string(tag) + "_" + std::to_string(QDS_GETPID()) + "_" +
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

struct RowSnapshot {
    int64_t timestamp = 0;
    bool value1_is_null = false;
    int64_t value1 = 0;
    bool value2_is_null = false;
    int64_t value2 = 0;
};

// Collects the value columns present in the result (2 and 3 overall when both
// series are selected).
void collect_rows(ResultSet* result, std::vector<RowSnapshot>& rows) {
    const uint32_t column_count = result->get_metadata()->get_column_count();
    bool has_next = false;
    while (true) {
        ASSERT_EQ(result->next(has_next), E_OK);
        if (!has_next) {
            break;
        }
        RowRecord* record = result->get_row_record();
        ASSERT_NE(record, nullptr);
        RowSnapshot row;
        row.timestamp = record->get_timestamp();
        Field* first = record->get_field(1);
        ASSERT_NE(first, nullptr);
        row.value1_is_null = first->type_ == NULL_TYPE;
        if (!row.value1_is_null) {
            ASSERT_EQ(first->type_, INT64);
            row.value1 = first->value_.lval_;
        }
        if (column_count > 2) {
            Field* second = record->get_field(2);
            ASSERT_NE(second, nullptr);
            row.value2_is_null = second->type_ == NULL_TYPE;
            if (!row.value2_is_null) {
                ASSERT_EQ(second->type_, INT64);
                row.value2 = second->value_.lval_;
            }
        }
        rows.push_back(row);
    }
}

std::vector<int64_t> timestamps(const std::vector<RowSnapshot>& rows) {
    std::vector<int64_t> result;
    result.reserve(rows.size());
    for (const auto& row : rows) {
        result.push_back(row.timestamp);
    }
    return result;
}

class QDSWithTimeGeneratorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(libtsfile_init(), E_OK);
        file_ = unique_file_name("qds_with_timegenerator_test");
        std::remove(file_.c_str());
        write_file();
        ASSERT_EQ(reader_.open(file_), E_OK);
    }

    void TearDown() override {
        reader_.close();
        std::remove(file_.c_str());
        libtsfile_destroy();
    }

    // s1: [1, 8] -> 10 * t
    // s2: {2, 4, 6} -> 100 * t
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

    const std::string device_ = "root.qds_with_timegenerator";
    std::string file_;
    TsFileReader reader_;
};

TEST_F(QDSWithTimeGeneratorTest, SingleSeriesWithTimeFilter) {
    ResultSet* result =
        query(series_expr(device_, "s1", TimeFilter::gt_eq(3)), {"s1"});
    ASSERT_NE(result, nullptr);

    std::shared_ptr<ResultSetMetadata> metadata = result->get_metadata();
    ASSERT_NE(metadata, nullptr);
    EXPECT_EQ(metadata->get_column_count(), 2U);
    EXPECT_EQ(metadata->get_column_name(1), "time");
    EXPECT_EQ(metadata->get_column_name(2), device_ + ".s1");
    EXPECT_EQ(metadata->get_column_type(2), INT64);

    std::vector<RowSnapshot> rows;
    bool has_next = false;
    while (true) {
        ASSERT_EQ(result->next(has_next), E_OK);
        if (!has_next) {
            break;
        }
        EXPECT_FALSE(result->is_null("s1"));
        EXPECT_FALSE(result->is_null(2));
        EXPECT_FALSE(result->is_null(1));
        RowRecord* record = result->get_row_record();
        rows.push_back({record->get_timestamp(), false,
                        record->get_field(1)->value_.lval_, true, 0});
    }
    ASSERT_EQ(rows.size(), 6U);
    for (size_t i = 0; i < rows.size(); ++i) {
        const int64_t expected_time = static_cast<int64_t>(i) + 3;
        EXPECT_EQ(rows[i].timestamp, expected_time);
        EXPECT_EQ(rows[i].value1, 10 * expected_time);
    }
    EXPECT_TRUE(result->is_null("unknown_column"));
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, AndOfTwoSeriesIntersectsTimestamps) {
    Expression* expression = new Expression(
        AND_EXPR, series_expr(device_, "s1", TimeFilter::gt_eq(2)),
        series_expr(device_, "s2", TimeFilter::lt_eq(5)));
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    std::vector<RowSnapshot> rows;
    collect_rows(result, rows);
    ASSERT_EQ(timestamps(rows), (std::vector<int64_t>{2, 4}));
    EXPECT_EQ(rows[0].value1, 20);
    EXPECT_EQ(rows[0].value2, 200);
    EXPECT_EQ(rows[1].value1, 40);
    EXPECT_EQ(rows[1].value2, 400);
    EXPECT_FALSE(result->is_null("s1"));
    EXPECT_FALSE(result->is_null("s2"));
    EXPECT_FALSE(result->is_null(3));
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, OrOfTwoSeriesUnionsAndMarksMissingNull) {
    Expression* expression =
        new Expression(OR_EXPR, series_expr(device_, "s1", TimeFilter::eq(3)),
                       series_expr(device_, "s2", TimeFilter::eq(4)));
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    std::vector<RowSnapshot> rows;
    collect_rows(result, rows);
    ASSERT_EQ(timestamps(rows), (std::vector<int64_t>{3, 4}));
    EXPECT_EQ(rows[0].value1, 30);
    EXPECT_TRUE(rows[0].value2_is_null);
    EXPECT_EQ(rows[1].value1, 40);
    EXPECT_EQ(rows[1].value2, 400);
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, AndNodeAdvancesTheRightSeriesFirst) {
    // The left stream starts after the right one, so the AND cursor must
    // advance the right series before the streams can meet.
    Expression* expression = new Expression(
        AND_EXPR, series_expr(device_, "s1", TimeFilter::gt_eq(4)),
        series_expr(device_, "s2", TimeFilter::gt_eq(0)));
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    std::vector<RowSnapshot> rows;
    collect_rows(result, rows);
    ASSERT_EQ(timestamps(rows), (std::vector<int64_t>{4, 6}));
    EXPECT_EQ(rows[0].value1, 40);
    EXPECT_EQ(rows[0].value2, 400);
    EXPECT_EQ(rows[1].value1, 60);
    EXPECT_EQ(rows[1].value2, 600);
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, OrNodeAdvancesLeftSeriesFirst) {
    Expression* expression = new Expression(
        OR_EXPR, series_expr(device_, "s1", TimeFilter::gt_eq(1)),
        series_expr(device_, "s2", TimeFilter::eq(4)));
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    std::vector<RowSnapshot> rows;
    collect_rows(result, rows);
    ASSERT_EQ(timestamps(rows), (std::vector<int64_t>{1, 2, 3, 4, 5, 6, 7, 8}));
    EXPECT_EQ(rows[0].value1, 10);
    EXPECT_TRUE(rows[0].value2_is_null);
    EXPECT_EQ(rows[1].value2, 200);
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, OrNodeAdvancesRightSeriesFirst) {
    Expression* expression =
        new Expression(OR_EXPR, series_expr(device_, "s1", TimeFilter::eq(4)),
                       series_expr(device_, "s2", TimeFilter::eq(2)));
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    std::vector<RowSnapshot> rows;
    collect_rows(result, rows);
    ASSERT_EQ(timestamps(rows), (std::vector<int64_t>{2, 4}));
    EXPECT_EQ(rows[0].value1, 20);
    EXPECT_EQ(rows[0].value2, 200);
    EXPECT_EQ(rows[1].value1, 40);
    EXPECT_EQ(rows[1].value2, 400);
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, ValueAtStopsAfterSeriesIsExhausted) {
    // s1 keeps producing timestamps past s2's last value, so the value lookup
    // for s2 runs past the end of its scan and then short-circuits.
    Expression* expression = new Expression(
        OR_EXPR, series_expr(device_, "s1", TimeFilter::gt_eq(7)),
        series_expr(device_, "s2", TimeFilter::eq(2)));
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    std::vector<RowSnapshot> rows;
    collect_rows(result, rows);
    ASSERT_EQ(timestamps(rows), (std::vector<int64_t>{2, 7, 8}));
    EXPECT_EQ(rows[0].value1, 20);
    EXPECT_EQ(rows[0].value2, 200);
    EXPECT_EQ(rows[1].value1, 70);
    EXPECT_TRUE(rows[1].value2_is_null);
    EXPECT_EQ(rows[2].value1, 80);
    EXPECT_TRUE(rows[2].value2_is_null);
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, ValueAtBeginsAfterSeriesRange) {
    // s2 is selected but does not appear in the expression: its value lookup
    // starts at a timestamp past its last value, so the first scan attempt is
    // already exhausted.
    Expression* expression = series_expr(device_, "s1", TimeFilter::gt_eq(7));
    ResultSet* result = query(expression, {"s1", "s2"});
    ASSERT_NE(result, nullptr);

    std::vector<RowSnapshot> rows;
    collect_rows(result, rows);
    ASSERT_EQ(timestamps(rows), (std::vector<int64_t>{7, 8}));
    EXPECT_EQ(rows[0].value1, 70);
    EXPECT_TRUE(rows[0].value2_is_null);
    EXPECT_EQ(rows[1].value1, 80);
    EXPECT_TRUE(rows[1].value2_is_null);
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, StatisticsPrunedFilterReturnsEmptyResult) {
    ResultSet* result =
        query(series_expr(device_, "s1", TimeFilter::gt(1000)), {"s1"});
    ASSERT_NE(result, nullptr);
    bool has_next = true;
    ASSERT_EQ(result->next(has_next), E_OK);
    EXPECT_FALSE(has_next);
    EXPECT_EQ(result->get_metadata()->get_column_count(), 2U);
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, RowFilteredSeriesReturnsEmptyResult) {
    // 5 lies inside s2's [2, 6] statistics range, so chunk pruning keeps the
    // series but no row satisfies the filter.
    ResultSet* result =
        query(series_expr(device_, "s2", TimeFilter::eq(5)), {"s2"});
    ASSERT_NE(result, nullptr);
    bool has_next = true;
    ASSERT_EQ(result->next(has_next), E_OK);
    EXPECT_FALSE(has_next);
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorTest, MissingSeriesFailsTheQuery) {
    ResultSet* result = nullptr;
    std::vector<Path> paths{series_path(device_, "missing")};
    EXPECT_NE(reader_.query(QueryExpression::create(
                                paths, series_expr(device_, "missing",
                                                   TimeFilter::gt_eq(0))),
                            result),
              E_OK);
    EXPECT_EQ(result, nullptr);
}

class QDSWithTimeGeneratorMultiBlockTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_EQ(libtsfile_init(), E_OK);
        saved_block_memory_ = g_config_value_.tsblock_max_memory_;
        g_config_value_.tsblock_max_memory_ = 2048;
        file_ = unique_file_name("qds_with_timegenerator_blocks_test");
        std::remove(file_.c_str());
        write_file();
        ASSERT_EQ(reader_.open(file_), E_OK);
    }

    void TearDown() override {
        reader_.close();
        std::remove(file_.c_str());
        g_config_value_.tsblock_max_memory_ = saved_block_memory_;
        libtsfile_destroy();
    }

    // 300 rows per series guarantee several TsBlocks per series at 2 KiB.
    void write_file() {
        TsFileWriter writer;
        ASSERT_EQ(writer.open(file_), E_OK);
        MeasurementSchema s1_schema("s1", INT64);
        MeasurementSchema s2_schema("s2", INT64);
        ASSERT_EQ(writer.register_timeseries(device_, s1_schema), E_OK);
        ASSERT_EQ(writer.register_timeseries(device_, s2_schema), E_OK);
        for (int64_t t = 1; t <= 300; ++t) {
            TsRecord record(t, device_);
            record.add_point("s1", static_cast<int64_t>(t));
            ASSERT_EQ(writer.write_record(record), E_OK);
        }
        for (int64_t t = 2; t <= 300; t += 2) {
            TsRecord record(t, device_);
            record.add_point("s2", static_cast<int64_t>(2 * t));
            ASSERT_EQ(writer.write_record(record), E_OK);
        }
        ASSERT_EQ(writer.flush(), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
    }

    const std::string device_ = "root.qds_with_timegenerator_blocks";
    uint32_t saved_block_memory_ = 0;
    std::string file_;
    TsFileReader reader_;
};

TEST_F(QDSWithTimeGeneratorMultiBlockTest, ScansAcrossMultipleTsBlocks) {
    std::vector<Path> paths{series_path(device_, "s1")};
    ResultSet* result = nullptr;
    ASSERT_EQ(reader_.query(
                  QueryExpression::create(
                      paths, series_expr(device_, "s1", TimeFilter::gt_eq(1))),
                  result),
              E_OK);
    ASSERT_NE(result, nullptr);

    std::vector<RowSnapshot> rows;
    collect_rows(result, rows);
    ASSERT_EQ(rows.size(), 300U);
    for (size_t i = 0; i < rows.size(); ++i) {
        const int64_t expected_time = static_cast<int64_t>(i) + 1;
        EXPECT_EQ(rows[i].timestamp, expected_time);
        EXPECT_EQ(rows[i].value1, expected_time);
    }
    reader_.destroy_query_data_set(result);
}

TEST_F(QDSWithTimeGeneratorMultiBlockTest,
       OrMergesSeriesAcrossBlockBoundaries) {
    Expression* expression = new Expression(
        OR_EXPR, series_expr(device_, "s1", TimeFilter::gt_eq(297)),
        series_expr(device_, "s2", TimeFilter::gt_eq(290)));
    std::vector<Path> paths{series_path(device_, "s1"),
                            series_path(device_, "s2")};
    ResultSet* result = nullptr;
    ASSERT_EQ(reader_.query(QueryExpression::create(paths, expression), result),
              E_OK);
    ASSERT_NE(result, nullptr);

    std::vector<RowSnapshot> rows;
    collect_rows(result, rows);
    // s2 contributes the even timestamps 290..300, s1 adds 297, 298, 299, 300.
    std::vector<int64_t> expected;
    for (int64_t t = 290; t <= 300; ++t) {
        if (t % 2 == 0 || t >= 297) {
            expected.push_back(t);
        }
    }
    EXPECT_EQ(timestamps(rows), expected);
    for (const auto& row : rows) {
        if (row.timestamp % 2 == 0) {
            EXPECT_EQ(row.value2, 2 * row.timestamp);
        } else {
            EXPECT_TRUE(row.value2_is_null);
        }
    }
    reader_.destroy_query_data_set(result);
}

}  // namespace
}  // namespace storage
