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

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <limits>
#include <tuple>
#include <vector>

#include "common/config/config.h"
#include "common/tablet.h"
#include "file/local_random_access_read_file.h"
#include "file/write_file.h"
#include "reader/filter/aligned_field_filter.h"
#include "reader/filter/time_filter.h"
#include "reader/filter/time_operator.h"
#include "reader/table_query_executor.h"
#include "writer/tsfile_table_writer.h"

namespace {
using namespace common;
using namespace storage;
using A = AlignedFieldFilter;
using Rows = std::vector<std::vector<int64_t>>;
const int64_t kNull = INT64_MIN;

bool compare(double value, double literal, A::CompareOp op) {
    switch (op) {
        case A::CompareOp::EQ:
            return value == literal;
        case A::CompareOp::NE:
            return value != literal;
        case A::CompareOp::LT:
            return value < literal;
        case A::CompareOp::LE:
            return value <= literal;
        case A::CompareOp::GT:
            return value > literal;
        case A::CompareOp::GE:
            return value >= literal;
    }
    return false;
}

class AlignedFieldFilterRegressionTest
    : public ::testing::TestWithParam<std::tuple<bool, int>> {
   protected:
    void SetUp() override {
        libtsfile_init();
        saved_ = g_config_value_;
        g_config_value_.page_writer_max_point_num_ = 3;
        g_config_value_.parallel_read_enabled_ = std::get<0>(GetParam());
        filename_ =
            "aligned_field_filter_regression_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".tsfile";
    }
    void TearDown() override {
        std::remove(filename_.c_str());
        g_config_value_ = saved_;
        libtsfile_destroy();
    }
    void create(WriteFile& file) {
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(file.create(filename_, flags, 0666), E_OK);
    }
    Rows scan(const std::vector<std::string>& columns, A::Ptr filter,
              int offset = 0, int limit = -1, Filter* time_filter = nullptr) {
        LocalRandomAccessReadFile file;
        EXPECT_EQ(file.open(filename_), E_OK);
        TableQueryExecutor executor(&file, std::get<1>(GetParam()));
        ResultSet* result = nullptr;
        EXPECT_EQ(executor.query("t", columns, time_filter, nullptr,
                                 filter.release(), offset, limit, result),
                  E_OK);
        Rows rows;
        if (!result) return rows;
        EXPECT_EQ(result->get_metadata()->get_column_count(),
                  columns.size() + 1);
        bool next = false;
        int ret;
        while ((ret = result->next(next)) == E_OK && next) {
            if (std::get<1>(GetParam()) < 0) {
                std::vector<int64_t> values;
                for (uint32_t c = 1; c <= columns.size() + 1; ++c)
                    values.push_back(
                        result->is_null(c) ? kNull : result->get_int64_at(c));
                rows.push_back(std::move(values));
            } else {
                EXPECT_EQ(result->next(next), E_OK);
                EXPECT_TRUE(next);
                TsBlock* block = nullptr;
                EXPECT_EQ(result->get_next_tsblock(block), E_OK);
                if (!block) break;
                RowIterator iter(block);
                while (iter.has_next()) {
                    std::vector<int64_t> values;
                    for (uint32_t c = 0; c <= columns.size(); ++c) {
                        uint32_t len = 0;
                        bool null = false;
                        char* data = iter.read(c, &len, &null);
                        int64_t value = kNull;
                        if (!null) {
                            EXPECT_EQ(len, sizeof(value));
                            std::memcpy(&value, data, sizeof(value));
                        }
                        values.push_back(value);
                    }
                    rows.push_back(std::move(values));
                    iter.next();
                }
            }
        }
        EXPECT_EQ(ret, E_OK);
        EXPECT_FALSE(next);
        EXPECT_EQ(result->next(next), E_OK);
        EXPECT_FALSE(next);
        executor.destroy_query_data_set(result);
        return rows;
    }
    void write_floats() {
        const std::vector<ColumnSchema> columns{
            {"id", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG},
            {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"f", FLOAT, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"d", DOUBLE, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}};
        TableSchema schema("t", columns);
        WriteFile file;
        create(file);
        TsFileTableWriter writer(&file, &schema);
        // d0/d1 carry the floating-point values; d2 omits both float columns,
        // so every row of d2 presents NULL to the predicate.
        for (int device = 0; device < 3; ++device) {
            std::vector<std::string> names{"id", "v"};
            std::vector<TSDataType> types{STRING, INT64};
            std::vector<ColumnCategory> categories{ColumnCategory::TAG,
                                                   ColumnCategory::FIELD};
            if (device < 2) {
                names.push_back("f");
                types.push_back(FLOAT);
                categories.push_back(ColumnCategory::FIELD);
                names.push_back("d");
                types.push_back(DOUBLE);
                categories.push_back(ColumnCategory::FIELD);
            }
            Tablet tablet("t", names, types, categories, float_values_.size());
            const std::string id = "d" + std::to_string(device);
            for (uint32_t r = 0; r < float_values_.size(); ++r) {
                ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
                ASSERT_EQ(tablet.add_value(r, "id", id.c_str()), E_OK);
                ASSERT_EQ(tablet.add_value(r, "v", int64_t(device * 100 + r)),
                          E_OK);
                if (device < 2) {
                    ASSERT_EQ(tablet.add_value(r, "f", float(float_values_[r])),
                              E_OK);
                    ASSERT_EQ(tablet.add_value(r, "d", float_values_[r]), E_OK);
                }
            }
            ASSERT_EQ(writer.write_table(tablet), E_OK);
            ASSERT_EQ(writer.flush(), E_OK);
        }
        ASSERT_EQ(writer.close(), E_OK);
    }
    void write_missing_columns() {
        const std::vector<ColumnSchema> columns{
            {"id", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG},
            {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"absent", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"s", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"ghost", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"__time_only_aligned_context__", INT64, UNCOMPRESSED, PLAIN,
             ColumnCategory::FIELD}};
        TableSchema schema("t", columns);
        WriteFile file;
        create(file);
        TsFileTableWriter writer(&file, &schema);
        for (int device = 0; device < 3; ++device) {
            std::vector<std::string> names{"id"};
            std::vector<TSDataType> types{STRING};
            std::vector<ColumnCategory> categories{ColumnCategory::TAG};
            if (device != 2) {
                names.push_back("v");
                types.push_back(INT64);
                categories.push_back(ColumnCategory::FIELD);
            }
            if (device != 0) {
                names.push_back("absent");
                types.push_back(INT64);
                categories.push_back(ColumnCategory::FIELD);
                names.push_back("s");
                types.push_back(STRING);
                categories.push_back(ColumnCategory::FIELD);
            }
            Tablet tablet("t", names, types, categories, 6);
            for (int r = 0; r < 6; ++r) {
                ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
                const std::string id = "d" + std::to_string(device);
                ASSERT_EQ(tablet.add_value(r, "id", id.c_str()), E_OK);
                if (device != 2)
                    ASSERT_EQ(
                        tablet.add_value(r, "v", int64_t(device * 100 + r)),
                        E_OK);
                if (device != 0) {
                    ASSERT_EQ(tablet.add_value(r, "absent",
                                               int64_t(device * 1000 + r)),
                              E_OK);
                    const std::string bytes =
                        r % 2 == 0 ? std::string("a\0b", 3) : "other";
                    ASSERT_EQ(tablet.add_value(
                                  r, "s", String(bytes.data(), bytes.size())),
                              E_OK);
                }
            }
            ASSERT_EQ(writer.write_table(tablet), E_OK);
            ASSERT_EQ(writer.flush(), E_OK);
        }
        ASSERT_EQ(writer.close(), E_OK);
    }
    ConfigValue saved_;
    std::string filename_;
    const std::vector<double> float_values_{
        1,
        std::numeric_limits<double>::quiet_NaN(),
        1,
        std::numeric_limits<double>::quiet_NaN(),
        1,
        1,
        1,
        std::numeric_limits<double>::infinity(),
        1,
        1,
        -std::numeric_limits<double>::infinity(),
        std::numeric_limits<double>::quiet_NaN()};
};

TEST_P(AlignedFieldFilterRegressionTest, FloatingStatisticsKeepNaNRowsCorrect) {
    write_floats();
    for (bool is_float : {true, false}) {
        for (auto op : {A::CompareOp::EQ, A::CompareOp::NE, A::CompareOp::LT,
                        A::CompareOp::LE, A::CompareOp::GT, A::CompareOp::GE}) {
            for (double literal :
                 {1.0, 2.0, std::numeric_limits<double>::quiet_NaN(),
                  std::numeric_limits<double>::infinity(),
                  -std::numeric_limits<double>::infinity()}) {
                SCOPED_TRACE(::testing::Message()
                             << is_float << ":" << int(op) << ":" << literal);
                auto filter =
                    A::compare(is_float ? "f" : "d", op,
                               is_float ? A::Literal::float32(float(literal))
                                        : A::Literal::float64(literal));
                Rows expected;
                for (int device = 0; device < 2; ++device)
                    for (uint32_t r = 0; r < float_values_.size(); ++r)
                        if (compare(float_values_[r], literal, op))
                            expected.push_back({r, device * 100 + r});
                EXPECT_EQ(scan({"v"}, std::move(filter)), expected);
            }
        }
    }
}

TEST_P(AlignedFieldFilterRegressionTest, NotPreservesFloatingNaNRowSemantics) {
    write_floats();
    // Control: d2 has no float data at all, so IS NULL must see all of its
    // rows. This proves the NULL device is actually part of the result stream
    // and that the NOT expectations below are not vacuous.
    Rows null_rows;
    for (uint32_t r = 0; r < float_values_.size(); ++r) {
        null_rows.push_back({r, 200 + r});
    }
    EXPECT_EQ(scan({"v"}, A::is_null("f")), null_rows);
    for (bool is_float : {true, false}) {
        for (auto op : {A::CompareOp::EQ, A::CompareOp::NE, A::CompareOp::LT,
                        A::CompareOp::LE, A::CompareOp::GT, A::CompareOp::GE}) {
            for (double literal :
                 {1.0, 2.0, std::numeric_limits<double>::quiet_NaN(),
                  std::numeric_limits<double>::infinity(),
                  -std::numeric_limits<double>::infinity()}) {
                SCOPED_TRACE(::testing::Message() << "not:" << is_float << ":"
                                                  << int(op) << ":" << literal);
                auto filter = A::not_filter(
                    A::compare(is_float ? "f" : "d", op,
                               is_float ? A::Literal::float32(float(literal))
                                        : A::Literal::float64(literal)));
                Rows expected;
                // Negation follows IEEE comparison semantics: NaN never
                // satisfies a comparison, so it always satisfies its
                // negation. NULL rows (device d2) satisfy neither form.
                for (int device = 0; device < 2; ++device)
                    for (uint32_t r = 0; r < float_values_.size(); ++r)
                        if (!compare(float_values_[r], literal, op))
                            expected.push_back({r, device * 100 + r});
                EXPECT_EQ(scan({"v"}, std::move(filter)), expected);
            }
        }
    }
}

TEST_P(AlignedFieldFilterRegressionTest, IncompleteOrCannotBypassEvaluation) {
    write_floats();
    // Every row of every page satisfies the left operand, so page statistics
    // classify the page as a full match. The missing right operand must still
    // surface as an error instead of being silently accepted.
    auto filter = A::or_filter(
        A::compare("v", A::CompareOp::GE, A::Literal::int64(0)), nullptr);
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(filename_), E_OK);
    TableQueryExecutor executor(&file, std::get<1>(GetParam()));
    ResultSet* result = nullptr;
    int ret = executor.query("t", {"v"}, nullptr, nullptr, filter.release(), 0,
                             -1, result);
    if (ret == E_OK) {
        ASSERT_NE(result, nullptr);
        bool next = false;
        int rows = 0;
        while ((ret = result->next(next)) == E_OK && next) {
            // Bound the loop: the broken path can stream rows indefinitely
            // instead of failing, which would otherwise hang the test.
            if (++rows > 1000) break;
        }
        executor.destroy_query_data_set(result);
        EXPECT_EQ(rows, 0) << "incomplete OR returned rows";
    }
    EXPECT_EQ(ret, E_INVALID_ARG);
}

TEST_P(AlignedFieldFilterRegressionTest,
       InvalidTreesCannotBypassEmptyTableOrZeroLimit) {
    for (bool populated : {false, true}) {
        {
            TableSchema schema("t", {{"v", INT64, UNCOMPRESSED, PLAIN,
                                      ColumnCategory::FIELD}});
            WriteFile file;
            create(file);
            TsFileTableWriter writer(&file, &schema);
            if (populated) {
                Tablet tablet("t", {"v"}, {INT64}, {ColumnCategory::FIELD}, 1);
                ASSERT_EQ(tablet.add_timestamp(0, 0), E_OK);
                ASSERT_EQ(tablet.add_value(0, "v", int64_t(1)), E_OK);
                ASSERT_EQ(writer.write_table(tablet), E_OK);
            }
            ASSERT_EQ(writer.close(), E_OK);
        }
        for (int mode : {0, 1, 2}) {
            for (bool conjunction : {false, true}) {
                for (bool malformed : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << populated << ":" << mode << ":"
                                 << conjunction << ":" << malformed);
                    auto left = A::is_not_null("v");
                    auto right = malformed ? A::Ptr() : A::is_null("v");
                    auto filter =
                        conjunction
                            ? A::and_filter(std::move(left), std::move(right))
                            : A::or_filter(std::move(left), std::move(right));
                    LocalRandomAccessReadFile file;
                    ASSERT_EQ(file.open(filename_), E_OK);
                    TableQueryExecutor executor(&file, std::get<1>(GetParam()));
                    ResultSet* result = nullptr;
                    const int ret =
                        mode == 0 ? executor.query("t", {"v"}, nullptr, nullptr,
                                                   filter.release(), result)
                                  : executor.query("t", {"v"}, nullptr, nullptr,
                                                   filter.release(), 0,
                                                   mode == 2 ? 0 : -1, result);
                    EXPECT_EQ(ret, malformed ? E_INVALID_ARG : E_OK);
                    if (malformed) EXPECT_EQ(result, nullptr);
                    if (result != nullptr) {
                        if (!malformed) {
                            bool next = false;
                            EXPECT_EQ(result->next(next), E_OK);
                            EXPECT_EQ(next,
                                      populated && mode != 2 && !conjunction);
                        }
                        executor.destroy_query_data_set(result);
                    }
                }
            }
        }
    }
}

TEST_P(AlignedFieldFilterRegressionTest,
       MissingFieldNamedLikeInternalContextRemainsNull) {
    write_missing_columns();
    const std::string internal_name = "__time_only_aligned_context__";
    for (const auto& columns :
         {std::vector<std::string>{}, std::vector<std::string>{"ghost"},
          std::vector<std::string>{internal_name},
          std::vector<std::string>{"ghost", internal_name},
          std::vector<std::string>{"v", internal_name}}) {
        Rows expected;
        for (int device = 0; device < 3; ++device) {
            for (int r = 0; r < 6; ++r) {
                std::vector<int64_t> row{r};
                for (const auto& column : columns)
                    row.push_back(column == "v" && device != 2
                                      ? device * 100 + r
                                      : kNull);
                expected.push_back(std::move(row));
            }
        }
        EXPECT_EQ(scan(columns, A::is_null(internal_name)), expected);
        EXPECT_EQ(scan(columns, A::is_null("ghost")), expected);
        EXPECT_TRUE(scan(columns, A::is_not_null(internal_name)).empty());
    }
}

TEST_P(AlignedFieldFilterRegressionTest,
       RealFieldNamedLikeInternalContextRetainsValues) {
    const std::string internal_name = "__time_only_aligned_context__";
    TableSchema schema(
        "t",
        {{"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
         {internal_name, INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
         {"ghost", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}});
    WriteFile file;
    create(file);
    TsFileTableWriter writer(&file, &schema);
    Tablet tablet("t", {"v", internal_name}, {INT64, INT64},
                  {ColumnCategory::FIELD, ColumnCategory::FIELD}, 6);
    Rows nulls, nonnulls;
    for (int r = 0; r < 6; ++r) {
        ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
        ASSERT_EQ(tablet.add_value(r, "v", int64_t(r)), E_OK);
        if (r % 3 == 0) {
            nulls.push_back({r, r, kNull, kNull});
        } else {
            ASSERT_EQ(tablet.add_value(r, internal_name, int64_t(100 + r)),
                      E_OK);
            nonnulls.push_back({r, r, kNull, 100 + r});
        }
    }
    ASSERT_EQ(writer.write_table(tablet), E_OK);
    ASSERT_EQ(writer.close(), E_OK);
    // The absent ghost column forces the row fallback, where the actual
    // same-named FIELD must still be read rather than treated as internal.
    const std::vector<std::string> columns{"v", "ghost", internal_name};
    EXPECT_EQ(scan(columns, A::is_null(internal_name)), nulls);
    EXPECT_EQ(scan(columns, A::is_not_null(internal_name)), nonnulls);
}

TEST_P(AlignedFieldFilterRegressionTest, PagePlanResumesAcrossSmallTsBlocks) {
    g_config_value_.page_writer_max_point_num_ = 32;
    g_config_value_.tsblock_max_memory_ = 128;
    const std::vector<ColumnSchema> columns{
        {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
        {"w", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}};
    TableSchema schema("t", columns);
    WriteFile file;
    create(file);
    TsFileTableWriter writer(&file, &schema);
    Tablet tablet("t", schema.get_measurement_names(), schema.get_data_types(),
                  schema.get_column_categories(), 96);
    Rows all, selected;
    for (int r = 0; r < 96; ++r) {
        ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
        ASSERT_EQ(tablet.add_value(r, "v", int64_t(r)), E_OK);
        ASSERT_EQ(tablet.add_value(r, "w", int64_t(-r)), E_OK);
        all.push_back({r, r, -r, r});
        if (r >= 31 && r < 65) selected.push_back({r, r, -r, r});
    }
    ASSERT_EQ(writer.write_table(tablet), E_OK);
    ASSERT_EQ(writer.close(), E_OK);
    EXPECT_EQ(scan({"v", "w", "v"}, nullptr), all);
    EXPECT_EQ(scan({"v", "w", "v"}, A::is_not_null("v")), all);
    auto filter =
        A::and_filter(A::compare("v", A::CompareOp::GE, A::Literal::int64(31)),
                      A::compare("v", A::CompareOp::LT, A::Literal::int64(65)));
    EXPECT_EQ(scan({"v", "w", "v"}, std::move(filter)), selected);
    Rows single_column;
    for (int r = 0; r < 96; ++r) single_column.push_back({r, r});
    EXPECT_EQ(scan({"v"}, A::is_not_null("v")), single_column);
    EXPECT_EQ(scan({"v", "w", "v"}, A::is_not_null("v"), 29, 38),
              Rows(all.begin() + 29, all.begin() + 67));
}

TEST_P(AlignedFieldFilterRegressionTest,
       EmptyFieldMaskResumesIntoPartialAndFullPages) {
    g_config_value_.page_writer_max_point_num_ = 8;
    g_config_value_.tsblock_max_memory_ = 64;
    TableSchema schema(
        "t", {{"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
              {"flag", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
              {"w", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}});
    WriteFile file;
    create(file);
    TsFileTableWriter writer(&file, &schema);
    Tablet tablet("t", schema.get_measurement_names(), schema.get_data_types(),
                  schema.get_column_categories(), 24);
    Rows expected;
    for (int r = 0; r < 24; ++r) {
        ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
        const int64_t value = r < 16 ? r % 8 : r - 6;
        const int64_t flag = r < 8 ? int64_t(r < 4) : 1;
        ASSERT_EQ(tablet.add_value(r, "v", value), E_OK);
        ASSERT_EQ(tablet.add_value(r, "flag", flag), E_OK);
        ASSERT_EQ(tablet.add_value(r, "w", int64_t(1000 + r)), E_OK);
        if (value >= 4 && flag == 1) expected.push_back({r, 1000 + r});
    }
    ASSERT_EQ(writer.write_table(tablet), E_OK);
    ASSERT_EQ(writer.close(), E_OK);
    const auto make_filter = [] {
        return A::and_filter(
            A::compare("v", A::CompareOp::GE, A::Literal::int64(4)),
            A::compare("flag", A::CompareOp::EQ, A::Literal::int64(1)));
    };
    // The first page passes both independent statistics checks, but the
    // conjunction matches no row. Its output-only w column stays undecoded.
    EXPECT_EQ(scan({"w"}, make_filter()), expected);
    EXPECT_EQ(scan({"w"}, make_filter(), 3, 6),
              Rows(expected.begin() + 3, expected.begin() + 9));
    EXPECT_EQ(
        scan({"w"}, make_filter(), 1, 4, TimeFilter::between(14, 20, false)),
        Rows(expected.begin() + 3, expected.begin() + 7));
}

TEST_P(AlignedFieldFilterRegressionTest,
       MissingFilterAndProjectionColumnsAreNull) {
    write_missing_columns();
    Rows null_filter, missing_projection;
    for (int d = 0; d < 2; ++d)
        for (int r = 0; r < 6; ++r) {
            if (d == 0) null_filter.push_back({r, r, kNull, r});
            missing_projection.push_back(
                {r, d == 0 ? kNull : d * 1000 + r, d * 100 + r});
        }
    EXPECT_EQ(scan({"v", "absent", "v"}, A::is_null("absent")), null_filter);
    EXPECT_EQ(scan({"absent", "v"}, A::is_not_null("v")), missing_projection);
    auto filter =
        A::or_filter(A::is_null("absent"),
                     A::compare("v", A::CompareOp::LT, A::Literal::int64(102)));
    Rows either;
    for (int r = 0; r < 6; ++r) either.push_back({r, r});
    either.push_back({0, 100});
    either.push_back({1, 101});
    EXPECT_EQ(scan({"v"}, std::move(filter)), either);
    EXPECT_EQ(scan({"v"}, A::not_filter(A::compare("v", A::CompareOp::LE,
                                                   A::Literal::int64(1)))),
              (Rows{{2, 2},
                    {3, 3},
                    {4, 4},
                    {5, 5},
                    {0, 100},
                    {1, 101},
                    {2, 102},
                    {3, 103},
                    {4, 104},
                    {5, 105}}));
}

TEST_P(AlignedFieldFilterRegressionTest,
       MissingStringFilterPreservesBinaryValues) {
    write_missing_columns();
    Rows expected;
    for (int d = 1; d < 3; ++d)
        for (int r : {0, 2, 4})
            expected.push_back({r, d == 2 ? kNull : d * 100 + r});
    EXPECT_EQ(
        scan({"v"}, A::compare("s", A::CompareOp::EQ,
                               A::Literal::string(std::string("a\0b", 3)))),
        expected);
}

TEST_P(AlignedFieldFilterRegressionTest, AllMissingColumnsUseSharedTimestamps) {
    write_missing_columns();
    Rows expected;
    for (int d = 0; d < 3; ++d)
        for (int r = 0; r < 6; ++r) expected.push_back({r, kNull});
    EXPECT_EQ(scan({"ghost"}, A::is_null("ghost")), expected);
    EXPECT_TRUE(scan({"ghost"}, A::is_not_null("ghost")).empty());
    EXPECT_EQ(scan({"ghost"}, A::is_null("ghost"), 3, 4,
                   TimeFilter::between(1, 4, false)),
              (Rows{{4, kNull}, {1, kNull}, {2, kNull}, {3, kNull}}));
}

TEST_P(AlignedFieldFilterRegressionTest,
       MissingColumnsApplyOffsetAfterFilterAcrossDevices) {
    write_missing_columns();
    for (int offset : {0, 3, 5, 6, 8, 20}) {
        auto filter = A::or_filter(
            A::is_null("absent"),
            A::compare("v", A::CompareOp::LT, A::Literal::int64(102)));
        Rows expected{{0, 0}, {1, 1}, {2, 2},   {3, 3},
                      {4, 4}, {5, 5}, {0, 100}, {1, 101}};
        const size_t start = std::min(expected.size(), size_t(offset));
        expected =
            Rows(expected.begin() + start,
                 expected.begin() + std::min(expected.size(), start + 3));
        EXPECT_EQ(scan({"v"}, std::move(filter), offset, 3), expected);
    }
}

INSTANTIATE_TEST_SUITE_P(RowAndBatchParallelModes,
                         AlignedFieldFilterRegressionTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Values(-1, 1, 4)));
}  // namespace
