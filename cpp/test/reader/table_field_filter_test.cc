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
#include "reader/filter/table_field_filter.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>

#include "common/config/config.h"
#include "common/tablet.h"
#include "file/local_random_access_read_file.h"
#include "file/write_file.h"
#include "reader/filter/tag_filter.h"
#include "reader/filter/time_filter.h"
#include "reader/filter/time_operator.h"
#include "reader/table_query_executor.h"
#include "writer/tsfile_table_writer.h"

namespace {
using namespace common;
using namespace storage;
using Predicate = std::function<int(const TableFieldRow&, bool&)>;

class RowPredicate : public TableFieldFilter {
   public:
    explicit RowPredicate(Predicate predicate) : predicate_(predicate) {}
    int satisfy_row(const TableFieldRow& row, bool& accepted) const override {
        return predicate_(row, accepted);
    }

   private:
    Predicate predicate_;
};

class LegacyPredicate : public Filter {
   public:
    int calls = 0;
    int64_t time = 0;
    int64_t number = 0;
    std::string bytes;
    bool accept = true;
    bool satisfy(int64_t timestamp, int64_t value) override {
        ++calls;
        time = timestamp;
        number = value;
        return accept;
    }
    bool satisfy(int64_t timestamp, String value) override {
        ++calls;
        time = timestamp;
        bytes.assign(value.buf_, value.len_);
        return accept;
    }
};

TEST(TableFieldFilterValueTest, LegacyDispatchPreservesTypesAndBytes) {
    LegacyPredicate filter;
    bool accepted = false;
    const int64_t timestamp = INT64_C(1099511627776);
    const std::string name = "v";
    const bool flag = true;
    const int32_t small = -123;
    const int64_t large = INT64_MIN;
    for (TSDataType type : {BOOLEAN, INT32, DATE, INT64, TIMESTAMP}) {
        const char* value = type == BOOLEAN
                                ? reinterpret_cast<const char*>(&flag)
                            : type == INT32 || type == DATE
                                ? reinterpret_cast<const char*>(&small)
                                : reinterpret_cast<const char*>(&large);
        const uint32_t size = type == BOOLEAN                 ? sizeof(flag)
                              : type == INT32 || type == DATE ? sizeof(small)
                                                              : sizeof(large);
        std::vector<TableFieldValue> fields{{name, type, value, size}};
        EXPECT_EQ(
            evaluate_table_field_filter(filter, {timestamp, fields}, accepted),
            E_OK);
        EXPECT_TRUE(accepted);
        EXPECT_EQ(filter.time, timestamp);
        EXPECT_EQ(filter.number, type == BOOLEAN                 ? 1
                                 : type == INT32 || type == DATE ? small
                                                                 : large);
        fields[0].size = 0;
        EXPECT_EQ(
            evaluate_table_field_filter(filter, {timestamp, fields}, accepted),
            E_DECODE_ERR);
        EXPECT_FALSE(accepted);
    }
    for (TSDataType type : {STRING, TEXT, BLOB}) {
        const std::string bytes("a\0b", 3);
        std::vector<TableFieldValue> fields{{name, type, bytes.data(), 3}};
        EXPECT_EQ(
            evaluate_table_field_filter(filter, {timestamp, fields}, accepted),
            E_OK);
        EXPECT_EQ(filter.bytes, bytes);
        fields[0].size = 0;
        EXPECT_EQ(
            evaluate_table_field_filter(filter, {timestamp, fields}, accepted),
            E_OK);
        EXPECT_TRUE(filter.bytes.empty());
    }
}

TEST(TableFieldFilterValueTest, InvalidLegacyShapesAndSafeGetters) {
    LegacyPredicate filter;
    bool accepted = true;
    const std::string name = "v";
    int64_t number = 42;
    std::vector<TableFieldValue> fields{{name, INT64, nullptr, 8}};
    EXPECT_EQ(evaluate_table_field_filter(filter, {0, fields}, accepted), E_OK);
    EXPECT_FALSE(accepted);
    EXPECT_EQ(filter.calls, 0);
    int64_t output = 7;
    EXPECT_FALSE(fields[0].get(output));
    fields[0].data = reinterpret_cast<const char*>(&number);
    EXPECT_TRUE(fields[0].get(output));
    EXPECT_EQ(output, 42);
    float wrong_type = 0;
    EXPECT_FALSE(fields[0].get(wrong_type));
    fields[0].size = sizeof(int32_t);
    int32_t truncated = 0;
    EXPECT_FALSE(fields[0].get(truncated));
    fields[0].type = DOUBLE;
    EXPECT_FALSE(fields[0].get(wrong_type));
    fields[0].type = FLOAT;
    EXPECT_EQ(evaluate_table_field_filter(filter, {0, fields}, accepted),
              E_NOT_SUPPORT);
    fields.clear();
    EXPECT_EQ(evaluate_table_field_filter(filter, {0, fields}, accepted),
              E_NOT_SUPPORT);
    fields.push_back({name, INT64, nullptr, 0});
    fields.push_back({name, INT64, nullptr, 0});
    EXPECT_EQ(evaluate_table_field_filter(filter, {0, fields}, accepted),
              E_NOT_SUPPORT);
    EXPECT_EQ((TableFieldRow{0, fields}).find("absent"), nullptr);
}

// A small page and explicit flush force rejection/acceptance at page and
// chunk boundaries. Both devices contain sparse NULLs and binary values.
class TableFieldFilterTest
    : public ::testing::TestWithParam<std::tuple<bool, int>> {
   protected:
    void SetUp() override {
        libtsfile_init();
        saved_ = g_config_value_;
        g_config_value_.page_writer_max_point_num_ = 3;
        g_config_value_.parallel_read_enabled_ = std::get<0>(GetParam());
        filename_ =
            "table_field_filter_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".tsfile";
        WriteFile file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(file.create(filename_, flags, 0666), E_OK);
        std::vector<ColumnSchema> columns;
        columns.emplace_back("id", STRING, UNCOMPRESSED, PLAIN,
                             ColumnCategory::TAG);
        const std::vector<std::string> names{"v",    "i",     "flag", "f",
                                             "d",    "s",     "text", "blob",
                                             "date", "stamp", "empty"};
        const std::vector<TSDataType> types{INT64,  INT32,     BOOLEAN, FLOAT,
                                            DOUBLE, STRING,    TEXT,    BLOB,
                                            DATE,   TIMESTAMP, INT64};
        for (size_t i = 0; i < names.size(); ++i)
            columns.emplace_back(names[i], types[i], UNCOMPRESSED, PLAIN,
                                 ColumnCategory::FIELD);
        TableSchema schema("t", columns);
        TsFileTableWriter writer(&file, &schema);
        for (int chunk = 0; chunk < 2; ++chunk) {
            Tablet tablet("t", schema.get_measurement_names(),
                          schema.get_data_types(),
                          schema.get_column_categories(), 14);
            for (int device = 0; device < 2; ++device) {
                for (int row = 0; row < 7; ++row) {
                    int index = device * 7 + row;
                    int t = chunk * 7 + row;
                    ASSERT_EQ(tablet.add_timestamp(index, base_ + t), E_OK);
                    const std::string id = "d" + std::to_string(device);
                    ASSERT_EQ(tablet.add_value(index, "id", id.c_str()), E_OK);
                    ASSERT_EQ(
                        tablet.add_value(index, "v", int64_t(t + device * 100)),
                        E_OK);
                    if (t % 3 != 0)
                        ASSERT_EQ(tablet.add_value(index, "i", int32_t(-t)),
                                  E_OK);
                    ASSERT_EQ(tablet.add_value(index, "flag", bool(t % 2)),
                              E_OK);
                    ASSERT_EQ(tablet.add_value(index, "f", float(t) + 0.25f),
                              E_OK);
                    ASSERT_EQ(tablet.add_value(index, "d", -double(t) - 0.5),
                              E_OK);
                    const std::string bytes =
                        t % 2 ? std::string("a\0b", 3) : std::string();
                    for (const std::string& name : {"s", "text", "blob"})
                        ASSERT_EQ(tablet.add_value(
                                      index, name,
                                      String(bytes.data(), bytes.size())),
                                  E_OK);
                    ASSERT_EQ(
                        tablet.add_value(index, "date", int32_t(20260101 + t)),
                        E_OK);
                    ASSERT_EQ(
                        tablet.add_value(index, "stamp", int64_t(-base_ - t)),
                        E_OK);
                }
            }
            ASSERT_EQ(writer.write_table(tablet), E_OK);
            ASSERT_EQ(writer.flush(), E_OK);
        }
        ASSERT_EQ(writer.close(), E_OK);
    }
    void TearDown() override {
        std::remove(filename_.c_str());
        g_config_value_ = saved_;
        libtsfile_destroy();
    }
    std::vector<int64_t> scan(const std::vector<std::string>& columns,
                              Filter* filter, int offset = 0, int limit = -1,
                              Filter* time = nullptr, Filter* tag = nullptr,
                              int expected_error = E_OK) {
        std::vector<int64_t> times;
        LocalRandomAccessReadFile file;
        EXPECT_EQ(file.open(filename_), E_OK);
        TableQueryExecutor executor(&file, std::get<1>(GetParam()));
        ResultSet* result = nullptr;
        EXPECT_EQ(executor.query("t", columns, time, tag, filter, offset, limit,
                                 result),
                  E_OK);
        if (!result) return times;
        bool next = false;
        int ret = E_OK;
        while ((ret = result->next(next)) == E_OK && next) {
            if (std::get<1>(GetParam()) <= 0) {
                times.push_back(result->get_value<int64_t>(1));
            } else {
                // has_next is idempotent until the block is consumed.
                EXPECT_EQ(result->next(next), E_OK);
                EXPECT_TRUE(next);
                TsBlock* block = nullptr;
                EXPECT_EQ(result->get_next_tsblock(block), E_OK);
                if (!block) break;
                RowIterator rows(block);
                while (rows.has_next()) {
                    uint32_t size = 0;
                    bool null = false;
                    int64_t timestamp = 0;
                    std::memcpy(&timestamp, rows.read(0, &size, &null),
                                sizeof(timestamp));
                    times.push_back(timestamp);
                    rows.next();
                }
            }
        }
        EXPECT_EQ(ret, expected_error);
        EXPECT_FALSE(next);
        // EOF and errors stay stable on repeated calls.
        EXPECT_EQ(result->next(next), expected_error);
        EXPECT_FALSE(next);
        executor.destroy_query_data_set(result);
        return times;
    }
    std::vector<int64_t> expected(const std::function<bool(int)>& predicate,
                                  int devices = 2) {
        std::vector<int64_t> out;
        for (int d = 0; d < devices; ++d)
            for (int t = 0; t < 14; ++t)
                if (predicate(t)) out.push_back(base_ + t);
        return out;
    }
    ConfigValue saved_;
    std::string filename_;
    const int64_t base_ = INT64_C(1099511627776);
};

TEST_P(TableFieldFilterTest, NullTrueFalseAndRejectWholePages) {
    EXPECT_EQ(scan({"v"}, nullptr), expected([](int) { return true; }));
    for (bool accept : {false, true}) {
        int calls = 0;
        auto* filter = new RowPredicate([&](const TableFieldRow&, bool& ok) {
            ++calls;
            ok = accept;
            return E_OK;
        });
        EXPECT_EQ(scan({"v"}, filter),
                  expected([accept](int) { return accept; }));
        EXPECT_EQ(calls, 28);
    }
    int calls = 0;
    EXPECT_EQ(
        scan({"v"}, new RowPredicate([&](const TableFieldRow& row, bool& ok) {
                 ++calls;
                 ok = row.timestamp >= base_ + 10;
                 return E_OK;
             })),
        expected([](int t) { return t >= 10; }));
    EXPECT_EQ(calls, 28);
    // Preserve the original ordinary Filter callback as well.
    auto* reject = new LegacyPredicate();
    reject->accept = false;
    EXPECT_TRUE(scan({"v"}, reject).empty());
    EXPECT_EQ(scan({"v"}, new LegacyPredicate()),
              expected([](int) { return true; }));
}

TEST_P(TableFieldFilterTest, TypedRowsPreserveNullsFloatsBinaryAndTimestamp) {
    int calls = 0;
    const std::vector<std::string> columns{"id",   "v",    "i",     "flag",
                                           "f",    "d",    "s",     "text",
                                           "blob", "date", "stamp", "v"};
    auto* filter = new RowPredicate([&](const TableFieldRow& row, bool& ok) {
        ++calls;
        const int t = int(row.timestamp - base_);
        EXPECT_EQ(row.fields.size(),
                  10u);  // Duplicate projections are one FIELD.
        EXPECT_EQ(row.find("id"), nullptr);
        int64_t v = 0, stamp = 0;
        EXPECT_TRUE(row.find("v")->get(v));
        EXPECT_TRUE(v == t || v == 100 + t);
        EXPECT_TRUE(row.find("stamp")->get(stamp));
        EXPECT_EQ(stamp, -row.timestamp);
        const auto* i = row.find("i");
        EXPECT_EQ(i->is_null(), t % 3 == 0);
        int32_t integer = 0;
        EXPECT_EQ(i->get(integer), t % 3 != 0);
        if (!i->is_null()) EXPECT_EQ(integer, -t);
        float f = 0;
        double d = 0;
        bool flag = false;
        int32_t date = 0;
        EXPECT_TRUE(row.find("f")->get(f));
        EXPECT_EQ(f, float(t) + 0.25f);
        EXPECT_TRUE(row.find("d")->get(d));
        EXPECT_EQ(d, -double(t) - 0.5);
        EXPECT_TRUE(row.find("flag")->get(flag));
        EXPECT_EQ(flag, bool(t % 2));
        EXPECT_TRUE(row.find("date")->get(date));
        EXPECT_EQ(date, 20260101 + t);
        for (const std::string& name : {"s", "text", "blob"}) {
            const auto* value = row.find(name);
            EXPECT_FALSE(value->is_null());
            EXPECT_EQ(std::string(value->data, value->size),
                      t % 2 ? std::string("a\0b", 3) : std::string());
        }
        ok = i->is_null() || (v % 2 == 0 && f > 4);
        return E_OK;
    });
    EXPECT_EQ(scan(columns, filter), expected([](int t) {
                  return t % 3 == 0 || (t % 2 == 0 && t >= 4);
              }));
    EXPECT_EQ(calls, 28);
}

TEST_P(TableFieldFilterTest, OffsetLimitApplyAfterFilterAcrossDevices) {
    auto all = expected([](int t) { return t % 2 == 1; });
    for (int offset : {0, 3, 8, 20}) {
        for (int limit : {0, 1, 5, -1}) {
            auto begin = all.begin() + std::min<size_t>(offset, all.size());
            auto end = limit < 0
                           ? all.end()
                           : begin + std::min<size_t>(limit, all.end() - begin);
            std::vector<int64_t> wanted(begin, end);
            EXPECT_EQ(
                scan({"v"},
                     new RowPredicate([](const TableFieldRow& row, bool& ok) {
                         int64_t v = 0;
                         row.find("v")->get(v);
                         ok = v % 2 == 1;
                         return E_OK;
                     }),
                     offset, limit),
                wanted);
        }
    }
}

TEST_P(TableFieldFilterTest, TimeAndTagFiltersComposeWithFieldFilter) {
    auto* time = TimeFilter::gt_eq(base_ + 5);
    auto* tag = new TagEq(1, "d1");
    EXPECT_EQ(scan({"id", "v"},
                   new RowPredicate([](const TableFieldRow& row, bool& ok) {
                       int64_t v = 0;
                       row.find("v")->get(v);
                       ok = v >= 100 && v % 2 == 0;
                       return E_OK;
                   }),
                   1, 3, time, tag),
              std::vector<int64_t>({base_ + 8, base_ + 10, base_ + 12}));
}

TEST_P(TableFieldFilterTest, CallbackErrorsAreTerminal) {
    int calls = 0;
    auto* filter = new RowPredicate([&](const TableFieldRow&, bool& ok) {
        ok = true;
        return ++calls == 5 ? E_DECODE_ERR : E_OK;
    });
    scan({"v"}, filter, 0, -1, nullptr, nullptr, E_DECODE_ERR);
    EXPECT_EQ(calls, 5);
    scan({"f"}, new LegacyPredicate(), 0, -1, nullptr, nullptr, E_NOT_SUPPORT);
    scan({"v", "i"}, new LegacyPredicate(), 0, -1, nullptr, nullptr,
         E_NOT_SUPPORT);
}

TEST_P(TableFieldFilterTest, LegacyNullAndAllIntegerStringTypes) {
    EXPECT_EQ(scan({"i"}, new LegacyPredicate()),
              expected([](int t) { return t % 3 != 0; }));
    for (const std::string& name :
         {"flag", "date", "stamp", "s", "text", "blob"})
        EXPECT_EQ(scan({name}, new LegacyPredicate()),
                  expected([](int) { return true; }));
}

TEST_P(TableFieldFilterTest, AllNullFieldDoesNotSuppressOtherFields) {
    int calls = 0;
    EXPECT_EQ(scan({"v", "empty"},
                   new RowPredicate([&](const TableFieldRow& row, bool& ok) {
                       ++calls;
                       const auto* empty = row.find("empty");
                       EXPECT_NE(empty, nullptr);
                       if (!empty) return E_DECODE_ERR;
                       EXPECT_EQ(empty->type, INT64);
                       EXPECT_TRUE(empty->is_null());
                       int64_t v = 0;
                       EXPECT_TRUE(row.find("v")->get(v));
                       ok = v % 2 == 0;
                       return E_OK;
                   })),
              expected([](int t) { return t % 2 == 0; }));
    EXPECT_EQ(calls, 28);
}

TEST_P(TableFieldFilterTest, TimeOnlyRowsDoNotInventFieldValues) {
    int calls = 0;
    EXPECT_EQ(
        scan({"id"}, new RowPredicate([&](const TableFieldRow& row, bool& ok) {
                 ++calls;
                 EXPECT_TRUE(row.fields.empty());
                 EXPECT_EQ(row.find("v"), nullptr);
                 ok = row.timestamp >= base_ + 10;
                 return E_OK;
             })),
        expected([](int t) { return t >= 10; }));
    EXPECT_EQ(calls, 28);
}

INSTANTIATE_TEST_SUITE_P(RowAndBatchParallelModes, TableFieldFilterTest,
                         ::testing::Combine(::testing::Bool(),
                                            ::testing::Values(-1, 1, 4)));
}  // namespace
