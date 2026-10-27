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
#include <memory>
#include <string>
#include <vector>

#include "common/config/config.h"
#include "common/tablet.h"
#include "file/local_random_access_read_file.h"
#include "file/write_file.h"
#include "reader/block/single_device_tsblock_reader.h"
#include "writer/tsfile_table_writer.h"

namespace {
using namespace common;
using namespace storage;

class MaskedBulkCopyTest : public ::testing::TestWithParam<bool> {
   protected:
    void SetUp() override {
        libtsfile_init();
        saved_ = g_config_value_;
        g_config_value_.parallel_read_enabled_ = GetParam();
        g_config_value_.page_writer_max_point_num_ = 31;
        // Source tuple: time + v + n + s + hidden + empty = 72 bytes/row.
        // Each source TsBlock has 137 rows, crossing two bitmap words.
        g_config_value_.tsblock_max_memory_ = 72 * 137;
        filename_ =
            "masked_bulk_copy_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".tsfile";
        const std::vector<ColumnSchema> columns{
            {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"n", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"s", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"hidden", TEXT, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"empty", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}};
        schema_ = std::make_shared<TableSchema>("t", columns);
        WriteFile file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(file.create(filename_, flags, 0666), E_OK);
        TsFileTableWriter writer(&file, schema_.get());
        Tablet tablet("t", schema_->get_measurement_names(),
                      schema_->get_data_types(),
                      schema_->get_column_categories(), kRows);
        for (int r = 0; r < kRows; ++r) {
            ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
            ASSERT_EQ(tablet.add_value(r, "v", int64_t(r * 7)), E_OK);
            if (r % 5 != 0)
                ASSERT_EQ(tablet.add_value(r, "n", int64_t(-r)), E_OK);
            if (r % 7 != 0) {
                const auto bytes = string_at(r);
                ASSERT_EQ(tablet.add_value(r, "s",
                                           String(bytes.data(), bytes.size())),
                          E_OK);
            }
            const std::string hidden = "hidden-" + std::to_string(r);
            ASSERT_EQ(tablet.add_value(r, "hidden", hidden.c_str()), E_OK);
        }
        ASSERT_EQ(writer.write_table(tablet), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
    }
    void TearDown() override {
        std::remove(filename_.c_str());
        g_config_value_ = saved_;
        libtsfile_destroy();
    }
    static std::string string_at(int row) {
        return row % 3 == 0   ? std::string()
               : row % 3 == 1 ? std::string("a\0b", 3)
                              : "value-" + std::to_string(row);
    }
    void run_selection(int pattern, int offset, int limit, uint32_t capacity) {
        SCOPED_TRACE(::testing::Message() << pattern << ":" << offset << ":"
                                          << limit << ":" << capacity);
        LocalRandomAccessReadFile file;
        ASSERT_EQ(file.open(filename_), E_OK);
        TsFileIOReader reader;
        ASSERT_EQ(reader.init(&file), E_OK);
        PageArena arena;
        arena.init(512, MOD_TSFILE_READER);
        auto mapping = std::make_shared<ColumnMapping>();
        const auto names = schema_->get_measurement_names();
        auto device = std::make_shared<StringArrayDeviceID>(
            std::vector<std::string>{"t"});
        DeviceQueryTask task(device, names, mapping, nullptr, schema_);
        auto source_filter = AlignedFieldFilter::is_not_null("v");
        VectorMeasurementColumnContext context(&reader);
        std::vector<std::vector<int32_t>> positions{{0, 4}, {1}, {2}, {}, {3}};
        ASSERT_EQ(context.init(&task, names, nullptr, positions, arena,
                               source_filter.get()),
                  E_OK);
        ASSERT_EQ(context.available_rows(), 137U);
        TupleDesc tuple;
        tuple.push_back(ColumnSchema("time", INT64, UNCOMPRESSED, PLAIN));
        for (const auto& name : {"v", "n", "s", "empty"})
            tuple.push_back(schema_->get_column_schema(name));
        tuple.push_back(ColumnSchema("duplicate", INT64, UNCOMPRESSED, PLAIN));
        tuple.push_back(ColumnSchema("extra_time", INT64, UNCOMPRESSED, PLAIN));
        TsBlock output(&tuple, capacity);
        ASSERT_EQ(output.init(), E_OK);
        RowAppender rows(&output);
        std::vector<std::unique_ptr<ColAppender>> owned;
        std::vector<ColAppender*> appenders;
        for (uint32_t c = 0; c < tuple.get_column_count(); ++c) {
            owned.emplace_back(new ColAppender(c, &output));
            appenders.push_back(owned.back().get());
        }
        if (pattern == 4) {
            SelectionMask bad_size(context.available_rows() - 1);
            bad_size.set_all();
            uint32_t source_row = 0, copied = 0;
            int remaining_offset = 0, remaining_limit = -1;
            bool exhausted = false;
            EXPECT_EQ(context.filtered_bulk_copy_into(
                          appenders, appenders[0], appenders[6], &rows,
                          bad_size, source_row, remaining_offset,
                          remaining_limit, copied, exhausted),
                      E_INVALID_ARG);
            EXPECT_EQ(context.available_rows(), 137U);
            EXPECT_EQ(output.get_row_count(), 0U);
            output.reset();
            for (auto* appender : appenders) appender->reset();
            SelectionMask valid(137);
            valid.set_all();
            source_row = 1;  // The source iterators are still at row zero.
            EXPECT_EQ(context.filtered_bulk_copy_into(
                          appenders, appenders[0], appenders[6], &rows, valid,
                          source_row, remaining_offset, remaining_limit, copied,
                          exhausted),
                      E_INVALID_ARG);
            EXPECT_EQ(output.get_row_count(), 0U);
            return;
        }
        int remaining_offset = offset, remaining_limit = limit;
        int base_row = 0;
        std::vector<int64_t> actual, expected;
        while (remaining_limit != 0) {
            const uint32_t count = context.available_rows();
            ASSERT_GT(count, 0U);
            SelectionMask mask(count);
            for (uint32_t r = 0; r < count; ++r) {
                const bool selected =
                    pattern == 0 ||
                    (pattern == 1 &&
                     (((r >= 3 && r < 80) || r == 100 || r >= 120) &&
                      r + 3 < count)) ||
                    (pattern == 2 && r % 2 == 0);
                if (selected) {
                    mask.set_one(r);
                    expected.push_back(base_row + r);
                }
            }
            uint32_t source_row = 0;
            bool exhausted = false;
            while (!exhausted && remaining_limit != 0) {
                output.reset();
                for (auto* appender : appenders) appender->reset();
                uint32_t copied = 0;
                ASSERT_EQ(context.filtered_bulk_copy_into(
                              appenders, appenders[0], appenders[6], &rows,
                              mask, source_row, remaining_offset,
                              remaining_limit, copied, exhausted),
                          E_OK);
                EXPECT_EQ(copied, output.get_row_count());
                EXPECT_LE(copied, capacity);
                EXPECT_EQ(context.available_rows(), count - source_row);
                EXPECT_EQ(exhausted, source_row == count);
                if (!exhausted && remaining_limit != 0) EXPECT_GT(copied, 0U);
                RowIterator iter(&output);
                while (iter.has_next()) {
                    uint32_t len = 0;
                    bool null = false;
                    int64_t time = 0;
                    std::memcpy(&time, iter.read(0, &len, &null), sizeof(time));
                    ASSERT_FALSE(null);
                    actual.push_back(time);
                    for (uint32_t c : {1U, 5U, 6U}) {
                        int64_t value = 0;
                        std::memcpy(&value, iter.read(c, &len, &null),
                                    sizeof(value));
                        EXPECT_FALSE(null);
                        EXPECT_EQ(value, c == 6 ? time : time * 7);
                    }
                    char* n = iter.read(2, &len, &null);
                    EXPECT_EQ(null, time % 5 == 0);
                    if (!null) {
                        int64_t value = 0;
                        std::memcpy(&value, n, sizeof(value));
                        EXPECT_EQ(value, -time);
                    }
                    char* text = iter.read(3, &len, &null);
                    EXPECT_EQ(null, time % 7 == 0);
                    if (!null)
                        EXPECT_EQ(std::string(text, len), string_at(time));
                    iter.read(4, &len, &null);
                    EXPECT_TRUE(null);
                    iter.next();
                }
            }
            if (remaining_limit == 0) break;
            // Copying/skipping to the end must not prefetch and invalidate
            // mask.
            EXPECT_EQ(context.available_rows(), 0U);
            base_row += count;
            const int ret = context.get_next_tsblock(false);
            if (ret == E_NO_MORE_DATA) break;
            ASSERT_EQ(ret, E_OK);
            int64_t time = -1;
            ASSERT_EQ(context.get_current_time(time), E_OK);
            EXPECT_EQ(time, base_row);
        }
        EXPECT_EQ(remaining_offset,
                  std::max(0, offset - static_cast<int>(expected.size())));
        if (limit >= 0)
            EXPECT_EQ(remaining_limit, limit - static_cast<int>(actual.size()));
        const size_t begin = std::min(expected.size(), size_t(offset));
        const size_t end =
            limit < 0 ? expected.size()
                      : std::min(expected.size(), begin + size_t(limit));
        EXPECT_EQ(actual, std::vector<int64_t>(expected.begin() + begin,
                                               expected.begin() + end));
    }
    static constexpr int kRows = 421;
    ConfigValue saved_;
    std::string filename_;
    std::shared_ptr<TableSchema> schema_;
};

TEST_P(MaskedBulkCopyTest,
       AllSelectedRunsResumeAcrossDestinationAndSourceBlocks) {
    for (uint32_t capacity : {1U, 4U, 67U, 1024U})
        for (int offset : {0, 63, 138, 1000})
            for (int limit : {-1, 0, 1, 80})
                run_selection(0, offset, limit, capacity);
}
TEST_P(MaskedBulkCopyTest,
       PartialRunsPreserveNullsStringsDuplicatesAndOffsets) {
    for (uint32_t capacity : {1U, 4U, 67U, 1024U})
        for (int offset : {0, 63, 138, 1000})
            for (int limit : {-1, 0, 1, 80})
                run_selection(1, offset, limit, capacity);
}
TEST_P(MaskedBulkCopyTest, IsolatedSelectionsAndEmptyMasksDoNotPrefetch) {
    for (int pattern : {2, 3})
        for (uint32_t capacity : {1U, 4U, 67U})
            for (int offset : {0, 65})
                run_selection(pattern, offset, -1, capacity);
}

TEST_P(MaskedBulkCopyTest, InvalidMaskAndCursorCannotModifyDestination) {
    run_selection(4, 0, -1, 4);
}

INSTANTIATE_TEST_SUITE_P(DefaultAndAlternateReadModes, MaskedBulkCopyTest,
                         ::testing::Bool());
}  // namespace
