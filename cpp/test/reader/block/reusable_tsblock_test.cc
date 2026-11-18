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
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common/config/config.h"
#include "common/tablet.h"
#include "common/tsblock/tsblock.h"
#include "file/local_random_access_read_file.h"
#include "file/tsfile_io_reader.h"
#include "file/write_file.h"
#include "reader/block/single_device_tsblock_reader.h"
#include "reader/table_result_set.h"
#include "reader/task/device_query_task.h"
#include "writer/tsfile_table_writer.h"

namespace {
using namespace common;
using namespace storage;

constexpr int kRows = 40;
constexpr int64_t kBaseTime = 1000;

TupleDesc MakeDesc(const char* second_name, TSDataType second_type) {
    TupleDesc desc;
    desc.push_back(ColumnSchema("time", INT64, UNCOMPRESSED, PLAIN));
    desc.push_back(ColumnSchema(second_name, second_type, UNCOMPRESSED, PLAIN));
    return desc;
}

class SingleBlockReader : public TsBlockReader {
   public:
    explicit SingleBlockReader(TsBlock* block) : block_(block) {}
    int has_next(bool& has_next) override {
        has_next = !delivered_;
        return E_OK;
    }
    int next(TsBlock*& block) override {
        block = block_;
        delivered_ = true;
        return E_OK;
    }
    void close() override {}

   private:
    TsBlock* block_;
    bool delivered_ = false;
};

TEST(TableResultSetMemoryTest, MaterializedStringsReuseRowStorage) {
    libtsfile_init();
    TupleDesc desc = MakeDesc("s", STRING);
    desc.push_back(ColumnSchema("t", TEXT, UNCOMPRESSED, PLAIN));
    desc.push_back(ColumnSchema("b", BLOB, UNCOMPRESSED, PLAIN));
    const int row_count = 2048;
    ReusableTsBlock reusable;
    TsBlock* block = reusable.acquire(desc, row_count);
    ASSERT_NE(block, nullptr);
    RowAppender rows(block);
    for (int64_t row = 0; row < row_count; ++row) {
        ASSERT_TRUE(rows.add_row());
        rows.append(0, reinterpret_cast<const char*>(&row), sizeof(row));
        const std::string value(row % 4 == 0 ? 2048 : 128,
                                static_cast<char>('a' + row % 26));
        for (uint32_t col = 1; col <= 3; ++col) {
            if (row % 7 == 1)
                rows.append_null(col);
            else
                rows.append(col, value.data(), value.size());
        }
    }
    TableResultSet result(
        std::unique_ptr<TsBlockReader>(new SingleBlockReader(block)),
        {"s", "t", "b"}, {STRING, TEXT, BLOB});
    auto& stats = ModStat::get_instance();
    const int64_t baseline = stats.get_stat(MOD_TSFILE_READER);
    int64_t peak = baseline;
    for (int row = 0; row < row_count; ++row) {
        bool has_next = false;
        ASSERT_EQ(result.next(has_next), E_OK);
        ASSERT_TRUE(has_next);
        EXPECT_EQ(result.get_value<int64_t>(1), row);
        for (uint32_t col = 2; col <= 4; ++col) {
            ASSERT_EQ(result.is_null(col), row % 7 == 1);
            if (!result.is_null(col)) {
                EXPECT_EQ(result.get_value<String*>(col)->to_std_string(),
                          std::string(row % 4 == 0 ? 2048 : 128,
                                      static_cast<char>('a' + row % 26)));
            }
        }
        peak = std::max(peak, stats.get_stat(MOD_TSFILE_READER));
    }
#ifdef ENABLE_MEM_STAT
    // Only the current row's three strings should occupy the materialization
    // arena, including oversized allocations; preceding rows must be released.
    EXPECT_LE(peak - baseline, 8 * 1024);
#endif
    result.close();
    result.close();
    EXPECT_EQ(stats.get_stat(MOD_TSFILE_READER), baseline);
}

TEST(ReusableTsBlockTest, ReusesTheBlockForTheSameLayout) {
    ReusableTsBlock reusable;
    TupleDesc desc = MakeDesc("v", INT64);
    TsBlock* first = reusable.acquire(desc, 32);
    ASSERT_NE(first, nullptr);
    EXPECT_EQ(first->get_column_count(), 2u);
    EXPECT_EQ(first->get_max_row_count(), 32u);

    RowAppender rows(first);
    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(rows.add_row());
        // Append every column of the layout: a partially written row would
        // leave the block in a state the reader never produces.
        int64_t t = i;
        int64_t value = i * 2;
        rows.append(0, reinterpret_cast<char*>(&t), sizeof(t));
        rows.append(1, reinterpret_cast<char*>(&value), sizeof(value));
    }
    first->reset();
    EXPECT_EQ(first->get_row_count(), 0u);

    EXPECT_EQ(reusable.acquire(desc, 32), first);
    EXPECT_EQ(reusable.TEST_build_count(), 1u);
}

// Devices of one table share the column types but not their names; a rename
// must not throw the buffers away.
TEST(ReusableTsBlockTest, RenamedColumnKeepsTheBlock) {
    ReusableTsBlock reusable;
    TupleDesc first_desc = MakeDesc("v", INT64);
    TsBlock* first = reusable.acquire(first_desc, 16);
    ASSERT_NE(first, nullptr);

    TupleDesc renamed = MakeDesc("renamed", INT64);
    TsBlock* second = reusable.acquire(renamed, 16);
    EXPECT_EQ(second, first);
    EXPECT_EQ(reusable.TEST_build_count(), 1u);
    ASSERT_EQ(second->get_column_count(), 2u);
    EXPECT_EQ(second->get_tuple_desc()->get_column_name(1), "renamed");
}

TEST(ReusableTsBlockTest, RebuildsOnADifferentLayout) {
    ReusableTsBlock reusable;
    TupleDesc two_columns = MakeDesc("v", INT64);
    ASSERT_NE(reusable.acquire(two_columns, 16), nullptr);
    EXPECT_EQ(reusable.TEST_build_count(), 1u);

    // Row budget changed: the buffers were sized for the old one.
    TsBlock* wider = reusable.acquire(two_columns, 64);
    ASSERT_NE(wider, nullptr);
    EXPECT_EQ(wider->get_max_row_count(), 64u);
    EXPECT_EQ(reusable.TEST_build_count(), 2u);

    // Column type changed.
    TupleDesc other_type = MakeDesc("v", DOUBLE);
    ASSERT_NE(reusable.acquire(other_type, 64), nullptr);
    EXPECT_EQ(reusable.TEST_build_count(), 3u);

    // Column count changed.
    TupleDesc three_columns = MakeDesc("v", DOUBLE);
    three_columns.push_back(ColumnSchema("w", DOUBLE, UNCOMPRESSED, PLAIN));
    ASSERT_NE(reusable.acquire(three_columns, 64), nullptr);
    EXPECT_EQ(reusable.TEST_build_count(), 4u);

    // Releasing frees the block; the next acquire builds a new one.
    reusable.release();
    ASSERT_NE(reusable.acquire(two_columns, 16), nullptr);
    EXPECT_EQ(reusable.TEST_build_count(), 5u);
}

class ReusableTsBlockScanTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        saved_ = g_config_value_;
        // 8 rows per TsBlock (time + v + w = 24 bytes per row) so one scan
        // asks the factory for several blocks, and 8 rows per page.
        g_config_value_.tsblock_max_memory_ = 24 * 8;
        g_config_value_.page_writer_max_point_num_ = 8;
        filename_ =
            std::string("reusable_tsblock_") +
            ::testing::UnitTest::GetInstance()->current_test_info()->name() +
            ".tsfile";
        std::remove(filename_.c_str());
        const std::vector<ColumnSchema> columns{
            {"v", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD},
            {"w", INT64, UNCOMPRESSED, PLAIN, ColumnCategory::FIELD}};
        schema_ = std::make_shared<TableSchema>("t", columns);
        {
            WriteFile file;
            int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
            flags |= O_BINARY;
#endif
            ASSERT_EQ(file.create(filename_, flags, 0666), E_OK);
            TsFileTableWriter writer(&file, schema_.get());
            Tablet tablet("t", {"v", "w"}, {INT64, INT64},
                          {ColumnCategory::FIELD, ColumnCategory::FIELD},
                          kRows);
            for (int r = 0; r < kRows; r++) {
                ASSERT_EQ(tablet.add_timestamp(r, kBaseTime + r), E_OK);
                ASSERT_EQ(tablet.add_value(r, "v", static_cast<int64_t>(r)),
                          E_OK);
                ASSERT_EQ(tablet.add_value(r, "w", static_cast<int64_t>(r) * 2),
                          E_OK);
            }
            ASSERT_EQ(writer.write_table(tablet), E_OK);
            ASSERT_EQ(writer.close(), E_OK);
        }
        ASSERT_EQ(read_file_.open(filename_), E_OK);
        ASSERT_EQ(reader_.init(&read_file_), E_OK);
        TsFileMeta* meta = nullptr;
        ASSERT_EQ(reader_.get_tsfile_meta(meta), E_OK);
    }

    void TearDown() override {
        std::remove(filename_.c_str());
        g_config_value_ = saved_;
        libtsfile_destroy();
    }

    // Reads every row of the table once, exactly as a device-ordered scan does
    // for one device: the series scan iterator asks @reusable for its output
    // block, and the shared multi-value context bulk-copies both value columns
    // into appenders on a destination block.
    std::vector<int64_t> scan(ReusableTsBlock& reusable) {
        PageArena arena;
        arena.init(512, MOD_TSFILE_READER);
        auto device = std::make_shared<StringArrayDeviceID>(
            std::vector<std::string>{"t"});
        std::vector<std::string> names{"v", "w"};
        DeviceQueryTask task(device, names, std::make_shared<ColumnMapping>(),
                             nullptr, schema_);
        std::vector<std::vector<int32_t>> positions{{0}, {1}};
        VectorMeasurementColumnContext context(&reader_);
        EXPECT_EQ(context.init(&task, names, nullptr, positions, arena, nullptr,
                               0, -1, &reusable),
                  common::E_OK);

        TupleDesc tuple;
        tuple.push_back(ColumnSchema("time", INT64, UNCOMPRESSED, PLAIN));
        tuple.push_back(ColumnSchema("v", INT64, UNCOMPRESSED, PLAIN));
        tuple.push_back(ColumnSchema("w", INT64, UNCOMPRESSED, PLAIN));
        TsBlock output(&tuple, kRows);
        EXPECT_EQ(output.init(), E_OK);
        RowAppender rows(&output);
        std::vector<std::unique_ptr<ColAppender>> owned;
        std::vector<ColAppender*> appenders;
        for (uint32_t c = 0; c < tuple.get_column_count(); ++c) {
            owned.emplace_back(new ColAppender(c, &output));
            appenders.push_back(owned.back().get());
        }

        std::vector<int64_t> times;
        while (true) {
            const uint32_t available = context.available_rows();
            if (available == 0) break;
            output.reset();
            for (auto* appender : appenders) appender->reset();
            const int ret = context.bulk_copy_into(appenders, appenders[0],
                                                   &rows, available);
            EXPECT_TRUE(ret == E_OK || ret == E_NO_MORE_DATA);
            RowIterator iter(&output);
            while (iter.has_next()) {
                uint32_t len = 0;
                bool is_null = false;
                int64_t time = 0;
                std::memcpy(&time, iter.read(0, &len, &is_null), sizeof(time));
                EXPECT_FALSE(is_null);
                int64_t v = 0;
                std::memcpy(&v, iter.read(1, &len, &is_null), sizeof(v));
                EXPECT_FALSE(is_null);
                EXPECT_EQ(v, time - kBaseTime);
                int64_t w = 0;
                std::memcpy(&w, iter.read(2, &len, &is_null), sizeof(w));
                EXPECT_FALSE(is_null);
                EXPECT_EQ(w, (time - kBaseTime) * 2);
                times.push_back(time);
                iter.next();
            }
            if (ret == E_NO_MORE_DATA) break;
        }
        return times;
    }

    ConfigValue saved_;
    std::string filename_;
    std::shared_ptr<TableSchema> schema_;
    LocalRandomAccessReadFile read_file_;
    TsFileIOReader reader_;
};

// Each device of a device-ordered scan gets its own series scan iterator but
// shares the output block: the second scan must see the same buffer, already
// reset, and produce the same rows.
TEST_F(ReusableTsBlockScanTest, ConsecutiveDeviceScansShareOneBlock) {
    ReusableTsBlock reusable;
    for (int device = 0; device < 3; device++) {
        const std::vector<int64_t> times = scan(reusable);
        ASSERT_EQ(times.size(), static_cast<size_t>(kRows)) << device;
        for (int r = 0; r < kRows; r++) {
            EXPECT_EQ(times[r], kBaseTime + r) << device;
        }
    }
    EXPECT_EQ(reusable.TEST_build_count(), 1u);
}

}  // namespace
