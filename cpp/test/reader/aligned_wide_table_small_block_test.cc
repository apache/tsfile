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

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "common/config/config.h"
#include "common/record.h"
#include "common/tablet.h"
#include "file/local_random_access_read_file.h"
#include "file/write_file.h"
#include "reader/table_query_executor.h"
#include "writer/tsfile_table_writer.h"

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

namespace {
using namespace common;
using namespace storage;

constexpr int kWideCols = 100;

class AlignedWideTableSmallBlockTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        saved_ = g_config_value_;
        g_config_value_.page_writer_max_point_num_ = 3;
        g_config_value_.parallel_read_enabled_ = true;
        // One row of kWideCols INT64 columns plus the time column is far
        // larger than 128 bytes, so a TsBlock sized by memory cannot hold a
        // single row.
        g_config_value_.tsblock_max_memory_ = 128;
#ifdef _WIN32
        const auto process_id = static_cast<uint64_t>(_getpid());
#else
        const auto process_id = static_cast<uint64_t>(getpid());
#endif
        static std::atomic<uint64_t> filename_counter{0};
        filename_ =
            "aligned_wide_table_small_block_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            "_" + std::to_string(process_id) + "_" +
            std::to_string(filename_counter.fetch_add(1)) + ".tsfile";
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
    void write_wide_table() {
        std::vector<ColumnSchema> columns;
        columns.push_back(
            {"id", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG});
        for (int i = 0; i < kWideCols; ++i) {
            columns.push_back({"f" + std::to_string(i), INT64, UNCOMPRESSED,
                               PLAIN, ColumnCategory::FIELD});
        }
        TableSchema schema("t", columns);
        WriteFile file;
        create(file);
        TsFileTableWriter writer(&file, &schema);

        std::vector<std::string> names{"id"};
        std::vector<TSDataType> types{STRING};
        std::vector<ColumnCategory> categories{ColumnCategory::TAG};
        for (int i = 0; i < kWideCols; ++i) {
            names.push_back("f" + std::to_string(i));
            types.push_back(INT64);
            categories.push_back(ColumnCategory::FIELD);
        }
        Tablet tablet("t", names, types, categories, 10);
        const std::string id = "d0";
        for (int r = 0; r < 10; ++r) {
            ASSERT_EQ(tablet.add_timestamp(r, r), E_OK);
            ASSERT_EQ(tablet.add_value(r, "id", id.c_str()), E_OK);
            for (int i = 0; i < kWideCols; ++i) {
                ASSERT_EQ(tablet.add_value(r, "f" + std::to_string(i),
                                           int64_t(i * 100 + r)),
                          E_OK);
            }
        }
        ASSERT_EQ(writer.write_table(tablet), E_OK);
        ASSERT_EQ(writer.flush(), E_OK);
        ASSERT_EQ(writer.close(), E_OK);
    }
    std::vector<std::string> wide_columns() const {
        std::vector<std::string> columns;
        columns.reserve(kWideCols);
        for (int i = 0; i < kWideCols; ++i) {
            columns.push_back("f" + std::to_string(i));
        }
        return columns;
    }

    std::string filename_;
    ConfigValue saved_;
};

// A TsBlock whose memory capacity is smaller than a single wide row has no
// usable capacity.  The read must surface E_INVALID_ARG instead of reporting
// empty successful blocks forever (which hung the first next()).
TEST_F(AlignedWideTableSmallBlockTest, UnusableCapacityReportsError) {
    write_wide_table();
    LocalRandomAccessReadFile file;
    ASSERT_EQ(file.open(filename_), E_OK);
    TableQueryExecutor executor(&file, /*batch_size=*/16);
    ResultSet* result = nullptr;
    int ret = executor.query("t", wide_columns(), nullptr, nullptr, nullptr, 0,
                             -1, result);
    if (ret == E_OK) {
        ASSERT_NE(result, nullptr);
        bool next = false;
        ret = result->next(next);
        EXPECT_FALSE(next);
        executor.destroy_query_data_set(result);
    }
    EXPECT_EQ(ret, E_INVALID_ARG);
}

}  // namespace
