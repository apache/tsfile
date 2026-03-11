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

#include <climits>
#include <random>

#include "common/config/config.h"
#include "common/record.h"
#include "common/schema.h"
#include "file/write_file.h"
#include "reader/result_set.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_tree_writer.h"

using namespace storage;
using namespace common;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture
// ─────────────────────────────────────────────────────────────────────────────

class TsFileTreeReaderRowQueryTest : public ::testing::Test {
   protected:
    static std::string generate_random_string(int length) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 61);
        const std::string chars =
            "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        std::string s;
        for (int i = 0; i < length; ++i) s += chars[dis(gen)];
        return s;
    }

    void SetUp() override {
        libtsfile_init();
        file_name_ = "tsfile_tree_row_query_test_" +
                     generate_random_string(10) + ".tsfile";
        remove(file_name_.c_str());
    }

    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    // Write `num_rows` records for each device in `device_ids`,
    // using measurement `mea` (INT64). Timestamps are 0..num_rows-1,
    // values are timestamp * 10.
    void write_data(const std::vector<std::string>& device_ids,
                    const std::string& mea, int num_rows) {
        WriteFile wf;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        wf.create(file_name_, flags, 0666);
        TsFileTreeWriter writer(&wf);
        for (auto dev :
             device_ids) {  // copy: register_timeseries needs non-const ref
            auto* schema = new MeasurementSchema(mea, INT64);
            writer.register_timeseries(dev, schema);
            delete schema;
            for (int i = 0; i < num_rows; i++) {
                TsRecord record(dev, static_cast<int64_t>(i));
                record.add_point(mea, static_cast<int64_t>(i * 10));
                writer.write(record);
            }
        }
        writer.flush();
        writer.close();
    }

    // Count the number of rows returned by queryByRow.
    int count_rows(const std::string& file,
                   const std::vector<std::string>& devs, const std::string& mea,
                   int offset, int limit) {
        TsFileTreeReader reader;
        EXPECT_EQ(reader.open(file), E_OK);
        ResultSet* rs = nullptr;
        EXPECT_EQ(reader.queryByRow(devs, {mea}, offset, limit, rs), E_OK);
        EXPECT_NE(rs, nullptr);
        int count = 0;
        bool has_next = false;
        while (IS_SUCC(rs->next(has_next)) && has_next) count++;
        reader.destroy_query_data_set(rs);
        reader.close();
        return count;
    }

    std::string file_name_;
    enum { TOTAL_ROWS = 50 };
    const std::string DEV = "device";
    const std::string MEA = "s1";
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

// ① limit = 0: empty result
TEST_F(TsFileTreeReaderRowQueryTest, LimitZeroReturnsEmpty) {
    write_data({DEV}, MEA, TOTAL_ROWS);
    ASSERT_EQ(count_rows(file_name_, {DEV}, MEA, 0, 0), 0);
}

// ② limit < total: only `limit` rows returned
TEST_F(TsFileTreeReaderRowQueryTest, LimitLessThanTotal) {
    write_data({DEV}, MEA, TOTAL_ROWS);
    ASSERT_EQ(count_rows(file_name_, {DEV}, MEA, 0, 20), 20);
}

// ③ limit > total: all rows returned
TEST_F(TsFileTreeReaderRowQueryTest, LimitExceedsTotal) {
    write_data({DEV}, MEA, TOTAL_ROWS);
    ASSERT_EQ(count_rows(file_name_, {DEV}, MEA, 0, 1000), TOTAL_ROWS);
}

// ④ limit < 0: unlimited, returns all rows
TEST_F(TsFileTreeReaderRowQueryTest, NegativeLimitMeansUnlimited) {
    write_data({DEV}, MEA, TOTAL_ROWS);
    ASSERT_EQ(count_rows(file_name_, {DEV}, MEA, 0, -1), TOTAL_ROWS);
}

// ⑤ offset + limit in the middle of the data
TEST_F(TsFileTreeReaderRowQueryTest, OffsetPlusLimit) {
    write_data({DEV}, MEA, TOTAL_ROWS);
    // offset=10, limit=15 → should return exactly 15 rows
    ASSERT_EQ(count_rows(file_name_, {DEV}, MEA, 10, 15), 15);
}

// ⑥ offset >= total: empty result
TEST_F(TsFileTreeReaderRowQueryTest, OffsetBeyondTotal) {
    write_data({DEV}, MEA, TOTAL_ROWS);
    ASSERT_EQ(count_rows(file_name_, {DEV}, MEA, 1000, 10), 0);
}

// ⑦ offset + limit > total: return remaining rows from offset
TEST_F(TsFileTreeReaderRowQueryTest, OffsetPlusLimitExceedsTotal) {
    write_data({DEV}, MEA, TOTAL_ROWS);
    // offset=40, limit=20 → only 10 rows remain
    ASSERT_EQ(count_rows(file_name_, {DEV}, MEA, 40, 20), 10);
}

// ⑧ Data correctness: verify timestamps start from `offset`
TEST_F(TsFileTreeReaderRowQueryTest, OffsetDataCorrectness) {
    write_data({DEV}, MEA, TOTAL_ROWS);
    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs = nullptr;
    ASSERT_EQ(reader.queryByRow({DEV}, {MEA}, 5, 10, rs), E_OK);

    int count = 0;
    bool has_next = false;
    while (IS_SUCC(rs->next(has_next)) && has_next) {
        int64_t ts = rs->get_row_record()->get_timestamp();
        EXPECT_EQ(ts, static_cast<int64_t>(5 + count));
        int64_t val = rs->get_value<int64_t>(2);
        EXPECT_EQ(val, (5 + count) * 10);
        count++;
    }
    EXPECT_EQ(count, 10);
    reader.destroy_query_data_set(rs);
    reader.close();
}

// ⑨ Cross-chunk: small page size to force multiple chunks, verify correctness
TEST_F(TsFileTreeReaderRowQueryTest, CorrectnessAcrossChunks) {
    int prev = g_config_value_.page_writer_max_point_num_;
    g_config_value_.page_writer_max_point_num_ = 5;  // tiny pages

    write_data({DEV}, MEA, 30);

    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs = nullptr;
    ASSERT_EQ(reader.queryByRow({DEV}, {MEA}, 5, 10, rs), E_OK);

    int count = 0;
    bool has_next = false;
    while (IS_SUCC(rs->next(has_next)) && has_next) {
        int64_t ts = rs->get_row_record()->get_timestamp();
        EXPECT_EQ(ts, static_cast<int64_t>(5 + count));
        count++;
    }
    EXPECT_EQ(count, 10);
    reader.destroy_query_data_set(rs);
    reader.close();

    g_config_value_.page_writer_max_point_num_ = prev;
}

// ⑩ Multiple devices: verify total row count with offset/limit
TEST_F(TsFileTreeReaderRowQueryTest, MultipleDevicesOffsetLimit) {
    // device_1 and device_2 each have 20 rows with timestamps 0..19.
    // QDSWithoutTimeGenerator merges them by time: at each timestamp both
    // devices' values appear in one RowRecord, so total distinct timestamps
    // = 20.
    write_data({"device_1", "device_2"}, MEA, 20);

    ASSERT_EQ(count_rows(file_name_, {"device_1", "device_2"}, MEA, 5, 10), 10);
}

// ⑪ queryByRow result set supports metadata inspection
TEST_F(TsFileTreeReaderRowQueryTest, MetadataAccessible) {
    write_data({DEV}, MEA, 10);

    TsFileTreeReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs = nullptr;
    ASSERT_EQ(reader.queryByRow({DEV}, {MEA}, 0, 5, rs), E_OK);

    auto meta = rs->get_metadata();
    ASSERT_NE(meta, nullptr);
    // column 1 = "time", column 2 = measurement
    EXPECT_EQ(meta->get_column_name(1), "time");
    EXPECT_EQ(meta->get_column_count(), 2u);

    reader.destroy_query_data_set(rs);
    reader.close();
}

// ⑫ Paging consistency: two pages together equal the full result
TEST_F(TsFileTreeReaderRowQueryTest, PaginationConsistency) {
    write_data({DEV}, MEA, 40);

    int page1 = count_rows(file_name_, {DEV}, MEA, 0, 20);
    int page2 = count_rows(file_name_, {DEV}, MEA, 20, 20);
    ASSERT_EQ(page1 + page2, 40);
}
