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
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

using namespace storage;
using namespace common;

// ─────────────────────────────────────────────────────────────────────────────
// Fixture
// ─────────────────────────────────────────────────────────────────────────────

class TsFileTableReaderRowQueryTest : public ::testing::Test {
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
        file_name_ = "tsfile_table_row_query_test_" +
                     generate_random_string(10) + ".tsfile";
        remove(file_name_.c_str());
    }

    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    // Write a simple table "t1" with columns ["s0" INT64] and `num_rows` rows.
    // Timestamps and values are both 0..num_rows-1.
    void write_simple_table(int num_rows) {
        WriteFile wf;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        wf.create(file_name_, flags, 0666);

        std::vector<ColumnSchema> col_schemas;
        col_schemas.emplace_back("s0", TSDataType::INT64,
                                 CompressionType::UNCOMPRESSED,
                                 TSEncoding::PLAIN, ColumnCategory::FIELD);
        auto* schema = new TableSchema(TABLE_NAME, col_schemas);
        auto writer = std::make_shared<TsFileTableWriter>(&wf, schema);

        Tablet tablet(TABLE_NAME, {"s0"}, {TSDataType::INT64},
                      {ColumnCategory::FIELD}, num_rows);
        for (int i = 0; i < num_rows; i++) {
            tablet.add_timestamp(i, static_cast<int64_t>(i));
            tablet.add_value(i, 0, static_cast<int64_t>(i));
        }
        writer->write_table(tablet);
        writer->flush();
        writer->close();
        delete schema;
    }

    // Count rows returned by queryByRow.
    int count_rows(int offset, int limit) {
        TsFileReader reader;
        EXPECT_EQ(reader.open(file_name_), E_OK);
        ResultSet* rs = nullptr;
        EXPECT_EQ(reader.queryByRow(TABLE_NAME, {"s0"}, offset, limit, rs),
                  E_OK);
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
    static constexpr const char* TABLE_NAME = "t1";
};

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

// ① limit = 0: empty result
TEST_F(TsFileTableReaderRowQueryTest, LimitZeroReturnsEmpty) {
    write_simple_table(TOTAL_ROWS);
    ASSERT_EQ(count_rows(0, 0), 0);
}

// ② limit < total: only `limit` rows
TEST_F(TsFileTableReaderRowQueryTest, LimitLessThanTotal) {
    write_simple_table(TOTAL_ROWS);
    ASSERT_EQ(count_rows(0, 10), 10);
}

// ③ limit > total: all rows
TEST_F(TsFileTableReaderRowQueryTest, LimitExceedsTotal) {
    write_simple_table(TOTAL_ROWS);
    ASSERT_EQ(count_rows(0, 9999), TOTAL_ROWS);
}

// ④ limit < 0: unlimited, returns all rows
TEST_F(TsFileTableReaderRowQueryTest, NegativeLimitMeansUnlimited) {
    write_simple_table(TOTAL_ROWS);
    ASSERT_EQ(count_rows(0, -1), TOTAL_ROWS);
}

// ⑤ offset in the middle
TEST_F(TsFileTableReaderRowQueryTest, OffsetPlusLimit) {
    write_simple_table(TOTAL_ROWS);
    ASSERT_EQ(count_rows(10, 15), 15);
}

// ⑥ offset >= total: empty result
TEST_F(TsFileTableReaderRowQueryTest, OffsetBeyondTotal) {
    write_simple_table(TOTAL_ROWS);
    ASSERT_EQ(count_rows(1000, 10), 0);
}

// ⑦ offset + limit > total: return only remaining rows
TEST_F(TsFileTableReaderRowQueryTest, OffsetPlusLimitExceedsTotal) {
    write_simple_table(TOTAL_ROWS);
    // offset=40, limit=20 → 10 rows remain
    ASSERT_EQ(count_rows(40, 20), 10);
}

// ⑧ Data correctness: time column starts at offset
TEST_F(TsFileTableReaderRowQueryTest, OffsetDataCorrectness) {
    write_simple_table(TOTAL_ROWS);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs = nullptr;
    ASSERT_EQ(reader.queryByRow(TABLE_NAME, {"s0"}, 5, 10, rs), E_OK);

    int count = 0;
    bool has_next = false;
    while (IS_SUCC(rs->next(has_next)) && has_next) {
        // column 1 = time
        int64_t ts = rs->get_value<int64_t>(1);
        EXPECT_EQ(ts, static_cast<int64_t>(5 + count));
        // column 2 = s0 (same value as timestamp in our write helper)
        int64_t val = rs->get_value<int64_t>(2);
        EXPECT_EQ(val, static_cast<int64_t>(5 + count));
        count++;
    }
    EXPECT_EQ(count, 10);
    reader.destroy_query_data_set(rs);
    reader.close();
}

// ⑨ Large dataset, small page size: limit stops loading early (no hang/OOM)
TEST_F(TsFileTableReaderRowQueryTest, LimitStopsEarlyAcrossPages) {
    int prev = g_config_value_.page_writer_max_point_num_;
    g_config_value_.page_writer_max_point_num_ = 5;

    write_simple_table(200);

    ASSERT_EQ(count_rows(0, 50), 50);

    g_config_value_.page_writer_max_point_num_ = prev;
}

// ⑩ Metadata accessible via queryByRow result set
TEST_F(TsFileTableReaderRowQueryTest, MetadataAccessible) {
    write_simple_table(10);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs = nullptr;
    ASSERT_EQ(reader.queryByRow(TABLE_NAME, {"s0"}, 0, 5, rs), E_OK);

    auto meta = rs->get_metadata();
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->get_column_name(1), "time");
    EXPECT_EQ(meta->get_column_name(2), "s0");
    EXPECT_EQ(meta->get_column_count(), 2u);

    reader.destroy_query_data_set(rs);
    reader.close();
}

// ⑪ Paging consistency: two pages together equal full result
TEST_F(TsFileTableReaderRowQueryTest, PaginationConsistency) {
    write_simple_table(40);
    ASSERT_EQ(count_rows(0, 20) + count_rows(20, 20), 40);
}

// ⑫ queryByRow result equivalent to full query with limit applied
TEST_F(TsFileTableReaderRowQueryTest, EquivalentToFullQueryWithLimit) {
    write_simple_table(TOTAL_ROWS);

    // Full query row count
    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs_full = nullptr;
    ASSERT_EQ(reader.query(TABLE_NAME, {"s0"}, INT64_MIN, INT64_MAX, rs_full),
              E_OK);
    int full_count = 0;
    bool has_next = false;
    while (IS_SUCC(rs_full->next(has_next)) && has_next) full_count++;
    reader.destroy_query_data_set(rs_full);

    // queryByRow with limit < 0 should return the same count
    ResultSet* rs_unlimited = nullptr;
    ASSERT_EQ(reader.queryByRow(TABLE_NAME, {"s0"}, 0, -1, rs_unlimited), E_OK);
    int unlimited_count = 0;
    has_next = false;
    while (IS_SUCC(rs_unlimited->next(has_next)) && has_next) unlimited_count++;
    reader.destroy_query_data_set(rs_unlimited);
    reader.close();

    ASSERT_EQ(full_count, unlimited_count);
}

// ⑬ Multiple flushes (multiple chunks): offset/limit still correct
TEST_F(TsFileTableReaderRowQueryTest, MultipleChunksCorrectness) {
    // Write in two batches to ensure multiple chunks
    WriteFile wf;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    wf.create(file_name_, flags, 0666);

    std::vector<ColumnSchema> col_schemas;
    col_schemas.emplace_back("s0", TSDataType::INT64,
                             CompressionType::UNCOMPRESSED, TSEncoding::PLAIN,
                             ColumnCategory::FIELD);
    auto* schema = new TableSchema(TABLE_NAME, col_schemas);
    auto writer = std::make_shared<TsFileTableWriter>(&wf, schema);

    // First batch: rows 0..29
    Tablet tablet1(TABLE_NAME, {"s0"}, {TSDataType::INT64},
                   {ColumnCategory::FIELD}, 30);
    for (int i = 0; i < 30; i++) {
        tablet1.add_timestamp(i, static_cast<int64_t>(i));
        tablet1.add_value(i, 0, static_cast<int64_t>(i));
    }
    writer->write_table(tablet1);
    writer->flush();

    // Second batch: rows 30..59
    Tablet tablet2(TABLE_NAME, {"s0"}, {TSDataType::INT64},
                   {ColumnCategory::FIELD}, 30);
    for (int i = 0; i < 30; i++) {
        tablet2.add_timestamp(i, static_cast<int64_t>(30 + i));
        tablet2.add_value(i, 0, static_cast<int64_t>(30 + i));
    }
    writer->write_table(tablet2);
    writer->flush();
    writer->close();
    delete schema;

    // offset=25, limit=20 → rows 25..44
    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs = nullptr;
    ASSERT_EQ(reader.queryByRow(TABLE_NAME, {"s0"}, 25, 20, rs), E_OK);

    int count = 0;
    bool has_next = false;
    while (IS_SUCC(rs->next(has_next)) && has_next) {
        int64_t ts = rs->get_value<int64_t>(1);
        EXPECT_EQ(ts, static_cast<int64_t>(25 + count));
        count++;
    }
    EXPECT_EQ(count, 20);
    reader.destroy_query_data_set(rs);
    reader.close();
}
