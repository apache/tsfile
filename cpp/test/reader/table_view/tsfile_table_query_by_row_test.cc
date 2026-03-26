/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#include <chrono>
#include <random>
#include <set>

#include "common/global.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

using namespace storage;
using namespace common;

class TableQueryByRowTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("table_query_by_row_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        mode_t mode = 0666;
        write_file_.create(file_name_, flags, mode);
    }

    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    static std::string generate_random_string(int length) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 61);
        const std::string chars =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        std::string result;
        for (int i = 0; i < length; ++i) {
            result += chars[dis(gen)];
        }
        return result;
    }

    void write_single_device_file(int num_rows) {
        std::vector<ColumnSchema> col_schemas = {
            ColumnSchema("id1", TSDataType::STRING,
                         CompressionType::UNCOMPRESSED, TSEncoding::PLAIN,
                         ColumnCategory::TAG),
            ColumnSchema("s1", TSDataType::INT64, CompressionType::UNCOMPRESSED,
                         TSEncoding::PLAIN, ColumnCategory::FIELD),
            ColumnSchema("s2", TSDataType::INT64, CompressionType::UNCOMPRESSED,
                         TSEncoding::PLAIN, ColumnCategory::FIELD),
        };
        auto* schema = new TableSchema("t1", col_schemas);
        auto* writer = new TsFileTableWriter(&write_file_, schema);

        Tablet tablet(
            "t1", {"id1", "s1", "s2"},
            {TSDataType::STRING, TSDataType::INT64, TSDataType::INT64},
            {ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD},
            num_rows);

        for (int i = 0; i < num_rows; i++) {
            tablet.add_timestamp(i, static_cast<int64_t>(i));
            tablet.add_value(i, "id1", "device_a");
            tablet.add_value(i, "s1", static_cast<int64_t>(i * 10));
            tablet.add_value(i, "s2", static_cast<int64_t>(i * 100));
        }

        ASSERT_EQ(writer->write_table(tablet), E_OK);
        ASSERT_EQ(writer->flush(), E_OK);
        ASSERT_EQ(writer->close(), E_OK);
        delete writer;
        delete schema;
    }

    void write_multi_device_file(int rows_per_device, int device_count) {
        std::vector<ColumnSchema> col_schemas = {
            ColumnSchema("id1", TSDataType::STRING,
                         CompressionType::UNCOMPRESSED, TSEncoding::PLAIN,
                         ColumnCategory::TAG),
            ColumnSchema("s1", TSDataType::INT64, CompressionType::UNCOMPRESSED,
                         TSEncoding::PLAIN, ColumnCategory::FIELD),
        };
        auto* schema = new TableSchema("t1", col_schemas);
        auto* writer = new TsFileTableWriter(&write_file_, schema);

        int total = rows_per_device * device_count;
        Tablet tablet("t1", {"id1", "s1"},
                      {TSDataType::STRING, TSDataType::INT64},
                      {ColumnCategory::TAG, ColumnCategory::FIELD}, total);

        int row = 0;
        for (int d = 0; d < device_count; d++) {
            std::string device_id = "dev" + std::to_string(d);
            for (int t = 0; t < rows_per_device; t++) {
                tablet.add_timestamp(row, static_cast<int64_t>(t));
                tablet.add_value(row, "id1", device_id);
                tablet.add_value(row, "s1", static_cast<int64_t>(d * 1000 + t));
                row++;
            }
        }

        ASSERT_EQ(writer->write_table(tablet), E_OK);
        ASSERT_EQ(writer->flush(), E_OK);
        ASSERT_EQ(writer->close(), E_OK);
        delete writer;
        delete schema;
    }

    // Writes single-device dense data in multiple batches with flush each time,
    // so the file has multiple ChunkGroups (multiple Chunks per column). Used
    // to exercise SSI-level pushdown where set_row_range causes
    // whole-Chunk/Page skip by count. memory_threshold_bytes should be small to
    // trigger flush.
    void write_single_device_dense_multi_chunk(
        int rows_per_batch, int num_batches, uint64_t memory_threshold_bytes) {
        std::vector<ColumnSchema> col_schemas = {
            ColumnSchema("id1", TSDataType::STRING,
                         CompressionType::UNCOMPRESSED, TSEncoding::PLAIN,
                         ColumnCategory::TAG),
            ColumnSchema("s1", TSDataType::INT64, CompressionType::UNCOMPRESSED,
                         TSEncoding::PLAIN, ColumnCategory::FIELD),
            ColumnSchema("s2", TSDataType::INT64, CompressionType::UNCOMPRESSED,
                         TSEncoding::PLAIN, ColumnCategory::FIELD),
        };
        auto* schema = new TableSchema("t1", col_schemas);
        auto* writer =
            new TsFileTableWriter(&write_file_, schema, memory_threshold_bytes);

        for (int b = 0; b < num_batches; b++) {
            Tablet tablet(
                "t1", {"id1", "s1", "s2"},
                {TSDataType::STRING, TSDataType::INT64, TSDataType::INT64},
                {ColumnCategory::TAG, ColumnCategory::FIELD,
                 ColumnCategory::FIELD},
                rows_per_batch);
            int base = b * rows_per_batch;
            for (int i = 0; i < rows_per_batch; i++) {
                int row_idx = base + i;
                tablet.add_timestamp(i, static_cast<int64_t>(row_idx));
                tablet.add_value(i, "id1", "device_a");
                tablet.add_value(i, "s1", static_cast<int64_t>(row_idx * 10));
                tablet.add_value(i, "s2", static_cast<int64_t>(row_idx * 100));
            }
            ASSERT_EQ(writer->write_table(tablet), E_OK);
            ASSERT_EQ(writer->flush(), E_OK);
        }
        ASSERT_EQ(writer->close(), E_OK);
        delete writer;
        delete schema;
    }

    void write_single_device_sparse_multi_chunk_with_equal_missing(
        int rows_per_batch, int num_batches, uint64_t memory_threshold_bytes,
        int64_t null_start, int64_t null_end) {
        std::vector<ColumnSchema> col_schemas = {
            ColumnSchema("id1", TSDataType::STRING,
                         CompressionType::UNCOMPRESSED, TSEncoding::PLAIN,
                         ColumnCategory::TAG),
            ColumnSchema("s1", TSDataType::INT64, CompressionType::UNCOMPRESSED,
                         TSEncoding::PLAIN, ColumnCategory::FIELD),
            ColumnSchema("s2", TSDataType::INT64, CompressionType::UNCOMPRESSED,
                         TSEncoding::PLAIN, ColumnCategory::FIELD),
        };
        auto* schema = new TableSchema("t1", col_schemas);
        auto* writer =
            new TsFileTableWriter(&write_file_, schema, memory_threshold_bytes);

        // Make s1/s2 have the same amount of missing points, but missing
        // positions differ across columns.
        const int64_t total_rows = static_cast<int64_t>(rows_per_batch) *
                                   static_cast<int64_t>(num_batches);
        const int64_t missing_len = null_end - null_start;
        ASSERT_GT(missing_len, 0);

        // Pick a shifted missing window of the same length for s2.
        int64_t null2_start = null_start + missing_len / 2;
        if (null2_start < 0) null2_start = 0;
        if (null2_start + missing_len > total_rows) {
            null2_start = total_rows - missing_len;
        }
        ASSERT_GE(null2_start, 0);
        ASSERT_LE(null2_start + missing_len, total_rows);
        const int64_t null2_end = null2_start + missing_len;

        for (int b = 0; b < num_batches; b++) {
            Tablet tablet(
                "t1", {"id1", "s1", "s2"},
                {TSDataType::STRING, TSDataType::INT64, TSDataType::INT64},
                {ColumnCategory::TAG, ColumnCategory::FIELD,
                 ColumnCategory::FIELD},
                rows_per_batch);
            int64_t base = static_cast<int64_t>(b) * rows_per_batch;
            for (int i = 0; i < rows_per_batch; i++) {
                int64_t row_idx = base + i;
                tablet.add_timestamp(i, row_idx);
                tablet.add_value(i, "id1", "device_a");

                const bool s1_missing =
                    (row_idx >= null_start && row_idx < null_end);
                const bool s2_missing =
                    (row_idx >= null2_start && row_idx < null2_end);

                if (!s1_missing) {
                    tablet.add_value(i, "s1", row_idx * 10);
                }
                if (!s2_missing) {
                    tablet.add_value(i, "s2", row_idx * 100);
                }
            }

            ASSERT_EQ(writer->write_table(tablet), E_OK);
            ASSERT_EQ(writer->flush(), E_OK);
        }

        ASSERT_EQ(writer->close(), E_OK);
        delete writer;
        delete schema;
    }

    std::vector<int64_t> query_all_s1(const std::string& table_name,
                                      const std::vector<std::string>& columns) {
        TsFileReader reader;
        EXPECT_EQ(reader.open(file_name_), E_OK);
        ResultSet* rs = nullptr;
        EXPECT_EQ(reader.query(table_name, columns, INT64_MIN, INT64_MAX, rs),
                  E_OK);
        std::vector<int64_t> result;
        bool has_next = false;
        while (IS_SUCC(rs->next(has_next)) && has_next) {
            result.push_back(rs->get_value<int64_t>("s1"));
        }
        reader.destroy_query_data_set(rs);
        reader.close();
        return result;
    }

    std::vector<int64_t> query_by_row_s1(const std::string& table_name,
                                         const std::vector<std::string>& cols,
                                         int offset, int limit) {
        TsFileReader reader;
        EXPECT_EQ(reader.open(file_name_), E_OK);
        ResultSet* rs = nullptr;
        EXPECT_EQ(reader.queryByRow(table_name, cols, offset, limit, rs), E_OK);
        EXPECT_NE(rs, nullptr);
        std::vector<int64_t> result;
        bool has_next = false;
        while (IS_SUCC(rs->next(has_next)) && has_next) {
            result.push_back(rs->get_value<int64_t>("s1"));
        }
        reader.destroy_query_data_set(rs);
        reader.close();
        return result;
    }

    std::vector<std::pair<int64_t, int64_t>> query_by_row_time_and_s1(
        const std::string& table_name, const std::vector<std::string>& cols,
        int offset, int limit) {
        TsFileReader reader;
        EXPECT_EQ(reader.open(file_name_), E_OK);
        ResultSet* rs = nullptr;
        EXPECT_EQ(reader.queryByRow(table_name, cols, offset, limit, rs), E_OK);
        EXPECT_NE(rs, nullptr);

        std::vector<std::pair<int64_t, int64_t>> result;
        bool has_next = false;
        while (IS_SUCC(rs->next(has_next)) && has_next) {
            int64_t time = rs->get_value<int64_t>("time");
            // s1 is INT64, use sentinel -1 for NULL.
            int64_t s1_val =
                rs->is_null("s1") ? -1 : rs->get_value<int64_t>("s1");
            result.emplace_back(time, s1_val);
        }

        reader.destroy_query_data_set(rs);
        reader.close();
        return result;
    }

    std::vector<std::pair<int64_t, int64_t>> query_manual_time_and_s1(
        const std::string& table_name, const std::vector<std::string>& cols,
        int offset, int limit) {
        TsFileReader reader;
        EXPECT_EQ(reader.open(file_name_), E_OK);

        ResultSet* rs = nullptr;
        EXPECT_EQ(reader.query(table_name, cols, INT64_MIN, INT64_MAX, rs),
                  E_OK);

        std::vector<std::pair<int64_t, int64_t>> manual;
        bool has_next = false;
        int skipped = 0;
        int taken = 0;
        while (IS_SUCC(rs->next(has_next)) && has_next) {
            if (skipped < offset) {
                skipped++;
                continue;
            }
            if (taken >= limit) {
                break;
            }
            int64_t time = rs->get_value<int64_t>("time");
            int64_t s1_val =
                rs->is_null("s1") ? -1 : rs->get_value<int64_t>("s1");
            manual.emplace_back(time, s1_val);
            taken++;
        }

        reader.destroy_query_data_set(rs);
        reader.close();
        return manual;
    }

    std::string file_name_;
    WriteFile write_file_;
};

// No offset or limit: queryByRow(0, -1) returns the same rows as full query.
TEST_F(TableQueryByRowTest, NoOffsetNoLimit) {
    int num_rows = 50;
    write_single_device_file(num_rows);

    auto all = query_all_s1("t1", {"id1", "s1", "s2"});
    auto result = query_by_row_s1("t1", {"id1", "s1", "s2"}, 0, -1);
    ASSERT_EQ(result.size(), all.size());
    ASSERT_EQ(result, all);
}

// Offset only: skip first N rows, return the rest; limit=-1 means no cap.
TEST_F(TableQueryByRowTest, OffsetOnly) {
    int num_rows = 50;
    write_single_device_file(num_rows);

    auto all = query_all_s1("t1", {"id1", "s1", "s2"});
    int offset = 20;
    auto result = query_by_row_s1("t1", {"id1", "s1", "s2"}, offset, -1);
    ASSERT_EQ(result.size(), static_cast<size_t>(num_rows - offset));
    for (size_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i], all[i + offset]);
    }
}

// Limit only: return at most M rows from the start; offset=0.
TEST_F(TableQueryByRowTest, LimitOnly) {
    int num_rows = 50;
    write_single_device_file(num_rows);

    auto all = query_all_s1("t1", {"id1", "s1", "s2"});
    int limit = 10;
    auto result = query_by_row_s1("t1", {"id1", "s1", "s2"}, 0, limit);
    ASSERT_EQ(result.size(), static_cast<size_t>(limit));
    for (size_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i], all[i]);
    }
}

// Both offset and limit: skip first N rows, then return at most M rows.
TEST_F(TableQueryByRowTest, OffsetAndLimit) {
    int num_rows = 100;
    write_single_device_file(num_rows);

    auto all = query_all_s1("t1", {"id1", "s1", "s2"});
    int offset = 30;
    int limit = 25;
    auto result = query_by_row_s1("t1", {"id1", "s1", "s2"}, offset, limit);
    ASSERT_EQ(result.size(), static_cast<size_t>(limit));
    for (size_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i], all[i + offset]);
    }
}

// Offset beyond total row count: returns empty result.
TEST_F(TableQueryByRowTest, OffsetBeyondData) {
    int num_rows = 30;
    write_single_device_file(num_rows);

    auto result = query_by_row_s1("t1", {"id1", "s1", "s2"}, 100, -1);
    ASSERT_EQ(result.size(), 0u);
}

// Limit zero: returns no rows (no data read).
TEST_F(TableQueryByRowTest, LimitZero) {
    int num_rows = 30;
    write_single_device_file(num_rows);

    auto result = query_by_row_s1("t1", {"id1", "s1", "s2"}, 0, 0);
    ASSERT_EQ(result.size(), 0u);
}

// Offset + limit exceeds total: returns all rows after offset (less than
// limit).
TEST_F(TableQueryByRowTest, OffsetPlusLimitExceedsTotal) {
    int num_rows = 50;
    write_single_device_file(num_rows);

    auto all = query_all_s1("t1", {"id1", "s1", "s2"});
    int offset = 40;
    int limit = 100;
    auto result = query_by_row_s1("t1", {"id1", "s1", "s2"}, offset, limit);
    ASSERT_EQ(result.size(), static_cast<size_t>(num_rows - offset));
    for (size_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i], all[i + offset]);
    }
}

// Multi-device, no offset/limit: queryByRow(0, -1) matches full query order.
TEST_F(TableQueryByRowTest, MultiDeviceNoOffset) {
    int rows_per_device = 20;
    int device_count = 3;
    write_multi_device_file(rows_per_device, device_count);

    auto all = query_all_s1("t1", {"id1", "s1"});
    auto result = query_by_row_s1("t1", {"id1", "s1"}, 0, -1);
    ASSERT_EQ(result.size(), all.size());
    ASSERT_EQ(result, all);
}

// Multi-device, offset within first device: skip applies to global row order.
TEST_F(TableQueryByRowTest, MultiDeviceOffsetWithinFirstDevice) {
    int rows_per_device = 20;
    int device_count = 3;
    write_multi_device_file(rows_per_device, device_count);

    auto all = query_all_s1("t1", {"id1", "s1"});
    int offset = 5;
    auto result = query_by_row_s1("t1", {"id1", "s1"}, offset, -1);
    ASSERT_EQ(result.size(), all.size() - offset);
    for (size_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i], all[i + offset]);
    }
}

// Multi-device, offset skips entire first device(s): verifies device-level
// skip.
TEST_F(TableQueryByRowTest, MultiDeviceOffsetSkipsEntireDevice) {
    int rows_per_device = 20;
    int device_count = 3;
    write_multi_device_file(rows_per_device, device_count);

    auto all = query_all_s1("t1", {"id1", "s1"});
    int offset = 25;
    int limit = 10;
    auto result = query_by_row_s1("t1", {"id1", "s1"}, offset, limit);
    ASSERT_EQ(result.size(), static_cast<size_t>(limit));
    for (size_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i], all[i + offset]);
    }
}

// Multi-device, offset and limit span device boundary: correct cross-device
// slice.
TEST_F(TableQueryByRowTest, MultiDeviceOffsetSpansDeviceBoundary) {
    int rows_per_device = 20;
    int device_count = 3;
    write_multi_device_file(rows_per_device, device_count);

    auto all = query_all_s1("t1", {"id1", "s1"});
    int offset = 18;
    int limit = 15;
    auto result = query_by_row_s1("t1", {"id1", "s1"}, offset, limit);
    ASSERT_EQ(result.size(), static_cast<size_t>(limit));
    for (size_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i], all[i + offset]);
    }
}

// Multi-device, offset beyond all data: returns empty.
TEST_F(TableQueryByRowTest, MultiDeviceOffsetSkipsAllDevices) {
    int rows_per_device = 10;
    int device_count = 3;
    write_multi_device_file(rows_per_device, device_count);

    auto result = query_by_row_s1("t1", {"id1", "s1"}, 100, 10);
    ASSERT_EQ(result.size(), 0u);
}

// Single device: queryByRow(offset, limit) equals full query + manual
// skip/limit in app.
TEST_F(TableQueryByRowTest, EquivalenceWithManualSkip) {
    int num_rows = 200;
    write_single_device_file(num_rows);

    int offset = 73;
    int limit = 42;

    auto by_row = query_by_row_s1("t1", {"id1", "s1", "s2"}, offset, limit);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs = nullptr;
    ASSERT_EQ(reader.query("t1", {"id1", "s1", "s2"}, INT64_MIN, INT64_MAX, rs),
              E_OK);
    std::vector<int64_t> manual;
    bool has_next = false;
    int skipped = 0;
    int collected = 0;
    while (IS_SUCC(rs->next(has_next)) && has_next) {
        if (skipped < offset) {
            skipped++;
            continue;
        }
        if (collected >= limit) break;
        manual.push_back(rs->get_value<int64_t>("s1"));
        collected++;
    }
    reader.destroy_query_data_set(rs);
    reader.close();

    ASSERT_EQ(by_row.size(), manual.size());
    ASSERT_EQ(by_row, manual);
}

// Multi-device: queryByRow(offset, limit) equals full query + manual skip/limit
// in app.
TEST_F(TableQueryByRowTest, MultiDeviceEquivalenceWithManualSkip) {
    int rows_per_device = 30;
    int device_count = 4;
    write_multi_device_file(rows_per_device, device_count);

    int offset = 50;
    int limit = 40;

    auto by_row = query_by_row_s1("t1", {"id1", "s1"}, offset, limit);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs = nullptr;
    ASSERT_EQ(reader.query("t1", {"id1", "s1"}, INT64_MIN, INT64_MAX, rs),
              E_OK);
    std::vector<int64_t> manual;
    bool has_next = false;
    int skipped = 0;
    int collected = 0;
    while (IS_SUCC(rs->next(has_next)) && has_next) {
        if (skipped < offset) {
            skipped++;
            continue;
        }
        if (collected >= limit) break;
        manual.push_back(rs->get_value<int64_t>("s1"));
        collected++;
    }
    reader.destroy_query_data_set(rs);
    reader.close();

    ASSERT_EQ(by_row.size(), manual.size());
    ASSERT_EQ(by_row, manual);
}

// Large single-device dataset: offset and limit correctness with many rows.
TEST_F(TableQueryByRowTest, LargeDatasetOffsetLimit) {
    int num_rows = 5000;
    write_single_device_file(num_rows);

    auto all = query_all_s1("t1", {"id1", "s1", "s2"});
    int offset = 2500;
    int limit = 1000;
    auto result = query_by_row_s1("t1", {"id1", "s1", "s2"}, offset, limit);
    ASSERT_EQ(result.size(), static_cast<size_t>(limit));
    for (size_t i = 0; i < result.size(); i++) {
        ASSERT_EQ(result[i], all[i + offset]);
    }
}

TEST_F(TableQueryByRowTest, DenseAlignedNullsMustUseTimeRowCount) {
    const int rows_per_batch = 200;
    const int num_batches = 4;
    write_single_device_sparse_multi_chunk_with_equal_missing(
        rows_per_batch, num_batches, /*memory_threshold_bytes=*/8 * 1024,
        /*null_start=*/250, /*null_end=*/550);

    const int offset = 260;
    const int limit = 100;

    const std::vector<std::string> cols = {"id1", "s1", "s2"};
    auto by_row = query_by_row_time_and_s1("t1", cols, offset, limit);
    auto manual = query_manual_time_and_s1("t1", cols, offset, limit);

    ASSERT_EQ(by_row.size(), manual.size());
    ASSERT_EQ(by_row, manual);
}

// SSI-level pushdown: dense single-device data with multiple Chunks per column.
// set_row_range(offset, limit) is applied to each column's SSI; SSI skips whole
// Chunks/Pages by ChunkMeta/PageHeader count without decoding. Multi-chunk
// file is produced by small memory_threshold and multiple flush; offset/limit
// are chosen so that at least one Chunk is skipped and result is correct.
TEST_F(TableQueryByRowTest, DenseSingleDeviceSsiLevelPushdown) {
    const int rows_per_batch = 300;
    const int num_batches = 4;
    write_single_device_dense_multi_chunk(rows_per_batch, num_batches,
                                          8 * 1024);

    int offset = 400;
    int limit = 200;
    auto by_row = query_by_row_s1("t1", {"id1", "s1", "s2"}, offset, limit);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    ResultSet* rs = nullptr;
    ASSERT_EQ(reader.query("t1", {"id1", "s1", "s2"}, INT64_MIN, INT64_MAX, rs),
              E_OK);
    std::vector<int64_t> manual;
    bool has_next = false;
    int skipped = 0;
    int collected = 0;
    while (IS_SUCC(rs->next(has_next)) && has_next) {
        if (skipped < offset) {
            skipped++;
            continue;
        }
        if (collected >= limit) break;
        manual.push_back(rs->get_value<int64_t>("s1"));
        collected++;
    }
    reader.destroy_query_data_set(rs);
    reader.close();

    ASSERT_EQ(by_row.size(), manual.size());
    ASSERT_EQ(by_row, manual);
}

// Pushdown is faster than full query + manual next: queryByRow(offset, limit)
// skips at device/SSI/Chunk level; old query then manual next decodes every
// row. Timing tolerance 20% to allow measurement noise.
TEST_F(TableQueryByRowTest, QueryByRowFasterThanManualNext) {
    const int num_rows = 8000;
    const int offset = 3000;
    const int limit = 1000;
    write_single_device_file(num_rows);

    const int num_iters = 5;
    const double tolerance = 0.2;

    auto run_query_by_row = [this, offset, limit]() {
        TsFileReader reader;
        if (reader.open(file_name_) != E_OK) return -1.0;
        ResultSet* rs = nullptr;
        if (reader.queryByRow("t1", {"id1", "s1", "s2"}, offset, limit, rs) !=
            E_OK) {
            reader.close();
            return -1.0;
        }
        auto start = std::chrono::steady_clock::now();
        bool has_next = false;
        while (IS_SUCC(rs->next(has_next)) && has_next) {
            (void)rs->get_value<int64_t>("s1");
        }
        auto end = std::chrono::steady_clock::now();
        reader.destroy_query_data_set(rs);
        reader.close();
        return std::chrono::duration<double, std::milli>(end - start).count();
    };

    auto run_manual_next = [this, offset, limit]() {
        TsFileReader reader;
        if (reader.open(file_name_) != E_OK) return -1.0;
        ResultSet* rs = nullptr;
        if (reader.query("t1", {"id1", "s1", "s2"}, INT64_MIN, INT64_MAX, rs) !=
            E_OK) {
            reader.close();
            return -1.0;
        }
        auto start = std::chrono::steady_clock::now();
        bool has_next = false;
        int skipped = 0;
        int taken = 0;
        while (IS_SUCC(rs->next(has_next)) && has_next) {
            if (skipped < offset) {
                skipped++;
                continue;
            }
            if (taken >= limit) break;
            (void)rs->get_value<int64_t>("s1");
            taken++;
        }
        auto end = std::chrono::steady_clock::now();
        reader.destroy_query_data_set(rs);
        reader.close();
        return std::chrono::duration<double, std::milli>(end - start).count();
    };

    double min_by_row = 1e9;
    double min_manual = 1e9;
    for (int i = 0; i < num_iters; i++) {
        double t1 = run_query_by_row();
        double t2 = run_manual_next();
        if (t1 > 0 && t1 < min_by_row) min_by_row = t1;
        if (t2 > 0 && t2 < min_manual) min_manual = t2;
    }
    ASSERT_GT(min_manual, 0.0) << "manual next timed run failed";
    ASSERT_GT(min_by_row, 0.0) << "queryByRow timed run failed";
    EXPECT_LT(min_by_row, min_manual * (1.0 + tolerance))
        << "queryByRow (pushdown) should be faster than query+manual next "
           "(min_by_row="
        << min_by_row << " ms, min_manual=" << min_manual << " ms)";
}
