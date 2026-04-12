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

#include "common/global.h"
#include "common/record.h"
#include "common/schema.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_tree_writer.h"

using namespace storage;
using namespace common;

class TreeQueryByRowTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("tree_query_by_row_test_") +
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

    // Write a simple tsfile with given devices, measurements, and row count.
    // Each device has all measurements, timestamps are 0, 1, ..., num_rows-1.
    void write_test_file(const std::vector<std::string>& device_ids,
                         const std::vector<std::string>& measurement_ids,
                         int num_rows) {
        TsFileTreeWriter writer(&write_file_);
        for (auto device_id : device_ids) {
            for (auto& measurement : measurement_ids) {
                auto* schema =
                    new MeasurementSchema(measurement, TSDataType::INT64);
                ASSERT_EQ(E_OK, writer.register_timeseries(device_id, schema));
                delete schema;
            }
        }
        for (int row = 0; row < num_rows; ++row) {
            for (const auto& device_id : device_ids) {
                TsRecord record(device_id, static_cast<int64_t>(row));
                for (size_t m = 0; m < measurement_ids.size(); ++m) {
                    record.add_point(measurement_ids[m],
                                     static_cast<int64_t>(row * 100 + m));
                }
                ASSERT_EQ(E_OK, writer.write(record));
            }
        }
        writer.flush();
        writer.close();
    }

    // Helper: collect all rows from a result set into a vector of timestamps.
    std::vector<int64_t> collect_timestamps(ResultSet* result_set) {
        std::vector<int64_t> timestamps;
        bool has_next = false;
        while (IS_SUCC(result_set->next(has_next)) && has_next) {
            timestamps.push_back(result_set->get_row_record()->get_timestamp());
        }
        return timestamps;
    }

    std::string file_name_;
    WriteFile write_file_;
};

// Basic test: queryByRow returns correct total count with no offset/limit.
TEST_F(TreeQueryByRowTest, NoOffsetNoLimit) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 10;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow(devices, measurements, 0, -1, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(num_rows));
    for (int i = 0; i < num_rows; ++i) {
        EXPECT_EQ(timestamps[i], i);
    }

    reader.destroy_query_data_set(result);
    reader.close();
}

// queryByRow skips paths whose device or measurement is missing in the file;
// only existing series are returned (aligned with Java tree reader).
TEST_F(TreeQueryByRowTest, QueryByRow_SkipsMissingDeviceAndMeasurement) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    const int num_rows = 5;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    std::vector<std::string> q_devices = {"d1", "d999"};
    std::vector<std::string> q_meas = {"s1", "ghost_m"};
    ASSERT_EQ(E_OK, reader.queryByRow(q_devices, q_meas, 0, -1, result));
    ASSERT_NE(result, nullptr);

    auto meta = result->get_metadata();
    ASSERT_EQ(2u, meta->get_column_count());

    bool has_next = false;
    int row_count = 0;
    while (IS_SUCC(result->next(has_next)) && has_next) {
        RowRecord* rr = result->get_row_record();
        int64_t ts = rr->get_timestamp();
        ASSERT_EQ(ts, static_cast<int64_t>(row_count));
        Field* f = rr->get_field(1);
        ASSERT_NE(f, nullptr);
        ASSERT_EQ(f->type_, INT64);
        EXPECT_EQ(f->get_value<int64_t>(), static_cast<int64_t>(ts * 100 + 0));
        row_count++;
    }
    EXPECT_EQ(row_count, num_rows);

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: offset skips leading rows.
TEST_F(TreeQueryByRowTest, OffsetOnly) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 10;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    int offset = 3;
    ASSERT_EQ(E_OK,
              reader.queryByRow(devices, measurements, offset, -1, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(num_rows - offset));
    for (size_t i = 0; i < timestamps.size(); ++i) {
        EXPECT_EQ(timestamps[i], static_cast<int64_t>(i + offset));
    }

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: limit caps the number of rows returned.
TEST_F(TreeQueryByRowTest, LimitOnly) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 10;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    int limit = 5;
    ASSERT_EQ(E_OK, reader.queryByRow(devices, measurements, 0, limit, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(limit));
    for (int i = 0; i < limit; ++i) {
        EXPECT_EQ(timestamps[i], i);
    }

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: offset + limit combined.
TEST_F(TreeQueryByRowTest, OffsetAndLimit) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 20;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    int offset = 5;
    int limit = 7;
    ASSERT_EQ(E_OK,
              reader.queryByRow(devices, measurements, offset, limit, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(limit));
    for (int i = 0; i < limit; ++i) {
        EXPECT_EQ(timestamps[i], static_cast<int64_t>(i + offset));
    }

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: offset exceeds total rows → empty result.
TEST_F(TreeQueryByRowTest, OffsetExceedsTotalRows) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 5;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow(devices, measurements, 100, -1, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), 0u);

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: limit=0 → empty result.
TEST_F(TreeQueryByRowTest, LimitZero) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 10;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow(devices, measurements, 0, 0, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), 0u);

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: multi-path (multiple devices, same measurement) merged by time.
// All devices write at same timestamps, so merged row count = num_rows.
TEST_F(TreeQueryByRowTest, MultiPathMerge) {
    std::vector<std::string> devices = {"d1", "d2", "d3"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 10;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    // Without offset/limit, should get num_rows rows (merged by time).
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow(devices, measurements, 0, -1, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    // Timestamps 0..9, each appearing once (all devices have same timestamps).
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(num_rows));
    for (int i = 0; i < num_rows; ++i) {
        EXPECT_EQ(timestamps[i], i);
    }

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: multi-path with offset and limit.
TEST_F(TreeQueryByRowTest, MultiPathOffsetLimit) {
    std::vector<std::string> devices = {"d1", "d2"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 20;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    int offset = 5;
    int limit = 8;
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK,
              reader.queryByRow(devices, measurements, offset, limit, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(limit));
    for (int i = 0; i < limit; ++i) {
        EXPECT_EQ(timestamps[i], static_cast<int64_t>(i + offset));
    }

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: single path with multiple measurements.
TEST_F(TreeQueryByRowTest, SingleDeviceMultipleMeasurements) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1", "s2", "s3"};
    int num_rows = 15;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    int offset = 3;
    int limit = 5;
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK,
              reader.queryByRow(devices, measurements, offset, limit, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    // Multiple measurements from same device at same time → merged row count.
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(limit));
    for (int i = 0; i < limit; ++i) {
        EXPECT_EQ(timestamps[i], static_cast<int64_t>(i + offset));
    }

    // Verify values in the returned rows.
    reader.destroy_query_data_set(result);

    // Re-query without offset/limit for verification.
    ResultSet* result2 = nullptr;
    ASSERT_EQ(E_OK,
              reader.queryByRow(devices, measurements, offset, limit, result2));
    ASSERT_NE(result2, nullptr);

    bool has_next = false;
    int row_count = 0;
    while (IS_SUCC(result2->next(has_next)) && has_next) {
        RowRecord* rr = result2->get_row_record();
        int64_t ts = rr->get_timestamp();
        int expected_row = static_cast<int>(ts);
        // Check first measurement value (s1).
        if (!result2->is_null(1)) {
            // Column 0 is time, column 1 is first path
            Field* f = rr->get_field(1);
            if (f != nullptr && f->type_ == INT64) {
                EXPECT_EQ(f->get_value<int64_t>(),
                          static_cast<int64_t>(expected_row * 100 + 0));
            }
        }
        row_count++;
    }
    EXPECT_EQ(row_count, limit);

    reader.destroy_query_data_set(result2);
    reader.close();
}

// Test: limit larger than available rows → returns all rows.
TEST_F(TreeQueryByRowTest, LimitLargerThanAvailable) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 5;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow(devices, measurements, 0, 100, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(num_rows));

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: larger dataset to exercise chunk/page boundaries.
TEST_F(TreeQueryByRowTest, LargeDatasetOffsetLimit) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    int num_rows = 5000;
    write_test_file(devices, measurements, num_rows);

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    int offset = 1000;
    int limit = 500;
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK,
              reader.queryByRow(devices, measurements, offset, limit, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(limit));
    for (int i = 0; i < limit; ++i) {
        EXPECT_EQ(timestamps[i], static_cast<int64_t>(i + offset));
    }

    reader.destroy_query_data_set(result);
    reader.close();
}

// Test: multi-device multi-measurement with interleaved timestamps.
TEST_F(TreeQueryByRowTest, MultiDeviceMultiMeasurementInterleaved) {
    // Device d1 has timestamps 0,2,4,6,...
    // Device d2 has timestamps 1,3,5,7,...
    // After merge, rows are 0,1,2,3,...
    TsFileTreeWriter writer(&write_file_);
    std::string device1 = "d1";
    std::string device2 = "d2";
    std::string measurement = "s1";

    auto* schema1 = new MeasurementSchema(measurement, TSDataType::INT64);
    auto* schema2 = new MeasurementSchema(measurement, TSDataType::INT64);
    ASSERT_EQ(E_OK, writer.register_timeseries(device1, schema1));
    ASSERT_EQ(E_OK, writer.register_timeseries(device2, schema2));
    delete schema1;
    delete schema2;

    int num_per_device = 10;
    for (int i = 0; i < num_per_device; ++i) {
        TsRecord r1(device1, static_cast<int64_t>(i * 2));
        r1.add_point(measurement, static_cast<int64_t>(i * 2));
        ASSERT_EQ(E_OK, writer.write(r1));

        TsRecord r2(device2, static_cast<int64_t>(i * 2 + 1));
        r2.add_point(measurement, static_cast<int64_t>(i * 2 + 1));
        ASSERT_EQ(E_OK, writer.write(r2));
    }
    writer.flush();
    writer.close();

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    std::vector<std::string> devices = {device1, device2};
    std::vector<std::string> measurements = {measurement};

    // Total merged rows = 20, offset=5, limit=8 → rows 5..12
    int offset = 5;
    int limit = 8;
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK,
              reader.queryByRow(devices, measurements, offset, limit, result));
    ASSERT_NE(result, nullptr);

    auto timestamps = collect_timestamps(result);
    EXPECT_EQ(timestamps.size(), static_cast<size_t>(limit));
    for (int i = 0; i < limit; ++i) {
        EXPECT_EQ(timestamps[i], static_cast<int64_t>(i + offset));
    }

    reader.destroy_query_data_set(result);
    reader.close();
}

// ============================================================
// Helpers for chunk/page boundary tests
// ============================================================

// PageGuard: RAII wrapper that temporarily sets page_writer_max_point_num_
// so that every `page_size` written data points a new page is flushed.
struct PageGuard {
    uint32_t saved_;
    explicit PageGuard(uint32_t page_size) {
        saved_ = g_config_value_.page_writer_max_point_num_;
        g_config_value_.page_writer_max_point_num_ = page_size;
    }
    ~PageGuard() { g_config_value_.page_writer_max_point_num_ = saved_; }
};

// Write `num_rows` rows to `device`/`measurement` starting at timestamp
// `t_start`. Flushes the writer after every `flush_every` rows to force a
// new Chunk boundary.  All values are INT64 = timestamp.
// Pass flush_every=0 to disable mid-write flushes.
static void write_single_path_multi_chunk(TsFileTreeWriter& writer,
                                          const std::string& device,
                                          const std::string& measurement,
                                          int64_t t_start, int num_rows,
                                          int flush_every) {
    auto* schema = new MeasurementSchema(measurement, TSDataType::INT64);
    auto device_name = device;
    ASSERT_EQ(E_OK, writer.register_timeseries(device_name, schema));
    delete schema;
    for (int r = 0; r < num_rows; ++r) {
        int64_t ts = t_start + r;
        TsRecord rec(device, ts);
        rec.add_point(measurement, ts);
        ASSERT_EQ(E_OK, writer.write(rec));
        if (flush_every > 0 && (r + 1) % flush_every == 0 && r + 1 < num_rows) {
            ASSERT_EQ(E_OK, writer.flush());
        }
    }
    ASSERT_EQ(E_OK, writer.flush());
}

// ============================================================
// Single-path: skip entire Chunks via count-based offset pushdown
// ============================================================
//
// Layout: page_size=10, 3 chunks x 30 rows each (forced by flush).
// Each Chunk has Statistic.count=30.
// should_skip_chunk_by_offset fires when remaining_offset >= chunk.count.
//
//   Chunk1 [t=0..29, count=30]
//   Chunk2 [t=30..59, count=30]
//   Chunk3 [t=60..89, count=30]

// offset exactly equals one chunk: Chunk1 is skipped wholesale.
TEST_F(TreeQueryByRowTest, SinglePath_SkipChunk_OffsetEqualsOneChunk) {
    PageGuard pg(10);  // 10 pts/page -> 3 pages/chunk
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 90, 30);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    // offset=30 -> count(Chunk1)=30 == offset -> skip Chunk1.
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 30, 10, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 10u);
    for (int i = 0; i < 10; ++i) EXPECT_EQ(ts[i], 30 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// offset equals two chunk counts: both Chunk1 and Chunk2 are skipped.
TEST_F(TreeQueryByRowTest, SinglePath_SkipChunk_OffsetEqualsTwoChunks) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 90, 30);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    // offset=60 -> skip Chunk1 (offset->30) then Chunk2 (offset->0).
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 60, 5, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 5u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(ts[i], 60 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// offset = chunk_count - 1: Chunk1 cannot be skipped (count=30 > 29);
// 29 rows consumed inside Chunk1, then result spans into Chunk2.
TEST_F(TreeQueryByRowTest, SinglePath_OffsetJustBeforeChunkBoundary) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 60, 30);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 29, 10, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 10u);
    for (int i = 0; i < 10; ++i) EXPECT_EQ(ts[i], 29 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// offset = chunk_count + 1: Chunk1 is skipped; 1 row consumed inside
// Chunk2; result starts at t=31.
TEST_F(TreeQueryByRowTest, SinglePath_OffsetJustAfterChunkBoundary) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 60, 30);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 31, 5, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 5u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(ts[i], 31 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// ============================================================
// Single-path: skip entire Pages via count-based offset pushdown
// ============================================================
//
// Layout: page_size=10, 30 rows in ONE chunk -> 3 pages.
// Each Page has Statistic.count=10.
// should_skip_page_by_offset fires when remaining_offset >= page.count.
//
//   Page1 [t=0..9, count=10]
//   Page2 [t=10..19, count=10]
//   Page3 [t=20..29, count=10]

// offset exactly equals one page: Page1 is skipped wholesale.
TEST_F(TreeQueryByRowTest, SinglePath_SkipPage_OffsetEqualsOnePage) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 30, 0);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    // offset=10 -> count(Page1)=10 == offset -> skip Page1.
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 10, 5, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 5u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(ts[i], 10 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// offset equals two page counts: Page1 + Page2 are both skipped.
TEST_F(TreeQueryByRowTest, SinglePath_SkipPage_OffsetEqualsTwoPages) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 30, 0);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    // offset=20 -> skip Page1 (offset->10) then Page2 (offset->0).
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 20, 5, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 5u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(ts[i], 20 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// offset = page_count - 1: Page1 cannot be skipped (count=10 > 9);
// 9 rows consumed row-by-row inside Page1, then result spans Page2.
TEST_F(TreeQueryByRowTest, SinglePath_SkipPage_OffsetJustBeforePageBoundary) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 30, 0);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 9, 5, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 5u);
    for (int i = 0; i < 5; ++i) EXPECT_EQ(ts[i], 9 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// ============================================================
// Single-path: early termination via row_limit_ = 0
// ============================================================

// limit < page_size: stop inside the first page.
// row_limit_ reaches 0 mid-page; subsequent pages/chunks must not load.
TEST_F(TreeQueryByRowTest, SinglePath_LimitStopsMidPage) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 30, 0);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 0, 3, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 3u);
    for (int i = 0; i < 3; ++i) EXPECT_EQ(ts[i], i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// limit = exactly one page: stop at the page boundary.
TEST_F(TreeQueryByRowTest, SinglePath_LimitEqualsOnePage) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 30, 0);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 0, 10, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 10u);
    for (int i = 0; i < 10; ++i) EXPECT_EQ(ts[i], i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// limit = exactly one chunk (3 pages): stop at the chunk boundary.
TEST_F(TreeQueryByRowTest, SinglePath_LimitEqualsOneChunk) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 90, 30);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    // limit=30 -> consume exactly Chunk1, row_limit_=0 prevents Chunk2.
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 0, 30, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 30u);
    for (int i = 0; i < 30; ++i) EXPECT_EQ(ts[i], i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// offset skips 2 chunks; limit stops mid-page inside the 3rd chunk.
TEST_F(TreeQueryByRowTest, SinglePath_SkipTwoChunksThenLimitMidPage) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        write_single_path_multi_chunk(writer, "d1", "s1", 0, 90, 30);
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1"}, {"s1"}, 60, 7, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 7u);
    for (int i = 0; i < 7; ++i) EXPECT_EQ(ts[i], 60 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// ============================================================
// Multi-path: offset/limit pushdown with multiple chunks per path
// ============================================================
//
// For multi-path, per-path chunk/page count skip is disabled because
// merged row order does not equal per-path row order.  Offset/limit
// are consumed at the merge layer row-by-row.  These tests verify
// correctness while exercising the multi-chunk merge path including
// get_next_tsblock_with_hint (min_time_hint forwarding).

TEST_F(TreeQueryByRowTest, MultiPath_OffsetLimitWithMultipleChunks) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        for (auto dev : {"d1", "d2"}) {
            auto* s = new MeasurementSchema("s1", TSDataType::INT64);
            std::string device_name = dev;
            ASSERT_EQ(E_OK, writer.register_timeseries(device_name, s));
            delete s;
        }
        // 60 timestamps, flush every 20 -> 3 chunks per device.
        for (int r = 0; r < 60; ++r) {
            for (auto& dev : {"d1", "d2"}) {
                TsRecord rec(dev, static_cast<int64_t>(r));
                rec.add_point("s1", static_cast<int64_t>(r));
                ASSERT_EQ(E_OK, writer.write(rec));
            }
            if ((r + 1) % 20 == 0 && r + 1 < 60) {
                ASSERT_EQ(E_OK, writer.flush());
            }
        }
        ASSERT_EQ(E_OK, writer.flush());
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    // offset=25, limit=10 -> merged rows t=25..34
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1", "d2"}, {"s1"}, 25, 10, result));
    ASSERT_NE(result, nullptr);
    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 10u);
    for (int i = 0; i < 10; ++i) EXPECT_EQ(ts[i], 25 + i);
    reader.destroy_query_data_set(result);

    // offset=40 (crosses chunk boundary) -> rows t=40..49
    ASSERT_EQ(E_OK, reader.queryByRow({"d1", "d2"}, {"s1"}, 40, 10, result));
    ASSERT_NE(result, nullptr);
    ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 10u);
    for (int i = 0; i < 10; ++i) EXPECT_EQ(ts[i], 40 + i);
    reader.destroy_query_data_set(result);

    reader.close();
}

// Two devices with interleaved timestamps (d1=even, d2=odd), multiple
// chunks each.  Merged stream is t=0,1,2,...,79 (80 rows).
TEST_F(TreeQueryByRowTest, MultiPath_InterleavedTimestamps_MultipleChunks) {
    PageGuard pg(5);
    {
        std::string d1 = "d1", d2 = "d2";
        TsFileTreeWriter writer(&write_file_);
        auto* s1 = new MeasurementSchema("s1", TSDataType::INT64);
        auto* s2 = new MeasurementSchema("s1", TSDataType::INT64);
        ASSERT_EQ(E_OK, writer.register_timeseries(d1, s1));
        ASSERT_EQ(E_OK, writer.register_timeseries(d2, s2));
        delete s1;
        delete s2;

        // d1: t=0,2,4,...,78  d2: t=1,3,5,...,79  flush every 20 pairs.
        for (int i = 0; i < 40; ++i) {
            TsRecord r1("d1", static_cast<int64_t>(i * 2));
            r1.add_point("s1", static_cast<int64_t>(i * 2));
            ASSERT_EQ(E_OK, writer.write(r1));

            TsRecord r2("d2", static_cast<int64_t>(i * 2 + 1));
            r2.add_point("s1", static_cast<int64_t>(i * 2 + 1));
            ASSERT_EQ(E_OK, writer.write(r2));

            if ((i + 1) % 20 == 0 && i + 1 < 40) {
                ASSERT_EQ(E_OK, writer.flush());
            }
        }
        ASSERT_EQ(E_OK, writer.flush());
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    // offset=30, limit=20 -> t=30..49
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1", "d2"}, {"s1"}, 30, 20, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 20u);
    for (int i = 0; i < 20; ++i) EXPECT_EQ(ts[i], 30 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// Three devices, offset at exact chunk boundary, limit cuts mid-chunk.
TEST_F(TreeQueryByRowTest, MultiPath_OffsetAtMergedChunkBoundary) {
    PageGuard pg(10);
    {
        TsFileTreeWriter writer(&write_file_);
        for (auto& dev : {"d1", "d2", "d3"}) {
            auto* s = new MeasurementSchema("s1", TSDataType::INT64);
            std::string device_name = dev;
            ASSERT_EQ(E_OK, writer.register_timeseries(device_name, s));
            delete s;
        }
        for (int r = 0; r < 60; ++r) {
            for (auto& dev : {"d1", "d2", "d3"}) {
                TsRecord rec(dev, static_cast<int64_t>(r));
                rec.add_point("s1", static_cast<int64_t>(r));
                ASSERT_EQ(E_OK, writer.write(rec));
            }
            if ((r + 1) % 20 == 0 && r + 1 < 60) {
                ASSERT_EQ(E_OK, writer.flush());
            }
        }
        ASSERT_EQ(E_OK, writer.flush());
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    // offset=20 -> skip first 20 merged rows, get t=20..26
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK,
              reader.queryByRow({"d1", "d2", "d3"}, {"s1"}, 20, 7, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 7u);
    for (int i = 0; i < 7; ++i) EXPECT_EQ(ts[i], 20 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// ============================================================
// Multi-path: min_time_hint skips stale (out-of-order) chunks
// ============================================================
//
// TsFile allows multiple flushes for the same path.  A later flush can
// produce a chunk whose time range is entirely BEFORE the current merge
// cursor.  The min_time_hint pushdown (should_skip_chunk_by_time) detects
// end_time < hint and skips that chunk to avoid re-inserting already-passed
// timestamps into the heap.
//
// File layout:
//   d2 flush-1: t=50..59  -> chunk-d2-1  (end_time=59)
//   d1 flush-2: t=0..99   -> chunk-d1-1
//   d2 flush-3: t=0..9    -> chunk-d2-2  (end_time=9)  <- stale!
//
// When the merge exhausts chunk-d2-1 (cursor ~59), it calls
// get_next_tsblock_with_hint(d2, hint=59).  chunk-d2-2 has end_time=9 < 59
// -> should_skip_chunk_by_time returns true -> chunk-d2-2 is skipped.
//
// Observable effect:
//   - Total merged rows = 100 (all from d1, t=0..99).
//   - d2 is non-null only at t=50..59 (the 10 rows from chunk-d2-1).
//   - No out-of-order or duplicate timestamps.
TEST_F(TreeQueryByRowTest, MultiPath_TimeHint_SkipsStaleChunk) {
    {
        TsFileTreeWriter writer(&write_file_);
        std::string d1 = "d1", d2 = "d2";
        auto* sd1 = new MeasurementSchema("s1", TSDataType::INT64);
        auto* sd2 = new MeasurementSchema("s1", TSDataType::INT64);
        ASSERT_EQ(E_OK, writer.register_timeseries(d1, sd1));
        ASSERT_EQ(E_OK, writer.register_timeseries(d2, sd2));
        delete sd1;
        delete sd2;

        // d2 chunk1: t=50..59
        for (int64_t t = 50; t < 60; ++t) {
            TsRecord rec("d2", t);
            rec.add_point("s1", t * 10);
            ASSERT_EQ(E_OK, writer.write(rec));
        }
        ASSERT_EQ(E_OK, writer.flush());

        // d1: t=0..99
        for (int64_t t = 0; t < 100; ++t) {
            TsRecord rec("d1", t);
            rec.add_point("s1", t);
            ASSERT_EQ(E_OK, writer.write(rec));
        }
        ASSERT_EQ(E_OK, writer.flush());

        // d2 chunk2 (stale): t=0..9
        for (int64_t t = 0; t < 10; ++t) {
            TsRecord rec("d2", t);
            rec.add_point("s1", t * 10);
            ASSERT_EQ(E_OK, writer.write(rec));
        }
        ASSERT_EQ(E_OK, writer.flush());
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1", "d2"}, {"s1"}, 0, -1, result));
    ASSERT_NE(result, nullptr);

    int d2_nonnull_count = 0;
    int64_t prev_ts = INT64_MIN;
    size_t total_rows = 0;
    bool has_next = false;
    while (IS_SUCC(result->next(has_next)) && has_next) {
        RowRecord* rr = result->get_row_record();
        int64_t t = rr->get_timestamp();

        EXPECT_GT(t, prev_ts)
            << "Non-monotonic timestamp: " << t << " after " << prev_ts;
        prev_ts = t;
        total_rows++;

        if (!result->is_null("d2.s1")) {
            EXPECT_GE(t, 50) << "d2 non-null at unexpected time " << t;
            EXPECT_LT(t, 60) << "d2 non-null at unexpected time " << t;
            d2_nonnull_count++;
        }
    }

    EXPECT_EQ(total_rows, 100u);      // d1 drives 100 rows
    EXPECT_EQ(d2_nonnull_count, 10);  // only chunk-d2-1 (t=50..59) emitted

    reader.destroy_query_data_set(result);
    reader.close();
}

// Same stale-chunk scenario but with offset/limit applied on top.
// offset=60, limit=10 -> rows t=60..69; d2 is null for all of them.
// Verifies that offset counting is not confused by the skipped stale chunk.
TEST_F(TreeQueryByRowTest, MultiPath_TimeHint_SkipsStaleChunk_WithOffset) {
    {
        TsFileTreeWriter writer(&write_file_);
        std::string d1 = "d1", d2 = "d2";
        auto* sd1 = new MeasurementSchema("s1", TSDataType::INT64);
        auto* sd2 = new MeasurementSchema("s1", TSDataType::INT64);
        ASSERT_EQ(E_OK, writer.register_timeseries(d1, sd1));
        ASSERT_EQ(E_OK, writer.register_timeseries(d2, sd2));
        delete sd1;
        delete sd2;

        for (int64_t t = 50; t < 60; ++t) {
            TsRecord rec("d2", t);
            rec.add_point("s1", t * 10);
            ASSERT_EQ(E_OK, writer.write(rec));
        }
        ASSERT_EQ(E_OK, writer.flush());

        for (int64_t t = 0; t < 100; ++t) {
            TsRecord rec("d1", t);
            rec.add_point("s1", t);
            ASSERT_EQ(E_OK, writer.write(rec));
        }
        ASSERT_EQ(E_OK, writer.flush());

        for (int64_t t = 0; t < 10; ++t) {
            TsRecord rec("d2", t);
            rec.add_point("s1", t * 10);
            ASSERT_EQ(E_OK, writer.write(rec));
        }
        ASSERT_EQ(E_OK, writer.flush());
        writer.close();
    }

    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));

    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.queryByRow({"d1", "d2"}, {"s1"}, 60, 10, result));
    ASSERT_NE(result, nullptr);

    auto ts = collect_timestamps(result);
    ASSERT_EQ(ts.size(), 10u);
    for (int i = 0; i < 10; ++i) EXPECT_EQ(ts[i], 60 + i);

    reader.destroy_query_data_set(result);
    reader.close();
}

// Pushdown is faster than full query + manual next: queryByRow(offset, limit)
// skips at Chunk/Page level; old query then manual next decodes every row.
// Timing tolerance 20% to allow measurement noise.
TEST_F(TreeQueryByRowTest, DISABLED_QueryByRowFasterThanManualNext) {
    std::vector<std::string> devices = {"d1"};
    std::vector<std::string> measurements = {"s1"};
    const int num_rows = 8000;
    const int offset = 3000;
    const int limit = 1000;
    write_test_file(devices, measurements, num_rows);

    const int num_iters = 5;
    const double tolerance = 0.2;

    auto run_query_by_row = [this, &devices, &measurements, offset, limit]() {
        TsFileTreeReader reader;
        if (reader.open(file_name_) != E_OK) return -1.0;
        ResultSet* rs = nullptr;
        if (reader.queryByRow(devices, measurements, offset, limit, rs) !=
            E_OK) {
            reader.close();
            return -1.0;
        }
        auto start = std::chrono::steady_clock::now();
        bool has_next = false;
        while (IS_SUCC(rs->next(has_next)) && has_next) {
            (void)rs->get_row_record()->get_timestamp();
        }
        auto end = std::chrono::steady_clock::now();
        reader.destroy_query_data_set(rs);
        reader.close();
        return std::chrono::duration<double, std::milli>(end - start).count();
    };

    auto run_manual_next = [this, &devices, &measurements, offset, limit]() {
        TsFileTreeReader reader;
        if (reader.open(file_name_) != E_OK) return -1.0;
        ResultSet* rs = nullptr;
        if (reader.query(devices, measurements, INT64_MIN, INT64_MAX, rs) !=
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
            (void)rs->get_row_record()->get_timestamp();
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
