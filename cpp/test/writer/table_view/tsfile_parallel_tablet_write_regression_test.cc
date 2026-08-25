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
// Regression tests for PR #909 / issue #908 bug 2: the parallel aligned
// tablet write path in TsFileWriter::write_table() submits per-device /
// per-column tasks to the global thread pool. The task lambdas used to
// capture the loop variables (ctx / vt) by reference; since the pool runs
// the tasks asynchronously after the submission loop advanced or exited,
// every task could read the same or out-of-scope state (use-after-scope).
// The fix captures the per-iteration addresses by value. These tests drive
// the parallel path (multiple devices x multiple value columns x enough
// rows to cross page boundaries) and verify every row survives round-trip.
#include <gtest/gtest.h>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <chrono>
#include <random>
#include <string>
#include <vector>

#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

using namespace storage;
using namespace common;

namespace {

class ParallelTabletWriteRegressionTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("tsfile_parallel_write_regression_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        write_file_.create(file_name_, flags, 0666);
    }
    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    std::string file_name_;
    WriteFile write_file_;

   public:
    static std::string generate_random_string(int length) {
        static std::atomic<uint64_t> counter{0};
        std::mt19937 gen(static_cast<unsigned int>(
            std::chrono::system_clock::now().time_since_epoch().count()));
        std::uniform_int_distribution<> dis(0, 61);
        const std::string chars =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        std::string random_string;
        for (int i = 0; i < length; ++i) {
            random_string += chars[dis(gen)];
        }
#ifdef _WIN32
        const auto process_id = static_cast<uint64_t>(_getpid());
#else
        const auto process_id = static_cast<uint64_t>(getpid());
#endif
        random_string += "_" + std::to_string(process_id) + "_" +
                         std::to_string(counter.fetch_add(1));
        return random_string;
    }

    // 1 TAG column (device id) + `field_col_num` INT64 field columns.
    static TableSchema* gen_table_schema(int field_col_num) {
        std::vector<MeasurementSchema*> measurement_schemas;
        std::vector<ColumnCategory> column_categories;
        measurement_schemas.emplace_back(
            new MeasurementSchema("id0", TSDataType::STRING, TSEncoding::PLAIN,
                                  CompressionType::UNCOMPRESSED));
        column_categories.emplace_back(ColumnCategory::TAG);
        for (int i = 0; i < field_col_num; i++) {
            measurement_schemas.emplace_back(new MeasurementSchema(
                "s" + std::to_string(i), TSDataType::INT64, TSEncoding::PLAIN,
                CompressionType::UNCOMPRESSED));
            column_categories.emplace_back(ColumnCategory::FIELD);
        }
        return new TableSchema("test_table", measurement_schemas,
                               column_categories);
    }

    // Fill a tablet with rows for `device_num` devices. Device d covers rows
    // [d * rows_per_device, (d+1) * rows_per_device). Field column c gets
    // value row * 100 + c so every cell is uniquely identifiable.
    static void gen_tablet(Tablet& tablet, TableSchema* table_schema,
                           int64_t time_base, int device_num,
                           int rows_per_device, int field_col_num) {
        tablet.set_table_name("test_table");
        PageArena pa;
        pa.init(512, MOD_DEFAULT);
        for (int d = 0; d < device_num; d++) {
            std::string device_str = "device_" + std::to_string(d);
            String literal_str(device_str, pa);
            for (int l = 0; l < rows_per_device; l++) {
                int row_index = d * rows_per_device + l;
                int64_t ts = time_base + row_index;
                ASSERT_EQ(tablet.add_timestamp(row_index, ts), E_OK);
                tablet.add_value(row_index, "id0", literal_str);
                for (int c = 0; c < field_col_num; c++) {
                    tablet.add_value(row_index, "s" + std::to_string(c),
                                     static_cast<int64_t>(row_index * 100 + c));
                }
            }
        }
    }
};

// Query the whole table back and verify every cell: row_count rows, tag
// column matching the owning device, and each field column carrying
// row_index * 100 + c.
// Query the table back and verify every cell. The reader returns rows
// grouped per device in device-id order (not tablet insertion order), so the
// check is keyed on the timestamp: every row was written with
// ts = time_base + row_index, field s<c> = row_index * 100 + c, and the tag
// column = "device_<row_index / rows_per_device>".
void VerifyTableRoundTrip(const std::string& file_name,
                          TableSchema* table_schema, int device_num,
                          int rows_per_device, int field_col_num,
                          int64_t time_base = 0) {
    TsFileReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name));
    ResultSet* tmp_result_set = nullptr;
    ASSERT_EQ(E_OK,
              reader.query("test_table", table_schema->get_measurement_names(),
                           0, INT64_MAX, tmp_result_set));
    auto* result_set = (TableResultSet*)tmp_result_set;
    std::vector<char> seen(static_cast<size_t>(device_num) * rows_per_device,
                           0);
    bool has_next = false;
    int64_t read_rows = 0;
    while (IS_SUCC(result_set->next(has_next)) && has_next) {
        const int64_t ts = result_set->get_value<int64_t>("time");
        ASSERT_GE(ts, time_base);
        const int64_t row_index = ts - time_base;
        ASSERT_LT(row_index, static_cast<int64_t>(device_num) * rows_per_device)
            << "timestamp out of written range: " << ts;
        ASSERT_EQ(seen[row_index], 0) << "duplicate row for timestamp " << ts;
        seen[row_index] = 1;

        const int device_idx = static_cast<int>(row_index / rows_per_device);
        char literal[32];
        snprintf(literal, sizeof(literal), "device_%d", device_idx);
        common::String* tag = result_set->get_value<common::String*>("id0");
        ASSERT_NE(tag, nullptr);
        EXPECT_EQ(0, tag->compare(String(literal, strlen(literal))))
            << "tag mismatch at timestamp " << ts;
        for (int c = 0; c < field_col_num; c++) {
            EXPECT_EQ(result_set->get_value<int64_t>("s" + std::to_string(c)),
                      row_index * 100 + c)
                << "field column s" << c << " corrupted at timestamp " << ts;
        }
        read_rows++;
    }
    const int64_t total_rows =
        static_cast<int64_t>(device_num) * rows_per_device;
    EXPECT_EQ(read_rows, total_rows)
        << "row loss: parallel aligned write dropped rows";
    reader.destroy_query_data_set(result_set);
    reader.close();
}

}  // namespace

// Core regression for the dangling-reference capture: enough devices and
// columns that the submission loop enqueues many tasks whose loop variables
// would alias (the old [&ctx] / [&vt] captures), plus enough rows to cross
// the 10000-point page boundary so page sealing also runs per task.
TEST_F(ParallelTabletWriteRegressionTest, MultiDeviceMultiColumnRoundTrip) {
    const int device_num = 8;
    const int rows_per_device = 1500;  // > page_writer_max_point_num_ (10000/8)
    const int field_col_num = 6;
    auto table_schema = gen_table_schema(field_col_num);
    auto writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    Tablet tablet(table_schema->get_measurement_names(),
                  table_schema->get_data_types(),
                  static_cast<uint32_t>(device_num * rows_per_device));
    gen_tablet(tablet, table_schema, 0, device_num, rows_per_device,
               field_col_num);
    ASSERT_EQ(E_OK, writer->write_table(tablet));
    ASSERT_EQ(E_OK, writer->flush());
    ASSERT_EQ(E_OK, writer->close());
    VerifyTableRoundTrip(file_name_, table_schema, device_num, rows_per_device,
                         field_col_num);
    delete table_schema;
}

// Single device, many columns: the per-column ValueTask loop is the inner
// one whose [&vt] capture dangled; with enough columns the tasks are queued
// well past the loop's lifetime.
TEST_F(ParallelTabletWriteRegressionTest, SingleDeviceManyColumnsRoundTrip) {
    const int device_num = 1;
    const int rows_per_device = 12000;  // crosses one page boundary
    const int field_col_num = 16;
    auto table_schema = gen_table_schema(field_col_num);
    auto writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    Tablet tablet(table_schema->get_measurement_names(),
                  table_schema->get_data_types(),
                  static_cast<uint32_t>(device_num * rows_per_device));
    gen_tablet(tablet, table_schema, 0, device_num, rows_per_device,
               field_col_num);
    ASSERT_EQ(E_OK, writer->write_table(tablet));
    ASSERT_EQ(E_OK, writer->flush());
    ASSERT_EQ(E_OK, writer->close());
    VerifyTableRoundTrip(file_name_, table_schema, device_num, rows_per_device,
                         field_col_num);
    delete table_schema;
}

// Many devices but few rows each: maximizes the number of DeviceWriteCtx
// entries whose vector reallocation would move the captured ctx references
// while queued tasks still hold them (the classic dangling scenario the fix
// addresses — device_ctxs grows via push_back as the loop progresses).
TEST_F(ParallelTabletWriteRegressionTest, ManyDevicesVectorReallocation) {
    const int device_num = 64;
    const int rows_per_device = 50;
    const int field_col_num = 4;
    auto table_schema = gen_table_schema(field_col_num);
    auto writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    Tablet tablet(table_schema->get_measurement_names(),
                  table_schema->get_data_types(),
                  static_cast<uint32_t>(device_num * rows_per_device));
    gen_tablet(tablet, table_schema, 0, device_num, rows_per_device,
               field_col_num);
    ASSERT_EQ(E_OK, writer->write_table(tablet));
    ASSERT_EQ(E_OK, writer->flush());
    ASSERT_EQ(E_OK, writer->close());
    VerifyTableRoundTrip(file_name_, table_schema, device_num, rows_per_device,
                         field_col_num);
    delete table_schema;
}

// Repeated write_table() calls on the same writer: each call builds a fresh
// device_ctxs vector on the stack, so previously-submitted tasks referencing
// the destroyed vector are exactly the use-after-scope hazard. Several
// sequential batches must all survive.
TEST_F(ParallelTabletWriteRegressionTest, SequentialBatchesRoundTrip) {
    const int device_num = 4;
    const int rows_per_device = 300;
    const int field_col_num = 5;
    const int batches = 6;
    auto table_schema = gen_table_schema(field_col_num);
    auto writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    for (int b = 0; b < batches; b++) {
        Tablet tablet(table_schema->get_measurement_names(),
                      table_schema->get_data_types(),
                      static_cast<uint32_t>(device_num * rows_per_device));
        gen_tablet(tablet, table_schema, 1000000 + b * 100000, device_num,
                   rows_per_device, field_col_num);
        ASSERT_EQ(E_OK, writer->write_table(tablet));
    }
    ASSERT_EQ(E_OK, writer->flush());
    ASSERT_EQ(E_OK, writer->close());

    // Each batch wrote the same relative row layout with different
    // timestamps; total rows = batches * device_num * rows_per_device.
    TsFileReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));
    ResultSet* tmp_result_set = nullptr;
    ASSERT_EQ(E_OK,
              reader.query("test_table", table_schema->get_measurement_names(),
                           0, INT64_MAX, tmp_result_set));
    auto* result_set = (TableResultSet*)tmp_result_set;
    bool has_next = false;
    int64_t row_num = 0;
    while (IS_SUCC(result_set->next(has_next)) && has_next) {
        row_num++;
    }
    EXPECT_EQ(row_num,
              static_cast<int64_t>(batches) * device_num * rows_per_device)
        << "row loss across sequential parallel batches";
    reader.destroy_query_data_set(result_set);
    reader.close();
    delete table_schema;
}

// Interleaved flushes between batches: the parallel path runs while earlier
// chunk groups are already sealed, and the value writers reused across
// batches carry non-zero initial_page_points (partial pages). The dangling
// capture could make tasks read the wrong ctx.initial_page_points and
// mis-align page boundaries; the round-trip check catches that.
TEST_F(ParallelTabletWriteRegressionTest, FlushBetweenBatchesRoundTrip) {
    const int device_num = 3;
    const int rows_per_device = 800;
    const int field_col_num = 3;
    const int batches = 4;
    auto table_schema = gen_table_schema(field_col_num);
    auto writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    for (int b = 0; b < batches; b++) {
        Tablet tablet(table_schema->get_measurement_names(),
                      table_schema->get_data_types(),
                      static_cast<uint32_t>(device_num * rows_per_device));
        gen_tablet(tablet, table_schema, 500000 + b * 100000, device_num,
                   rows_per_device, field_col_num);
        ASSERT_EQ(E_OK, writer->write_table(tablet));
        ASSERT_EQ(E_OK, writer->flush());
    }
    ASSERT_EQ(E_OK, writer->close());

    TsFileReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));
    ResultSet* tmp_result_set = nullptr;
    ASSERT_EQ(E_OK,
              reader.query("test_table", table_schema->get_measurement_names(),
                           0, INT64_MAX, tmp_result_set));
    auto* result_set = (TableResultSet*)tmp_result_set;
    bool has_next = false;
    int64_t row_num = 0;
    while (IS_SUCC(result_set->next(has_next)) && has_next) {
        row_num++;
    }
    EXPECT_EQ(row_num,
              static_cast<int64_t>(batches) * device_num * rows_per_device)
        << "row loss with flush between parallel batches";
    reader.destroy_query_data_set(result_set);
    reader.close();
    delete table_schema;
}

// ===== Adversarial additions =====

// Small page size (8 points) makes every task seal pages repeatedly on the
// pool threads and drives the initial_page_points continuation logic hard:
// rows_per_device=50 crosses 6 page boundaries per column, and the
// batch-to-batch continuation keeps partial pages live across write_table
// calls.
TEST_F(ParallelTabletWriteRegressionTest, TinyPageBoundaryRoundTrip) {
    const int prev_page_point_num =
        common::g_config_value_.page_writer_max_point_num_;
    common::g_config_value_.page_writer_max_point_num_ = 8;

    const int device_num = 4;
    const int rows_per_device = 50;
    const int field_col_num = 3;
    auto table_schema = gen_table_schema(field_col_num);
    auto writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    const int batches = 3;
    for (int b = 0; b < batches; b++) {
        Tablet tablet(table_schema->get_measurement_names(),
                      table_schema->get_data_types(),
                      static_cast<uint32_t>(device_num * rows_per_device));
        gen_tablet(tablet, table_schema, 900000 + b * 100000, device_num,
                   rows_per_device, field_col_num);
        ASSERT_EQ(E_OK, writer->write_table(tablet));
    }
    ASSERT_EQ(E_OK, writer->flush());
    ASSERT_EQ(E_OK, writer->close());

    // Verify every batch: rows for batch b carry ts in [base, base+total).
    TsFileReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));
    for (int b = 0; b < batches; b++) {
        const int64_t base = 900000 + b * 100000;
        // Per-batch duplicate tracking: row_index is batch-relative.
        std::vector<char> seen(static_cast<size_t>(device_num) *
                               rows_per_device);
        ResultSet* tmp_result_set = nullptr;
        ASSERT_EQ(E_OK,
                  reader.query(
                      "test_table", table_schema->get_measurement_names(), base,
                      base + device_num * rows_per_device - 1, tmp_result_set));
        auto* result_set = (TableResultSet*)tmp_result_set;
        bool has_next = false;
        while (IS_SUCC(result_set->next(has_next)) && has_next) {
            const int64_t ts = result_set->get_value<int64_t>("time");
            const int64_t row_index = ts - base;
            ASSERT_GE(row_index, 0);
            ASSERT_LT(row_index,
                      static_cast<int64_t>(device_num) * rows_per_device);
            ASSERT_EQ(seen[row_index], 0)
                << "duplicate row at timestamp " << ts;
            seen[row_index] = 1;
            const int device_idx =
                static_cast<int>(row_index / rows_per_device);
            char literal[32];
            snprintf(literal, sizeof(literal), "device_%d", device_idx);
            common::String* tag = result_set->get_value<common::String*>("id0");
            ASSERT_NE(tag, nullptr);
            EXPECT_EQ(0, tag->compare(String(literal, strlen(literal))));
            for (int c = 0; c < field_col_num; c++) {
                EXPECT_EQ(
                    result_set->get_value<int64_t>("s" + std::to_string(c)),
                    row_index * 100 + c)
                    << "field s" << c << " corrupted at ts " << ts;
            }
        }
        reader.destroy_query_data_set(result_set);
    }
    reader.close();
    delete table_schema;

    common::g_config_value_.page_writer_max_point_num_ = prev_page_point_num;
}

// Thread-pool boundary configs: a 1-thread pool serializes the tasks on one
// worker (worst case for the old same-slot aliasing), and a larger pool
// runs them concurrently. Both must round-trip every row.
TEST_F(ParallelTabletWriteRegressionTest, ThreadCountBoundariesRoundTrip) {
    for (int threads : {1, 8}) {
        ASSERT_EQ(E_OK, set_thread_count(threads));
        // set_thread_count rebuilds the global pool; TsFileTableWriter is
        // constructed per iteration so no writer holds state across the
        // rebuild.
        WriteFile write_file;
        std::string file_name =
            std::string("tsfile_parallel_write_regression_thr") +
            std::to_string(threads) + "_" + generate_random_string(8) +
            ".tsfile";
        remove(file_name.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        write_file.create(file_name, flags, 0666);

        const int device_num = 6;
        const int rows_per_device = 400;
        const int field_col_num = 4;
        auto table_schema = gen_table_schema(field_col_num);
        auto writer =
            std::make_shared<TsFileTableWriter>(&write_file, table_schema);
        Tablet tablet(table_schema->get_measurement_names(),
                      table_schema->get_data_types(),
                      static_cast<uint32_t>(device_num * rows_per_device));
        gen_tablet(tablet, table_schema, 0, device_num, rows_per_device,
                   field_col_num);
        ASSERT_EQ(E_OK, writer->write_table(tablet));
        ASSERT_EQ(E_OK, writer->flush());
        ASSERT_EQ(E_OK, writer->close());
        VerifyTableRoundTrip(file_name, table_schema, device_num,
                             rows_per_device, field_col_num);
        delete table_schema;
        ASSERT_EQ(0, remove(file_name.c_str()));
    }
    // Restore the default pool size for the rest of the suite.
    ASSERT_EQ(E_OK, set_thread_count(6));
}
