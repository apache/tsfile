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
 *
 * Performance comparison (4 groups):
 * - Tree model: query single time series / multi time series
 * - Table model: query single time series / multi time series
 *
 * Dataset:
 * - points per time series = 50000 (a time series = device_id + measurement_id)
 * - offset default = 25000, limit default = 1000
 * - random "none" (NULL / missing values) to make the data non-dense
 *
 * Run (suite is DISABLED by default; omit DISABLED_ in filter when using
 * --gtest_also_run_disabled_tests):
 *   TsFile_Test --gtest_also_run_disabled_tests
 * --gtest_filter=DISABLED_QueryByRowPerformance*
 *
 * Dynamic offset (optional):
 *   QUERY_BY_ROW_PERF_OFFSET=<absolute offset>
 *   QUERY_BY_ROW_PERF_OFFSET_RATIO=<percent [0..100] of num_rows>
 *
 * Optional output aggregation:
 *   QUERY_BY_ROW_PERF_RESULT=/path/to/result.md (append)
 *
 * More iterations for stabler avg (default 5):
 *   QUERY_BY_ROW_PERF_ITERS=30
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "common/global.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"

using namespace storage;
using namespace common;

static bool get_env_int(const char* name, int& out) {
    const char* v = std::getenv(name);
    if (v == nullptr || v[0] == '\0') return false;
    char* end = nullptr;
    long x = std::strtol(v, &end, 10);
    if (end == v) return false;
    out = static_cast<int>(x);
    return true;
}

/** Average latency iterations per (offset,limit) case; clamp [1,200]. */
static int query_by_row_perf_iters() {
    int n = 100;
    if (get_env_int("QUERY_BY_ROW_PERF_ITERS", n)) {
        if (n < 1) n = 1;
        if (n > 200) n = 200;
    }
    return n;
}

static int compute_offset_with_env(int num_rows, int default_offset) {
    int offset = default_offset;
    int abs = 0;
    if (get_env_int("QUERY_BY_ROW_PERF_OFFSET", abs)) {
        offset = abs;
    } else {
        int ratio = 0;
        if (get_env_int("QUERY_BY_ROW_PERF_OFFSET_RATIO", ratio)) {
            if (ratio < 0) ratio = 0;
            if (ratio > 100) ratio = 100;
            offset =
                static_cast<int>(static_cast<int64_t>(num_rows) * ratio / 100);
        }
    }

    if (num_rows <= 0) return 0;
    if (offset < 0) offset = 0;
    if (offset >= num_rows) offset = num_rows - 1;
    return offset;
}

static void write_result_if_needed(const std::string& md) {
    const char* result_path = std::getenv("QUERY_BY_ROW_PERF_RESULT");
    if (result_path == nullptr || result_path[0] == '\0') return;
    std::ofstream f(result_path, std::ios::app);
    if (f) f << md << "\n";
}

// Entire suite skipped in default runs
class DISABLED_QueryByRowPerformanceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("query_by_row_perf_") +
                     generate_random_string(8) + std::string(".tsfile");
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

    void write_tree_multi_device_file(
        int num_rows_total, int device_count,
        const std::vector<std::string>& measurement_ids, double none_prob_s1,
        double none_prob_s2, uint32_t seed) {
        TsFileTreeWriter writer(&write_file_);

        // Register all selected measurements (only those in measurement_ids).
        std::vector<std::string> device_ids;
        for (int d = 0; d < device_count; ++d) {
            device_ids.push_back("d" + std::to_string(d));
        }
        for (auto& device_id : device_ids) {
            for (const auto& measurement_id : measurement_ids) {
                auto* schema =
                    new MeasurementSchema(measurement_id, TSDataType::INT64);
                std::string device_id_nc =
                    device_id;  // register_timeseries needs std::string&
                ASSERT_EQ(E_OK,
                          writer.register_timeseries(device_id_nc, schema));
                delete schema;
            }
        }

        std::mt19937 rng(seed);
        std::uniform_real_distribution<double> u01(0.0, 1.0);

        // Ensure every (device_id + measurement_id) time series has exactly
        // num_rows_total points at timestamps [0, num_rows_total).
        for (int64_t timestamp = 0; timestamp < num_rows_total; ++timestamp) {
            for (const auto& device_id : device_ids) {
                TsRecord record(timestamp, device_id,
                                static_cast<int32_t>(measurement_ids.size()));
                for (size_t m = 0; m < measurement_ids.size(); ++m) {
                    const auto& measurement_id = measurement_ids[m];
                    bool make_null = false;
                    if (measurement_id == "s1") {
                        make_null = (u01(rng) < none_prob_s1);
                    } else if (measurement_id == "s2") {
                        make_null = (u01(rng) < none_prob_s2);
                    }

                    if (make_null) {
                        // DataPoint(measurement_name) creates a NULL point.
                        record.points_.emplace_back(DataPoint(measurement_id));
                    } else {
                        // Make values unique per (timestamp, measurement) to
                        // prevent constant folding.
                        int64_t value =
                            timestamp * 100 + static_cast<int64_t>(m);
                        record.add_point(measurement_id, value);
                    }
                }

                ASSERT_EQ(E_OK, writer.write(record));
            }
        }
        writer.flush();
        writer.close();
    }

    void write_table_multi_device_file(int num_rows_total, int device_count,
                                       double none_prob_s1, double none_prob_s2,
                                       uint32_t seed) {
        // Schema always contains both s1 and s2; single-sequence tests will
        // only query s1.
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

        // Each device has num_rows_total points, and timestamps are aligned
        // across devices.
        const int num_total_rows = num_rows_total * device_count;
        Tablet tablet(
            "t1", {"id1", "s1", "s2"},
            {TSDataType::STRING, TSDataType::INT64, TSDataType::INT64},
            {ColumnCategory::TAG, ColumnCategory::FIELD, ColumnCategory::FIELD},
            num_total_rows);
        std::mt19937 rng(seed);
        std::uniform_real_distribution<double> u01(0.0, 1.0);

        for (int device_index = 0; device_index < device_count;
             ++device_index) {
            for (int row_in_device = 0; row_in_device < num_rows_total;
                 ++row_in_device) {
                const int row = device_index * num_rows_total + row_in_device;
                const int64_t timestamp = static_cast<int64_t>(row_in_device);

                tablet.add_timestamp(row, timestamp);
                tablet.add_value(row, "id1",
                                 "device_" + std::to_string(device_index));

                if (u01(rng) >= none_prob_s1) {
                    tablet.add_value(row, "s1", timestamp * 10);
                }
                if (u01(rng) >= none_prob_s2) {
                    tablet.add_value(row, "s2", timestamp * 100);
                }
            }
        }

        ASSERT_EQ(writer->write_table(tablet), E_OK);
        ASSERT_EQ(writer->flush(), E_OK);
        ASSERT_EQ(writer->close(), E_OK);
        delete writer;
        delete schema;
    }

    std::string file_name_;
    WriteFile write_file_;
};

static const int kNumRowsTotal = 50000;  // points per time series
static const int kDeviceCount =
    1;  // keep "offset/limit" aligned to a single time series

struct OffsetLimitCase {
    int offset;
    int limit;
    const char* label;
};

// Multiple (offset, limit) groups for performance analysis.
// Notes:
// - The dataset has offsets in [0, kNumRowsTotal-1]. (50000, 1000) means
//   "offset beyond end" and should return empty results for that series.
static const OffsetLimitCase kOffsetLimitCases[] = {
    {0, 1000, "(0,1000)"},           {12500, 1000, "(12500,1000)"},
    {25000, 1000, "(25000,1000)"},   {37500, 1000, "(37500,1000)"},
    {50000, 1000, "(50000,1000)"},   {25000, 10, "(25000,10)"},
    {25000, 100, "(25000,100)"},     {25000, 1000, "(25000,1000)"},
    {25000, 10000, "(25000,10000)"},
};

static const int kOffsetLimitCaseCount =
    static_cast<int>(sizeof(kOffsetLimitCases) / sizeof(kOffsetLimitCases[0]));

// Keep dataset fully dense (no NULL) to maximize offset/limit pushdown effects.
static const double kNoneProbSingle = 0.0;   // s1 in single-sequence
static const double kNoneProbS2 = 0.0;       // s2 in multi-sequence
static const double kNoneProbMultiS1 = 0.0;  // s1 in multi-sequence

template <typename RunByRowFn, typename RunManualFn>
static void compute_avg_times(RunByRowFn&& run_by_row, RunManualFn&& run_manual,
                              int iters, double& avg_by_row, double& avg_manual,
                              int& valid_iters) {
    double sum_by_row = 0.0;
    double sum_manual = 0.0;
    valid_iters = 0;
    for (int i = 0; i < iters; i++) {
        const double t1 = run_by_row();
        const double t2 = run_manual();
        if (t1 > 0 && t2 > 0) {
            sum_by_row += t1;
            sum_manual += t2;
            valid_iters++;
        }
    }
    avg_by_row = (valid_iters > 0) ? (sum_by_row / valid_iters) : -1.0;
    avg_manual = (valid_iters > 0) ? (sum_manual / valid_iters) : -1.0;
}

TEST_F(DISABLED_QueryByRowPerformanceTest, TreeModel_SingleSequence) {
    const std::vector<std::string> measurement_ids = {"s1"};
    write_tree_multi_device_file(kNumRowsTotal, kDeviceCount, measurement_ids,
                                 kNoneProbSingle, /*none_prob_s2=*/0.0, 123);
    std::vector<std::string> devices{"d0"};

    const int perf_iters = query_by_row_perf_iters();
    std::ostringstream out;
    out << "# QueryByRow (Tree, single) vs Manual Next – Performance Result\n\n"
        << "Avg iterations per cell: **" << perf_iters
        << "** (`QUERY_BY_ROW_PERF_ITERS`)\n\n"
        << "| Case | Offset | Limit | queryByRow(avg ms) | Manual(avg ms) | "
           "Speedup |\n"
        << "|------|--------|-------|-------------------|--------------|-------"
           "--|\n";

    double best_speedup = 0.0;
    for (int c = 0; c < kOffsetLimitCaseCount; ++c) {
        const OffsetLimitCase& cs = kOffsetLimitCases[c];
        const int offset = cs.offset;
        const int limit = cs.limit;

        auto run_query_by_row = [this, &devices, &measurement_ids, offset,
                                 limit]() {
            TsFileTreeReader reader;
            if (reader.open(file_name_) != E_OK) return -1.0;
            ResultSet* rs = nullptr;
            if (reader.queryByRow(devices, measurement_ids, offset, limit,
                                  rs) != E_OK) {
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
            return std::chrono::duration<double, std::milli>(end - start)
                .count();
        };

        auto run_manual_next = [this, &devices, &measurement_ids, offset,
                                limit]() {
            TsFileTreeReader reader;
            if (reader.open(file_name_) != E_OK) return -1.0;
            ResultSet* rs = nullptr;
            if (reader.query(devices, measurement_ids, INT64_MIN, INT64_MAX,
                             rs) != E_OK) {
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
            return std::chrono::duration<double, std::milli>(end - start)
                .count();
        };

        double avg_by_row = -1.0;
        double avg_manual = -1.0;
        int valid_iters = 0;
        compute_avg_times(run_query_by_row, run_manual_next, perf_iters,
                          avg_by_row, avg_manual, valid_iters);
        ASSERT_GT(valid_iters, 0);
        ASSERT_GT(avg_manual, 0.0);

        const double speedup =
            (avg_by_row > 0.0) ? (avg_manual / avg_by_row) : 0.0;
        best_speedup = std::max(best_speedup, speedup);

        out << "| " << cs.label << " | " << offset << " | " << limit << " | "
            << avg_by_row << " | " << avg_manual << " | " << speedup << "x |\n";
    }

    out << "\n";
    std::cout << "\n" << out.str() << "\n";
    write_result_if_needed(out.str());
    EXPECT_GT(best_speedup, 1.0);
}

TEST_F(DISABLED_QueryByRowPerformanceTest, TreeModel_MultiSequence) {
    const std::vector<std::string> measurement_ids = {"s1", "s2"};
    write_tree_multi_device_file(kNumRowsTotal, kDeviceCount, measurement_ids,
                                 kNoneProbMultiS1, kNoneProbS2, 456);
    std::vector<std::string> devices;
    for (int d = 0; d < kDeviceCount; ++d)
        devices.push_back("d" + std::to_string(d));

    const int perf_iters = query_by_row_perf_iters();
    std::ostringstream out;
    out << "# QueryByRow (Tree, multi) vs Manual Next – Performance Result\n\n"
        << "Avg iterations per cell: **" << perf_iters
        << "** (`QUERY_BY_ROW_PERF_ITERS`)\n\n"
        << "| Case | Offset | Limit | queryByRow(avg ms) | Manual(avg ms) | "
           "Speedup |\n"
        << "|------|--------|-------|-------------------|--------------|-------"
           "--|\n";

    double best_speedup = 0.0;
    for (int c = 0; c < kOffsetLimitCaseCount; ++c) {
        const OffsetLimitCase& cs = kOffsetLimitCases[c];
        const int offset = cs.offset;
        const int limit = cs.limit;

        auto run_query_by_row = [this, &devices, &measurement_ids, offset,
                                 limit]() {
            TsFileTreeReader reader;
            if (reader.open(file_name_) != E_OK) return -1.0;
            ResultSet* rs = nullptr;
            if (reader.queryByRow(devices, measurement_ids, offset, limit,
                                  rs) != E_OK) {
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
            return std::chrono::duration<double, std::milli>(end - start)
                .count();
        };

        auto run_manual_next = [this, &devices, &measurement_ids, offset,
                                limit]() {
            TsFileTreeReader reader;
            if (reader.open(file_name_) != E_OK) return -1.0;
            ResultSet* rs = nullptr;
            if (reader.query(devices, measurement_ids, INT64_MIN, INT64_MAX,
                             rs) != E_OK) {
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
            return std::chrono::duration<double, std::milli>(end - start)
                .count();
        };

        double avg_by_row = -1.0;
        double avg_manual = -1.0;
        int valid_iters = 0;
        compute_avg_times(run_query_by_row, run_manual_next, perf_iters,
                          avg_by_row, avg_manual, valid_iters);
        ASSERT_GT(valid_iters, 0);
        ASSERT_GT(avg_manual, 0.0);

        const double speedup =
            (avg_by_row > 0.0) ? (avg_manual / avg_by_row) : 0.0;
        best_speedup = std::max(best_speedup, speedup);

        out << "| " << cs.label << " | " << offset << " | " << limit << " | "
            << avg_by_row << " | " << avg_manual << " | " << speedup << "x |\n";
    }

    out << "\n";
    std::cout << "\n" << out.str() << "\n";
    write_result_if_needed(out.str());
    EXPECT_GT(best_speedup, 1.0);
}

TEST_F(DISABLED_QueryByRowPerformanceTest, TableModel_SingleSequence) {
    write_table_multi_device_file(kNumRowsTotal, kDeviceCount, kNoneProbSingle,
                                  0.0, 789);
    const std::vector<std::string> cols = {"id1", "s1"};

    const int perf_iters = query_by_row_perf_iters();
    std::ostringstream out;
    out << "# QueryByRow (Table, single) vs Manual Next – Performance "
           "Result\n\n"
        << "Avg iterations per cell: **" << perf_iters
        << "** (`QUERY_BY_ROW_PERF_ITERS`)\n\n"
        << "| Case | Offset | Limit | queryByRow(avg ms) | Manual(avg ms) | "
           "Speedup |\n"
        << "|------|--------|-------|-------------------|--------------|-------"
           "--|\n";

    double best_speedup = 0.0;
    for (int c = 0; c < kOffsetLimitCaseCount; ++c) {
        const OffsetLimitCase& cs = kOffsetLimitCases[c];
        const int offset = cs.offset;
        const int limit = cs.limit;

        auto run_query_by_row = [this, cols, offset, limit]() {
            TsFileReader reader;
            if (reader.open(file_name_) != E_OK) return -1.0;
            ResultSet* rs = nullptr;
            if (reader.queryByRow("t1", cols, offset, limit, rs) != E_OK) {
                reader.close();
                return -1.0;
            }
            auto start = std::chrono::steady_clock::now();
            bool has_next = false;
            while (IS_SUCC(rs->next(has_next)) && has_next) {
                if (!rs->is_null("s1")) {
                    (void)rs->get_value<int64_t>("s1");
                }
            }
            auto end = std::chrono::steady_clock::now();
            reader.destroy_query_data_set(rs);
            reader.close();
            return std::chrono::duration<double, std::milli>(end - start)
                .count();
        };

        auto run_manual_next = [this, cols, offset, limit]() {
            TsFileReader reader;
            if (reader.open(file_name_) != E_OK) return -1.0;
            ResultSet* rs = nullptr;
            if (reader.query("t1", cols, INT64_MIN, INT64_MAX, rs) != E_OK) {
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
                if (!rs->is_null("s1")) {
                    (void)rs->get_value<int64_t>("s1");
                }
                taken++;
            }
            auto end = std::chrono::steady_clock::now();
            reader.destroy_query_data_set(rs);
            reader.close();
            return std::chrono::duration<double, std::milli>(end - start)
                .count();
        };

        double avg_by_row = -1.0;
        double avg_manual = -1.0;
        int valid_iters = 0;
        compute_avg_times(run_query_by_row, run_manual_next, perf_iters,
                          avg_by_row, avg_manual, valid_iters);
        ASSERT_GT(valid_iters, 0);
        ASSERT_GT(avg_manual, 0.0);

        const double speedup =
            (avg_by_row > 0.0) ? (avg_manual / avg_by_row) : 0.0;
        best_speedup = std::max(best_speedup, speedup);

        out << "| " << cs.label << " | " << offset << " | " << limit << " | "
            << avg_by_row << " | " << avg_manual << " | " << speedup << "x |\n";
    }

    out << "\n";
    std::cout << "\n" << out.str() << "\n";
    write_result_if_needed(out.str());
    EXPECT_GT(best_speedup, 1.0);
}

TEST_F(DISABLED_QueryByRowPerformanceTest, TableModel_MultiSequence) {
    write_table_multi_device_file(kNumRowsTotal, kDeviceCount, kNoneProbMultiS1,
                                  kNoneProbS2, 101);
    const std::vector<std::string> cols = {"id1", "s1", "s2"};

    const int perf_iters = query_by_row_perf_iters();
    std::ostringstream out;
    out << "# QueryByRow (Table, multi) vs Manual Next – Performance Result\n\n"
        << "Avg iterations per cell: **" << perf_iters
        << "** (`QUERY_BY_ROW_PERF_ITERS`)\n\n"
        << "| Case | Offset | Limit | queryByRow(avg ms) | Manual(avg ms) | "
           "Speedup |\n"
        << "|------|--------|-------|-------------------|--------------|-------"
           "--|\n";

    double best_speedup = 0.0;
    for (int c = 0; c < kOffsetLimitCaseCount; ++c) {
        const OffsetLimitCase& cs = kOffsetLimitCases[c];
        const int offset = cs.offset;
        const int limit = cs.limit;

        auto run_query_by_row = [this, cols, offset, limit]() {
            TsFileReader reader;
            if (reader.open(file_name_) != E_OK) return -1.0;
            ResultSet* rs = nullptr;
            if (reader.queryByRow("t1", cols, offset, limit, rs) != E_OK) {
                reader.close();
                return -1.0;
            }
            auto start = std::chrono::steady_clock::now();
            bool has_next = false;
            while (IS_SUCC(rs->next(has_next)) && has_next) {
                if (!rs->is_null("s1")) (void)rs->get_value<int64_t>("s1");
                (void)rs->is_null("s2");
            }
            auto end = std::chrono::steady_clock::now();
            reader.destroy_query_data_set(rs);
            reader.close();
            return std::chrono::duration<double, std::milli>(end - start)
                .count();
        };

        auto run_manual_next = [this, cols, offset, limit]() {
            TsFileReader reader;
            if (reader.open(file_name_) != E_OK) return -1.0;
            ResultSet* rs = nullptr;
            if (reader.query("t1", cols, INT64_MIN, INT64_MAX, rs) != E_OK) {
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
                if (!rs->is_null("s1")) (void)rs->get_value<int64_t>("s1");
                (void)rs->is_null("s2");
                taken++;
            }
            auto end = std::chrono::steady_clock::now();
            reader.destroy_query_data_set(rs);
            reader.close();
            return std::chrono::duration<double, std::milli>(end - start)
                .count();
        };

        double avg_by_row = -1.0;
        double avg_manual = -1.0;
        int valid_iters = 0;
        compute_avg_times(run_query_by_row, run_manual_next, perf_iters,
                          avg_by_row, avg_manual, valid_iters);
        ASSERT_GT(valid_iters, 0);
        ASSERT_GT(avg_manual, 0.0);

        const double speedup =
            (avg_by_row > 0.0) ? (avg_manual / avg_by_row) : 0.0;
        best_speedup = std::max(best_speedup, speedup);

        out << "| " << cs.label << " | " << offset << " | " << limit << " | "
            << avg_by_row << " | " << avg_manual << " | " << speedup << "x |\n";
    }

    out << "\n";
    std::cout << "\n" << out.str() << "\n";
    write_result_if_needed(out.str());
    EXPECT_GT(best_speedup, 1.0);
}
