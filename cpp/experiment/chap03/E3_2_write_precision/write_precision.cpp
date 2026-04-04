/*
 * E3-2: Write memory formula precision with PLAIN + UNCOMPRESSED.
 *
 * Without encoding/compression, data memory should be deterministic:
 *   M_data ≈ s_data × F   (F = rows accumulated before flush)
 *
 * For each test batch_size:
 *   1. Create a fresh writer with PLAIN + UNCOMPRESSED fields
 *   2. Write batch_size rows for one device (no flush)
 *   3. Measure: calculate_mem_size_for_all_group() → M_data_direct
 *   4. Compare with formula: s_data × batch_size
 *
 * After precision tests, writes a full dataset for the reader experiment.
 *
 * Schema: same 8-FIELD "mem_bench" table, but PLAIN + UNCOMPRESSED.
 *   s_data = 8 (time) + (4+4+8+8+4+4+8+8) = 56 bytes/row
 *   b      = 8 * 104 + 96 = 928 bytes/device/flush
 *
 * Build:
 *   cmake -DBUILD_TEST=OFF ..          (ENABLE_MEM_STAT optional)
 *   cmake --build . --target write_precision
 *
 * Usage:
 *   ./write_precision [csv_path] [tsfile_path] [full_total_rows]
 */

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <chrono>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "common/config/config.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "writer/tsfile_writer.h"

#ifdef ENABLE_MEM_STAT
#include "common/allocator/alloc_base.h"
#endif

using namespace storage;
using namespace common;

static const char* kTable = "mem_bench";
static const int kNumDevices = 10;
static const int64_t kSData = 56;  // bytes per row (aligned)
static const int64_t kB = 928;     // meta bytes per device per flush

static std::string device_name(int i) { return "device_" + std::to_string(i); }

// Build PLAIN + UNCOMPRESSED table schema
static std::shared_ptr<TableSchema> make_plain_schema() {
    return std::make_shared<TableSchema>(
        std::string(kTable), std::vector<ColumnSchema>{
                                 ColumnSchema("id1", STRING, UNCOMPRESSED,
                                              PLAIN, ColumnCategory::TAG),
                                 ColumnSchema("id2", STRING, UNCOMPRESSED,
                                              PLAIN, ColumnCategory::TAG),
                                 ColumnSchema("s1", INT32, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s2", INT32, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s3", INT64, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s4", INT64, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s5", FLOAT, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s6", FLOAT, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s7", DOUBLE, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s8", DOUBLE, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                             });
}

static Tablet make_tablet(uint32_t n) {
    return Tablet(
        kTable, {"id1", "id2", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"},
        {STRING, STRING, INT32, INT32, INT64, INT64, FLOAT, FLOAT, DOUBLE,
         DOUBLE},
        {ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::FIELD,
         ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD,
         ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD,
         ColumnCategory::FIELD},
        std::max(n, 1u));
}

static void fill_row(Tablet& tablet, uint32_t row, int64_t ts,
                     const std::string& dev_id, std::mt19937_64& rng) {
    std::uniform_int_distribution<int32_t> ni(-100, 100);
    std::uniform_real_distribution<float> nf(-5.0f, 5.0f);
    std::uniform_real_distribution<double> nd(-0.5, 0.5);

    tablet.add_timestamp(row, ts);
    tablet.add_value(row, "id1", dev_id.c_str());
    tablet.add_value(row, "id2", "tag_b");
    tablet.add_value(row, "s1", static_cast<int32_t>(ts % 100000) + ni(rng));
    tablet.add_value(row, "s2", static_cast<int32_t>(ts % 50000) + ni(rng));
    tablet.add_value(row, "s3", ts + static_cast<int64_t>(ni(rng)));
    tablet.add_value(row, "s4", ts * 2 + static_cast<int64_t>(ni(rng)));
    tablet.add_value(row, "s5", static_cast<float>(ts % 10000) + nf(rng));
    tablet.add_value(row, "s6", static_cast<float>(ts % 5000) + nf(rng));
    tablet.add_value(row, "s7", ts * 1.1 + nd(rng));
    tablet.add_value(row, "s8", ts * 0.5 + nd(rng));
}

// -----------------------------------------------------------------------
// Phase 1: Precision measurement
//   For each test batch_size, write exactly that many rows and compare
//   direct measurement vs formula.
// -----------------------------------------------------------------------
struct PrecisionResult {
    int64_t batch_size;
    int64_t m_data_direct;   // calculate_mem_size_for_all_group()
    int64_t m_data_formula;  // s_data * batch_size
    int64_t m_meta_direct;   // calculate_meta_mem_size()
    int64_t m_meta_formula;  // 1 device * 0 flushes = 0 (no flush yet)
    double error_pct;
};

static PrecisionResult measure_one(int64_t batch_size,
                                   const std::string& tmp_path) {
    // Disable built-in auto-flush
    g_config_value_.chunk_group_size_threshold_ = INT64_MAX / 2;

    WriteFile wf;
    int ret = wf.create(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (ret != 0) {
        std::cerr << "create file failed: " << ret << "\n";
        exit(1);
    }

    auto schema = make_plain_schema();
    TsFileWriter writer;
    writer.init(&wf);
    writer.register_table(schema);

    // Write in chunks of up to 65536 rows
    std::mt19937_64 rng(42);
    std::string dev_id = device_name(0);
    int64_t remaining = batch_size;
    int64_t ts = 0;

    while (remaining > 0) {
        uint32_t n = static_cast<uint32_t>(std::min<int64_t>(65536, remaining));
        auto tablet = make_tablet(n);
        for (uint32_t i = 0; i < n; i++) {
            fill_row(tablet, i, ts++, dev_id, rng);
        }
        ret = writer.write_table(tablet);
        if (ret != 0) {
            std::cerr << "write_table failed: " << ret << "\n";
            exit(1);
        }
        remaining -= n;
    }

    // Measure before flush
    int64_t m_data = writer.calculate_mem_size_for_all_group();
    int64_t m_meta = writer.calculate_meta_mem_size();
    int64_t m_formula = kSData * batch_size;

    double err =
        m_formula > 0 ? 100.0 * std::abs(m_data - m_formula) / m_formula : 0.0;

    writer.flush();
    writer.close();

    PrecisionResult r;
    r.batch_size = batch_size;
    r.m_data_direct = m_data;
    r.m_data_formula = m_formula;
    r.m_meta_direct = m_meta;
    r.m_meta_formula = 0;  // no flush happened yet
    r.error_pct = err;
    return r;
}

// -----------------------------------------------------------------------
// Phase 2: Write a full dataset for reader experiment
// -----------------------------------------------------------------------
static void write_full_dataset(const std::string& path, int64_t total_rows) {
    g_config_value_.chunk_group_size_threshold_ = INT64_MAX / 2;

    WriteFile wf;
    int ret = wf.create(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (ret != 0) {
        std::cerr << "create file failed: " << ret << "\n";
        exit(1);
    }

    auto schema = make_plain_schema();
    TsFileWriter writer;
    writer.init(&wf);
    writer.register_table(schema);

    std::mt19937_64 rng(42);
    int64_t rows_per_dev = total_rows / kNumDevices;
    int64_t rows_written = 0;

    using clock = std::chrono::high_resolution_clock;
    auto t_start = clock::now();

    for (int dev = 0; dev < kNumDevices; dev++) {
        std::string dev_id = device_name(dev);
        int64_t dev_base = static_cast<int64_t>(dev) * rows_per_dev;

        for (int64_t off = 0; off < rows_per_dev;) {
            uint32_t n = static_cast<uint32_t>(
                std::min<int64_t>(65536, rows_per_dev - off));
            auto tablet = make_tablet(n);
            for (uint32_t i = 0; i < n; i++) {
                fill_row(tablet, i, dev_base + off + i, dev_id, rng);
            }
            ret = writer.write_table(tablet);
            if (ret != 0) {
                std::cerr << "write_table failed: " << ret << "\n";
                exit(1);
            }
            off += n;
            rows_written += n;
        }

        std::cout << "  device " << dev << " done (" << rows_written
                  << " rows)\n";
    }

    writer.flush();
    writer.close();

    double sec = std::chrono::duration<double>(clock::now() - t_start).count();
    std::cout << "  Full dataset: " << rows_written << " rows, " << std::fixed
              << std::setprecision(1) << sec << " s\n";
}

// -----------------------------------------------------------------------
// main
// -----------------------------------------------------------------------
int main(int argc, char* argv[]) {
    std::string csv_path = "write_precision.csv";
    std::string tsfile_path =
        "/Users/colin/dev/tsfile_b1/cpp/experiment/experiment_plain.tsfile";
    int64_t full_total_rows = 20000000LL;  // 10 dev × 2M rows

    if (argc > 1) csv_path = argv[1];
    if (argc > 2) tsfile_path = argv[2];
    if (argc > 3) full_total_rows = std::atoll(argv[3]);

    std::string tmp_path = tsfile_path + ".tmp";

    libtsfile_init();

    std::cout << "=== E3-2: Write Precision (PLAIN + UNCOMPRESSED) ===\n"
              << "  encoding:    PLAIN\n"
              << "  compression: UNCOMPRESSED\n"
              << "  s_data:      " << kSData << " bytes/row\n"
              << "  b:           " << kB << " bytes/device/flush\n"
              << "  csv:         " << csv_path << "\n"
              << "  tsfile:      " << tsfile_path << "\n\n";

    // ---- Phase 1: Precision measurement ----
    int64_t test_sizes[] = {5000, 8000, 16000, 32000, 65536};
    int n_tests = sizeof(test_sizes) / sizeof(test_sizes[0]);

    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    csv << "batch_size,m_data_direct,m_data_formula,m_meta_direct,"
           "m_meta_formula,error_pct\n";

    std::cout << std::setw(12) << "batch_size" << std::setw(16)
              << "M_data_direct" << std::setw(16) << "M_data_formula"
              << std::setw(10) << "error%"
              << "\n"
              << std::string(54, '-') << "\n";

    for (int i = 0; i < n_tests; i++) {
        auto r = measure_one(test_sizes[i], tmp_path);

        csv << r.batch_size << "," << r.m_data_direct << "," << r.m_data_formula
            << "," << r.m_meta_direct << "," << r.m_meta_formula << ","
            << std::fixed << std::setprecision(2) << r.error_pct << "\n";

        std::cout << std::setw(12) << r.batch_size << std::setw(16)
                  << r.m_data_direct << std::setw(16) << r.m_data_formula
                  << std::setw(9) << std::fixed << std::setprecision(2)
                  << r.error_pct << "%\n";
    }

    csv.close();
    std::remove(tmp_path.c_str());

    std::cout << "\nPrecision CSV: " << csv_path << "\n";

    // ---- Phase 2: Write full dataset for reader ----
    std::cout << "\n=== Writing full dataset (PLAIN + UNCOMPRESSED) ===\n"
              << "  total_rows: " << full_total_rows << "\n"
              << "  path:       " << tsfile_path << "\n\n";

    write_full_dataset(tsfile_path, full_total_rows);

    std::cout << "\nDone. TsFile: " << tsfile_path << "\n";

    libtsfile_destroy();
    return 0;
}
