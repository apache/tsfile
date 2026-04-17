/*
 * Task-granularity benchmark for parallel write.
 *
 * Measures how rows-per-device affects the speedup of column-parallel
 * encoding.  A single tablet always contains all 4 devices; the variable
 * is the number of rows each device contributes per write_table() call.
 *
 * When rows_per_device is small the per-device encode task is lightweight
 * and thread-pool scheduling overhead dominates; when rows_per_device is
 * large the overhead is amortised and parallelism pays off.
 *
 * Fixed parameters:
 *   - total rows per device : 1,250,000  (5M / 4 devices)
 *   - FIELD columns         : 8
 *   - threads               : 4
 *   - encoding              : TS_2DIFF
 *   - compression           : LZ4
 *
 * Build (requires ENABLE_THREADS=ON):
 *   cmake -DENABLE_THREADS=ON -DBUILD_TEST=OFF -DCMAKE_BUILD_TYPE=Release ..
 *   cmake --build . --target granularity_bench
 *
 * Run:
 *   ./granularity_bench [csv_path]
 *
 * Default csv_path: granularity_bench.csv
 */

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "common/global.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "writer/tsfile_table_writer.h"

using namespace common;
using namespace storage;

static const char* kTable = "bench";
static const char* kTagName = "dev";

static const int kNumDevices = 4;
static const int kNumFields = 8;
static const int kThreadCount = 4;
static const int64_t kRowsPerDevice = 1250000;  // 5M / 4

static const char* kDevNames[kNumDevices] = {"d0", "d1", "d2", "d3"};

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

static std::shared_ptr<TableSchema> make_schema() {
    std::vector<ColumnSchema> cols;
    cols.emplace_back(kTagName, STRING, ColumnCategory::TAG);
    for (int i = 0; i < kNumFields; i++) {
        cols.emplace_back("f" + std::to_string(i), INT64,
                          ColumnCategory::FIELD);
    }
    return std::make_shared<TableSchema>(std::string(kTable), cols);
}

// ---------------------------------------------------------------------------
// Result record
// ---------------------------------------------------------------------------

struct RunResult {
    int rows_per_device;
    bool parallel;
    double wall_ms;
    double rows_per_sec;  // total rows (all devices)
};

// ---------------------------------------------------------------------------
// Run one configuration
// ---------------------------------------------------------------------------

static RunResult run_one(int rows_per_device, bool parallel,
                         const std::string& tmp_path) {
    g_config_value_.write_thread_count_ = kThreadCount;
    g_config_value_.parallel_write_enabled_ = parallel;

    WriteFile file;
    int ret = file.create(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (ret != 0) {
        std::cerr << "create file failed: " << ret << "\n";
        std::exit(1);
    }

    auto schema = make_schema();
    TsFileTableWriter writer(&file, schema.get(),
                             static_cast<uint64_t>(4) * 1024 * 1024 * 1024ULL);

    // Build Tablet column descriptors
    std::vector<std::string> col_names;
    std::vector<TSDataType> col_types;
    std::vector<ColumnCategory> col_cats;
    col_names.push_back(kTagName);
    col_types.push_back(STRING);
    col_cats.push_back(ColumnCategory::TAG);
    for (int i = 0; i < kNumFields; i++) {
        col_names.push_back("f" + std::to_string(i));
        col_types.push_back(INT64);
        col_cats.push_back(ColumnCategory::FIELD);
    }

    // Tablet capacity = rows_per_device * kNumDevices
    int tablet_rows = rows_per_device * kNumDevices;
    Tablet tablet(kTable, col_names, col_types, col_cats, tablet_rows);
    if (tablet.err_code_ != E_OK) {
        std::cerr << "tablet init failed\n";
        std::exit(1);
    }

    // Pre-fill random values so encoding + compression have real CPU work
    std::vector<int64_t> val_arr(tablet_rows);
    uint64_t rng = 0xdeadbeefcafe1234ULL;
    for (int i = 0; i < tablet_rows; i++) {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        val_arr[i] = static_cast<int64_t>(rng);
    }

    // ------------------------------------
    // Timed region
    // ------------------------------------
    auto t0 = std::chrono::steady_clock::now();

    int64_t device_rows_written = 0;  // rows written per device so far
    while (device_rows_written < kRowsPerDevice) {
        int cur_rpd = static_cast<int>(std::min(
            (int64_t)rows_per_device, kRowsPerDevice - device_rows_written));
        int cur_total = cur_rpd * kNumDevices;

        // Fill timestamps — each device gets the same timestamp range
        // Rows are laid out: [dev0 rows][dev1 rows][dev2 rows][dev3 rows]
        std::vector<int64_t> ts_arr(cur_total);
        for (int d = 0; d < kNumDevices; d++) {
            for (int r = 0; r < cur_rpd; r++) {
                ts_arr[d * cur_rpd + r] = device_rows_written + r;
            }
        }
        tablet.set_timestamps(ts_arr.data(), cur_total);

        // Fill TAG column — device name per row, sorted by device
        for (int d = 0; d < kNumDevices; d++) {
            for (int r = 0; r < cur_rpd; r++) {
                tablet.add_value(static_cast<uint32_t>(d * cur_rpd + r),
                                 static_cast<uint32_t>(0), kDevNames[d]);
            }
        }

        // Fill FIELD columns
        for (int c = 1; c <= kNumFields; c++) {
            tablet.set_column_values(c, val_arr.data(), nullptr, cur_total);
        }

        ret = writer.write_table(tablet);
        if (ret != E_OK) {
            std::cerr << "write_table failed: " << ret << "\n";
            std::exit(1);
        }
        device_rows_written += cur_rpd;
    }

    writer.close();

    auto t1 = std::chrono::steady_clock::now();
    double wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    ::unlink(tmp_path.c_str());

    int64_t total_rows = kRowsPerDevice * kNumDevices;
    double rows_per_sec = (double)total_rows / (wall_ms / 1000.0);

    RunResult r;
    r.rows_per_device = rows_per_device;
    r.parallel = parallel;
    r.wall_ms = wall_ms;
    r.rows_per_sec = rows_per_sec;
    return r;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    std::string csv_path = "granularity_bench.csv";
    if (argc > 1) csv_path = argv[1];

    int64_t total_rows = kRowsPerDevice * kNumDevices;

    std::cout << "=== Task Granularity Benchmark ===\n"
              << "  devices          : " << kNumDevices << "\n"
              << "  fields           : " << kNumFields << "\n"
              << "  threads          : " << kThreadCount << "\n"
              << "  rows per device  : " << kRowsPerDevice << "\n"
              << "  total rows       : " << total_rows << "\n"
              << "  encoding         : TS_2DIFF\n"
              << "  compression      : LZ4\n"
              << "  csv_output       : " << csv_path << "\n\n";

    libtsfile_init();

    // Fixed: TS_2DIFF + LZ4
    g_config_value_.int64_encoding_type_ = TSEncoding::TS_2DIFF;
    g_config_value_.default_compression_type_ = LZ4;

#ifndef ENABLE_THREADS
    std::cout << "[WARNING] Built without ENABLE_THREADS — "
                 "parallel mode will fall back to serial.\n\n";
#endif

    const std::vector<int> rpd_values = {100,   500,   1000, 5000,
                                         10000, 50000, 65536};

    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    csv << "rows_per_device,mode,wall_ms,rows_per_sec,speedup\n";

    std::cout << std::left << std::setw(12) << "rpd" << std::setw(10) << "mode"
              << std::setw(12) << "wall_ms" << std::setw(16) << "rows/sec"
              << std::setw(10) << "speedup"
              << "\n"
              << std::string(60, '-') << "\n";

    const std::string tmp_path = "/tmp/_granularity_bench.tsfile";

    for (int rpd : rpd_values) {
        // Serial baseline
        RunResult serial_r = run_one(rpd, false, tmp_path);
        double serial_ms = serial_r.wall_ms;

        std::cout << std::left << std::setw(12) << rpd << std::setw(10)
                  << "serial" << std::setw(12) << std::fixed
                  << std::setprecision(1) << serial_r.wall_ms << std::setw(16)
                  << std::fixed << std::setprecision(0) << serial_r.rows_per_sec
                  << std::setw(10) << "1.00x"
                  << "\n";
        csv << rpd << ",serial," << serial_r.wall_ms << ","
            << serial_r.rows_per_sec << ",1.00\n";

        // Parallel
        RunResult par_r = run_one(rpd, true, tmp_path);
        double speedup = serial_ms / par_r.wall_ms;

        std::ostringstream sp;
        sp << std::fixed << std::setprecision(2) << speedup << "x";

        std::cout << std::left << std::setw(12) << rpd << std::setw(10)
                  << "parallel" << std::setw(12) << std::fixed
                  << std::setprecision(1) << par_r.wall_ms << std::setw(16)
                  << std::fixed << std::setprecision(0) << par_r.rows_per_sec
                  << std::setw(10) << sp.str() << "\n\n";
        csv << rpd << ",parallel," << par_r.wall_ms << "," << par_r.rows_per_sec
            << "," << std::fixed << std::setprecision(4) << speedup << "\n";
        csv.flush();
    }

    std::cout << "Done. Results written to " << csv_path << "\n";
    libtsfile_destroy();
    return 0;
}
