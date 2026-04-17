/*
 * Parallel write throughput benchmark (device-level parallelism).
 *
 * The TsFile table-aligned write path parallelises across devices: each
 * device's ChunkGroup (time + value columns) is encoded by a separate
 * thread-pool task.  This benchmark evaluates that strategy by writing
 * a multi-device tablet under varying device counts and thread counts.
 *
 * Fixed parameters:
 *   - FIELD columns    : 5  (INT64, TS_2DIFF, LZ4)
 *   - total rows       : 5 000 000 (split equally across devices)
 *   - batch_size       : 65 536   (rows per tablet submission)
 *
 * Sweep:
 *   - n_devices : 3, 10
 *   - threads   : 1 (serial baseline), 2, 4, 8
 *
 * Build (requires ENABLE_THREADS=ON):
 *   cmake -DENABLE_THREADS=ON -DBUILD_TEST=OFF -DCMAKE_BUILD_TYPE=Release ..
 *   cmake --build . --target thread_bench
 *
 * Run:
 *   ./thread_bench [total_rows] [batch_size] [csv_path]
 *
 * Defaults: total_rows=5000000, batch_size=65536, csv_path=thread_bench.csv
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
static const int kNumFields = 5;

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

static std::shared_ptr<TableSchema> make_table_schema() {
    std::vector<ColumnSchema> cols;
    cols.emplace_back(kTagName, STRING, ColumnCategory::TAG);
    for (int i = 0; i < kNumFields; i++) {
        cols.emplace_back("f" + std::to_string(i), INT64,
                          ColumnCategory::FIELD);
    }
    return std::make_shared<TableSchema>(std::string(kTable), cols);
}

// ---------------------------------------------------------------------------
// Device name helpers
// ---------------------------------------------------------------------------

static std::vector<std::string> make_device_names(int n_devices) {
    std::vector<std::string> names;
    for (int i = 0; i < n_devices; i++) {
        names.push_back("d" + std::to_string(i));
    }
    return names;
}

// ---------------------------------------------------------------------------
// Result record
// ---------------------------------------------------------------------------

struct RunResult {
    int n_devices;
    int thread_count;
    bool parallel;
    int64_t total_rows;
    int batch_size;
    double wall_ms;
    double rows_per_sec;
    double mb_per_sec;
};

// ---------------------------------------------------------------------------
// Run one configuration
// ---------------------------------------------------------------------------

static RunResult run_one(int n_devices, int thread_count, bool parallel,
                         int64_t total_rows, int batch_size,
                         const std::string& tmp_path) {
    g_config_value_.write_thread_count_ = thread_count;
    g_config_value_.parallel_write_enabled_ = parallel;

    WriteFile file;
    int ret = file.create(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (ret != 0) {
        std::cerr << "create file failed: " << ret << "\n";
        std::exit(1);
    }

    auto schema = make_table_schema();
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

    auto dev_names = make_device_names(n_devices);

    // rows_per_device per tablet submission
    int rpd = batch_size / n_devices;
    if (rpd < 1) rpd = 1;
    int tablet_rows = rpd * n_devices;

    Tablet tablet(kTable, col_names, col_types, col_cats, tablet_rows);
    if (tablet.err_code_ != E_OK) {
        std::cerr << "tablet init failed\n";
        std::exit(1);
    }

    // Pre-fill random values
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

    int64_t rows_written = 0;  // total rows across all devices
    while (rows_written < total_rows) {
        int cur_rpd = static_cast<int>(
            std::min((int64_t)rpd,
                     (total_rows - rows_written + n_devices - 1) / n_devices));
        int cur_total = cur_rpd * n_devices;

        // Timestamps: each device gets [device_base, device_base + cur_rpd)
        // Rows laid out sorted by device: [d0 rows][d1 rows]...[dN rows]
        std::vector<int64_t> ts_arr(cur_total);
        int64_t device_base = rows_written / n_devices;
        for (int d = 0; d < n_devices; d++) {
            for (int r = 0; r < cur_rpd; r++) {
                ts_arr[d * cur_rpd + r] = device_base + r;
            }
        }
        tablet.set_timestamps(ts_arr.data(), cur_total);

        // TAG column: device names sorted
        for (int d = 0; d < n_devices; d++) {
            for (int r = 0; r < cur_rpd; r++) {
                tablet.add_value(static_cast<uint32_t>(d * cur_rpd + r),
                                 static_cast<uint32_t>(0),
                                 dev_names[d].c_str());
            }
        }

        // FIELD columns
        for (int c = 1; c <= kNumFields; c++) {
            tablet.set_column_values(c, val_arr.data(), nullptr, cur_total);
        }

        ret = writer.write_table(tablet);
        if (ret != E_OK) {
            std::cerr << "write_table failed: " << ret << "\n";
            std::exit(1);
        }
        rows_written += cur_total;
    }

    writer.close();

    auto t1 = std::chrono::steady_clock::now();
    double wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    ::unlink(tmp_path.c_str());

    double rows_per_sec = (double)rows_written / (wall_ms / 1000.0);
    double bytes = 8.0 * kNumFields * rows_written;
    double mb_per_sec = bytes / (wall_ms / 1000.0) / (1024.0 * 1024.0);

    RunResult r;
    r.n_devices = n_devices;
    r.thread_count = thread_count;
    r.parallel = parallel;
    r.total_rows = rows_written;
    r.batch_size = batch_size;
    r.wall_ms = wall_ms;
    r.rows_per_sec = rows_per_sec;
    r.mb_per_sec = mb_per_sec;
    return r;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    int64_t total_rows = 5000000;
    int batch_size = 65536;
    std::string csv_path = "thread_bench.csv";

    if (argc > 1) total_rows = std::atoll(argv[1]);
    if (argc > 2) batch_size = std::atoi(argv[2]);
    if (argc > 3) csv_path = argv[3];

    std::cout << "=== Parallel Write Benchmark (device-level) ===\n"
              << "  fields     : " << kNumFields << "\n"
              << "  total_rows : " << total_rows << "\n"
              << "  batch_size : " << batch_size << "\n"
              << "  csv_output : " << csv_path << "\n\n";

    libtsfile_init();

    // Fixed: TS_2DIFF encoding + LZ4 compression
    g_config_value_.int64_encoding_type_ = TSEncoding::TS_2DIFF;
    g_config_value_.default_compression_type_ = LZ4;

#ifndef ENABLE_THREADS
    std::cout << "[WARNING] Built without ENABLE_THREADS — "
                 "parallel mode will fall back to serial.\n\n";
#endif

    const std::vector<int> device_counts = {3, 10};
    const std::vector<int> thread_counts = {1, 2, 4, 8};

    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    csv << "n_devices,mode,thread_count,total_rows,batch_size,"
           "wall_ms,rows_per_sec,mb_per_sec\n";

    std::cout << std::left << std::setw(10) << "devices" << std::setw(10)
              << "mode" << std::setw(9) << "threads" << std::setw(12)
              << "wall_ms" << std::setw(16) << "rows/sec" << std::setw(12)
              << "MB/sec"
              << "\n"
              << std::string(69, '-') << "\n";

    const std::string tmp_path = "/tmp/_thread_bench.tsfile";

    for (int nd : device_counts) {
        // Serial baseline
        {
            RunResult r =
                run_one(nd, 1, false, total_rows, batch_size, tmp_path);
            std::cout << std::left << std::setw(10) << nd << std::setw(10)
                      << "serial" << std::setw(9) << 1 << std::setw(12)
                      << std::fixed << std::setprecision(1) << r.wall_ms
                      << std::setw(16) << std::fixed << std::setprecision(0)
                      << r.rows_per_sec << std::setw(12) << std::fixed
                      << std::setprecision(1) << r.mb_per_sec << "\n";
            csv << nd << ",serial,1," << r.total_rows << "," << batch_size
                << "," << r.wall_ms << "," << r.rows_per_sec << ","
                << r.mb_per_sec << "\n";
        }

        // Parallel with varying thread counts
        for (int t : thread_counts) {
            RunResult r =
                run_one(nd, t, true, total_rows, batch_size, tmp_path);
            std::cout << std::left << std::setw(10) << nd << std::setw(10)
                      << "parallel" << std::setw(9) << t << std::setw(12)
                      << std::fixed << std::setprecision(1) << r.wall_ms
                      << std::setw(16) << std::fixed << std::setprecision(0)
                      << r.rows_per_sec << std::setw(12) << std::fixed
                      << std::setprecision(1) << r.mb_per_sec << "\n";
            csv << nd << ",parallel," << t << "," << r.total_rows << ","
                << batch_size << "," << r.wall_ms << "," << r.rows_per_sec
                << "," << r.mb_per_sec << "\n";
        }
        std::cout << "\n";
        csv.flush();
    }

    std::cout << "Done. Results written to " << csv_path << "\n";
    libtsfile_destroy();
    return 0;
}
