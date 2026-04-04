/*
 * Parallel write throughput benchmark.
 *
 * Measures the throughput of TsFileWriter::write_table() with and without
 * column-parallel encoding.  For each (n_field_cols, thread_count) pair the
 * benchmark runs two modes:
 *
 *   serial   — parallel_write_enabled = false
 *   parallel — parallel_write_enabled = true, thread_pool size = thread_count
 *
 * UNCOMPRESSED / PLAIN encoding is used to isolate encoding throughput from
 * compression overhead.  Data arrays are pre-filled outside the timed loop so
 * the measured time reflects only write_table() — i.e., the encoding path
 * that the thread pool accelerates.
 *
 * Build (requires ENABLE_THREADS=ON):
 *   cmake -DENABLE_THREADS=ON -DBUILD_TEST=OFF -DCMAKE_BUILD_TYPE=Release ..
 *   cmake --build . --target thread_bench
 *
 * Run:
 *   ./thread_bench [total_rows] [batch_size] [csv_path]
 *
 * Defaults: total_rows=2000000, batch_size=8192, csv_path=thread_bench.csv
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
static const char* kTagVal = "d0";

// ---------------------------------------------------------------------------
// Schema helpers
// ---------------------------------------------------------------------------

static std::shared_ptr<TableSchema> make_table_schema(int n_field_cols) {
    std::vector<ColumnSchema> cols;
    cols.emplace_back(kTagName, STRING, UNCOMPRESSED, PLAIN,
                      ColumnCategory::TAG);
    for (int i = 0; i < n_field_cols; i++) {
        cols.emplace_back("f" + std::to_string(i), DOUBLE, UNCOMPRESSED, PLAIN,
                          ColumnCategory::FIELD);
    }
    return std::make_shared<TableSchema>(std::string(kTable), cols);
}

// ---------------------------------------------------------------------------
// Result record
// ---------------------------------------------------------------------------

struct RunResult {
    int n_field_cols;
    int thread_count;
    bool parallel;
    int64_t total_rows;
    int batch_size;
    double wall_ms;
    double rows_per_sec;
    double mb_per_sec;  // uncompressed DOUBLE payload only
};

// ---------------------------------------------------------------------------
// Run one configuration
// ---------------------------------------------------------------------------

static RunResult run_one(int n_field_cols, int thread_count, bool parallel,
                         int64_t total_rows, int batch_size,
                         const std::string& tmp_path) {
    // Set global config before constructing the writer: thread_pool_ is
    // initialized from write_thread_count_ at TsFileWriter construction time.
    g_config_value_.write_thread_count_ = thread_count;
    g_config_value_.parallel_write_enabled_ = parallel;

    // Open temp file
    WriteFile file;
    int ret = file.create(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (ret != 0) {
        std::cerr << "create file failed: " << ret << "\n";
        std::exit(1);
    }

    auto schema = make_table_schema(n_field_cols);
    // Very large memory threshold — avoid I/O flushes during the timed region.
    TsFileTableWriter writer(&file, schema.get(),
                             static_cast<uint64_t>(4) * 1024 * 1024 * 1024ULL);

    // Build Tablet column descriptor vectors (TAG + FIELD columns).
    std::vector<std::string> col_names;
    std::vector<TSDataType> col_types;
    std::vector<ColumnCategory> col_cats;
    col_names.push_back(kTagName);
    col_types.push_back(STRING);
    col_cats.push_back(ColumnCategory::TAG);
    for (int i = 0; i < n_field_cols; i++) {
        col_names.push_back("f" + std::to_string(i));
        col_types.push_back(DOUBLE);
        col_cats.push_back(ColumnCategory::FIELD);
    }

    Tablet tablet(kTable, col_names, col_types, col_cats, batch_size);
    if (tablet.err_code_ != E_OK) {
        std::cerr << "tablet init failed\n";
        std::exit(1);
    }

    // Pre-fill static arrays (allocated once, reused every batch).
    std::vector<int64_t> ts_arr(batch_size);
    for (int i = 0; i < batch_size; i++) ts_arr[i] = i;

    // One DOUBLE array suffices — all FIELD columns get the same values.
    // We benchmark encoding throughput, not data variety.
    std::vector<double> d_arr(batch_size);
    for (int i = 0; i < batch_size; i++) d_arr[i] = i * 0.001;

    // ------------------------------------
    // Timed region: write_table calls only
    // ------------------------------------
    auto t0 = std::chrono::steady_clock::now();

    int64_t rows_written = 0;
    while (rows_written < total_rows) {
        int cur_batch = static_cast<int>(
            std::min((int64_t)batch_size, total_rows - rows_written));

        // Bulk-fill tablet — avoids per-row string lookup overhead.
        // set_timestamps also updates cur_row_size_.
        tablet.set_timestamps(ts_arr.data(), cur_batch);
        // TAG column (schema_index = 0, STRING)
        tablet.set_column_string_repeated(
            0, kTagVal, static_cast<uint32_t>(strlen(kTagVal)), cur_batch);
        // FIELD columns (schema_index 1 .. n_field_cols)
        for (int c = 1; c <= n_field_cols; c++) {
            tablet.set_column_values(c, d_arr.data(), nullptr, cur_batch);
        }

        ret = writer.write_table(tablet);
        if (ret != E_OK) {
            std::cerr << "write_table failed: " << ret << "\n";
            std::exit(1);
        }
        rows_written += cur_batch;
    }

    writer.close();

    auto t1 = std::chrono::steady_clock::now();
    double wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    // Clean up temp file
    ::unlink(tmp_path.c_str());

    double rows_per_sec = (double)total_rows / (wall_ms / 1000.0);
    // Uncompressed DOUBLE payload: 8 bytes * n_field_cols * total_rows
    double bytes = 8.0 * n_field_cols * total_rows;
    double mb_per_sec = bytes / (wall_ms / 1000.0) / (1024.0 * 1024.0);

    RunResult r;
    r.n_field_cols = n_field_cols;
    r.thread_count = thread_count;
    r.parallel = parallel;
    r.total_rows = total_rows;
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
    int64_t total_rows = 2000000;
    int batch_size = 8192;
    std::string csv_path = "thread_bench.csv";

    if (argc > 1) total_rows = std::atoll(argv[1]);
    if (argc > 2) batch_size = std::atoi(argv[2]);
    if (argc > 3) csv_path = argv[3];

    std::cout << "=== Parallel Write Benchmark ===\n"
              << "  total_rows : " << total_rows << "\n"
              << "  batch_size : " << batch_size << "\n"
              << "  csv_output : " << csv_path << "\n\n";

    libtsfile_init();

#ifndef ENABLE_THREADS
    std::cout << "[WARNING] Built without ENABLE_THREADS — "
                 "parallel mode will fall back to serial.\n\n";
#endif

    const std::vector<int> col_counts = {4, 8, 16, 32};
    const std::vector<int> thread_counts = {1, 2, 4, 8};

    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    csv << "n_field_cols,mode,thread_count,total_rows,batch_size,"
           "wall_ms,rows_per_sec,mb_per_sec\n";

    std::cout << std::left << std::setw(8) << "cols" << std::setw(10) << "mode"
              << std::setw(9) << "threads" << std::setw(12) << "wall_ms"
              << std::setw(16) << "rows/sec" << std::setw(12) << "MB/sec"
              << "\n"
              << std::string(67, '-') << "\n";

    const std::string tmp_path = "/tmp/_thread_bench.tsfile";

    for (int n_cols : col_counts) {
        // Serial baseline (thread_count=1, parallel_write_enabled=false)
        {
            RunResult r =
                run_one(n_cols, 1, false, total_rows, batch_size, tmp_path);
            std::cout << std::left << std::setw(8) << n_cols << std::setw(10)
                      << "serial" << std::setw(9) << 1 << std::setw(12)
                      << std::fixed << std::setprecision(1) << r.wall_ms
                      << std::setw(16) << std::fixed << std::setprecision(0)
                      << r.rows_per_sec << std::setw(12) << std::fixed
                      << std::setprecision(1) << r.mb_per_sec << "\n";
            csv << n_cols << ",serial,1," << total_rows << "," << batch_size
                << "," << r.wall_ms << "," << r.rows_per_sec << ","
                << r.mb_per_sec << "\n";
        }

        // Parallel with varying thread counts
        for (int t : thread_counts) {
            RunResult r =
                run_one(n_cols, t, true, total_rows, batch_size, tmp_path);
            std::cout << std::left << std::setw(8) << n_cols << std::setw(10)
                      << "parallel" << std::setw(9) << t << std::setw(12)
                      << std::fixed << std::setprecision(1) << r.wall_ms
                      << std::setw(16) << std::fixed << std::setprecision(0)
                      << r.rows_per_sec << std::setw(12) << std::fixed
                      << std::setprecision(1) << r.mb_per_sec << "\n";
            csv << n_cols << ",parallel," << t << "," << total_rows << ","
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
