/*
 * Parallel READ throughput benchmark.
 *
 * Measures the throughput of TsFileReader table-model batch reads with and
 * without the column-parallel decode path.
 *
 * For each (compression, n_field_cols, thread_count) combination the benchmark
 * runs two modes:
 *
 *   serial   — parallel_read_enabled = false
 *   parallel — parallel_read_enabled = true, read_thread_count = thread_count
 *
 * Encoding and compression are controlled via g_config_value_ (the ColumnSchema
 * parameters are ignored by the writer — the global config is authoritative).
 *
 * Build (requires ENABLE_THREADS=ON):
 *   cmake -DENABLE_THREADS=ON -DBUILD_TEST=OFF -DCMAKE_BUILD_TYPE=Release ..
 *   cmake --build . --target read_thread_bench
 *
 * Run:
 *   ./read_thread_bench [total_rows] [batch_size] [csv_path]
 *
 * Defaults: total_rows=5000000, batch_size=65536,
 * csv_path=read_thread_bench.csv
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
#include "common/tsblock/tsblock.h"
#include "file/write_file.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

using namespace common;
using namespace storage;

static const char* kTable = "bench";
static const char* kTagName = "dev";
static const char* kTagVal = "d0";

// ---------------------------------------------------------------------------
// Write a test TsFile with the given number of FIELD columns
// ---------------------------------------------------------------------------

static void write_test_file(const std::string& path, int n_field_cols,
                            int64_t total_rows, int batch_size) {
    std::vector<ColumnSchema> cols;
    cols.emplace_back(kTagName, STRING, ColumnCategory::TAG);
    for (int i = 0; i < n_field_cols; i++) {
        cols.emplace_back("f" + std::to_string(i), INT64,
                          ColumnCategory::FIELD);
    }
    auto schema = std::make_shared<TableSchema>(std::string(kTable), cols);

    WriteFile file;
    int ret = file.create(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (ret != 0) {
        std::cerr << "create file failed: " << ret << "\n";
        std::exit(1);
    }

    TsFileTableWriter writer(&file, schema.get(),
                             static_cast<uint64_t>(4) * 1024 * 1024 * 1024ULL);

    std::vector<std::string> col_names;
    std::vector<TSDataType> col_types;
    std::vector<ColumnCategory> col_cats;
    col_names.push_back(kTagName);
    col_types.push_back(STRING);
    col_cats.push_back(ColumnCategory::TAG);
    for (int i = 0; i < n_field_cols; i++) {
        col_names.push_back("f" + std::to_string(i));
        col_types.push_back(INT64);
        col_cats.push_back(ColumnCategory::FIELD);
    }

    Tablet tablet(kTable, col_names, col_types, col_cats, batch_size);

    std::vector<int64_t> ts_arr(batch_size);
    std::vector<int64_t> val_arr(batch_size);

    // Pre-fill random values so decompression has real work to do
    uint64_t rng = 0xdeadbeefcafe1234ULL;
    for (int i = 0; i < batch_size; i++) {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        val_arr[i] = static_cast<int64_t>(rng);
    }

    int64_t rows_written = 0;
    while (rows_written < total_rows) {
        int cur_batch = static_cast<int>(
            std::min((int64_t)batch_size, total_rows - rows_written));

        for (int i = 0; i < cur_batch; i++) {
            ts_arr[i] = rows_written + i;
        }

        tablet.set_timestamps(ts_arr.data(), cur_batch);
        tablet.set_column_string_repeated(
            0, kTagVal, static_cast<uint32_t>(strlen(kTagVal)), cur_batch);
        for (int c = 1; c <= n_field_cols; c++) {
            tablet.set_column_values(c, val_arr.data(), nullptr, cur_batch);
        }

        ret = writer.write_table(tablet);
        if (ret != E_OK) {
            std::cerr << "write_table failed: " << ret << "\n";
            std::exit(1);
        }
        rows_written += cur_batch;
    }
    writer.flush();
    writer.close();
}

// ---------------------------------------------------------------------------
// Result record
// ---------------------------------------------------------------------------

struct ReadResult {
    std::string compression;
    int n_field_cols;
    int thread_count;
    bool parallel;
    int64_t total_rows;
    int batch_size;
    double wall_ms;
    double rows_per_sec;
    double values_per_sec;  // rows * n_field_cols / sec
};

// ---------------------------------------------------------------------------
// Run one read configuration
// ---------------------------------------------------------------------------

static ReadResult run_read(const std::string& comp_name,
                           const std::string& path, int n_field_cols,
                           int thread_count, bool parallel, int64_t total_rows,
                           int batch_size) {
    // Configure threading
    g_config_value_.parallel_read_enabled_ = parallel;
    g_config_value_.read_thread_count_ = thread_count;

    // Build column name list for query
    std::vector<std::string> col_names;
    col_names.push_back(kTagName);
    for (int i = 0; i < n_field_cols; i++) {
        col_names.push_back("f" + std::to_string(i));
    }

    // Warm up: one read to populate OS page cache
    {
        TsFileReader reader;
        reader.open(path);
        ResultSet* rs = nullptr;
        reader.query(kTable, col_names, 0, total_rows + 1, rs, batch_size);
        if (rs) {
            auto* trs = dynamic_cast<TableResultSet*>(rs);
            TsBlock* blk = nullptr;
            while (trs->get_next_tsblock(blk) == E_OK) {
            }
            reader.destroy_query_data_set(rs);
        }
        reader.close();
    }

    // Timed run
    auto t0 = std::chrono::steady_clock::now();

    TsFileReader reader;
    reader.open(path);
    ResultSet* rs = nullptr;
    int ret =
        reader.query(kTable, col_names, 0, total_rows + 1, rs, batch_size);
    if (ret != E_OK || rs == nullptr) {
        std::cerr << "query failed: " << ret << "\n";
        std::exit(1);
    }

    auto* trs = dynamic_cast<TableResultSet*>(rs);
    int64_t rows_read = 0;
    TsBlock* blk = nullptr;
    while (trs->get_next_tsblock(blk) == E_OK) {
        rows_read += blk->get_row_count();
    }

    reader.destroy_query_data_set(rs);
    reader.close();

    auto t1 = std::chrono::steady_clock::now();
    double wall_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    if (rows_read != total_rows) {
        std::cerr << "WARNING: expected " << total_rows << " rows, got "
                  << rows_read << "\n";
    }

    double sec = wall_ms / 1000.0;
    ReadResult r;
    r.compression = comp_name;
    r.n_field_cols = n_field_cols;
    r.thread_count = thread_count;
    r.parallel = parallel;
    r.total_rows = rows_read;
    r.batch_size = batch_size;
    r.wall_ms = wall_ms;
    r.rows_per_sec = rows_read / sec;
    r.values_per_sec = (double)rows_read * n_field_cols / sec;
    return r;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char* argv[]) {
    int64_t total_rows = 5000000;
    int batch_size = 65536;
    std::string csv_path = "read_thread_bench.csv";

    if (argc > 1) total_rows = std::atoll(argv[1]);
    if (argc > 2) batch_size = std::atoi(argv[2]);
    if (argc > 3) csv_path = argv[3];

    std::cout << "=== Parallel Read Benchmark ===\n"
              << "  total_rows : " << total_rows << "\n"
              << "  batch_size : " << batch_size << "\n"
              << "  csv_output : " << csv_path << "\n\n";

    libtsfile_init();

#ifndef ENABLE_THREADS
    std::cout << "[WARNING] Built without ENABLE_THREADS — "
                 "parallel mode will fall back to serial.\n\n";
#endif

    const std::vector<int> col_counts = {4, 8, 16};
    const std::vector<int> thread_counts = {2, 4, 8};
    const std::string tmp_path = "/tmp/_read_thread_bench.tsfile";

    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    csv << "n_field_cols,mode,thread_count,total_rows,batch_size,"
           "wall_ms,rows_per_sec,values_per_sec\n";

    std::cout << std::left << std::setw(8) << "cols" << std::setw(10) << "mode"
              << std::setw(9) << "threads" << std::setw(12) << "wall_ms"
              << std::setw(16) << "rows/sec" << std::setw(16) << "values/sec"
              << std::setw(10) << "speedup"
              << "\n"
              << std::string(81, '-') << "\n";

    // Fixed: TS_2DIFF encoding + LZ4 compression
    g_config_value_.int64_encoding_type_ = TSEncoding::TS_2DIFF;
    g_config_value_.default_compression_type_ = LZ4;

    for (int n_cols : col_counts) {
        // Write test file for this column count
        std::cout << "[Writing LZ4 " << n_cols << "-col file, " << total_rows
                  << " rows...]\n";
        write_test_file(tmp_path, n_cols, total_rows, 8192);

        // Serial baseline
        ReadResult serial_r =
            run_read("LZ4", tmp_path, n_cols, 1, false, total_rows, batch_size);
        double serial_ms = serial_r.wall_ms;

        std::cout << std::left << std::setw(8) << n_cols << std::setw(10)
                  << "serial" << std::setw(9) << 1 << std::setw(12)
                  << std::fixed << std::setprecision(1) << serial_r.wall_ms
                  << std::setw(16) << std::fixed << std::setprecision(0)
                  << serial_r.rows_per_sec << std::setw(16) << std::fixed
                  << std::setprecision(0) << serial_r.values_per_sec
                  << std::setw(10) << "1.00x"
                  << "\n";
        csv << n_cols << ",serial,1," << total_rows << "," << batch_size << ","
            << serial_r.wall_ms << "," << serial_r.rows_per_sec << ","
            << serial_r.values_per_sec << "\n";

        // Parallel with varying thread counts
        for (int t : thread_counts) {
            ReadResult r = run_read("LZ4", tmp_path, n_cols, t, true,
                                    total_rows, batch_size);
            double speedup = serial_ms / r.wall_ms;

            std::ostringstream sp;
            sp << std::fixed << std::setprecision(2) << speedup << "x";

            std::cout << std::left << std::setw(8) << n_cols << std::setw(10)
                      << "parallel" << std::setw(9) << t << std::setw(12)
                      << std::fixed << std::setprecision(1) << r.wall_ms
                      << std::setw(16) << std::fixed << std::setprecision(0)
                      << r.rows_per_sec << std::setw(16) << std::fixed
                      << std::setprecision(0) << r.values_per_sec
                      << std::setw(10) << sp.str() << "\n";
            csv << n_cols << ",parallel," << t << "," << total_rows << ","
                << batch_size << "," << r.wall_ms << "," << r.rows_per_sec
                << "," << r.values_per_sec << "\n";
        }
        std::cout << "\n";
        csv.flush();
    }

    ::unlink(tmp_path.c_str());
    std::cout << "Done. Results written to " << csv_path << "\n";
    libtsfile_destroy();
    return 0;
}
