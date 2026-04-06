/*
 * E4-1/E4-2: Write & Read Parallel Throughput Benchmark
 *
 * Measures write/read throughput under column-level parallelism using realistic
 * compression (SNAPPY, LZ4) and TS_2DIFF encoding for INT64 FIELD columns.
 * This extends the basic thread_bench to cover both axes of the Amdahl model:
 *
 *   write (E4-1) — parallel_write_enabled + write_thread_count
 *   read  (E4-2) — parallel_read_enabled  + read_thread_count
 *
 * Internal sweep:
 *   cols:        4, 8, 16
 *   threads:     1 (serial), 2, 4, 8  (when ENABLE_THREADS; else only 1)
 *   compression: SNAPPY, LZ4
 *
 * Build:
 *   cmake -DENABLE_THREADS=OFF -DBUILD_TEST=OFF -DCMAKE_BUILD_TYPE=Release ..
 *   cmake -DENABLE_THREADS=ON  -DBUILD_TEST=OFF -DCMAKE_BUILD_TYPE=Release ..
 *   cmake --build . --target throughput_bench
 *
 * Run:
 *   ./throughput_bench [total_rows] [csv_dir]
 *   Defaults: total_rows=5000000, csv_dir=.
 *
 * Output (one CSV per operation, appended with build label):
 *   write_results_{ON|OFF}.csv
 *   read_results_{ON|OFF}.csv
 *
 * CSV columns:
 *   cols, threads, parallel, compression, throughput_mrows_s, time_s
 */

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
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

// ─── Configuration ───────────────────────────────────────────────────────────

static int64_t gTotalRows = 5000000;
static std::string gCsvDir = ".";
static const int kBatchSize = 65536;
// Very large write threshold: avoid mid-bench flushes in timed region.
// close() flushes everything at the end.
static const uint64_t kWriteMemThreshold = 4ULL * 1024 * 1024 * 1024;

static const char* kTable = "bench";
static const char* kTagColName = "dev";
static const char* kTagVal = "d0";

static const int gColsList[] = {4, 8, 16};
static const int gNumCols = 3;

static const CompressionType gCompList[] = {SNAPPY, LZ4};
static const char* gCompNames[] = {"SNAPPY", "LZ4"};
static const int gNumComp = 2;

// ─── Helpers ─────────────────────────────────────────────────────────────────

static std::string threads_label() {
#ifdef ENABLE_THREADS
    return "ON";
#else
    return "OFF";
#endif
}

static std::string tmp_path(int cols, const char* comp, int trial) {
    char buf[256];
    snprintf(buf, sizeof(buf), "/tmp/e4_bench_c%d_%s_%d.tsfile", cols, comp,
             trial);
    return buf;
}

// ─── Schema / tablet helpers
// ──────────────────────────────────────────────────

// Schema: 1 TAG (STRING/PLAIN/UNCOMPRESSED) + n_fields FIELD
// (INT64/TS_2DIFF/comp)
static std::shared_ptr<TableSchema> make_schema(int n_fields,
                                                CompressionType comp) {
    std::vector<ColumnSchema> cols;
    cols.emplace_back(kTagColName, STRING, UNCOMPRESSED, PLAIN,
                      ColumnCategory::TAG);
    for (int i = 0; i < n_fields; i++) {
        cols.emplace_back("f" + std::to_string(i), INT64, comp, TS_2DIFF,
                          ColumnCategory::FIELD);
    }
    return std::make_shared<TableSchema>(std::string(kTable), cols);
}

// Column descriptor vectors for Tablet constructor (TAG + n FIELD cols).
static void make_col_vecs(int n_fields, std::vector<std::string>& col_names,
                          std::vector<TSDataType>& col_types,
                          std::vector<ColumnCategory>& col_cats) {
    col_names.clear();
    col_types.clear();
    col_cats.clear();
    col_names.push_back(kTagColName);
    col_types.push_back(STRING);
    col_cats.push_back(ColumnCategory::TAG);
    for (int i = 0; i < n_fields; i++) {
        col_names.push_back("f" + std::to_string(i));
        col_types.push_back(INT64);
        col_cats.push_back(ColumnCategory::FIELD);
    }
}

// ─── Result records
// ───────────────────────────────────────────────────────────

struct BenchResult {
    int cols;
    int threads;
    bool parallel;
    const char* compression;
    double time_s;
    double throughput_mrows_s;
};

static std::vector<BenchResult> gWriteResults;
static std::vector<BenchResult> gReadResults;

// ─── Write one run
// ────────────────────────────────────────────────────────────

static int do_write(const std::string& path, int n_fields,
                    CompressionType comp) {
    WriteFile file;
    int ret = file.create(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (ret != E_OK) {
        std::cerr << "create file failed: " << ret << "\n";
        return ret;
    }

    auto schema = make_schema(n_fields, comp);
    TsFileTableWriter writer(&file, schema.get(), kWriteMemThreshold);

    std::vector<std::string> col_names;
    std::vector<TSDataType> col_types;
    std::vector<ColumnCategory> col_cats;
    make_col_vecs(n_fields, col_names, col_types, col_cats);

    Tablet tablet(kTable, col_names, col_types, col_cats, kBatchSize);
    if (tablet.err_code_ != E_OK) {
        std::cerr << "tablet init failed\n";
        return -1;
    }

    // Pre-fill timestamp and value arrays (outside timed region when called
    // from write bench; shared here for simplicity).
    // Values are random so pages are less compressible, making the decode
    // phase CPU-bound enough to benefit from parallel decompression.
    std::vector<int64_t> ts_arr(kBatchSize);
    std::vector<int64_t> val_arr(kBatchSize);
    uint64_t rng = 0xdeadbeefcafe1234ULL;
    for (int i = 0; i < kBatchSize; i++) {
        ts_arr[i] = i + 1;
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;  // xorshift64
        val_arr[i] = static_cast<int64_t>(rng);
    }

    int64_t written = 0;
    while (written < gTotalRows) {
        int cur = (int)std::min((int64_t)kBatchSize, gTotalRows - written);

        // Update timestamps for this batch (shifted by written offset)
        for (int i = 0; i < cur; i++) ts_arr[i] = written + i + 1;

        tablet.set_timestamps(ts_arr.data(), (uint32_t)cur);
        tablet.set_column_string_repeated(0, kTagVal, (uint32_t)strlen(kTagVal),
                                          cur);
        for (int c = 1; c <= n_fields; c++) {
            tablet.set_column_values(c, val_arr.data(), nullptr, (uint32_t)cur);
        }

        ret = writer.write_table(tablet);
        if (ret != E_OK) {
            std::cerr << "write_table failed: " << ret << "\n";
            return ret;
        }
        written += cur;
    }

    ret = writer.flush();
    if (ret != E_OK) {
        std::cerr << "writer.flush() failed: " << ret << "\n";
        return ret;
    }
    ret = writer.close();
    if (ret != E_OK) {
        std::cerr << "writer.close() failed: " << ret << "\n";
    }
    return ret;
}

// ─── Write benchmark
// ──────────────────────────────────────────────────────────

static void run_write_bench(int n_cols, CompressionType comp,
                            const char* comp_name, int threads) {
    bool parallel = (threads > 1);
    std::string path = tmp_path(n_cols, comp_name, threads);

#ifdef ENABLE_THREADS
    g_config_value_.parallel_write_enabled_ = parallel;
    g_config_value_.write_thread_count_ = threads;
#else
    (void)parallel;
#endif

    // Warmup round (not recorded)
    ::unlink(path.c_str());
    if (do_write(path, n_cols, comp) != 0) return;
    ::unlink(path.c_str());

    // Timed round
    auto t0 = std::chrono::steady_clock::now();
    if (do_write(path, n_cols, comp) != 0) return;
    double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();
    ::unlink(path.c_str());

    double tp = gTotalRows / sec / 1e6;
    std::cout << "  WRITE cols=" << std::setw(2) << n_cols << " t=" << threads
              << " " << std::left << std::setw(7) << comp_name << std::right
              << " : " << std::fixed << std::setprecision(3) << sec << " s"
              << "  " << std::setprecision(2) << tp << " Mrows/s\n";

    gWriteResults.push_back({n_cols, threads, parallel, comp_name, sec, tp});
}

// ─── Read benchmark
// ───────────────────────────────────────────────────────────

static void run_read_bench(int n_cols, CompressionType comp,
                           const char* comp_name, int threads,
                           const std::string& read_file_path) {
    bool parallel = (threads > 1);

#ifdef ENABLE_THREADS
    g_config_value_.parallel_read_enabled_ = parallel;
    g_config_value_.read_thread_count_ = threads;
#else
    (void)parallel;
#endif

    // Build field column list
    std::vector<std::string> field_cols;
    for (int c = 0; c < n_cols; c++) {
        field_cols.push_back("f" + std::to_string(c));
    }

    // Use TsBlock batch mode: eliminates per-row iteration overhead so that
    // the decode phase (decompression) dominates and parallelism is visible.
    auto do_read = [&]() {
        TsFileReader reader;
        if (reader.open(read_file_path) != E_OK) return (int64_t)0;
        ResultSet* rs = nullptr;
        reader.query(kTable, field_cols, 0, INT64_MAX, rs, kBatchSize);
        int64_t count = 0;
        if (rs) {
            common::TsBlock* block = nullptr;
            while (rs->get_next_tsblock(block) == E_OK && block != nullptr) {
                count += static_cast<int64_t>(block->get_row_count());
                block = nullptr;
            }
            reader.destroy_query_data_set(rs);
        }
        reader.close();
        return count;
    };

    // Warmup round
    do_read();

    // Timed round
    auto t0 = std::chrono::steady_clock::now();
    int64_t count = do_read();
    (void)count;
    double sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
            .count();

    double tp = gTotalRows / sec / 1e6;
    std::cout << "  READ  cols=" << std::setw(2) << n_cols << " t=" << threads
              << " " << std::left << std::setw(7) << comp_name << std::right
              << " : " << std::fixed << std::setprecision(3) << sec << " s"
              << "  " << std::setprecision(2) << tp << " Mrows/s\n";

    gReadResults.push_back({n_cols, threads, parallel, comp_name, sec, tp});
}

// ─── CSV output
// ───────────────────────────────────────────────────────────────

static void write_csv() {
    std::string label = threads_label();

    // Write results
    {
        std::string path = gCsvDir + "/write_results_" + label + ".csv";
        std::ofstream csv(path);
        csv << "cols,threads,parallel,compression,throughput_mrows_s,time_s\n";
        for (auto& r : gWriteResults) {
            csv << r.cols << "," << r.threads << "," << (r.parallel ? 1 : 0)
                << "," << r.compression << "," << std::fixed
                << std::setprecision(3) << r.throughput_mrows_s << ","
                << std::setprecision(4) << r.time_s << "\n";
        }
        std::cout << "\nCSV: " << path << "\n";
    }

    // Read results
    {
        std::string path = gCsvDir + "/read_results_" + label + ".csv";
        std::ofstream csv(path);
        csv << "cols,threads,parallel,compression,throughput_mrows_s,time_s\n";
        for (auto& r : gReadResults) {
            csv << r.cols << "," << r.threads << "," << (r.parallel ? 1 : 0)
                << "," << r.compression << "," << std::fixed
                << std::setprecision(3) << r.throughput_mrows_s << ","
                << std::setprecision(4) << r.time_s << "\n";
        }
        std::cout << "CSV: " << path << "\n";
    }
}

// ─── Main
// ─────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc > 1) gTotalRows = std::atoll(argv[1]);
    if (argc > 2) gCsvDir = argv[2];

    std::cout << "=== E4-1/E4-2: Parallel Throughput Benchmark ===\n"
              << "  ENABLE_THREADS : " << threads_label() << "\n"
              << "  total_rows     : " << gTotalRows << "\n"
              << "  batch_size     : " << kBatchSize << "\n"
              << "  csv_dir        : " << gCsvDir << "\n\n";

    libtsfile_init();

#ifndef ENABLE_THREADS
    std::cout << "[NOTE] Built without ENABLE_THREADS — "
                 "only threads=1 (serial) will be tested.\n\n";
#endif

    // Thread counts to sweep.  When ENABLE_THREADS is off, only 1 makes sense
    // but we still record it for the serial baseline comparison.
#ifdef ENABLE_THREADS
    const std::vector<int> thread_counts = {1, 2, 4, 8};
#else
    const std::vector<int> thread_counts = {1};
#endif

    for (int ci = 0; ci < gNumComp; ci++) {
        CompressionType comp = gCompList[ci];
        const char* cname = gCompNames[ci];

        for (int ki = 0; ki < gNumCols; ki++) {
            int n_cols = gColsList[ki];

            std::cout << "── Write: cols=" << n_cols << " comp=" << cname
                      << " ──\n";
            for (int t : thread_counts) {
                run_write_bench(n_cols, comp, cname, t);
            }

            // Write the read-bench file with serial config (not timed)
            std::string rpath = tmp_path(n_cols, cname, /*trial=*/0);
            ::unlink(rpath.c_str());
#ifdef ENABLE_THREADS
            g_config_value_.parallel_write_enabled_ = false;
            g_config_value_.write_thread_count_ = 1;
#endif
            if (do_write(rpath, n_cols, comp) != 0) {
                std::cerr << "Failed to write read-bench file\n";
                continue;
            }

            std::cout << "── Read:  cols=" << n_cols << " comp=" << cname
                      << " ──\n";
            for (int t : thread_counts) {
                run_read_bench(n_cols, comp, cname, t, rpath);
            }

            ::unlink(rpath.c_str());
            std::cout << "\n";
        }
    }

    write_csv();
    libtsfile_destroy();
    return 0;
}
