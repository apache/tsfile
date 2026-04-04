/*
 * E5-2: Time Filtering + Late Materialization Benchmark
 *
 * End-to-end filtering test at TsFile layer (I/O + decompression + decoding
 * + filtering). Data is warmed to OS page cache before timing.
 *
 * Two metrics (controlled by --metric):
 *   throughput: end-to-end read throughput at varying time selectivity
 *   decode_ratio: proportion of rows actually decoded vs time window
 *
 * Parameters:
 *   W0 baseline: 200M rows, 10 devices, 8 INT64 fields, TS_2DIFF + SNAPPY
 *   Selectivities: 1%, 10%, 50%, 90%, 100%
 *   Compile C1 (SIMD OFF) vs C3 (SIMD ON)
 *
 * Output:
 *   filter_results.csv: config, selectivity_pct, throughput_mrows_s, time_s,
 * rows_read
 *
 * Build:
 *   cmake -DENABLE_SIMD=OFF -DENABLE_THREADS=OFF -DBUILD_TEST=OFF ..
 *   cmake --build . --target filter_bench
 *
 * Run:
 *   ./filter_bench [mode] [total_rows] [csv_dir]
 *   mode: "write" | "read" | "all" (default: all)
 */

#include <fcntl.h>
#include <sys/stat.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsblock/tsblock.h"
#include "common/tsblock/vector/vector.h"
#include "file/write_file.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

using Clock = std::chrono::high_resolution_clock;

// ─── Configuration ──────────────────────────────────────────────────────────

static const char* kTable = "filter_bench";
static const int kNumDevices = 10;
static const int kNumFields = 8;
static const int kBatchSize = 65536;

static int64_t gTotalRows = 200000000;  // 200M
static std::string gCsvDir = ".";
static std::string gTsPath;
static std::string gMode = "all";

static double gSelectivities[] = {0.01, 0.10, 0.50, 0.90, 1.00};
static const int kNumSelectivities = 5;
static const int kWarmupReads = 1;
static const int kBenchReads = 3;

#define HANDLE_ERR(expr)                                               \
    do {                                                               \
        int _e = (expr);                                               \
        if (_e != 0) {                                                 \
            std::cerr << "ERROR " << _e << " at " << __LINE__ << "\n"; \
            return _e;                                                 \
        }                                                              \
    } while (0)

// ─── Helpers ────────────────────────────────────────────────────────────────

static std::string simd_label() {
#ifdef ENABLE_SIMD
    return "C3";
#else
    return "C1";
#endif
}

static std::string device_name(int i) { return "device_" + std::to_string(i); }

static int64_t sum_vec_int64(common::Vector* vec, uint32_t rows) {
    int64_t sum = 0;
    if (!vec->has_null()) {
        const int64_t* p =
            reinterpret_cast<const int64_t*>(vec->get_value_data().get_data());
        for (uint32_t r = 0; r < rows; ++r) sum += p[r];
    }
    return sum;
}

static int find_vec_idx(storage::ResultSet* rs, const std::string& name) {
    auto meta = rs->get_metadata();
    for (int i = 1; i <= static_cast<int>(meta->get_column_count()); ++i) {
        if (meta->get_column_name(i) == name) return i - 1;
    }
    return -1;
}

// ─── Write test data ────────────────────────────────────────────────────────

static int write_data() {
    std::cout << "Writing " << gTotalRows / 1000000 << "M rows ...\n";

    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    HANDLE_ERR(file.create(gTsPath.c_str(), flags, 0666));

    // Schema: 2 TAG + 8 INT64 FIELD (TS_2DIFF + SNAPPY)
    std::vector<common::ColumnSchema> cols;
    cols.emplace_back("id1", common::STRING, common::UNCOMPRESSED,
                      common::PLAIN, common::ColumnCategory::TAG);
    cols.emplace_back("id2", common::STRING, common::UNCOMPRESSED,
                      common::PLAIN, common::ColumnCategory::TAG);
    for (int f = 0; f < kNumFields; f++) {
        std::string name = "s" + std::to_string(f + 1);
        cols.emplace_back(name, common::INT64, common::SNAPPY, common::TS_2DIFF,
                          common::ColumnCategory::FIELD);
    }

    auto* schema = new storage::TableSchema(std::string(kTable), cols);
    auto* writer =
        new storage::TsFileTableWriter(&file, schema, 128ULL * 1024 * 1024);

    int64_t rows_per_dev = gTotalRows / kNumDevices;
    const uint32_t batch_cap = kBatchSize;

    // Build column names/types/categories for Tablet
    std::vector<std::string> col_names;
    std::vector<common::TSDataType> col_types;
    std::vector<common::ColumnCategory> col_cats;
    col_names.push_back("id1");
    col_types.push_back(common::STRING);
    col_cats.push_back(common::ColumnCategory::TAG);
    col_names.push_back("id2");
    col_types.push_back(common::STRING);
    col_cats.push_back(common::ColumnCategory::TAG);
    for (int f = 0; f < kNumFields; f++) {
        col_names.push_back("s" + std::to_string(f + 1));
        col_types.push_back(common::INT64);
        col_cats.push_back(common::ColumnCategory::FIELD);
    }

    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int> noise(-100, 100);
    auto t0 = Clock::now();

    for (int dev = 0; dev < kNumDevices; dev++) {
        std::string dev_id = device_name(dev);
        int64_t dev_base = dev * rows_per_dev;

        for (int64_t off = 0; off < rows_per_dev;) {
            uint32_t n = static_cast<uint32_t>(
                std::min<int64_t>(batch_cap, rows_per_dev - off));

            storage::Tablet tablet(kTable, col_names, col_types, col_cats,
                                   std::max(n, 1u));

            for (uint32_t i = 0; i < n; i++) {
                int64_t ts = dev_base + off + i;
                tablet.add_timestamp(i, ts);
                tablet.add_value(i, "id1", dev_id.c_str());
                tablet.add_value(i, "id2", "tag_b");
                for (int f = 0; f < kNumFields; f++) {
                    std::string fname = "s" + std::to_string(f + 1);
                    int64_t val = ts * (f + 1) + noise(rng);
                    tablet.add_value(i, fname, val);
                }
            }

            HANDLE_ERR(writer->write_table(tablet));
            off += n;
        }

        double elapsed =
            std::chrono::duration<double>(Clock::now() - t0).count();
        std::cout << "\r  device " << dev + 1 << "/" << kNumDevices << "  ("
                  << std::fixed << std::setprecision(1) << elapsed << "s)"
                  << std::flush;
    }

    HANDLE_ERR(writer->flush());
    HANDLE_ERR(writer->close());
    delete writer;
    delete schema;

    double total = std::chrono::duration<double>(Clock::now() - t0).count();
    struct stat st;
    stat(gTsPath.c_str(), &st);
    std::cout << "\n  Write done: " << std::fixed << std::setprecision(1)
              << total << " s, file " << st.st_size / 1024 / 1024 << " MB\n\n";
    return 0;
}

// ─── Read benchmark ─────────────────────────────────────────────────────────

struct FilterResult {
    std::string config;
    int selectivity_pct;
    double throughput_mrows;
    double time_s;
    int64_t rows_read;
    int64_t checksum;
};

static std::vector<FilterResult> gResults;

static int bench_read(double selectivity, int sel_pct) {
    // Time range: all devices share the same timestamp space per-device.
    // Device d has timestamps [d*rows_per_dev, (d+1)*rows_per_dev).
    // For selectivity S, we query [dev_base, dev_base + rows_per_dev * S)
    // for each device. But TsFile queries by global time range.
    // Since all device timestamps are disjoint, querying [0, total*S) captures
    // ~S fraction from the first devices, and nothing from later ones.
    //
    // Better approach: query full time range but per-device: not needed here.
    // Since all 10 devices use disjoint time ranges [0, rows_per_dev),
    // [rows_per_dev, 2*rows_per_dev) etc., a global time filter
    // effectively hits all devices proportionally if we pick the right range.
    //
    // Actually, we query [dev_base, dev_base + sel_range) for device 0 only,
    // times kNumDevices. But simpler: just set ts_end = total * selectivity.
    // This selects first S*total rows across the file.
    int64_t ts_end;
    if (selectivity >= 1.0) {
        ts_end = gTotalRows;
    } else {
        ts_end = static_cast<int64_t>(gTotalRows * selectivity);
        if (ts_end == 0) ts_end = 1;
    }

    std::string config = simd_label();

    double best_time = 1e18;
    int64_t best_rows = 0;
    int64_t best_sum = 0;

    // Column names to query
    std::vector<std::string> query_cols;
    query_cols.push_back("id1");
    query_cols.push_back("id2");
    for (int f = 0; f < kNumFields; f++) {
        query_cols.push_back("s" + std::to_string(f + 1));
    }

    for (int r = 0; r < kWarmupReads + kBenchReads; r++) {
        storage::TsFileReader reader;
        if (reader.open(gTsPath) != 0) {
            std::cerr << "Failed to open " << gTsPath << "\n";
            return 1;
        }

        storage::ResultSet* rs = nullptr;
        int ret = reader.query(kTable, query_cols, 0, ts_end, rs, kBatchSize);
        if (ret != 0) {
            std::cerr << "query failed: " << ret << "\n";
            reader.close();
            return ret;
        }

        const int s1_idx = find_vec_idx(rs, "s1");
        int64_t sum = 0;
        int64_t total_rows = 0;

        auto t0 = Clock::now();
        common::TsBlock* block = nullptr;
        while (rs->get_next_tsblock(block) == common::E_OK && block) {
            uint32_t nr = block->get_row_count();
            sum += sum_vec_int64(block->get_vector(s1_idx), nr);
            total_rows += nr;
        }
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();

        rs->close();
        reader.close();

        if (r >= kWarmupReads && sec < best_time) {
            best_time = sec;
            best_rows = total_rows;
            best_sum = sum;
        }
    }

    double throughput = best_rows / best_time / 1e6;
    gResults.push_back(
        {config, sel_pct, throughput, best_time, best_rows, best_sum});

    std::cout << "  sel=" << std::right << std::setw(3) << sel_pct << "%  "
              << std::fixed << std::setprecision(3) << best_time << " s  "
              << std::setprecision(2) << throughput << " M rows/s  "
              << "rows=" << best_rows << "  sum=" << best_sum << "\n";

    return 0;
}

// ─── Row-by-row read (scalar baseline, no batch) ────────────────────────────

static int bench_read_row(double selectivity, int sel_pct) {
    int64_t ts_end;
    if (selectivity >= 1.0) {
        ts_end = gTotalRows;
    } else {
        ts_end = static_cast<int64_t>(gTotalRows * selectivity);
        if (ts_end == 0) ts_end = 1;
    }

    std::string config = "ROW";

    double best_time = 1e18;
    int64_t best_rows = 0;
    int64_t best_sum = 0;

    std::vector<std::string> query_cols;
    query_cols.push_back("id1");
    query_cols.push_back("id2");
    for (int f = 0; f < kNumFields; f++) {
        query_cols.push_back("s" + std::to_string(f + 1));
    }

    for (int r = 0; r < kWarmupReads + kBenchReads; r++) {
        storage::TsFileReader reader;
        if (reader.open(gTsPath) != 0) {
            std::cerr << "Failed to open " << gTsPath << "\n";
            return 1;
        }

        storage::ResultSet* rs = nullptr;
        // No batch_size → row-by-row mode
        int ret = reader.query(kTable, query_cols, 0, ts_end, rs);
        if (ret != 0) {
            std::cerr << "query failed: " << ret << "\n";
            reader.close();
            return ret;
        }

        int64_t sum = 0;
        int64_t total_rows = 0;

        auto t0 = Clock::now();
        bool has_next = false;
        while (rs->next(has_next) == common::E_OK && has_next) {
            if (!rs->is_null("s1")) {
                sum += rs->get_value<int64_t>("s1");
            }
            total_rows++;
        }
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();

        rs->close();
        reader.close();

        if (r >= kWarmupReads && sec < best_time) {
            best_time = sec;
            best_rows = total_rows;
            best_sum = sum;
        }
    }

    double throughput = best_rows / best_time / 1e6;
    gResults.push_back(
        {config, sel_pct, throughput, best_time, best_rows, best_sum});

    std::cout << "  sel=" << std::right << std::setw(3) << sel_pct << "%  "
              << std::fixed << std::setprecision(3) << best_time << " s  "
              << std::setprecision(2) << throughput << " M rows/s  "
              << "rows=" << best_rows << "  [ROW]\n";

    return 0;
}

// ─── CSV output ─────────────────────────────────────────────────────────────

static void write_csv() {
    std::string path = gCsvDir + "/filter_results_" + simd_label() + ".csv";
    std::ofstream csv(path);
    csv << "config,selectivity_pct,throughput_mrows_s,time_s,rows_read,"
           "checksum\n";

    for (auto& r : gResults) {
        csv << r.config << "," << r.selectivity_pct << "," << std::fixed
            << std::setprecision(2) << r.throughput_mrows << ","
            << std::setprecision(4) << r.time_s << "," << r.rows_read << ","
            << r.checksum << "\n";
    }
    std::cout << "\nCSV: " << path << "\n";
}

// ─── Main ───────────────────────────────────────────────────────────────────

static void print_build_config() {
    std::cout << "=== E5-2: Time Filter + Late Materialization Benchmark ===\n"
              << "  Config:     " << simd_label() << "\n"
#ifdef ENABLE_SIMD
              << "  SIMD:       ON\n"
#else
              << "  SIMD:       OFF\n"
#endif
              << "  total_rows: " << gTotalRows << "\n"
              << "  devices:    " << kNumDevices << "\n"
              << "  fields:     " << kNumFields << " x INT64\n"
              << "  batch_size: " << kBatchSize << "\n"
              << "  csv_dir:    " << gCsvDir << "\n\n";
}

int main(int argc, char* argv[]) {
    if (argc > 1) gMode = argv[1];
    if (argc > 2) gTotalRows = std::atoll(argv[2]);
    if (argc > 3) gCsvDir = argv[3];

    gTsPath = gCsvDir + "/filter_bench.tsfile";

    print_build_config();
    storage::libtsfile_init();

    // Write phase
    if (gMode == "write" || gMode == "all") {
        int ret = write_data();
        if (ret != 0) return ret;
    }

    if (gMode == "write") {
        storage::libtsfile_destroy();
        return 0;
    }

    // Check file exists
    struct stat st;
    if (stat(gTsPath.c_str(), &st) != 0) {
        std::cerr << "Error: " << gTsPath
                  << " not found. Run with 'write' first.\n";
        return 1;
    }

    // Row-by-row baseline (scalar, no batch)
    std::cout << "── Row-by-row Read (Scalar Baseline) ──\n";
    for (int s = 0; s < kNumSelectivities; s++) {
        int sel_pct = static_cast<int>(gSelectivities[s] * 100);
        int ret = bench_read_row(gSelectivities[s], sel_pct);
        if (ret != 0) return ret;
    }

    // Batch read (with SIMD if enabled)
    std::cout << "\n── Batch Read (" << simd_label() << ") ──\n";
    for (int s = 0; s < kNumSelectivities; s++) {
        int sel_pct = static_cast<int>(gSelectivities[s] * 100);
        int ret = bench_read(gSelectivities[s], sel_pct);
        if (ret != 0) return ret;
    }

    write_csv();
    storage::libtsfile_destroy();
    return 0;
}
