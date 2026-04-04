/*
 * E3-4: EOQ optimal strategy validation.
 *
 * Proves the U-shaped memory curve: M_peak = M_init + s*F + (R/F)*D*b
 * and that the minimum occurs at F_opt = sqrt(R*D*b/s).
 *
 * Experiment design:
 *   1. Fix R (total rows/device), D (devices), schema (→ s, b).
 *   2. Sweep F (rows per flush) across a wide range including F_opt.
 *   3. For each F: write all data with formula-controlled flushing,
 *      record measured M_data_peak, M_meta_peak, M_total_peak.
 *   4. Also record formula predictions for comparison.
 *
 * Schema: 4 DOUBLE fields, PLAIN + UNCOMPRESSED (incl. timestamp).
 *   s_data  = 8 + 4*8 = 40 bytes/row (aligned, shared timestamp)
 *   b       = 4*104 + 96 = 512 bytes/device/flush
 *
 * Build:
 *   cmake -DBUILD_TEST=OFF ..
 *   cmake --build . --target eoq_validate
 *
 * Usage:
 *   ./eoq_validate [csv_path] [base_path]
 */

#include <fcntl.h>

#include <algorithm>
#include <chrono>
#include <climits>
#include <cmath>
#include <cstdint>
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

using namespace storage;
using namespace common;

static const char* kTable = "eoq_bench";
static const int kNumDevices = 20;
static const int kNumFields = 8;

// Formula constants (aligned, PLAIN+UNCOMPRESSED, 8 DOUBLE fields)
static const int64_t kSData = 8 + kNumFields * 8;  // 72 bytes/row
static const int64_t kB = kNumFields * 104 + 96;   // 928 bytes/device/flush
static const int64_t kMInit = 900LL * 1024;        // ~900 KB

static std::string device_name(int i) { return "dev_" + std::to_string(i); }

static std::shared_ptr<TableSchema> make_schema() {
    std::vector<ColumnSchema> cols;
    cols.push_back(
        ColumnSchema("id", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG));
    for (int i = 0; i < kNumFields; i++) {
        cols.push_back(ColumnSchema("v" + std::to_string(i), DOUBLE,
                                    UNCOMPRESSED, PLAIN,
                                    ColumnCategory::FIELD));
    }
    return std::make_shared<TableSchema>(std::string(kTable), cols);
}

static Tablet make_tablet(uint32_t n) {
    std::vector<std::string> names = {"id"};
    std::vector<TSDataType> types = {STRING};
    std::vector<ColumnCategory> cats = {ColumnCategory::TAG};
    for (int i = 0; i < kNumFields; i++) {
        names.push_back("v" + std::to_string(i));
        types.push_back(DOUBLE);
        cats.push_back(ColumnCategory::FIELD);
    }
    return Tablet(kTable, names, types, cats, n);
}

// -----------------------------------------------------------------------
// Run one F value: write total_rows with flush every F rows.
// Returns measured peak M_data, M_meta, M_total.
// -----------------------------------------------------------------------
struct RunResult {
    int64_t F;             // rows per flush
    int64_t K;             // actual flush count
    int file_count;        // files created
    int64_t peak_m_data;   // max M_data observed (direct)
    int64_t peak_m_meta;   // max M_meta observed (direct)
    int64_t peak_m_total;  // max (M_data + M_meta) observed
    // Formula predictions
    int64_t formula_m_data;   // s * F
    int64_t formula_m_meta;   // K * D * b  (K = ceil(R/F) * D)
    int64_t formula_m_total;  // M_init + formula_m_data + formula_m_meta
};

static RunResult run_one_F(int64_t F, int64_t rows_per_dev,
                           const std::string& base_path) {
    // Disable built-in auto-flush
    g_config_value_.chunk_group_size_threshold_ = INT64_MAX / 2;

    int64_t total_rows = rows_per_dev * kNumDevices;

    // We'll use ONE file (no rotation) to measure peak M_meta accurately.
    std::string path = base_path + ".tsfile";
    WriteFile wf;
    wf.create(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);

    auto schema = make_schema();
    TsFileWriter writer;
    writer.init(&wf);
    writer.register_table(schema);

    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> nd(-1.0, 1.0);

    int64_t rows_since_flush = 0;
    int64_t flush_count = 0;
    int64_t peak_data = 0, peak_meta = 0, peak_total = 0;
    uint32_t batch_cap = 65536;

    for (int dev = 0; dev < kNumDevices; dev++) {
        std::string dev_id = device_name(dev);
        int64_t dev_base = static_cast<int64_t>(dev) * rows_per_dev;

        for (int64_t off = 0; off < rows_per_dev;) {
            // Limit batch to not overshoot F
            int64_t remaining_before_flush = F - rows_since_flush;
            uint32_t n = static_cast<uint32_t>(std::min<int64_t>(
                std::min<int64_t>(batch_cap, remaining_before_flush),
                rows_per_dev - off));
            auto tablet = make_tablet(n);

            for (uint32_t i = 0; i < n; i++) {
                int64_t ts = dev_base + off + i;
                tablet.add_timestamp(i, ts);
                tablet.add_value(i, "id", dev_id.c_str());
                for (int f = 0; f < kNumFields; f++) {
                    tablet.add_value(i, ("v" + std::to_string(f)).c_str(),
                                     ts * 0.1 + nd(rng));
                }
            }

            writer.write_table(tablet);
            rows_since_flush += n;
            off += n;

            // Measure
            int64_t md = writer.calculate_mem_size_for_all_group();
            int64_t mm = writer.calculate_meta_mem_size();
            if (md > peak_data) peak_data = md;
            if (mm > peak_meta) peak_meta = mm;
            if (md + mm > peak_total) peak_total = md + mm;

            // Formula-controlled flush: flush every F rows
            if (rows_since_flush >= F) {
                writer.flush();
                flush_count++;
                rows_since_flush = 0;

                // Re-measure after flush
                mm = writer.calculate_meta_mem_size();
                if (mm > peak_meta) peak_meta = mm;
            }
        }
    }

    // Final flush
    if (rows_since_flush > 0) {
        writer.flush();
        flush_count++;
    }
    int64_t final_meta = writer.calculate_meta_mem_size();
    if (final_meta > peak_meta) peak_meta = final_meta;

    writer.close();

    RunResult r;
    r.F = F;
    r.K = flush_count;
    r.file_count = 1;
    r.peak_m_data = peak_data;
    r.peak_m_meta = peak_meta;
    r.peak_m_total = peak_total;
    // Formula: in sequential mode, each flush writes ONE device → meta += b
    r.formula_m_data = kSData * F;
    r.formula_m_meta = flush_count * kB;
    r.formula_m_total = kMInit + r.formula_m_data + r.formula_m_meta;
    return r;
}

int main(int argc, char* argv[]) {
    std::string csv_path = "eoq_validate.csv";
    std::string base_path = "/tmp/eoq_validate";

    if (argc > 1) csv_path = argv[1];
    if (argc > 2) base_path = argv[2];

    libtsfile_init();
    // Force PLAIN + UNCOMPRESSED for timestamp
    g_config_value_.time_encoding_type_ = PLAIN;
    g_config_value_.time_compress_type_ = UNCOMPRESSED;

    int64_t R = 2000000;  // rows per device (2M × 20 dev = 40M total)

    // Compute F_opt
    double F_opt_d = std::sqrt((double)R * kNumDevices * kB / kSData);
    int64_t F_opt = static_cast<int64_t>(F_opt_d);
    double M_min_d =
        kMInit + 2.0 * std::sqrt((double)R * kSData * kNumDevices * kB);

    std::cout << "=== EOQ Validation Experiment ===\n"
              << "  R (rows/dev):  " << R << "\n"
              << "  D (devices):   " << kNumDevices << "\n"
              << "  s_data:        " << kSData << " bytes/row\n"
              << "  b:             " << kB << " bytes/dev/flush\n"
              << "  M_init:        " << kMInit / 1024 << " KB\n"
              << "  F_opt:         " << F_opt << " rows\n"
              << "  M_min:         " << std::fixed << std::setprecision(0)
              << M_min_d / (1024 * 1024) << " MB\n"
              << "  csv:           " << csv_path << "\n\n";

    // Sweep F: from F_opt/8 to F_opt*8, logarithmic steps
    std::vector<int64_t> F_values;
    for (double scale = 0.125; scale <= 8.01; scale *= std::sqrt(2.0)) {
        int64_t f = std::max<int64_t>(100, static_cast<int64_t>(F_opt * scale));
        F_values.push_back(f);
    }
    // Ensure F_opt itself is included
    F_values.push_back(F_opt);
    // Sort and deduplicate
    std::sort(F_values.begin(), F_values.end());
    F_values.erase(std::unique(F_values.begin(), F_values.end()),
                   F_values.end());

    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    csv << "F,K,file_count,"
           "peak_m_data,peak_m_meta,peak_m_total,"
           "formula_m_data,formula_m_meta,formula_m_total,"
           "F_opt,M_min\n";

    std::cout << std::setw(10) << "F" << std::setw(8) << "K" << std::setw(14)
              << "peak_total" << std::setw(14) << "formula_total"
              << std::setw(10) << "error%" << std::setw(12) << "F/F_opt"
              << "\n"
              << std::string(68, '-') << "\n";

    for (int64_t F : F_values) {
        auto r = run_one_F(F, R, base_path);

        double err = r.formula_m_total > 0
                         ? 100.0 * (r.peak_m_total - r.formula_m_total) /
                               r.formula_m_total
                         : 0;

        csv << r.F << "," << r.K << "," << r.file_count << "," << r.peak_m_data
            << "," << r.peak_m_meta << "," << r.peak_m_total << ","
            << r.formula_m_data << "," << r.formula_m_meta << ","
            << r.formula_m_total << "," << F_opt << ","
            << static_cast<int64_t>(M_min_d) << "\n";

        std::cout << std::setw(10) << r.F << std::setw(8) << r.K
                  << std::setw(14) << r.peak_m_total << std::setw(14)
                  << r.formula_m_total << std::setw(9) << std::fixed
                  << std::setprecision(1) << err << "%" << std::setw(11)
                  << std::setprecision(2) << (double)F / F_opt << "\n";
    }

    csv.close();
    std::cout << "\nF_opt = " << F_opt << ", M_min = " << std::fixed
              << std::setprecision(1) << M_min_d / (1024 * 1024) << " MB\n"
              << "CSV: " << csv_path << "\n";

    libtsfile_destroy();
    return 0;
}
