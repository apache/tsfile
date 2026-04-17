/*
 * E3-4: EOQ optimal strategy validation.
 *
 * Proves the U-shaped memory curve for mixed-device writes:
 *   M_peak = M_init + s*F + ceil(R*D/F)*D_active*b
 * When every flush window covers all devices, D_active ~= D and
 *   F_opt = sqrt(R*D*D*b/s).
 *
 * Experiment design:
 *   1. Fix R (rows/device), D (devices), schema (-> s, b).
 *   2. Sweep F (global rows per flush) across a wide range including F_opt.
 *   3. For each F: write interleaved multi-device tablets,
 *      flush every F global rows, record active devices per flush window,
 *      record measured M_data_peak, M_meta_peak, M_total_peak.
 *   4. Also record formula predictions for comparison.
 *
 * Schema: 8 DOUBLE fields, PLAIN + UNCOMPRESSED (incl. timestamp).
 *   s_data  = 8 + 8*8 = 72 bytes/row (aligned, shared timestamp)
 *   b       = 8*104 + 96 = 928 bytes/device/flush
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

#include "common/allocator/alloc_base.h"
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

static std::string device_name(int i) { return "dev_" + std::to_string(i); }

static const int kModCount = __LAST_MOD_ID;

static int64_t total_modstat() {
    auto& ms = ModStat::get_instance();
    int64_t total = 0;
    for (int i = 0; i < kModCount; i++) {
        total += ms.get_stat(i);
    }
    return total;
}

static int64_t g_measured_m_init = -1;

static std::shared_ptr<TableSchema> make_schema() {
    std::vector<ColumnSchema> cols;
    cols.push_back(
        ColumnSchema("id", STRING, UNCOMPRESSED, PLAIN, ColumnCategory::TAG));
    for (int i = 0; i < kNumFields; i++) {
        cols.push_back(ColumnSchema("v" + std::to_string(i), DOUBLE,
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
    int64_t K;             // writer.flush() calls
    int64_t active_sum;    // sum of active devices across flush windows
    int file_count;        // files created
    int64_t m_init;        // measured writer init ModStat delta
    int64_t peak_m_data;   // max M_data observed (direct)
    int64_t peak_m_meta;   // max M_meta observed (direct)
    int64_t peak_m_total;  // m_init + max(M_data + M_meta)
    // Formula predictions
    int64_t formula_m_data;   // s * F
    int64_t formula_m_meta;   // active_sum * b
    int64_t formula_m_total;  // m_init + formula_m_data + formula_m_meta
};

static RunResult run_one_F(int64_t F, int64_t rows_per_dev,
                           const std::string& base_path) {
    // Force PLAIN + UNCOMPRESSED to match formula assumptions.
    g_config_value_.time_encoding_type_ = PLAIN;
    g_config_value_.int32_encoding_type_ = PLAIN;
    g_config_value_.int64_encoding_type_ = PLAIN;
    g_config_value_.float_encoding_type_ = PLAIN;
    g_config_value_.double_encoding_type_ = PLAIN;
    g_config_value_.default_compression_type_ = UNCOMPRESSED;
    g_config_value_.time_compress_type_ = UNCOMPRESSED;

    // Disable built-in auto-flush
    g_config_value_.chunk_group_size_threshold_ = INT64_MAX / 2;

    int64_t total_rows = rows_per_dev * kNumDevices;

    // We'll use ONE file (no rotation) to measure peak M_meta accurately.
    std::string path = base_path + ".tsfile";
    WriteFile wf;
    wf.create(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);

    int64_t mod_before_init = total_modstat();
    auto schema = make_schema();
    TsFileWriter writer;
    writer.init(&wf);
    writer.register_table(schema);
    int64_t mod_after_init = total_modstat();
    int64_t measured_init =
        std::max<int64_t>(0, mod_after_init - mod_before_init);
    if (g_measured_m_init < 0) {
        g_measured_m_init = measured_init;
    }
    int64_t m_init = g_measured_m_init;

    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> nd(-1.0, 1.0);

    int64_t flush_count = 0;
    int64_t active_device_sum = 0;
    int64_t peak_data = 0, peak_meta = 0, peak_total = 0;
    uint32_t batch_cap = 65536;
    int64_t rows_since_flush = 0;
    int64_t rows_written_total = 0;
    int next_dev = 0;
    std::vector<int64_t> rows_written_per_dev(kNumDevices, 0);
    std::vector<unsigned char> active_devices(kNumDevices, 0);

    auto active_device_count = [&]() -> int64_t {
        int64_t count = 0;
        for (auto active : active_devices) {
            if (active) count++;
        }
        return count;
    };

    auto clear_active_devices = [&]() {
        std::fill(active_devices.begin(), active_devices.end(), 0);
    };

    auto update_peak = [&]() {
        int64_t md = writer.calculate_mem_size_for_all_group();
        int64_t mm = writer.calculate_meta_mem_size();
        if (md > peak_data) peak_data = md;
        if (mm > peak_meta) peak_meta = mm;
        if (md + mm > peak_total) peak_total = md + mm;
    };

    auto do_flush = [&]() {
        int64_t active_count = active_device_count();
        writer.flush();
        flush_count++;
        active_device_sum += active_count;
        rows_since_flush = 0;
        clear_active_devices();

        int64_t mm = writer.calculate_meta_mem_size();
        if (mm > peak_meta) peak_meta = mm;
        if (mm > peak_total) peak_total = mm;
    };

    while (rows_written_total < total_rows) {
        int64_t remaining_before_flush = F - rows_since_flush;
        int64_t remaining_total = total_rows - rows_written_total;
        uint32_t n = static_cast<uint32_t>(std::min<int64_t>(
            std::min<int64_t>(batch_cap, remaining_before_flush),
            remaining_total));
        auto tablet = make_tablet(n);

        for (uint32_t i = 0; i < n; i++) {
            int searched = 0;
            while (rows_written_per_dev[next_dev] >= rows_per_dev &&
                   searched < kNumDevices) {
                next_dev = (next_dev + 1) % kNumDevices;
                searched++;
            }
            int dev = next_dev;
            int64_t dev_row = rows_written_per_dev[dev];
            int64_t ts = static_cast<int64_t>(dev) * rows_per_dev + dev_row;
            std::string dev_id = device_name(dev);

            tablet.add_timestamp(i, ts);
            tablet.add_value(i, "id", dev_id.c_str());
            for (int f = 0; f < kNumFields; f++) {
                tablet.add_value(i, ("v" + std::to_string(f)).c_str(),
                                 ts * 0.1 + nd(rng));
            }

            rows_written_per_dev[dev]++;
            rows_written_total++;
            active_devices[dev] = 1;
            next_dev = (next_dev + 1) % kNumDevices;
        }

        writer.write_table(tablet);
        rows_since_flush += n;
        update_peak();

        // Formula-controlled flush: flush every F global rows.
        if (rows_since_flush >= F) {
            do_flush();
        }
    }

    // Final flush for the last partial global window.
    if (rows_since_flush > 0) {
        do_flush();
    }

    writer.close();

    RunResult r;
    r.F = F;
    r.K = flush_count;
    r.active_sum = active_device_sum;
    r.file_count = 1;
    r.m_init = m_init;
    r.peak_m_data = peak_data;
    r.peak_m_meta = peak_meta;
    r.peak_m_total = m_init + peak_total;
    // Formula: measured writer init ModStat delta + data buffer + metadata
    // accumulated by active device chunk groups.
    r.formula_m_data = kSData * std::min<int64_t>(F, total_rows);
    r.formula_m_meta = active_device_sum * kB;
    r.formula_m_total = m_init + r.formula_m_data + r.formula_m_meta;
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
    // Use a very large page capacity so that page seals never occur
    // mid-flush — otherwise the sealed page data moves from the page
    // writer into the chunk ByteStream and calculate_mem_size_for_all_group()
    // may undercount, creating artefacts in the U-curve.
    g_config_value_.page_writer_max_point_num_ = 1000000;
    g_config_value_.page_writer_max_memory_bytes_ = 512 * 1024 * 1024;

    int64_t R = 2000000;  // rows per device (2M × 20 dev = 40M total)

    // Compute F_opt for mixed-device writes. F is global rows per flush and
    // each flush window is designed to cover all devices, so meta growth is
    // ceil(R*D/F) * D * b.
    double total_rows = (double)R * kNumDevices;
    double F_opt_d = std::sqrt(total_rows * kNumDevices * kB / kSData);
    int64_t F_opt = static_cast<int64_t>(F_opt_d);
    double M_min_var_d =
        2.0 * std::sqrt(total_rows * kSData * kNumDevices * kB);

    std::cout << "=== EOQ Validation Experiment ===\n"
              << "  R (rows/dev):  " << R << "\n"
              << "  D (devices):   " << kNumDevices << "\n"
              << "  s_data:        " << kSData << " bytes/row\n"
              << "  b:             " << kB << " bytes/dev/flush\n"
              << "  M_init:        measured from ModStat init delta\n"
              << "  F_opt:         " << F_opt << " rows\n"
              << "  M_min(var):    " << std::fixed << std::setprecision(0)
              << M_min_var_d / (1024 * 1024) << " MB\n"
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
    csv << "F,K,active_sum,file_count,m_init,"
           "peak_m_data,peak_m_meta,peak_m_total,"
           "formula_m_data,formula_m_meta,formula_m_total,"
           "F_opt,M_min_var\n";

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

        csv << r.F << "," << r.K << "," << r.active_sum << "," << r.file_count
            << "," << r.m_init << "," << r.peak_m_data << "," << r.peak_m_meta
            << "," << r.peak_m_total << "," << r.formula_m_data << ","
            << r.formula_m_meta << "," << r.formula_m_total << "," << F_opt
            << "," << static_cast<int64_t>(M_min_var_d) << "\n";

        std::cout << std::setw(10) << r.F << std::setw(8) << r.K
                  << std::setw(14) << r.peak_m_total << std::setw(14)
                  << r.formula_m_total << std::setw(9) << std::fixed
                  << std::setprecision(1) << err << "%" << std::setw(11)
                  << std::setprecision(2) << (double)F / F_opt << "\n";
    }

    csv.close();
    std::cout << "\nF_opt = " << F_opt << ", M_min(var) = " << std::fixed
              << std::setprecision(1) << M_min_var_d / (1024 * 1024) << " MB\n"
              << "CSV: " << csv_path << "\n";

    libtsfile_destroy();
    return 0;
}
