/*
 * E3-2: Read memory formula precision with PLAIN + UNCOMPRESSED.
 *
 * Validates the read memory formula:
 *   M_read ≈ M_fixed + batch_size × s_row + N_cols × C_page
 *
 * With PLAIN + UNCOMPRESSED data, the decompression path is trivial
 * and formula predictions should be highly accurate.
 *
 * Prerequisite: run write_precision first to generate experiment_plain.tsfile.
 *
 * Build:
 *   cmake -DENABLE_MEM_STAT=ON -DBUILD_TEST=OFF ..
 *   cmake --build . --target read_precision
 *
 * Usage:
 *   ./read_precision [tsfile_path] [csv_path]
 */

#include <sys/resource.h>
#include <sys/time.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/config/config.h"
#include "common/tsblock/tsblock.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"

using namespace storage;
using namespace common;

static const char* kTable = "mem_bench";
static const char* kTagCols[] = {"id1", "id2"};
static const int kNumTags = 2;
static const char* kFieldCols[] = {"s1", "s2", "s3", "s4",
                                   "s5", "s6", "s7", "s8"};
static const int kMaxFields = 8;

// sizeof of each field column type (bytes)
static const int kFieldSizes[] = {4, 4, 8, 8, 4, 4, 8, 8};
static const int kTagSize = 8;  // STRING stored as pointer in TsBlock

// C_page: decompression buffer per column (default
// page_writer_max_memory_bytes_)
static const int64_t kDefaultCPage = 128LL * 1024;

static const int kModCount = __LAST_MOD_ID;

// -----------------------------------------------------------------------
// ModStat helpers
// -----------------------------------------------------------------------
static int64_t total_modstat() {
    auto& ms = ModStat::get_instance();
    int64_t t = 0;
    for (int i = 0; i < kModCount; i++) t += ms.get_stat(i);
    return t;
}

static int64_t g_baseline = 0;
static int64_t g_peak_above_baseline = 0;

static void begin_query_mem() {
    g_baseline = total_modstat();
    g_peak_above_baseline = 0;
}

static void update_peak() {
    int64_t delta = total_modstat() - g_baseline;
    if (delta > g_peak_above_baseline) g_peak_above_baseline = delta;
}

static int64_t query_peak_mem() { return g_peak_above_baseline; }

// -----------------------------------------------------------------------
// CSV header / row
// -----------------------------------------------------------------------
static void write_csv_header(std::ofstream& csv) {
    csv << "n_cols,batch_size,rows_read,"
           "peak_actual,m_formula,error_pct\n";
}

// -----------------------------------------------------------------------
// main
// -----------------------------------------------------------------------
int main(int argc, char* argv[]) {
    std::string tsfile_path =
        "/Users/colin/dev/tsfile_b1/cpp/experiment/experiment_plain.tsfile";
    std::string csv_path = "read_precision.csv";

    if (argc > 1) tsfile_path = argv[1];
    if (argc > 2) csv_path = argv[2];

    libtsfile_init();

    std::cout << "=== E3-2: Read Precision (PLAIN + UNCOMPRESSED) ===\n"
              << "  tsfile:  " << tsfile_path << "\n"
              << "  csv:     " << csv_path << "\n"
              << "  c_page:  " << kDefaultCPage / 1024 << " KB\n\n";

    // Parameter matrix
    int col_counts[] = {1, 2, 4, 6, 8};
    uint32_t batch_sizes[] = {1024, 4096, 16384, 65536};

    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    write_csv_header(csv);

    std::cout << std::setw(8) << "N_cols" << std::setw(10) << "batch"
              << std::setw(14) << "rows_read" << std::setw(14) << "peak_actual"
              << std::setw(14) << "m_formula" << std::setw(10) << "error%"
              << "\n"
              << std::string(70, '-') << "\n";

    int64_t t_start_ts = 0;
    int64_t t_end_ts = INT64_MAX;

    for (int n_field : col_counts) {
        for (uint32_t batch_size : batch_sizes) {
            // Build column list
            std::vector<std::string> cols;
            for (int t = 0; t < kNumTags; t++) cols.push_back(kTagCols[t]);
            for (int f = 0; f < n_field && f < kMaxFields; f++)
                cols.push_back(kFieldCols[f]);

            // s_row = 8 (time) + 2*kTagSize + sum(field sizes)
            int64_t s_row = 8 + static_cast<int64_t>(kNumTags) * kTagSize;
            for (int f = 0; f < n_field; f++) s_row += kFieldSizes[f];

            int n_total_cols = kNumTags + n_field;

            TsFileReader reader;
            if (reader.open(tsfile_path) != 0) {
                std::cerr << "Failed to open: " << tsfile_path << "\n";
                return 1;
            }

            begin_query_mem();

            ResultSet* rs = nullptr;
            int ret = reader.query(kTable, cols, t_start_ts, t_end_ts, rs,
                                   batch_size);
            if (ret != 0 || rs == nullptr) {
                std::cerr << "query failed: " << ret << "\n";
                reader.close();
                continue;
            }

            TsBlock* block = nullptr;
            int64_t rows = 0;
            while (rs->get_next_tsblock(block) == E_OK && block) {
                rows += block->get_row_count();
                update_peak();
            }
            update_peak();

            int64_t peak_actual = query_peak_mem();

            rs->close();
            reader.close();

            // Formula: M_fixed is measured as the peak with minimal config
            // For comparison, we compute the variable part:
            //   M_data = batch_size * s_row
            //   M_page = n_total_cols * c_page
            //   M_formula_var = M_data + M_page
            int64_t m_data = static_cast<int64_t>(batch_size) * s_row;
            int64_t m_page = static_cast<int64_t>(n_total_cols) * kDefaultCPage;
            int64_t m_formula = m_data + m_page;

            double err =
                m_formula > 0
                    ? 100.0 * std::abs(peak_actual - m_formula) / m_formula
                    : 0.0;

            csv << n_field << "," << batch_size << "," << rows << ","
                << peak_actual << "," << m_formula << "," << std::fixed
                << std::setprecision(2) << err << "\n";

            std::cout << std::setw(8) << n_field << std::setw(10) << batch_size
                      << std::setw(14) << rows << std::setw(14) << peak_actual
                      << std::setw(14) << m_formula << std::setw(9)
                      << std::fixed << std::setprecision(2) << err << "%\n";
        }
    }

    csv.close();
    std::cout << "\nCSV: " << csv_path << "\n";

    libtsfile_destroy();
    return 0;
}
