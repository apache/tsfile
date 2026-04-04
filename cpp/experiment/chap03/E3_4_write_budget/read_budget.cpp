/*
 * E3-4: Memory-budgeted read performance (PLAIN + UNCOMPRESSED).
 *
 * Given a memory limit, automatically derives batch_size using:
 *   batch_size = (M_limit - M_fixed - N_cols × C_page) / s_row
 *
 * Reads from tsfile(s) produced by write_budget and measures whether
 * actual peak memory stays within the budget.
 *
 * Build:
 *   cmake -DENABLE_MEM_STAT=ON -DBUILD_TEST=OFF ..
 *   cmake --build . --target read_budget
 *
 * Usage:
 *   ./read_budget [tsfile_path] [mem_limit_mb] [m_fixed_mb] [csv_path]
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
static const int kFieldSizes[] = {4, 4, 8, 8, 4, 4, 8, 8};
static const int kTagSize = 8;
static const int64_t kDefaultCPage = 128LL * 1024;

#ifdef ENABLE_MEM_STAT
static const int kModCount = __LAST_MOD_ID;

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
#endif

// Derive batch_size from memory budget
struct ReadParams {
    int n_field_cols;
    int n_total_cols;
    int64_t s_row;
    int64_t c_page;
    int64_t m_fixed;
    int64_t mem_limit;
    int64_t m_data_budget;
    int32_t batch_size;
};

static ReadParams derive_params(int n_field_cols, int64_t mem_limit,
                                int64_t m_fixed, int64_t c_page) {
    ReadParams p;
    p.n_field_cols = n_field_cols;
    p.n_total_cols = kNumTags + n_field_cols;
    p.c_page = c_page;
    p.m_fixed = m_fixed;
    p.mem_limit = mem_limit;

    p.s_row = 8 + static_cast<int64_t>(kNumTags) * kTagSize;
    for (int i = 0; i < n_field_cols; i++) p.s_row += kFieldSizes[i];

    p.m_data_budget =
        mem_limit - m_fixed - static_cast<int64_t>(p.n_total_cols) * c_page;
    if (p.m_data_budget <= 0) {
        p.batch_size = 1;
    } else {
        int64_t bs = p.m_data_budget / p.s_row;
        p.batch_size = static_cast<int32_t>(bs > INT32_MAX ? INT32_MAX : bs);
        if (p.batch_size < 1) p.batch_size = 1;
    }
    return p;
}

int main(int argc, char* argv[]) {
    std::string tsfile_path =
        "/Users/colin/dev/tsfile_b1/cpp/experiment/experiment_plain.tsfile";
    int64_t mem_limit_mb = 16;
    int64_t m_fixed_mb = 4;
    std::string csv_path = "read_budget.csv";

    if (argc > 1) tsfile_path = argv[1];
    if (argc > 2) mem_limit_mb = std::atoll(argv[2]);
    if (argc > 3) m_fixed_mb = std::atoll(argv[3]);
    if (argc > 4) csv_path = argv[4];

    int64_t mem_limit = mem_limit_mb * 1024LL * 1024;
    int64_t m_fixed = m_fixed_mb * 1024LL * 1024;
    int64_t c_page = kDefaultCPage;

    libtsfile_init();

#ifdef ENABLE_MEM_STAT
    std::cout << "ModStat: ENABLED\n";
#else
    std::cout << "ModStat: DISABLED (build with -DENABLE_MEM_STAT=ON)\n";
#endif

    std::cout << "=== E3-4: Read Budget (PLAIN + UNCOMPRESSED) ===\n"
              << "  tsfile:     " << tsfile_path << "\n"
              << "  mem_limit:  " << mem_limit_mb << " MB\n"
              << "  m_fixed:    " << m_fixed_mb << " MB\n"
              << "  c_page:     " << c_page / 1024 << " KB\n"
              << "  csv:        " << csv_path << "\n\n";

    // Show derived parameters
    std::cout << std::setw(10) << "N_field" << std::setw(10) << "N_total"
              << std::setw(8) << "s_row" << std::setw(14) << "data_budget"
              << std::setw(12) << "batch_size\n"
              << std::string(54, '-') << "\n";

    int field_counts[] = {1, 2, 4, 6, 8};
    for (int n : field_counts) {
        auto p = derive_params(n, mem_limit, m_fixed, c_page);
        std::cout << std::setw(10) << p.n_field_cols << std::setw(10)
                  << p.n_total_cols << std::setw(8) << p.s_row << std::setw(14)
                  << p.m_data_budget << std::setw(12) << p.batch_size << "\n";
    }
    std::cout << "\n";

    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    csv << "n_field_cols,n_total_cols,s_row,c_page,m_fixed_mb,"
           "mem_limit_mb,m_data_budget,"
           "batch_size_derived,"
           "rows_read,"
           "peak_memory_actual,"
           "m_data_formula,"
           "within_budget\n";

    int64_t t_start = 0;
    int64_t t_end = INT64_MAX;

    for (int n_field : field_counts) {
        auto p = derive_params(n_field, mem_limit, m_fixed, c_page);

        std::vector<std::string> cols;
        for (int t = 0; t < kNumTags; t++) cols.push_back(kTagCols[t]);
        for (int f = 0; f < n_field && f < kMaxFields; f++)
            cols.push_back(kFieldCols[f]);

        std::cout << "  [cols=" << std::setw(2) << n_field
                  << " batch=" << std::setw(7) << p.batch_size << "]"
                  << std::flush;

        TsFileReader reader;
        if (reader.open(tsfile_path) != 0) {
            std::cerr << " Failed to open\n";
            continue;
        }

#ifdef ENABLE_MEM_STAT
        begin_query_mem();
#endif

        ResultSet* rs = nullptr;
        int ret = reader.query(kTable, cols, t_start, t_end, rs, p.batch_size);
        if (ret != 0 || rs == nullptr) {
            std::cerr << " query failed: " << ret << "\n";
            reader.close();
            continue;
        }

        TsBlock* block = nullptr;
        int64_t rows = 0;
        while (rs->get_next_tsblock(block) == E_OK && block) {
            rows += block->get_row_count();
#ifdef ENABLE_MEM_STAT
            update_peak();
#endif
        }

#ifdef ENABLE_MEM_STAT
        update_peak();
        int64_t peak_actual = query_peak_mem();
#else
        int64_t peak_actual = -1;
#endif

        rs->close();
        reader.close();

        int64_t m_data_formula = static_cast<int64_t>(p.batch_size) * p.s_row;
        bool within = peak_actual >= 0 && peak_actual <= mem_limit;

        csv << p.n_field_cols << "," << p.n_total_cols << "," << p.s_row << ","
            << p.c_page << "," << (p.m_fixed / (1024 * 1024)) << ","
            << (p.mem_limit / (1024 * 1024)) << "," << p.m_data_budget << ","
            << p.batch_size << "," << rows << "," << peak_actual << ","
            << m_data_formula << "," << (within ? "yes" : "no") << "\n";

        std::cout << " rows=" << rows << " peak="
                  << (peak_actual >= 0
                          ? std::to_string(peak_actual / (1024 * 1024)) + " MB"
                          : "N/A")
                  << (within ? " [OK]" : " [OVER]") << "\n";
    }

    csv.close();
    std::cout << "\nCSV: " << csv_path << "\n";

    libtsfile_destroy();
    return 0;
}
