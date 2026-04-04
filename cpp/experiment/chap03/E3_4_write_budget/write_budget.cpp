/*
 * E3-4: Memory-budgeted write performance (PLAIN + UNCOMPRESSED).
 *
 * Wraps TsFileWriter with two-level memory control:
 *   Level 1 – data budget: flush when M_data exceeds data_budget
 *   Level 2 – meta budget: rotate file when M_meta exceeds meta_budget
 *
 * With PLAIN + UNCOMPRESSED, formula estimates match direct measurements
 * closely, validating the EOQ-based budget allocation strategy.
 *
 * Runs multiple memory budgets and outputs throughput / flush / rotate stats.
 *
 * Schema: 8-FIELD "mem_bench" table, PLAIN + UNCOMPRESSED.
 *   s_data = 56 bytes/row,  b = 928 bytes/device/flush
 *
 * Build:
 *   cmake -DBUILD_TEST=OFF ..
 *   cmake --build . --target write_budget
 *
 * Usage:
 *   ./write_budget [total_rows] [csv_path] [base_path]
 */

#include <fcntl.h>

#include <chrono>
#include <climits>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
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

static const char* kTable = "mem_bench";
static const int kNumDevices = 10;
static const int64_t kSData = 56;
static const int64_t kB = 928;

static std::string device_name(int i) { return "device_" + std::to_string(i); }

static std::shared_ptr<TableSchema> make_plain_schema() {
    return std::make_shared<TableSchema>(
        std::string(kTable), std::vector<ColumnSchema>{
                                 ColumnSchema("id1", STRING, UNCOMPRESSED,
                                              PLAIN, ColumnCategory::TAG),
                                 ColumnSchema("id2", STRING, UNCOMPRESSED,
                                              PLAIN, ColumnCategory::TAG),
                                 ColumnSchema("s1", INT32, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s2", INT32, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s3", INT64, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s4", INT64, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s5", FLOAT, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s6", FLOAT, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s7", DOUBLE, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                                 ColumnSchema("s8", DOUBLE, UNCOMPRESSED, PLAIN,
                                              ColumnCategory::FIELD),
                             });
}

static Tablet make_tablet(uint32_t n) {
    return Tablet(
        kTable, {"id1", "id2", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"},
        {STRING, STRING, INT32, INT32, INT64, INT64, FLOAT, FLOAT, DOUBLE,
         DOUBLE},
        {ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::FIELD,
         ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD,
         ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD,
         ColumnCategory::FIELD},
        std::max(n, 1u));
}

// ---------------------------------------------------------------------------
// MemConstrainedWriter (PLAIN + UNCOMPRESSED variant)
// ---------------------------------------------------------------------------
struct MemSnapshot {
    int64_t rows_written;
    std::string event;
    int file_idx;
    int flush_count;
    int64_t m_data_direct;
    int64_t m_meta_direct;
    int64_t m_data_formula;
    int64_t m_meta_formula;
    int64_t data_budget;
    int64_t meta_budget;
};

class MemConstrainedWriter {
   public:
    MemConstrainedWriter(const std::string& base_path,
                         std::shared_ptr<TableSchema> schema, int n_devices,
                         int64_t mem_limit_bytes,
                         int64_t m_init_bytes = 900LL * 1024,
                         double alpha = 0.9)
        : base_path_(base_path),
          schema_(std::move(schema)),
          n_devices_(n_devices),
          mem_limit_(mem_limit_bytes),
          m_init_(m_init_bytes),
          alpha_(alpha),
          rows_since_flush_(0),
          total_flush_count_(0),
          flush_count_cur_(0),
          file_count_(0),
          total_rows_(0),
          writer_(nullptr),
          write_file_(nullptr) {
        m_avail_ = mem_limit_ - m_init_;
        data_budget_ = m_avail_ / 2;
        meta_budget_ = m_avail_ / 2;

        g_config_value_.chunk_group_size_threshold_ = INT64_MAX / 2;
        open_new_file();
    }

    ~MemConstrainedWriter() { cleanup_writer(); }

    int write_tablet(Tablet& tablet, int64_t nrows) {
        int ret = writer_->write_table(tablet);
        if (ret != 0) return ret;

        rows_since_flush_ += nrows;
        total_rows_ += nrows;

        take_snapshot("batch");

        int64_t m_data = writer_->calculate_mem_size_for_all_group();
        if (m_data > data_budget_) {
            do_flush();
        }
        return 0;
    }

    int close() {
        if (writer_->calculate_mem_size_for_all_group() > 0) {
            take_snapshot("flush_before");
            writer_->flush();
            total_flush_count_++;
            flush_count_cur_++;
            rows_since_flush_ = 0;
            take_snapshot("flush_after");
        }
        writer_->close();
        take_snapshot("final_close");
        cleanup_writer();
        return 0;
    }

    const std::vector<MemSnapshot>& snapshots() const { return snapshots_; }
    int64_t data_budget() const { return data_budget_; }
    int64_t meta_budget() const { return meta_budget_; }
    int total_flush_count() const { return total_flush_count_; }
    int file_count() const { return file_count_ + 1; }

   private:
    void open_new_file() {
        std::string path =
            base_path_ + "_" + std::to_string(file_count_) + ".tsfile";
        write_file_ = new WriteFile();
        int ret = write_file_->create(path.c_str(),
                                      O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (ret != 0) {
            std::cerr << "Failed to create: " << path << " err=" << ret << "\n";
            return;
        }
        writer_ = new TsFileWriter();
        writer_->init(write_file_);
        writer_->register_table(schema_);
        rows_since_flush_ = 0;
        flush_count_cur_ = 0;
    }

    void cleanup_writer() {
        delete writer_;
        delete write_file_;
        writer_ = nullptr;
        write_file_ = nullptr;
    }

    void do_flush() {
        take_snapshot("flush_before");
        writer_->flush();
        total_flush_count_++;
        flush_count_cur_++;
        rows_since_flush_ = 0;
        take_snapshot("flush_after");

        int64_t m_meta = writer_->calculate_meta_mem_size();
        if (m_meta >= static_cast<int64_t>(meta_budget_ * alpha_)) {
            rotate_file();
        }
    }

    void rotate_file() {
        take_snapshot("rotate_before");
        writer_->close();
        cleanup_writer();
        file_count_++;
        flush_count_cur_ = 0;
        open_new_file();
        take_snapshot("rotate_after");
    }

    void take_snapshot(const std::string& event) {
        MemSnapshot s;
        s.rows_written = total_rows_;
        s.event = event;
        s.file_idx = file_count_;
        s.flush_count = flush_count_cur_;
        s.m_data_direct =
            writer_ ? writer_->calculate_mem_size_for_all_group() : 0;
        s.m_meta_direct = writer_ ? writer_->calculate_meta_mem_size() : 0;
        s.m_data_formula = kSData * rows_since_flush_;
        s.m_meta_formula = total_flush_count_ * kB;
        s.data_budget = data_budget_;
        s.meta_budget = meta_budget_;
        snapshots_.push_back(std::move(s));
    }

    std::string base_path_;
    std::shared_ptr<TableSchema> schema_;
    [[maybe_unused]] int n_devices_;
    int64_t mem_limit_;
    int64_t m_init_;
    double alpha_;
    int64_t m_avail_;
    int64_t data_budget_;
    int64_t meta_budget_;
    int64_t rows_since_flush_;
    int total_flush_count_;
    int flush_count_cur_;
    int file_count_;
    int64_t total_rows_;
    TsFileWriter* writer_;
    WriteFile* write_file_;
    std::vector<MemSnapshot> snapshots_;
};

// ---------------------------------------------------------------------------
// Run one budget configuration, return summary
// ---------------------------------------------------------------------------
struct BudgetResult {
    int64_t mem_limit_mb;
    double throughput_mrows_s;
    int flush_count;
    int file_count;
    int64_t total_rows;
};

static BudgetResult run_budget(int64_t mem_limit_mb, int64_t total_rows,
                               uint32_t batch_cap, const std::string& base_path,
                               const std::string& detail_csv_path) {
    int64_t mem_limit = mem_limit_mb * 1024LL * 1024;
    auto schema = make_plain_schema();

    MemConstrainedWriter writer(base_path, schema, kNumDevices, mem_limit);

    std::cout << "  mem_limit=" << mem_limit_mb << " MB"
              << "  data_budget=" << writer.data_budget() / (1024 * 1024)
              << " MB"
              << "  meta_budget=" << writer.meta_budget() / (1024 * 1024)
              << " MB" << std::flush;

    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int32_t> ni(-100, 100);
    std::uniform_real_distribution<float> nf(-5.0f, 5.0f);
    std::uniform_real_distribution<double> nd(-0.5, 0.5);

    int64_t rows_per_dev = total_rows / kNumDevices;

    using clock = std::chrono::high_resolution_clock;
    auto t_start = clock::now();

    for (int dev = 0; dev < kNumDevices; dev++) {
        std::string dev_id = device_name(dev);
        int64_t dev_base = static_cast<int64_t>(dev) * rows_per_dev;

        for (int64_t off = 0; off < rows_per_dev;) {
            uint32_t n = static_cast<uint32_t>(
                std::min<int64_t>(batch_cap, rows_per_dev - off));
            auto tablet = make_tablet(n);

            for (uint32_t i = 0; i < n; i++) {
                int64_t ts = dev_base + off + i;
                tablet.add_timestamp(i, ts);
                tablet.add_value(i, "id1", dev_id.c_str());
                tablet.add_value(i, "id2", "tag_b");
                tablet.add_value(i, "s1",
                                 static_cast<int32_t>(ts % 100000) + ni(rng));
                tablet.add_value(i, "s2",
                                 static_cast<int32_t>(ts % 50000) + ni(rng));
                tablet.add_value(i, "s3", ts + static_cast<int64_t>(ni(rng)));
                tablet.add_value(i, "s4",
                                 ts * 2 + static_cast<int64_t>(ni(rng)));
                tablet.add_value(i, "s5",
                                 static_cast<float>(ts % 10000) + nf(rng));
                tablet.add_value(i, "s6",
                                 static_cast<float>(ts % 5000) + nf(rng));
                tablet.add_value(i, "s7", ts * 1.1 + nd(rng));
                tablet.add_value(i, "s8", ts * 0.5 + nd(rng));
            }

            int ret = writer.write_tablet(tablet, static_cast<int64_t>(n));
            if (ret != 0) {
                std::cerr << "write_tablet failed: " << ret << "\n";
                exit(1);
            }
            off += n;
        }
    }

    writer.close();

    double sec = std::chrono::duration<double>(clock::now() - t_start).count();
    double throughput = total_rows / sec / 1e6;

    // Write detail CSV for this budget
    std::ofstream detail(detail_csv_path);
    if (detail.is_open()) {
        detail << "rows_written,event,file_idx,flush_count,"
                  "m_data_direct,m_meta_direct,m_total_direct,"
                  "m_data_formula,m_meta_formula,m_total_formula,"
                  "data_budget,meta_budget\n";
        for (const auto& s : writer.snapshots()) {
            detail << s.rows_written << "," << s.event << "," << s.file_idx
                   << "," << s.flush_count << "," << s.m_data_direct << ","
                   << s.m_meta_direct << ","
                   << (s.m_data_direct + s.m_meta_direct) << ","
                   << s.m_data_formula << "," << s.m_meta_formula << ","
                   << (s.m_data_formula + s.m_meta_formula) << ","
                   << s.data_budget << "," << s.meta_budget << "\n";
        }
        detail.close();
    }

    std::cout << "  -> " << std::fixed << std::setprecision(2) << throughput
              << " Mrows/s, " << writer.total_flush_count() << " flushes, "
              << writer.file_count() << " files\n";

    BudgetResult r;
    r.mem_limit_mb = mem_limit_mb;
    r.throughput_mrows_s = throughput;
    r.flush_count = writer.total_flush_count();
    r.file_count = writer.file_count();
    r.total_rows = total_rows;
    return r;
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    int64_t total_rows =
        50000000LL;  // 10 dev × 5M rows (enough to trigger rotation)
    std::string csv_path = "write_budget.csv";
    std::string base_path = "/tmp/wb_plain";
    uint32_t batch_cap = 4096;  // small batch to avoid overshooting data_budget

    if (argc > 1) total_rows = std::atoll(argv[1]);
    if (argc > 2) csv_path = argv[2];
    if (argc > 3) base_path = argv[3];
    if (argc > 4) batch_cap = static_cast<uint32_t>(std::atoi(argv[4]));

    libtsfile_init();
    // Force PLAIN + UNCOMPRESSED for timestamp (override global TS_2DIFF
    // default)
    g_config_value_.time_encoding_type_ = PLAIN;
    g_config_value_.time_compress_type_ = UNCOMPRESSED;

    std::cout << "=== E3-4: Write Budget (PLAIN + UNCOMPRESSED) ===\n"
              << "  total_rows:  " << total_rows << "\n"
              << "  batch_cap:   " << batch_cap << "\n"
              << "  s_data:      " << kSData << " bytes/row\n"
              << "  b:           " << kB << " bytes/device/flush\n"
              << "  csv:         " << csv_path << "\n"
              << "  base_path:   " << base_path << "\n\n";

    int64_t budgets_mb[] = {2, 4, 8, 16, 32};
    int n_budgets = sizeof(budgets_mb) / sizeof(budgets_mb[0]);

    std::vector<BudgetResult> results;

    for (int i = 0; i < n_budgets; i++) {
        char detail_name[256];
        snprintf(detail_name, sizeof(detail_name),
                 "write_budget_detail_%lldMB.csv", (long long)budgets_mb[i]);
        auto r = run_budget(budgets_mb[i], total_rows, batch_cap, base_path,
                            std::string(detail_name));
        results.push_back(r);
    }

    // Write summary CSV
    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    csv << "mem_limit_mb,throughput_mrows_s,flush_count,file_count,"
           "total_rows\n";
    for (const auto& r : results) {
        csv << r.mem_limit_mb << "," << std::fixed << std::setprecision(3)
            << r.throughput_mrows_s << "," << r.flush_count << ","
            << r.file_count << "," << r.total_rows << "\n";
    }
    csv.close();

    std::cout << "\n=== Summary ===\n"
              << std::setw(12) << "budget_MB" << std::setw(14) << "Mrows/s"
              << std::setw(10) << "flushes" << std::setw(8) << "files"
              << "\n"
              << std::string(44, '-') << "\n";
    for (const auto& r : results) {
        std::cout << std::setw(12) << r.mem_limit_mb << std::setw(14)
                  << std::fixed << std::setprecision(2) << r.throughput_mrows_s
                  << std::setw(10) << r.flush_count << std::setw(8)
                  << r.file_count << "\n";
    }

    std::cout << "\nCSV: " << csv_path << "\n";

    libtsfile_destroy();
    return 0;
}
