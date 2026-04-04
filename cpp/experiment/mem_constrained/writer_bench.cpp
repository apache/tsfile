/*
 * MemConstrainedWriter experiment.
 *
 * Wraps TsFileWriter with explicit two-level memory-budget control:
 *   Level 1 – data budget: flush when M_data exceeds data_budget.
 *   Level 2 – meta budget: rotate file when M_meta exceeds meta_budget.
 *
 * Two measurement methods are computed simultaneously and logged to CSV:
 *   Direct   – calculate_mem_size_for_all_group() / calculate_meta_mem_size()
 *   Formula  – s_data * rows_since_flush  /  K_flush * b
 *
 * Control uses the DIRECT measurements.  Formula estimates are logged purely
 * for comparison (they overestimate M_data because they ignore compression).
 *
 * Schema: same 8-FIELD aligned table as write_memory experiment.
 *
 * Build (ENABLE_MEM_STAT not required):
 *   cmake -DBUILD_TEST=OFF ..
 *   cmake --build . --target writer_bench
 *
 * Usage:
 *   ./writer_bench [total_rows] [mem_limit_mb] [batch_cap] [csv] [base_path]
 *
 * Default:
 *   total_rows  = 20 000 000   (10 devices x 2 M rows)
 *   mem_limit   = 30 MB
 *   batch_cap   = 65 536 rows
 *   csv         = writer_bench.csv
 *   base_path   = /tmp/mc_writer  (files: base_path_0.tsfile, _1.tsfile, ...)
 */

#include <fcntl.h>

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

// ---------------------------------------------------------------------------
// Schema constants (aligned table model)
//
//   s_data = 8 (shared timestamp) + sum_field sizeof(field_type)
//          = 8 + (4+4+8+8+4+4+8+8) = 56 bytes / row
//
//   b      = n_field * 104 + 96
//          = 8 * 104 + 96 = 928 bytes / device / flush
// ---------------------------------------------------------------------------
static const char* kTable = "mem_bench";
static const int kNumDevices = 10;
static const int64_t kSData = 56;  // formula: bytes per row (aligned)
static const int64_t kB = 928;     // formula: meta bytes per device per flush

static std::string device_name(int i) { return "device_" + std::to_string(i); }

// ---------------------------------------------------------------------------
// MemSnapshot
// ---------------------------------------------------------------------------
struct MemSnapshot {
    int64_t rows_written;
    std::string event;  // "batch" | "flush_before" | "flush_after" |
                        // "rotate_before" | "rotate_after" | "final_close"
    int file_idx;
    int flush_count;  // K_cur: flushes in the current file

    // Direct measurements
    int64_t m_data_direct;
    int64_t m_meta_direct;

    // Formula estimates
    int64_t m_data_formula;
    int64_t
        m_meta_formula;  // total_flush_count * b   (sequential: 1 dev/flush)

    // Budget reference lines
    int64_t data_budget;
    int64_t meta_budget;
};

// ---------------------------------------------------------------------------
// MemConstrainedWriter
// ---------------------------------------------------------------------------
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

        // Disable TsFileWriter's built-in auto-flush so we drive it ourselves.
        g_config_value_.chunk_group_size_threshold_ = INT64_MAX / 2;

        open_new_file();
    }

    ~MemConstrainedWriter() { cleanup_writer(); }

    // nrows must match the number of rows written to the tablet.
    int write_tablet(Tablet& tablet, int64_t nrows) {
        int ret = writer_->write_table(tablet);
        if (ret != 0) return ret;

        rows_since_flush_ += nrows;
        total_rows_ += nrows;

        take_snapshot("batch");

        // Level 1: flush when direct M_data exceeds data budget
        int64_t m_data = writer_->calculate_mem_size_for_all_group();
        if (m_data > data_budget_) {
            do_flush();
        }
        return 0;
    }

    int close() {
        take_snapshot("before_close");
        // Final flush (if any data remains)
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

    // Derived parameters (for printing)
    int64_t data_budget() const { return data_budget_; }
    int64_t meta_budget() const { return meta_budget_; }
    int64_t rows_per_flush_formula() const { return data_budget_ / kSData; }
    int64_t flushes_per_file_formula() const {
        // sequential: 1 device per flush, so meta grows by kB per flush
        int64_t k = meta_budget_ / kB;
        return k > 0 ? k : 1;
    }

   private:
    // Open a new tsfile and register the table schema
    void open_new_file() {
        std::string path =
            base_path_ + "_" + std::to_string(file_count_) + ".tsfile";

        write_file_ = new WriteFile();
        int ret = write_file_->create(path.c_str(),
                                      O_WRONLY | O_CREAT | O_TRUNC, 0666);
        if (ret != 0) {
            std::cerr << "Failed to create file: " << path << " err=" << ret
                      << "\n";
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

        // Level 2: rotate file when direct M_meta exceeds meta budget * alpha
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

        // Direct
        s.m_data_direct =
            writer_ ? writer_->calculate_mem_size_for_all_group() : 0;
        s.m_meta_direct = writer_ ? writer_->calculate_meta_mem_size() : 0;

        // Formula (sequential mode: each flush = 1 device)
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
    int64_t total_flush_count_;  // all flushes across all files
    int flush_count_cur_;        // flushes in the current file
    int file_count_;
    int64_t total_rows_;

    TsFileWriter* writer_;
    WriteFile* write_file_;

    std::vector<MemSnapshot> snapshots_;
};

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    int64_t total_rows = 20000000LL;
    int64_t mem_limit_mb = 30;
    uint32_t batch_cap = 65536;
    std::string csv_path = "writer_bench.csv";
    std::string base_path = "/tmp/mc_writer";

    if (argc > 1) total_rows = std::atoll(argv[1]);
    if (argc > 2) mem_limit_mb = std::atoll(argv[2]);
    if (argc > 3) batch_cap = static_cast<uint32_t>(std::atoi(argv[3]));
    if (argc > 4) csv_path = argv[4];
    if (argc > 5) base_path = argv[5];

    int64_t mem_limit = mem_limit_mb * 1024LL * 1024;

    libtsfile_init();

    // ------------------------------------------------------------------
    // Build table schema (same as write_memory experiment)
    // ------------------------------------------------------------------
    auto schema = std::make_shared<TableSchema>(
        std::string(kTable),
        std::vector<ColumnSchema>{
            ColumnSchema("id1", STRING, UNCOMPRESSED, PLAIN,
                         ColumnCategory::TAG),
            ColumnSchema("id2", STRING, UNCOMPRESSED, PLAIN,
                         ColumnCategory::TAG),
            ColumnSchema("s1", INT32, SNAPPY, TS_2DIFF, ColumnCategory::FIELD),
            ColumnSchema("s2", INT32, SNAPPY, TS_2DIFF, ColumnCategory::FIELD),
            ColumnSchema("s3", INT64, SNAPPY, TS_2DIFF, ColumnCategory::FIELD),
            ColumnSchema("s4", INT64, SNAPPY, TS_2DIFF, ColumnCategory::FIELD),
            ColumnSchema("s5", FLOAT, SNAPPY, GORILLA, ColumnCategory::FIELD),
            ColumnSchema("s6", FLOAT, SNAPPY, GORILLA, ColumnCategory::FIELD),
            ColumnSchema("s7", DOUBLE, SNAPPY, GORILLA, ColumnCategory::FIELD),
            ColumnSchema("s8", DOUBLE, SNAPPY, GORILLA, ColumnCategory::FIELD),
        });

    MemConstrainedWriter writer(base_path, schema, kNumDevices, mem_limit);

    std::cout << "=== MemConstrainedWriter Experiment ===\n"
              << "  total_rows:         " << total_rows << "\n"
              << "  mem_limit:          " << mem_limit_mb << " MB\n"
              << "  batch_cap:          " << batch_cap << " rows\n"
              << "  devices:            " << kNumDevices << "\n"
              << "  s_data (formula):   " << kSData << " bytes/row\n"
              << "  b (formula):        " << kB << " bytes/device/flush\n"
              << "  data_budget:        "
              << writer.data_budget() / (1024 * 1024) << " MB\n"
              << "  meta_budget:        "
              << writer.meta_budget() / (1024 * 1024) << " MB\n"
              << "  F (formula):        " << writer.rows_per_flush_formula()
              << " rows/flush\n"
              << "  K (formula, seq):   " << writer.flushes_per_file_formula()
              << " flushes/file\n"
              << "  csv:                " << csv_path << "\n"
              << "  base_path:          " << base_path << "\n\n";

    // ------------------------------------------------------------------
    // Data generation helpers
    // ------------------------------------------------------------------
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int32_t> ni(-100, 100);
    std::uniform_real_distribution<float> nf(-5.0f, 5.0f);
    std::uniform_real_distribution<double> nd(-0.5, 0.5);

    auto make_tablet = [](uint32_t n) {
        return Tablet(
            kTable,
            {"id1", "id2", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"},
            {STRING, STRING, INT32, INT32, INT64, INT64, FLOAT, FLOAT, DOUBLE,
             DOUBLE},
            {ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::FIELD,
             ColumnCategory::FIELD, ColumnCategory::FIELD,
             ColumnCategory::FIELD, ColumnCategory::FIELD,
             ColumnCategory::FIELD, ColumnCategory::FIELD,
             ColumnCategory::FIELD},
            std::max(n, 1u));
    };

    // ------------------------------------------------------------------
    // Write: sequential per-device
    // ------------------------------------------------------------------
    int64_t rows_per_dev = total_rows / kNumDevices;

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
                return 1;
            }
            off += n;
        }

        std::cout << "  device " << std::setw(2) << dev << " done\n";
    }

    writer.close();

    // ------------------------------------------------------------------
    // Write CSV
    // ------------------------------------------------------------------
    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }

    csv << "rows_written,event,file_idx,flush_count,"
           "m_data_direct,m_meta_direct,m_total_direct,"
           "m_data_formula,m_meta_formula,m_total_formula,"
           "data_budget,meta_budget\n";

    for (const auto& s : writer.snapshots()) {
        int64_t m_total_direct = s.m_data_direct + s.m_meta_direct;
        int64_t m_total_formula = s.m_data_formula + s.m_meta_formula;
        csv << s.rows_written << "," << s.event << "," << s.file_idx << ","
            << s.flush_count << "," << s.m_data_direct << "," << s.m_meta_direct
            << "," << m_total_direct << "," << s.m_data_formula << ","
            << s.m_meta_formula << "," << m_total_formula << ","
            << s.data_budget << "," << s.meta_budget << "\n";
    }
    csv.close();

    std::cout << "\nSnapshots: " << writer.snapshots().size() << "\n"
              << "CSV: " << csv_path << "\n";

    libtsfile_destroy();
    return 0;
}
