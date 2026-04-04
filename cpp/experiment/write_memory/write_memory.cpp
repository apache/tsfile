/*
 * Write-path memory profiling experiment.
 *
 * Writes a configurable number of rows via TsFileTableWriter, printing
 * per-module memory statistics (ModStat) at regular intervals and
 * writing a CSV file for plotting.
 *
 * Build:
 *   cmake -DENABLE_MEM_STAT=ON -DBUILD_TEST=OFF ..
 *   cmake --build . --target write_memory
 *
 * Run:
 *   ./write_memory [total_rows] [batch_size] [print_interval] [csv_path]
 */

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/time.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>

#include "common/allocator/alloc_base.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "writer/tsfile_table_writer.h"

static const char* kTable = "mem_bench";
static const int kNumDevices = 10;

static std::string device_name(int i) { return "device_" + std::to_string(i); }

static const int kModCount = common::__LAST_MOD_ID;

// Returns timeval as microseconds.
static int64_t tv_to_us(const struct timeval& tv) {
    return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

using steady_clock = std::chrono::steady_clock;
static steady_clock::time_point g_t0;

static void write_csv_header(std::ofstream& csv) {
    csv << "rows_written,phase,wall_us,user_cpu_us,sys_cpu_us";
    for (int i = 0; i < kModCount; i++) csv << "," << common::g_mod_names[i];
    csv << ",TOTAL\n";
}

static void write_csv_row(std::ofstream& csv, int64_t rows, const char* phase) {
    // Wall clock
    int64_t wall_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          steady_clock::now() - g_t0)
                          .count();

    // CPU time via getrusage
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    int64_t user_us = tv_to_us(ru.ru_utime);
    int64_t sys_us = tv_to_us(ru.ru_stime);

    auto& ms = common::ModStat::get_instance();
    csv << rows << "," << phase << "," << wall_us << "," << user_us << ","
        << sys_us;
    int64_t total = 0;
    for (int i = 0; i < kModCount; i++) {
        int32_t val = ms.get_stat(i);
        csv << "," << val;
        total += val;
    }
    csv << "," << total << "\n";
}

static void print_separator(const char* label) {
    std::cout << "\n──── " << label << " ";
    for (int i = 0; i < 50 - (int)std::string(label).size(); i++)
        std::cout << "─";
    std::cout << "\n";
}

static void print_mem(const char* label) {
    print_separator(label);
    common::ModStat::get_instance().print_stat();
}

int main(int argc, char* argv[]) {
    int64_t total_rows = 200000000;
    uint32_t batch_cap = 65536;
    int64_t print_interval_rows = 500000;
    std::string csv_path = "write_memory_stats.csv";
    uint64_t mem_threshold_mb = 128;
    int write_mode = 0;  // 0=sequential (per-device), 1=interleaved (mixed
                         // devices per tablet)

    if (argc > 1) total_rows = std::atoll(argv[1]);
    if (argc > 2) batch_cap = std::atoi(argv[2]);
    if (argc > 3) print_interval_rows = std::atoll(argv[3]);
    if (argc > 4) csv_path = argv[4];
    if (argc > 5) mem_threshold_mb = std::atoll(argv[5]);
    if (argc > 6) write_mode = std::atoi(argv[6]);

    const char* mode_str = write_mode == 0 ? "sequential" : "interleaved";
    std::cout << "=== Write Memory Experiment ===\n"
              << "  total_rows:     " << total_rows << "\n"
              << "  batch_size:     " << batch_cap << "\n"
              << "  print_interval: " << print_interval_rows << "\n"
              << "  devices:        " << kNumDevices << "\n"
              << "  mem_threshold:  " << mem_threshold_mb << " MB\n"
              << "  write_mode:     " << mode_str << "\n"
              << "  csv_output:     " << csv_path << "\n\n";

    std::ofstream csv(csv_path);
    if (!csv.is_open()) {
        std::cerr << "cannot open csv: " << csv_path << "\n";
        return 1;
    }
    write_csv_header(csv);

    storage::libtsfile_init();
    g_t0 = steady_clock::now();
    write_csv_row(csv, 0, "init");

    // --- Create writer ---
    const std::string path =
        "/Users/colin/dev/tsfile_b1/cpp/experiment/experiment.tsfile";
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    int ret = file.create(path.c_str(), flags, 0666);
    if (ret != 0) {
        std::cerr << "create file failed: " << ret << "\n";
        return 1;
    }

    auto* schema = new storage::TableSchema(
        std::string(kTable),
        {
            common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("id2", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("s1", common::INT32, common::SNAPPY,
                                 common::TS_2DIFF,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s2", common::INT32, common::SNAPPY,
                                 common::TS_2DIFF,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s3", common::INT64, common::SNAPPY,
                                 common::TS_2DIFF,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s4", common::INT64, common::SNAPPY,
                                 common::TS_2DIFF,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s5", common::FLOAT, common::SNAPPY,
                                 common::GORILLA,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s6", common::FLOAT, common::SNAPPY,
                                 common::GORILLA,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s7", common::DOUBLE, common::SNAPPY,
                                 common::GORILLA,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s8", common::DOUBLE, common::SNAPPY,
                                 common::GORILLA,
                                 common::ColumnCategory::FIELD),
        });

    uint64_t mem_threshold = mem_threshold_mb * 1024 * 1024;
    auto* writer = new storage::TsFileTableWriter(&file, schema, mem_threshold);
    write_csv_row(csv, 0, "writer_created");

    // --- Write data ---
    int64_t rows_per_dev = total_rows / kNumDevices;
    int64_t rows_written = 0;
    int64_t next_print_at = print_interval_rows;

    // Console: print every 10x of csv interval to avoid flooding
    int64_t console_interval = print_interval_rows * 10;
    int64_t next_console_at = console_interval;

    // Noise source: fixed seed for reproducibility.
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<int> ni(-100, 100);
    std::uniform_real_distribution<double> nd(-0.5, 0.5);
    std::uniform_real_distribution<float> nf(-5.0f, 5.0f);

    // Helper: fill a tablet row
    auto fill_row = [&](storage::Tablet& tablet, uint32_t row, int64_t ts,
                        const std::string& dev_id) {
        tablet.add_timestamp(row, ts);
        tablet.add_value(row, "id1", dev_id.c_str());
        tablet.add_value(row, "id2", "tag_b");
        tablet.add_value(row, "s1",
                         static_cast<int32_t>(ts % 100000) + ni(rng));
        tablet.add_value(row, "s2", static_cast<int32_t>(ts % 50000) + ni(rng));
        tablet.add_value(row, "s3", ts + ni(rng));
        tablet.add_value(row, "s4", ts * 2 + ni(rng));
        tablet.add_value(row, "s5", static_cast<float>(ts % 10000) + nf(rng));
        tablet.add_value(row, "s6", static_cast<float>(ts % 5000) + nf(rng));
        tablet.add_value(row, "s7", ts * 1.1 + nd(rng));
        tablet.add_value(row, "s8", ts * 0.5 + nd(rng));
    };

    // Helper: create a tablet with capacity n
    auto make_tablet = [](uint32_t n) {
        return storage::Tablet(
            kTable,
            {"id1", "id2", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"},
            {common::STRING, common::STRING, common::INT32, common::INT32,
             common::INT64, common::INT64, common::FLOAT, common::FLOAT,
             common::DOUBLE, common::DOUBLE},
            {common::ColumnCategory::TAG, common::ColumnCategory::TAG,
             common::ColumnCategory::FIELD, common::ColumnCategory::FIELD,
             common::ColumnCategory::FIELD, common::ColumnCategory::FIELD,
             common::ColumnCategory::FIELD, common::ColumnCategory::FIELD,
             common::ColumnCategory::FIELD, common::ColumnCategory::FIELD},
            std::max(n, 1u));
    };

    // Helper: check print / console progress
    auto check_progress =
        [&](std::chrono::high_resolution_clock::time_point t_start) {
            while (rows_written >= next_print_at) {
                write_csv_row(csv, rows_written, "write");
                next_print_at += print_interval_rows;
            }
            if (rows_written >= next_console_at) {
                double elapsed =
                    std::chrono::duration<double>(
                        std::chrono::high_resolution_clock::now() - t_start)
                        .count();
                std::cout << "\r  " << rows_written / 1000000 << "M / "
                          << total_rows / 1000000 << "M rows  (" << std::fixed
                          << std::setprecision(1) << elapsed << "s)"
                          << std::flush;
                next_console_at += console_interval;
            }
        };

    using clock = std::chrono::high_resolution_clock;
    auto t_start = clock::now();

    if (write_mode == 0) {
        // ===== Mode 0: Sequential — write all rows for each device in turn
        // =====
        for (int dev = 0; dev < kNumDevices; dev++) {
            std::string dev_id = device_name(dev);
            int64_t dev_base = dev * rows_per_dev;

            for (int64_t off = 0; off < rows_per_dev;) {
                uint32_t n = static_cast<uint32_t>(
                    std::min<int64_t>(batch_cap, rows_per_dev - off));

                auto tablet = make_tablet(n);
                for (uint32_t i = 0; i < n; i++) {
                    fill_row(tablet, i, dev_base + off + i, dev_id);
                }

                ret = writer->write_table(tablet);
                if (ret != 0) {
                    std::cerr << "write_table failed: " << ret << "\n";
                    return 1;
                }

                off += n;
                rows_written += n;
                check_progress(t_start);
            }
        }
    } else {
        // ===== Mode 1: Interleaved — each tablet contains all devices =====
        // Each tablet: kNumDevices blocks, each block has rows_per_block rows
        // for the same device, arranged contiguously.
        uint32_t rows_per_block = batch_cap / kNumDevices;
        if (rows_per_block == 0) rows_per_block = 1;
        uint32_t tablet_rows = rows_per_block * kNumDevices;

        // Per-device timestamp counters
        std::vector<int64_t> dev_ts(kNumDevices, 0);
        // Precompute device names
        std::vector<std::string> dev_ids(kNumDevices);
        for (int d = 0; d < kNumDevices; d++) dev_ids[d] = device_name(d);

        int64_t remaining = total_rows;
        while (remaining > 0) {
            // Adjust last batch
            uint32_t actual_per_block = rows_per_block;
            if (static_cast<int64_t>(tablet_rows) > remaining) {
                actual_per_block =
                    static_cast<uint32_t>(remaining / kNumDevices);
                if (actual_per_block == 0) actual_per_block = 1;
            }
            uint32_t actual_total = actual_per_block * kNumDevices;

            auto tablet = make_tablet(actual_total);

            uint32_t row = 0;
            for (int d = 0; d < kNumDevices; d++) {
                for (uint32_t i = 0; i < actual_per_block; i++) {
                    fill_row(tablet, row, dev_ts[d], dev_ids[d]);
                    dev_ts[d]++;
                    row++;
                }
            }

            ret = writer->write_table(tablet);
            if (ret != 0) {
                std::cerr << "write_table failed: " << ret << "\n";
                return 1;
            }

            rows_written += actual_total;
            remaining -= actual_total;
            check_progress(t_start);
        }
    }

    double write_sec =
        std::chrono::duration<double>(clock::now() - t_start).count();
    std::cout << "\n";

    write_csv_row(csv, rows_written, "before_flush");
    print_mem("BEFORE FLUSH");

    // --- Flush ---
    auto t_flush = clock::now();
    ret = writer->flush();
    if (ret != 0) {
        std::cerr << "flush failed: " << ret << "\n";
        return 1;
    }
    double flush_sec =
        std::chrono::duration<double>(clock::now() - t_flush).count();

    write_csv_row(csv, rows_written, "after_flush");
    print_mem("AFTER FLUSH");

    // --- Close ---
    ret = writer->close();
    if (ret != 0) {
        std::cerr << "close failed: " << ret << "\n";
        return 1;
    }
    delete writer;
    delete schema;

    write_csv_row(csv, rows_written, "after_close");
    print_mem("AFTER CLOSE + DELETE");

    csv.close();

    // --- Summary ---
    print_separator("SUMMARY");
    double total_sec =
        std::chrono::duration<double>(clock::now() - t_start).count();
    std::cout << std::fixed << std::setprecision(3)
              << "  rows written: " << rows_written << "\n"
              << "  write time:   " << write_sec << " s ("
              << static_cast<int64_t>(rows_written / write_sec) << " rows/s)\n"
              << "  flush time:   " << flush_sec << " s\n"
              << "  total time:   " << total_sec << " s\n"
              << "  csv output:   " << csv_path << "\n"
              << "  tsfile:       " << path << "\n";

    storage::libtsfile_destroy();
    return 0;
}
