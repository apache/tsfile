/*
 * Debug: print per-ChunkWriter estimate breakdown.
 * Writes a small number of rows and prints calculate_mem_size_for_all_group()
 * alongside manual per-column estimates.
 */
#include <fcntl.h>

#include <climits>
#include <cstdint>
#include <iostream>
#include <random>
#include <string>

#include "common/allocator/alloc_base.h"
#include "common/config/config.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "writer/tsfile_writer.h"

using namespace storage;
using namespace common;

static const char* kTable = "mem_bench";

int main() {
    libtsfile_init();
    g_config_value_.chunk_group_size_threshold_ = INT64_MAX / 2;
    // Force timestamp to PLAIN + UNCOMPRESSED (overrides global TS_2DIFF
    // default)
    g_config_value_.time_encoding_type_ = PLAIN;
    g_config_value_.time_compress_type_ = UNCOMPRESSED;

    const char* path = "/tmp/debug_estimate.tsfile";
    WriteFile wf;
    wf.create(path, O_WRONLY | O_CREAT | O_TRUNC, 0666);

    // Same 8-field schema as write_budget to measure matching M_init
    auto schema = std::make_shared<TableSchema>(
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

    TsFileWriter writer;
    writer.init(&wf);
    writer.register_table(schema);

    const int N = 100;  // minimal write, just to trigger ChunkWriter creation
    auto tablet = Tablet(
        kTable, {"id1", "id2", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"},
        {STRING, STRING, INT32, INT32, INT64, INT64, FLOAT, FLOAT, DOUBLE,
         DOUBLE},
        {ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::FIELD,
         ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD,
         ColumnCategory::FIELD, ColumnCategory::FIELD, ColumnCategory::FIELD,
         ColumnCategory::FIELD},
        N);

    for (int i = 0; i < N; i++) {
        tablet.add_timestamp(i, static_cast<int64_t>(i));
        tablet.add_value(i, "id1", "device_0");
        tablet.add_value(i, "id2", "tag_b");
        tablet.add_value(i, "s1", static_cast<int32_t>(i));
        tablet.add_value(i, "s2", static_cast<int32_t>(i));
        tablet.add_value(i, "s3", static_cast<int64_t>(i));
        tablet.add_value(i, "s4", static_cast<int64_t>(i));
        tablet.add_value(i, "s5", static_cast<float>(i));
        tablet.add_value(i, "s6", static_cast<float>(i));
        tablet.add_value(i, "s7", static_cast<double>(i));
        tablet.add_value(i, "s8", static_cast<double>(i));
    }

    // Measure M_init: ModStat total AFTER writer creation, BEFORE any writes
    auto& ms = ModStat::get_instance();
    const int kModCount = __LAST_MOD_ID;
    {
        int64_t m_init_total = 0;
        std::cout << "=== M_init (after writer creation, before writes) ===\n";
        for (int i = 0; i < kModCount; i++) {
            int64_t v = ms.get_stat(i);
            if (v != 0)
                std::cout << "  " << g_mod_names[i] << ": " << v << " ("
                          << v / 1024 << " KB)\n";
            m_init_total += v;
        }
        std::cout << "  M_init TOTAL: " << m_init_total << " ("
                  << m_init_total / 1024 << " KB)\n\n";
    }

    // Record ModStat before write
    std::vector<int64_t> before(kModCount);
    for (int i = 0; i < kModCount; i++) before[i] = ms.get_stat(i);

    int ret = writer.write_table(tablet);
    std::cout << "write_table ret=" << ret << "\n";

    int64_t total = writer.calculate_mem_size_for_all_group();
    int64_t meta = writer.calculate_meta_mem_size();

    // ModStat delta
    std::cout << "\n=== ModStat delta after write ===\n";
    int64_t modstat_total = 0;
    for (int i = 0; i < kModCount; i++) {
        int64_t delta = ms.get_stat(i) - before[i];
        if (delta != 0) {
            std::cout << "  " << g_mod_names[i] << ": " << delta << " ("
                      << delta / 1024 << " KB)\n";
            modstat_total += delta;
        }
    }
    std::cout << "  TOTAL ModStat delta: " << modstat_total << " ("
              << modstat_total / 1024 << " KB)\n";

    std::cout << "\n=== After writing " << N << " rows ===\n"
              << "calculate_mem_size_for_all_group() = " << total << " ("
              << total / 1024 << " KB)\n"
              << "calculate_meta_mem_size()          = " << meta << "\n"
              << "per row (estimate):                  " << (double)total / N
              << " bytes\n"
              << "per row (ModStat):                   "
              << (double)modstat_total / N << " bytes\n"
              << "expected: time(8) + v1(8) = 16 bytes/row\n"
              << "expected total:                      " << 16 * N << " bytes ("
              << 16 * N / 1024 << " KB)\n"
              << "ratio (estimate/expected):           "
              << (double)total / (16 * N) << "\n"
              << "ratio (ModStat/expected):            "
              << (double)modstat_total / (16 * N) << "\n";

    writer.flush();
    writer.close();
    libtsfile_destroy();
    return 0;
}
