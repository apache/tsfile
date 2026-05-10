#include <chrono>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

#include "reader/tsfile_reader.h"
#include "common/tsfile_common.h"
#include "reader/filter/tag_filter.h"

using Clock = std::chrono::high_resolution_clock;
using Ms = std::chrono::duration<double, std::milli>;

static double to_ms(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration_cast<Ms>(b - a).count();
}

int main(int argc, char* argv[]) {
    const char* file_path =
        "/Volumes/timecho-yuan/data/timeBench/TimeBench_TsFile/"
        "ecg_dataset/part_0.tsfile";
    if (argc > 1) file_path = argv[1];

    const int WARMUP = 20;
    const int BENCH  = 100;
    const int OFFSET = 1000;
    const int LIMIT  = 3584;

    storage::libtsfile_init();
    std::cout << "=== C++ TsFile Read Benchmark ===" << std::endl;
    std::cout << "File: " << file_path << std::endl;

    // ---- Phase 1: Open reader ----
    auto t0 = Clock::now();
    storage::TsFileReader reader;
    int ret = reader.open(file_path);
    auto t1 = Clock::now();
    if (ret != 0) {
        std::cerr << "Failed to open file, ret=" << ret << std::endl;
        return 1;
    }
    std::cout << "\n[1] reader.open(): " << to_ms(t0, t1) << " ms" << std::endl;

    // ---- Phase 2: get_timeseries_metadata ----
    auto t2 = Clock::now();
    auto metadata_map = reader.get_timeseries_metadata();
    auto t3 = Clock::now();
    std::cout << "[2] get_timeseries_metadata(): " << to_ms(t2, t3)
              << " ms  (" << metadata_map.size() << " devices)" << std::endl;

    // ---- Phase 3: get_all_table_schemas ----
    auto t4 = Clock::now();
    auto schemas = reader.get_all_table_schemas();
    auto t5 = Clock::now();
    std::cout << "[3] get_all_table_schemas(): " << to_ms(t4, t5)
              << " ms  (" << schemas.size() << " tables)" << std::endl;

    // Pick first table and first device for queryByRow
    if (schemas.empty()) {
        std::cerr << "No tables found" << std::endl;
        return 1;
    }
    auto table_schema = schemas[0];
    std::string table_name = table_schema->get_table_name();
    std::cout << "\nUsing table: " << table_name << std::endl;

    // Get field columns (non-tag, non-time)
    std::vector<std::string> field_columns;
    auto measurement_names = table_schema->get_measurement_names();
    auto categories = table_schema->get_column_categories();
    std::vector<std::string> tag_columns;
    for (size_t i = 0; i < measurement_names.size(); i++) {
        if (categories[i] == common::ColumnCategory::FIELD) {
            field_columns.push_back(measurement_names[i]);
        } else if (categories[i] == common::ColumnCategory::TAG) {
            tag_columns.push_back(measurement_names[i]);
        }
    }
    std::cout << "Field columns: " << field_columns.size()
              << ", Tag columns: " << tag_columns.size() << std::endl;

    // Pick first device to build tag filter
    auto device_ids = reader.get_all_devices(table_name);
    if (device_ids.empty()) {
        std::cerr << "No devices found" << std::endl;
        return 1;
    }
    std::cout << "Total devices: " << device_ids.size() << std::endl;

    // Debug: print first device's segments
    {
        auto& d = device_ids[0];
        auto& segs = d->get_segments();
        std::cout << "First device segments (" << segs.size() << "): ";
        for (size_t i = 0; i < segs.size(); i++) {
            std::cout << "[" << i << "]=\"" << (segs[i] ? *segs[i] : "null") << "\" ";
        }
        std::cout << std::endl;
        std::cout << "First device name: " << d->get_device_name() << std::endl;
        std::cout << "First device table: " << d->get_table_name() << std::endl;
    }

    // Use only first field column for benchmark (like Python does)
    std::vector<std::string> query_columns;
    if (!field_columns.empty()) {
        query_columns.push_back(field_columns[0]);
    }
    std::cout << "Query column: " << query_columns[0] << std::endl;
    std::cout << "Offset: " << OFFSET << ", Limit: " << LIMIT << std::endl;

    // ---- Phase 4: Benchmark queryByRow with detail timing ----
    // Build tag filter for first device
    auto& first_device = device_ids[0];
    first_device->split_table_name();

    std::cout << "\n=== queryByRow Benchmark ===" << std::endl;

    // Stats accumulators
    double total_build_filter = 0, total_query_create = 0;
    double total_first_next = 0, total_remaining_next = 0;
    double total_close = 0;
    int total_rows = 0;

    for (int iter = 0; iter < WARMUP + BENCH; iter++) {
        // Pick a device (round-robin for variety)
        auto& device = device_ids[iter % device_ids.size()];
        device->split_table_name();

        // 4a: Build tag filter
        auto tf0 = Clock::now();
        storage::Filter* tag_filter = nullptr;
        {
            auto ts = reader.get_table_schema(table_name);
            storage::TagFilterBuilder builder(ts.get());
            storage::Filter* combined = nullptr;
            for (size_t i = 0; i < tag_columns.size(); i++) {
                int seg_idx = i + 1;
                std::string* seg = device->get_split_segname_at(seg_idx);
                if (seg == nullptr) continue;
                auto* eq = builder.eq(tag_columns[i], *seg);
                if (combined == nullptr) {
                    combined = eq;
                } else {
                    combined = builder.and_filter(combined, eq);
                }
            }
            tag_filter = combined;
        }
        auto tf1 = Clock::now();

        // 4b: Create query (ResultSet)
        auto tq0 = Clock::now();
        storage::ResultSet* result_set = nullptr;
        ret = reader.queryByRow(table_name, query_columns, OFFSET, LIMIT,
                                result_set, tag_filter);
        auto tq1 = Clock::now();
        if (ret != 0 || result_set == nullptr) {
            if (tag_filter) delete tag_filter;
            continue;
        }

        // 4c: First next() call (triggers lazy init)
        auto tn0 = Clock::now();
        bool has_next = false;
        ret = result_set->next(has_next);
        auto tn1 = Clock::now();
        int row_count = has_next ? 1 : 0;

        // 4d: Remaining next() calls
        auto tr0 = Clock::now();
        while (ret == 0 && has_next) {
            ret = result_set->next(has_next);
            if (has_next) row_count++;
        }
        auto tr1 = Clock::now();

        // 4e: Close
        auto tc0 = Clock::now();
        result_set->close();
        reader.destroy_query_data_set(result_set);
        auto tc1 = Clock::now();

        if (iter >= WARMUP) {
            total_build_filter   += to_ms(tf0, tf1);
            total_query_create   += to_ms(tq0, tq1);
            total_first_next     += to_ms(tn0, tn1);
            total_remaining_next += to_ms(tr0, tr1);
            total_close          += to_ms(tc0, tc1);
            total_rows           += row_count;
        }

        if (iter == WARMUP) {
            std::cout << "Warmup done (" << WARMUP << " iters). "
                      << "First bench iter: " << row_count << " rows" << std::endl;
        }
    }

    int N = BENCH;
    std::cout << "\n=== Results (avg over " << N << " iterations) ===" << std::endl;
    std::cout << "  build_tag_filter:  " << (total_build_filter / N) << " ms" << std::endl;
    std::cout << "  queryByRow create: " << (total_query_create / N) << " ms" << std::endl;
    std::cout << "  first next():      " << (total_first_next / N) << " ms" << std::endl;
    std::cout << "  remaining next():  " << (total_remaining_next / N) << " ms"
              << "  (avg " << (total_rows / N) << " rows)" << std::endl;
    std::cout << "  close+destroy:     " << (total_close / N) << " ms" << std::endl;
    std::cout << "  ----- total:       "
              << ((total_build_filter + total_query_create + total_first_next +
                   total_remaining_next + total_close) / N)
              << " ms" << std::endl;

    reader.close();
    std::cout << "\nDone." << std::endl;
    return 0;
}
