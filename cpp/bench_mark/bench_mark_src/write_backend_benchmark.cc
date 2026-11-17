/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Compares the local-file and in-memory RandomAccessWriteFile backends on the
// same table-model write workload, and measures the cost of reaching the
// local backend through the abstract interface instead of the concrete
// WriteFile class.
//
// Build with -DBUILD_BENCHMARK=ON, run:
//   ./build/Release/write_backend_benchmark [--points=N] [--repeat=N]

#include <fcntl.h>
#include <stdint.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/global.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/memory_random_access_write_file.h"
#include "file/write_file.h"
#include "writer/tsfile_table_writer.h"

namespace {

const uint32_t kBatchedRows = 50000;
const size_t kDispatchChunkBytes = 64 * 1024;
const size_t kDispatchTotalBytes = static_cast<size_t>(1) << 28;
const uint32_t kFieldCount = 4;

struct RunResult {
    double seconds;
    int64_t bytes;
};

double now_seconds() {
    return std::chrono::duration<double>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// Deterministic pseudo-random values so the page encoders/compressors see
// realistic, poorly compressible data instead of arithmetic sequences.
uint64_t next_random(uint64_t& state) {
    state = state * 6364136223846793005ULL + 1442695040888963407ULL;
    return state;
}

storage::TableSchema make_table_schema() {
    std::vector<common::ColumnSchema> columns;
    columns.emplace_back("device", common::TSDataType::STRING,
                         common::ColumnCategory::TAG);
    columns.emplace_back("v0", common::TSDataType::INT64,
                         common::ColumnCategory::FIELD);
    columns.emplace_back("v1", common::TSDataType::INT64,
                         common::ColumnCategory::FIELD);
    columns.emplace_back("v2", common::TSDataType::DOUBLE,
                         common::ColumnCategory::FIELD);
    columns.emplace_back("v3", common::TSDataType::INT32,
                         common::ColumnCategory::FIELD);
    return storage::TableSchema("bench_table", columns);
}

std::vector<storage::Tablet> make_batches(int64_t points) {
    std::vector<std::string> names = {"device", "v0", "v1", "v2", "v3"};
    std::vector<common::TSDataType> types = {
        common::TSDataType::STRING, common::TSDataType::INT64,
        common::TSDataType::INT64, common::TSDataType::DOUBLE,
        common::TSDataType::INT32};
    std::vector<storage::Tablet> batches;
    uint64_t random_state = 0x9e3779b97f4a7c15ULL;
    int64_t written = 0;
    int64_t timestamp = 1600000000000LL;
    int64_t batch_index = 0;
    while (written < points) {
        const uint32_t rows = static_cast<uint32_t>(
            std::min<int64_t>(kBatchedRows, points - written));
        batches.emplace_back(names, types, rows);
        storage::Tablet& tablet = batches.back();
        const std::string device = "device_" + std::to_string(batch_index % 8);
        for (uint32_t row = 0; row < rows; ++row) {
            timestamp += 1 + static_cast<int64_t>(
                                 (next_random(random_state) >> 33) % 100);
            tablet.add_timestamp(row, timestamp);
            tablet.add_value(row, "device", device.c_str());
            tablet.add_value(row, "v0",
                             static_cast<int64_t>(next_random(random_state)));
            tablet.add_value(row, "v1",
                             static_cast<int64_t>(next_random(random_state)));
            tablet.add_value(
                row, "v2",
                static_cast<double>(next_random(random_state) >> 11) /
                    static_cast<double>(1ULL << 53));
            tablet.add_value(row, "v3",
                             static_cast<int32_t>(next_random(random_state)));
        }
        written += rows;
        ++batch_index;
    }
    return batches;
}

template <typename SinkFactory>
RunResult run_table_workload(int64_t points, storage::TableSchema& schema,
                             SinkFactory make_sink) {
    std::vector<storage::Tablet> batches = make_batches(points);
    std::unique_ptr<storage::RandomAccessWriteFile> sink = make_sink();

    const double start = now_seconds();
    {
        storage::TsFileTableWriter writer(sink.get(), &schema);
        if (writer.get_error() != common::E_OK) {
            std::cerr << "table writer init failed: " << writer.get_error()
                      << std::endl;
            return {0, 0};
        }
        for (storage::Tablet& tablet : batches) {
            const int ret = writer.write_table(tablet);
            if (ret != common::E_OK) {
                std::cerr << "write_table failed: " << ret << std::endl;
                return {0, 0};
            }
        }
        if (writer.close() != common::E_OK) {
            std::cerr << "table writer close failed" << std::endl;
            return {0, 0};
        }
    }
    const double seconds = now_seconds() - start;
    // The writer's close() closes the sink, so read the size back from the
    // backend itself: the in-memory sink keeps its bytes, the local backend
    // is measured through the file it produced.
    int64_t bytes = 0;
    auto* memory_sink =
        dynamic_cast<storage::MemoryRandomAccessWriteFile*>(sink.get());
    if (memory_sink != nullptr) {
        bytes = memory_sink->size();
    } else {
        std::ifstream in(sink->file_path(), std::ios::binary | std::ios::ate);
        if (in) {
            bytes = static_cast<int64_t>(in.tellg());
        }
    }
    return {seconds, bytes};
}

double median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    return values[values.size() / 2];
}

void print_table_workload(const std::string& name,
                          const std::vector<RunResult>& runs, int64_t points) {
    std::vector<double> seconds;
    for (const RunResult& run : runs) {
        seconds.push_back(run.seconds);
    }
    const double med = median(seconds);
    const double values =
        static_cast<double>(points) * static_cast<double>(kFieldCount);
    int64_t bytes = 0;
    for (const RunResult& run : runs) {
        bytes = run.bytes;
    }
    std::cout << std::left << std::setw(10) << name << std::right << std::fixed
              << std::setprecision(3);
    for (double s : seconds) {
        std::cout << std::setw(9) << s;
    }
    std::cout << std::setw(11) << med << std::setw(11)
              << (static_cast<double>(bytes) / med / 1e6) << std::setw(12)
              << (values / med / 1e6) << std::endl;
}

// Measures the one indirect call the abstraction adds by writing the same
// buffer to a pre-allocated in-memory sink through the concrete class and
// through RandomAccessWriteFile*.  The memory backend is used so filesystem
// and page-cache effects do not swamp the ~1 ns dispatch cost.
RunResult run_memory_dispatch_bench(bool interface_call) {
    std::vector<char> buffer(kDispatchChunkBytes);
    for (size_t i = 0; i < buffer.size(); ++i) {
        buffer[i] = static_cast<char>(i * 131);
    }
    storage::MemoryRandomAccessWriteFile sink;
    storage::RandomAccessWriteFile* sink_iface = &sink;
    const size_t calls = kDispatchTotalBytes / kDispatchChunkBytes;
    const uint32_t chunk = static_cast<uint32_t>(buffer.size());

    const double start = now_seconds();
    int ret = common::E_OK;
    if (interface_call) {
        for (size_t i = 0; i < calls && ret == common::E_OK; ++i) {
            ret = sink_iface->write(buffer.data(), chunk);
        }
    } else {
        for (size_t i = 0; i < calls && ret == common::E_OK; ++i) {
            ret = sink.write(buffer.data(), chunk);
        }
    }
    if (ret != common::E_OK) {
        return {0, 0};
    }
    return {now_seconds() - start, sink.size()};
}

void print_dispatch_workload(const std::string& name, double seconds,
                             int64_t bytes) {
    const double calls =
        static_cast<double>(kDispatchTotalBytes / kDispatchChunkBytes);
    std::cout << std::left << std::setw(26) << name << std::right << std::fixed
              << std::setprecision(3) << std::setw(9) << seconds
              << std::setw(11) << (static_cast<double>(bytes) / seconds / 1e9)
              << std::setw(12) << (seconds * 1e9 / calls) << std::endl;
}

int64_t parse_int_option(const char* arg, const char* prefix,
                         int64_t fallback) {
    const std::string value(arg);
    const std::string name(prefix);
    if (value.rfind(name, 0) != 0) {
        return fallback;
    }
    std::istringstream stream(value.substr(name.size()));
    int64_t parsed = fallback;
    stream >> parsed;
    return stream.fail() ? fallback : parsed;
}

}  // namespace

int main(int argc, char** argv) {
    int64_t points = 1000000;
    int repeat = 3;
    for (int i = 1; i < argc; ++i) {
        points = parse_int_option(argv[i], "--points=", points);
        const int64_t repeat_arg = parse_int_option(argv[i], "--repeat=", 0);
        if (repeat_arg > 0) {
            repeat = static_cast<int>(repeat_arg);
        }
    }
    if (points <= 0 || repeat <= 0) {
        std::cerr << "usage: write_backend_benchmark [--points=N] [--repeat=N]"
                  << std::endl;
        return 2;
    }

    const int init_ret = storage::libtsfile_init();
    if (init_ret != common::E_OK) {
        std::cerr << "failed to initialize TsFile: error " << init_ret
                  << std::endl;
        return 1;
    }

    storage::TableSchema schema = make_table_schema();
    const std::string local_path = "write_backend_benchmark.tsfile";

    std::vector<RunResult> local_runs;
    std::vector<RunResult> memory_runs;
    for (int run = 0; run < repeat; ++run) {
        local_runs.push_back(run_table_workload(points, schema, [&]() {
            std::remove(local_path.c_str());
            std::unique_ptr<storage::WriteFile> file(new storage::WriteFile());
            int flags = O_RDWR | O_CREAT | O_TRUNC;
#ifdef _WIN32
            flags |= O_BINARY;
#endif
            if (file->create(local_path, flags, 0666) != common::E_OK) {
                std::cerr << "failed to create " << local_path << std::endl;
            }
            return std::unique_ptr<storage::RandomAccessWriteFile>(
                file.release());
        }));
        memory_runs.push_back(run_table_workload(points, schema, []() {
            return std::unique_ptr<storage::RandomAccessWriteFile>(
                new storage::MemoryRandomAccessWriteFile(
                    "memory://bench.tsfile"));
        }));
    }
    std::remove(local_path.c_str());

    std::cout << "table-model target: " << points << " rows x " << kFieldCount
              << " fields, " << kBatchedRows << " rows per tablet, " << repeat
              << " runs" << std::endl;
    std::cout << std::left << std::setw(10) << "sink" << std::right;
    for (int run = 1; run <= repeat; ++run) {
        std::cout << std::setw(9) << ("run" + std::to_string(run));
    }
    std::cout << std::setw(11) << "median" << std::setw(11) << "MB/s"
              << std::setw(12) << "Mval/s" << std::endl;
    print_table_workload("local", local_runs, points);
    print_table_workload("memory", memory_runs, points);

    std::cout << "\nsequential " << (kDispatchTotalBytes >> 20) << " MiB in "
              << (kDispatchChunkBytes >> 10)
              << " KiB writes to the in-memory sink (median of 5, alternated)"
              << std::endl;
    std::cout << std::left << std::setw(26) << "sink" << std::right
              << std::setw(9) << "median" << std::setw(11) << "GiB/s"
              << std::setw(12) << "ns/call" << std::endl;
    std::vector<double> concrete;
    std::vector<double> through_interface;
    int64_t bytes = 0;
    for (int run = 0; run < 5; ++run) {
        RunResult direct{0, 0};
        RunResult indirect{0, 0};
        if ((run % 2) == 0) {
            direct = run_memory_dispatch_bench(false);
            indirect = run_memory_dispatch_bench(true);
        } else {
            indirect = run_memory_dispatch_bench(true);
            direct = run_memory_dispatch_bench(false);
        }
        concrete.push_back(direct.seconds);
        through_interface.push_back(indirect.seconds);
        bytes = direct.bytes;
    }
    print_dispatch_workload("memory (concrete)", median(concrete), bytes);
    print_dispatch_workload("memory (interface ptr)", median(through_interface),
                            bytes);

    storage::libtsfile_destroy();
    return 0;
}
