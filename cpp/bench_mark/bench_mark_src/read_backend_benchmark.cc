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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/global.h"
#include "file/read_file.h"
#include "file/tsfile_io_reader.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"

namespace {

const int32_t kSequentialBlockSize = 64 * 1024;
const int32_t kRandomBlockSize = 4 * 1024;
const size_t kRandomOperationsPerFile = 50000;
const size_t kMetadataLoadsPerFile = 100;
const size_t kRandomQueriesPerFile = 200;
const int kQueryRowLimit = 64;
const size_t kConcurrentOperationsPerFile = 50000;

struct Result {
    Result()
        : bytes(0),
          operations(0),
          checksum(0),
          seconds(0),
          success(true),
          skipped(false) {}

    uint64_t bytes;
    uint64_t operations;
    uint64_t checksum;
    double seconds;
    bool success;
    bool skipped;
};

struct QueryPlan {
    QueryPlan() : table_model(false), row_count(0) {}

    bool table_model;
    std::string table_name;
    std::vector<std::string> columns_or_paths;
    int row_count;
};

typedef std::vector<std::unique_ptr<storage::ReadFile>> OpenFiles;

uint64_t update_checksum(uint64_t checksum, const std::vector<char>& buffer,
                         int32_t read_len) {
    if (read_len == 0) {
        return checksum;
    }
    const uint64_t first =
        static_cast<uint64_t>(static_cast<unsigned char>(buffer.front()));
    const uint64_t last = static_cast<uint64_t>(
        static_cast<unsigned char>(buffer[static_cast<size_t>(read_len - 1)]));
    return (checksum * 1099511628211ULL) ^ (first << 8) ^ last ^
           static_cast<uint64_t>(read_len);
}

int consume_result(storage::TsFileReader& reader, storage::ResultSet* result,
                   uint64_t& row_count) {
    row_count = 0;
    if (result == nullptr) {
        return common::E_INVALID_ARG;
    }
    bool has_next = false;
    int ret = common::E_OK;
    while ((ret = result->next(has_next)) == common::E_OK && has_next) {
        ++row_count;
    }
    reader.destroy_query_data_set(result);
    return ret;
}

int build_query_plan(storage::TsFileReader& reader, QueryPlan& plan,
                     bool& found) {
    found = false;
    const std::vector<std::shared_ptr<storage::TableSchema>> table_schemas =
        reader.get_all_table_schemas();
    for (size_t i = 0; i < table_schemas.size(); ++i) {
        if (table_schemas[i] == nullptr) {
            continue;
        }
        const std::vector<std::string> columns =
            table_schemas[i]->get_measurement_names();
        if (columns.empty()) {
            continue;
        }
        storage::ResultSet* result = nullptr;
        const int ret = reader.queryByRow(table_schemas[i]->get_table_name(),
                                          columns, 0, -1, result);
        uint64_t row_count = 0;
        const int consume_ret = ret == common::E_OK
                                    ? consume_result(reader, result, row_count)
                                    : ret;
        if (ret != common::E_OK) {
            if (result != nullptr) {
                reader.destroy_query_data_set(result);
            }
            return ret;
        }
        if (consume_ret != common::E_OK) {
            return consume_ret;
        }
        if (row_count >
            static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            return common::E_NOT_SUPPORT;
        }
        if (row_count > 0) {
            plan.table_model = true;
            plan.table_name = table_schemas[i]->get_table_name();
            plan.columns_or_paths = columns;
            plan.row_count = static_cast<int>(row_count);
            found = true;
            return common::E_OK;
        }
    }

    const std::vector<std::shared_ptr<storage::IDeviceID>> devices =
        reader.get_all_device_ids();
    for (size_t device_index = 0; device_index < devices.size();
         ++device_index) {
        if (devices[device_index] == nullptr) {
            continue;
        }
        std::vector<storage::MeasurementSchema> schemas;
        const int schema_ret =
            reader.get_timeseries_schema(devices[device_index], schemas);
        if (schema_ret != common::E_OK) {
            return schema_ret;
        }
        if (schemas.empty()) {
            continue;
        }
        std::vector<std::string> paths;
        const size_t path_count = std::min<size_t>(schemas.size(), 4);
        for (size_t schema_index = 0; schema_index < path_count;
             ++schema_index) {
            paths.push_back(devices[device_index]->get_device_name() + "." +
                            schemas[schema_index].measurement_name_);
        }
        storage::ResultSet* result = nullptr;
        const int ret = reader.queryByRow(paths, 0, -1, result);
        uint64_t row_count = 0;
        const int consume_ret = ret == common::E_OK
                                    ? consume_result(reader, result, row_count)
                                    : ret;
        if (ret != common::E_OK) {
            if (result != nullptr) {
                reader.destroy_query_data_set(result);
            }
            return ret;
        }
        if (consume_ret != common::E_OK) {
            return consume_ret;
        }
        if (row_count >
            static_cast<uint64_t>(std::numeric_limits<int>::max())) {
            return common::E_NOT_SUPPORT;
        }
        if (row_count > 0) {
            plan.table_model = false;
            plan.columns_or_paths = paths;
            plan.row_count = static_cast<int>(row_count);
            found = true;
            return common::E_OK;
        }
    }
    return common::E_OK;
}

bool open_files(const std::vector<std::string>& paths, OpenFiles& files) {
    files.clear();
    for (size_t i = 0; i < paths.size(); ++i) {
        std::unique_ptr<storage::ReadFile> file(new storage::ReadFile());
        const int ret = file->open(paths[i]);
        if (ret != common::E_OK) {
            std::cerr << "failed to open " << paths[i] << ": error " << ret
                      << std::endl;
            return false;
        }
        files.push_back(std::move(file));
    }
    return true;
}

Result run_sequential(const OpenFiles& files) {
    Result result;
    std::vector<char> buffer(static_cast<size_t>(kSequentialBlockSize));
    const std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();
    for (size_t file_index = 0; file_index < files.size(); ++file_index) {
        storage::ReadFile& file = *files[file_index];
        for (int64_t offset = 0; offset < file.file_size();
             offset += kSequentialBlockSize) {
            int32_t read_len = 0;
            const int ret = file.read(offset, buffer.data(),
                                      kSequentialBlockSize, read_len);
            if (ret != common::E_OK) {
                std::cerr << "sequential read failed: error " << ret
                          << std::endl;
                result.success = false;
                return result;
            }
            result.bytes += static_cast<uint64_t>(read_len);
            ++result.operations;
            result.checksum =
                update_checksum(result.checksum, buffer, read_len);
        }
    }
    result.seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    return result;
}

Result run_random(const OpenFiles& files, int32_t requested_block_size,
                  size_t operations_per_file) {
    Result result;
    std::vector<char> buffer(static_cast<size_t>(requested_block_size));
    std::mt19937_64 random(0x903ULL);
    const std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();
    for (size_t file_index = 0; file_index < files.size(); ++file_index) {
        storage::ReadFile& file = *files[file_index];
        const uint64_t file_size = static_cast<uint64_t>(file.file_size());
        const int32_t block_size = static_cast<int32_t>(std::min<uint64_t>(
            file_size, static_cast<uint64_t>(requested_block_size)));
        const uint64_t maximum_offset = file_size - block_size;
        std::uniform_int_distribution<uint64_t> offsets(0, maximum_offset);
        for (size_t operation = 0; operation < operations_per_file;
             ++operation) {
            int32_t read_len = 0;
            const uint64_t offset = offsets(random);
            const int ret = file.read(static_cast<int64_t>(offset),
                                      buffer.data(), block_size, read_len);
            if (ret != common::E_OK || read_len != block_size) {
                std::cerr << "random read failed: error " << ret << std::endl;
                result.success = false;
                return result;
            }
            result.bytes += static_cast<uint64_t>(read_len);
            ++result.operations;
            result.checksum =
                update_checksum(result.checksum, buffer, read_len);
        }
    }
    result.seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    return result;
}

Result run_metadata(const std::vector<std::string>& paths,
                    const std::vector<uint64_t>& file_sizes) {
    Result result;
    const std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();
    for (size_t file_index = 0; file_index < paths.size(); ++file_index) {
        for (size_t operation = 0; operation < kMetadataLoadsPerFile;
             ++operation) {
            storage::TsFileIOReader reader;
            const int ret = reader.init(paths[file_index]);
            if (ret != common::E_OK) {
                std::cerr << "metadata reader open failed: error " << ret
                          << std::endl;
                result.success = false;
                return result;
            }
            storage::TsFileMeta* metadata = reader.get_tsfile_meta();
            if (metadata == nullptr || metadata->meta_offset_ <= 0 ||
                static_cast<uint64_t>(metadata->meta_offset_) >=
                    file_sizes[file_index]) {
                std::cerr << "metadata load produced an invalid offset"
                          << std::endl;
                result.success = false;
                return result;
            }
            const uint64_t metadata_bytes =
                file_sizes[file_index] -
                static_cast<uint64_t>(metadata->meta_offset_);
            result.bytes += metadata_bytes;
            ++result.operations;
            result.checksum =
                (result.checksum * 1099511628211ULL) ^ metadata_bytes ^
                static_cast<uint64_t>(metadata->table_schemas_.size()) ^
                static_cast<uint64_t>(metadata->tsfile_properties_.size());
        }
    }
    result.seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    return result;
}

Result run_random_queries(const std::vector<std::string>& paths) {
    Result result;
    std::vector<std::unique_ptr<storage::TsFileReader>> readers;
    std::vector<QueryPlan> plans;
    readers.reserve(paths.size());
    plans.reserve(paths.size());
    for (size_t file_index = 0; file_index < paths.size(); ++file_index) {
        std::unique_ptr<storage::TsFileReader> reader(
            new storage::TsFileReader());
        const int ret = reader->open(paths[file_index]);
        if (ret != common::E_OK) {
            std::cerr << "query reader open failed: error " << ret << std::endl;
            result.success = false;
            return result;
        }
        QueryPlan plan;
        bool found = false;
        const int plan_ret = build_query_plan(*reader, plan, found);
        if (plan_ret != common::E_OK) {
            std::cerr << "random-query planning failed for "
                      << paths[file_index] << ": error " << plan_ret
                      << std::endl;
            result.success = false;
            return result;
        }
        if (!found) {
            std::cerr << "skipping random queries for " << paths[file_index]
                      << ": no queryable rows" << std::endl;
            continue;
        }
        readers.push_back(std::move(reader));
        plans.push_back(plan);
    }
    if (readers.empty()) {
        result.skipped = true;
        return result;
    }

    std::mt19937 random(0x903U);
    const std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();
    for (size_t file_index = 0; file_index < readers.size(); ++file_index) {
        QueryPlan& plan = plans[file_index];
        std::uniform_int_distribution<int> offsets(0, plan.row_count - 1);
        for (size_t operation = 0; operation < kRandomQueriesPerFile;
             ++operation) {
            const int offset = offsets(random);
            const int limit = std::min(kQueryRowLimit, plan.row_count - offset);
            storage::ResultSet* query_result = nullptr;
            int ret = common::E_OK;
            if (plan.table_model) {
                ret = readers[file_index]->queryByRow(
                    plan.table_name, plan.columns_or_paths, offset, limit,
                    query_result);
            } else {
                ret = readers[file_index]->queryByRow(
                    plan.columns_or_paths, offset, limit, query_result);
            }
            uint64_t rows = 0;
            if (ret != common::E_OK ||
                consume_result(*readers[file_index], query_result, rows) !=
                    common::E_OK ||
                rows != static_cast<uint64_t>(limit)) {
                if (query_result != nullptr && ret != common::E_OK) {
                    readers[file_index]->destroy_query_data_set(query_result);
                }
                std::cerr << "random query failed: error " << ret << std::endl;
                result.success = false;
                return result;
            }
            ++result.operations;
            result.checksum = (result.checksum * 1099511628211ULL) ^
                              static_cast<uint64_t>(offset) ^ (rows << 32);
        }
    }
    result.seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    return result;
}

Result run_concurrent(const std::vector<std::string>& paths) {
    const std::chrono::steady_clock::time_point start =
        std::chrono::steady_clock::now();
    std::vector<Result> per_file(paths.size());
    std::vector<std::thread> workers;
    workers.reserve(paths.size());
    for (size_t file_index = 0; file_index < paths.size(); ++file_index) {
        workers.push_back(std::thread([&, file_index]() {
            storage::ReadFile file;
            if (file.open(paths[file_index]) != common::E_OK) {
                per_file[file_index].success = false;
                return;
            }
            const uint64_t file_size = static_cast<uint64_t>(file.file_size());
            const int32_t block_size = static_cast<int32_t>(
                std::min<uint64_t>(file_size, kRandomBlockSize));
            const uint64_t maximum_offset = file_size - block_size;
            std::mt19937_64 random(0x903ULL + file_index);
            std::uniform_int_distribution<uint64_t> offsets(0, maximum_offset);
            std::vector<char> buffer(static_cast<size_t>(block_size));
            Result& result = per_file[file_index];
            for (size_t operation = 0; operation < kConcurrentOperationsPerFile;
                 ++operation) {
                int32_t read_len = 0;
                const int ret = file.read(static_cast<int64_t>(offsets(random)),
                                          buffer.data(), block_size, read_len);
                if (ret != common::E_OK || read_len != block_size) {
                    result.success = false;
                    return;
                }
                result.bytes += static_cast<uint64_t>(read_len);
                ++result.operations;
                result.checksum =
                    update_checksum(result.checksum, buffer, read_len);
            }
        }));
    }
    for (size_t i = 0; i < workers.size(); ++i) {
        workers[i].join();
    }

    Result total;
    for (size_t i = 0; i < per_file.size(); ++i) {
        total.success = total.success && per_file[i].success;
        total.bytes += per_file[i].bytes;
        total.operations += per_file[i].operations;
        total.checksum ^= per_file[i].checksum;
    }
    total.seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    return total;
}

void print_result(const char* workload, const Result& result,
                  bool report_throughput = true) {
    if (result.skipped) {
        std::cout << std::left << std::setw(24) << workload
                  << "SKIPPED (no queryable rows)" << std::endl;
        return;
    }
    if (!result.success) {
        std::cout << std::left << std::setw(24) << workload << "FAILED"
                  << std::endl;
        return;
    }
    const double mib = static_cast<double>(result.bytes) / (1024.0 * 1024.0);
    const double throughput = result.seconds > 0 ? mib / result.seconds : 0;
    const double operations_per_second =
        result.seconds > 0 ? result.operations / result.seconds : 0;
    std::cout << std::left << std::setw(24) << workload << std::right
              << std::fixed << std::setprecision(3) << std::setw(10)
              << result.seconds << " s  ";
    if (report_throughput) {
        std::cout << std::setw(12) << throughput << " MiB/s  ";
    } else {
        std::cout << std::setw(21) << "";
    }
    std::cout << std::setw(14) << operations_per_second
              << " ops/s  checksum=" << result.checksum << std::endl;
}

const char* backend_name(common::FileReadBackend backend) {
    return backend == common::FileReadBackend::MMAP ? "MMAP" : "PREAD";
}

bool run_backend(common::FileReadBackend backend,
                 const std::vector<std::string>& paths) {
    if (common::set_file_read_backend(backend) != common::E_OK) {
        return false;
    }
    OpenFiles files;
    if (!open_files(paths, files)) {
        return false;
    }

    std::cout << "\n" << backend_name(backend) << std::endl;
    const Result sequential = run_sequential(files);
    const Result random =
        run_random(files, kRandomBlockSize, kRandomOperationsPerFile);
    std::vector<uint64_t> file_sizes;
    file_sizes.reserve(files.size());
    for (size_t i = 0; i < files.size(); ++i) {
        file_sizes.push_back(static_cast<uint64_t>(files[i]->file_size()));
    }
    print_result("sequential-64KiB", sequential);
    print_result("random-read-4KiB", random);
    files.clear();
    const Result metadata = run_metadata(paths, file_sizes);
    print_result("metadata-parse", metadata);
    const Result random_queries = run_random_queries(paths);
    print_result("random-query-by-row", random_queries, false);
    const Result concurrent = run_concurrent(paths);
    print_result("concurrent-multi-file", concurrent);
    return sequential.success && random.success && metadata.success &&
           random_queries.success && concurrent.success;
}

}  // namespace

int main(int argc, char** argv) {
    const bool mmap_first = argc > 1 && std::string(argv[1]) == "--mmap-first";
    const int first_path = mmap_first ? 2 : 1;
    if (argc <= first_path) {
        std::cerr << "usage: read_backend_benchmark [--mmap-first] FILE.tsfile "
                     "[FILE.tsfile ...]"
                  << std::endl;
        return 2;
    }

    std::vector<std::string> paths;
    for (int i = first_path; i < argc; ++i) {
        paths.push_back(argv[i]);
    }

    const int init_ret = storage::libtsfile_init();
    if (init_ret != common::E_OK) {
        std::cerr << "failed to initialize TsFile: error " << init_ret
                  << std::endl;
        return 1;
    }

    const common::FileReadBackend original = common::get_file_read_backend();
    bool pread_ok = false;
    bool mmap_ok = false;
    if (mmap_first) {
        mmap_ok = run_backend(common::FileReadBackend::MMAP, paths);
        pread_ok = run_backend(common::FileReadBackend::PREAD, paths);
    } else {
        pread_ok = run_backend(common::FileReadBackend::PREAD, paths);
        mmap_ok = run_backend(common::FileReadBackend::MMAP, paths);
    }
    common::set_file_read_backend(original);
    storage::libtsfile_destroy();
    std::cout << "\nRun again with the opposite order (toggle --mmap-first) "
                 "when measuring warm page-cache effects."
              << std::endl;
    return pread_ok && mmap_ok ? 0 : 1;
}
