/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "bench_read.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/statistics.h>

#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsblock/tsblock.h"
#include "common/tsblock/vector/fixed_length_vector.h"
#include "common/tsblock/vector/vector.h"
#include "file/write_file.h"
#include "reader/filter/tag_filter.h"
#include "reader/result_set.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "utils/util_define.h"
#include "writer/tsfile_table_writer.h"

#define BENCH_HANDLE_ERROR(err_no)         \
    do {                                   \
        if ((err_no) != 0) {               \
            std::cerr << "tsfile err "     \
                      << (err_no) << "\n"; \
            return (err_no);               \
        }                                  \
    } while (0)

#define BENCH_CHECK_RET_NEG1(expr)                          \
    do {                                                    \
        int _ts_err = (expr);                               \
        if (_ts_err != 0) {                                 \
            std::cerr << "tsfile err " << _ts_err << "\n"; \
            return -1;                                      \
        }                                                   \
    } while (0)

namespace {

static const char* kTable        = "bench_table";
static const char* kTag2Val      = "tag_b";
static const int   kNumDevices   = 10;
static const char* kFilterDevice = "device_0";

static const std::vector<std::string> kReadCols{
    "id1", "id2", "s1", "s2", "s3", "s4"};

static std::string device_name(int i) {
    return "device_" + std::to_string(i);
}

// ─── Cache drop ──────────────────────────────────────────────────────────────

void bench_drop_cache() {
#if defined(__APPLE__)
    if (system("sudo purge") != 0) {
        std::cerr << "[bench] purge failed or not available "
                     "(run `sudo purge` manually before bench_read)\n";
    }
#elif defined(__linux__)
    if (system("sync && sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'") != 0) {
        std::cerr << "[bench] drop_caches failed "
                     "(run `sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'` manually)\n";
    }
#else
    std::cerr << "[bench] bench_drop_cache not supported on this platform\n";
#endif
}

// ─── Write ────────────────────────────────────────────────────────────────────

int write_tsfile(const std::string& path, int64_t row_count) {
    storage::libtsfile_init();
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    BENCH_HANDLE_ERROR(file.create(path.c_str(), flags, 0666));

    auto* schema = new storage::TableSchema(
        std::string(kTable),
        {
            common::ColumnSchema("id1", common::STRING,
                                 common::UNCOMPRESSED, common::PLAIN,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("id2", common::STRING,
                                 common::UNCOMPRESSED, common::PLAIN,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("s1", common::INT64,
                                 common::SNAPPY, common::PLAIN,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s2", common::DOUBLE,
                                 common::SNAPPY, common::PLAIN,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s3", common::FLOAT,
                                 common::SNAPPY, common::PLAIN,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("s4", common::INT32,
                                 common::SNAPPY, common::PLAIN,
                                 common::ColumnCategory::FIELD),
        });

    auto* writer        = new storage::TsFileTableWriter(&file, schema);
    const uint32_t batch_cap    = 65536;
    int64_t        rows_per_dev = row_count / kNumDevices;

    for (int dev = 0; dev < kNumDevices; dev++) {
        std::string dev_id   = device_name(dev);
        int64_t     dev_base = dev * rows_per_dev;

        for (int64_t off = 0; off < rows_per_dev;) {
            uint32_t n = static_cast<uint32_t>(
                std::min<int64_t>(batch_cap, rows_per_dev - off));
            storage::Tablet tablet(
                kTable,
                {"id1", "id2", "s1", "s2", "s3", "s4"},
                {common::STRING, common::STRING, common::INT64,
                 common::DOUBLE, common::FLOAT,  common::INT32},
                {common::ColumnCategory::TAG,   common::ColumnCategory::TAG,
                 common::ColumnCategory::FIELD, common::ColumnCategory::FIELD,
                 common::ColumnCategory::FIELD, common::ColumnCategory::FIELD},
                std::max(n, 1u));
            for (uint32_t i = 0; i < n; i++) {
                int64_t ts = dev_base + off + i;
                BENCH_HANDLE_ERROR(tablet.add_timestamp(i, ts));
                BENCH_HANDLE_ERROR(tablet.add_value(i, "id1", dev_id.c_str()));
                BENCH_HANDLE_ERROR(tablet.add_value(i, "id2", kTag2Val));
                BENCH_HANDLE_ERROR(tablet.add_value(i, "s1", ts));
                BENCH_HANDLE_ERROR(tablet.add_value(i, "s2", ts * 1.1));
                BENCH_HANDLE_ERROR(tablet.add_value(i, "s3", static_cast<float>(ts % 10000)));
                BENCH_HANDLE_ERROR(tablet.add_value(i, "s4", static_cast<int32_t>(ts % 100000)));
            }
            BENCH_HANDLE_ERROR(writer->write_table(tablet));
            off += n;
        }
    }
    BENCH_HANDLE_ERROR(writer->flush());
    BENCH_HANDLE_ERROR(writer->close());
    delete writer;
    delete schema;
    return 0;
}

int write_parquet(const std::string& path, int64_t row_count) {
    try {
        auto schema = arrow::schema({
            arrow::field("time", arrow::int64()),
            arrow::field("id1",  arrow::utf8()),
            arrow::field("id2",  arrow::utf8()),
            arrow::field("s1",   arrow::int64()),
            arrow::field("s2",   arrow::float64()),
            arrow::field("s3",   arrow::float32()),
            arrow::field("s4",   arrow::int32()),
        });

        auto writer_props = parquet::WriterProperties::Builder()
                                .compression(parquet::Compression::SNAPPY)
                                ->build();
        auto arrow_props  = parquet::ArrowWriterProperties::Builder().build();

        const int64_t batch_cap    = 65536;
        int64_t       rows_per_dev = row_count / kNumDevices;
        arrow::MemoryPool* pool    = arrow::default_memory_pool();

        PARQUET_ASSIGN_OR_THROW(auto out,
                                arrow::io::FileOutputStream::Open(path));
        PARQUET_ASSIGN_OR_THROW(
            std::unique_ptr<parquet::arrow::FileWriter> pw,
            parquet::arrow::FileWriter::Open(*schema, pool, out,
                                             writer_props, arrow_props));

        for (int dev = 0; dev < kNumDevices; dev++) {
            std::string dev_id   = device_name(dev);
            int64_t     dev_base = dev * rows_per_dev;

            arrow::Int64Builder  time_b;
            arrow::StringBuilder id1_b;
            arrow::StringBuilder id2_b;
            arrow::Int64Builder  s1_b;
            arrow::DoubleBuilder s2_b;
            arrow::FloatBuilder  s3_b;
            arrow::Int32Builder  s4_b;

            for (int64_t off = 0; off < rows_per_dev;) {
                int64_t n = std::min(batch_cap, rows_per_dev - off);
                time_b.Reset(); id1_b.Reset(); id2_b.Reset();
                s1_b.Reset();   s2_b.Reset();  s3_b.Reset(); s4_b.Reset();
                for (int64_t i = 0; i < n; i++) {
                    int64_t ts = dev_base + off + i;
                    PARQUET_THROW_NOT_OK(time_b.Append(ts));
                    PARQUET_THROW_NOT_OK(id1_b.Append(dev_id));
                    PARQUET_THROW_NOT_OK(id2_b.Append(kTag2Val));
                    PARQUET_THROW_NOT_OK(s1_b.Append(ts));
                    PARQUET_THROW_NOT_OK(s2_b.Append(ts * 1.1));
                    PARQUET_THROW_NOT_OK(s3_b.Append(static_cast<float>(ts % 10000)));
                    PARQUET_THROW_NOT_OK(s4_b.Append(static_cast<int32_t>(ts % 100000)));
                }
                PARQUET_ASSIGN_OR_THROW(auto a_time, time_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_id1,  id1_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_id2,  id2_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_s1,   s1_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_s2,   s2_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_s3,   s3_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_s4,   s4_b.Finish());
                auto batch = arrow::RecordBatch::Make(
                    schema, n,
                    {a_time, a_id1, a_id2, a_s1, a_s2, a_s3, a_s4});
                PARQUET_THROW_NOT_OK(pw->WriteRecordBatch(*batch));
                off += n;
            }
        }
        PARQUET_THROW_NOT_OK(pw->Close());
        PARQUET_THROW_NOT_OK(out->Close());
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "parquet write: " << e.what() << "\n";
        return 1;
    }
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

static void print_result(const char* engine, double secs,
                         int64_t result_rows, int64_t checksum) {
    std::cout << "  " << std::left  << std::setw(16) << engine
              << std::fixed << std::setprecision(4) << secs << " s  |  "
              << std::right << std::setw(12)
              << static_cast<int64_t>(result_rows / secs) << " rows/s"
              << "  |  sum_s1=" << checksum << "\n";
}

// ─── Scenario 1: Tag Filter ───────────────────────────────────────────────────

int64_t tsfile_tag_filter(const std::string& path, int64_t row_count) {
    storage::libtsfile_init();
    storage::TsFileReader reader;
    BENCH_CHECK_RET_NEG1(reader.open(path));

    auto table_schema = reader.get_table_schema(std::string(kTable));
    storage::Filter* tag_filter =
        storage::TagFilterBuilder(table_schema.get()).eq("id1", kFilterDevice);

    storage::ResultSet* rs = nullptr;
    BENCH_CHECK_RET_NEG1(
        reader.query(kTable, kReadCols, 0, row_count, rs, tag_filter));

    int64_t sum = 0;
    bool    has_next = false;
    int     ret = common::E_OK;
    while (IS_SUCC(ret = rs->next(has_next)) && has_next) {
        if (!rs->is_null("s1")) {
            sum += rs->get_value<int64_t>("s1");
        }
    }
    rs->close();
    reader.close();
    delete tag_filter;
    return sum;
}

// Collect row group indices whose statistics overlap the given string equality.
// Equivalent to TsFile's device-level chunk pruning.
static std::vector<int> rg_prune_string_eq(
    const parquet::FileMetaData& meta, int col_idx, const std::string& target) {
    std::vector<int> result;
    for (int rg = 0; rg < meta.num_row_groups(); ++rg) {
        auto stats = meta.RowGroup(rg)->ColumnChunk(col_idx)->statistics();
        if (stats && stats->HasMinMax()) {
            auto s = std::static_pointer_cast<parquet::ByteArrayStatistics>(stats);
            std::string mn(reinterpret_cast<const char*>(s->min().ptr), s->min().len);
            std::string mx(reinterpret_cast<const char*>(s->max().ptr), s->max().len);
            if (target < mn || target > mx) continue;  // prune
        }
        result.push_back(rg);
    }
    return result;
}

// Collect row group indices whose time range overlaps [ts_start, ts_end).
// Equivalent to TsFile's page-level time statistics pruning.
static std::vector<int> rg_prune_time_range(
    const parquet::FileMetaData& meta, int col_idx,
    int64_t ts_start, int64_t ts_end) {
    std::vector<int> result;
    for (int rg = 0; rg < meta.num_row_groups(); ++rg) {
        auto stats = meta.RowGroup(rg)->ColumnChunk(col_idx)->statistics();
        if (stats && stats->HasMinMax()) {
            auto s = std::static_pointer_cast<parquet::Int64Statistics>(stats);
            if (s->max() < ts_start || s->min() >= ts_end) continue;  // prune
        }
        result.push_back(rg);
    }
    return result;
}

int64_t parquet_tag_filter(const std::string& path) {
    try {
        std::vector<std::string> cols{"time", "id1", "id2", "s1", "s2", "s3", "s4"};
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        PARQUET_ASSIGN_OR_THROW(auto infile,
                                arrow::io::ReadableFile::Open(path));
        PARQUET_ASSIGN_OR_THROW(
            std::unique_ptr<parquet::arrow::FileReader> reader,
            parquet::arrow::OpenFile(infile, pool));

        std::shared_ptr<arrow::Schema> file_schema;
        PARQUET_THROW_NOT_OK(reader->GetSchema(&file_schema));
        std::vector<int> indices;
        for (const auto& name : cols)
            indices.push_back(file_schema->GetFieldIndex(name));

        // Row group pruning via min/max statistics on id1 column.
        auto& meta = *reader->parquet_reader()->metadata();
        int id1_col = meta.schema()->ColumnIndex("id1");
        auto matching_rgs = rg_prune_string_eq(meta, id1_col, kFilterDevice);

        PARQUET_ASSIGN_OR_THROW(auto batch_reader,
            reader->GetRecordBatchReader(matching_rgs, indices));

        int64_t sum = 0;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch) {
            auto id1_arr = std::static_pointer_cast<arrow::StringArray>(
                batch->GetColumnByName("id1"));
            auto s1_arr = std::static_pointer_cast<arrow::Int64Array>(
                batch->GetColumnByName("s1"));
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if (!id1_arr->IsNull(i) &&
                    id1_arr->GetString(i) == kFilterDevice &&
                    !s1_arr->IsNull(i)) {
                    sum += s1_arr->Value(i);
                }
            }
        }
        return sum;
    } catch (const std::exception& e) {
        std::cerr << "parquet tag filter: " << e.what() << "\n";
        return -1;
    }
}

// ─── Scenario 2: Time Range Filter ───────────────────────────────────────────

// TsFile query(start, end) is inclusive on both sides: [start, end].
// Pass (ts_end - 1) to match Parquet's half-open [ts_start, ts_end) semantics.
int64_t tsfile_time_filter(const std::string& path,
                            int64_t ts_start, int64_t ts_end) {
    storage::libtsfile_init();
    storage::TsFileReader reader;
    BENCH_CHECK_RET_NEG1(reader.open(path));

    storage::ResultSet* rs = nullptr;
    BENCH_CHECK_RET_NEG1(
        reader.query(kTable, kReadCols, ts_start, ts_end - 1, rs, nullptr));

    int64_t sum = 0;
    bool    has_next = false;
    int     ret = common::E_OK;
    while (IS_SUCC(ret = rs->next(has_next)) && has_next) {
        if (!rs->is_null("s1")) sum += rs->get_value<int64_t>("s1");
    }
    rs->close();
    reader.close();
    return sum;
}

int64_t parquet_time_filter(const std::string& path,
                             int64_t ts_start, int64_t ts_end) {
    try {
        std::vector<std::string> cols{"time", "id1", "id2", "s1", "s2", "s3", "s4"};
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        PARQUET_ASSIGN_OR_THROW(auto infile,
                                arrow::io::ReadableFile::Open(path));
        PARQUET_ASSIGN_OR_THROW(
            std::unique_ptr<parquet::arrow::FileReader> reader,
            parquet::arrow::OpenFile(infile, pool));

        std::shared_ptr<arrow::Schema> file_schema;
        PARQUET_THROW_NOT_OK(reader->GetSchema(&file_schema));
        std::vector<int> indices;
        for (const auto& name : cols)
            indices.push_back(file_schema->GetFieldIndex(name));

        // Row group pruning via min/max statistics on time column.
        auto& meta = *reader->parquet_reader()->metadata();
        int time_col = meta.schema()->ColumnIndex("time");
        auto matching_rgs = rg_prune_time_range(meta, time_col, ts_start, ts_end);

        PARQUET_ASSIGN_OR_THROW(auto batch_reader,
            reader->GetRecordBatchReader(matching_rgs, indices));

        int64_t sum = 0;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch) {
            auto time_arr = std::static_pointer_cast<arrow::Int64Array>(
                batch->GetColumnByName("time"));
            auto s1_arr = std::static_pointer_cast<arrow::Int64Array>(
                batch->GetColumnByName("s1"));
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                int64_t t = time_arr->Value(i);
                if (t >= ts_start && t < ts_end && !s1_arr->IsNull(i))
                    sum += s1_arr->Value(i);
            }
        }
        return sum;
    } catch (const std::exception& e) {
        std::cerr << "parquet time filter: " << e.what() << "\n";
        return -1;
    }
}

// ─── Optimized: Batch columnar read ──────────────────────────────────────────

// Find the 0-based TsBlock vector index for a named column.
// ResultSetMetadata prepends "time" as column 1 (1-indexed), so
// TsBlock vector index = metadata column index - 1.
static int find_vec_idx(storage::ResultSet* rs, const std::string& name) {
    auto meta = rs->get_metadata();
    for (int i = 1; i <= static_cast<int>(meta->get_column_count()); ++i) {
        if (meta->get_column_name(i) == name) return i - 1;
    }
    return -1;
}

// Sum all INT64 values in a Vector, using direct buffer access for the
// common no-null case to avoid per-element overhead.
static int64_t sum_vec_int64(common::Vector* vec, uint32_t rows) {
    int64_t sum = 0;
    if (!vec->has_null()) {
        // Fast path: dense int64_t array, single pointer scan.
        const int64_t* p = reinterpret_cast<const int64_t*>(
            vec->get_value_data().get_data());
        for (uint32_t r = 0; r < rows; ++r) sum += p[r];
    } else {
        // Slow path: skip null rows; advance sequential cursor manually.
        vec->reset_offset();
        for (uint32_t r = 0; r < rows; ++r) {
            if (!vec->is_null(r)) {
                uint32_t len = 0;
                bool null = false;
                char* val = vec->read(&len, &null, r);
                sum += *reinterpret_cast<int64_t*>(val);
                vec->update_offset();
            }
        }
    }
    return sum;
}

// batch_size controls TsBlock capacity; 65536 rows/block matches write batches.
static const int kBatchSize = 65536;

int64_t tsfile_tag_filter_batch(const std::string& path, int64_t row_count) {
    storage::libtsfile_init();
    storage::TsFileReader reader;
    BENCH_CHECK_RET_NEG1(reader.open(path));

    auto table_schema = reader.get_table_schema(std::string(kTable));
    storage::Filter* tag_filter =
        storage::TagFilterBuilder(table_schema.get()).eq("id1", kFilterDevice);

    storage::ResultSet* rs = nullptr;
    BENCH_CHECK_RET_NEG1(
        reader.query(kTable, kReadCols, 0, row_count, rs, tag_filter, kBatchSize));

    const int s1_idx = find_vec_idx(rs, "s1");
    int64_t   sum    = 0;
    common::TsBlock* block = nullptr;
    while (rs->get_next_tsblock(block) == common::E_OK && block) {
        sum += sum_vec_int64(block->get_vector(s1_idx), block->get_row_count());
    }
    rs->close();
    reader.close();
    delete tag_filter;
    return sum;
}

int64_t tsfile_time_filter_batch(const std::string& path,
                                  int64_t ts_start, int64_t ts_end) {
    storage::libtsfile_init();
    storage::TsFileReader reader;
    BENCH_CHECK_RET_NEG1(reader.open(path));

    storage::ResultSet* rs = nullptr;
    BENCH_CHECK_RET_NEG1(
        reader.query(kTable, kReadCols, ts_start, ts_end - 1, rs, kBatchSize));

    const int s1_idx = find_vec_idx(rs, "s1");
    int64_t   sum    = 0;
    common::TsBlock* block = nullptr;
    while (rs->get_next_tsblock(block) == common::E_OK && block) {
        sum += sum_vec_int64(block->get_vector(s1_idx), block->get_row_count());
    }
    rs->close();
    reader.close();
    return sum;
}

}  // namespace

// ─── Entry point ─────────────────────────────────────────────────────────────

int bench_write(int64_t row_count, bool run_parquet) {
    const std::string ts_path = "read_perf_bench.tsfile";
    const std::string pq_path = "read_perf_bench.parquet";

    std::cout << "rows_total=" << row_count
              << "  devices=" << kNumDevices
              << "  rows_per_device=" << row_count / kNumDevices
              << "\ncolumns: time, id1, id2, s1(INT64), s2(DOUBLE),"
                 " s3(FLOAT), s4(INT32)\ncompression: SNAPPY\n";

    {
        using clock = std::chrono::high_resolution_clock;
        auto t0 = clock::now();
        if (write_tsfile(ts_path, row_count) != 0) return 1;
        double s = std::chrono::duration<double>(clock::now() - t0).count();
        std::cout << "write TsFile  : " << std::fixed << std::setprecision(3)
                  << s << " s\n";
    }
    if (run_parquet) {
        using clock = std::chrono::high_resolution_clock;
        auto t0 = clock::now();
        if (write_parquet(pq_path, row_count) != 0) return 1;
        double s = std::chrono::duration<double>(clock::now() - t0).count();
        std::cout << "write Parquet : " << std::fixed << std::setprecision(3)
                  << s << " s\n";
    }
    std::cout << "\n";
    return 0;
}

int bench_read(int64_t row_count, bool run_parquet) {
    int64_t rows_per_device  = row_count / kNumDevices;
    // TIME_FILTER: query the first 1/3 of the total time range.
    // Timestamps are laid out as [0, row_count) across all devices.
    int64_t time_range_start = 0;
    int64_t time_range_end   = row_count / 3;   // ~333K rows for 1M total
    int64_t time_result_rows = time_range_end - time_range_start;

    const std::string ts_path = "read_perf_bench.tsfile";
    const std::string pq_path = "read_perf_bench.parquet";

    std::cout << "\n";

    using clock = std::chrono::high_resolution_clock;

    // ── Scenario 1: Tag Filter ────────────────────────────────────────────────
    std::cout << "[TAG_FILTER] id1=\"" << kFilterDevice
              << "\"  result_rows=" << rows_per_device << "\n";

    auto    t0              = clock::now();
    int64_t sum_ts_tag_row  = tsfile_tag_filter(ts_path, row_count);
    double  sec_ts_tag_row  = std::chrono::duration<double>(clock::now() - t0).count();
    if (sum_ts_tag_row < 0) return 1;

    auto    t1              = clock::now();
    int64_t sum_ts_tag_bat  = tsfile_tag_filter_batch(ts_path, row_count);
    double  sec_ts_tag_bat  = std::chrono::duration<double>(clock::now() - t1).count();
    if (sum_ts_tag_bat < 0) return 1;

    print_result("TsFile (row)",   sec_ts_tag_row, rows_per_device, sum_ts_tag_row);
    print_result("TsFile (batch)", sec_ts_tag_bat, rows_per_device, sum_ts_tag_bat);
    if (run_parquet) {
        auto    t2         = clock::now();
        int64_t sum_pq_tag = parquet_tag_filter(pq_path);
        double  sec_pq_tag = std::chrono::duration<double>(clock::now() - t2).count();
        if (sum_pq_tag < 0) return 1;
        print_result("Parquet+Arrow",  sec_pq_tag, rows_per_device, sum_pq_tag);
        if (sum_ts_tag_row != sum_pq_tag || sum_ts_tag_bat != sum_pq_tag)
            std::cerr << "  warning: tag filter checksum mismatch\n";
    }
    std::cout << "\n";

    // ── Scenario 2: Time Range Filter ─────────────────────────────────────────
    // Both TsFile and Parquet query the identical half-open interval
    // [time_range_start, time_range_end).  TsFile query() is inclusive on
    // both ends, so pass (time_range_end - 1) as the upper bound.
    std::cout << "[TIME_FILTER] time in [" << time_range_start
              << ", " << time_range_end << ")"
              << "  result_rows=" << time_result_rows << "\n";

    auto    t3               = clock::now();
    int64_t sum_ts_time_row  = tsfile_time_filter(ts_path, time_range_start, time_range_end);
    double  sec_ts_time_row  = std::chrono::duration<double>(clock::now() - t3).count();
    if (sum_ts_time_row < 0) return 1;

    auto    t4               = clock::now();
    int64_t sum_ts_time_bat  = tsfile_time_filter_batch(ts_path, time_range_start, time_range_end);
    double  sec_ts_time_bat  = std::chrono::duration<double>(clock::now() - t4).count();
    if (sum_ts_time_bat < 0) return 1;

    print_result("TsFile (row)",   sec_ts_time_row, time_result_rows, sum_ts_time_row);
    print_result("TsFile (batch)", sec_ts_time_bat, time_result_rows, sum_ts_time_bat);
    if (run_parquet) {
        auto    t5          = clock::now();
        int64_t sum_pq_time = parquet_time_filter(pq_path, time_range_start, time_range_end);
        double  sec_pq_time = std::chrono::duration<double>(clock::now() - t5).count();
        if (sum_pq_time < 0) return 1;
        print_result("Parquet+Arrow",  sec_pq_time, time_result_rows, sum_pq_time);
        if (sum_ts_time_row != sum_pq_time || sum_ts_time_bat != sum_pq_time)
            std::cerr << "  warning: time filter checksum mismatch\n";
    }

    return 0;
}
