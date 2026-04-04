/*
 * Read Performance Benchmark for TsFile C++ vs Parquet+Arrow
 *
 * Experiments:
 *   0) Write test data
 *   1) Full scan baseline (all devices, all time)
 *   2) Tag filter (single device selection, B-tree index pruning)
 *   3) Time filter at varying selectivity (1%, 10%, 50%, 100%)
 *   4) Batch vs row-by-row read mode
 *   5) Warm cache effect (repeated reads, device node cache)
 *
 * SIMD / threading experiments: same binary, controlled via cmake defines.
 * run.sh builds multiple configurations and compares.
 */

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <fcntl.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/statistics.h>
#include <sys/stat.h>

#include <chrono>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

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
#include "writer/tsfile_table_writer.h"

// ─── Configuration ───────────────────────────────────────────────────────────

static const char* kTable = "bench_table";
static const char* kTag2Val = "tag_b";
static const int kNumDevices = 10;
static const int kBatchSize = 65536;
static const char* kFilterDevice = "device_0";

static int64_t gRowCount = 10000000;  // 10M default
static std::string gTsPath = "bench_read.tsfile";
static std::string gPqPath = "bench_read.parquet";
static std::string gCsvOut = "results.csv";

#define HANDLE_ERR(expr)                                               \
    do {                                                               \
        int _e = (expr);                                               \
        if (_e != 0) {                                                 \
            std::cerr << "ERROR " << _e << " at " << __LINE__ << "\n"; \
            return _e;                                                 \
        }                                                              \
    } while (0)

using Clock = std::chrono::high_resolution_clock;

// ─── Helpers ─────────────────────────────────────────────────────────────────

static std::string device_name(int i) { return "device_" + std::to_string(i); }

static int64_t sum_vec_int64(common::Vector* vec, uint32_t rows) {
    int64_t sum = 0;
    if (!vec->has_null()) {
        const int64_t* p =
            reinterpret_cast<const int64_t*>(vec->get_value_data().get_data());
        for (uint32_t r = 0; r < rows; ++r) sum += p[r];
    } else {
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

static int find_vec_idx(storage::ResultSet* rs, const std::string& name) {
    auto meta = rs->get_metadata();
    for (int i = 1; i <= static_cast<int>(meta->get_column_count()); ++i) {
        if (meta->get_column_name(i) == name) return i - 1;
    }
    return -1;
}

struct BenchResult {
    std::string experiment;
    std::string engine;
    std::string params;
    double seconds;
    int64_t result_rows;
    int64_t checksum;
};

static std::vector<BenchResult> gResults;

static void record(const std::string& exp, const std::string& engine,
                   const std::string& params, double secs, int64_t rows,
                   int64_t checksum) {
    gResults.push_back({exp, engine, params, secs, rows, checksum});
    double throughput = rows / secs;
    std::cout << "  " << std::left << std::setw(24) << exp << std::setw(16)
              << engine << std::fixed << std::setprecision(4) << secs << " s  "
              << std::right << std::setw(12) << static_cast<int64_t>(throughput)
              << " rows/s"
              << "  sum=" << checksum;
    if (!params.empty()) std::cout << "  [" << params << "]";
    std::cout << "\n";
}

static void write_csv() {
    std::ofstream f(gCsvOut);
    f << "experiment,engine,params,seconds,result_rows,rows_per_sec,checksum\n";
    for (auto& r : gResults) {
        f << r.experiment << "," << r.engine << "," << r.params << ","
          << std::fixed << std::setprecision(6) << r.seconds << ","
          << r.result_rows << ","
          << static_cast<int64_t>(r.result_rows / r.seconds) << ","
          << r.checksum << "\n";
    }
    std::cout << "\nResults written to " << gCsvOut << "\n";
}

// ─── Write TsFile ────────────────────────────────────────────────────────────

static int write_tsfile() {
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    HANDLE_ERR(file.create(gTsPath.c_str(), flags, 0666));

    auto* schema = new storage::TableSchema(
        std::string(kTable),
        {
            common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("id2", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("s1", common::INT64, common::SNAPPY,
                                 common::PLAIN, common::ColumnCategory::FIELD),
            common::ColumnSchema("s2", common::DOUBLE, common::SNAPPY,
                                 common::PLAIN, common::ColumnCategory::FIELD),
            common::ColumnSchema("s3", common::FLOAT, common::SNAPPY,
                                 common::PLAIN, common::ColumnCategory::FIELD),
            common::ColumnSchema("s4", common::INT32, common::SNAPPY,
                                 common::PLAIN, common::ColumnCategory::FIELD),
        });

    auto* writer = new storage::TsFileTableWriter(&file, schema);
    const uint32_t batch_cap = 65536;
    int64_t rows_per_dev = gRowCount / kNumDevices;

    for (int dev = 0; dev < kNumDevices; dev++) {
        std::string dev_id = device_name(dev);
        int64_t dev_base = dev * rows_per_dev;
        for (int64_t off = 0; off < rows_per_dev;) {
            uint32_t n = static_cast<uint32_t>(
                std::min<int64_t>(batch_cap, rows_per_dev - off));
            storage::Tablet tablet(
                kTable, {"id1", "id2", "s1", "s2", "s3", "s4"},
                {common::STRING, common::STRING, common::INT64, common::DOUBLE,
                 common::FLOAT, common::INT32},
                {common::ColumnCategory::TAG, common::ColumnCategory::TAG,
                 common::ColumnCategory::FIELD, common::ColumnCategory::FIELD,
                 common::ColumnCategory::FIELD, common::ColumnCategory::FIELD},
                std::max(n, 1u));
            for (uint32_t i = 0; i < n; i++) {
                int64_t ts = dev_base + off + i;
                HANDLE_ERR(tablet.add_timestamp(i, ts));
                HANDLE_ERR(tablet.add_value(i, "id1", dev_id.c_str()));
                HANDLE_ERR(tablet.add_value(i, "id2", kTag2Val));
                HANDLE_ERR(tablet.add_value(i, "s1", ts));
                HANDLE_ERR(tablet.add_value(i, "s2", ts * 1.1));
                HANDLE_ERR(
                    tablet.add_value(i, "s3", static_cast<float>(ts % 10000)));
                HANDLE_ERR(tablet.add_value(i, "s4",
                                            static_cast<int32_t>(ts % 100000)));
            }
            HANDLE_ERR(writer->write_table(tablet));
            off += n;
        }
    }
    HANDLE_ERR(writer->flush());
    HANDLE_ERR(writer->close());
    delete writer;
    delete schema;
    return 0;
}

// ─── Write Parquet ───────────────────────────────────────────────────────────

static int write_parquet() {
    try {
        auto schema = arrow::schema({
            arrow::field("time", arrow::int64()),
            arrow::field("id1", arrow::utf8()),
            arrow::field("id2", arrow::utf8()),
            arrow::field("s1", arrow::int64()),
            arrow::field("s2", arrow::float64()),
            arrow::field("s3", arrow::float32()),
            arrow::field("s4", arrow::int32()),
        });

        auto writer_props = parquet::WriterProperties::Builder()
                                .compression(parquet::Compression::SNAPPY)
                                ->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().build();
        int64_t rows_per_dev = gRowCount / kNumDevices;
        arrow::MemoryPool* pool = arrow::default_memory_pool();

        PARQUET_ASSIGN_OR_THROW(auto out,
                                arrow::io::FileOutputStream::Open(gPqPath));
        PARQUET_ASSIGN_OR_THROW(
            std::unique_ptr<parquet::arrow::FileWriter> pw,
            parquet::arrow::FileWriter::Open(*schema, pool, out, writer_props,
                                             arrow_props));

        const int64_t batch_cap = 65536;
        for (int dev = 0; dev < kNumDevices; dev++) {
            std::string dev_id = device_name(dev);
            int64_t dev_base = dev * rows_per_dev;

            arrow::Int64Builder time_b;
            arrow::StringBuilder id1_b, id2_b;
            arrow::Int64Builder s1_b;
            arrow::DoubleBuilder s2_b;
            arrow::FloatBuilder s3_b;
            arrow::Int32Builder s4_b;

            for (int64_t off = 0; off < rows_per_dev;) {
                int64_t n = std::min(batch_cap, rows_per_dev - off);
                time_b.Reset();
                id1_b.Reset();
                id2_b.Reset();
                s1_b.Reset();
                s2_b.Reset();
                s3_b.Reset();
                s4_b.Reset();
                for (int64_t i = 0; i < n; i++) {
                    int64_t ts = dev_base + off + i;
                    PARQUET_THROW_NOT_OK(time_b.Append(ts));
                    PARQUET_THROW_NOT_OK(id1_b.Append(dev_id));
                    PARQUET_THROW_NOT_OK(id2_b.Append(kTag2Val));
                    PARQUET_THROW_NOT_OK(s1_b.Append(ts));
                    PARQUET_THROW_NOT_OK(s2_b.Append(ts * 1.1));
                    PARQUET_THROW_NOT_OK(
                        s3_b.Append(static_cast<float>(ts % 10000)));
                    PARQUET_THROW_NOT_OK(
                        s4_b.Append(static_cast<int32_t>(ts % 100000)));
                }
                PARQUET_ASSIGN_OR_THROW(auto a_time, time_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_id1, id1_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_id2, id2_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_s1, s1_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_s2, s2_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_s3, s3_b.Finish());
                PARQUET_ASSIGN_OR_THROW(auto a_s4, s4_b.Finish());
                auto batch = arrow::RecordBatch::Make(
                    schema, n, {a_time, a_id1, a_id2, a_s1, a_s2, a_s3, a_s4});
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

// ─── Parquet helpers ─────────────────────────────────────────────────────────

static std::vector<int> rg_prune_string_eq(const parquet::FileMetaData& meta,
                                           int col_idx,
                                           const std::string& target) {
    std::vector<int> result;
    for (int rg = 0; rg < meta.num_row_groups(); ++rg) {
        auto stats = meta.RowGroup(rg)->ColumnChunk(col_idx)->statistics();
        if (stats && stats->HasMinMax()) {
            auto s =
                std::static_pointer_cast<parquet::ByteArrayStatistics>(stats);
            std::string mn(reinterpret_cast<const char*>(s->min().ptr),
                           s->min().len);
            std::string mx(reinterpret_cast<const char*>(s->max().ptr),
                           s->max().len);
            if (target < mn || target > mx) continue;
        }
        result.push_back(rg);
    }
    return result;
}

static std::vector<int> rg_prune_time_range(const parquet::FileMetaData& meta,
                                            int col_idx, int64_t ts_start,
                                            int64_t ts_end) {
    std::vector<int> result;
    for (int rg = 0; rg < meta.num_row_groups(); ++rg) {
        auto stats = meta.RowGroup(rg)->ColumnChunk(col_idx)->statistics();
        if (stats && stats->HasMinMax()) {
            auto s = std::static_pointer_cast<parquet::Int64Statistics>(stats);
            if (s->max() < ts_start || s->min() >= ts_end) continue;
        }
        result.push_back(rg);
    }
    return result;
}

// ─── TsFile read: batch mode ────────────────────────────────────────────────

static int64_t tsfile_batch_read(const std::string& path, int64_t ts_start,
                                 int64_t ts_end, storage::Filter* tag_filter,
                                 int64_t& out_rows) {
    storage::TsFileReader reader;
    if (reader.open(path) != 0) return -1;

    std::vector<std::string> cols{"id1", "id2", "s1", "s2", "s3", "s4"};
    storage::ResultSet* rs = nullptr;
    int ret;
    if (tag_filter) {
        ret = reader.query(kTable, cols, ts_start, ts_end, rs, tag_filter,
                           kBatchSize);
    } else {
        ret = reader.query(kTable, cols, ts_start, ts_end, rs, kBatchSize);
    }
    if (ret != 0) {
        reader.close();
        return -1;
    }

    const int s1_idx = find_vec_idx(rs, "s1");
    int64_t sum = 0;
    int64_t total_rows = 0;
    common::TsBlock* block = nullptr;
    while (rs->get_next_tsblock(block) == common::E_OK && block) {
        uint32_t nr = block->get_row_count();
        sum += sum_vec_int64(block->get_vector(s1_idx), nr);
        total_rows += nr;
    }
    rs->close();
    reader.close();
    out_rows = total_rows;
    return sum;
}

// ─── TsFile read: row mode ──────────────────────────────────────────────────

static int64_t tsfile_row_read(const std::string& path, int64_t ts_start,
                               int64_t ts_end, storage::Filter* tag_filter,
                               int64_t& out_rows) {
    storage::TsFileReader reader;
    if (reader.open(path) != 0) return -1;

    std::vector<std::string> cols{"id1", "id2", "s1", "s2", "s3", "s4"};
    storage::ResultSet* rs = nullptr;
    int ret;
    if (tag_filter) {
        ret = reader.query(kTable, cols, ts_start, ts_end, rs, tag_filter);
    } else {
        ret = reader.query(kTable, cols, ts_start, ts_end, rs);
    }
    if (ret != 0) {
        reader.close();
        return -1;
    }

    int64_t sum = 0;
    int64_t total_rows = 0;
    bool has_next = false;
    while (rs->next(has_next) == common::E_OK && has_next) {
        if (!rs->is_null("s1")) {
            sum += rs->get_value<int64_t>("s1");
        }
        total_rows++;
    }
    rs->close();
    reader.close();
    out_rows = total_rows;
    return sum;
}

// ─── Parquet read ────────────────────────────────────────────────────────────

static int64_t parquet_read_tag(const std::string& path,
                                const std::string& device, int64_t& out_rows) {
    try {
        std::vector<std::string> cols{"time", "id1", "id2", "s1",
                                      "s2",   "s3",  "s4"};
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        PARQUET_ASSIGN_OR_THROW(auto infile,
                                arrow::io::ReadableFile::Open(path));
        PARQUET_ASSIGN_OR_THROW(
            std::unique_ptr<parquet::arrow::FileReader> reader,
            parquet::arrow::OpenFile(infile, pool));

        std::shared_ptr<arrow::Schema> file_schema;
        PARQUET_THROW_NOT_OK(reader->GetSchema(&file_schema));
        std::vector<int> indices;
        for (auto& name : cols)
            indices.push_back(file_schema->GetFieldIndex(name));

        auto& meta = *reader->parquet_reader()->metadata();
        int id1_col = meta.schema()->ColumnIndex("id1");
        auto matching_rgs = rg_prune_string_eq(meta, id1_col, device);

        PARQUET_ASSIGN_OR_THROW(auto batch_reader, reader->GetRecordBatchReader(
                                                       matching_rgs, indices));

        int64_t sum = 0;
        int64_t total_rows = 0;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch) {
            auto id1_arr = std::static_pointer_cast<arrow::StringArray>(
                batch->GetColumnByName("id1"));
            auto s1_arr = std::static_pointer_cast<arrow::Int64Array>(
                batch->GetColumnByName("s1"));
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if (!id1_arr->IsNull(i) && id1_arr->GetString(i) == device &&
                    !s1_arr->IsNull(i)) {
                    sum += s1_arr->Value(i);
                    total_rows++;
                }
            }
        }
        out_rows = total_rows;
        return sum;
    } catch (const std::exception& e) {
        std::cerr << "parquet tag read: " << e.what() << "\n";
        return -1;
    }
}

static int64_t parquet_read_time(const std::string& path, int64_t ts_start,
                                 int64_t ts_end, int64_t& out_rows) {
    try {
        std::vector<std::string> cols{"time", "id1", "id2", "s1",
                                      "s2",   "s3",  "s4"};
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        PARQUET_ASSIGN_OR_THROW(auto infile,
                                arrow::io::ReadableFile::Open(path));
        PARQUET_ASSIGN_OR_THROW(
            std::unique_ptr<parquet::arrow::FileReader> reader,
            parquet::arrow::OpenFile(infile, pool));

        std::shared_ptr<arrow::Schema> file_schema;
        PARQUET_THROW_NOT_OK(reader->GetSchema(&file_schema));
        std::vector<int> indices;
        for (auto& name : cols)
            indices.push_back(file_schema->GetFieldIndex(name));

        auto& meta = *reader->parquet_reader()->metadata();
        int time_col = meta.schema()->ColumnIndex("time");
        auto matching_rgs =
            rg_prune_time_range(meta, time_col, ts_start, ts_end);

        PARQUET_ASSIGN_OR_THROW(auto batch_reader, reader->GetRecordBatchReader(
                                                       matching_rgs, indices));

        int64_t sum = 0;
        int64_t total_rows = 0;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch) {
            auto time_arr = std::static_pointer_cast<arrow::Int64Array>(
                batch->GetColumnByName("time"));
            auto s1_arr = std::static_pointer_cast<arrow::Int64Array>(
                batch->GetColumnByName("s1"));
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                int64_t t = time_arr->Value(i);
                if (t >= ts_start && t < ts_end && !s1_arr->IsNull(i)) {
                    sum += s1_arr->Value(i);
                    total_rows++;
                }
            }
        }
        out_rows = total_rows;
        return sum;
    } catch (const std::exception& e) {
        std::cerr << "parquet time read: " << e.what() << "\n";
        return -1;
    }
}

static int64_t parquet_read_full(const std::string& path, int64_t& out_rows) {
    try {
        std::vector<std::string> cols{"time", "id1", "id2", "s1",
                                      "s2",   "s3",  "s4"};
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        PARQUET_ASSIGN_OR_THROW(auto infile,
                                arrow::io::ReadableFile::Open(path));
        PARQUET_ASSIGN_OR_THROW(
            std::unique_ptr<parquet::arrow::FileReader> reader,
            parquet::arrow::OpenFile(infile, pool));

        std::shared_ptr<arrow::Schema> file_schema;
        PARQUET_THROW_NOT_OK(reader->GetSchema(&file_schema));
        std::vector<int> indices;
        for (auto& name : cols)
            indices.push_back(file_schema->GetFieldIndex(name));

        int num_rgs = reader->parquet_reader()->metadata()->num_row_groups();
        std::vector<int> all_rgs;
        for (int i = 0; i < num_rgs; ++i) all_rgs.push_back(i);

        PARQUET_ASSIGN_OR_THROW(auto batch_reader,
                                reader->GetRecordBatchReader(all_rgs, indices));

        int64_t sum = 0;
        int64_t total_rows = 0;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch) {
            auto s1_arr = std::static_pointer_cast<arrow::Int64Array>(
                batch->GetColumnByName("s1"));
            for (int64_t i = 0; i < batch->num_rows(); ++i) {
                if (!s1_arr->IsNull(i)) sum += s1_arr->Value(i);
            }
            total_rows += batch->num_rows();
        }
        out_rows = total_rows;
        return sum;
    } catch (const std::exception& e) {
        std::cerr << "parquet full read: " << e.what() << "\n";
        return -1;
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Experiments
// ═════════════════════════════════════════════════════════════════════════════

static void exp_full_scan() {
    std::cout << "\n=== Experiment 1: Full Scan Baseline ===\n";
    std::cout << "  total_rows=" << gRowCount << "\n";

    int64_t rows = 0, sum;
    auto t0 = Clock::now();
    sum = tsfile_batch_read(gTsPath, 0, gRowCount, nullptr, rows);
    double sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record("full_scan", "tsfile_batch", "", sec, rows, sum);

    t0 = Clock::now();
    sum = parquet_read_full(gPqPath, rows);
    sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record("full_scan", "parquet", "", sec, rows, sum);
}

static void exp_tag_filter() {
    std::cout << "\n=== Experiment 2: Tag Filter (Device Pruning) ===\n";
    std::cout << "  filter: id1=\"" << kFilterDevice << "\"\n";

    storage::TsFileReader tmp_reader;
    tmp_reader.open(gTsPath);
    auto table_schema = tmp_reader.get_table_schema(std::string(kTable));
    tmp_reader.close();

    storage::Filter* tag_filter =
        storage::TagFilterBuilder(table_schema.get()).eq("id1", kFilterDevice);

    int64_t rows = 0, sum;
    auto t0 = Clock::now();
    sum = tsfile_batch_read(gTsPath, 0, gRowCount, tag_filter, rows);
    double sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record("tag_filter", "tsfile_batch", "device_0", sec, rows, sum);

    t0 = Clock::now();
    sum = parquet_read_tag(gPqPath, kFilterDevice, rows);
    sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record("tag_filter", "parquet", "device_0", sec, rows, sum);

    delete tag_filter;
}

static void exp_time_selectivity() {
    std::cout << "\n=== Experiment 3: Time Filter Selectivity ===\n";

    double selectivities[] = {0.01, 0.10, 0.50, 1.00};
    for (double sel : selectivities) {
        int64_t ts_end = static_cast<int64_t>(gRowCount * sel);
        if (ts_end == 0) ts_end = 1;
        std::string param = std::to_string(static_cast<int>(sel * 100)) + "%";

        int64_t rows = 0, sum;

        auto t0 = Clock::now();
        sum = tsfile_batch_read(gTsPath, 0, ts_end, nullptr, rows);
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        record("time_sel", "tsfile_batch", param, sec, rows, sum);

        t0 = Clock::now();
        sum = parquet_read_time(gPqPath, 0, ts_end, rows);
        sec = std::chrono::duration<double>(Clock::now() - t0).count();
        record("time_sel", "parquet", param, sec, rows, sum);
    }
}

static void exp_batch_vs_row() {
    std::cout << "\n=== Experiment 4: Batch vs Row-by-Row ===\n";
    std::cout << "  query: tag_filter id1=\"" << kFilterDevice << "\"\n";

    storage::TsFileReader tmp_reader;
    tmp_reader.open(gTsPath);
    auto table_schema = tmp_reader.get_table_schema(std::string(kTable));
    tmp_reader.close();

    storage::Filter* tag_filter =
        storage::TagFilterBuilder(table_schema.get()).eq("id1", kFilterDevice);

    int64_t rows = 0, sum;

    auto t0 = Clock::now();
    sum = tsfile_batch_read(gTsPath, 0, gRowCount, tag_filter, rows);
    double sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record("batch_vs_row", "tsfile_batch", "", sec, rows, sum);

    t0 = Clock::now();
    sum = tsfile_row_read(gTsPath, 0, gRowCount, tag_filter, rows);
    sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record("batch_vs_row", "tsfile_row", "", sec, rows, sum);

    delete tag_filter;
}

static void exp_cache_warmup() {
    std::cout << "\n=== Experiment 5: Cache Warmup (Repeated Reads) ===\n";

    for (int iter = 0; iter < 3; iter++) {
        std::string param = "iter_" + std::to_string(iter);

        int64_t rows = 0, sum;
        auto t0 = Clock::now();
        sum = tsfile_batch_read(gTsPath, 0, gRowCount, nullptr, rows);
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        record("cache_warmup", "tsfile_batch", param, sec, rows, sum);
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Main
// ═════════════════════════════════════════════════════════════════════════════

static void print_build_config() {
    std::cout << "Build configuration:\n";
#ifdef ENABLE_SIMD
    std::cout << "  SIMD:    ON\n";
#else
    std::cout << "  SIMD:    OFF\n";
#endif
#ifdef ENABLE_THREADS
    std::cout << "  THREADS: ON\n";
#else
    std::cout << "  THREADS: OFF\n";
#endif
    std::cout << "  rows:    " << gRowCount << "\n";
    std::cout << "  devices: " << kNumDevices << "\n";
    std::cout << "  columns: time + 2 TAG + 4 FIELD\n";
    std::cout << "  batch:   " << kBatchSize << "\n\n";
}

static void print_usage(const char* prog) {
    std::cerr << "Usage:\n"
              << "  " << prog << " write [row_count]\n"
              << "  " << prog << " read  [row_count] [csv_output]\n"
              << "  " << prog << " all   [row_count] [csv_output]\n";
}

int main(int argc, char* argv[]) {
    std::string mode = "all";
    if (argc >= 2) mode = argv[1];

    // "write" mode: write [row_count]
    // "read"  mode: read  [row_count] [csv_output]
    // "all"   mode: all   [row_count] [csv_output]
    if (mode == "write") {
        if (argc >= 3) gRowCount = std::atoll(argv[2]);
    } else if (mode == "read") {
        if (argc >= 3) gRowCount = std::atoll(argv[2]);
        if (argc >= 4) gCsvOut = argv[3];
    } else if (mode == "all") {
        if (argc >= 3) gRowCount = std::atoll(argv[2]);
        if (argc >= 4) gCsvOut = argv[3];
    } else {
        print_usage(argv[0]);
        return 1;
    }

    print_build_config();
    storage::libtsfile_init();

    // Write phase
    if (mode == "write" || mode == "all") {
        std::cout << "Writing test data...\n";
        auto t0 = Clock::now();
        if (write_tsfile() != 0) return 1;
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        std::cout << "  TsFile write: " << std::fixed << std::setprecision(3)
                  << sec << " s\n";

        t0 = Clock::now();
        if (write_parquet() != 0) return 1;
        sec = std::chrono::duration<double>(Clock::now() - t0).count();
        std::cout << "  Parquet write: " << std::fixed << std::setprecision(3)
                  << sec << " s\n";

        struct stat st;
        if (stat(gTsPath.c_str(), &st) == 0)
            std::cout << "  TsFile size:  " << (st.st_size / 1024 / 1024)
                      << " MB\n";
        if (stat(gPqPath.c_str(), &st) == 0)
            std::cout << "  Parquet size: " << (st.st_size / 1024 / 1024)
                      << " MB\n";
    }

    if (mode == "write") return 0;

    // Read phase (separate from write to avoid global state issues)
    if (mode == "read") {
        struct stat st;
        if (stat(gTsPath.c_str(), &st) != 0) {
            std::cerr << "Error: " << gTsPath
                      << " not found. Run 'write' first.\n";
            return 1;
        }
    }

    exp_full_scan();
    exp_tag_filter();
    exp_time_selectivity();
    exp_batch_vs_row();
    exp_cache_warmup();

    write_csv();
    return 0;
}
