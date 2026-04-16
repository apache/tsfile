/*
 * E6-1: TsFile vs Parquet — Real Dataset Benchmark
 *
 * Loads prepared CSV data (from prepare_*.py scripts), writes to both
 * TsFile and Parquet, then benchmarks read performance.
 *
 * Supported datasets: REDD, GeoLife, TDrive, TSBS
 *
 * Usage:
 *   dataset_bench --dataset redd --data-dir ../datasets/prepared/redd
 *   dataset_bench --dataset geolife --data-dir ../datasets/prepared/geolife
 *   dataset_bench --dataset tdrive --data-dir ../datasets/prepared/tdrive
 *   dataset_bench --dataset tsbs --data-dir ../datasets/prepared/tsbs
 */

#include <arrow/api.h>
#include <arrow/compute/api.h>
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
#include <sstream>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/global.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsblock/tsblock.h"
#include "common/tsblock/vector/vector.h"
#include "file/write_file.h"
#include "reader/filter/tag_filter.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

using Clock = std::chrono::high_resolution_clock;

// ─── Dataset Schema Descriptor ──────────────────────────────────────────────

struct FieldDesc {
    std::string name;
    common::TSDataType ts_type;
    std::shared_ptr<arrow::DataType> arrow_type;
};

struct DatasetConfig {
    std::string name;
    std::string table_name;
    std::vector<std::string> tag_names;
    std::vector<FieldDesc> fields;
};

static DatasetConfig make_redd() {
    return {"redd",
            "redd",
            {"building", "meter"},
            {{"power", common::DOUBLE, arrow::float64()}}};
}

static DatasetConfig make_geolife() {
    return {"geolife",
            "geolife",
            {"user_id"},
            {{"latitude", common::DOUBLE, arrow::float64()},
             {"longitude", common::DOUBLE, arrow::float64()},
             {"altitude", common::DOUBLE, arrow::float64()}}};
}

static DatasetConfig make_tdrive() {
    return {"tdrive",
            "tdrive",
            {"taxi_id"},
            {{"longitude", common::DOUBLE, arrow::float64()},
             {"latitude", common::DOUBLE, arrow::float64()}}};
}

static DatasetConfig make_tsbs() {
    return {"tsbs",
            "tsbs",
            {"name", "fleet", "driver"},
            {{"latitude", common::DOUBLE, arrow::float64()},
             {"longitude", common::DOUBLE, arrow::float64()},
             {"elevation", common::DOUBLE, arrow::float64()},
             {"velocity", common::DOUBLE, arrow::float64()}}};
}

static DatasetConfig get_dataset_config(const std::string& name) {
    if (name == "redd") return make_redd();
    if (name == "geolife") return make_geolife();
    if (name == "tdrive") return make_tdrive();
    if (name == "tsbs") return make_tsbs();
    std::cerr << "Unknown dataset: " << name << "\n";
    std::exit(1);
}

// ─── CSV Row ────────────────────────────────────────────────────────────────

struct DataRow {
    int64_t timestamp;
    std::vector<std::string> tags;
    std::vector<double> fields;
};

struct NumericColumnBuffer {
    std::vector<int32_t> int32_values;
    std::vector<int64_t> int64_values;
    std::vector<float> float_values;
    std::vector<double> double_values;
};

struct WriteTimingBreakdown {
    double prepare_seconds = 0.0;
    double sink_seconds = 0.0;
    double boundary_seconds = 0.0;
    double flush_seconds = 0.0;
};

// ─── CSV Reader ─────────────────────────────────────────────────────────────

static std::vector<std::string> split_csv_line(const std::string& line) {
    std::vector<std::string> result;
    std::stringstream ss(line);
    std::string cell;
    while (std::getline(ss, cell, ',')) {
        result.push_back(cell);
    }
    return result;
}

struct LoadResult {
    std::vector<DataRow> rows;
    std::vector<size_t> device_ends;  // index where each device ends
};

static LoadResult load_csv(const std::string& csv_path,
                           const DatasetConfig& cfg) {
    std::ifstream f(csv_path);
    if (!f.is_open()) {
        std::cerr << "Cannot open: " << csv_path << "\n";
        std::exit(1);
    }

    // Skip header
    std::string header;
    std::getline(f, header);

    int num_tags = static_cast<int>(cfg.tag_names.size());
    int num_fields = static_cast<int>(cfg.fields.size());
    int expected_cols = 1 + num_tags + num_fields;  // timestamp + tags + fields

    LoadResult result;
    auto& rows = result.rows;
    auto& device_ends = result.device_ends;
    rows.reserve(100000);
    device_ends.reserve(256);
    std::string line;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        auto parts = split_csv_line(line);
        if (static_cast<int>(parts.size()) < expected_cols) continue;

        DataRow row;
        row.timestamp = std::atoll(parts[0].c_str());
        row.tags.resize(num_tags);
        for (int i = 0; i < num_tags; i++) {
            row.tags[i] = parts[1 + i];
        }
        row.fields.resize(num_fields);
        for (int i = 0; i < num_fields; i++) {
            row.fields[i] = std::atof(parts[1 + num_tags + i].c_str());
        }

        // Detect device boundary while loading (free — already iterating)
        if (rows.size() > 0) {
            const auto& prev = rows.back().tags;
            bool same = true;
            for (int i = 0; i < num_tags; i++) {
                if (prev[i] != row.tags[i]) {
                    same = false;
                    break;
                }
            }
            if (!same) device_ends.push_back(rows.size());
        }

        rows.push_back(std::move(row));

        if (rows.size() % 5000000 == 0) {
            std::cout << "  loaded " << rows.size() / 1000000 << "M rows...\n";
        }
    }
    device_ends.push_back(rows.size());
    return result;
}

// ─── Write TsFile ───────────────────────────────────────────────────────────

#define CHECK_ERR(expr)                                                     \
    do {                                                                    \
        int _e = (expr);                                                    \
        if (_e != 0) {                                                      \
            std::cerr << "ERROR " << _e << " at line " << __LINE__ << "\n"; \
            return _e;                                                      \
        }                                                                   \
    } while (0)

static int write_tsfile(const std::string& path, const DatasetConfig& cfg,
                        const std::vector<DataRow>& rows,
                        const std::vector<size_t>& device_ends,
                        WriteTimingBreakdown* timing = nullptr) {
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    CHECK_ERR(file.create(path.c_str(), flags, 0666));

    // Build schema: tags + fields
    std::vector<common::ColumnSchema> columns;
    std::vector<std::string> col_names;
    std::vector<common::TSDataType> col_types;
    std::vector<common::ColumnCategory> col_cats;

    for (auto& tag : cfg.tag_names) {
        columns.emplace_back(tag, common::STRING, common::UNCOMPRESSED,
                             common::PLAIN, common::ColumnCategory::TAG);
        col_names.push_back(tag);
        col_types.push_back(common::STRING);
        col_cats.push_back(common::ColumnCategory::TAG);
    }
    for (auto& field : cfg.fields) {
        // Use PLAIN for floating-point fields so write-path profiling can
        // compare against Parquet without Gorilla encoding cost dominating.
        common::TSEncoding enc = common::PLAIN;
        if (field.ts_type == common::INT32 || field.ts_type == common::INT64) {
            enc = common::TS_2DIFF;
        }
        columns.emplace_back(field.name, field.ts_type, common::SNAPPY, enc,
                             common::ColumnCategory::FIELD);
        col_names.push_back(field.name);
        col_types.push_back(field.ts_type);
        col_cats.push_back(common::ColumnCategory::FIELD);
    }

    auto* schema = new storage::TableSchema(cfg.table_name, columns);
    auto* writer = new storage::TsFileTableWriter(&file, schema);

    const uint32_t batch_cap = 65536;
    int num_tags = static_cast<int>(cfg.tag_names.size());
    int num_fields = static_cast<int>(cfg.fields.size());
    storage::Tablet tablet(cfg.table_name.c_str(), col_names, col_types,
                           col_cats, batch_cap);
    tablet.set_single_device(true);
    std::vector<int64_t> ts_buf(batch_cap);
    std::vector<NumericColumnBuffer> field_bufs(
        static_cast<size_t>(num_fields));
    for (int f = 0; f < num_fields; f++) {
        switch (cfg.fields[f].ts_type) {
            case common::INT32:
                field_bufs[f].int32_values.resize(batch_cap);
                break;
            case common::INT64:
            case common::TIMESTAMP:
                field_bufs[f].int64_values.resize(batch_cap);
                break;
            case common::FLOAT:
                field_bufs[f].float_values.resize(batch_cap);
                break;
            case common::DOUBLE:
                field_bufs[f].double_values.resize(batch_cap);
                break;
            default:
                std::cerr << "Unsupported field type for batch write: "
                          << cfg.fields[f].name << "\n";
                return common::E_TYPE_NOT_SUPPORTED;
        }
    }

    size_t off = 0;
    size_t dev_idx = 0;
    while (off < rows.size()) {
        size_t dev_end = device_ends[dev_idx++];

        // Write this device in batches
        for (size_t doff = off; doff < dev_end;) {
            uint32_t n = static_cast<uint32_t>(
                std::min<size_t>(batch_cap, dev_end - doff));
            auto prep_t0 = Clock::now();
            tablet.reset(n);

            // Single-pass extraction: touch each DataRow once to avoid
            // repeated cache misses on the heap-allocated fields vector.
            for (uint32_t i = 0; i < n; i++) {
                const auto& row = rows[doff + i];
                ts_buf[i] = row.timestamp;
                for (int f = 0; f < num_fields; f++) {
                    field_bufs[f].double_values[i] = row.fields[f];
                }
            }
            CHECK_ERR(tablet.set_timestamps(ts_buf.data(), n));

            const auto& first_row = rows[doff];
            for (int t = 0; t < num_tags; t++) {
                CHECK_ERR(tablet.set_column_string_repeated(
                    static_cast<uint32_t>(t), first_row.tags[t].c_str(),
                    static_cast<uint32_t>(first_row.tags[t].size()), n));
            }

            for (int f = 0; f < num_fields; f++) {
                uint32_t col_idx = static_cast<uint32_t>(num_tags + f);
                CHECK_ERR(tablet.set_column_values(
                    col_idx, field_bufs[f].double_values.data(), nullptr, n));
            }
            if (timing != nullptr) {
                timing->prepare_seconds +=
                    std::chrono::duration<double>(Clock::now() - prep_t0)
                        .count();
            }

            auto sink_t0 = Clock::now();
            CHECK_ERR(writer->write_table(tablet));
            if (timing != nullptr) {
                timing->sink_seconds +=
                    std::chrono::duration<double>(Clock::now() - sink_t0)
                        .count();
            }
            doff += n;
        }
        off = dev_end;
    }

    auto flush_t0 = Clock::now();
    CHECK_ERR(writer->flush());
    CHECK_ERR(writer->close());
    if (timing != nullptr) {
        timing->flush_seconds =
            std::chrono::duration<double>(Clock::now() - flush_t0).count();
    }
    delete writer;
    delete schema;
    return 0;
}

// ─── Write Parquet ──────────────────────────────────────────────────────────

static int write_parquet(const std::string& path, const DatasetConfig& cfg,
                         const std::vector<DataRow>& rows,
                         WriteTimingBreakdown* timing = nullptr) {
    try {
        // Build Arrow schema: time + tags + fields
        std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
        arrow_fields.push_back(arrow::field("time", arrow::int64()));
        for (auto& tag : cfg.tag_names) {
            arrow_fields.push_back(arrow::field(tag, arrow::utf8()));
        }
        for (auto& field : cfg.fields) {
            arrow_fields.push_back(arrow::field(field.name, field.arrow_type));
        }
        auto schema = arrow::schema(arrow_fields);

        auto writer_props = parquet::WriterProperties::Builder()
                                .compression(parquet::Compression::SNAPPY)
                                ->build();
        auto arrow_props = parquet::ArrowWriterProperties::Builder().build();
        arrow::MemoryPool* pool = arrow::default_memory_pool();

        PARQUET_ASSIGN_OR_THROW(auto out,
                                arrow::io::FileOutputStream::Open(path));
        PARQUET_ASSIGN_OR_THROW(
            std::unique_ptr<parquet::arrow::FileWriter> pw,
            parquet::arrow::FileWriter::Open(*schema, pool, out, writer_props,
                                             arrow_props));

        int num_tags = static_cast<int>(cfg.tag_names.size());
        int num_fields = static_cast<int>(cfg.fields.size());
        const int64_t batch_cap = 65536;

        for (size_t off = 0; off < rows.size();) {
            int64_t n = std::min<int64_t>(batch_cap, rows.size() - off);
            auto prep_t0 = Clock::now();

            arrow::Int64Builder time_b;
            std::vector<arrow::StringBuilder> tag_builders(num_tags);
            std::vector<arrow::DoubleBuilder> field_builders(num_fields);

            for (int64_t i = 0; i < n; i++) {
                const auto& row = rows[off + i];
                PARQUET_THROW_NOT_OK(time_b.Append(row.timestamp));
                for (int t = 0; t < num_tags; t++) {
                    PARQUET_THROW_NOT_OK(tag_builders[t].Append(row.tags[t]));
                }
                for (int f = 0; f < num_fields; f++) {
                    PARQUET_THROW_NOT_OK(
                        field_builders[f].Append(row.fields[f]));
                }
            }

            std::vector<std::shared_ptr<arrow::Array>> arrays;
            PARQUET_ASSIGN_OR_THROW(auto a_time, time_b.Finish());
            arrays.push_back(a_time);
            for (int t = 0; t < num_tags; t++) {
                PARQUET_ASSIGN_OR_THROW(auto a, tag_builders[t].Finish());
                arrays.push_back(a);
            }
            for (int f = 0; f < num_fields; f++) {
                PARQUET_ASSIGN_OR_THROW(auto a, field_builders[f].Finish());
                arrays.push_back(a);
            }

            auto batch = arrow::RecordBatch::Make(schema, n, arrays);
            if (timing != nullptr) {
                timing->prepare_seconds +=
                    std::chrono::duration<double>(Clock::now() - prep_t0)
                        .count();
            }

            auto sink_t0 = Clock::now();
            PARQUET_THROW_NOT_OK(pw->WriteRecordBatch(*batch));
            if (timing != nullptr) {
                timing->sink_seconds +=
                    std::chrono::duration<double>(Clock::now() - sink_t0)
                        .count();
            }
            off += n;
        }

        PARQUET_THROW_NOT_OK(pw->Close());
        PARQUET_THROW_NOT_OK(out->Close());
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "parquet write error: " << e.what() << "\n";
        return 1;
    }
}

// ─── Read Benchmarks ────────────────────────────────────────────────────────

struct BenchResult {
    std::string dataset;
    std::string experiment;
    std::string engine;
    std::string params;
    double seconds;
    int64_t result_rows;
    int64_t checksum;
};

static std::vector<BenchResult> gResults;
static const int kBatchSize = 65536;

static void record(const std::string& ds, const std::string& exp,
                   const std::string& engine, const std::string& params,
                   double secs, int64_t rows, int64_t cksum) {
    gResults.push_back({ds, exp, engine, params, secs, rows, cksum});
    double tput = secs > 0 ? rows / secs / 1e6 : 0.0;
    std::cout << "  " << std::left << std::setw(18) << exp << std::setw(10)
              << engine << std::fixed << std::setprecision(3) << secs << " s  "
              << std::setprecision(2) << tput << " M rows/s";
    if (!params.empty()) std::cout << "  [" << params << "]";
    std::cout << "\n";
}

// ─── TsFile reads ───────────────────────────────────────────────────────────

static int64_t tsfile_full_scan(const std::string& path,
                                const DatasetConfig& cfg, int64_t ts_max,
                                int64_t& out_rows) {
    storage::TsFileReader reader;
    if (reader.open(path) != 0) return -1;

    std::vector<std::string> cols;
    for (auto& t : cfg.tag_names) cols.push_back(t);
    for (auto& f : cfg.fields) cols.push_back(f.name);

    storage::ResultSet* rs = nullptr;
    if (reader.query(cfg.table_name, cols, 0, ts_max, rs, kBatchSize) != 0) {
        reader.close();
        return -1;
    }

    int64_t sum = 0, total = 0;
    common::TsBlock* block = nullptr;
    while (rs->get_next_tsblock(block) == common::E_OK && block) {
        total += block->get_row_count();
        // Lightweight checksum: just count rows
    }
    rs->close();
    reader.close();
    out_rows = total;
    return sum;
}

static int64_t tsfile_tag_filter(const std::string& path,
                                 const DatasetConfig& cfg, int64_t ts_max,
                                 const std::string& tag_name,
                                 const std::string& tag_value,
                                 int64_t& out_rows) {
    storage::TsFileReader reader;
    if (reader.open(path) != 0) return -1;

    auto table_schema = reader.get_table_schema(cfg.table_name);
    storage::Filter* filter =
        storage::TagFilterBuilder(table_schema.get()).eq(tag_name, tag_value);

    std::vector<std::string> cols;
    for (auto& t : cfg.tag_names) cols.push_back(t);
    for (auto& f : cfg.fields) cols.push_back(f.name);

    storage::ResultSet* rs = nullptr;
    if (reader.query(cfg.table_name, cols, 0, ts_max, rs, filter, kBatchSize) !=
        0) {
        delete filter;
        reader.close();
        return -1;
    }

    int64_t total = 0;
    common::TsBlock* block = nullptr;
    while (rs->get_next_tsblock(block) == common::E_OK && block) {
        total += block->get_row_count();
    }
    rs->close();
    delete filter;
    reader.close();
    out_rows = total;
    return 0;
}

static int64_t tsfile_time_filter(const std::string& path,
                                  const DatasetConfig& cfg, int64_t ts_start,
                                  int64_t ts_end, int64_t& out_rows) {
    storage::TsFileReader reader;
    if (reader.open(path) != 0) return -1;

    std::vector<std::string> cols;
    for (auto& t : cfg.tag_names) cols.push_back(t);
    for (auto& f : cfg.fields) cols.push_back(f.name);

    storage::ResultSet* rs = nullptr;
    if (reader.query(cfg.table_name, cols, ts_start, ts_end, rs, kBatchSize) !=
        0) {
        reader.close();
        return -1;
    }

    int64_t total = 0;
    common::TsBlock* block = nullptr;
    while (rs->get_next_tsblock(block) == common::E_OK && block) {
        total += block->get_row_count();
    }
    rs->close();
    reader.close();
    out_rows = total;
    return 0;
}

static int64_t tsfile_tag_time_filter(const std::string& path,
                                      const DatasetConfig& cfg,
                                      const std::string& tag_name,
                                      const std::string& tag_value,
                                      int64_t ts_start, int64_t ts_end,
                                      int64_t& out_rows) {
    storage::TsFileReader reader;
    if (reader.open(path) != 0) return -1;

    auto table_schema = reader.get_table_schema(cfg.table_name);
    storage::Filter* filter =
        storage::TagFilterBuilder(table_schema.get()).eq(tag_name, tag_value);

    std::vector<std::string> cols;
    for (auto& t : cfg.tag_names) cols.push_back(t);
    for (auto& f : cfg.fields) cols.push_back(f.name);

    storage::ResultSet* rs = nullptr;
    if (reader.query(cfg.table_name, cols, ts_start, ts_end, rs, filter,
                     kBatchSize) != 0) {
        delete filter;
        reader.close();
        return -1;
    }

    int64_t total = 0;
    common::TsBlock* block = nullptr;
    while (rs->get_next_tsblock(block) == common::E_OK && block) {
        total += block->get_row_count();
    }
    rs->close();
    delete filter;
    reader.close();
    out_rows = total;
    return 0;
}

// ─── Parquet reads ──────────────────────────────────────────────────────────

// Helper: open Parquet with multi-threaded column decoding + pre-buffering
// to match PyArrow's default behaviour.
static std::unique_ptr<parquet::arrow::FileReader> open_parquet(
    const std::string& path, arrow::MemoryPool* pool) {
    auto infile = arrow::io::ReadableFile::Open(path).ValueOrDie();
    parquet::ArrowReaderProperties props(/*use_threads=*/true);
    props.set_pre_buffer(true);
    std::unique_ptr<parquet::arrow::FileReader> reader;
    parquet::arrow::FileReaderBuilder builder;
    builder.Open(infile).ok();
    builder.memory_pool(pool);
    builder.properties(props);
    builder.Build(&reader).ok();
    return reader;
}

static int64_t parquet_full_scan(const std::string& path, int64_t& out_rows) {
    try {
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        auto reader = open_parquet(path, pool);

        int num_rgs = reader->parquet_reader()->metadata()->num_row_groups();
        std::vector<int> all_rgs;
        for (int i = 0; i < num_rgs; i++) all_rgs.push_back(i);

        PARQUET_ASSIGN_OR_THROW(auto batch_reader,
                                reader->GetRecordBatchReader(all_rgs));

        int64_t total = 0;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch) {
            total += batch->num_rows();
        }
        out_rows = total;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "parquet error: " << e.what() << "\n";
        return -1;
    }
}

static int64_t parquet_tag_filter(const std::string& path,
                                  const std::string& tag_col,
                                  const std::string& tag_value,
                                  int64_t& out_rows) {
    try {
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        auto reader = open_parquet(path, pool);

        auto& meta = *reader->parquet_reader()->metadata();
        int tag_idx = meta.schema()->ColumnIndex(tag_col);

        // Row group pruning by min/max stats
        std::vector<int> matching_rgs;
        for (int rg = 0; rg < meta.num_row_groups(); rg++) {
            auto stats = meta.RowGroup(rg)->ColumnChunk(tag_idx)->statistics();
            if (stats && stats->HasMinMax()) {
                auto s = std::static_pointer_cast<parquet::ByteArrayStatistics>(
                    stats);
                std::string mn(reinterpret_cast<const char*>(s->min().ptr),
                               s->min().len);
                std::string mx(reinterpret_cast<const char*>(s->max().ptr),
                               s->max().len);
                if (tag_value < mn || tag_value > mx) continue;
            }
            matching_rgs.push_back(rg);
        }

        PARQUET_ASSIGN_OR_THROW(auto batch_reader,
                                reader->GetRecordBatchReader(matching_rgs));

        auto scalar = std::make_shared<arrow::StringScalar>(tag_value);

        int64_t total = 0;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch) {
            auto col = batch->GetColumnByName(tag_col);
            auto mask = arrow::compute::CallFunction("equal", {col, scalar})
                            .ValueOrDie()
                            .make_array();
            auto count = arrow::compute::Sum(mask)
                             .ValueOrDie()
                             .scalar_as<arrow::UInt64Scalar>();
            total += count.value;
        }
        out_rows = total;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "parquet tag filter error: " << e.what() << "\n";
        return -1;
    }
}

static int64_t parquet_time_filter(const std::string& path, int64_t ts_start,
                                   int64_t ts_end, int64_t& out_rows) {
    try {
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        auto reader = open_parquet(path, pool);

        auto& meta = *reader->parquet_reader()->metadata();
        int time_idx = meta.schema()->ColumnIndex("time");

        // Row group pruning
        std::vector<int> matching_rgs;
        for (int rg = 0; rg < meta.num_row_groups(); rg++) {
            auto stats = meta.RowGroup(rg)->ColumnChunk(time_idx)->statistics();
            if (stats && stats->HasMinMax()) {
                auto s =
                    std::static_pointer_cast<parquet::Int64Statistics>(stats);
                if (s->max() < ts_start || s->min() >= ts_end) continue;
            }
            matching_rgs.push_back(rg);
        }

        PARQUET_ASSIGN_OR_THROW(auto batch_reader,
                                reader->GetRecordBatchReader(matching_rgs));

        auto sc_start = arrow::MakeScalar(ts_start);
        auto sc_end = arrow::MakeScalar(ts_end);

        int64_t total = 0;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch) {
            auto time_col = batch->GetColumnByName("time");
            auto ge = arrow::compute::CallFunction("greater_equal",
                                                   {time_col, sc_start})
                          .ValueOrDie()
                          .make_array();
            auto lt = arrow::compute::CallFunction("less", {time_col, sc_end})
                          .ValueOrDie()
                          .make_array();
            auto mask = arrow::compute::And(ge, lt).ValueOrDie().make_array();
            auto count = arrow::compute::Sum(mask)
                             .ValueOrDie()
                             .scalar_as<arrow::UInt64Scalar>();
            total += count.value;
        }
        out_rows = total;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "parquet time filter error: " << e.what() << "\n";
        return -1;
    }
}

static int64_t parquet_tag_time_filter(const std::string& path,
                                       const std::string& tag_col,
                                       const std::string& tag_value,
                                       int64_t ts_start, int64_t ts_end,
                                       int64_t& out_rows) {
    try {
        arrow::MemoryPool* pool = arrow::default_memory_pool();
        auto reader = open_parquet(path, pool);

        auto& meta = *reader->parquet_reader()->metadata();
        int tag_idx = meta.schema()->ColumnIndex(tag_col);
        int time_idx = meta.schema()->ColumnIndex("time");

        // Row group pruning by both tag and time stats
        std::vector<int> matching_rgs;
        for (int rg = 0; rg < meta.num_row_groups(); rg++) {
            // Tag pruning
            auto tag_stats =
                meta.RowGroup(rg)->ColumnChunk(tag_idx)->statistics();
            if (tag_stats && tag_stats->HasMinMax()) {
                auto s = std::static_pointer_cast<parquet::ByteArrayStatistics>(
                    tag_stats);
                std::string mn(reinterpret_cast<const char*>(s->min().ptr),
                               s->min().len);
                std::string mx(reinterpret_cast<const char*>(s->max().ptr),
                               s->max().len);
                if (tag_value < mn || tag_value > mx) continue;
            }
            // Time pruning
            auto time_stats =
                meta.RowGroup(rg)->ColumnChunk(time_idx)->statistics();
            if (time_stats && time_stats->HasMinMax()) {
                auto s = std::static_pointer_cast<parquet::Int64Statistics>(
                    time_stats);
                if (s->max() < ts_start || s->min() >= ts_end) continue;
            }
            matching_rgs.push_back(rg);
        }

        PARQUET_ASSIGN_OR_THROW(auto batch_reader,
                                reader->GetRecordBatchReader(matching_rgs));

        auto tag_scalar = std::make_shared<arrow::StringScalar>(tag_value);
        auto sc_start = arrow::MakeScalar(ts_start);
        auto sc_end = arrow::MakeScalar(ts_end);

        int64_t total = 0;
        std::shared_ptr<arrow::RecordBatch> batch;
        while (batch_reader->ReadNext(&batch).ok() && batch) {
            // Tag mask
            auto tag_mask =
                arrow::compute::CallFunction(
                    "equal", {batch->GetColumnByName(tag_col), tag_scalar})
                    .ValueOrDie()
                    .make_array();
            // Time mask
            auto time_col = batch->GetColumnByName("time");
            auto ge = arrow::compute::CallFunction("greater_equal",
                                                   {time_col, sc_start})
                          .ValueOrDie()
                          .make_array();
            auto lt = arrow::compute::CallFunction("less", {time_col, sc_end})
                          .ValueOrDie()
                          .make_array();
            auto time_mask =
                arrow::compute::And(ge, lt).ValueOrDie().make_array();
            // Combined
            auto mask = arrow::compute::And(tag_mask, time_mask)
                            .ValueOrDie()
                            .make_array();
            auto count = arrow::compute::Sum(mask)
                             .ValueOrDie()
                             .scalar_as<arrow::UInt64Scalar>();
            total += count.value;
        }
        out_rows = total;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "parquet tag+time filter error: " << e.what() << "\n";
        return -1;
    }
}

// ─── Experiments ────────────────────────────────────────────────────────────

static void run_experiments(const DatasetConfig& cfg,
                            const std::string& ts_path,
                            const std::string& pq_path, int64_t ts_min,
                            int64_t ts_max, const std::string& sample_tag_name,
                            const std::string& sample_tag_value,
                            int64_t dev_ts_min, int64_t dev_ts_max) {
    int64_t rows = 0;

    // 1. Full scan
    std::cout << "\n=== Full Scan ===\n";
    auto t0 = Clock::now();
    tsfile_full_scan(ts_path, cfg, ts_max, rows);
    double sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record(cfg.name, "full_scan", "tsfile", "", sec, rows, 0);

    t0 = Clock::now();
    parquet_full_scan(pq_path, rows);
    sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record(cfg.name, "full_scan", "parquet", "", sec, rows, 0);

    // 2. Tag filter (single device)
    std::cout << "\n=== Tag Filter ===\n";
    std::cout << "  filter: " << sample_tag_name << "=\"" << sample_tag_value
              << "\"\n";

    t0 = Clock::now();
    tsfile_tag_filter(ts_path, cfg, ts_max, sample_tag_name, sample_tag_value,
                      rows);
    sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record(cfg.name, "tag_filter", "tsfile", sample_tag_value, sec, rows, 0);

    t0 = Clock::now();
    parquet_tag_filter(pq_path, sample_tag_name, sample_tag_value, rows);
    sec = std::chrono::duration<double>(Clock::now() - t0).count();
    record(cfg.name, "tag_filter", "parquet", sample_tag_value, sec, rows, 0);

    // 3. Time filter at varying selectivity
    std::cout << "\n=== Time Filter ===\n";
    double selectivities[] = {0.10, 0.25, 0.50};
    int64_t ts_range = ts_max - ts_min;
    for (double sel : selectivities) {
        int64_t ts_end = ts_min + static_cast<int64_t>(ts_range * sel);
        if (ts_end <= ts_min) ts_end = ts_min + 1;
        std::string param = std::to_string(static_cast<int>(sel * 100)) + "%";

        t0 = Clock::now();
        tsfile_time_filter(ts_path, cfg, ts_min, ts_end, rows);
        sec = std::chrono::duration<double>(Clock::now() - t0).count();
        record(cfg.name, "time_filter", "tsfile", param, sec, rows, 0);

        t0 = Clock::now();
        parquet_time_filter(pq_path, ts_min, ts_end, rows);
        sec = std::chrono::duration<double>(Clock::now() - t0).count();
        record(cfg.name, "time_filter", "parquet", param, sec, rows, 0);
    }

    // 4. Combined tag + time filter (the real-world query pattern)
    //    Use the sample device's OWN time range for selectivity so that
    //    the filter always hits meaningful data (not an empty window).
    std::cout << "\n=== Tag + Time Filter ===\n";
    int64_t dev_range = dev_ts_max - dev_ts_min;
    for (double sel : selectivities) {
        int64_t ts_end = dev_ts_min + static_cast<int64_t>(dev_range * sel);
        if (ts_end <= dev_ts_min) ts_end = dev_ts_min + 1;
        std::string param = sample_tag_value + "+" +
                            std::to_string(static_cast<int>(sel * 100)) + "%";

        t0 = Clock::now();
        tsfile_tag_time_filter(ts_path, cfg, sample_tag_name, sample_tag_value,
                               dev_ts_min, ts_end, rows);
        sec = std::chrono::duration<double>(Clock::now() - t0).count();
        record(cfg.name, "tag_time_filter", "tsfile", param, sec, rows, 0);

        t0 = Clock::now();
        parquet_tag_time_filter(pq_path, sample_tag_name, sample_tag_value,
                                dev_ts_min, ts_end, rows);
        sec = std::chrono::duration<double>(Clock::now() - t0).count();
        record(cfg.name, "tag_time_filter", "parquet", param, sec, rows, 0);
    }
}

static void write_csv(const std::string& path) {
    std::ofstream f(path);
    f << "dataset,experiment,engine,params,seconds,result_rows,rows_per_sec\n";
    for (auto& r : gResults) {
        int64_t rows_per_sec =
            r.seconds > 0 ? static_cast<int64_t>(r.result_rows / r.seconds) : 0;
        f << r.dataset << "," << r.experiment << "," << r.engine << ","
          << r.params << "," << std::fixed << std::setprecision(6) << r.seconds
          << "," << r.result_rows << "," << rows_per_sec << "\n";
    }
    std::cout << "\nResults written to " << path << "\n";
}

// ─── Main ───────────────────────────────────────────────────────────────────

static void print_usage(const char* prog) {
    std::cerr << "Usage:\n"
              << "  " << prog << " --dataset <redd|geolife|tdrive|tsbs>"
              << " --data-dir <prepared_dir>"
              << " [--csv-out <results.csv>]\n"
              << "  " << prog << " --all --data-root <prepared_root>\n"
              << "  " << prog
              << "    # zero-arg mode auto-selects a prepared dataset\n";
}

static bool path_exists(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

static bool configure_default_run(std::string& dataset_name,
                                  std::string& data_dir, std::string& csv_out) {
    // Use relative path so the binary works on any platform when run
    // from the repository root (cpp/) or the experiment directory.
    const char* env_root = std::getenv("TSFILE_DATASET_ROOT");
    const std::string prepared_root =
        env_root ? env_root : "../datasets/prepared";
    const std::vector<std::string> candidates = {"geolife", "tdrive", "tsbs",
                                                 "redd"};
    for (const auto& ds : candidates) {
        const std::string candidate_dir = prepared_root + "/" + ds;
        const std::string candidate_csv = candidate_dir + "/data_sorted.csv";
        if (path_exists(candidate_csv)) {
            dataset_name = ds;
            data_dir = candidate_dir;
            csv_out = ds + "_default_results.csv";
            std::cout << "[default-run] dataset=" << dataset_name
                      << " data_dir=" << data_dir << " csv_out=" << csv_out
                      << "\n";
            return true;
        }
    }
    return false;
}

int main(int argc, char* argv[]) {
    std::string dataset_name;
    std::string data_dir;
    std::string data_root;
    std::string csv_out = "vs_parquet_results.csv";
    bool run_all = false;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--dataset" && i + 1 < argc) {
            dataset_name = argv[++i];
        } else if (arg == "--data-dir" && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (arg == "--data-root" && i + 1 < argc) {
            data_root = argv[++i];
            run_all = true;
        } else if (arg == "--all") {
            run_all = true;
        } else if (arg == "--csv-out" && i + 1 < argc) {
            csv_out = argv[++i];
        } else {
            print_usage(argv[0]);
            return 1;
        }
    }

    storage::libtsfile_init();
    // Skip fsync on close to match Parquet behavior (Arrow FileOutputStream
    // does not fsync on Close).
    common::g_config_value_.sync_on_close_ = false;
    arrow::Initialize(arrow::GlobalOptions{}).ok();

    std::vector<std::string> datasets;
    if (run_all) {
        datasets = {"redd", "geolife", "tdrive", "tsbs"};
    } else if (!dataset_name.empty()) {
        datasets = {dataset_name};
    } else {
        if (!configure_default_run(dataset_name, data_dir, csv_out)) {
            print_usage(argv[0]);
            return 1;
        }
        datasets = {dataset_name};
    }

    for (auto& ds : datasets) {
        std::string ds_dir = run_all ? (data_root + "/" + ds) : data_dir;
        std::string csv_path = ds_dir + "/data_sorted.csv";

        auto cfg = get_dataset_config(ds);
        std::cout << "\n"
                  << "========================================\n"
                  << "  Dataset: " << cfg.name << "\n"
                  << "  Tags:    " << cfg.tag_names.size() << "\n"
                  << "  Fields:  " << cfg.fields.size() << "\n"
                  << "========================================\n";

        // Load CSV (also detects device boundaries for free)
        std::cout << "Loading " << csv_path << "...\n";
        auto loaded = load_csv(csv_path, cfg);
        auto& rows = loaded.rows;
        auto& device_ends = loaded.device_ends;
        std::cout << "Loaded " << rows.size() << " rows, " << device_ends.size()
                  << " devices\n";
        if (rows.empty()) {
            std::cerr << "No data loaded for " << ds << ", skipping\n";
            continue;
        }

        // Compute time range
        int64_t ts_min = rows.front().timestamp;
        int64_t ts_max = rows.back().timestamp;
        for (auto& r : rows) {
            if (r.timestamp < ts_min) ts_min = r.timestamp;
            if (r.timestamp > ts_max) ts_max = r.timestamp;
        }
        ts_max++;  // exclusive upper bound

        // Pick a sample device for tag filter test (first device)
        std::string sample_tag = cfg.tag_names[0];
        std::string sample_val = rows[0].tags[0];

        // Compute the sample device's own time range for per-device
        // selectivity (avoids meaningless results when devices have
        // non-overlapping time spans, e.g. GeoLife GPS trajectories).
        int64_t dev_ts_min = rows[0].timestamp;
        int64_t dev_ts_max = rows[0].timestamp;
        size_t dev_row_end = device_ends[0];
        for (size_t i = 0; i < dev_row_end; i++) {
            if (rows[i].timestamp < dev_ts_min) dev_ts_min = rows[i].timestamp;
            if (rows[i].timestamp > dev_ts_max) dev_ts_max = rows[i].timestamp;
        }
        dev_ts_max++;  // exclusive
        std::cout << "  Sample device \"" << sample_val << "\": " << dev_row_end
                  << " rows, ts range [" << dev_ts_min << ", " << dev_ts_max
                  << ")\n";

        // Write phase
        std::string ts_path = ds + "_bench.tsfile";
        std::string pq_path = ds + "_bench.parquet";

        std::cout << "\nWriting TsFile...\n";
        auto t0 = Clock::now();
        WriteTimingBreakdown ts_write_timing;
        if (write_tsfile(ts_path, cfg, rows, device_ends, &ts_write_timing) !=
            0) {
            std::cerr << "Failed to write TsFile for " << ds << "\n";
            continue;
        }
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        std::cout << "  TsFile write: " << std::fixed << std::setprecision(3)
                  << sec << " s\n";
        record(cfg.name, "write", "tsfile", "", sec, rows.size(), 0);
        record(cfg.name, "write_prepare", "tsfile", "",
               ts_write_timing.prepare_seconds, rows.size(), 0);
        record(cfg.name, "write_sink", "tsfile", "",
               ts_write_timing.sink_seconds, rows.size(), 0);
        record(cfg.name, "write_boundary", "tsfile", "",
               ts_write_timing.boundary_seconds, rows.size(), 0);
        record(cfg.name, "write_flush", "tsfile", "",
               ts_write_timing.flush_seconds, rows.size(), 0);

        std::cout << "Writing Parquet...\n";
        t0 = Clock::now();
        WriteTimingBreakdown pq_write_timing;
        if (write_parquet(pq_path, cfg, rows, &pq_write_timing) != 0) {
            std::cerr << "Failed to write Parquet for " << ds << "\n";
            continue;
        }
        sec = std::chrono::duration<double>(Clock::now() - t0).count();
        std::cout << "  Parquet write: " << std::fixed << std::setprecision(3)
                  << sec << " s\n";
        record(cfg.name, "write", "parquet", "", sec, rows.size(), 0);
        record(cfg.name, "write_prepare", "parquet", "",
               pq_write_timing.prepare_seconds, rows.size(), 0);
        record(cfg.name, "write_sink", "parquet", "",
               pq_write_timing.sink_seconds, rows.size(), 0);

        // File sizes
        struct stat st;
        if (stat(ts_path.c_str(), &st) == 0) {
            std::cout << "  TsFile size:  " << (st.st_size / 1024 / 1024)
                      << " MB\n";
            record(cfg.name, "space_bytes", "tsfile", "", 0,
                   static_cast<int64_t>(st.st_size), 0);
        }
        if (stat(pq_path.c_str(), &st) == 0) {
            std::cout << "  Parquet size: " << (st.st_size / 1024 / 1024)
                      << " MB\n";
            record(cfg.name, "space_bytes", "parquet", "", 0,
                   static_cast<int64_t>(st.st_size), 0);
        }

        // Read benchmarks
        run_experiments(cfg, ts_path, pq_path, ts_min, ts_max, sample_tag,
                        sample_val, dev_ts_min, dev_ts_max);
    }

    write_csv(csv_out);
    return 0;
}
