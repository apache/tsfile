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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <fcntl.h>
#include <sys/stat.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "cli/cli_args.h"
#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/datatype/date_converter.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "format/atomic_output.h"
#include "format/input_format.h"
#include "format/output_format.h"
#include "writer/tsfile_table_writer.h"

#ifdef _WIN32
#define lstat stat
#endif

namespace tsfile_cli {
namespace {

struct CsvCell {
    std::string value;
    bool quoted;
};

struct DataRow {
    long long line_no;
    int64_t timestamp;
    std::vector<CsvCell> cells;
};

struct WritePhysicalConfig {
    std::map<common::TSDataType, common::TSEncoding> encodings;
    std::map<common::TSDataType, common::CompressionType> compressions;
};

bool has_strict_decimal_body(const std::string& s, size_t start) {
    if (start >= s.size()) {
        return false;
    }
    if (s[start] == '0' && start + 1 != s.size()) {
        return false;
    }
    for (size_t i = start; i < s.size(); ++i) {
        if (s[i] < '0' || s[i] > '9') {
            return false;
        }
    }
    return true;
}

bool parse_strict_timestamp_cell(const std::string& s, int64_t& out) {
    if (s.empty()) {
        return false;
    }
    size_t start = (s[0] == '-') ? 1 : 0;
    if (s[0] == '-' && start + 1 == s.size() && s[start] == '0') {
        return false;
    }
    if (!has_strict_decimal_body(s, start)) {
        return false;
    }
    char* e = nullptr;
    errno = 0;
    long long ts = std::strtoll(s.c_str(), &e, 10);
    if (e == nullptr || *e != '\0' || errno == ERANGE) {
        return false;
    }
    out = static_cast<int64_t>(ts);
    return true;
}

bool stat_is_regular_file(const struct stat& st) {
#ifdef _WIN32
    return (st.st_mode & S_IFREG) != 0;
#else
    return S_ISREG(st.st_mode);
#endif
}

bool stat_regular_file(const std::string& path, struct stat& st) {
    return stat(path.c_str(), &st) == 0 && stat_is_regular_file(st);
}

// Parse a calendar date in strict YYYY-MM-DD form into a std::tm (year offset
// from 1900, month 0-based) the way storage::Tablet expects for DATE columns.
// Validates that it is a real date (DateConverter rejects e.g. 2024-13-40),
// since the writer silently drops an invalid std::tm rather than erroring.
bool parse_date_cell(const std::string& cell, std::tm& out) {
    if (cell.size() != 10 || cell[4] != '-' || cell[7] != '-') {
        return false;
    }
    const size_t digits[] = {0, 1, 2, 3, 5, 6, 8, 9};
    for (size_t i : digits) {
        if (cell[i] < '0' || cell[i] > '9') {
            return false;
        }
    }
    int y = 0;
    int m = 0;
    int d = 0;
    if (std::sscanf(cell.c_str(), "%4d-%2d-%2d", &y, &m, &d) != 3) {
        return false;
    }
    out = std::tm();
    out.tm_year = y - 1900;
    out.tm_mon = m - 1;
    out.tm_mday = d;
    int32_t date_int = 0;
    return common::DateConverter::date_to_int(out, date_int) == common::E_OK;
}

std::string lower_ascii(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        out += static_cast<char>(c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c);
    }
    return out;
}

bool parse_encoding_name(const std::string& name, common::TSEncoding& out) {
    static const std::pair<const char*, common::TSEncoding> kEncodings[] = {
        {"PLAIN", common::PLAIN},
        {"DICTIONARY", common::DICTIONARY},
        {"RLE", common::RLE},
        {"DIFF", common::DIFF},
        {"TS_2DIFF", common::TS_2DIFF},
        {"BITMAP", common::BITMAP},
        {"GORILLA_V1", common::GORILLA_V1},
        {"REGULAR", common::REGULAR},
        {"GORILLA", common::GORILLA},
        {"ZIGZAG", common::ZIGZAG},
        {"FREQ", common::FREQ},
        {"CHIMP", common::CHIMP},
        {"SPRINTZ", common::SPRINTZ},
        {"RLBE", common::RLBE},
        {"CAMEL", common::CAMEL},
    };
    for (const auto& e : kEncodings) {
        if (name == e.first) {
            out = e.second;
            return true;
        }
    }
    return false;
}

bool parse_compression_name(const std::string& name,
                            common::CompressionType& out) {
    static const std::pair<const char*, common::CompressionType>
        kCompressions[] = {{"UNCOMPRESSED", common::UNCOMPRESSED},
                           {"SNAPPY", common::SNAPPY},
                           {"GZIP", common::GZIP},
                           {"LZO", common::LZO},
                           {"SDT", common::SDT},
                           {"PAA", common::PAA},
                           {"PLA", common::PLA},
                           {"LZ4", common::LZ4},
                           {"ZSTD", common::ZSTD},
                           {"LZMA2", common::LZMA2}};
    for (const auto& c : kCompressions) {
        if (name == c.first) {
            out = c.second;
            return true;
        }
    }
    return false;
}

bool encoding_supported(common::TSDataType type, common::TSEncoding encoding) {
    switch (type) {
        case common::BOOLEAN:
            return encoding == common::PLAIN;
        case common::INT32:
        case common::DATE:
        case common::INT64:
        case common::TIMESTAMP:
            return encoding == common::PLAIN || encoding == common::TS_2DIFF ||
                   encoding == common::GORILLA || encoding == common::ZIGZAG ||
                   encoding == common::RLE || encoding == common::SPRINTZ ||
                   encoding == common::CHIMP || encoding == common::RLBE;
        case common::FLOAT:
            return encoding == common::PLAIN || encoding == common::TS_2DIFF ||
                   encoding == common::GORILLA || encoding == common::SPRINTZ ||
                   encoding == common::CHIMP || encoding == common::RLBE;
        case common::DOUBLE:
            return encoding == common::PLAIN || encoding == common::TS_2DIFF ||
                   encoding == common::GORILLA || encoding == common::SPRINTZ ||
                   encoding == common::CHIMP || encoding == common::RLBE ||
                   encoding == common::CAMEL;
        case common::STRING:
        case common::TEXT:
        case common::BLOB:
            return encoding == common::PLAIN || encoding == common::DICTIONARY;
        default:
            return false;
    }
}

bool compression_supported(common::CompressionType compression) {
    return compression == common::UNCOMPRESSED ||
           compression == common::SNAPPY || compression == common::GZIP ||
           compression == common::LZO || compression == common::LZ4 ||
           compression == common::ZSTD || compression == common::LZMA2;
}

bool build_physical_config(const ParsedArgs& args,
                           const std::vector<ColumnDef>& columns,
                           WritePhysicalConfig& config, std::ostream& err) {
    std::set<common::TSDataType> used_types;
    for (const ColumnDef& c : columns) {
        used_types.insert(c.type);
    }
    for (const ParsedArgs::PhysicalOverride& o : args.physical_overrides) {
        common::TSDataType type = common::INVALID_DATATYPE;
        if (!parse_datatype_name(o.data_type, type) ||
            o.data_type != tsdatatype_name(type)) {
            err << "Error: physical override type '" << o.data_type
                << "' must be a used canonical data type\n";
            return false;
        }
        if (used_types.find(type) == used_types.end()) {
            err << "Error: physical override type " << o.data_type
                << " is not used by any declared TAG or FIELD\n";
            return false;
        }
        if (o.kind == ParsedArgs::PhysicalOverride::Kind::kEncoding) {
            if (config.encodings.find(type) != config.encodings.end()) {
                err << "Error: encoding for data type " << o.data_type
                    << " specified more than once\n";
                return false;
            }
            common::TSEncoding encoding = common::INVALID_ENCODING;
            if (!parse_encoding_name(o.value, encoding) ||
                !encoding_supported(type, encoding)) {
                err << "Error: encoding " << o.value
                    << " is not supported for data type " << o.data_type
                    << "\n";
                return false;
            }
            config.encodings[type] = encoding;
        } else {
            if (config.compressions.find(type) != config.compressions.end()) {
                err << "Error: compression for data type " << o.data_type
                    << " specified more than once\n";
                return false;
            }
            common::CompressionType compression = common::INVALID_COMPRESSION;
            if (!parse_compression_name(o.value, compression) ||
                !compression_supported(compression)) {
                err << "Error: compression " << o.value
                    << " is not supported\n";
                return false;
            }
            config.compressions[type] = compression;
        }
    }
    return true;
}

bool build_header_mapping(const std::string& header,
                          const std::vector<ColumnDef>& columns,
                          std::vector<size_t>& field_indexes,
                          size_t& time_index, std::ostream& err) {
    std::vector<std::string> fields = split_line(header, ',', true);
    std::set<std::string> seen_lower;
    std::map<std::string, size_t> header_indexes;
    bool found_time = false;
    for (size_t i = 0; i < fields.size(); ++i) {
        std::string folded = lower_ascii(fields[i]);
        if (seen_lower.find(folded) != seen_lower.end()) {
            err << "Error: CSV header name '" << fields[i]
                << "' conflicts case-insensitively\n";
            return false;
        }
        seen_lower.insert(folded);
        if (folded == "time") {
            found_time = true;
            time_index = i;
        } else {
            std::string name_error;
            if (!validate_identifier(folded, name_error)) {
                err << "Error: invalid CSV header name '" << fields[i]
                    << "': " << name_error << "\n";
                return false;
            }
            header_indexes[folded] = i;
        }
    }
    if (!found_time) {
        err << "Error: CSV header is missing required column 'time'\n";
        return false;
    }

    std::set<std::string> declared;
    field_indexes.clear();
    field_indexes.reserve(columns.size());
    for (const ColumnDef& c : columns) {
        const std::string folded = lower_ascii(c.name);
        declared.insert(folded);
        auto it = header_indexes.find(folded);
        if (it == header_indexes.end()) {
            err << "Error: CSV header is missing required column '" << c.name
                << "'\n";
            return false;
        }
        field_indexes.push_back(it->second);
    }
    for (const auto& kv : header_indexes) {
        if (declared.find(kv.first) == declared.end()) {
            err << "Error: CSV contains undeclared column '" << kv.first
                << "'\n";
            return false;
        }
    }
    return true;
}

std::vector<CsvCell> split_csv_cells(const std::string& line, char delim,
                                     bool& closed_quotes) {
    std::vector<CsvCell> out;
    CsvCell field;
    field.quoted = false;
    bool in_quotes = false;
    bool at_field_start = true;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    field.value += '"';
                    ++i;
                } else {
                    in_quotes = false;
                }
            } else {
                field.value += c;
            }
        } else if (c == '"') {
            if (at_field_start) {
                field.quoted = true;
            }
            in_quotes = true;
            at_field_start = false;
        } else if (c == delim) {
            out.push_back(field);
            field = CsvCell();
            at_field_start = true;
        } else {
            field.value += c;
            at_field_start = false;
        }
    }
    out.push_back(field);
    closed_quotes = !in_quotes;
    return out;
}

bool is_csv_null(const CsvCell& cell) {
    return !cell.quoted && cell.value == "\\N";
}

bool add_typed_value(storage::Tablet& tablet, uint32_t row,
                     uint32_t schema_index, const ColumnDef& def,
                     const CsvCell& csv_cell, std::string& error) {
    if (is_csv_null(csv_cell)) {
        return true;  // null
    }
    const std::string& cell = csv_cell.value;
    char* e = nullptr;
    switch (def.type) {
        case common::BOOLEAN: {
            bool v = false;
            if (!parse_bool_cell(cell, v)) {
                error = "bad BOOLEAN '" + cell + "'";
                return false;
            }
            tablet.add_value(row, schema_index, v);
            return true;
        }
        case common::INT32: {
            errno = 0;
            long long v = std::strtoll(cell.c_str(), &e, 10);
            if (e == nullptr || *e != '\0') {
                error = "bad INT32 '" + cell + "'";
                return false;
            }
            if (errno == ERANGE || v < INT32_MIN || v > INT32_MAX) {
                error = "INT32 out of range '" + cell + "'";
                return false;
            }
            tablet.add_value(row, schema_index, static_cast<int32_t>(v));
            return true;
        }
        case common::INT64: {
            errno = 0;
            long long v = std::strtoll(cell.c_str(), &e, 10);
            if (e == nullptr || *e != '\0') {
                error = "bad INT64 '" + cell + "'";
                return false;
            }
            if (errno == ERANGE) {
                error = "INT64 out of range '" + cell + "'";
                return false;
            }
            tablet.add_value(row, schema_index, static_cast<int64_t>(v));
            return true;
        }
        case common::TIMESTAMP: {
            errno = 0;
            long long v = std::strtoll(cell.c_str(), &e, 10);
            if (e == nullptr || *e != '\0') {
                error = "bad TIMESTAMP '" + cell + "'";
                return false;
            }
            if (errno == ERANGE) {
                error = "TIMESTAMP out of range '" + cell + "'";
                return false;
            }
            tablet.add_value(row, schema_index, static_cast<int64_t>(v));
            return true;
        }
        case common::DATE: {
            std::tm d;
            if (!parse_date_cell(cell, d)) {
                error = "bad DATE '" + cell + "' (want YYYY-MM-DD)";
                return false;
            }
            tablet.add_value(row, schema_index, d);
            return true;
        }
        case common::FLOAT: {
            errno = 0;
            float v = std::strtof(cell.c_str(), &e);
            if (e == nullptr || *e != '\0') {
                error = "bad FLOAT '" + cell + "'";
                return false;
            }
            if (errno == ERANGE) {
                error = "FLOAT out of range '" + cell + "'";
                return false;
            }
            tablet.add_value(row, schema_index, v);
            return true;
        }
        case common::DOUBLE: {
            errno = 0;
            double v = std::strtod(cell.c_str(), &e);
            if (e == nullptr || *e != '\0') {
                error = "bad DOUBLE '" + cell + "'";
                return false;
            }
            if (errno == ERANGE) {
                error = "DOUBLE out of range '" + cell + "'";
                return false;
            }
            tablet.add_value(row, schema_index, v);
            return true;
        }
        case common::STRING:
        case common::TEXT:
        case common::BLOB: {
            // Add by index using the c-string overload to avoid the per-cell
            // name lowercasing + map lookup the by-name overload would do.
            tablet.add_value(row, schema_index, cell.c_str());
            return true;
        }
        default:
            error = "unsupported column type";
            return false;
    }
}

}  // namespace

int cmd_write(const ParsedArgs& args, std::ostream& /*out*/,
              std::ostream& err) {
    std::vector<ColumnDef> columns;
    std::string perr;
    if (!normalize_write_columns(args.columns, columns, perr)) {
        err << "Error: " << perr << "\n";
        return kExitUsage;
    }
    const std::string table_name = lower_ascii(args.table);
    if (!validate_identifier(table_name, perr)) {
        err << "Error: invalid table name '" << args.table << "': " << perr
            << "\n";
        return kExitUsage;
    }
    bool has_field = false;
    for (const ColumnDef& column : columns) {
        has_field =
            has_field || column.category == common::ColumnCategory::FIELD;
    }
    if (!has_field) {
        err << "Error: write requires at least one --field column\n";
        return kExitUsage;
    }
    WritePhysicalConfig physical_config;
    if (!build_physical_config(args, columns, physical_config, err)) {
        return kExitUsage;
    }

    std::istream* in = &std::cin;
    std::ifstream fin;
    struct stat input_stat;
    bool has_input_stat = false;
    if (!args.file.empty() && args.file != "-") {
        if (!stat_regular_file(args.file, input_stat)) {
            err << "Error: input must be a regular CSV file: " << args.file
                << "\n";
            return kExitFile;
        }
        has_input_stat = true;
        fin.open(args.file.c_str());
        if (!fin.is_open()) {
            err << "Error: cannot open input: " << args.file << "\n";
            return kExitFile;
        }
        in = &fin;
    }

    const char delim = ',';
    const bool csv_quotes = true;

    std::string line;
    long long line_no = 0;
    long long record_lines = 0;
    if (!read_record(*in, csv_quotes, line, record_lines)) {
        err << "Error: CSV header is missing required column 'time'\n";
        return kExitFile;
    }
    line_no += record_lines;
    const std::string bom = "\xEF\xBB\xBF";
    if (line.compare(0, bom.size(), bom) == 0) {
        line.erase(0, bom.size());
    }
    if (!is_valid_utf8(line) || contains_utf8_bom(line)) {
        err << "Error: CSV header contains invalid UTF-8 or a misplaced BOM\n";
        return kExitFile;
    }
    bool closed_quotes = true;
    split_csv_cells(line, delim, closed_quotes);
    if (!closed_quotes) {
        err << "Error: unterminated quoted CSV field in header\n";
        return kExitFile;
    }
    std::vector<size_t> field_indexes;
    size_t time_index = 0;
    if (!build_header_mapping(line, columns, field_indexes, time_index, err)) {
        return kExitFile;
    }

    std::vector<std::string> names;
    std::vector<common::TSDataType> types;
    std::vector<common::ColumnCategory> cats;
    std::vector<common::ColumnSchema> col_schemas;
    std::vector<size_t> tag_idx;
    for (size_t j = 0; j < columns.size(); ++j) {
        const ColumnDef& d = columns[j];
        names.push_back(d.name);
        types.push_back(d.type);
        cats.push_back(d.category);
        common::CompressionType compression = common::get_default_compressor();
        auto comp_it = physical_config.compressions.find(d.type);
        if (comp_it != physical_config.compressions.end()) {
            compression = comp_it->second;
        }
        common::TSEncoding encoding = common::get_value_encoder(d.type);
        auto enc_it = physical_config.encodings.find(d.type);
        if (enc_it != physical_config.encodings.end()) {
            encoding = enc_it->second;
        }
        col_schemas.push_back(common::ColumnSchema(d.name, d.type, compression,
                                                   encoding, d.category));
        if (d.category == common::ColumnCategory::TAG) {
            tag_idx.push_back(j);
        }
    }

    std::string temp_output;
    int prepare_ret = prepare_atomic_output(
        args.output, has_input_stat ? args.file : std::string(), false,
        temp_output, err);
    if (prepare_ret != kExitOk) {
        return prepare_ret;
    }

    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    int cret = file.create(temp_output, flags, 0600);
    if (cret != 0) {
        err << "Error: cannot create output " << args.output << ": "
            << error_code_message(cret) << " (code " << cret << ")\n";
        remove_atomic_temp(temp_output, err);
        return kExitRuntime;
    }
    auto* schema = new storage::TableSchema(table_name, col_schemas);
    auto* writer = new storage::TsFileTableWriter(&file, schema);

    // Stream rows into fixed-size batches so memory stays bounded regardless of
    // input size; a full file is never buffered in memory.
    const size_t kBatch = 1024;
    int result_code = kExitOk;
    long long total_rows = 0;
    std::vector<DataRow> batch;
    batch.reserve(kBatch);
    // The table writer requires strictly increasing timestamps per device, and
    // a device is identified by its tag-column values. Track the last timestamp
    // seen for each device so out-of-order input is rejected with a clear,
    // located message instead of an opaque write failure.
    std::unordered_map<std::string, int64_t> last_ts_by_device;

    auto flush_batch = [&]() -> int {
        if (batch.empty()) {
            return kExitOk;
        }
        storage::Tablet tablet(table_name, names, types, cats,
                               static_cast<int>(batch.size()));
        for (size_t i = 0; i < batch.size(); ++i) {
            uint32_t r = static_cast<uint32_t>(i);
            tablet.add_timestamp(r, batch[i].timestamp);
            for (size_t j = 0; j < columns.size(); ++j) {
                std::string cell_err;
                if (!add_typed_value(tablet, r, static_cast<uint32_t>(j),
                                     columns[j], batch[i].cells[j], cell_err)) {
                    err << "Error: " << cell_err << " (line "
                        << batch[i].line_no << ")\n";
                    return kExitFile;
                }
            }
        }
        int wt = writer->write_table(tablet);
        if (wt != 0) {
            err << "Error: failed to write rows: " << error_code_message(wt)
                << " (code " << wt << ")\n";
            return kExitRuntime;
        }
        total_rows += static_cast<long long>(batch.size());
        batch.clear();
        return kExitOk;
    };

    while (read_record(*in, csv_quotes, line, record_lines)) {
        line_no += record_lines;
        if (line.empty()) {
            continue;
        }
        if (!is_valid_utf8(line) || contains_utf8_bom(line)) {
            err << "Error: invalid UTF-8 or misplaced BOM (line " << line_no
                << ")\n";
            result_code = kExitFile;
            break;
        }
        std::vector<CsvCell> fields =
            split_csv_cells(line, delim, closed_quotes);
        if (!closed_quotes) {
            err << "Error: unterminated quoted CSV field (line " << line_no
                << ")\n";
            result_code = kExitFile;
            break;
        }
        if (fields.size() != field_indexes.size() + 1) {
            err << "Error: expected " << (field_indexes.size() + 1)
                << " fields, got " << fields.size() << " (line " << line_no
                << ")\n";
            result_code = kExitFile;
            break;
        }
        int64_t ts = 0;
        if (!parse_strict_timestamp_cell(fields[time_index].value, ts)) {
            err << "Error: bad timestamp '" << fields[time_index].value
                << "' (line " << line_no << ")\n";
            result_code = kExitFile;
            break;
        }
        DataRow r;
        r.line_no = line_no;
        r.timestamp = ts;
        r.cells.resize(columns.size());
        for (size_t j = 0; j < columns.size(); ++j) {
            r.cells[j] = fields[field_indexes[j]];
        }

        std::string device_key;
        for (size_t k : tag_idx) {
            if (is_csv_null(r.cells[k])) {
                device_key += '\1';
            } else {
                device_key += '\2';
                device_key += r.cells[k].value;
            }
            device_key.push_back('\0');
        }
        auto seen = last_ts_by_device.find(device_key);
        if (seen != last_ts_by_device.end() && r.timestamp <= seen->second) {
            err << "Error: timestamps must be strictly increasing per device "
                   "(line "
                << line_no << ": " << r.timestamp << " <= previous "
                << seen->second << ")\n";
            result_code = kExitFile;
            break;
        }
        last_ts_by_device[device_key] = r.timestamp;

        batch.push_back(std::move(r));
        if (batch.size() >= kBatch) {
            result_code = flush_batch();
        }
        if (result_code != kExitOk) {
            break;
        }
    }

    if (result_code == kExitOk) {
        result_code = flush_batch();
    }

    if (result_code == kExitOk) {
        int fr = writer->flush();
        if (fr != 0) {
            err << "Error: failed to flush output: " << error_code_message(fr)
                << " (code " << fr << ")\n";
            result_code = kExitRuntime;
        } else {
            int cr = writer->close();
            if (cr != 0) {
                err << "Error: failed to close output: "
                    << error_code_message(cr) << " (code " << cr << ")\n";
                result_code = kExitRuntime;
            }
        }
    } else {
        writer->close();
    }
    delete writer;
    delete schema;

    int file_close_ret = file.close();
    if (result_code == kExitOk && file_close_ret != common::E_OK) {
        err << "Error: failed to close output: "
            << error_code_message(file_close_ret) << " (code " << file_close_ret
            << ")\n";
        result_code = kExitRuntime;
    }
    if (result_code == kExitOk) {
        result_code =
            commit_atomic_output(temp_output, args.output, false, err);
    }
    if (result_code != kExitOk) {
        if (!remove_atomic_temp(temp_output, err)) {
            result_code = kExitRuntime;
        }
    } else if (args.verbose) {
        err << "created model=table object=" << table_name
            << " rows=" << total_rows << " output=" << args.output << "\n";
        for (const ColumnDef& d : columns) {
            auto enc_it = physical_config.encodings.find(d.type);
            common::TSEncoding encoding =
                enc_it == physical_config.encodings.end()
                    ? common::get_value_encoder(d.type)
                    : enc_it->second;
            auto comp_it = physical_config.compressions.find(d.type);
            common::CompressionType compression =
                comp_it == physical_config.compressions.end()
                    ? common::get_default_compressor()
                    : comp_it->second;
            err << "column=" << d.name << " category="
                << (d.category == common::ColumnCategory::TAG ? "TAG" : "FIELD")
                << " data_type=" << tsdatatype_name(d.type)
                << " encoding=" << tsencoding_name(encoding) << " source="
                << (enc_it == physical_config.encodings.end() ? "default"
                                                              : "type-override")
                << " compression=" << compression_name(compression)
                << " source="
                << (comp_it == physical_config.compressions.end()
                        ? "default"
                        : "type-override")
                << "\n";
        }
    }
    return result_code;
}

}  // namespace tsfile_cli
