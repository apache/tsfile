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

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <iostream>
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
#include "format/input_format.h"
#include "writer/tsfile_table_writer.h"

namespace tsfile_cli {
namespace {

struct DataRow {
    long long line_no;
    int64_t timestamp;
    std::vector<std::string> cells;
};

// Parse a calendar date in strict YYYY-MM-DD form into a std::tm (year offset
// from 1900, month 0-based) the way storage::Tablet expects for DATE columns.
// Validates that it is a real date (DateConverter rejects e.g. 2024-13-40),
// since the writer silently drops an invalid std::tm rather than erroring.
bool parse_date_cell(const std::string& cell, std::tm& out) {
    int y = 0;
    int m = 0;
    int d = 0;
    char extra = 0;
    if (std::sscanf(cell.c_str(), "%4d-%2d-%2d%c", &y, &m, &d, &extra) != 3) {
        return false;
    }
    out = std::tm();
    out.tm_year = y - 1900;
    out.tm_mon = m - 1;
    out.tm_mday = d;
    int32_t date_int = 0;
    return common::DateConverter::date_to_int(out, date_int) == common::E_OK;
}

bool add_typed_value(storage::Tablet& tablet, uint32_t row,
                     uint32_t schema_index, const ColumnDef& def,
                     const std::string& cell, std::string& error) {
    if (cell.empty()) {
        return true;  // null
    }
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
    if (!parse_columns_spec(args.columns, columns, perr)) {
        err << "Error: " << perr << "\n";
        return kExitUsage;
    }

    std::istream* in = &std::cin;
    std::ifstream fin;
    if (!args.file.empty() && args.file != "-") {
        fin.open(args.file.c_str());
        if (!fin.is_open()) {
            err << "Error: cannot open input: " << args.file << "\n";
            return kExitFile;
        }
        in = &fin;
    }

    const char delim = (args.format == ParsedArgs::Format::kTsv) ? '\t' : ',';
    const bool csv_quotes = (delim == ',');

    if (args.verbose) {
        // Echo the resolved write configuration so the user can self-check the
        // parsed flags before any rows are imported.
        err << "write: table=" << args.table << " output=" << args.output
            << " input=" << (args.file.empty() ? "-" : args.file)
            << " format=" << (delim == '\t' ? "tsv" : "csv")
            << " header=" << (args.no_header ? "none" : "yes")
            << (args.header_match ? " (match)" : "") << "\n";
        for (const ColumnDef& d : columns) {
            err << "  column " << d.name << ":" << tsdatatype_name(d.type) << ":"
                << (d.category == common::ColumnCategory::TAG ? "tag" : "field")
                << "\n";
        }
    }

    std::string line;
    long long line_no = 0;
    long long record_lines = 0;
    if (!args.no_header) {
        if (read_record(*in, csv_quotes, line, record_lines)) {
            line_no += record_lines;
            if (args.header_match) {
                std::vector<std::string> h =
                    split_line(line, delim, csv_quotes);
                const size_t expected = columns.size() + 1;
                if (h.size() != expected) {
                    err << "Error: header has " << h.size()
                        << " columns, expected " << expected
                        << " (time + --columns) (line 1)\n";
                    return kExitRuntime;
                }
                for (size_t i = 0; i < columns.size(); ++i) {
                    if (h[i + 1] != columns[i].name) {
                        err << "Error: header column " << (i + 2) << " is '"
                            << h[i + 1] << "', expected '" << columns[i].name
                            << "' (line 1)\n";
                        return kExitRuntime;
                    }
                }
            }
        }
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
        col_schemas.push_back(common::ColumnSchema(
            d.name, d.type, common::get_default_compressor(),
            common::get_value_encoder(d.type), d.category));
        if (d.category == common::ColumnCategory::TAG) {
            tag_idx.push_back(j);
        }
    }

    // Creating the output truncates it; refuse to clobber the input we are
    // still reading from, which would otherwise silently destroy the source
    // data.
    if (!args.file.empty() && args.file != "-" && args.output == args.file) {
        err << "Error: --output is the same as the input file: " << args.output
            << "\n";
        return kExitUsage;
    }

    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    if (file.create(args.output, flags, 0666) != 0) {
        err << "Error: cannot create output: " << args.output << "\n";
        return kExitFile;
    }
    auto* schema = new storage::TableSchema(args.table, col_schemas);
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

    auto flush_batch = [&]() -> bool {
        if (batch.empty()) {
            return true;
        }
        storage::Tablet tablet(args.table, names, types, cats,
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
                    return false;
                }
            }
        }
        int wt = writer->write_table(tablet);
        if (wt != 0) {
            err << "Error: failed to write rows: " << error_code_message(wt)
                << " (code " << wt << ")\n";
            return false;
        }
        total_rows += static_cast<long long>(batch.size());
        batch.clear();
        return true;
    };

    while (read_record(*in, csv_quotes, line, record_lines)) {
        line_no += record_lines;
        if (line.empty()) {
            continue;
        }
        std::vector<std::string> fields = split_line(line, delim, csv_quotes);
        if (fields.size() != columns.size() + 1) {
            err << "Error: expected " << (columns.size() + 1) << " fields, got "
                << fields.size() << " (line " << line_no << ")\n";
            result_code = kExitRuntime;
            break;
        }
        char* e = nullptr;
        errno = 0;
        long long ts = std::strtoll(fields[0].c_str(), &e, 10);
        if (e == nullptr || *e != '\0' || errno == ERANGE) {
            err << "Error: bad timestamp '" << fields[0] << "' (line "
                << line_no << ")\n";
            result_code = kExitRuntime;
            break;
        }
        DataRow r;
        r.line_no = line_no;
        r.timestamp = static_cast<int64_t>(ts);
        r.cells.assign(fields.begin() + 1, fields.end());

        std::string device_key;
        for (size_t k : tag_idx) {
            device_key += r.cells[k];
            device_key.push_back('\0');
        }
        auto seen = last_ts_by_device.find(device_key);
        if (seen != last_ts_by_device.end() && r.timestamp <= seen->second) {
            err << "Error: timestamps must be strictly increasing per device "
                   "(line "
                << line_no << ": " << r.timestamp << " <= previous "
                << seen->second << ")\n";
            result_code = kExitRuntime;
            break;
        }
        last_ts_by_device[device_key] = r.timestamp;

        batch.push_back(std::move(r));
        if (batch.size() >= kBatch && !flush_batch()) {
            result_code = kExitRuntime;
            break;
        }
    }

    if (result_code == kExitOk && !flush_batch()) {
        result_code = kExitRuntime;
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

    if (result_code != kExitOk) {
        // The import failed; do not leave a partial/corrupt .tsfile behind.
        file.close();
        std::remove(args.output.c_str());
    } else if (args.verbose) {
        err << "wrote " << total_rows << " rows to " << args.output << "\n";
    }
    return result_code;
}

}  // namespace tsfile_cli
