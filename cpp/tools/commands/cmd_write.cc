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
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "cli/cli_args.h"
#include "cli/exit_codes.h"
#include "commands/commands.h"
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

void strip_cr(std::string& s) {
    if (!s.empty() && s.back() == '\r') {
        s.pop_back();
    }
}

bool add_typed_value(storage::Tablet& tablet, uint32_t row,
                     const ColumnDef& def, const std::string& cell,
                     std::string& error) {
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
            tablet.add_value(row, def.name, v);
            return true;
        }
        case common::INT32: {
            long v = std::strtol(cell.c_str(), &e, 10);
            if (e == nullptr || *e != '\0') {
                error = "bad INT32 '" + cell + "'";
                return false;
            }
            tablet.add_value(row, def.name, static_cast<int32_t>(v));
            return true;
        }
        case common::INT64: {
            long long v = std::strtoll(cell.c_str(), &e, 10);
            if (e == nullptr || *e != '\0') {
                error = "bad INT64 '" + cell + "'";
                return false;
            }
            tablet.add_value(row, def.name, static_cast<int64_t>(v));
            return true;
        }
        case common::FLOAT: {
            float v = std::strtof(cell.c_str(), &e);
            if (e == nullptr || *e != '\0') {
                error = "bad FLOAT '" + cell + "'";
                return false;
            }
            tablet.add_value(row, def.name, v);
            return true;
        }
        case common::DOUBLE: {
            double v = std::strtod(cell.c_str(), &e);
            if (e == nullptr || *e != '\0') {
                error = "bad DOUBLE '" + cell + "'";
                return false;
            }
            tablet.add_value(row, def.name, v);
            return true;
        }
        case common::STRING:
        case common::TEXT: {
            tablet.add_value(row, def.name, cell);
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

    std::string line;
    long long line_no = 0;
    if (!args.no_header) {
        if (std::getline(*in, line)) {
            ++line_no;
            strip_cr(line);
            if (args.header_match) {
                std::vector<std::string> h =
                    split_line(line, delim, csv_quotes);
                bool ok = (h.size() == columns.size() + 1);
                for (size_t i = 0; ok && i < columns.size(); ++i) {
                    if (h[i + 1] != columns[i].name) {
                        ok = false;
                    }
                }
                if (!ok) {
                    err << "Error: header does not match --columns (line 1)\n";
                    return kExitRuntime;
                }
            }
        }
    }

    std::vector<DataRow> rows;
    while (std::getline(*in, line)) {
        ++line_no;
        strip_cr(line);
        if (line.empty()) {
            continue;
        }
        std::vector<std::string> fields = split_line(line, delim, csv_quotes);
        if (fields.size() != columns.size() + 1) {
            err << "Error: expected " << (columns.size() + 1) << " fields, got "
                << fields.size() << " (line " << line_no << ")\n";
            return kExitRuntime;
        }
        char* e = nullptr;
        long long ts = std::strtoll(fields[0].c_str(), &e, 10);
        if (e == nullptr || *e != '\0') {
            err << "Error: bad timestamp '" << fields[0] << "' (line "
                << line_no << ")\n";
            return kExitRuntime;
        }
        DataRow r;
        r.line_no = line_no;
        r.timestamp = static_cast<int64_t>(ts);
        r.cells.assign(fields.begin() + 1, fields.end());
        rows.push_back(r);
    }

    std::vector<std::string> names;
    std::vector<common::TSDataType> types;
    std::vector<common::ColumnCategory> cats;
    std::vector<common::ColumnSchema> col_schemas;
    for (const ColumnDef& d : columns) {
        names.push_back(d.name);
        types.push_back(d.type);
        cats.push_back(d.category);
        col_schemas.push_back(common::ColumnSchema(
            d.name, d.type, common::UNCOMPRESSED, common::PLAIN, d.category));
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

    int rc = kExitOk;
    const size_t kBatch = 1024;
    for (size_t start = 0; start < rows.size() && rc == kExitOk;
         start += kBatch) {
        size_t end = std::min(start + kBatch, rows.size());
        storage::Tablet tablet(args.table, names, types, cats,
                               static_cast<int>(end - start));
        for (size_t i = start; i < end && rc == kExitOk; ++i) {
            uint32_t r = static_cast<uint32_t>(i - start);
            tablet.add_timestamp(r, rows[i].timestamp);
            for (size_t j = 0; j < columns.size(); ++j) {
                std::string cerr;
                if (!add_typed_value(tablet, r, columns[j], rows[i].cells[j],
                                     cerr)) {
                    err << "Error: " << cerr << " (line " << rows[i].line_no
                        << ")\n";
                    rc = kExitRuntime;
                    break;
                }
            }
        }
        if (rc == kExitOk && writer->write_table(tablet) != 0) {
            err << "Error: write_table failed\n";
            rc = kExitRuntime;
        }
    }

    if (rc == kExitOk) {
        if (writer->flush() != 0 || writer->close() != 0) {
            err << "Error: flush/close failed\n";
            rc = kExitRuntime;
        }
    } else {
        writer->close();
    }
    delete writer;
    delete schema;

    if (rc == kExitOk && args.verbose) {
        err << "wrote " << rows.size() << " rows to " << args.output << "\n";
    }
    return rc;
}

}  // namespace tsfile_cli
