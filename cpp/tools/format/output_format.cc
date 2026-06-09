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

#include "format/output_format.h"

#include <algorithm>
#include <cstdio>
#include <utility>

#include "common/csv_utils.h"
#include "utils/errno_define.h"

namespace tsfile_cli {

const char* error_code_message(int code) {
    switch (code) {
        case common::E_OOM:
            return "out of memory";
        case common::E_NOT_EXIST:
            return "not found";
        case common::E_INVALID_ARG:
            return "invalid argument";
        case common::E_OUT_OF_RANGE:
            return "value out of range";
        case common::E_OUT_OF_ORDER:
            return "data is out of order";
        case common::E_FILE_OPEN_ERR:
            return "cannot open file";
        case common::E_FILE_WRITE_ERR:
            return "file write error";
        case common::E_FILE_READ_ERR:
            return "file read error";
        case common::E_TSFILE_CORRUPTED:
            return "file is corrupted";
        case common::E_INVALID_PATH:
            return "invalid path";
        case common::E_DEVICE_NOT_EXIST:
            return "device does not exist";
        case common::E_MEASUREMENT_NOT_EXIST:
            return "measurement does not exist";
        case common::E_TABLE_NOT_EXIST:
            return "table does not exist";
        case common::E_COLUMN_NOT_EXIST:
            return "column does not exist";
        case common::E_INVALID_QUERY:
            return "invalid query";
        case common::E_TYPE_NOT_SUPPORTED:
            return "data type not supported";
        case common::E_TYPE_NOT_MATCH:
            return "data type mismatch";
        case common::E_ENCODE_ERR:
            return "failed to encode data";
        case common::E_DECODE_ERR:
            return "failed to decode data";
        default:
            return "internal error";
    }
}

OutputFormat resolve_format(ParsedArgs::Format f, bool stdout_is_tty) {
    switch (f) {
        case ParsedArgs::Format::kCsv:
            return OutputFormat::kCsv;
        case ParsedArgs::Format::kTsv:
            return OutputFormat::kTsv;
        case ParsedArgs::Format::kJson:
            return OutputFormat::kJson;
        case ParsedArgs::Format::kTable:
            return OutputFormat::kTable;
        case ParsedArgs::Format::kAuto:
        default:
            return stdout_is_tty ? OutputFormat::kTable : OutputFormat::kTsv;
    }
}

const char* tsdatatype_name(common::TSDataType t) {
    switch (t) {
        case common::BOOLEAN:
            return "BOOLEAN";
        case common::INT32:
            return "INT32";
        case common::INT64:
            return "INT64";
        case common::FLOAT:
            return "FLOAT";
        case common::DOUBLE:
            return "DOUBLE";
        case common::TEXT:
            return "TEXT";
        case common::VECTOR:
            return "VECTOR";
        case common::UNKNOWN:
            return "UNKNOWN";
        case common::TIMESTAMP:
            return "TIMESTAMP";
        case common::DATE:
            return "DATE";
        case common::BLOB:
            return "BLOB";
        case common::STRING:
            return "STRING";
        case common::NULL_TYPE:
            return "NULL";
        case common::INVALID_DATATYPE:
        default:
            return "INVALID";
    }
}

const char* tsencoding_name(common::TSEncoding e) {
    switch (e) {
        case common::PLAIN:
            return "PLAIN";
        case common::DICTIONARY:
            return "DICTIONARY";
        case common::RLE:
            return "RLE";
        case common::DIFF:
            return "DIFF";
        case common::TS_2DIFF:
            return "TS_2DIFF";
        case common::BITMAP:
            return "BITMAP";
        case common::GORILLA_V1:
            return "GORILLA_V1";
        case common::REGULAR:
            return "REGULAR";
        case common::GORILLA:
            return "GORILLA";
        case common::ZIGZAG:
            return "ZIGZAG";
        case common::FREQ:
            return "FREQ";
        case common::SPRINTZ:
            return "SPRINTZ";
        case common::INVALID_ENCODING:
        default:
            return "UNKNOWN";
    }
}

const char* compression_name(common::CompressionType c) {
    switch (c) {
        case common::UNCOMPRESSED:
            return "UNCOMPRESSED";
        case common::SNAPPY:
            return "SNAPPY";
        case common::GZIP:
            return "GZIP";
        case common::LZO:
            return "LZO";
        case common::SDT:
            return "SDT";
        case common::PAA:
            return "PAA";
        case common::PLA:
            return "PLA";
        case common::LZ4:
            return "LZ4";
        case common::INVALID_COMPRESSION:
        default:
            return "UNKNOWN";
    }
}

std::string csv_escape(const std::string& field) {
    return common::csv_escape(field, ',');
}

std::string json_escape(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 2);
    for (unsigned char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    out += buf;
                } else {
                    out += static_cast<char>(c);
                }
        }
    }
    return out;
}

RowWriter::RowWriter(std::ostream& out, OutputFormat fmt,
                     std::vector<std::string> header,
                     std::vector<common::TSDataType> types, bool no_header)
    : out_(out),
      fmt_(fmt),
      header_(std::move(header)),
      types_(std::move(types)),
      no_header_(no_header) {}

bool RowWriter::emits_json_bare(size_t col) const {
    if (col >= types_.size()) {
        return false;
    }
    // These types serialize to a bare (unquoted) JSON token: numbers as numeric
    // literals, BOOLEAN as the literal true/false, TIMESTAMP as a number.
    // Everything else (strings, blobs, dates) is quoted.
    switch (types_[col]) {
        case common::BOOLEAN:
        case common::INT32:
        case common::INT64:
        case common::FLOAT:
        case common::DOUBLE:
        case common::TIMESTAMP:
            return true;
        default:
            return false;
    }
}

void RowWriter::ensure_header() {
    if (header_done_) {
        return;
    }
    header_done_ = true;
    if (no_header_) {
        return;
    }
    const char sep = (fmt_ == OutputFormat::kCsv) ? ',' : '\t';
    for (size_t i = 0; i < header_.size(); ++i) {
        if (i) {
            out_ << sep;
        }
        out_ << (fmt_ == OutputFormat::kCsv ? csv_escape(header_[i])
                                            : header_[i]);
    }
    out_ << "\n";
}

void RowWriter::write(const std::vector<std::string>& cells,
                      const std::vector<bool>& is_null) {
    if (fmt_ == OutputFormat::kTable) {
        rows_.push_back(cells);
        rows_null_.push_back(is_null);
        return;
    }
    if (fmt_ == OutputFormat::kJson) {
        out_ << "{";
        for (size_t i = 0; i < header_.size(); ++i) {
            if (i) {
                out_ << ",";
            }
            out_ << "\"" << json_escape(header_[i]) << "\":";
            if (i < is_null.size() && is_null[i]) {
                out_ << "null";
            } else if (emits_json_bare(i)) {
                out_ << (i < cells.size() ? cells[i] : "null");
            } else {
                out_ << "\"" << json_escape(i < cells.size() ? cells[i] : "")
                     << "\"";
            }
        }
        out_ << "}\n";
        return;
    }

    ensure_header();
    const char sep = (fmt_ == OutputFormat::kCsv) ? ',' : '\t';
    for (size_t i = 0; i < cells.size(); ++i) {
        if (i) {
            out_ << sep;
        }
        bool null_cell = i < is_null.size() && is_null[i];
        if (null_cell) {
            continue;
        }
        out_ << (fmt_ == OutputFormat::kCsv ? csv_escape(cells[i]) : cells[i]);
    }
    out_ << "\n";
}

void RowWriter::finish() {
    if (fmt_ != OutputFormat::kTable) {
        if (fmt_ == OutputFormat::kCsv || fmt_ == OutputFormat::kTsv) {
            ensure_header();
        }
        return;
    }

    const size_t ncols = header_.size();
    std::vector<size_t> width(ncols, 0);
    if (!no_header_) {
        for (size_t i = 0; i < ncols; ++i) {
            width[i] = header_[i].size();
        }
    }
    for (const auto& row : rows_) {
        for (size_t i = 0; i < ncols && i < row.size(); ++i) {
            width[i] = std::max(width[i], row[i].size());
        }
    }

    auto emit = [&](const std::vector<std::string>& cells,
                    const std::vector<bool>& nulls) {
        for (size_t i = 0; i < ncols; ++i) {
            std::string cell =
                (i < cells.size() && !(i < nulls.size() && nulls[i])) ? cells[i]
                                                                      : "";
            out_ << cell;
            if (i + 1 < ncols) {
                out_ << std::string(width[i] - cell.size() + 2, ' ');
            }
        }
        out_ << "\n";
    };

    if (!no_header_) {
        std::vector<bool> no_nulls(ncols, false);
        emit(header_, no_nulls);
    }
    for (size_t r = 0; r < rows_.size(); ++r) {
        emit(rows_[r], rows_null_[r]);
    }
}

}  // namespace tsfile_cli
