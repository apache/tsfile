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
        case common::E_TYPE_NOT_SUPPORTED:
            return "data type not supported";
        case common::E_TYPE_NOT_MATCH:
            return "data type mismatch";
        case common::E_ENCODE_ERR:
            return "failed to encode data";
        case common::E_DECODE_ERR:
            return "failed to decode data";
        case common::E_FILE_MAP_ERR:
            return "failed to memory-map file";
        case common::E_UNSUPPORTED_VERSION:
            return "unsupported TsFile format version";
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
            (void)stdout_is_tty;
            return OutputFormat::kTable;
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
        case common::CHIMP:
            return "CHIMP";
        case common::SPRINTZ:
            return "SPRINTZ";
        case common::RLBE:
            return "RLBE";
        case common::CAMEL:
            return "CAMEL";
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
        case common::ZSTD:
            return "ZSTD";
        case common::LZMA2:
            return "LZMA2";
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

namespace {

// FLOAT/DOUBLE cells render non-finite values as nan/inf tokens, which have
// no JSON representation; finite numbers never contain these letters.
bool json_nonfinite(common::TSDataType t, const std::string& cell) {
    if (t != common::FLOAT && t != common::DOUBLE) {
        return false;
    }
    return cell.find_first_of("nNiI") != std::string::npos;
}

std::string blob_hex(const std::string& cell) {
    static const char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(2 + cell.size() * 2);
    out += "0x";
    for (unsigned char c : cell) {
        out += kHex[c >> 4];
        out += kHex[c & 0x0f];
    }
    return out;
}

}  // namespace

RowWriter::RowWriter(std::ostream& out, OutputFormat fmt,
                     std::vector<std::string> header,
                     std::vector<common::TSDataType> types, bool no_header)
    : out_(out),
      fmt_(fmt),
      header_(std::move(header)),
      types_(std::move(types)),
      no_header_(no_header),
      table_widths_(header_.size(), 0) {
    if (!no_header_) {
        for (size_t i = 0; i < header_.size(); ++i) {
            table_widths_[i] = header_[i].size();
        }
    }
}

RowWriter::~RowWriter() {
    if (spool_ != nullptr) {
        std::fclose(spool_);
    }
}

bool RowWriter::emits_json_bare(common::TSDataType type) const {
    // External NDJSON keeps INT64 and TIMESTAMP as decimal strings so
    // JavaScript consumers do not lose precision. Smaller and floating-point
    // numeric types remain bare JSON tokens; everything else is quoted.
    switch (type) {
        case common::BOOLEAN:
        case common::INT32:
        case common::FLOAT:
        case common::DOUBLE:
            return true;
        default:
            return false;
    }
}

std::string RowWriter::format_cell(common::TSDataType type,
                                   const std::string& cell) const {
    if (type == common::BLOB) {
        return blob_hex(cell);
    }
    return cell;
}

bool RowWriter::ensure_header() {
    if (header_done_) {
        return out_.good();
    }
    header_done_ = true;
    if (no_header_) {
        return out_.good();
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
    return out_.good();
}

bool RowWriter::write(const std::vector<std::string>& cells,
                      const std::vector<bool>& is_null) {
    return write(cells, is_null, types_);
}

bool RowWriter::write(const std::vector<std::string>& cells,
                      const std::vector<bool>& is_null,
                      const std::vector<common::TSDataType>& row_types) {
    if (!out_.good()) {
        return false;
    }
    if (fmt_ == OutputFormat::kTable) {
        if (spool_ == nullptr) {
            spool_ = std::tmpfile();
            if (spool_ == nullptr) {
                return false;
            }
        }
        for (size_t i = 0; i < header_.size(); ++i) {
            const unsigned char null =
                i >= cells.size() || (i < is_null.size() && is_null[i]);
            const common::TSDataType type =
                i < row_types.size() ? row_types[i] : common::STRING;
            const std::string value = null ? std::string() : cells[i];
            const uint64_t length = static_cast<uint64_t>(value.size());
            const int32_t serialized_type = static_cast<int32_t>(type);
            if (std::fwrite(&null, sizeof(null), 1, spool_) != 1 ||
                std::fwrite(&serialized_type, sizeof(serialized_type), 1,
                            spool_) != 1 ||
                std::fwrite(&length, sizeof(length), 1, spool_) != 1 ||
                (length > 0 &&
                 std::fwrite(value.data(), 1, length, spool_) != length)) {
                return false;
            }
            table_widths_[i] =
                std::max(table_widths_[i], format_cell(type, value).size());
        }
        return true;
    }
    if (fmt_ == OutputFormat::kJson) {
        out_ << "{";
        for (size_t i = 0; i < header_.size(); ++i) {
            if (i) {
                out_ << ",";
            }
            out_ << "\"" << json_escape(header_[i]) << "\":";
            const common::TSDataType type =
                i < row_types.size() ? row_types[i] : common::STRING;
            if (i < is_null.size() && is_null[i]) {
                out_ << "null";
            } else if (emits_json_bare(type)) {
                const std::string cell =
                    format_cell(type, i < cells.size() ? cells[i] : "");
                if (json_nonfinite(type, cell)) {
                    out_ << "null";  // NaN/Inf: match JSON serializer practice
                } else {
                    out_ << cell;
                }
            } else {
                out_ << "\""
                     << json_escape(
                            format_cell(type, i < cells.size() ? cells[i] : ""))
                     << "\"";
            }
        }
        out_ << "}\n";
        return out_.good();
    }

    if (!ensure_header()) {
        return false;
    }
    const char sep = (fmt_ == OutputFormat::kCsv) ? ',' : '\t';
    for (size_t i = 0; i < cells.size(); ++i) {
        if (i) {
            out_ << sep;
        }
        bool null_cell = i < is_null.size() && is_null[i];
        if (null_cell) {
            if (fmt_ == OutputFormat::kCsv) {
                out_ << "\\N";
            }
            continue;
        }
        const common::TSDataType type =
            i < row_types.size() ? row_types[i] : common::STRING;
        const std::string cell = format_cell(type, cells[i]);
        if (fmt_ == OutputFormat::kCsv && cell.empty()) {
            out_ << "\"\"";
        } else {
            out_ << (fmt_ == OutputFormat::kCsv ? csv_escape(cell) : cell);
        }
    }
    out_ << "\n";
    return out_.good();
}

bool RowWriter::finish() {
    if (!out_.good()) {
        return false;
    }
    if (fmt_ != OutputFormat::kTable) {
        if (fmt_ == OutputFormat::kCsv || fmt_ == OutputFormat::kTsv) {
            if (!ensure_header()) {
                return false;
            }
        }
        out_.flush();
        return out_.good();
    }

    const size_t ncols = header_.size();

    auto emit = [&](const std::vector<std::string>& cells,
                    const std::vector<bool>& nulls,
                    const std::vector<common::TSDataType>& row_types) {
        for (size_t i = 0; i < ncols; ++i) {
            std::string cell =
                (i < cells.size() && !(i < nulls.size() && nulls[i]))
                    ? format_cell(
                          i < row_types.size() ? row_types[i] : common::STRING,
                          cells[i])
                    : "";
            out_ << cell;
            if (i + 1 < ncols) {
                out_ << std::string(table_widths_[i] - cell.size() + 2, ' ');
            }
        }
        out_ << "\n";
    };

    if (!no_header_) {
        std::vector<bool> no_nulls(ncols, false);
        emit(header_, no_nulls,
             std::vector<common::TSDataType>(ncols, common::STRING));
    }
    if (spool_ != nullptr) {
        if (std::fflush(spool_) != 0 || std::fseek(spool_, 0, SEEK_SET) != 0) {
            return false;
        }
        while (true) {
            std::vector<std::string> cells(ncols);
            std::vector<bool> nulls(ncols, false);
            std::vector<common::TSDataType> row_types(ncols, common::STRING);
            bool eof = false;
            for (size_t i = 0; i < ncols; ++i) {
                unsigned char null = 0;
                int32_t serialized_type = 0;
                uint64_t length = 0;
                if (std::fread(&null, sizeof(null), 1, spool_) != 1) {
                    eof = i == 0 && std::feof(spool_);
                    if (!eof) {
                        return false;
                    }
                    break;
                }
                if (std::fread(&serialized_type, sizeof(serialized_type), 1,
                               spool_) != 1 ||
                    std::fread(&length, sizeof(length), 1, spool_) != 1) {
                    return false;
                }
                nulls[i] = null != 0;
                row_types[i] = static_cast<common::TSDataType>(serialized_type);
                cells[i].resize(static_cast<size_t>(length));
                if (length > 0 &&
                    std::fread(&cells[i][0], 1, length, spool_) != length) {
                    return false;
                }
            }
            if (eof) {
                break;
            }
            emit(cells, nulls, row_types);
            if (!out_.good()) {
                return false;
            }
        }
        std::fclose(spool_);
        spool_ = nullptr;
    }
    out_.flush();
    return out_.good();
}

}  // namespace tsfile_cli
