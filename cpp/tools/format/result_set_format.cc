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

#include "format/result_set_format.h"

#include <algorithm>
#include <cstdio>
#include <ctime>
#include <iomanip>
#include <limits>
#include <random>
#include <sstream>
#include <utility>
#include <vector>

#include "utils/errno_define.h"

namespace tsfile_cli {

std::string cell_to_string(storage::ResultSet* rs, uint32_t i,
                           common::TSDataType type) {
    std::ostringstream ss;
    switch (type) {
        case common::BOOLEAN:
            return rs->get_value<bool>(i) ? "true" : "false";
        case common::INT32:
            ss << rs->get_value<int32_t>(i);
            return ss.str();
        case common::INT64:
        case common::TIMESTAMP:
            ss << rs->get_value<int64_t>(i);
            return ss.str();
        case common::FLOAT:
            ss << std::setprecision(std::numeric_limits<float>::max_digits10)
               << rs->get_value<float>(i);
            return ss.str();
        case common::DOUBLE:
            ss << std::setprecision(std::numeric_limits<double>::max_digits10)
               << rs->get_value<double>(i);
            return ss.str();
        case common::DATE: {
            std::tm d = rs->get_value<std::tm>(i);
            char buf[16];
            std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", d.tm_year + 1900,
                          d.tm_mon + 1, d.tm_mday);
            return buf;
        }
        case common::TEXT:
        case common::STRING:
        case common::BLOB: {
            common::String* s = rs->get_value<common::String*>(i);
            return s == nullptr ? std::string() : s->to_std_string();
        }
        default:
            return "<UNKNOWN>";
    }
}

int emit_result_set(storage::ResultSet* rs, OutputFormat fmt, bool no_header,
                    std::ostream& out, long long offset, long long limit,
                    long long* emitted_rows) {
    auto meta = rs->get_metadata();
    const uint32_t ncol = meta->get_column_count();
    std::vector<std::string> header;
    std::vector<common::TSDataType> types;
    header.reserve(ncol);
    types.reserve(ncol);
    for (uint32_t i = 1; i <= ncol; ++i) {
        header.push_back(meta->get_column_name(i));
        types.push_back(meta->get_column_type(i));
    }

    RowWriter writer(out, fmt, header, types, no_header);
    bool has_next = false;
    int code = common::E_OK;
    long long skipped = 0;
    long long emitted = 0;
    while ((code = rs->next(has_next)) == common::E_OK && has_next) {
        if (skipped < offset) {
            ++skipped;
            continue;
        }
        if (limit >= 0 && emitted >= limit) {
            break;
        }
        std::vector<std::string> cells(ncol);
        std::vector<bool> nulls(ncol, false);
        for (uint32_t i = 1; i <= ncol; ++i) {
            if (rs->is_null(i)) {
                nulls[i - 1] = true;
            } else {
                cells[i - 1] = cell_to_string(rs, i, types[i - 1]);
            }
        }
        if (!writer.write(cells, nulls)) {
            return common::E_FILE_WRITE_ERR;
        }
        ++emitted;
    }
    if (code == common::E_OK && skipped < offset) {
        return common::E_OUT_OF_RANGE;
    }
    if (!writer.finish()) {
        return common::E_FILE_WRITE_ERR;
    }
    if (emitted_rows != nullptr) {
        *emitted_rows = emitted;
    }
    return code;
}

namespace {

struct BufferedRow {
    std::vector<std::string> cells;
    std::vector<bool> nulls;
};

BufferedRow read_current_row(storage::ResultSet* rs,
                             const std::vector<common::TSDataType>& types) {
    BufferedRow row;
    const uint32_t ncol = static_cast<uint32_t>(types.size());
    row.cells.assign(ncol, "");
    row.nulls.assign(ncol, false);
    for (uint32_t i = 1; i <= ncol; ++i) {
        if (rs->is_null(i)) {
            row.nulls[i - 1] = true;
        } else {
            row.cells[i - 1] = cell_to_string(rs, i, types[i - 1]);
        }
    }
    return row;
}

}  // namespace

int emit_result_set_sampled(storage::ResultSet* rs, OutputFormat fmt,
                            bool no_header, std::ostream& out, long long limit,
                            unsigned long long seed) {
    auto meta = rs->get_metadata();
    const uint32_t ncol = meta->get_column_count();
    std::vector<std::string> header;
    std::vector<common::TSDataType> types;
    header.reserve(ncol);
    types.reserve(ncol);
    for (uint32_t i = 1; i <= ncol; ++i) {
        header.push_back(meta->get_column_name(i));
        types.push_back(meta->get_column_type(i));
    }

    std::vector<BufferedRow> reservoir;
    // Cap the pre-allocation: limit is user input and a huge -n would make
    // reserve() throw std::length_error before any row is read. The vector
    // still grows up to `limit` as rows actually arrive.
    reservoir.reserve(static_cast<size_t>(std::min<long long>(limit, 4096)));
    std::mt19937_64 rng(seed);
    bool has_next = false;
    int code = common::E_OK;
    long long seen = 0;
    while ((code = rs->next(has_next)) == common::E_OK && has_next) {
        BufferedRow row = read_current_row(rs, types);
        if (static_cast<long long>(reservoir.size()) < limit) {
            reservoir.push_back(std::move(row));
        } else {
            std::uniform_int_distribution<long long> dist(0, seen);
            long long idx = dist(rng);
            if (idx < limit) {
                reservoir[static_cast<size_t>(idx)] = std::move(row);
            }
        }
        ++seen;
    }

    RowWriter writer(out, fmt, header, types, no_header);
    for (const BufferedRow& row : reservoir) {
        if (!writer.write(row.cells, row.nulls)) {
            return common::E_FILE_WRITE_ERR;
        }
    }
    if (!writer.finish()) {
        return common::E_FILE_WRITE_ERR;
    }
    return code;
}

}  // namespace tsfile_cli
