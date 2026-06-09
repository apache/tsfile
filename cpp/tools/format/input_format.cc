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

#include "format/input_format.h"

#include <cctype>
#include <istream>

#include "common/csv_utils.h"

namespace tsfile_cli {

namespace {

void strip_cr(std::string& s) {
    if (!s.empty() && s.back() == '\r') {
        s.pop_back();
    }
}

}  // namespace

bool read_record(std::istream& in, bool csv_quotes, std::string& record,
                 long long& lines_consumed) {
    record.clear();
    lines_consumed = 0;
    std::string physical;
    bool open_quote = false;
    while (std::getline(in, physical)) {
        strip_cr(physical);
        ++lines_consumed;
        if (!record.empty()) {
            record.push_back('\n');  // restore the newline inside the field
        }
        record += physical;
        if (csv_quotes) {
            for (char c : physical) {
                if (c == '"') {
                    open_quote = !open_quote;
                }
            }
            if (open_quote) {
                continue;  // quote still open: the field spans the next line
            }
        }
        return true;
    }
    return lines_consumed > 0;  // trailing record with an unterminated quote
}

bool parse_datatype_name(const std::string& s, common::TSDataType& out) {
    return common::parse_data_type_name(s, out);
}

bool parse_category(const std::string& s, common::ColumnCategory& out) {
    std::string l;
    l.reserve(s.size());
    for (char c : s) {
        l += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (l == "tag") {
        out = common::ColumnCategory::TAG;
    } else if (l == "field") {
        out = common::ColumnCategory::FIELD;
    } else {
        return false;
    }
    return true;
}

std::vector<std::string> split_line(const std::string& line, char delim,
                                    bool csv_quotes) {
    return common::split_csv_line(line, delim, csv_quotes);
}

bool parse_columns_spec(const std::string& spec, std::vector<ColumnDef>& out,
                        std::string& error) {
    out.clear();
    if (spec.empty()) {
        error = "empty --columns";
        return false;
    }
    std::vector<std::string> items = split_line(spec, ',', false);
    for (const std::string& item : items) {
        std::vector<std::string> parts = split_line(item, ':', false);
        if (parts.size() != 3) {
            error = "bad column '" + item + "' (want name:TYPE:category)";
            return false;
        }
        ColumnDef def;
        def.name = parts[0];
        if (def.name.empty()) {
            error = "empty column name in '" + item + "'";
            return false;
        }
        if (!parse_datatype_name(parts[1], def.type)) {
            error = "unknown type '" + parts[1] + "'";
            return false;
        }
        if (!parse_category(parts[2], def.category)) {
            error = "bad category '" + parts[2] + "' (want tag|field)";
            return false;
        }
        for (const ColumnDef& prev : out) {
            if (prev.name == def.name) {
                error = "duplicate column name '" + def.name + "'";
                return false;
            }
        }
        out.push_back(def);
    }
    return true;
}

bool parse_bool_cell(const std::string& s, bool& out) {
    std::string l;
    l.reserve(s.size());
    for (char c : s) {
        l += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    if (l == "true" || l == "1") {
        out = true;
        return true;
    }
    if (l == "false" || l == "0") {
        out = false;
        return true;
    }
    return false;
}

}  // namespace tsfile_cli
