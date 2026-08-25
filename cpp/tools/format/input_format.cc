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

std::string lower_ascii(const std::string& s) {
    std::string out;
    out.reserve(s.size());
    for (unsigned char c : s) {
        out += static_cast<char>(c >= 'A' && c <= 'Z' ? c + ('a' - 'A') : c);
    }
    return out;
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
        def.name = lower_ascii(parts[0]);
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
        if (!validate_identifier(def.name, error)) {
            return false;
        }
        if (def.category == common::ColumnCategory::TAG &&
            def.type != common::STRING) {
            error = "TAG column '" + def.name + "' must use STRING";
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

bool is_valid_utf8(const std::string& s) {
    const unsigned char* bytes =
        reinterpret_cast<const unsigned char*>(s.data());
    size_t i = 0;
    while (i < s.size()) {
        unsigned char b = bytes[i];
        if (b <= 0x7F) {
            ++i;
            continue;
        }
        size_t count = 0;
        if (b >= 0xC2 && b <= 0xDF) {
            count = 1;
        } else if (b >= 0xE0 && b <= 0xEF) {
            count = 2;
        } else if (b >= 0xF0 && b <= 0xF4) {
            count = 3;
        } else {
            return false;
        }
        if (i + count >= s.size()) {
            return false;
        }
        for (size_t j = 1; j <= count; ++j) {
            if (bytes[i + j] < 0x80 || bytes[i + j] > 0xBF) {
                return false;
            }
        }
        if ((b == 0xE0 && bytes[i + 1] < 0xA0) ||
            (b == 0xED && bytes[i + 1] > 0x9F) ||
            (b == 0xF0 && bytes[i + 1] < 0x90) ||
            (b == 0xF4 && bytes[i + 1] > 0x8F)) {
            return false;
        }
        i += count + 1;
    }
    return true;
}

bool contains_utf8_bom(const std::string& s) {
    return s.find("\xEF\xBB\xBF") != std::string::npos;
}

bool validate_identifier(const std::string& name, std::string& error) {
    if (name.empty()) {
        error = "name must not be empty";
        return false;
    }
    if (!is_valid_utf8(name) || contains_utf8_bom(name)) {
        error = "name '" + name + "' is not valid UTF-8";
        return false;
    }
    for (size_t i = 0; i < name.size(); ++i) {
        const unsigned char c = static_cast<unsigned char>(name[i]);
        if (c < 0x20 || c == 0x7F) {
            error = "name contains a control character";
            return false;
        }
        if (c == 0xC2 && i + 1 < name.size()) {
            const unsigned char next = static_cast<unsigned char>(name[i + 1]);
            if (next >= 0x80 && next <= 0x9F) {
                error = "name contains a control character";
                return false;
            }
        }
    }
    if (lower_ascii(name) == "time") {
        error = "name '" + name + "' is reserved";
        return false;
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
