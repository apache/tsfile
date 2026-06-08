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

#include "common/csv_utils.h"

namespace common {

std::vector<std::string> split_csv_line(const std::string& line, char delim,
                                        bool csv_quotes) {
    std::vector<std::string> out;
    std::string field;
    if (!csv_quotes) {
        for (char c : line) {
            if (c == delim) {
                out.push_back(field);
                field.clear();
            } else {
                field += c;
            }
        }
        out.push_back(field);
        return out;
    }
    bool in_quotes = false;
    for (size_t i = 0; i < line.size(); ++i) {
        char c = line[i];
        if (in_quotes) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    field += '"';
                    ++i;
                } else {
                    in_quotes = false;
                }
            } else {
                field += c;
            }
        } else if (c == '"') {
            in_quotes = true;
        } else if (c == delim) {
            out.push_back(field);
            field.clear();
        } else {
            field += c;
        }
    }
    out.push_back(field);
    return out;
}

std::string csv_escape(const std::string& field, char delim) {
    const char triggers[] = {delim, '"', '\n', '\r'};
    bool needs_quote =
        field.find_first_of(triggers, 0, sizeof(triggers)) != std::string::npos;
    if (!needs_quote) {
        return field;
    }
    std::string out = "\"";
    for (char c : field) {
        if (c == '"') {
            out += "\"\"";
        } else {
            out += c;
        }
    }
    out += "\"";
    return out;
}

}  // namespace common
