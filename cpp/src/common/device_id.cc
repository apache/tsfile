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

#include "common/device_id.h"

#include <algorithm>
#include <cctype>
#include <numeric>
#include <stdexcept>
#include <unordered_set>

#include "constant/tsfile_constant.h"
#ifdef ENABLE_ANTLR4
#include "parser/path_nodes_generator.h"
#endif
#include "utils/errno_define.h"

namespace storage {

// IDeviceID implementation
IDeviceID::IDeviceID() : empty_segments_() {}

int IDeviceID::serialize(common::ByteStream& write_stream) { return 0; }
int IDeviceID::deserialize(common::ByteStream& read_stream) { return 0; }
std::string IDeviceID::get_table_name() { return ""; }
int IDeviceID::segment_num() { return 0; }
const std::vector<std::string*>& IDeviceID::get_segments() const {
    return empty_segments_;
}
std::string IDeviceID::get_device_name() const { return ""; }
bool IDeviceID::operator<(const IDeviceID& other) { return false; }
bool IDeviceID::operator==(const IDeviceID& other) { return false; }
bool IDeviceID::operator!=(const IDeviceID& other) { return false; }

// IDeviceIDComparator implementation
bool IDeviceIDComparator::operator()(
    const std::shared_ptr<IDeviceID>& lhs,
    const std::shared_ptr<IDeviceID>& rhs) const {
    return *lhs < *rhs;
}

// StringArrayDeviceID implementation
StringArrayDeviceID::StringArrayDeviceID(
    const std::vector<std::string>& segments)
    : segments_(formalize(segments)) {}

StringArrayDeviceID::StringArrayDeviceID(const std::string& device_id_string) {
    auto segments = split_device_id_string(device_id_string);
    segments_.reserve(segments.size());
    for (const auto& segment : segments) {
        segments_.push_back(new std::string(segment));
    }
}

StringArrayDeviceID::StringArrayDeviceID(
    const std::vector<std::string*>& segments) {
    segments_.reserve(segments.size());
    for (const auto& segment : segments) {
        segments_.push_back(segment == nullptr ? nullptr
                                               : new std::string(*segment));
    }
}

StringArrayDeviceID::StringArrayDeviceID() : segments_() {}

StringArrayDeviceID::~StringArrayDeviceID() {
    for (const auto& segment : segments_) {
        delete segment;
    }
    for (const auto& prefix_segments : prefix_segments_) {
        delete prefix_segments;
    }
}

std::string StringArrayDeviceID::get_device_name() const {
    if (segments_.empty()) {
        return "";
    }
    // Builds device name by concatenating segments with '.' delimiter,
    // handling null segments by replacing them with "null"
    return std::accumulate(std::next(segments_.begin()), segments_.end(),
                           segments_.front() ? *segments_.front() : "null",
                           [](std::string acc, const std::string* segment) {
                               return std::move(acc) + "." +
                                      (segment ? *segment : "null");
                           });
}

void StringArrayDeviceID::init_prefix_segments() {
#ifdef ENABLE_ANTLR4
    auto splits = storage::PathNodesGenerator::invokeParser(*segments_[0]);
#else
    auto splits = split_string(*segments_[0], '.');
#endif
    for (int i = 0; i < splits.size(); ++i) {
        prefix_segments_.push_back(new std::string(splits[i]));
    }
}

int StringArrayDeviceID::serialize(common::ByteStream& write_stream) {
    int ret = common::E_OK;
    if (RET_FAIL(common::SerializationUtil::write_var_uint(segment_num(),
                                                           write_stream))) {
        return ret;
    }
    for (const auto& segment : segments_) {
        if (RET_FAIL(common::SerializationUtil::write_var_char_ptr(
                segment, write_stream))) {
            return ret;
        }
    }
    return ret;
}

int StringArrayDeviceID::deserialize(common::ByteStream& read_stream) {
    int ret = common::E_OK;
    uint32_t num_segments;
    if (RET_FAIL(common::SerializationUtil::read_var_uint(num_segments,
                                                          read_stream))) {
        return ret;
    }

    for (auto& segment : segments_) {
        if (segment != nullptr) {
            delete segment;
        }
    }

    segments_.clear();
    for (uint32_t i = 0; i < num_segments; ++i) {
        std::string* segment;
        if (RET_FAIL(common::SerializationUtil::read_var_char_ptr(
                segment, read_stream))) {
            delete segment;
            return ret;
        }
        segments_.push_back(segment);
    }
    return ret;
}

std::string StringArrayDeviceID::get_table_name() {
    return segments_.empty() ? "" : *segments_[0];
}

int StringArrayDeviceID::segment_num() {
    return static_cast<int>(segments_.size());
}

const std::vector<std::string*>& StringArrayDeviceID::get_segments() const {
    return segments_;
}

bool StringArrayDeviceID::operator<(const IDeviceID& other) {
    auto other_segments = other.get_segments();
    return std::lexicographical_compare(
        segments_.begin(), segments_.end(), other_segments.begin(),
        other_segments.end(), [](const std::string* a, const std::string* b) {
            if (a == nullptr && b == nullptr) return false;  // equal
            if (a == nullptr) return true;   // nullptr < any string
            if (b == nullptr) return false;  // any string > nullptr
            return *a < *b;
        });
}

bool StringArrayDeviceID::operator==(const IDeviceID& other) {
    auto other_segments = other.get_segments();
    return (segments_.size() == other_segments.size()) &&
           std::equal(segments_.begin(), segments_.end(),
                      other_segments.begin(),
                      [](const std::string* a, const std::string* b) {
                          if (a == nullptr && b == nullptr) return true;
                          if (a == nullptr || b == nullptr) return false;
                          return *a == *b;
                      });
}

bool StringArrayDeviceID::operator!=(const IDeviceID& other) {
    return !(*this == other);
}

std::vector<std::string*> StringArrayDeviceID::formalize(
    const std::vector<std::string>& segments) {
    std::vector<std::string*> result;
    result.reserve(segments.size());
    for (const auto& segment : segments) {
        result.emplace_back(new std::string(segment));
    }
    return result;
}

std::vector<std::string> StringArrayDeviceID::split_device_id_string(
    const std::string& device_id_string) {
#ifdef ENABLE_ANTLR4
    auto splits = storage::PathNodesGenerator::invokeParser(device_id_string);
#else
    auto splits = split_string(device_id_string, '.');
#endif
    return split_device_id_string(splits);
}

std::vector<std::string> StringArrayDeviceID::split_device_id_string(
    const std::vector<std::string>& splits) {
    size_t segment_cnt = splits.size();
    std::vector<std::string> final_segments;

    if (segment_cnt == 0) {
        return final_segments;
    }

    if (segment_cnt == 1) {
        // "root" -> {"root"}
        final_segments.push_back(splits[0]);
    } else if (segment_cnt <
               static_cast<size_t>(storage::DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME +
                                   1)) {
        // "root.a" -> {"root", "a"}
        // "root.a.b" -> {"root.a", "b"}
        std::string table_name = std::accumulate(
            splits.begin(), splits.end() - 1, std::string(),
            [](const std::string& a, const std::string& b) {
                return a.empty() ? b : a + storage::PATH_SEPARATOR + b;
            });
        final_segments.push_back(table_name);
        final_segments.push_back(splits.back());
    } else {
        // "root.a.b.c" -> {"root.a.b", "c"}
        // "root.a.b.c.d" -> {"root.a.b", "c", "d"}
        std::string table_name = std::accumulate(
            splits.begin(),
            splits.begin() + storage::DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME,
            std::string(), [](const std::string& a, const std::string& b) {
                return a.empty() ? b : a + storage::PATH_SEPARATOR + b;
            });

        final_segments.emplace_back(std::move(table_name));
        final_segments.insert(
            final_segments.end(),
            splits.begin() + storage::DEFAULT_SEGMENT_NUM_FOR_TABLE_NAME,
            splits.end());
    }

    return final_segments;
}

std::vector<std::string> IDeviceID::split_string(const std::string& str,
                                                 char delimiter) {
    std::vector<std::string> tokens;

    // Reject newlines in path explicitly (illegal path name).
    if (str.find('\n') != std::string::npos ||
        str.find('\r') != std::string::npos) {
        throw std::runtime_error("Path contains newline");
    }

    std::string token;
    bool in_back_quotes = false;    // Inside `quoted` section
    bool in_double_quotes = false;  // Inside "quoted" section
    bool in_single_quotes = false;  // Inside 'quoted' section

    for (size_t i = 0; i < str.length(); ++i) {
        char c = str[i];

        // Toggle quote state when encountering a quote character outside other
        // quote types.
        if (c == '`' && !in_double_quotes && !in_single_quotes) {
            in_back_quotes = !in_back_quotes;
            token += c;  // preserve the backtick character
        } else if (c == '"' && !in_back_quotes && !in_single_quotes) {
            in_double_quotes = !in_double_quotes;
            token += c;  // preserve
        } else if (c == '\'' && !in_back_quotes && !in_double_quotes) {
            in_single_quotes = !in_single_quotes;
            token += c;  // preserve
        } else if (c == delimiter && !in_back_quotes && !in_double_quotes &&
                   !in_single_quotes) {
            // delimiter outside quotes -> split
            if (!token.empty()) {
                validate_identifier(token);
                tokens.push_back(unquote_identifier(token));
                token.clear();
            } else {
                tokens.push_back(token);
                token.clear();
            }
        } else {
            // preserve all characters verbatim (including backslashes and
            // doubled backticks)
            token += c;
        }
    }

    // Unmatched quotes are errors
    if (in_back_quotes || in_double_quotes || in_single_quotes) {
        throw std::runtime_error("Unmatched quotes in path");
    }

    // Add the last token if non-empty (mirror original behaviour)
    if (!token.empty()) {
        validate_identifier(token);
        tokens.push_back(unquote_identifier(token));
    }

    return tokens;
}

std::string IDeviceID::unquote_identifier(const std::string& identifier) {
    if (identifier.length() >= 2) {
        char first = identifier.front();
        char last = identifier.back();

        if ((first == '`' && last == '`') || (first == '"' && last == '"') ||
            (first == '\'' && last == '\'')) {
            std::string inner = identifier.substr(1, identifier.length() - 2);

            // Lowercase copy for case-insensitive comparison
            std::string lower_inner = inner;
            std::transform(lower_inner.begin(), lower_inner.end(),
                           lower_inner.begin(),
                           [](unsigned char ch) { return std::tolower(ch); });

            static const std::unordered_set<std::string> keywords = {
                "select", "device", "drop_trigger", "and",
                "or",     "not",    "null",         "contains"};

            // If the identifier is enclosed in backticks AND inner is a
            // keyword, unquote it (testcase: `select` -> select)
            if (first == '`' && keywords.find(lower_inner) != keywords.end()) {
                return inner;
            }

            // Otherwise: keep original quoting
        }
    }

    return identifier;
}

void IDeviceID::validate_identifier(const std::string& identifier) {
    if (identifier.empty()) return;

    bool quoted = (identifier.size() >= 2 &&
                   ((identifier.front() == '`' && identifier.back() == '`') ||
                    (identifier.front() == '"' && identifier.back() == '"') ||
                    (identifier.front() == '\'' && identifier.back() == '\'')));

    if (quoted) return;  // quoted identifiers are always accepted

    // 1. Pure digits - unquoted numeric literals not allowed as identifiers
    bool all_digits = true;
    for (char c : identifier) {
        if (!std::isdigit((unsigned char)c)) {
            all_digits = false;
            break;
        }
    }
    if (all_digits) {
        throw std::runtime_error("Unquoted pure digits are illegal");
    }

    // 2. Numeric-like illegal patterns (e.g., 0e38, 00.12 not allowed unquoted)
    if (!identifier.empty() && std::isdigit((unsigned char)identifier[0])) {
        throw std::runtime_error("Identifier cannot start with a digit");
    }

    // 3. Illegal wildcards in unquoted identifiers
    if (identifier.find('%') != std::string::npos) {
        throw std::runtime_error("Illegal wildcard in unquoted identifier");
    }

    // 4. Asterisk (*) validation: if present, must consist only of asterisks
    if (identifier.find('*') != std::string::npos) {
        // Check if all characters are asterisks
        bool all_asterisks = true;
        for (char c : identifier) {
            if (c != '*') {
                all_asterisks = false;
                break;
            }
        }
        if (!all_asterisks) {
            throw std::runtime_error(
                "Asterisk wildcard must be the only character type");
        }
    }
}

}  // namespace storage