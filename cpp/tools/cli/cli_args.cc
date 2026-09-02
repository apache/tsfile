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

#include "cli/cli_args.h"

#include <algorithm>
#include <cerrno>
#include <cstdlib>

namespace tsfile_cli {
namespace {

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

bool parse_strict_i64(const std::string& s, long long& out) {
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
    char* endp = nullptr;
    errno = 0;
    long long v = std::strtoll(s.c_str(), &endp, 10);
    if (endp == nullptr || *endp != '\0' || errno == ERANGE) {
        return false;
    }
    out = v;
    return true;
}

bool parse_strict_non_negative(const std::string& s, long long& out) {
    if (s.empty()) {
        return false;
    }
    if (!has_strict_decimal_body(s, 0)) {
        return false;
    }
    char* endp = nullptr;
    errno = 0;
    long long v = std::strtoll(s.c_str(), &endp, 10);
    if (endp == nullptr || *endp != '\0' || errno == ERANGE || v < 0) {
        return false;
    }
    out = v;
    return true;
}

bool parse_format(const std::string& s, ParsedArgs::Format& out) {
    if (s == "csv") {
        out = ParsedArgs::Format::kCsv;
    } else if (s == "ndjson") {
        out = ParsedArgs::Format::kJson;
    } else if (s == "table") {
        out = ParsedArgs::Format::kTable;
    } else {
        return false;
    }
    return true;
}

bool parse_tag_filter_op(const std::string& s, ParsedArgs::TagFilterOp& out) {
    if (s == "eq") {
        out = ParsedArgs::TagFilterOp::kEq;
    } else if (s == "neq") {
        out = ParsedArgs::TagFilterOp::kNeq;
    } else if (s == "regexp") {
        out = ParsedArgs::TagFilterOp::kRegexp;
    } else if (s == "is-null") {
        out = ParsedArgs::TagFilterOp::kIsNull;
    } else if (s == "not-null") {
        out = ParsedArgs::TagFilterOp::kNotNull;
    } else {
        return false;
    }
    return true;
}

bool tag_filter_op_needs_value(ParsedArgs::TagFilterOp op) {
    return op == ParsedArgs::TagFilterOp::kEq ||
           op == ParsedArgs::TagFilterOp::kNeq ||
           op == ParsedArgs::TagFilterOp::kRegexp;
}

}  // namespace

ParsedArgs parse_args(const std::vector<std::string>& args) {
    ParsedArgs p;
    if (args.empty()) {
        return p;
    }
    p.command = args[0];
    if (p.command == "--version") {
        p.version = true;
    }
    if (p.command == "--help" || p.command == "-h") {
        p.help = true;
    }
    // The subcommand must come first. A leading option means it was omitted;
    // say so explicitly instead of failing later with a confusing message about
    // the first real positional argument.
    if (p.command.size() > 1 && p.command[0] == '-' && !p.version && !p.help) {
        p.error = "the command must come before options (got option '" +
                  p.command + "'); run with --help for usage";
        return p;
    }

    size_t i = 1;
    auto need_value = [&](const std::string& flag, std::string& dst) -> bool {
        if (i + 1 >= args.size()) {
            p.error = "Missing value for " + flag;
            return false;
        }
        dst = args[++i];
        return true;
    };
    auto append_column = [&](const std::string& name, const std::string& type,
                             bool is_tag) {
        p.columns.emplace_back(name, type, is_tag);
    };
    bool positional_file_set = false;
    bool limit_set = false;
    bool offset_set = false;
    bool output_set = false;
    bool output_dir_set = false;
    bool force_set = false;
    for (; i < args.size(); ++i) {
        const std::string& a = args[i];
        std::string val;
        if (positional_file_set) {
            p.error = "Unexpected argument after file: " + a;
            return p;
        }
        if (a == "--") {
            if (i + 1 >= args.size()) {
                p.error = "Missing value after --";
                return p;
            }
            if (!p.file.empty() || i + 2 != args.size()) {
                p.error = "Unexpected argument after file: " + args[i + 1];
                return p;
            }
            p.file = args[++i];
            positional_file_set = true;
            continue;
        }
        if (a == "-f" || a == "--format") {
            if (p.format_set) {
                p.error = "--format specified more than once";
                return p;
            }
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_format(val, p.format)) {
                p.error = "unsupported format '" + val +
                          "'; expected table, ndjson, or csv";
                return p;
            }
            p.format_set = true;
        } else if (a == "-d" || a == "--device") {
            if (!need_value(a, val)) {
                return p;
            }
            p.device = val;
            p.devices.push_back(val);
        } else if (a == "-t" || a == "--table") {
            if (!need_value(a, val)) {
                return p;
            }
            p.table = val;
            p.tables.push_back(val);
        } else if (a == "-m" || a == "--measurements") {
            if (!need_value(a, val)) {
                return p;
            }
            if (val.find(',') != std::string::npos) {
                p.error =
                    "--measurements accepts one column per option; repeat -m";
                return p;
            }
            if (std::find(p.measurements.begin(), p.measurements.end(), val) !=
                p.measurements.end()) {
                p.error = "measurement '" + val + "' specified more than once";
                return p;
            }
            p.measurements.push_back(val);
        } else if (a == "-n" || a == "--limit") {
            if (limit_set) {
                p.error = "--limit specified more than once";
                return p;
            }
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_strict_non_negative(val, p.limit)) {
                p.error = "Invalid -n/--limit: " + val;
                return p;
            }
            limit_set = true;
        } else if (a == "--offset") {
            if (offset_set) {
                p.error = "--offset specified more than once";
                return p;
            }
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_strict_non_negative(val, p.offset)) {
                p.error = "Invalid --offset: " + val;
                return p;
            }
            offset_set = true;
        } else if (a == "--start") {
            if (p.has_start) {
                p.error = "--start specified more than once";
                return p;
            }
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_strict_i64(val, p.start)) {
                p.error = "Invalid --start: " + val;
                return p;
            }
            p.has_start = true;
        } else if (a == "--end") {
            if (p.has_end) {
                p.error = "--end specified more than once";
                return p;
            }
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_strict_i64(val, p.end)) {
                p.error = "Invalid --end: " + val;
                return p;
            }
            p.has_end = true;
        } else if (a == "--seed") {
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_strict_i64(val, p.seed)) {
                p.error = "Invalid --seed: " + val;
                return p;
            }
            p.has_seed = true;
        } else if (a == "--type") {
            if (p.export_format_set) {
                p.error = "--type specified more than once";
                return p;
            }
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_format(val, p.export_format)) {
                p.error = "unsupported export type '" + val +
                          "'; expected table, ndjson, or csv";
                return p;
            }
            p.export_format_set = true;
        } else if (a == "-o" || a == "--output") {
            if (output_set) {
                p.error = "--output specified more than once";
                return p;
            }
            if (!need_value(a, p.output)) {
                return p;
            }
            output_set = true;
        } else if (a == "--output-dir") {
            if (output_dir_set) {
                p.error = "--output-dir specified more than once";
                return p;
            }
            if (!need_value(a, p.output_dir)) {
                return p;
            }
            output_dir_set = true;
        } else if (a == "--columns") {
            p.error = "Unknown flag: --columns";
            return p;
        } else if (a == "--field" || a == "--tag") {
            if (i + 2 >= args.size()) {
                p.error = "Missing value for " + a;
                return p;
            }
            std::string name = args[++i];
            std::string type = args[++i];
            append_column(name, type, a == "--tag");
        } else if (a == "-i" || a == "--input") {
            if (p.input_set) {
                p.error = "choose exactly one of --input or --stdin";
                return p;
            }
            if (!need_value(a, p.file)) {
                return p;
            }
            p.input_set = true;
        } else if (a == "--stdin") {
            if (p.input_set) {
                p.error = "choose exactly one of --input or --stdin";
                return p;
            }
            p.file = "-";
            p.input_set = true;
        } else if (a == "--encoding" || a == "--compression") {
            if (i + 2 >= args.size()) {
                p.error = "Missing value for " + a;
                return p;
            }
            ParsedArgs::PhysicalOverride override;
            override.kind =
                a == "--encoding"
                    ? ParsedArgs::PhysicalOverride::Kind::kEncoding
                    : ParsedArgs::PhysicalOverride::Kind::kCompression;
            override.data_type = args[++i];
            override.value = args[++i];
            p.physical_overrides.push_back(override);
        } else if (a == "--force") {
            if (force_set) {
                p.error = "--force specified more than once";
                return p;
            }
            p.force = true;
            force_set = true;
        } else if (a == "-v" || a == "--verbose") {
            p.verbose = true;
        } else if (a == "--header-match") {
            p.header_match = true;
        } else if (a == "--tag-filter") {
            if (i + 2 >= args.size()) {
                p.error = "Missing value for " + a;
                return p;
            }
            ParsedArgs::TagFilterSpec spec;
            spec.column = args[++i];
            std::string op = args[++i];
            if (!parse_tag_filter_op(op, spec.op)) {
                p.error = "Invalid --tag-filter operator: " + op +
                          " (use eq|neq|regexp|is-null|not-null)";
                return p;
            }
            if (tag_filter_op_needs_value(spec.op)) {
                if (i + 1 >= args.size()) {
                    p.error = "Missing value for " + a;
                    return p;
                }
                spec.value = args[++i];
            }
            p.has_tag_filter = true;
            p.tag_filters.push_back(spec);
        } else if (a == "--tag-match") {
            if (!need_value(a, val)) {
                return p;
            }
            if (val != "all" && val != "any") {
                p.error = "Invalid --tag-match: " + val + " (use all or any)";
                return p;
            }
            if (!p.tag_match.empty()) {
                p.error = "--tag-match specified more than once";
                return p;
            }
            p.tag_match = val;
        } else if (a == "--model") {
            p.error = "Unknown flag: --model";
            return p;
        } else if (a == "--no-header") {
            p.no_header = true;
        } else if (a == "-h" || a == "--help") {
            if (i != 1 || args.size() != 2) {
                p.error =
                    "--help must appear by itself or immediately after a "
                    "command";
                return p;
            }
            p.help = true;
            return p;
        } else if (a == "--version") {
            p.error = "--version must appear by itself";
            return p;
        } else if (a.size() > 1 && a[0] == '-') {
            p.error = "Unknown flag: " + a;
            return p;
        } else {
            if (p.file.empty()) {
                p.file = a;
                positional_file_set = true;
            } else {
                p.error = "Unexpected argument: " + a;
                return p;
            }
        }
    }
    return p;
}

}  // namespace tsfile_cli
