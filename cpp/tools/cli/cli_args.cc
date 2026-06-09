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

#include <cstdlib>
#include <sstream>

namespace tsfile_cli {
namespace {

std::vector<std::string> split_csv(const std::string& s) {
    std::vector<std::string> out;
    std::string item;
    std::istringstream iss(s);
    while (std::getline(iss, item, ',')) {
        if (!item.empty()) {
            out.push_back(item);
        }
    }
    return out;
}

bool parse_ll(const std::string& s, long long& out) {
    if (s.empty()) {
        return false;
    }
    char* endp = nullptr;
    long long v = std::strtoll(s.c_str(), &endp, 10);
    if (endp == nullptr || *endp != '\0') {
        return false;
    }
    out = v;
    return true;
}

bool parse_format(const std::string& s, ParsedArgs::Format& out) {
    if (s == "csv") {
        out = ParsedArgs::Format::kCsv;
    } else if (s == "tsv") {
        out = ParsedArgs::Format::kTsv;
    } else if (s == "json") {
        out = ParsedArgs::Format::kJson;
    } else if (s == "table") {
        out = ParsedArgs::Format::kTable;
    } else {
        return false;
    }
    return true;
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

    for (; i < args.size(); ++i) {
        const std::string& a = args[i];
        std::string val;
        if (a == "-f" || a == "--format") {
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_format(val, p.format)) {
                p.error =
                    "Invalid format: " + val + " (use csv|tsv|json|table)";
                return p;
            }
        } else if (a == "-d" || a == "--device") {
            if (!need_value(a, p.device)) {
                return p;
            }
        } else if (a == "-t" || a == "--table") {
            if (!need_value(a, p.table)) {
                return p;
            }
        } else if (a == "-m" || a == "--measurements") {
            if (!need_value(a, val)) {
                return p;
            }
            p.measurements = split_csv(val);
        } else if (a == "-n" || a == "--limit") {
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_ll(val, p.limit)) {
                p.error = "Invalid -n/--limit: " + val;
                return p;
            }
        } else if (a == "--offset") {
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_ll(val, p.offset)) {
                p.error = "Invalid --offset: " + val;
                return p;
            }
        } else if (a == "--start") {
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_ll(val, p.start)) {
                p.error = "Invalid --start: " + val;
                return p;
            }
            p.has_start = true;
        } else if (a == "--end") {
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_ll(val, p.end)) {
                p.error = "Invalid --end: " + val;
                return p;
            }
            p.has_end = true;
        } else if (a == "--seed") {
            if (!need_value(a, val)) {
                return p;
            }
            if (!parse_ll(val, p.seed)) {
                p.error = "Invalid --seed: " + val;
                return p;
            }
            p.has_seed = true;
        } else if (a == "-o" || a == "--output") {
            if (!need_value(a, p.output)) {
                return p;
            }
        } else if (a == "--columns") {
            if (!need_value(a, p.columns)) {
                return p;
            }
        } else if (a == "-v" || a == "--verbose") {
            p.verbose = true;
        } else if (a == "--header-match") {
            p.header_match = true;
        } else if (a == "--model") {
            if (!need_value(a, val)) {
                return p;
            }
            if (val != "tree" && val != "table") {
                p.error = "Invalid --model: " + val + " (use tree|table)";
                return p;
            }
            p.model = val;
        } else if (a == "--no-header") {
            p.no_header = true;
        } else if (a == "-h" || a == "--help") {
            p.help = true;
            return p;  // help wins; stop parsing the rest
        } else if (a == "--version") {
            p.version = true;
            return p;  // version wins; stop parsing the rest
        } else if (a.size() > 1 && a[0] == '-') {
            p.error = "Unknown flag: " + a;
            return p;
        } else {
            if (p.file.empty()) {
                p.file = a;
            } else {
                p.error = "Unexpected argument: " + a;
                return p;
            }
        }
    }
    return p;
}

}  // namespace tsfile_cli
