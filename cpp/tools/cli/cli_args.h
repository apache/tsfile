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

#ifndef TSFILE_CLI_CLI_ARGS_H
#define TSFILE_CLI_CLI_ARGS_H

#include <climits>
#include <string>
#include <vector>

#include "cli/write_columns.h"

namespace tsfile_cli {

struct ParsedArgs {
    enum class Format { kAuto, kCsv, kTsv, kJson, kTable };
    enum class TagFilterOp {
        kNone,
        kEq,
        kNeq,
        kRegexp,
        kIsNull,
        kNotNull,
    };

    struct TagFilterSpec {
        std::string column;
        TagFilterOp op = TagFilterOp::kNone;
        std::string value;
    };

    struct PhysicalOverride {
        enum class Kind { kEncoding, kCompression };
        Kind kind;
        std::string data_type;
        std::string value;
    };

    std::string command;  // subcommand, e.g. "ls"/"write" (args[0])
    std::string file;     // positional <file.tsfile>; for write, the CSV/TSV
                          // input ("" or "-" means read stdin)
    std::string device;   // -d/--device filter (tree model)
    std::string table;    // -t/--table filter (table model); write target table
    std::vector<std::string> devices;  // all -d/--device values, in order
    std::vector<std::string> tables;   // all -t/--table values, in order
    std::vector<std::string> measurements;  // -m/--measurements projection
    long long limit = -1;          // -n/--limit; -1 means unlimited
    long long offset = 0;          // --offset; rows to skip before emitting
    long long start = LLONG_MIN;   // --start; inclusive lower time bound
    long long end = LLONG_MAX;     // --end; inclusive upper time bound
    bool has_start = false;        // whether --start was supplied
    bool has_end = false;          // whether --end was supplied
    long long seed = 0;            // parsed only to reject obsolete --seed
    bool has_seed = false;         // whether --seed was supplied
    Format format = Format::kAuto;  // -f/--format; kAuto resolves to table
    bool format_set = false;        // whether -f/--format was supplied
    bool no_header = false;        // parsed only to reject obsolete --no-header
    std::string model;             // reserved; model is always auto-detected
    std::string output;            // -o/--output; write destination .tsfile
    std::vector<WriteColumnSpec> columns;  // --tag/--field declarations
    bool verbose = false;          // -v/--verbose; write progress to stderr
    bool header_match = false;     // --header-match; validate write header row
    bool input_set = false;        // write input was explicitly set
    bool has_tag_filter = false;   // one or more --tag-filter was supplied
    std::vector<TagFilterSpec> tag_filters;
    std::string tag_match;  // empty, all, or any
    bool help = false;      // -h/--help requested
    bool version = false;   // --version requested
    Format export_format = Format::kAuto;  // --type for export
    bool export_format_set = false;        // whether --type was supplied
    bool force = false;                    // --force for export/sketch
    std::string output_dir;  // --output-dir for multi-object export
    std::vector<PhysicalOverride> physical_overrides;
    std::string error;  // non-empty if parsing failed (the message)
};

ParsedArgs parse_args(const std::vector<std::string>& args);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_CLI_ARGS_H
