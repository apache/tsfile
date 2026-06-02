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

#include "cli/run_cli.h"

#include <cstdio>
#include <set>

#include "cli/cli_args.h"
#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "format/output_format.h"
#include "reader/tsfile_reader.h"

#ifdef _WIN32
#include <io.h>
#define TSFILE_ISATTY _isatty
#define TSFILE_FILENO _fileno
#else
#include <unistd.h>
#define TSFILE_ISATTY isatty
#define TSFILE_FILENO fileno
#endif

#ifndef TSFILE_CLI_VERSION
#define TSFILE_CLI_VERSION "unknown"
#endif

namespace tsfile_cli {
namespace {

void print_usage(std::ostream& os) {
    os << "Usage: tsfile <command> [options] <file.tsfile>\n"
          "Commands:\n"
          "  ls       list devices (tree) or tables (table)\n"
          "  schema   per-measurement data type/encoding/compression\n"
          "  meta     file metadata summary\n"
          "  stats    per-series row count and time range\n"
          "  head     first N rows (use -n)\n"
          "  cat      all rows of a device/table\n"
          "  count    row count\n"
          "  sample   deterministic sample rows (use -n and --seed)\n"
          "Options: -f/--format csv|tsv|json|table, -d/--device, -t/--table,\n"
          "         -m/--measurements a,b, -n/--limit, --offset, --seed,\n"
          "         --start, --end,\n"
          "         --no-header, --model tree|table, -h/--help, --version\n";
}

bool is_known_command(const std::string& c) {
    static const std::set<std::string> kCmds = {
        "ls",   "schema", "meta",  "stats",
        "head", "cat",    "count", "sample"};
    return kCmds.count(c) != 0;
}

bool validate_command_flags(const ParsedArgs& p, std::ostream& err) {
    if (p.has_seed && p.command != "sample") {
        err << "Error: --seed is only valid for sample\n";
        return false;
    }
    if (p.command == "sample" && p.offset != 0) {
        err << "Error: --offset is not valid for sample\n";
        return false;
    }
    if (!p.device.empty() && !p.table.empty()) {
        err << "Error: --device and --table cannot be used together\n";
        return false;
    }
    if (p.limit < -1) {
        err << "Error: --limit must be >= -1\n";
        return false;
    }
    if (p.offset < 0) {
        err << "Error: --offset must be >= 0\n";
        return false;
    }
    if (p.has_start && p.has_end && p.start > p.end) {
        err << "Error: --start must be <= --end\n";
        return false;
    }
    return true;
}

}  // namespace

int run_cli(const std::vector<std::string>& args, std::ostream& out,
            std::ostream& err) {
    ParsedArgs p = parse_args(args);

    if (p.version) {
        out << "tsfile (Apache TsFile C++) " << TSFILE_CLI_VERSION << "\n";
        return kExitOk;
    }
    if (args.empty()) {
        print_usage(err);
        return kExitUsage;
    }
    if (p.command == "help" || p.command == "--help" || p.command == "-h" ||
        (p.help && p.file.empty())) {
        print_usage(out);
        return kExitOk;
    }
    if (!p.error.empty()) {
        err << "Error: " << p.error << "\n";
        print_usage(err);
        return kExitUsage;
    }
    if (!is_known_command(p.command)) {
        err << "Unknown command: " << p.command << "\n";
        print_usage(err);
        return kExitUsage;
    }
    if (p.file.empty()) {
        err << "Error: missing <file.tsfile> argument\n";
        return kExitUsage;
    }
    if (!validate_command_flags(p, err)) {
        print_usage(err);
        return kExitUsage;
    }

    storage::libtsfile_init();
    storage::TsFileReader reader;
    int open_ret = reader.open(p.file);
    if (open_ret != 0) {
        err << "Error: cannot open or corrupted file: " << p.file << "\n";
        return kExitFile;
    }

    bool stdout_tty = TSFILE_ISATTY(TSFILE_FILENO(stdout)) != 0;
    OutputFormat fmt = resolve_format(p.format, stdout_tty);

    int code;
    if (p.command == "ls") {
        code = cmd_ls(p, reader, fmt, out, err);
    } else if (p.command == "schema") {
        code = cmd_schema(p, reader, fmt, out, err);
    } else if (p.command == "meta") {
        code = cmd_meta(p, reader, fmt, out, err);
    } else if (p.command == "stats") {
        code = cmd_stats(p, reader, fmt, out, err);
    } else if (p.command == "head") {
        code = cmd_head(p, reader, fmt, out, err);
    } else if (p.command == "cat") {
        code = cmd_cat(p, reader, fmt, out, err);
    } else if (p.command == "count") {
        code = cmd_count(p, reader, fmt, out, err);
    } else if (p.command == "sample") {
        code = cmd_sample(p, reader, fmt, out, err);
    } else {
        err << "Unknown command: " << p.command << "\n";
        code = kExitUsage;
    }

    reader.close();
    return code;
}

}  // namespace tsfile_cli
