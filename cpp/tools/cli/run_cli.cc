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
#ifndef TSFILE_CLI_COMMIT
#define TSFILE_CLI_COMMIT "unknown"
#endif
#ifndef TSFILE_CLI_BUILT
#define TSFILE_CLI_BUILT "unknown"
#endif

namespace tsfile_cli {
namespace {

void print_usage(std::ostream& os) {
    os << "Usage: tsfile-cli <command> [options]\n"
          "Commands: ls schema meta stats count sketch head cat export write\n"
          "Formats: table ndjson csv\n"
          "Common read options:\n"
          "  -f, --format table|ndjson|csv  output format (default: table)\n"
          "  -d, --device <name>            select one tree-model device\n"
          "  -t, --table <name>             select one table-model table\n"
          "  -m, --measurements <name>      repeat for FIELD projection\n"
          "  -n, --limit N                  max rows for head/cat\n"
          "      --offset N                 skip N matching rows\n"
          "      --start <int64>            inclusive lower time bound\n"
          "      --end <int64>              inclusive upper time bound\n"
          "Export options:\n"
          "  -o, --output <file>            single-object export target\n"
          "      --type table|ndjson|csv    export file type\n"
          "      --force                    replace a regular output file\n"
          "Write options:\n"
          "      --table <name>             target table name\n"
          "      --tag <name> STRING        declare a TAG column\n"
          "      --field <name> <type>      declare a FIELD column\n"
          "  -i, --input <file.csv>         input CSV file\n"
          "      --stdin                    read CSV from stdin\n"
          "  -o, --output <file>            destination .tsfile\n"
          "  -v, --verbose                  report write details to stderr\n"
          "  -h, --help                     print help\n"
          "      --version                  print version\n";
}

void print_command_usage(const std::string& command, std::ostream& os) {
    if (command == "ls") {
        os << "Usage: tsfile-cli ls [-f|--format table|ndjson|csv] "
              "<file.tsfile>\n"
              "Lists every object in the selected tree or table model.\n"
              "Result fields: model,object\n"
              "Default: --format table\n"
              "Examples:\n"
              "  tsfile-cli ls data.tsfile\n"
              "  tsfile-cli ls -f ndjson data.tsfile\n";
    } else if (command == "schema") {
        os << "Usage: tsfile-cli schema [-d|--device <device> | "
              "-t|--table <table>] [-m|--measurements <column>]... "
              "[-f|--format table|ndjson|csv] <file.tsfile>\n"
              "Shows TIME/TAG/ATTRIBUTE/FIELD structure and physical "
              "settings.\n"
              "Result fields: model,object,column,category,data_type,encoding,"
              "compression\n"
              "Default: --format table; omitted scope visits every object in "
              "file order.\n"
              "Examples:\n"
              "  tsfile-cli schema -t sensors -f csv data.tsfile\n"
              "  tsfile-cli schema -d root.sg.d1 -m temperature data.tsfile\n";
    } else if (command == "meta") {
        os << "Usage: tsfile-cli meta [-f|--format table|ndjson|csv] "
              "<file.tsfile>\n"
              "Shows file-level metadata and the selected data model.\n"
              "Result fields: size_bytes,format_version,model\n"
              "Default: --format table\n"
              "Examples:\n"
              "  tsfile-cli meta data.tsfile\n"
              "  tsfile-cli meta -f ndjson data.tsfile\n";
    } else if (command == "stats") {
        os << "Usage: tsfile-cli stats [-d|--device <device> | "
              "-t|--table <table>] [-m|--measurements <field>]... "
              "[-f|--format table|ndjson|csv] <file.tsfile>\n"
              "Shows FIELD value statistics and null counts.\n"
              "Result fields: tree uses model,object,field,data_type,"
              "non_null_count,null_count,min_time,max_time,min,max,first,last,"
              "sum,stats_source; table inserts tag.<name> fields after "
              "object.\n"
              "Default: --format table; omitted scope visits every object in "
              "file order.\n"
              "Examples:\n"
              "  tsfile-cli stats -t sensors -m temperature -f csv "
              "data.tsfile\n";
    } else if (command == "count") {
        os << "Usage: tsfile-cli count [-d|--device <device> | "
              "-t|--table <table>] [-m|--measurements <column>]... "
              "[-f|--format table|ndjson|csv] <file.tsfile>\n"
              "Shows logical row, entity, and column counts.\n"
              "Result fields: model,object,column,category,row_count,"
              "entity_count,non_null_count,null_count,min_time,max_time,"
              "time_source\n"
              "Default: --format table; omitted scope visits every object in "
              "file order.\n"
              "Examples:\n"
              "  tsfile-cli count -t sensors -m site -f csv data.tsfile\n";
    } else if (command == "sketch") {
        os << "Usage: tsfile-cli sketch [-o|--output <file>] [--force] "
              "<file.tsfile>\n"
              "Writes the physical layout text from the bound printSketch "
              "behavior.\n"
              "Result fields: printSketch text; --format is not supported.\n"
              "Default: stdout; --force requires --output and replaces only a "
              "regular file.\n"
              "Examples:\n"
              "  tsfile-cli sketch data.tsfile\n"
              "  tsfile-cli sketch -o layout.txt data.tsfile\n";
    } else if (command == "head") {
        os << "Usage: tsfile-cli head [-d|--device <device> | "
              "-t|--table <table>] [-m|--measurements <field>]... "
              "[--start <int64>] [--end <int64>] [--offset N] [-n|--limit N] "
              "[--tag-filter <tag> <op> [value]] [--tag-match all|any] "
              "[-f|--format table|ndjson|csv] <file.tsfile>\n"
              "Reads the first matching rows from one object.\n"
              "Result fields: time, all table TAG columns, selected FIELD "
              "columns.\n"
              "Default: --limit 10, --offset 0, --format table.\n"
              "Examples:\n"
              "  tsfile-cli head -t sensors -m temperature -n 5 data.tsfile\n";
    } else if (command == "cat") {
        os << "Usage: tsfile-cli cat [-d|--device <device> | "
              "-t|--table <table>] [-m|--measurements <field>]... "
              "[--start <int64>] [--end <int64>] [--offset N] [-n|--limit N] "
              "[--tag-filter <tag> <op> [value]] [--tag-match all|any] "
              "[-f|--format table|ndjson|csv] <file.tsfile>\n"
              "Reads all matching rows from one object unless --limit is set.\n"
              "Result fields: time, all table TAG columns, selected FIELD "
              "columns.\n"
              "Default: no row limit, --offset 0, --format table.\n"
              "Examples:\n"
              "  tsfile-cli cat -t sensors --tag-filter site eq a -f ndjson "
              "data.tsfile\n";
    } else if (command == "export") {
        os << "Usage: tsfile-cli export (-d|--device <device> | "
              "-t|--table <table>) -o|--output <file> "
              "--type table|ndjson|csv [query options] [--force] "
              "<file.tsfile>\n"
              "       tsfile-cli export (-d <device>... | -t <table>...) "
              "--output-dir <dir> --type table|ndjson|csv [query options] "
              "<file.tsfile>\n"
              "Writes one object atomically, or a numbered multi-object "
              "directory with _manifest.json.\n"
              "Result fields: same bytes as cat for the same scope and type; "
              "multi-object manifest records file,model,object,type,rows.\n"
              "Default: no row limit, --offset 0; --type is required.\n"
              "Examples:\n"
              "  tsfile-cli export -t sensors --type csv -o sensors.csv "
              "data.tsfile\n";
    } else if (command == "write") {
        os << "Usage: tsfile-cli write --table <name> "
              "(--tag <name> STRING)* (--field <name> <type>)+ "
              "[--encoding <type> <encoding>] [--compression <type> "
              "<compression>] (-i|--input <input.csv> | --stdin) "
              "-o|--output <out.tsfile> [-v|--verbose]\n"
              "Creates a new single-table, table-model TsFile from strict "
              "CSV.\n"
              "Result fields: none on stdout; -v writes a post-commit summary "
              "and resolved column physical settings to stderr.\n"
              "Default: success is silent; target must not exist; CSV header "
              "must contain time and declared TAG/FIELD names.\n"
              "Examples:\n"
              "  tsfile-cli write --table sensors --tag site STRING "
              "--field temperature DOUBLE -i input.csv -o out.tsfile\n";
    } else {
        print_usage(os);
    }
}

bool has_mixed_model_metadata(storage::TsFileReader& reader) {
    const auto schemas = reader.get_all_table_schemas();
    if (schemas.empty()) {
        return false;
    }

    std::set<std::string> table_names;
    for (const auto& schema : schemas) {
        if (schema != nullptr) {
            table_names.insert(storage::to_lower(schema->get_table_name()));
        }
    }
    for (const auto& device : reader.get_all_device_ids()) {
        if (device == nullptr) {
            continue;
        }
        // In a pure table file every device ID belongs to one of the table
        // schemas.  A tree device can coexist in the same footer only when a
        // file was assembled from both model kinds, which is unsupported by
        // the CLI and must be reported as an input error.
        if (table_names.find(storage::to_lower(device->get_table_name())) ==
            table_names.end()) {
            return true;
        }
    }
    return false;
}

bool is_known_command(const std::string& c) {
    static const std::set<std::string> kCmds = {
        "ls",     "schema", "meta", "stats",  "count",
        "sketch", "head",   "cat",  "export", "write"};
    return kCmds.find(c) != kCmds.end();
}

bool validate_command_flags(const ParsedArgs& p, std::ostream& err) {
    if (p.has_seed) {
        err << "Error: --seed is not supported by tsfile-cli\n";
        return false;
    }
    if (!p.devices.empty() && !p.tables.empty()) {
        err << "Error: -d/--device and -t/--table cannot be used together\n";
        return false;
    }
    if (p.limit < -1) {
        err << "Error: -n/--limit must be >= -1\n";
        return false;
    }
    if (p.offset < 0) {
        err << "Error: --offset must be >= 0\n";
        return false;
    }
    if ((p.command == "head" || p.command == "cat" || p.command == "export") &&
        p.limit == 0 && p.offset > 0) {
        err << "Error: --offset requires a positive --limit\n";
        return false;
    }
    if (p.has_start && p.has_end && p.start > p.end) {
        err << "Error: --start must be <= --end\n";
        return false;
    }
    return true;
}

bool validate_write_flags(const ParsedArgs& p, std::ostream& err) {
    if (p.tables.size() > 1) {
        err << "Error: --table specified more than once\n";
        return false;
    }
    if (p.table.empty()) {
        err << "Error: write requires -t/--table\n";
        return false;
    }
    if (p.columns.empty()) {
        err << "Error: write requires at least one --field column\n";
        return false;
    }
    if (p.output.empty()) {
        err << "Error: write requires -o/--output\n";
        return false;
    }
    if (p.format_set) {
        err << "Error: write input format is fixed CSV; --format is not "
               "valid\n";
        return false;
    }
    if (!p.input_set) {
        err << "Error: choose exactly one of --input or --stdin\n";
        return false;
    }
    if (p.has_tag_filter) {
        err << "Error: tag filter flags are not valid for write\n";
        return false;
    }
    if (!p.tag_match.empty()) {
        err << "Error: --tag-match is not valid for write\n";
        return false;
    }
    // Name the offending flag so the user does not have to guess which of
    // the read-only options triggered the rejection.
    if (!p.measurements.empty()) {
        err << "Error: -m/--measurements is not valid for write\n";
        return false;
    }
    if (!p.device.empty()) {
        err << "Error: -d/--device is not valid for write\n";
        return false;
    }
    if (p.has_start || p.has_end) {
        err << "Error: --start/--end are not valid for write\n";
        return false;
    }
    if (p.no_header || p.header_match) {
        err << "Error: --no-header/--header-match are not valid for write\n";
        return false;
    }
    if (p.has_seed) {
        err << "Error: --seed is not valid for write\n";
        return false;
    }
    if (p.limit != -1) {
        err << "Error: -n/--limit is not valid for write\n";
        return false;
    }
    if (p.offset != 0) {
        err << "Error: --offset is not valid for write\n";
        return false;
    }
    if (!p.model.empty()) {
        err << "Error: --model is not valid for write\n";
        return false;
    }
    return true;
}

bool validate_export_flags(const ParsedArgs& p, std::ostream& err) {
    if (!p.export_format_set) {
        err << "Error: export requires --type table|ndjson|csv\n";
        return false;
    }
    if (p.format_set) {
        err << "Error: export uses --type, not --format\n";
        return false;
    }
    if (p.no_header) {
        err << "Error: --no-header is not supported\n";
        return false;
    }
    if (p.has_tag_filter && !p.devices.empty()) {
        err << "Error: tag filter flags are only valid for table export\n";
        return false;
    }
    if (!p.tag_match.empty()) {
        if (p.tag_filters.size() == 0) {
            err << "Error: --tag-match requires tag filters\n";
            return false;
        }
        if (p.tag_filters.size() == 1) {
            err << "Error: --tag-match requires at least two tag filters\n";
            return false;
        }
    }
    if (p.tag_filters.size() >= 2 && p.tag_match.empty()) {
        err << "Error: two or more tag filters require --tag-match all or "
               "any\n";
        return false;
    }
    const size_t scope_count = p.devices.size() + p.tables.size();
    if (scope_count == 0) {
        err << "Error: export requires -d/--device or -t/--table\n";
        return false;
    }
    if (scope_count == 1) {
        if (p.output.empty()) {
            err << "Error: export requires -o/--output\n";
            return false;
        }
        if (!p.output_dir.empty()) {
            err << "Error: --output-dir is only valid for multi-object "
                   "export\n";
            return false;
        }
        return true;
    }
    if (!p.output.empty()) {
        err << "Error: -o/--output is only valid for single-object export\n";
        return false;
    }
    if (p.output_dir.empty()) {
        err << "Error: multi-object export requires --output-dir\n";
        return false;
    }
    if (p.force) {
        err << "Error: --force is only valid for single-object export\n";
        return false;
    }
    return true;
}

// Reject flags that have no effect for the given read command, instead of
// silently ignoring them, so misuse is caught rather than producing surprising
// output. Only called for non-write commands; write has its own validation.
bool validate_read_flag_applicability(const ParsedArgs& p, std::ostream& err) {
    const std::string& c = p.command;
    const bool is_row = (c == "head" || c == "cat");
    const bool scoped = is_row || c == "schema" || c == "stats" || c == "count";

    if (c == "sketch") {
        if (p.format_set) {
            err << "Error: sketch does not accept --format; its output follows "
                   "printSketch\n";
            return false;
        }
        if (p.force && p.output.empty()) {
            err << "Error: --force requires --output\n";
            return false;
        }
        if (!p.device.empty() || !p.table.empty() || !p.measurements.empty() ||
            p.limit != -1 || p.offset != 0 || p.has_start || p.has_end ||
            p.has_tag_filter) {
            err << "Error: sketch does not accept scope or query options\n";
            return false;
        }
        return true;
    }

    if (p.no_header) {
        err << "Error: --no-header is not supported\n";
        return false;
    }
    if (!p.output.empty() || !p.output_dir.empty() || p.force ||
        p.export_format_set) {
        err << "Error: -o/--output is only valid for write\n";
        return false;
    }
    if (!p.columns.empty()) {
        err << "Error: --tag/--field are only valid for write\n";
        return false;
    }
    if (p.header_match) {
        err << "Error: --header-match is only valid for write\n";
        return false;
    }
    if (p.verbose) {
        err << "Error: -v/--verbose is only valid for write\n";
        return false;
    }
    if (!is_row && p.limit != -1) {
        err << "Error: -n/--limit is only valid for head/cat\n";
        return false;
    }
    if (!is_row && p.offset != 0) {
        err << "Error: --offset is only valid for head/cat\n";
        return false;
    }
    if (!is_row && (p.has_start || p.has_end)) {
        err << "Error: --start/--end are only valid for head/cat\n";
        return false;
    }
    if (p.has_tag_filter && !is_row) {
        err << "Error: tag filter flags are only valid for head/cat\n";
        return false;
    }
    if (!p.tag_match.empty() && !is_row) {
        err << "Error: --tag-match is only valid for head/cat\n";
        return false;
    }
    if (!p.tag_match.empty()) {
        if (p.tag_filters.size() == 0) {
            err << "Error: --tag-match requires tag filters\n";
            return false;
        }
        if (p.tag_filters.size() == 1) {
            err << "Error: --tag-match requires at least two tag filters\n";
            return false;
        }
    }
    if (p.tag_filters.size() >= 2 && p.tag_match.empty()) {
        err << "Error: two or more tag filters require --tag-match all or "
               "any\n";
        return false;
    }
    if (p.has_tag_filter && p.model == "tree") {
        err << "Error: tag filter flags are only valid for table model\n";
        return false;
    }
    if (p.has_tag_filter && !p.device.empty()) {
        err << "Error: tag filter flags cannot be combined with -d/--device\n";
        return false;
    }
    if (!scoped && !p.device.empty()) {
        err << "Error: -d/--device is not valid for " << c << "\n";
        return false;
    }
    if (!scoped && !p.table.empty()) {
        err << "Error: -t/--table is not valid for " << c << "\n";
        return false;
    }
    if (!scoped && !p.measurements.empty()) {
        err << "Error: -m/--measurements is not valid for " << c << "\n";
        return false;
    }
    if (c != "export" && (p.devices.size() > 1 || p.tables.size() > 1)) {
        err << "Error: scope option specified more than once\n";
        return false;
    }
    return true;
}

}  // namespace

int run_cli(const std::vector<std::string>& args, std::ostream& out,
            std::ostream& err) {
    ParsedArgs p = parse_args(args);

    if (args.empty()) {
        print_usage(err);
        return kExitUsage;
    }
    if (!p.error.empty()) {
        err << "Error: " << p.error << "\n";
        print_usage(err);
        return kExitUsage;
    }
    if (p.command == "--version") {
        if (args.size() != 1) {
            err << "Error: --version must appear by itself\n";
            print_usage(err);
            return kExitUsage;
        }
        out << "tsfile-cli " << TSFILE_CLI_VERSION
            << " tsfile=" << TSFILE_CLI_VERSION
            << " commit=" << TSFILE_CLI_COMMIT << " built=" << TSFILE_CLI_BUILT
            << "\n";
        return kExitOk;
    }
    if (p.command == "help" || p.command == "--help" || p.command == "-h") {
        if (args.size() != 1) {
            err << "Error: " << p.command << " must appear by itself\n";
            print_usage(err);
            return kExitUsage;
        }
        print_usage(out);
        return kExitOk;
    }
    if (!is_known_command(p.command)) {
        err << "Unknown command: " << p.command << "\n";
        print_usage(err);
        return kExitUsage;
    }
    if (p.help) {
        print_command_usage(p.command, out);
        return kExitOk;
    }
    if (p.command != "write" && p.file.empty()) {
        err << "Error: missing <file.tsfile> argument\n";
        return kExitUsage;
    }
    if (!validate_command_flags(p, err)) {
        print_usage(err);
        return kExitUsage;
    }

    if (p.command == "write") {
        if (!validate_write_flags(p, err)) {
            print_usage(err);
            return kExitUsage;
        }
        storage::libtsfile_init();
        return cmd_write(p, out, err);
    }

    if (p.command == "export") {
        if (!validate_export_flags(p, err)) {
            print_usage(err);
            return kExitUsage;
        }
    }

    if (p.command != "export" && !validate_read_flag_applicability(p, err)) {
        print_usage(err);
        return kExitUsage;
    }

    storage::libtsfile_init();
    storage::TsFileReader reader;
    int open_ret = reader.open(p.file);
    if (open_ret != 0) {
        err << "Error: cannot open " << p.file << ": "
            << error_code_message(open_ret) << " (code " << open_ret << ")\n";
        return kExitFile;
    }

    if (has_mixed_model_metadata(reader)) {
        err << "Error: input TsFile must be a pure tree-model or pure "
               "table-model file\n";
        reader.close();
        return kExitFile;
    }

    // head/cat/export/schema dispatch on the data model and would silently
    // ignore the scope flag of the other model; reject that instead.
    if (p.command == "head" || p.command == "cat" || p.command == "export" ||
        p.command == "schema") {
        const bool table_model = is_table_model(p, reader);
        if (table_model && !p.device.empty()) {
            err << "Error: -d/--device does not apply to the table model; "
                   "use -t/--table (or force --model tree)\n";
            reader.close();
            return kExitUsage;
        }
        if (!table_model && !p.table.empty()) {
            err << "Error: -t/--table does not apply to the tree model; "
                   "use -d/--device (or force --model table)\n";
            reader.close();
            return kExitUsage;
        }
    }

    bool stdout_tty = TSFILE_ISATTY(TSFILE_FILENO(stdout)) != 0;
    OutputFormat fmt = p.command == "export"
                           ? resolve_format(p.export_format, stdout_tty)
                           : resolve_format(p.format, stdout_tty);

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
    } else if (p.command == "export") {
        code = cmd_export(p, reader, fmt, out, err);
    } else if (p.command == "sketch") {
        code = cmd_sketch(p, reader, out, err);
    } else {
        err << "Unknown command: " << p.command << "\n";
        code = kExitUsage;
    }

    reader.close();
    return code;
}

}  // namespace tsfile_cli
