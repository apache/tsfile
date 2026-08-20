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

#include <cstdio>
#include <set>
#include <sys/stat.h>
#include <sys/types.h>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/device_id.h"
#include "common/schema.h"
#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {
namespace {

bool path_exists(const std::string& path) {
    std::ifstream in(path.c_str(), std::ios::binary);
    return in.good();
}

bool directory_exists(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool any_path_exists(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

int create_directory_no_replace(const std::string& path, std::ostream& err) {
    if (any_path_exists(path)) {
        err << "Error: output directory '" << path
            << "' already exists\n";
        return kExitRuntime;
    }
    if (mkdir(path.c_str(), 0777) != 0) {
        err << "Error: cannot create output directory '" << path << "'\n";
        return kExitRuntime;
    }
    if (!directory_exists(path)) {
        err << "Error: output path '" << path << "' is not a directory\n";
        return kExitRuntime;
    }
    return kExitOk;
}

int write_atomic_text(const std::string& path, const std::string& content,
                      bool force, std::ostream& err) {
    if (!force && path_exists(path)) {
        err << "Error: output target '" << path
            << "' already exists; use --force to replace a regular file\n";
        return kExitRuntime;
    }
    const std::string tmp = path + ".tmp";
    {
        std::ofstream out(tmp.c_str(), std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            err << "Error: cannot create output target '" << path << "'\n";
            return kExitRuntime;
        }
        out << content;
        if (!out.good()) {
            err << "Error: failed to write output target '" << path << "'\n";
            out.close();
            std::remove(tmp.c_str());
            return kExitRuntime;
        }
    }
    if (std::rename(tmp.c_str(), path.c_str()) != 0) {
        err << "Error: failed to commit output target '" << path << "'\n";
        std::remove(tmp.c_str());
        return kExitRuntime;
    }
    return kExitOk;
}

std::string extension_for_format(ParsedArgs::Format fmt) {
    if (fmt == ParsedArgs::Format::kCsv) {
        return ".csv";
    }
    if (fmt == ParsedArgs::Format::kJson) {
        return ".ndjson";
    }
    return ".txt";
}

std::string type_name_for_format(ParsedArgs::Format fmt) {
    if (fmt == ParsedArgs::Format::kCsv) {
        return "csv";
    }
    if (fmt == ParsedArgs::Format::kJson) {
        return "ndjson";
    }
    return "table";
}

std::string numbered_file_name(size_t index, ParsedArgs::Format fmt) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%04zu", index + 1);
    return std::string(buf) + extension_for_format(fmt);
}

std::string json_escape(const std::string& s) {
    std::ostringstream out;
    for (char c : s) {
        switch (c) {
            case '\\':
                out << "\\\\";
                break;
            case '"':
                out << "\\\"";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                out << c;
        }
    }
    return out.str();
}

long long count_result_rows(const std::string& content, OutputFormat fmt) {
    long long lines = 0;
    for (char c : content) {
        if (c == '\n') {
            ++lines;
        }
    }
    if (fmt == OutputFormat::kJson) {
        return lines;
    }
    return lines > 0 ? lines - 1 : 0;
}

struct ManifestEntry {
    std::string file;
    std::string model;
    std::string object;
    std::string type;
    std::string rows;
};

std::string render_manifest(bool complete,
                            const std::vector<ManifestEntry>& entries) {
    std::ostringstream out;
    out << "{\n  \"complete\": " << (complete ? "true" : "false")
        << ",\n  \"files\": [\n";
    for (size_t i = 0; i < entries.size(); ++i) {
        const ManifestEntry& e = entries[i];
        out << "    {\"file\":\"" << json_escape(e.file)
            << "\",\"model\":\"" << json_escape(e.model)
            << "\",\"object\":\"" << json_escape(e.object)
            << "\",\"type\":\"" << json_escape(e.type)
            << "\",\"rows\":\"" << json_escape(e.rows) << "\"}";
        if (i + 1 != entries.size()) {
            out << ",";
        }
        out << "\n";
    }
    out << "  ]\n}\n";
    return out.str();
}

int write_manifest(const std::string& dir, bool complete,
                   const std::vector<ManifestEntry>& entries,
                   std::ostream& err) {
    return write_atomic_text(dir + "/_manifest.json",
                             render_manifest(complete, entries), true, err);
}

bool table_exists(storage::TsFileReader& reader, const std::string& table) {
    auto schemas = reader.get_all_table_schemas();
    for (const auto& schema : schemas) {
        if (schema && schema->get_table_name() == table) {
            return true;
        }
    }
    return false;
}

bool device_exists(storage::TsFileReader& reader, const std::string& device) {
    auto did = std::make_shared<storage::StringArrayDeviceID>(device);
    std::vector<storage::MeasurementSchema> schema;
    return reader.get_timeseries_schema(did, schema) == 0 && !schema.empty();
}

int validate_multi_export_objects(const ParsedArgs& args,
                                  storage::TsFileReader& reader,
                                  std::ostream& err) {
    std::set<std::string> seen;
    if (!args.tables.empty()) {
        for (const std::string& table : args.tables) {
            if (!seen.insert(table).second) {
                err << "Error: table '" << table
                    << "' was specified more than once\n";
                return kExitUsage;
            }
            if (!table_exists(reader, table)) {
                err << "Error: table '" << table << "' does not exist\n";
                return kExitUsage;
            }
        }
    } else {
        for (const std::string& device : args.devices) {
            if (!seen.insert(device).second) {
                err << "Error: device '" << device
                    << "' was specified more than once\n";
                return kExitUsage;
            }
            if (!device_exists(reader, device)) {
                err << "Error: device '" << device << "' does not exist\n";
                return kExitUsage;
            }
        }
    }
    return kExitOk;
}

}  // namespace

int cmd_export(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& /*out*/, std::ostream& err) {
    const bool multi_object = args.devices.size() + args.tables.size() > 1;
    if (multi_object) {
        int code = validate_multi_export_objects(args, reader, err);
        if (code != kExitOk) {
            return code;
        }
        code = create_directory_no_replace(args.output_dir, err);
        if (code != kExitOk) {
            return code;
        }

        std::vector<ManifestEntry> entries;
        code = write_manifest(args.output_dir, false, entries, err);
        if (code != kExitOk) {
            return code;
        }

        const std::vector<std::string>& objects =
            !args.tables.empty() ? args.tables : args.devices;
        const bool table_mode = !args.tables.empty();
        for (size_t i = 0; i < objects.size(); ++i) {
            ParsedArgs one = args;
            one.output = args.output_dir + "/" +
                         numbered_file_name(i, args.export_format);
            one.output_dir.clear();
            one.devices.clear();
            one.tables.clear();
            one.device.clear();
            one.table.clear();
            if (table_mode) {
                one.table = objects[i];
                one.tables.push_back(objects[i]);
            } else {
                one.device = objects[i];
                one.devices.push_back(objects[i]);
            }
            std::ostringstream content;
            one.command = "cat";
            code = run_row_query(one, reader, fmt, content, err, one.offset,
                                 one.limit);
            if (code != kExitOk) {
                return code;
            }
            code = write_atomic_text(one.output, content.str(), false, err);
            if (code != kExitOk) {
                return code;
            }
            entries.push_back({numbered_file_name(i, args.export_format),
                               table_mode ? "table" : "tree", objects[i],
                               type_name_for_format(args.export_format),
                               std::to_string(count_result_rows(content.str(),
                                                                fmt))});
            code = write_manifest(args.output_dir, false, entries, err);
            if (code != kExitOk) {
                return code;
            }
        }
        return write_manifest(args.output_dir, true, entries, err);
    }

    std::ostringstream content;
    ParsedArgs query = args;
    query.command = "cat";
    int code = run_row_query(query, reader, fmt, content, err, query.offset,
                             query.limit);
    if (code != kExitOk) {
        return code;
    }
    return write_atomic_text(args.output, content.str(), args.force, err);
}

int cmd_sketch(const ParsedArgs& args, storage::TsFileReader& reader,
               std::ostream& out, std::ostream& err) {
    std::ostringstream content;
    content << "-------------------------------- TsFile Sketch "
               "--------------------------------\n"
            << "file path: " << args.file << "\n"
            << "model: " << (is_table_model(args, reader) ? "table" : "tree")
            << "\n"
            << "---------------------------------- TsFile Sketch End "
               "----------------------------------\n";
    if (args.output.empty()) {
        out << content.str();
        return kExitOk;
    }
    return write_atomic_text(args.output, content.str(), args.force, err);
}

}  // namespace tsfile_cli
