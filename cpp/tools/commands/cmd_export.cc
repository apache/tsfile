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

#include <sys/stat.h>
#include <sys/types.h>

#include <cstdio>
#include <set>
#ifdef _WIN32
#include <direct.h>
#define lstat stat
#endif
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "cli/exit_codes.h"
#include "commands/commands.h"
#include "common/device_id.h"
#include "common/schema.h"
#include "format/atomic_output.h"
#include "reader/tsfile_reader.h"

namespace tsfile_cli {
namespace {

bool stat_is_directory(const struct stat& st) {
#ifdef _WIN32
    return (st.st_mode & S_IFDIR) != 0;
#else
    return S_ISDIR(st.st_mode);
#endif
}

bool directory_exists(const std::string& path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && stat_is_directory(st);
}

int make_directory(const std::string& path) {
#ifdef _WIN32
    return _mkdir(path.c_str());
#else
    return mkdir(path.c_str(), 0777);
#endif
}

int create_directory_no_replace(const std::string& path, std::ostream& err) {
    struct stat existing;
    if (lstat(path.c_str(), &existing) == 0) {
        err << "Error: output directory '" << path << "' already exists\n";
        return kExitRuntime;
    }
    if (make_directory(path) != 0) {
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
                      const std::string& source, bool force,
                      std::ostream& err) {
    std::string tmp;
    int code = prepare_atomic_output(path, source, force, tmp, err);
    if (code != kExitOk) {
        return code;
    }
    {
        std::ofstream out(tmp.c_str(), std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            err << "Error: cannot create output target '" << path << "'\n";
            remove_atomic_temp(tmp, err);
            return kExitRuntime;
        }
        out << content;
        out.flush();
        if (!out.good()) {
            err << "Error: failed to write output target '" << path << "'\n";
            out.close();
            remove_atomic_temp(tmp, err);
            return kExitRuntime;
        }
        out.close();
        if (out.fail()) {
            err << "Error: failed to close output target '" << path << "'\n";
            remove_atomic_temp(tmp, err);
            return kExitRuntime;
        }
    }
    code = commit_atomic_output(tmp, path, force, err);
    if (code != kExitOk && !remove_atomic_temp(tmp, err)) {
        code = kExitRuntime;
    }
    return code;
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
        out << "    {\"file\":\"" << json_escape(e.file) << "\",\"model\":\""
            << json_escape(e.model) << "\",\"object\":\""
            << json_escape(e.object) << "\",\"type\":\"" << json_escape(e.type)
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
                             render_manifest(complete, entries), "", true, err);
}

int stream_query_to_file(const ParsedArgs& args, storage::TsFileReader& reader,
                         OutputFormat fmt, const std::string& path, bool force,
                         long long& rows, std::ostream& err) {
    std::string temp;
    int code = prepare_atomic_output(path, args.file, force, temp, err);
    if (code != kExitOk) {
        return code;
    }
    std::ofstream output(temp.c_str(), std::ios::binary | std::ios::trunc);
    if (!output.is_open()) {
        err << "Error: cannot open temporary output for '" << path << "'\n";
        remove_atomic_temp(temp, err);
        return kExitRuntime;
    }
    code = run_row_query(args, reader, fmt, output, err, args.offset,
                         args.limit, &rows);
    output.flush();
    if (code == kExitOk && !output.good()) {
        err << "Error: failed to flush output target '" << path << "'\n";
        code = kExitRuntime;
    }
    output.close();
    if (code == kExitOk && output.fail()) {
        err << "Error: failed to close output target '" << path << "'\n";
        code = kExitRuntime;
    }
    if (code == kExitOk) {
        code = commit_atomic_output(temp, path, force, err);
    }
    if (code != kExitOk && !remove_atomic_temp(temp, err)) {
        code = kExitRuntime;
    }
    return code;
}

bool table_exists(storage::TsFileReader& reader, const std::string& table) {
    const std::string canonical = storage::to_lower(table);
    auto schemas = reader.get_all_table_schemas();
    for (const auto& schema : schemas) {
        if (schema && schema->get_table_name() == canonical) {
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
            const std::string canonical = storage::to_lower(table);
            if (!seen.insert(canonical).second) {
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
                one.table = storage::to_lower(objects[i]);
                one.tables.push_back(one.table);
            } else {
                one.device = objects[i];
                one.devices.push_back(objects[i]);
            }
            one.command = "cat";
            long long rows = 0;
            code = stream_query_to_file(one, reader, fmt, one.output, false,
                                        rows, err);
            if (code != kExitOk) {
                return code;
            }
            entries.push_back({numbered_file_name(i, args.export_format),
                               table_mode ? "table" : "tree",
                               table_mode ? one.table : objects[i],
                               type_name_for_format(args.export_format),
                               std::to_string(rows)});
            code = write_manifest(args.output_dir, false, entries, err);
            if (code != kExitOk) {
                if (std::remove(one.output.c_str()) != 0) {
                    err << "Error: failed to clean unrecorded output '"
                        << one.output << "'\n";
                    return kExitRuntime;
                }
                return code;
            }
        }
        return write_manifest(args.output_dir, true, entries, err);
    }

    ParsedArgs query = args;
    query.command = "cat";
    long long rows = 0;
    return stream_query_to_file(query, reader, fmt, args.output, args.force,
                                rows, err);
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
        out.flush();
        return out.good() ? kExitOk : kExitRuntime;
    }
    return write_atomic_text(args.output, content.str(), args.file, args.force,
                             err);
}

}  // namespace tsfile_cli
