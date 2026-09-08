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

#ifndef TSFILE_CLI_OUTPUT_FORMAT_H
#define TSFILE_CLI_OUTPUT_FORMAT_H

#include <cstdio>
#include <ostream>
#include <string>
#include <vector>

#include "cli/cli_args.h"
#include "common/db_common.h"

namespace tsfile_cli {

enum class OutputFormat { kCsv, kTsv, kJson, kTable };

OutputFormat resolve_format(ParsedArgs::Format f, bool stdout_is_tty);

// Translate a storage-engine error code (common::E_*) into a human-readable
// phrase so CLI diagnostics carry meaning instead of a bare numeric code.
const char* error_code_message(int code);

const char* tsdatatype_name(common::TSDataType t);
const char* tsencoding_name(common::TSEncoding e);
const char* compression_name(common::CompressionType c);

std::string csv_escape(const std::string& field);
std::string json_escape(const std::string& s);

class RowWriter {
   public:
    RowWriter(std::ostream& out, OutputFormat fmt,
              std::vector<std::string> header,
              std::vector<common::TSDataType> types, bool no_header);
    ~RowWriter();

    bool write(const std::vector<std::string>& cells,
               const std::vector<bool>& is_null);
    bool write(const std::vector<std::string>& cells,
               const std::vector<bool>& is_null,
               const std::vector<common::TSDataType>& row_types);
    bool finish();

   private:
    bool ensure_header();
    bool emits_json_bare(common::TSDataType type) const;
    std::string format_cell(common::TSDataType type,
                            const std::string& cell) const;

    std::ostream& out_;
    OutputFormat fmt_;
    std::vector<std::string> header_;
    std::vector<common::TSDataType> types_;
    bool no_header_;
    bool header_done_ = false;
    std::FILE* spool_ = nullptr;
    std::vector<size_t> table_widths_;
};

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_OUTPUT_FORMAT_H
