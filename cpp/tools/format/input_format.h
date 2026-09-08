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

#ifndef TSFILE_CLI_INPUT_FORMAT_H
#define TSFILE_CLI_INPUT_FORMAT_H

#include <istream>
#include <string>
#include <vector>

#include "cli/write_columns.h"
#include "common/db_common.h"
#include "utils/db_utils.h"

namespace tsfile_cli {

struct ColumnDef {
    std::string name;
    common::TSDataType type;
    common::ColumnCategory category;
};

bool parse_datatype_name(const std::string& s, common::TSDataType& out);
bool normalize_write_columns(const std::vector<WriteColumnSpec>& specs,
                             std::vector<ColumnDef>& out,
                             std::string& error);
std::vector<std::string> split_line(const std::string& line, char delim,
                                    bool csv_quotes);
bool parse_bool_cell(const std::string& s, bool& out);
bool is_valid_utf8(const std::string& s);
bool contains_utf8_bom(const std::string& s);
bool validate_identifier(const std::string& name, std::string& error);

// Read one logical record from `in`. When `csv_quotes` is true a field may span
// multiple physical lines if it opens a double-quote that is not yet closed, so
// continuation lines are appended (newline preserved) until the quote closes.
// `lines_consumed` returns how many physical lines were read, so callers can
// keep accurate line numbers. Returns false at end of input with no record.
bool read_record(std::istream& in, bool csv_quotes, std::string& record,
                 long long& lines_consumed);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_INPUT_FORMAT_H
