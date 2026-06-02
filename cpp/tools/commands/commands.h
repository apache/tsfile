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

#ifndef TSFILE_CLI_COMMANDS_H
#define TSFILE_CLI_COMMANDS_H

#include <ostream>

#include "cli/cli_args.h"
#include "format/output_format.h"

namespace storage {
class TsFileReader;
}  // namespace storage

namespace tsfile_cli {

bool is_table_model(const ParsedArgs& args, storage::TsFileReader& reader);

int run_row_query(const ParsedArgs& args, storage::TsFileReader& reader,
                  OutputFormat fmt, std::ostream& out, std::ostream& err,
                  long long offset, long long limit);

int cmd_ls(const ParsedArgs& args, storage::TsFileReader& reader,
           OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_schema(const ParsedArgs& args, storage::TsFileReader& reader,
               OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_meta(const ParsedArgs& args, storage::TsFileReader& reader,
             OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_count(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_stats(const ParsedArgs& args, storage::TsFileReader& reader,
              OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_head(const ParsedArgs& args, storage::TsFileReader& reader,
             OutputFormat fmt, std::ostream& out, std::ostream& err);
int cmd_cat(const ParsedArgs& args, storage::TsFileReader& reader,
            OutputFormat fmt, std::ostream& out, std::ostream& err);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_COMMANDS_H
