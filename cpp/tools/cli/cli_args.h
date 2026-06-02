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

namespace tsfile_cli {

struct ParsedArgs {
    enum class Format { kAuto, kCsv, kTsv, kJson, kTable };

    std::string command;
    std::string file;
    std::string device;
    std::string table;
    std::vector<std::string> measurements;
    long long limit = -1;
    long long offset = 0;
    long long start = LLONG_MIN;
    long long end = LLONG_MAX;
    bool has_start = false;
    bool has_end = false;
    long long seed = 0;
    bool has_seed = false;
    Format format = Format::kAuto;
    bool no_header = false;
    std::string model;
    bool help = false;
    bool version = false;
    std::string error;
};

ParsedArgs parse_args(const std::vector<std::string>& args);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_CLI_ARGS_H
