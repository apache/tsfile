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

#ifndef TSFILE_CLI_STATISTICS_H
#define TSFILE_CLI_STATISTICS_H

#include <string>
#include <vector>

#include "cli/cli_args.h"

namespace storage {
class Statistic;
class TsFileReader;
}  // namespace storage

namespace tsfile_cli {

struct StatisticCells {
    std::vector<std::string> values;
    std::vector<bool> is_null;
};

struct SeriesStatRow {
    std::string target;
    std::string measurement;
    long long count = 0;
    long long start_time = 0;
    long long end_time = 0;
    StatisticCells value_cells;
};

struct FileSummary {
    std::string file;
    std::string model;
    long long device_count = 0;
    long long table_count = 0;
    long long series_count = 0;
    long long start_time = 0;
    long long end_time = 0;
    bool has_time_range = false;
    long long file_size_bytes = 0;
};

StatisticCells statistic_value_cells(storage::Statistic* st);
std::vector<SeriesStatRow> collect_series_stats(const ParsedArgs& args,
                                                storage::TsFileReader& reader);
FileSummary collect_file_summary(const ParsedArgs& args,
                                 storage::TsFileReader& reader);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_STATISTICS_H
