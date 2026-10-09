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

#ifndef TSFILE_CLI_RESULT_SET_FORMAT_H
#define TSFILE_CLI_RESULT_SET_FORMAT_H

#include <ostream>
#include <string>

#include "common/db_common.h"
#include "format/output_format.h"
#include "reader/result_set.h"

namespace tsfile_cli {

std::string cell_to_string(storage::ResultSet* rs, uint32_t col_index,
                           common::TSDataType type);

int emit_result_set(storage::ResultSet* rs, OutputFormat fmt, bool no_header,
                    std::ostream& out, long long offset = 0,
                    long long limit = -1, long long* emitted_rows = nullptr);

int emit_result_set_sampled(storage::ResultSet* rs, OutputFormat fmt,
                            bool no_header, std::ostream& out, long long limit,
                            unsigned long long seed);

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_RESULT_SET_FORMAT_H
