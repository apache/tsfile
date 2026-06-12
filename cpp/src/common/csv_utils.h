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

#ifndef COMMON_CSV_UTILS_H
#define COMMON_CSV_UTILS_H

#include <string>
#include <vector>

namespace common {

// Split a single logical line on `delim`. When `csv_quotes` is true the
// splitter is RFC 4180 aware: a field may be wrapped in double quotes, the
// delimiter is literal inside quotes, and a doubled quote ("") inside a quoted
// field is an escaped quote. When `csv_quotes` is false a quote is treated as
// ordinary data (suitable for TSV).
std::vector<std::string> split_csv_line(const std::string& line, char delim,
                                        bool csv_quotes);

// Quote/escape a field for RFC 4180 CSV output: wrap in double quotes and
// double any embedded quote, but only when the field contains a delimiter,
// quote, or newline. The delimiter that triggers quoting is `delim` (default
// comma).
std::string csv_escape(const std::string& field, char delim = ',');

}  // namespace common

#endif  // COMMON_CSV_UTILS_H
