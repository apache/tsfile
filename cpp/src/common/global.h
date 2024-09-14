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
#ifndef COMMON_GLOBAL_H
#define COMMON_GLOBAL_H

#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "common/config/config.h"

namespace common {

extern ConfigValue g_config_value_;
extern ColumnDesc g_time_column_desc;
extern int init_common();
extern bool is_timestamp_column_name(const char *time_col_name);
extern void cols_to_json(ByteStream *byte_stream,
                         std::vector<common::ColumnDesc> &ret_ts_list);
extern void print_backtrace();

}  // namespace common

#endif  // COMMON_GLOBAL_H
