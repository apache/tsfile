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
#ifndef COMMON_CONFIG_CONFIG_H
#define COMMON_CONFIG_CONFIG_H

#include <cstdint>

#include "utils/db_utils.h"

namespace common {
enum ConfigLevel {
    INIT,     // Unchangeable, initialized during database init
    RESTART,  // Can be changed, but the database must be restarted to take
              // effect
    USERSET   // Session level update
};

typedef struct ConfigValue {
    uint32_t
        tsblock_mem_inc_step_size_;  // tsblock memory self-increment step size
    uint32_t tsblock_max_memory_;    // the maximum memory of a single tsblock
    uint32_t page_writer_max_point_num_;
    uint32_t page_writer_max_memory_bytes_;
    uint32_t max_degree_of_index_node_;
    double tsfile_index_bloom_filter_error_percent_;
    TSEncoding time_encoding_type_;
    TSDataType time_data_type_;
    CompressionType time_compress_type_;
    int32_t chunk_group_size_threshold_;
    int32_t record_count_for_next_mem_check_;
    bool encrypt_flag_ = false;
    TSEncoding boolean_encoding_type_;
    TSEncoding int32_encoding_type_;
    TSEncoding int64_encoding_type_;
    TSEncoding float_encoding_type_;
    TSEncoding double_encoding_type_;
    TSEncoding string_encoding_type_;
    CompressionType default_compression_type_;
} ConfigValue;

extern void init_config_value();
extern TSEncoding get_value_encoder(TSDataType data_type);
extern CompressionType get_default_compressor();
// In the future, configuration items need to be dynamically adjusted according
// to the level
extern void set_config_value();
extern void config_set_page_max_point_count(uint32_t page_max_point_count);
extern void config_set_max_degree_of_index_node(
    uint32_t max_degree_of_index_node);

}  // namespace common

#endif  // COMMON_CONFIG_CONFIG_H
