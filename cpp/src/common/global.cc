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

#include "global.h"

#ifndef _WIN32
#include <execinfo.h>
#endif
#include <stdlib.h>

#include "utils/injection.h"

namespace common {

ColumnDesc g_time_column_desc;
ConfigValue g_config_value_;

void init_config_value() {
    g_config_value_.tsblock_mem_inc_step_size_ = 8000;  // 8k
    g_config_value_.tsblock_max_memory_ = 64000;        // 64k
    // g_config_value_.tsblock_max_memory_ = 32;
    g_config_value_.page_writer_max_point_num_ = 5;
    g_config_value_.page_writer_max_memory_bytes_ = 128 * 1024;  // 128 k
    g_config_value_.max_degree_of_index_node_ = 256;
    g_config_value_.tsfile_index_bloom_filter_error_percent_ = 0.05;
    g_config_value_.record_count_for_next_mem_check_ = 100;
    g_config_value_.chunk_group_size_threshold_ = 128 * 1024 * 1024;
    g_config_value_.time_encoding_type_ = TS_2DIFF;
    g_config_value_.time_data_type_ = INT64;
    g_config_value_.time_compress_type_ = LZ4;
}

void config_set_page_max_point_count(uint32_t page_max_ponint_count) {
    g_config_value_.page_writer_max_point_num_ = page_max_ponint_count;
}

void config_set_max_degree_of_index_node(uint32_t max_degree_of_index_node) {
    g_config_value_.max_degree_of_index_node_ = max_degree_of_index_node;
}

void set_config_value() {}

const char* s_data_type_names[7] = {"BOOLEAN", "INT32", "INT64", "FLOAT",
                                    "DOUBLE",  "TEXT",  "VECTOR"};

const char* s_encoding_names[12] = {
    "PLAIN",      "DICTIONARY", "RLE",     "DIFF",   "TS_2DIFF", "BITMAP",
    "GORILLA_V1", "REGULAR",    "GORILLA", "ZIGZAG", "FREQ"};

const char* s_compression_names[8] = {
    "UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "SDT", "PAA", "PLA", "LZ4",
};

int init_common() {
    int ret = E_OK;
    common::init_config_value();
    g_time_column_desc.type_ = INT64;
    g_time_column_desc.encoding_ = PLAIN;
    g_time_column_desc.compression_ = UNCOMPRESSED;
    g_time_column_desc.ttl_ = INT64_MAX;  // TODO
    g_time_column_desc.column_name_ = std::string("time");
    g_time_column_desc.ts_id_ = TsID(0, 0, 0);
    return ret;
}

bool is_timestamp_column_name(const char* time_col_name) {
    // both "time" and "timestamp" refer to timestmap column.
    int32_t len = strlen(time_col_name);
    if (len == 4) {
        return strncasecmp(time_col_name, "time", 4) == 0;
    } else if (len == 9) {
        return strncasecmp(time_col_name, "timestamp", 9) == 0;
    } else {
        return false;
    }
}

void cols_to_json(ByteStream* byte_stream,
                  std::vector<common::ColumnDesc>& ret_ts_list) {
    // 1. append start tag
    byte_stream->write_buf("{\n", 2);

    size_t ts_count = ret_ts_list.size();
    for (size_t i = 0; i < ts_count; ++i) {
        // 2. append timeseries name
        std::string name = ret_ts_list[i].column_name_;
        byte_stream->write_buf("  \"", 3);
        byte_stream->write_buf(name.c_str(), name.length());
        byte_stream->write_buf("\" : {\n", 6);

        // 3. append DataType
        const char* data_type = get_data_type_name(ret_ts_list[i].type_);
        byte_stream->write_buf("    \"DataType\" : \"", 18);
        byte_stream->write_buf(data_type, strlen(data_type));
        byte_stream->write_buf("\",\n", 3);

        // 4. append Encoding
        const char* encoding = get_encoding_name(ret_ts_list[i].encoding_);
        byte_stream->write_buf("    \"Encoding\" : \"", 18);
        byte_stream->write_buf(encoding, strlen(encoding));
        byte_stream->write_buf("\",\n", 3);

        // 5. append CompressionType
        const char* compression =
            get_compression_name(ret_ts_list[i].compression_);
        byte_stream->write_buf("    \"Compression\" : \"", 21);
        byte_stream->write_buf(compression, strlen(compression));
        byte_stream->write_buf("\",\n", 3);

        // 6. append TTL
        std::string ttl_str = to_string(ret_ts_list[i].ttl_);
        if (ret_ts_list[i].ttl_ == INVALID_TTL) {
            byte_stream->write_buf("    \"TTL\" : \"INVALID\",\n",
                                   23);  // if it is the end, delete ','
        } else {
            byte_stream->write_buf("    \"TTL\" : \"", 13);
            byte_stream->write_buf(ttl_str.c_str(), ttl_str.length());
            byte_stream->write_buf("\",\n", 3);  // if it is the end, delete ','
        }

        // TODO : this is used for Debug
        // 7. append TsId
        std::string tsid_str = ret_ts_list[i].ts_id_.to_string();
        byte_stream->write_buf("    \"TsID\" : \"", 14);
        byte_stream->write_buf(tsid_str.c_str(), tsid_str.length());
        byte_stream->write_buf("\"\n", 2);

        // 8. append footer
        if (i == ts_count - 1) {
            byte_stream->write_buf("  }\n", 4);
        } else {
            byte_stream->write_buf("  },\n", 5);
        }
    }

    // 9. end
    byte_stream->write_buf("}\n", 2);

    // DEBUG_print_byte_stream(*byte_stream);  // for debug
}

#ifndef _WIN32
void print_backtrace() {
    const int MAX_FRAMES = 32;
    int layers = 0;
    char** symbols = NULL;
    void* frames[MAX_FRAMES];

    memset(frames, 0, sizeof(frames));
    layers = backtrace(frames, MAX_FRAMES);
    symbols = backtrace_symbols(frames, layers);
    if (symbols) {
        for (int i = 0; i < layers; i++) {
            printf("SYMBOL layer %d: %s\n", i, symbols[i]);
        }
        free(symbols);
    }
}
#endif

Mutex g_all_inject_points_mutex;
std::map<std::string, InjectPoint> g_all_inject_points;

}  // namespace common
