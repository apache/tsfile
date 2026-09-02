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

#include <atomic>

#ifdef ENABLE_THREADS
#include "common/thread_pool.h"
#endif

#ifndef _WIN32
#include <execinfo.h>
#include <strings.h>  // strncasecmp
#endif
#include <stdlib.h>
#include <string.h>  // strlen

#include "utils/injection.h"
#include "utils/util_define.h"  // strncasecmp -> _strnicmp shim on Windows

namespace common {

namespace {
// Kept outside ConfigValue for ABI compatibility. It is also intentionally not
// reset by init_common(), so callers may configure the first reader before
// libtsfile_init().
std::atomic<FileReadBackend> g_file_read_backend(FileReadBackend::PREAD);
}  // namespace

ColumnSchema g_time_column_schema;
ConfigValue g_config_value_;
#ifdef ENABLE_THREADS
ThreadPool* g_thread_pool_ = nullptr;
#endif

void init_config_value() {
    g_config_value_.tsblock_mem_inc_step_size_ = 8000;      // 8k
    g_config_value_.tsblock_max_memory_ = 2 * 1024 * 1024;  // 2 MB
    g_config_value_.page_writer_max_point_num_ = 10000;
    g_config_value_.page_writer_max_memory_bytes_ = 128 * 1024;  // 128 k
    g_config_value_.max_degree_of_index_node_ = 256;
    g_config_value_.tsfile_index_bloom_filter_error_percent_ = 0.05;
    g_config_value_.record_count_for_next_mem_check_ = 100;
    g_config_value_.chunk_group_size_threshold_ = 128 * 1024 * 1024;
    g_config_value_.time_encoding_type_ = TS_2DIFF;
    g_config_value_.time_data_type_ = INT64;
#ifdef ENABLE_LZ4
    g_config_value_.time_compress_type_ = LZ4;
#else
    g_config_value_.time_compress_type_ = UNCOMPRESSED;
#endif
    // Not support RLE yet.
    g_config_value_.boolean_encoding_type_ = PLAIN;
    g_config_value_.int32_encoding_type_ = TS_2DIFF;
    g_config_value_.int64_encoding_type_ = TS_2DIFF;
    g_config_value_.float_encoding_type_ = GORILLA;
    g_config_value_.double_encoding_type_ = GORILLA;
    g_config_value_.string_encoding_type_ = PLAIN;
    // Default compression is LZ4, matching the Java reference implementation
    // (TSFileConfig.compressor) and the previous C++ default; LZ4 generally
    // matches or beats Snappy on both ratio and decompression speed.  Fall
    // back to whatever was actually compiled in so the factory can always
    // produce the chosen compressor (an earlier revision gated on ENABLE_LZ4
    // but set SNAPPY, returning nullptr at write time when Snappy was off).
#ifdef ENABLE_LZ4
    g_config_value_.default_compression_type_ = LZ4;
#elif defined(ENABLE_SNAPPY)
    g_config_value_.default_compression_type_ = SNAPPY;
#else
    g_config_value_.default_compression_type_ = UNCOMPRESSED;
#endif
    g_config_value_.parallel_read_enabled_ = true;
    g_config_value_.parallel_write_enabled_ = true;
    // thread_count_ keeps its in-class default (see config.h) so a
    // set_thread_count() before libtsfile_init() is not reset here.
}

extern TSEncoding get_value_encoder(TSDataType data_type) {
    switch (data_type) {
        case BOOLEAN:
            return g_config_value_.boolean_encoding_type_;
        case INT32:
        case DATE:
            return g_config_value_.int32_encoding_type_;
        case INT64:
        case TIMESTAMP:
            return g_config_value_.int64_encoding_type_;
        case FLOAT:
            return g_config_value_.float_encoding_type_;
        case DOUBLE:
            return g_config_value_.double_encoding_type_;
        case TEXT:
        case STRING:
        case BLOB:
            return g_config_value_.string_encoding_type_;
        case VECTOR:
            break;
        case NULL_TYPE:
            break;
        case INVALID_DATATYPE:
            break;
        default:
            break;
    }
    return TSEncoding::INVALID_ENCODING;
}

extern CompressionType get_default_compressor() {
    return g_config_value_.default_compression_type_;
}

int config_set_page_max_point_count(uint32_t page_max_point_count) {
    if (page_max_point_count == 0) {
        return E_INVALID_ARG;
    }
    g_config_value_.page_writer_max_point_num_ = page_max_point_count;
    return E_OK;
}

int config_set_max_degree_of_index_node(uint32_t max_degree_of_index_node) {
    if (max_degree_of_index_node < 2u) {
        return E_INVALID_ARG;
    }
    g_config_value_.max_degree_of_index_node_ = max_degree_of_index_node;
    return E_OK;
}

void set_config_value() {}
const char* s_data_type_names[12] = {"BOOLEAN",   "INT32", "INT64",  "FLOAT",
                                     "DOUBLE",    "TEXT",  "VECTOR", "UNKNOWN",
                                     "TIMESTAMP", "DATE",  "BLOB",   "STRING"};

const char* s_encoding_names[15] = {
    "PLAIN",  "DICTIONARY", "RLE",     "DIFF",    "TS_2DIFF",
    "BITMAP", "GORILLA_V1", "REGULAR", "GORILLA", "ZIGZAG",
    "FREQ",   "CHIMP",      "SPRINTZ", "RLBE",    "CAMEL"};

const char* s_compression_names[10] = {
    "UNCOMPRESSED", "SNAPPY", "GZIP", "LZO",  "SDT",
    "PAA",          "PLA",    "LZ4",  "ZSTD", "LZMA2",
};

int init_common() {
    int ret = E_OK;
    common::init_config_value();
    g_time_column_schema.data_type_ = INT64;
    g_time_column_schema.encoding_ = PLAIN;
    g_time_column_schema.compression_ = UNCOMPRESSED;
    g_time_column_schema.column_name_ = storage::TIME_COLUMN_NAME;
#ifdef ENABLE_THREADS
    // (Re)create the single global worker pool with the configured size.  All
    // parallel write/read paths submit here; torn down in libtsfile_destroy().
    delete g_thread_pool_;
    size_t pool_size = g_config_value_.thread_count_ > 0
                           ? static_cast<size_t>(g_config_value_.thread_count_)
                           : size_t{1};
    g_thread_pool_ = new ThreadPool(pool_size);
#endif
    return ret;
}

int set_thread_count(int32_t count) {
    if (count < 1 || count > 64) return E_INVALID_ARG;
    g_config_value_.thread_count_ = count;
#ifdef ENABLE_THREADS
    // If the global pool already exists (libtsfile_init has run) rebuild it at
    // the new size so the change takes effect immediately instead of only at
    // the next libtsfile_init().  This joins all current workers and recreates
    // them, so the caller must ensure no read/write is concurrently using the
    // pool — intended for setup / benchmark reconfiguration, not mid-operation
    // resizing.
    if (g_thread_pool_ != nullptr) {
        delete g_thread_pool_;
        g_thread_pool_ = new ThreadPool(static_cast<size_t>(count));
    }
#endif
    return E_OK;
}

int set_file_read_backend(FileReadBackend backend) {
    switch (backend) {
        case FileReadBackend::AUTO:
        case FileReadBackend::MMAP:
        case FileReadBackend::PREAD:
            g_file_read_backend.store(backend, std::memory_order_relaxed);
            return E_OK;
        default:
            return E_INVALID_ARG;
    }
}

FileReadBackend get_file_read_backend() {
    return g_file_read_backend.load(std::memory_order_relaxed);
}

bool is_timestamp_column_name(const char* time_col_name) {
    // both "time" and "timestamp" refer to timestamp column.
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
                  std::vector<common::ColumnSchema>& ret_ts_list) {
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
        const char* data_type = get_data_type_name(ret_ts_list[i].data_type_);
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

        // 6. append footer
        if (i == ts_count - 1) {
            byte_stream->write_buf("  }\n", 4);
        } else {
            byte_stream->write_buf("  },\n", 5);
        }
    }

    // 7. end
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

std::map<std::string, InjectPoint> g_all_inject_points;

#ifdef ENABLE_TEST
void enable_injection(const char* inject_point_name, int count) {
    g_all_inject_points[inject_point_name] = InjectPoint{count};
}

void disable_injection(const char* inject_point_name) {
    g_all_inject_points.erase(inject_point_name);
}
#endif

}  // namespace common
