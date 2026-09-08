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

extern TSFILE_API ConfigValue g_config_value_;
extern TSFILE_API ColumnSchema g_time_column_schema;

#ifdef ENABLE_THREADS
class ThreadPool;
// The single process-wide worker pool shared by every parallel code path
// (write column encoding, read column decoding).  Created in init_common()
// and torn down in libtsfile_destroy(); null until libtsfile_init() runs, so
// every caller must fall back to the serial path when it is null.
extern TSFILE_API ThreadPool* g_thread_pool_;
#endif

FORCE_INLINE int set_global_time_data_type(uint8_t data_type) {
    ASSERT(data_type >= BOOLEAN && data_type <= STRING);
    if (data_type != INT64) {
        return E_NOT_SUPPORT;
    }
    g_config_value_.time_data_type_ = static_cast<TSDataType>(data_type);
    return E_OK;
}

FORCE_INLINE int set_global_time_encoding(uint8_t encoding) {
    ASSERT(encoding >= PLAIN && encoding <= CAMEL);
    if (encoding != TS_2DIFF && encoding != PLAIN) {
        return E_NOT_SUPPORT;
    }
    g_config_value_.time_encoding_type_ = static_cast<TSEncoding>(encoding);
    return E_OK;
}

FORCE_INLINE int set_global_time_compression(uint8_t compression) {
    ASSERT(compression >= UNCOMPRESSED && compression <= LZMA2);
    if (compression != UNCOMPRESSED && compression != SNAPPY &&
        compression != GZIP && compression != LZO && compression != LZ4 &&
        compression != ZSTD && compression != LZMA2) {
        return E_NOT_SUPPORT;
    }
    g_config_value_.time_compress_type_ =
        static_cast<CompressionType>(compression);
    return E_OK;
}

FORCE_INLINE int set_datatype_encoding(uint8_t data_type, uint8_t encoding) {
    const TSDataType dtype = static_cast<TSDataType>(data_type);
    const TSEncoding encoding_type = static_cast<TSEncoding>(encoding);

    // Validate input parameters
    ASSERT(dtype >= BOOLEAN && dtype <= STRING);
    ASSERT(encoding >= PLAIN && encoding <= CAMEL);

    // Check encoding support for each data type
    switch (dtype) {
        case BOOLEAN:
            if (encoding_type != PLAIN) return E_NOT_SUPPORT;
            g_config_value_.boolean_encoding_type_ = encoding_type;
            break;

        case INT32:
        case DATE:
        case INT64:
        case TIMESTAMP:
            if (encoding_type != PLAIN && encoding_type != TS_2DIFF &&
                encoding_type != GORILLA && encoding_type != ZIGZAG &&
                encoding_type != RLE && encoding_type != SPRINTZ &&
                encoding_type != CHIMP && encoding_type != RLBE) {
                return E_NOT_SUPPORT;
            }
            (dtype == INT32 || dtype == DATE)
                ? g_config_value_.int32_encoding_type_ = encoding_type
                : g_config_value_.int64_encoding_type_ = encoding_type;
            break;

        case FLOAT:
            if (encoding_type != PLAIN && encoding_type != TS_2DIFF &&
                encoding_type != GORILLA && encoding_type != SPRINTZ &&
                encoding_type != CHIMP && encoding_type != RLBE) {
                return E_NOT_SUPPORT;
            }
            g_config_value_.float_encoding_type_ = encoding_type;
            break;

        case DOUBLE:
            if (encoding_type != PLAIN && encoding_type != TS_2DIFF &&
                encoding_type != GORILLA && encoding_type != SPRINTZ &&
                encoding_type != CHIMP && encoding_type != RLBE &&
                encoding_type != CAMEL) {
                return E_NOT_SUPPORT;
            }
            g_config_value_.double_encoding_type_ = encoding_type;
            break;

        case STRING:
        case TEXT:
            if (encoding_type != PLAIN && encoding_type != DICTIONARY) {
                return E_NOT_SUPPORT;
            }
            g_config_value_.string_encoding_type_ = encoding_type;
            break;

        default:
            break;
    }
    return E_OK;
}

FORCE_INLINE int set_global_compression(uint8_t compression) {
    ASSERT(compression >= UNCOMPRESSED && compression <= LZMA2);
    if (compression != UNCOMPRESSED && compression != SNAPPY &&
        compression != GZIP && compression != LZO && compression != LZ4 &&
        compression != ZSTD && compression != LZMA2) {
        return E_NOT_SUPPORT;
    }
    g_config_value_.default_compression_type_ =
        static_cast<CompressionType>(compression);
    return E_OK;
}

FORCE_INLINE uint8_t get_global_time_encoding() {
    return static_cast<uint8_t>(g_config_value_.time_encoding_type_);
}

FORCE_INLINE uint8_t get_global_time_compression() {
    return static_cast<uint8_t>(g_config_value_.time_compress_type_);
}

FORCE_INLINE uint8_t get_datatype_encoding(uint8_t data_type) {
    const TSDataType dtype = static_cast<TSDataType>(data_type);

    // Validate input parameter
    ASSERT(dtype >= BOOLEAN && dtype <= STRING);

    switch (dtype) {
        case BOOLEAN:
            return static_cast<uint8_t>(g_config_value_.boolean_encoding_type_);
        case INT32:
            return static_cast<uint8_t>(g_config_value_.int32_encoding_type_);
        case INT64:
            return static_cast<uint8_t>(g_config_value_.int64_encoding_type_);
        case FLOAT:
            return static_cast<uint8_t>(g_config_value_.float_encoding_type_);
        case DOUBLE:
            return static_cast<uint8_t>(g_config_value_.double_encoding_type_);
        case STRING:
        case TEXT:
            return static_cast<uint8_t>(g_config_value_.string_encoding_type_);
        case DATE:
            return static_cast<uint8_t>(g_config_value_.int32_encoding_type_);
        default:
            return static_cast<uint8_t>(
                PLAIN);  // Return default encoding for unknown types
    }
}

FORCE_INLINE uint8_t get_global_compression() {
    return static_cast<uint8_t>(g_config_value_.default_compression_type_);
}

FORCE_INLINE void set_parallel_read_enabled(bool enabled) {
    g_config_value_.parallel_read_enabled_ = enabled;
}

FORCE_INLINE bool get_parallel_read_enabled() {
    return g_config_value_.parallel_read_enabled_;
}

FORCE_INLINE void set_parallel_write_enabled(bool enabled) {
    g_config_value_.parallel_write_enabled_ = enabled;
}

FORCE_INLINE bool get_parallel_write_enabled() {
    return g_config_value_.parallel_write_enabled_;
}

// Select the backend used by subsequently opened local files. Existing
// ReadFile instances retain the backend selected when they were opened. This
// setting deliberately lives outside exported ConfigValue so adding it does
// not change that public data structure's ABI.
extern int set_file_read_backend(FileReadBackend backend);
extern FileReadBackend get_file_read_backend();

// Size of the single global worker pool.  Rejects values outside [1, 64] with
// E_INVALID_ARG, leaving the field untouched.  If the pool already exists
// (libtsfile_init has run) it is rebuilt at the new size immediately; the
// caller must ensure no read/write is concurrently using the pool.  Defined in
// global.cc (needs the full ThreadPool type).
extern int set_thread_count(int32_t count);

extern int init_common();
extern bool is_timestamp_column_name(const char* time_col_name);
extern void cols_to_json(ByteStream* byte_stream,
                         std::vector<common::ColumnSchema>& ret_ts_list);
extern void print_backtrace();

}  // namespace common

#endif  // COMMON_GLOBAL_H
