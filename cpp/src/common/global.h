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
extern ColumnSchema g_time_column_schema;

FORCE_INLINE int set_global_time_data_type(uint8_t data_type) {
    ASSERT(data_type >= BOOLEAN && data_type <= STRING);
    if (data_type != INT64) {
        return E_NOT_SUPPORT;
    }
    g_config_value_.time_data_type_ = static_cast<TSDataType>(data_type);
    return E_OK;
}

FORCE_INLINE int set_global_time_encoding(uint8_t encoding) {
    ASSERT(encoding >= PLAIN && encoding <= FREQ);
    if (encoding != TS_2DIFF && encoding != PLAIN && encoding != GORILLA &&
        encoding != ZIGZAG && encoding != RLE && encoding != SPRINTZ) {
        return E_NOT_SUPPORT;
    }
    g_config_value_.time_encoding_type_ = static_cast<TSEncoding>(encoding);
    return E_OK;
}

FORCE_INLINE int set_global_time_compression(uint8_t compression) {
    ASSERT(compression >= UNCOMPRESSED && compression <= LZ4);
    if (compression != UNCOMPRESSED && compression != SNAPPY &&
        compression != GZIP && compression != LZO && compression != LZ4) {
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
    ASSERT(encoding >= PLAIN && encoding <= SPRINTZ);

    // Check encoding support for each data type
    switch (dtype) {
        case BOOLEAN:
            if (encoding_type != PLAIN) return E_NOT_SUPPORT;
            g_config_value_.boolean_encoding_type_ = encoding_type;
            break;

        case INT32:
        case DATE:
        case INT64:
            if (encoding_type != PLAIN && encoding_type != TS_2DIFF &&
                encoding_type != GORILLA && encoding_type != ZIGZAG &&
                encoding_type != RLE && encoding_type != SPRINTZ) {
                return E_NOT_SUPPORT;
            }
            dtype == INT32
                ? g_config_value_.int32_encoding_type_ = encoding_type
                : g_config_value_.int64_encoding_type_ = encoding_type;
            break;

        case FLOAT:
        case DOUBLE:
            if (encoding_type != PLAIN && encoding_type != TS_2DIFF &&
                encoding_type != GORILLA && encoding_type != SPRINTZ) {
                return E_NOT_SUPPORT;
            }
            dtype == FLOAT
                ? g_config_value_.float_encoding_type_ = encoding_type
                : g_config_value_.double_encoding_type_ = encoding_type;
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
    ASSERT(compression >= UNCOMPRESSED && compression <= LZ4);
    if (compression != UNCOMPRESSED && compression != SNAPPY &&
        compression != GZIP && compression != LZO && compression != LZ4) {
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
            return static_cast<uint8_t>(g_config_value_.int64_encoding_type_);
        default:
            return static_cast<uint8_t>(
                PLAIN);  // Return default encoding for unknown types
    }
}

FORCE_INLINE uint8_t get_global_compression() {
    return static_cast<uint8_t>(g_config_value_.default_compression_type_);
}

extern int init_common();
extern bool is_timestamp_column_name(const char *time_col_name);
extern void cols_to_json(ByteStream *byte_stream,
                         std::vector<common::ColumnSchema> &ret_ts_list);
extern void print_backtrace();

}  // namespace common

#endif  // COMMON_GLOBAL_H
