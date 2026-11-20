/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#include "python_s3_read_file.h"

#include <cstdio>
#include <cstring>
#include <memory>
#include <new>
#include <string>

#include "common/config/config.h"
#include "common/global.h"
#include "file/cached_random_access_read_file.h"
#include "file/s3_random_access_read_file.h"
#include "file/s3_signing.h"
#include "reader/tsfile_reader.h"
#include "utils/errno_define.h"

namespace {

std::string as_string(const char* value) {
    return value != nullptr ? std::string(value) : std::string();
}

void set_error(char* error_buffer, int32_t error_buffer_size,
               const std::string& message) {
    if (error_buffer == nullptr || error_buffer_size <= 0) {
        return;
    }
    std::snprintf(error_buffer, static_cast<size_t>(error_buffer_size), "%s",
                  message.c_str());
}

}  // namespace

#ifdef __cplusplus
extern "C" {
#endif

int s3_native_support_available() {
    return storage::s3_support_available() ? 1 : 0;
}

void* create_tsfile_reader_from_s3(
    const char* uri, const char* endpoint, const char* region,
    const char* access_key_id, const char* secret_access_key,
    const char* session_token, int32_t path_style, int32_t connect_timeout_ms,
    int32_t request_timeout_ms, int32_t max_retries, int32_t prefetch_threads,
    char* error_buffer, int32_t error_buffer_size, int32_t* error_code) {
    if (error_code == nullptr) {
        set_error(error_buffer, error_buffer_size,
                  "error_code must not be null");
        return nullptr;
    }
    *error_code = common::E_OK;
    if (uri == nullptr) {
        *error_code = common::E_INVALID_ARG;
        set_error(error_buffer, error_buffer_size, "object URI is required");
        return nullptr;
    }
    storage::S3Options options;
    std::string parse_error;
    if (!storage::s3_parse_uri(uri, options.bucket, options.key, parse_error)) {
        *error_code = common::E_INVALID_ARG;
        set_error(error_buffer, error_buffer_size, parse_error);
        return nullptr;
    }
    options.endpoint = as_string(endpoint);
    options.region = as_string(region);
    options.access_key_id = as_string(access_key_id);
    options.secret_access_key = as_string(secret_access_key);
    options.session_token = as_string(session_token);
    options.path_style = path_style != 0;
    if (connect_timeout_ms > 0) {
        options.connect_timeout_ms = connect_timeout_ms;
    }
    if (request_timeout_ms > 0) {
        options.request_timeout_ms = request_timeout_ms;
    }
    if (max_retries >= 0) {
        options.max_retries = max_retries;
    }

    try {
        *error_code = storage::libtsfile_init();
        if (*error_code != common::E_OK) {
            set_error(error_buffer, error_buffer_size,
                      "failed to initialize the native TsFile runtime");
            return nullptr;
        }
        std::string open_error;
        std::unique_ptr<storage::RandomAccessReadFile> source =
            storage::open_s3_read_file(options, &open_error);
        if (!source) {
            *error_code = common::E_FILE_OPEN_ERR;
            set_error(error_buffer, error_buffer_size, open_error);
            return nullptr;
        }
        storage::ReadCacheOptions cache_options;
        cache_options.block_size = static_cast<int32_t>(
            common::g_config_value_.read_cache_block_size_);
        cache_options.capacity_bytes =
            static_cast<int64_t>(common::g_config_value_.read_cache_capacity_);
        cache_options.max_readahead_blocks = static_cast<int32_t>(
            common::g_config_value_.read_cache_max_readahead_blocks_);
        cache_options.prefetch_threads = prefetch_threads;
        if (common::g_config_value_.read_cache_enabled_) {
            source = storage::make_cached_read_file(std::move(source),
                                                    cache_options);
        }
        std::unique_ptr<storage::TsFileReader> reader(
            new storage::TsFileReader());
        *error_code = reader->open(std::move(source));
        if (*error_code != common::E_OK) {
            set_error(error_buffer, error_buffer_size,
                      std::string("failed to open ") + uri);
            return nullptr;
        }
        return reader.release();
    } catch (const std::bad_alloc&) {
        *error_code = common::E_OOM;
        set_error(error_buffer, error_buffer_size, "out of memory");
    } catch (...) {
        *error_code = common::E_FILE_OPEN_ERR;
        set_error(error_buffer, error_buffer_size,
                  std::string("failed to open ") + uri);
    }
    return nullptr;
}

#ifdef __cplusplus
}
#endif
