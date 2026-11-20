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

#ifndef PYTHON_S3_READ_FILE_H
#define PYTHON_S3_READ_FILE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/** 1 when this build has native S3/OSS support, 0 otherwise. */
int s3_native_support_available();

/**
 * Open a TsFile reader over an s3:// or oss:// URI.  Credentials, endpoint and
 * tuning values are passed as plain strings; empty/NULL values fall back to
 * anonymous access and library defaults.  On failure returns NULL and writes a
 * message into @p error_buffer.
 */
void* create_tsfile_reader_from_s3(
    const char* uri, const char* endpoint, const char* region,
    const char* access_key_id, const char* secret_access_key,
    const char* session_token, int32_t path_style, int32_t connect_timeout_ms,
    int32_t request_timeout_ms, int32_t max_retries, int32_t prefetch_threads,
    char* error_buffer, int32_t error_buffer_size, int32_t* error_code);

#ifdef __cplusplus
}
#endif

#endif  // PYTHON_S3_READ_FILE_H
