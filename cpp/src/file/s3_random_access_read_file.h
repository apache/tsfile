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

#ifndef FILE_S3_RANDOM_ACCESS_READ_FILE_H
#define FILE_S3_RANDOM_ACCESS_READ_FILE_H

#include <stdint.h>

#include <memory>
#include <string>

#include "file/random_access_read_file.h"

namespace storage {

/**
 * Connection settings for one object. @p endpoint is the service endpoint,
 * for example "https://oss-cn-beijing.aliyuncs.com" or
 * "https://s3.us-east-1.amazonaws.com"; an optional base path is preserved.
 *
 * The region is required for SigV4 signing; when it is empty it is derived
 * from the endpoint host when the pattern is recognized. Empty credentials
 * select anonymous access (public buckets).
 */
struct S3Options {
    std::string endpoint;
    std::string bucket;
    std::string key;
    std::string region;
    std::string access_key_id;
    std::string secret_access_key;
    std::string session_token;
    bool path_style = true;
    int32_t connect_timeout_ms = 5000;
    int32_t request_timeout_ms = 60000;
    int32_t max_retries = 3;
};

/** Whether this build has native S3/OSS support (requires libcurl). */
bool s3_support_available();

/**
 * Open @p options as a random-access source. Performs a HEAD request to learn
 * the object size and ETag. Returns nullptr on failure and, when non-null,
 * fills @p error_message.
 */
std::unique_ptr<RandomAccessReadFile> open_s3_read_file(
    const S3Options& options, std::string* error_message);

}  // end namespace storage
#endif  // FILE_S3_RANDOM_ACCESS_READ_FILE_H
