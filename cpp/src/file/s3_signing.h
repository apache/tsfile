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

#ifndef FILE_S3_SIGNING_H
#define FILE_S3_SIGNING_H

#include <string>
#include <utility>
#include <vector>

namespace storage {

/** AWS Signature Version 4 credentials. Empty keys mean anonymous access. */
struct S3Credentials {
    std::string access_key_id;
    std::string secret_access_key;
    std::string region;
    std::string session_token;

    bool has_credentials() const {
        return !access_key_id.empty() && !secret_access_key.empty();
    }
};

/**
 * The parts of a request SigV4 signs. Header names must be lower case; the
 * signer trims values, sorts the headers and lower-cases names again, so a
 * caller may pass them in any order.
 */
struct S3CanonicalRequest {
    std::string method;
    std::string canonical_uri;
    std::string canonical_query;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string payload_hash;
};

/** RFC 3986 percent-encoding; keep_slash leaves '/' unescaped (paths). */
std::string s3_uri_encode(const std::string& value, bool keep_slash);

/** Current UTC time formatted as YYYYMMDDTHHMMSSZ. */
std::string s3_amz_date_now();

/**
 * Build the SigV4 Authorization header value for one request. Returns an empty
 * string when the credentials are anonymous. @p amz_date must be the same
 * YYYYMMDDTHHMMSSZ value sent in x-amz-date; @p service is "s3".
 */
std::string s3_authorization_header(const S3CanonicalRequest& request,
                                    const S3Credentials& credentials,
                                    const std::string& amz_date,
                                    const std::string& service);

/**
 * Best-effort region lookup from an endpoint host for callers that did not
 * configure one (OSS and AWS styles). Returns an empty string when unknown.
 */
std::string s3_derive_region_from_endpoint(const std::string& endpoint);

/** Split "s3://bucket/key" or "oss://bucket/key". */
bool s3_parse_uri(const std::string& uri, std::string& bucket, std::string& key,
                  std::string& error_message);

}  // end namespace storage
#endif  // FILE_S3_SIGNING_H
