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

#include "file/s3_signing.h"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <ctime>

#include "utils/sha256.h"

namespace storage {

namespace {

std::string trim_copy(const std::string& value) {
    size_t begin = 0;
    size_t end = value.size();
    while (begin < end && (value[begin] == ' ' || value[begin] == '\t')) {
        begin++;
    }
    while (end > begin && (value[end - 1] == ' ' || value[end - 1] == '\t')) {
        end--;
    }
    return value.substr(begin, end - begin);
}

std::string lower_copy(const std::string& value) {
    std::string out = value;
    for (size_t i = 0; i < out.size(); i++) {
        if (out[i] >= 'A' && out[i] <= 'Z') {
            out[i] = static_cast<char>(out[i] - 'A' + 'a');
        }
    }
    return out;
}

std::string join_headers(
    const std::vector<std::pair<std::string, std::string>>& headers) {
    std::string out;
    for (size_t i = 0; i < headers.size(); i++) {
        out += headers[i].first;
        out += ':';
        out += headers[i].second;
        out += '\n';
    }
    return out;
}

std::string join_signed_header_names(
    const std::vector<std::pair<std::string, std::string>>& headers) {
    std::string out;
    for (size_t i = 0; i < headers.size(); i++) {
        if (i > 0) {
            out += ';';
        }
        out += headers[i].first;
    }
    return out;
}

std::string host_from_endpoint(const std::string& endpoint) {
    std::string host = endpoint;
    const size_t scheme = host.find("://");
    if (scheme != std::string::npos) {
        host = host.substr(scheme + 3);
    }
    const size_t slash = host.find('/');
    if (slash != std::string::npos) {
        host.erase(slash);
    }
    return lower_copy(host);
}

}  // namespace

std::string s3_uri_encode(const std::string& value, bool keep_slash) {
    static const char kHex[] = "0123456789ABCDEF";
    std::string out;
    out.reserve(value.size() * 3);
    for (size_t i = 0; i < value.size(); i++) {
        const unsigned char c = static_cast<unsigned char>(value[i]);
        const bool unreserved = (c >= 'A' && c <= 'Z') ||
                                (c >= 'a' && c <= 'z') ||
                                (c >= '0' && c <= '9') || c == '-' ||
                                c == '.' || c == '_' || c == '~';
        if (unreserved || (keep_slash && c == '/')) {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(kHex[c >> 4]);
            out.push_back(kHex[c & 0x0F]);
        }
    }
    return out;
}

std::string s3_amz_date_now() {
    const std::time_t now = std::time(nullptr);
    std::tm utc;
#ifdef _WIN32
    gmtime_s(&utc, &now);
#else
    gmtime_r(&now, &utc);
#endif
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%04d%02d%02dT%02d%02d%02dZ",
                  utc.tm_year + 1900, utc.tm_mon + 1, utc.tm_mday, utc.tm_hour,
                  utc.tm_min, utc.tm_sec);
    return std::string(buffer);
}

std::string s3_authorization_header(const S3CanonicalRequest& request,
                                    const S3Credentials& credentials,
                                    const std::string& amz_date,
                                    const std::string& service) {
    if (!credentials.has_credentials()) {
        return std::string();
    }
    std::vector<std::pair<std::string, std::string>> headers;
    headers.reserve(request.headers.size());
    for (size_t i = 0; i < request.headers.size(); i++) {
        headers.push_back(std::make_pair(lower_copy(request.headers[i].first),
                                         trim_copy(request.headers[i].second)));
    }
    std::sort(headers.begin(), headers.end(),
              [](const std::pair<std::string, std::string>& left,
                 const std::pair<std::string, std::string>& right) {
                  return left.first < right.first;
              });

    const std::string canonical_request =
        request.method + "\n" + request.canonical_uri + "\n" +
        request.canonical_query + "\n" + join_headers(headers) + "\n" +
        join_signed_header_names(headers) + "\n" + request.payload_hash;

    const std::string date_stamp = amz_date.substr(0, 8);
    const std::string scope =
        date_stamp + "/" + credentials.region + "/" + service + "/aws4_request";
    const std::string string_to_sign = "AWS4-HMAC-SHA256\n" + amz_date + "\n" +
                                       scope + "\n" +
                                       crypto::sha256_hex(canonical_request);

    const std::string signing_key = "AWS4" + credentials.secret_access_key;
    unsigned char k_date[32];
    unsigned char k_region[32];
    unsigned char k_service[32];
    unsigned char k_signing[32];
    crypto::hmac_sha256(
        reinterpret_cast<const unsigned char*>(signing_key.data()),
        signing_key.size(), date_stamp.data(), date_stamp.size(), k_date);
    crypto::hmac_sha256(k_date, sizeof(k_date), credentials.region.data(),
                        credentials.region.size(), k_region);
    crypto::hmac_sha256(k_region, sizeof(k_region), service.data(),
                        service.size(), k_service);
    const char kTerminator[] = "aws4_request";
    crypto::hmac_sha256(k_service, sizeof(k_service),
                        reinterpret_cast<const unsigned char*>(kTerminator),
                        std::strlen(kTerminator), k_signing);

    unsigned char signature[32];
    crypto::hmac_sha256(
        k_signing, sizeof(k_signing),
        reinterpret_cast<const unsigned char*>(string_to_sign.data()),
        string_to_sign.size(), signature);

    std::string header =
        "AWS4-HMAC-SHA256 Credential=" + credentials.access_key_id + "/" +
        scope + ",SignedHeaders=" + join_signed_header_names(headers) +
        ",Signature=" + crypto::to_hex(signature, sizeof(signature));
    return header;
}

std::string s3_derive_region_from_endpoint(const std::string& endpoint) {
    const std::string host = host_from_endpoint(endpoint);
    if (host.empty()) {
        return std::string();
    }
    // Alibaba Cloud OSS: oss-<region>.aliyuncs.com, optionally -internal.
    const size_t oss = host.find("oss-");
    if (oss != std::string::npos &&
        host.find("aliyuncs.com") != std::string::npos) {
        std::string region = host.substr(oss + 4);
        const size_t dot = region.find('.');
        if (dot != std::string::npos) {
            region.erase(dot);
        }
        const std::string internal = "-internal";
        if (region.size() > internal.size() &&
            region.compare(region.size() - internal.size(), internal.size(),
                           internal) == 0) {
            region.erase(region.size() - internal.size());
        }
        return region;
    }
    // AWS: bucket.s3.<region>.amazonaws.com, s3.<region>.amazonaws.com,
    // s3-<region>.amazonaws.com, or the legacy s3.amazonaws.com (us-east-1).
    const size_t s3_dot = host.find(".s3.");
    std::string remainder;
    if (s3_dot != std::string::npos) {
        remainder = host.substr(s3_dot + 4);
    } else if (host.compare(0, 3, "s3.") == 0) {
        remainder = host.substr(3);
    } else if (host.compare(0, 3, "s3-") == 0) {
        remainder = host.substr(3);
        const size_t dot = remainder.find('.');
        if (dot != std::string::npos) {
            remainder.erase(dot);
        }
        return remainder;
    } else {
        return std::string();
    }
    if (remainder.compare(0, 10, "dualstack.") == 0) {
        remainder = remainder.substr(10);
    }
    if (remainder.compare(0, 9, "amazonaws") == 0) {
        return "us-east-1";
    }
    const size_t dot = remainder.find('.');
    if (dot != std::string::npos) {
        remainder.erase(dot);
    }
    return remainder;
}

bool s3_parse_uri(const std::string& uri, std::string& bucket, std::string& key,
                  std::string& error_message) {
    std::string rest;
    if (uri.compare(0, 5, "s3://") == 0) {
        rest = uri.substr(5);
    } else if (uri.compare(0, 6, "oss://") == 0) {
        rest = uri.substr(6);
    } else {
        error_message = "expected an s3:// or oss:// URI, got: " + uri;
        return false;
    }
    const size_t slash = rest.find('/');
    if (slash == std::string::npos || slash == 0 || slash + 1 >= rest.size()) {
        error_message = "object URI must name a bucket and a key: " + uri;
        return false;
    }
    bucket = rest.substr(0, slash);
    key = rest.substr(slash + 1);
    return true;
}

}  // end namespace storage
