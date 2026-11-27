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

#include "file/s3_random_access_read_file.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "file/s3_signing.h"
#include "utils/errno_define.h"

#if defined(TSFILE_ENABLE_S3)
#include <curl/curl.h>
#endif

namespace storage {

namespace {

// SHA-256 of the empty payload; GET requests have no body.
const char kEmptyPayloadSha256[] =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

const int32_t kMaxPooledHandles = 8;

struct EndpointParts {
    std::string scheme;
    std::string authority;
    std::string base_path;
};

bool parse_endpoint(const std::string& endpoint, EndpointParts& parts,
                    std::string* error_message) {
    std::string rest = endpoint;
    const size_t scheme_end = rest.find("://");
    if (scheme_end == std::string::npos) {
        parts.scheme = "https";
    } else {
        parts.scheme = rest.substr(0, scheme_end);
        rest = rest.substr(scheme_end + 3);
        for (size_t i = 0; i < parts.scheme.size(); i++) {
            parts.scheme[i] = static_cast<char>(
                std::tolower(static_cast<unsigned char>(parts.scheme[i])));
        }
    }
    if (parts.scheme != "http" && parts.scheme != "https") {
        if (error_message != nullptr) {
            *error_message = "S3 endpoint must use http or https: " + endpoint;
        }
        return false;
    }
    const size_t slash = rest.find('/');
    if (slash == std::string::npos) {
        parts.authority = rest;
        parts.base_path.clear();
    } else {
        parts.authority = rest.substr(0, slash);
        parts.base_path = rest.substr(slash);
        while (!parts.base_path.empty() &&
               parts.base_path[parts.base_path.size() - 1] == '/') {
            parts.base_path.erase(parts.base_path.size() - 1);
        }
    }
    if (parts.authority.empty()) {
        if (error_message != nullptr) {
            *error_message = "S3 endpoint is missing a host: " + endpoint;
        }
        return false;
    }
    return true;
}

uint64_t fnv1a_hash(const std::string& value) {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < value.size(); i++) {
        hash ^= static_cast<unsigned char>(value[i]);
        hash *= 1099511628211ULL;
    }
    return hash;
}

std::string to_lower(const std::string& value) {
    std::string out = value;
    for (size_t i = 0; i < out.size(); i++) {
        out[i] =
            static_cast<char>(std::tolower(static_cast<unsigned char>(out[i])));
    }
    return out;
}

}  // namespace

#if defined(TSFILE_ENABLE_S3)

namespace {

std::once_flag g_curl_init_once;
std::mutex g_share_mutex;

void curl_init_once() { curl_global_init(CURL_GLOBAL_DEFAULT); }

void share_lock_cb(CURL* /* handle */, curl_lock_data /* data */,
                   curl_lock_access /* access */, void* userptr) {
    static_cast<std::mutex*>(userptr)->lock();
}

void share_unlock_cb(CURL* /* handle */, curl_lock_data /* data */,
                     void* userptr) {
    static_cast<std::mutex*>(userptr)->unlock();
}

struct TransferContext {
    TransferContext()
        : buffer(nullptr),
          skip(0),
          want(0),
          copied(0),
          handle(nullptr),
          status(0),
          content_length(-1),
          overflow(false) {}

    char* buffer;
    int64_t skip;
    int64_t want;
    int64_t copied;
    CURL* handle;
    long status;
    int64_t content_length;
    bool overflow;
    std::string etag;
};

size_t write_cb(char* ptr, size_t size, size_t nmemb, void* userdata) {
    TransferContext* ctx = static_cast<TransferContext*>(userdata);
    const size_t total = size * nmemb;
    if (ctx->status == 0 &&
        curl_easy_getinfo(ctx->handle, CURLINFO_RESPONSE_CODE, &ctx->status) !=
            CURLE_OK) {
        return 0;
    }
    size_t start = 0;
    if (ctx->skip > 0 && ctx->status == 200) {
        // The server ignored the Range header and sent the whole object.
        const size_t skip = static_cast<size_t>(
            std::min<int64_t>(ctx->skip, static_cast<int64_t>(total)));
        ctx->skip -= static_cast<int64_t>(skip);
        start = skip;
    }
    if (start < total) {
        const int64_t room = ctx->want - ctx->copied;
        const size_t available = total - start;
        const size_t take = static_cast<size_t>(
            std::min<int64_t>(static_cast<int64_t>(available), room));
        if (take > 0) {
            std::memcpy(ctx->buffer + ctx->copied, ptr + start, take);
            ctx->copied += static_cast<int64_t>(take);
        }
        if (take < available && ctx->status != 200) {
            ctx->overflow = true;
        }
    }
    return total;
}

bool header_name_is(const char* line, size_t length, const char* name,
                    size_t* value_start) {
    const size_t name_length = std::strlen(name);
    if (length < name_length + 1) {
        return false;
    }
    for (size_t i = 0; i < name_length; i++) {
        if (std::tolower(static_cast<unsigned char>(line[i])) != name[i]) {
            return false;
        }
    }
    if (line[name_length] != ':') {
        return false;
    }
    size_t start = name_length + 1;
    while (start < length && (line[start] == ' ' || line[start] == '\t')) {
        start++;
    }
    *value_start = start;
    return true;
}

size_t header_cb(char* buffer, size_t size, size_t nitems, void* userdata) {
    TransferContext* ctx = static_cast<TransferContext*>(userdata);
    const size_t length = size * nitems;
    if (length >= 5 && std::strncmp(buffer, "HTTP/", 5) == 0) {
        // A proxy or informational response can precede the object's headers.
        ctx->status = 0;
        ctx->content_length = -1;
        ctx->etag.clear();
        return length;
    }
    size_t start = 0;
    if (header_name_is(buffer, length, "content-length", &start)) {
        std::string value(buffer + start, length - start);
        ctx->content_length = std::strtoll(value.c_str(), nullptr, 10);
    } else if (header_name_is(buffer, length, "etag", &start)) {
        std::string value(buffer + start, length - start);
        while (!value.empty() && (value[value.size() - 1] == '\r' ||
                                  value[value.size() - 1] == '\n' ||
                                  value[value.size() - 1] == ' ')) {
            value.erase(value.size() - 1);
        }
        ctx->etag = value;
    }
    return length;
}

class S3ReadFile : public RandomAccessReadFile {
   public:
    explicit S3ReadFile(const S3Options& options)
        : options_(options),
          file_size_(0),
          closed_(false),
          generation_fingerprint_(0) {
        std::call_once(g_curl_init_once, curl_init_once);
        share_ = curl_share_init();
        if (share_ != nullptr) {
            curl_share_setopt(share_, CURLSHOPT_USERDATA, &g_share_mutex);
            curl_share_setopt(share_, CURLSHOPT_LOCKFUNC, share_lock_cb);
            curl_share_setopt(share_, CURLSHOPT_UNLOCKFUNC, share_unlock_cb);
            curl_share_setopt(share_, CURLSHOPT_SHARE, CURL_LOCK_DATA_DNS);
            curl_share_setopt(share_, CURLSHOPT_SHARE,
                              CURL_LOCK_DATA_SSL_SESSION);
        }
    }

    ~S3ReadFile() override { close(); }

    int open(std::string* error_message);

    bool is_opened() const override { return !closed_.load(); }
    int64_t file_size() const override { return file_size_; }
    const std::string& file_path() const override { return display_path_; }

    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        size = static_cast<uint64_t>(file_size_);
        fingerprint = generation_fingerprint_;
        return common::E_OK;
    }

    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override;

    void close() override;

   private:
    struct RequestTarget {
        std::string url;
        std::string canonical_uri;
        std::string host;
    };

    bool build_target(std::string* error_message);
    CURL* acquire_handle();
    void release_handle(CURL* handle);
    int perform(int64_t offset, int32_t size, char* buffer, bool head_request,
                TransferContext* context, std::string* error_message);

    S3Options options_;
    S3Credentials credentials_;
    EndpointParts endpoint_;
    RequestTarget target_;
    std::string display_path_;
    int64_t file_size_;
    uint64_t generation_fingerprint_;
    std::string etag_;
    std::atomic<bool> closed_;

    std::mutex pool_mutex_;
    std::vector<CURL*> pooled_handles_;
    CURLSH* share_ = nullptr;
};

bool S3ReadFile::build_target(std::string* error_message) {
    if (!parse_endpoint(options_.endpoint, endpoint_, error_message)) {
        return false;
    }
    const std::string encoded_key = s3_uri_encode(options_.key, true);
    std::string path;
    if (options_.path_style) {
        target_.host = endpoint_.authority;
        path = endpoint_.base_path + "/" +
               s3_uri_encode(options_.bucket, false) + "/" + encoded_key;
    } else {
        target_.host = options_.bucket + "." + endpoint_.authority;
        path = endpoint_.base_path + "/" + encoded_key;
    }
    target_.canonical_uri = path;
    target_.url = endpoint_.scheme + "://" + target_.host + path;
    display_path_ = "s3://" + options_.bucket + "/" + options_.key;
    return true;
}

CURL* S3ReadFile::acquire_handle() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    if (!pooled_handles_.empty()) {
        CURL* handle = pooled_handles_.back();
        pooled_handles_.pop_back();
        return handle;
    }
    return curl_easy_init();
}

void S3ReadFile::release_handle(CURL* handle) {
    if (handle == nullptr) {
        return;
    }
    std::lock_guard<std::mutex> lock(pool_mutex_);
    if (static_cast<int32_t>(pooled_handles_.size()) < kMaxPooledHandles) {
        pooled_handles_.push_back(handle);
    } else {
        curl_easy_cleanup(handle);
    }
}

int S3ReadFile::perform(int64_t offset, int32_t size, char* buffer,
                        bool head_request, TransferContext* context,
                        std::string* error_message) {
    CURL* handle = acquire_handle();
    if (handle == nullptr) {
        if (error_message != nullptr) {
            *error_message = "failed to create a libcurl handle";
        }
        return common::E_OOM;
    }
    curl_easy_reset(handle);
    context->handle = handle;
    curl_easy_setopt(handle, CURLOPT_PATH_AS_IS, 1L);
    curl_easy_setopt(handle, CURLOPT_URL, target_.url.c_str());
    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT_MS,
                     static_cast<long>(options_.connect_timeout_ms));
    curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS,
                     static_cast<long>(options_.request_timeout_ms));
    curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(handle, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, context);
    curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, header_cb);
    curl_easy_setopt(handle, CURLOPT_HEADERDATA, context);
    if (share_ != nullptr) {
        curl_easy_setopt(handle, CURLOPT_SHARE, share_);
    }
    if (head_request) {
        curl_easy_setopt(handle, CURLOPT_NOBODY, 1L);
    } else {
        curl_easy_setopt(handle, CURLOPT_NOBODY, 0L);
        const int64_t end = offset + static_cast<int64_t>(size) - 1;
        char range[64];
        std::snprintf(range, sizeof(range), "%lld-%lld",
                      static_cast<long long>(offset),
                      static_cast<long long>(end));
        curl_easy_setopt(handle, CURLOPT_RANGE, range);
#if LIBCURL_VERSION_NUM >= 0x075500
        curl_easy_setopt(handle, CURLOPT_PROTOCOLS_STR, "http,https");
#else
        curl_easy_setopt(handle, CURLOPT_PROTOCOLS,
                         static_cast<long>(CURLPROTO_HTTP | CURLPROTO_HTTPS));
#endif
    }

    struct curl_slist* headers = nullptr;
    std::vector<std::string> request_headers;
    if (!head_request) {
        request_headers.push_back("If-Match: " + etag_);
    }
    std::string amz_date;
    if (credentials_.has_credentials()) {
        amz_date = s3_amz_date_now();
        S3CanonicalRequest canonical;
        canonical.method = head_request ? "HEAD" : "GET";
        canonical.canonical_uri = target_.canonical_uri;
        canonical.canonical_query.clear();
        canonical.payload_hash = kEmptyPayloadSha256;
        canonical.headers.push_back(std::make_pair("host", target_.host));
        canonical.headers.push_back(std::make_pair(
            "x-amz-content-sha256", std::string(kEmptyPayloadSha256)));
        canonical.headers.push_back(std::make_pair("x-amz-date", amz_date));
        if (!credentials_.session_token.empty()) {
            canonical.headers.push_back(std::make_pair(
                "x-amz-security-token", credentials_.session_token));
        }
        const std::string authorization =
            s3_authorization_header(canonical, credentials_, amz_date, "s3");
        request_headers.push_back("x-amz-date: " + amz_date);
        request_headers.push_back(std::string("x-amz-content-sha256: ") +
                                  kEmptyPayloadSha256);
        if (!credentials_.session_token.empty()) {
            request_headers.push_back("x-amz-security-token: " +
                                      credentials_.session_token);
        }
        request_headers.push_back("Authorization: " + authorization);
    }
    for (const auto& header : request_headers) {
        curl_slist* next = curl_slist_append(headers, header.c_str());
        if (next == nullptr) {
            curl_slist_free_all(headers);
            release_handle(handle);
            return common::E_OOM;
        }
        headers = next;
    }
    CURLcode code = curl_easy_setopt(handle, CURLOPT_HTTPHEADER, headers);
    if (code == CURLE_OK) {
        code = curl_easy_perform(handle);
    }
    if (code == CURLE_OK) {
        code =
            curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &context->status);
    }
    curl_slist_free_all(headers);
    release_handle(handle);

    if (code != CURLE_OK) {
        if (error_message != nullptr) {
            *error_message =
                std::string("HTTP request failed: ") + curl_easy_strerror(code);
        }
        return common::E_FILE_READ_ERR;
    }
    return common::E_OK;
}

int S3ReadFile::open(std::string* error_message) {
    if (!build_target(error_message)) {
        return common::E_FILE_OPEN_ERR;
    }
    credentials_.access_key_id = options_.access_key_id;
    credentials_.secret_access_key = options_.secret_access_key;
    credentials_.region = options_.region;
    credentials_.session_token = options_.session_token;

    std::string last_error;
    for (int32_t attempt = 0; attempt <= options_.max_retries; attempt++) {
        TransferContext context;
        const int ret = perform(0, 0, nullptr, true, &context, &last_error);
        if (ret == common::E_OK && context.status >= 200 &&
            context.status < 300 && context.content_length >= 0) {
            if (context.etag.size() < 2 || context.etag.front() != '"' ||
                context.etag.back() != '"') {
                last_error =
                    "HEAD " + display_path_ + " returned no strong ETag";
                break;
            }
            file_size_ = context.content_length;
            etag_ = context.etag;
            generation_fingerprint_ =
                fnv1a_hash(etag_.substr(1, etag_.size() - 2));
            return common::E_OK;
        }
        if (ret == common::E_OK) {
            char buffer[160];
            std::snprintf(buffer, sizeof(buffer), "HEAD %s returned HTTP %ld",
                          display_path_.c_str(), context.status);
            last_error = buffer;
        }
        if (attempt < options_.max_retries) {
            std::this_thread::sleep_for(std::chrono::milliseconds(
                50 * (1 << (attempt < 5 ? attempt : 5))));
        }
    }
    if (error_message != nullptr) {
        *error_message = last_error.empty()
                             ? std::string("failed to open ") + display_path_
                             : last_error;
    }
    return common::E_FILE_OPEN_ERR;
}

int S3ReadFile::read(int64_t offset, char* buffer, int32_t size,
                     int32_t& read_size) {
    read_size = 0;
    if (offset < 0 || size < 0 || (buffer == nullptr && size > 0)) {
        return common::E_INVALID_ARG;
    }
    if (closed_.load()) {
        return common::E_FILE_READ_ERR;
    }
    if (size == 0 || offset >= file_size_) {
        return common::E_OK;
    }
    int64_t end = offset + static_cast<int64_t>(size);
    if (end > file_size_) {
        end = file_size_;
    }
    const int32_t want = static_cast<int32_t>(end - offset);

    std::string last_error;
    for (int32_t attempt = 0; attempt <= options_.max_retries; attempt++) {
        TransferContext context;
        context.buffer = buffer;
        context.want = want;
        // A server that ignores the Range header replies 200 with the whole
        // object; drop the bytes before the requested offset.
        context.skip = offset;
        const int ret =
            perform(offset, want, buffer, false, &context, &last_error);
        if (context.status == 412 ||
            (ret == common::E_OK &&
             (context.status == 200 || context.status == 206) &&
             !context.etag.empty() && context.etag != etag_)) {
            return common::E_FILE_READ_ERR;
        }
        if (ret == common::E_OK && context.copied == want &&
            !context.overflow &&
            (context.status == 200 || context.status == 206)) {
            read_size = want;
            return common::E_OK;
        }
        if (ret == common::E_OK) {
            if (context.status != 200 && context.status != 206) {
                char buffer_msg[160];
                std::snprintf(buffer_msg, sizeof(buffer_msg),
                              "GET %s returned HTTP %ld", display_path_.c_str(),
                              context.status);
                last_error = buffer_msg;
            } else {
                char buffer_msg[200];
                std::snprintf(buffer_msg, sizeof(buffer_msg),
                              "GET %s returned %lld of %d bytes (HTTP %ld)",
                              display_path_.c_str(),
                              static_cast<long long>(context.copied), want,
                              context.status);
                last_error = buffer_msg;
            }
        }
        if (attempt < options_.max_retries) {
            std::this_thread::sleep_for(std::chrono::milliseconds(
                50 * (1 << (attempt < 5 ? attempt : 5))));
        }
    }
    (void)last_error;
    return common::E_FILE_READ_ERR;
}

void S3ReadFile::close() {
    bool expected = false;
    if (!closed_.compare_exchange_strong(expected, true)) {
        return;
    }
    std::vector<CURL*> handles;
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        handles.swap(pooled_handles_);
    }
    for (size_t i = 0; i < handles.size(); i++) {
        curl_easy_cleanup(handles[i]);
    }
    if (share_ != nullptr) {
        curl_share_cleanup(share_);
        share_ = nullptr;
    }
}

}  // namespace

bool s3_support_available() { return true; }

std::unique_ptr<RandomAccessReadFile> open_s3_read_file(
    const S3Options& options, std::string* error_message) {
    if (options.endpoint.empty()) {
        if (error_message != nullptr) {
            *error_message = "S3 endpoint is required";
        }
        return std::unique_ptr<RandomAccessReadFile>();
    }
    if (options.bucket.empty() || options.key.empty()) {
        if (error_message != nullptr) {
            *error_message = "S3 bucket and key are required";
        }
        return std::unique_ptr<RandomAccessReadFile>();
    }
    S3Options resolved = options;
    if (resolved.region.empty()) {
        resolved.region = s3_derive_region_from_endpoint(resolved.endpoint);
    }
    if (!resolved.access_key_id.empty() &&
        !resolved.secret_access_key.empty() && resolved.region.empty()) {
        if (error_message != nullptr) {
            *error_message =
                "S3 region is required for signed requests; pass it explicitly "
                "or use an endpoint whose host names the region";
        }
        return std::unique_ptr<RandomAccessReadFile>();
    }
    std::unique_ptr<S3ReadFile> file(new S3ReadFile(resolved));
    const int ret = file->open(error_message);
    if (ret != common::E_OK) {
        return std::unique_ptr<RandomAccessReadFile>();
    }
    return std::unique_ptr<RandomAccessReadFile>(file.release());
}

#else  // !TSFILE_ENABLE_S3

bool s3_support_available() { return false; }

std::unique_ptr<RandomAccessReadFile> open_s3_read_file(
    const S3Options& /* options */, std::string* error_message) {
    if (error_message != nullptr) {
        *error_message =
            "native S3/OSS support is not built into this library "
            "(libcurl was not available at build time)";
    }
    return std::unique_ptr<RandomAccessReadFile>();
}

#endif  // TSFILE_ENABLE_S3

}  // end namespace storage
