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

#include <gtest/gtest.h>

#include <string>

namespace {

storage::S3Credentials aws_example_credentials() {
    storage::S3Credentials credentials;
    credentials.access_key_id = "AKIAIOSFODNN7EXAMPLE";
    credentials.secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    credentials.region = "us-east-1";
    return credentials;
}

TEST(S3SigningTest, MatchesAwsGetObjectExample) {
    // AWS "Signature Calculation Examples" / S3 GET Object example.
    storage::S3CanonicalRequest request;
    request.method = "GET";
    request.canonical_uri = "/test.txt";
    request.canonical_query = "";
    request.payload_hash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    // Deliberately unsorted to exercise normalization.
    request.headers.push_back(std::make_pair("x-amz-date", "20130524T000000Z"));
    request.headers.push_back(
        std::make_pair("host", "examplebucket.s3.amazonaws.com"));
    request.headers.push_back(std::make_pair("range", "bytes=0-9"));
    request.headers.push_back(std::make_pair(
        "x-amz-content-sha256",
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));

    const std::string authorization = storage::s3_authorization_header(
        request, aws_example_credentials(), "20130524T000000Z", "s3");
    EXPECT_EQ(
        "AWS4-HMAC-SHA256 "
        "Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request,"
        "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date,"
        "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c603"
        "6bdb41",
        authorization);
}

TEST(S3SigningTest, AnonymousCredentialsProduceNoAuthorization) {
    storage::S3CanonicalRequest request;
    request.method = "GET";
    request.canonical_uri = "/bucket/key";
    request.payload_hash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    request.headers.push_back(std::make_pair("host", "example.com"));
    storage::S3Credentials credentials;
    EXPECT_TRUE(storage::s3_authorization_header(request, credentials,
                                                 "20240101T000000Z", "s3")
                    .empty());
}

TEST(S3SigningTest, SecurityTokenIsSignedWhenPresent) {
    storage::S3Credentials credentials = aws_example_credentials();
    credentials.session_token = "token-value";
    storage::S3CanonicalRequest request;
    request.method = "GET";
    request.canonical_uri = "/a";
    request.payload_hash =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    request.headers.push_back(std::make_pair("host", "h"));
    request.headers.push_back(
        std::make_pair("x-amz-security-token", credentials.session_token));
    const std::string authorization = storage::s3_authorization_header(
        request, credentials, "20240101T000000Z", "s3");
    EXPECT_NE(std::string::npos,
              authorization.find("SignedHeaders=host;x-amz-security-token"));
}

TEST(S3SigningTest, UriEncodeFollowsRfc3986) {
    EXPECT_EQ("a%20b/c%2Bd", storage::s3_uri_encode("a b/c+d", true));
    EXPECT_EQ("a%20b%2Fc%2Bd", storage::s3_uri_encode("a b/c+d", false));
    EXPECT_EQ("aA09-._~", storage::s3_uri_encode("aA09-._~", false));
    EXPECT_EQ("%E4%B8%AD", storage::s3_uri_encode("\xe4\xb8\xad", false));
}

TEST(S3SigningTest, DerivesRegionFromOssEndpoint) {
    EXPECT_EQ("cn-beijing", storage::s3_derive_region_from_endpoint(
                                "https://oss-cn-beijing.aliyuncs.com"));
    EXPECT_EQ("cn-hangzhou", storage::s3_derive_region_from_endpoint(
                                 "oss-cn-hangzhou-internal.aliyuncs.com"));
}

TEST(S3SigningTest, DerivesRegionFromAwsEndpoint) {
    EXPECT_EQ("us-west-2", storage::s3_derive_region_from_endpoint(
                               "https://s3.us-west-2.amazonaws.com"));
    EXPECT_EQ("eu-central-1",
              storage::s3_derive_region_from_endpoint(
                  "https://bucket.s3.eu-central-1.amazonaws.com"));
    EXPECT_EQ("us-east-1", storage::s3_derive_region_from_endpoint(
                               "https://s3.amazonaws.com"));
    EXPECT_EQ("ap-east-1", storage::s3_derive_region_from_endpoint(
                               "https://s3.ap-east-1.amazonaws.com"));
    EXPECT_TRUE(storage::s3_derive_region_from_endpoint("http://127.0.0.1:9000")
                    .empty());
}

TEST(S3SigningTest, ParsesObjectUris) {
    std::string bucket;
    std::string key;
    std::string error;
    EXPECT_TRUE(
        storage::s3_parse_uri("s3://bucket/a/b/c.tsfile", bucket, key, error));
    EXPECT_EQ("bucket", bucket);
    EXPECT_EQ("a/b/c.tsfile", key);
    EXPECT_TRUE(
        storage::s3_parse_uri("oss://b2/key.tsfile", bucket, key, error));
    EXPECT_EQ("b2", bucket);
    EXPECT_EQ("key.tsfile", key);

    EXPECT_FALSE(storage::s3_parse_uri("s3://bucket", bucket, key, error));
    EXPECT_FALSE(storage::s3_parse_uri("s3://bucket/", bucket, key, error));
    EXPECT_FALSE(
        storage::s3_parse_uri("/local/path.tsfile", bucket, key, error));
    EXPECT_FALSE(error.empty());
}

}  // namespace
