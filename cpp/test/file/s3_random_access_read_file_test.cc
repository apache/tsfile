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

#include <gtest/gtest.h>

#include <memory>
#include <string>

namespace {

TEST(S3RandomAccessReadFileTest, RejectsMissingSettingsWithoutNetwork) {
    storage::S3Options options;
    std::string error;
    std::unique_ptr<storage::RandomAccessReadFile> file =
        storage::open_s3_read_file(options, &error);
    EXPECT_TRUE(file == nullptr);
    EXPECT_FALSE(error.empty());

    options.endpoint = "https://127.0.0.1:1";
    error.clear();
    file = storage::open_s3_read_file(options, &error);
    EXPECT_TRUE(file == nullptr);
    EXPECT_FALSE(error.empty()) << "bucket/key are required";
}

TEST(S3RandomAccessReadFileTest, SignedRequestRequiresRegion) {
    storage::S3Options options;
    options.endpoint = "http://127.0.0.1:1";
    options.bucket = "bucket";
    options.key = "key.tsfile";
    options.access_key_id = "ak";
    options.secret_access_key = "sk";
    std::string error;
    std::unique_ptr<storage::RandomAccessReadFile> file =
        storage::open_s3_read_file(options, &error);
    EXPECT_TRUE(file == nullptr);
    EXPECT_NE(std::string::npos, error.find("region"));
}

TEST(S3RandomAccessReadFileTest, ReportsBuildSupport) {
#ifdef TSFILE_ENABLE_S3
    EXPECT_TRUE(storage::s3_support_available());
#else
    EXPECT_FALSE(storage::s3_support_available());
#endif
}

}  // namespace
