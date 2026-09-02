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

#include "file/read_file.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include "common/global.h"
#include "utils/injection.h"

namespace {

std::string process_temp_path(const char* stem) {
    std::ostringstream path;
    path << ::testing::TempDir() << stem << "_";
#ifdef _WIN32
    path << _getpid();
#else
    path << getpid();
#endif
    path << ".tsfile";
    return path.str();
}

class BackendGuard {
   public:
    BackendGuard() : original_(common::get_file_read_backend()) {}
    ~BackendGuard() { common::set_file_read_backend(original_); }

   private:
    common::FileReadBackend original_;
};

class InjectionGuard {
   public:
    explicit InjectionGuard(const char* point) : point_(point) {
        common::enable_injection(point_, 0);
    }
    ~InjectionGuard() { common::disable_injection(point_); }

   private:
    const char* point_;
};

class ReadFileBackendTest : public ::testing::Test {
   protected:
    void SetUp() override {
        content_ = "TsFile";
        content_.push_back('\x03');
        content_ += "read-backend-payload";
        content_ += "TsFile";
        write_file(file_name_, content_);
    }

    void TearDown() override {
        std::remove(file_name_.c_str());
        std::remove(empty_file_name_.c_str());
    }

    static void write_file(const std::string& path,
                           const std::string& content) {
        std::ofstream output(
            path.c_str(), std::ios::out | std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(output.is_open());
        output.write(content.data(),
                     static_cast<std::streamsize>(content.size()));
        ASSERT_TRUE(output.good());
    }

    BackendGuard backend_guard_;
    const std::string file_name_ = process_temp_path("read_file_backend_test");
    const std::string empty_file_name_ =
        process_temp_path("read_file_backend_empty");
    std::string content_;
};

TEST_F(ReadFileBackendTest, PreadIsTheDefaultBackend) {
    EXPECT_EQ(common::get_file_read_backend(), common::FileReadBackend::PREAD);
}

TEST_F(ReadFileBackendTest, PreadPreservesPositionedReadBehavior) {
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::PREAD),
              common::E_OK);
    storage::ReadFile file;
    ASSERT_EQ(file.open(file_name_), common::E_OK);
    EXPECT_TRUE(file.is_opened());
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::PREAD);
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::PREAD);

    std::vector<char> buffer(content_.size() + 8, '\0');
    int32_t read_len = -1;
    ASSERT_EQ(file.read(0, buffer.data(), static_cast<int32_t>(buffer.size()),
                        read_len),
              common::E_OK);
    EXPECT_EQ(read_len, static_cast<int32_t>(content_.size()));
    EXPECT_EQ(std::string(buffer.data(), static_cast<size_t>(read_len)),
              content_);

    EXPECT_EQ(file.read(-1, buffer.data(), 1, read_len), common::E_INVALID_ARG);
    EXPECT_EQ(file.read(0, nullptr, 1, read_len), common::E_INVALID_ARG);
    EXPECT_EQ(file.read(0, nullptr, 0, read_len), common::E_OK);
    EXPECT_EQ(read_len, 0);
}

TEST_F(ReadFileBackendTest, MmapReadsBoundedRangesAndReleasesResources) {
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    storage::ReadFile file;
    ASSERT_EQ(file.open(file_name_), common::E_OK);
    EXPECT_TRUE(file.is_opened());
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::MMAP);

    char tail[16] = {};
    int32_t read_len = -1;
    const int64_t offset = static_cast<int64_t>(content_.size()) - 3;
    ASSERT_EQ(file.read(offset, tail, sizeof(tail), read_len), common::E_OK);
    EXPECT_EQ(read_len, 3);
    EXPECT_EQ(std::string(tail, static_cast<size_t>(read_len)),
              content_.substr(content_.size() - 3));

    uint64_t size = 0;
    uint64_t fingerprint = 0;
    // MMAP releases the descriptor before open() returns, so generation()
    // must use the snapshot captured while the mapping was established.
    EXPECT_EQ(file.generation(size, fingerprint), common::E_OK);
    EXPECT_EQ(size, content_.size());
    EXPECT_NE(fingerprint, 0u);

    file.close();
    EXPECT_FALSE(file.is_opened());
    EXPECT_EQ(std::remove(file_name_.c_str()), 0);
}

TEST_F(ReadFileBackendTest, AutoPrefersMmapAndCloseIsIdempotent) {
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::AUTO),
              common::E_OK);
    storage::ReadFile file;
    ASSERT_EQ(file.open(file_name_), common::E_OK);
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::MMAP);

    file.close();
    file.close();
    EXPECT_FALSE(file.is_opened());

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::PREAD),
              common::E_OK);
    ASSERT_EQ(file.open(file_name_), common::E_OK);
    EXPECT_EQ(file.active_backend(), common::FileReadBackend::PREAD);
}

TEST_F(ReadFileBackendTest, AutoFallsBackButRequiredMmapReportsFailure) {
    InjectionGuard mmap_failure("read_file_mmap_fail");

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::AUTO),
              common::E_OK);
    storage::ReadFile automatic;
    ASSERT_EQ(automatic.open(file_name_), common::E_OK);
    EXPECT_EQ(automatic.active_backend(), common::FileReadBackend::PREAD);
    automatic.close();

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    storage::ReadFile required;
    EXPECT_EQ(required.open(file_name_), common::E_FILE_MAP_ERR);
    EXPECT_FALSE(required.is_opened());
}

TEST_F(ReadFileBackendTest, AutoFallsBackButRequiredMmapReportsUnsupported) {
    InjectionGuard mmap_unsupported("read_file_mmap_unsupported");

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::AUTO),
              common::E_OK);
    storage::ReadFile automatic;
    ASSERT_EQ(automatic.open(file_name_), common::E_OK);
    EXPECT_EQ(automatic.active_backend(), common::FileReadBackend::PREAD);
    automatic.close();

    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    storage::ReadFile required;
    EXPECT_EQ(required.open(file_name_), common::E_NOT_SUPPORT);
    EXPECT_FALSE(required.is_opened());
}

TEST_F(ReadFileBackendTest, EmptyFileIsRejectedBeforeMapping) {
    write_file(empty_file_name_, "");
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::MMAP),
              common::E_OK);
    storage::ReadFile file;
    EXPECT_EQ(file.open(empty_file_name_), common::E_TSFILE_CORRUPTED);
    EXPECT_FALSE(file.is_opened());
}

TEST_F(ReadFileBackendTest, InvalidConfigurationDoesNotChangeBackend) {
    ASSERT_EQ(common::set_file_read_backend(common::FileReadBackend::PREAD),
              common::E_OK);
    EXPECT_EQ(
        common::set_file_read_backend(static_cast<common::FileReadBackend>(99)),
        common::E_INVALID_ARG);
    EXPECT_EQ(common::get_file_read_backend(), common::FileReadBackend::PREAD);
}

}  // namespace
