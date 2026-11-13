/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include "file/memory_random_access_write_file.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <string>

#include "file/write_file.h"

using namespace storage;
using namespace common;

namespace {

std::string data_as_string(const MemoryRandomAccessWriteFile& sink) {
    return std::string(sink.data().begin(), sink.data().end());
}

}  // namespace

TEST(MemoryRandomAccessWriteFileTest, StartsOpenAndEmpty) {
    MemoryRandomAccessWriteFile sink("memory://test.tsfile");
    EXPECT_TRUE(sink.is_opened());
    EXPECT_EQ(sink.get_position(), 0);
    EXPECT_EQ(sink.size(), 0);
    EXPECT_TRUE(sink.data().empty());
    EXPECT_EQ(sink.file_path(), "memory://test.tsfile");
    EXPECT_EQ(sink.sync(), E_OK);
}

TEST(MemoryRandomAccessWriteFileTest, SequentialWritesAdvancePosition) {
    MemoryRandomAccessWriteFile sink;
    ASSERT_EQ(sink.write("hello", 5), E_OK);
    EXPECT_EQ(sink.get_position(), 5);
    ASSERT_EQ(sink.write(" world", 6), E_OK);
    EXPECT_EQ(sink.get_position(), 11);
    EXPECT_EQ(sink.size(), 11);
    EXPECT_EQ(data_as_string(sink), "hello world");
}

TEST(MemoryRandomAccessWriteFileTest, WriteAfterCloseFailsAndKeepsBytes) {
    MemoryRandomAccessWriteFile sink;
    ASSERT_EQ(sink.write("abc", 3), E_OK);
    sink.close();
    EXPECT_FALSE(sink.is_opened());
    EXPECT_EQ(sink.write("more", 4), E_FILE_WRITE_ERR);
    EXPECT_EQ(sink.sync(), E_FILE_WRITE_ERR);
    EXPECT_EQ(sink.truncate(1), E_FILE_WRITE_ERR);
    EXPECT_EQ(sink.seek_to_end(), E_FILE_WRITE_ERR);
    EXPECT_EQ(sink.get_position(), 0);
    // The bytes survive close() so the caller can still obtain the TsFile.
    EXPECT_EQ(data_as_string(sink), "abc");
    // close() is idempotent.
    sink.close();
    EXPECT_FALSE(sink.is_opened());
}

TEST(MemoryRandomAccessWriteFileTest, TruncateShrinksAndKeepsCursor) {
    MemoryRandomAccessWriteFile sink;
    const char* content = "Hello, Truncate World!";
    const uint32_t len = static_cast<uint32_t>(std::strlen(content));
    ASSERT_EQ(sink.write(content, len), E_OK);
    ASSERT_EQ(sink.truncate(7), E_OK);
    EXPECT_EQ(sink.size(), 7);
    EXPECT_EQ(data_as_string(sink), "Hello, ");
    // ftruncate() does not move the cursor; neither does the sink.
    EXPECT_EQ(sink.get_position(), static_cast<int64_t>(len));
    ASSERT_EQ(sink.seek_to_end(), E_OK);
    EXPECT_EQ(sink.get_position(), 7);
    ASSERT_EQ(sink.write("X", 1), E_OK);
    EXPECT_EQ(data_as_string(sink), "Hello, X");
}

TEST(MemoryRandomAccessWriteFileTest, TruncateBeyondEndZeroFills) {
    MemoryRandomAccessWriteFile sink;
    ASSERT_EQ(sink.write("abc", 3), E_OK);
    ASSERT_EQ(sink.truncate(6), E_OK);
    EXPECT_EQ(sink.size(), 6);
    const std::vector<char>& bytes = sink.data();
    EXPECT_EQ(bytes[0], 'a');
    EXPECT_EQ(bytes[1], 'b');
    EXPECT_EQ(bytes[2], 'c');
    EXPECT_EQ(bytes[3], 0);
    EXPECT_EQ(bytes[4], 0);
    EXPECT_EQ(bytes[5], 0);

    // A write through the hole keeps the zero bytes in between.
    ASSERT_EQ(sink.truncate(0), E_OK);
    ASSERT_EQ(sink.seek_to_end(), E_OK);
    ASSERT_EQ(sink.write("a", 1), E_OK);
    ASSERT_EQ(sink.truncate(3), E_OK);
    ASSERT_EQ(sink.seek_to_end(), E_OK);
    ASSERT_EQ(sink.write("d", 1), E_OK);
    EXPECT_EQ(sink.size(), 4);
    EXPECT_EQ(sink.data()[0], 'a');
    EXPECT_EQ(sink.data()[1], 0);
    EXPECT_EQ(sink.data()[2], 0);
    EXPECT_EQ(sink.data()[3], 'd');
}

TEST(MemoryRandomAccessWriteFileTest, RejectsInvalidArguments) {
    MemoryRandomAccessWriteFile sink;
    EXPECT_EQ(sink.write(nullptr, 1), E_INVALID_ARG);
    EXPECT_EQ(sink.write(nullptr, 0), E_OK);
    EXPECT_EQ(sink.write("", 0), E_OK);
    EXPECT_EQ(sink.truncate(-1), E_INVALID_ARG);
    EXPECT_EQ(sink.get_position(), 0);
}

TEST(MemoryRandomAccessWriteFileTest, ReleaseDataMovesBytesOut) {
    MemoryRandomAccessWriteFile sink;
    ASSERT_EQ(sink.write("payload", 7), E_OK);
    std::vector<char> bytes = sink.release_data();
    EXPECT_EQ(std::string(bytes.begin(), bytes.end()), "payload");
    EXPECT_EQ(sink.size(), 0);
    EXPECT_EQ(sink.get_position(), 0);
    EXPECT_TRUE(sink.is_opened());
    ASSERT_EQ(sink.write("next", 4), E_OK);
    EXPECT_EQ(data_as_string(sink), "next");
}

// The fd-backed local backend must remain usable through the abstract
// interface, since that is how the writers now reach it.
TEST(RandomAccessWriteFileTest, LocalWriteFileThroughInterface) {
    const std::string file_name = "random_access_write_file_interface.dat";
    std::remove(file_name.c_str());

    WriteFile write_file;
    RandomAccessWriteFile* sink = &write_file;

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(write_file.create(file_name, flags, 0666), E_OK);
    ASSERT_EQ(sink->write("abc", 3), E_OK);
    EXPECT_EQ(sink->get_position(), 3);
    EXPECT_EQ(sink->sync(), E_OK);
    ASSERT_EQ(sink->seek_to_end(), E_OK);
    ASSERT_EQ(sink->write("def", 3), E_OK);
    ASSERT_EQ(sink->truncate(4), E_OK);
    sink->close();
    EXPECT_FALSE(sink->is_opened());
    EXPECT_EQ(sink->write("x", 1), E_FILE_WRITE_ERR);

    std::ifstream in(file_name, std::ios::binary);
    std::string content((std::istreambuf_iterator<char>(in)),
                        std::istreambuf_iterator<char>());
    EXPECT_EQ(content, "abcd");

    std::remove(file_name.c_str());
}
