/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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
#include "file/write_file.h"

#include <fcntl.h>
#include <gtest/gtest.h>

#include <fstream>

using namespace storage;
using namespace common;

class WriteFileTest : public ::testing::Test {};

TEST_F(WriteFileTest, CreateFile) {
    WriteFile write_file;
    std::string file_name = "write_file_test.dat";

    remove(file_name.c_str());

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;
    EXPECT_EQ(write_file.create(file_name, flags, mode), E_OK);
    EXPECT_TRUE(write_file.file_opened());
    EXPECT_EQ(write_file.get_file_path(), file_name);

    write_file.close();
    remove(file_name.c_str());
}

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC push_options
#pragma GCC optimize("O0")
#endif
TEST_F(WriteFileTest, WriteToFile) {
    WriteFile write_file;
    std::string file_name = "test_file_write.dat";

    remove(file_name.c_str());

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;
    EXPECT_EQ(write_file.create(file_name, flags, mode), E_OK);
    EXPECT_TRUE(write_file.file_opened());

    const char* content = "Hello, World!";
    uint32_t content_len = strlen(content);
    EXPECT_EQ(write_file.write(content, content_len), E_OK);

    write_file.close();

    std::ifstream file(file_name);
    std::string file_content((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
    EXPECT_EQ(file_content, content);

    remove(file_name.c_str());
}
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC pop_options
#endif

TEST_F(WriteFileTest, SyncFile) {
    WriteFile write_file;
    std::string file_name = "test_file_sync.dat";

    remove(file_name.c_str());

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;
    EXPECT_EQ(write_file.create(file_name, flags, mode), E_OK);
    EXPECT_TRUE(write_file.file_opened());

    const char* content = "Hello, Sync!";
    uint32_t content_len = strlen(content);
    EXPECT_EQ(write_file.write(content, content_len), E_OK);
    EXPECT_EQ(write_file.sync(), E_OK);

    write_file.close();
    remove(file_name.c_str());
}

TEST_F(WriteFileTest, CloseFile) {
    WriteFile write_file;
    std::string file_name = "test_file_close.dat";

    remove(file_name.c_str());

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;
    EXPECT_EQ(write_file.create(file_name, flags, mode), E_OK);
    EXPECT_TRUE(write_file.file_opened());

    const char* content = "Closing file.";
    uint32_t content_len = strlen(content);
    EXPECT_EQ(write_file.write(content, content_len), E_OK);
    EXPECT_EQ(write_file.close(), E_OK);
}

// Truncate file to a given size (used by RestorableTsFileIOWriter after
// recovery).
TEST_F(WriteFileTest, TruncateFile) {
    WriteFile write_file;
    std::string file_name = "test_file_truncate.dat";

    remove(file_name.c_str());

    int flags = O_RDWR | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    mode_t mode = 0666;
    EXPECT_EQ(write_file.create(file_name, flags, mode), E_OK);
    EXPECT_TRUE(write_file.file_opened());

    const char* content = "Hello, Truncate World!";
    uint32_t content_len = strlen(content);
    EXPECT_EQ(write_file.write(content, content_len), E_OK);
    EXPECT_EQ(write_file.truncate(7), E_OK);
    write_file.close();

    std::ifstream file(file_name);
    std::string file_content((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
    EXPECT_EQ(file_content, "Hello, ");
    remove(file_name.c_str());
}

#include "file/tsfile_io_writer.h"

// Regression: TsFileIOWriter::init() used to leave destroyed_=true after a
// previous destroy(), so the second destroy() (during ~TsFileIOWriter())
// short-circuited and skipped meta_allocator_.destroy() /
// write_stream_.destroy() / file_ cleanup, leaking everything from the
// new lifecycle.  Verify init() rearms the lifecycle by checking destroy()
// runs again cleanly.
TEST(TsFileIOWriterLifecycle, DestroyInitDestroyIsClean) {
    std::string fn = "tsfile_iowriter_lifecycle.dat";
    remove(fn.c_str());

    WriteFile wf1;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(wf1.create(fn, flags, 0666), E_OK);

    TsFileIOWriter w;
    ASSERT_EQ(w.init(&wf1), E_OK);
    w.destroy();

    // Re-init against a fresh WriteFile (same writer object).  Under the
    // old bug, destroyed_ stays true here.
    remove(fn.c_str());
    WriteFile wf2;
    ASSERT_EQ(wf2.create(fn, flags, 0666), E_OK);
    ASSERT_EQ(w.init(&wf2), E_OK);

    // get_meta_size() reads meta_allocator_.get_total_used_bytes(); on a
    // fresh init() this should be 0 (the allocator was reinitialised).
    // If destroyed_ had been left true the allocator pages from before
    // would still be there.
    EXPECT_EQ(w.get_meta_size(), 0);

    // Trigger second destroy() — must not crash on the re-initialised
    // resources.
    w.destroy();

    wf2.close();
    remove(fn.c_str());
}
