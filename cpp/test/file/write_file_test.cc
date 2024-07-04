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

TEST_F(WriteFileTest, WriteToFile) {
    WriteFile write_file;
    std::string file_name = "test_file_write.dat";

    remove(file_name.c_str());

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;
    EXPECT_EQ(write_file.create(file_name, flags, mode), E_OK);
    EXPECT_TRUE(write_file.file_opened());

    const char *content = "Hello, World!";
    uint32_t content_len = strlen(content);
    EXPECT_EQ(write_file.write(content, content_len), E_OK);

    write_file.close();

    std::ifstream file(file_name);
    std::string file_content((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
    EXPECT_EQ(file_content, content);

    remove(file_name.c_str());
}

TEST_F(WriteFileTest, SyncFile) {
    WriteFile write_file;
    std::string file_name = "test_file_sync.dat";

    remove(file_name.c_str());

    int flags = O_WRONLY | O_CREAT | O_TRUNC;
    mode_t mode = 0666;
    EXPECT_EQ(write_file.create(file_name, flags, mode), E_OK);
    EXPECT_TRUE(write_file.file_opened());

    const char *content = "Hello, Sync!";
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

    const char *content = "Closing file.";
    uint32_t content_len = strlen(content);
    EXPECT_EQ(write_file.write(content, content_len), E_OK);
    EXPECT_EQ(write_file.close(), E_OK);
}
