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
#include <gtest/gtest.h>
#include <unistd.h>

#include "cwrapper/TsFile-cwrapper.h"
#include "utils/errno_define.h"

using namespace common;

namespace cwrapper {
#define TSFILE_NAME "cwrapper.tsfile"
class CWrapperTest : public testing::Test {};

TEST_F(CWrapperTest, write_tsfile) {
    ErrorCode code = 0;
    CTsFileWriter writer = ts_writer_open(TSFILE_NAME, &code);
    ASSERT_EQ(code, 0);
    ASSERT_NE(writer, nullptr);
    // open again
    writer = ts_writer_open(TSFILE_NAME, &code);
    ASSERT_EQ(code, E_ALREADY_EXIST);
    ts_writer_close(writer);
    ASSERT_EQ(writer, nullptr);
    ASSERT_EQ(access(TSFILE_NAME, F_OK), 0);

    writer = ts_writer_open(TSFILE_NAME, &code);
    ASSERT_EQ(code, E_ALREADY_EXIST);
    ASSERT_EQ(writer, nullptr);
}
}  // namespace cwrapper