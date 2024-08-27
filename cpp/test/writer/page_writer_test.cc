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
#include "writer/page_writer.h"

#include <gtest/gtest.h>

#include "common/allocator/byte_stream.h"
#include "common/statistic.h"

using namespace storage;
using namespace common;

class PageWriterTest : public ::testing::Test {};

TEST_F(PageWriterTest, WriteBooleanSuccess) {
    PageWriter page_writer;
    page_writer.init(TSDataType::BOOLEAN, TSEncoding::PLAIN, UNCOMPRESSED);
    int result = page_writer.write(1234567890, true);
    EXPECT_EQ(result, E_OK);
}

TEST_F(PageWriterTest, WriteInt32Success) {
    PageWriter page_writer;
    page_writer.init(TSDataType::INT32, TSEncoding::PLAIN, UNCOMPRESSED);
    int result = page_writer.write(1234567890, int32_t(42));
    EXPECT_EQ(result, E_OK);
}

TEST_F(PageWriterTest, WriteInt64Success) {
    PageWriter page_writer;
    page_writer.init(TSDataType::INT64, TSEncoding::PLAIN, UNCOMPRESSED);
    int result = page_writer.write(1234567890, int64_t(42));
    EXPECT_EQ(result, E_OK);
}

TEST_F(PageWriterTest, WriteFloatSuccess) {
    PageWriter page_writer;
    page_writer.init(TSDataType::FLOAT, TSEncoding::PLAIN, UNCOMPRESSED);
    int result = page_writer.write(1234567890, float(42.0));
    EXPECT_EQ(result, E_OK);
}

TEST_F(PageWriterTest, WriteDoubleSuccess) {
    PageWriter page_writer;
    page_writer.init(TSDataType::DOUBLE, TSEncoding::PLAIN, UNCOMPRESSED);
    int result = page_writer.write(1234567890, double(42.0));
    EXPECT_EQ(result, E_OK);
}

TEST_F(PageWriterTest, WriteLargeDataSet) {
    PageWriter page_writer;
    page_writer.init(TSDataType::DOUBLE, TSEncoding::PLAIN, UNCOMPRESSED);
    for (int i = 0; i < 10000; ++i) {
        page_writer.write(i, double(i * 0.1));
    }
    EXPECT_EQ(page_writer.get_point_numer(), 10000);
}

TEST_F(PageWriterTest, ResetPageWriter) {
    PageWriter page_writer;
    page_writer.init(TSDataType::INT64, TSEncoding::PLAIN, UNCOMPRESSED);
    page_writer.write(1234567890, int64_t(42));
    page_writer.reset();
    EXPECT_EQ(page_writer.get_point_numer(), 0);
    EXPECT_EQ(page_writer.get_time_out_stream_size(), 0);
}

TEST_F(PageWriterTest, DestroyPageWriter) {
    PageWriter page_writer;
    page_writer.init(TSDataType::INT64, TSEncoding::PLAIN, UNCOMPRESSED);
    page_writer.write(1234567890, int64_t(42));
    Int64Statistic* stat = (Int64Statistic*)page_writer.get_statistic();
    EXPECT_TRUE(stat);
    EXPECT_EQ(stat->count_, 1);
}