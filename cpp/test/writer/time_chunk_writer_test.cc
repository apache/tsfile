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
#include "writer/time_chunk_writer.h"

#include <gtest/gtest.h>

#include "common/allocator/byte_stream.h"
#include "common/config/config.h"
#include "common/statistic.h"

using namespace storage;
using namespace common;

class TimeChunkWriterTest : public ::testing::Test {
   protected:
    TimeChunkWriter time_chunk_writer;
    ColumnDesc col_desc;

    void SetUp() override {
        col_desc.column_name_ = "time";
        col_desc.type_ = TSDataType::VECTOR;
        col_desc.encoding_ = TSEncoding::PLAIN;
        col_desc.compression_ = CompressionType::UNCOMPRESSED;

        ASSERT_EQ(time_chunk_writer.init(col_desc), E_OK);
    }

    void TearDown() override { time_chunk_writer.destroy(); }
};

TEST_F(TimeChunkWriterTest, InitWithColumnDesc) {
    EXPECT_EQ(time_chunk_writer.init(col_desc), E_OK);
}

TEST_F(TimeChunkWriterTest, InitWithParameters) {
    TimeChunkWriter writer;
    EXPECT_EQ(
        writer.init("time", TSEncoding::PLAIN, CompressionType::UNCOMPRESSED),
        E_OK);
    writer.destroy();
}

TEST_F(TimeChunkWriterTest, WriteBoolean) {
    EXPECT_EQ(time_chunk_writer.write(true), E_OK);
    EXPECT_EQ(time_chunk_writer.write(false), E_OK);
}

TEST_F(TimeChunkWriterTest, WriteLargeDataSet) {
    for (int i = 0; i < 10000; ++i) {
        time_chunk_writer.write(i);
    }
    EXPECT_EQ(time_chunk_writer.get_chunk_statistic()->count_, 10000);
}

TEST_F(TimeChunkWriterTest, EndEncodeChunk) {
    EXPECT_EQ(time_chunk_writer.write(1234567890), E_OK);
    EXPECT_EQ(time_chunk_writer.end_encode_chunk(), E_OK);
    EXPECT_GT(time_chunk_writer.get_chunk_data().total_size(), 0);
}

TEST_F(TimeChunkWriterTest, DestroyTimeChunkWriter) {
    time_chunk_writer.write(1234567890);
    time_chunk_writer.destroy();
    EXPECT_EQ(time_chunk_writer.get_chunk_statistic(), nullptr);
    EXPECT_EQ(time_chunk_writer.get_chunk_data().total_size(), 0);
}