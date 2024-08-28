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
#include "writer/chunk_writer.h"

#include <gtest/gtest.h>

#include "common/allocator/byte_stream.h"
#include "common/config/config.h"
#include "common/statistic.h"
#include "compress/compressor.h"
#include "encoding/encoder.h"

using namespace storage;
using namespace common;

class ChunkWriterTest : public ::testing::Test {
   protected:
    ChunkWriter chunk_writer;
    ColumnDesc col_desc;

    void SetUp() override {
        col_desc.column_name_ = "test_measurement";
        col_desc.type_ = TSDataType::DOUBLE;
        col_desc.encoding_ = TSEncoding::PLAIN;
        col_desc.compression_ = CompressionType::UNCOMPRESSED;

        ASSERT_EQ(chunk_writer.init(col_desc), E_OK);
    }

    void TearDown() override { chunk_writer.destroy(); }
};

TEST_F(ChunkWriterTest, InitWithColumnDesc) {
    EXPECT_EQ(chunk_writer.init(col_desc), E_OK);
}

TEST_F(ChunkWriterTest, InitWithParameters) {
    ChunkWriter writer;
    EXPECT_EQ(writer.init("test_measurement", TSDataType::DOUBLE,
                          TSEncoding::PLAIN, CompressionType::UNCOMPRESSED),
              E_OK);
    writer.destroy();
}

TEST_F(ChunkWriterTest, WriteBoolean) {
    EXPECT_EQ(chunk_writer.write(1234567890, true), E_TYPE_NOT_MATCH);
}

TEST_F(ChunkWriterTest, WriteInt32) {
    EXPECT_EQ(chunk_writer.write(1234567890, int32_t(42)), E_TYPE_NOT_MATCH);
}

TEST_F(ChunkWriterTest, WriteInt64) {
    EXPECT_EQ(chunk_writer.write(1234567890, int64_t(42)), E_TYPE_NOT_MATCH);
}

TEST_F(ChunkWriterTest, WriteFloat) {
    EXPECT_EQ(chunk_writer.write(1234567890, float(42.0)), E_TYPE_NOT_MATCH);
}

TEST_F(ChunkWriterTest, WriteDouble) {
    EXPECT_EQ(chunk_writer.write(1234567890, double(42.0)), E_OK);
}

TEST_F(ChunkWriterTest, WriteLargeDataSet) {
    for (int i = 0; i < 10000; ++i) {
        chunk_writer.write(i, double(i * 0.1));
    }
    EXPECT_EQ(chunk_writer.get_chunk_statistic()->count_, 10000);
}

TEST_F(ChunkWriterTest, EndEncodeChunk) {
    chunk_writer.write(1234567890, double(42.0));
    EXPECT_EQ(chunk_writer.end_encode_chunk(), E_OK);
    EXPECT_GT(chunk_writer.get_chunk_data().total_size(), 0);
}

TEST_F(ChunkWriterTest, DestroyChunkWriter) {
    chunk_writer.write(1234567890, double(42.0));
    chunk_writer.destroy();
    EXPECT_EQ(chunk_writer.get_chunk_statistic(), nullptr);
    EXPECT_EQ(chunk_writer.get_chunk_data().total_size(), 0);
}