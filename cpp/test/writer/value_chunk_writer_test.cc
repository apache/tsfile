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
#include "writer/value_chunk_writer.h"

#include <gtest/gtest.h>

#include <vector>

#include "common/allocator/byte_stream.h"
#include "common/config/config.h"
#include "common/statistic.h"

using namespace storage;
using namespace common;

class ValueChunkWriterTest : public ::testing::Test {
   protected:
    ValueChunkWriter value_chunk_writer;
    ColumnSchema col_schema;

    void SetUp() override {
        col_schema.column_name_ = "test_measurement";
        col_schema.data_type_ = TSDataType::DOUBLE;
        col_schema.encoding_ = TSEncoding::PLAIN;
        col_schema.compression_ = CompressionType::UNCOMPRESSED;

        ASSERT_EQ(value_chunk_writer.init(col_schema), E_OK);
    }

    void TearDown() override { value_chunk_writer.destroy(); }
};

TEST_F(ValueChunkWriterTest, InitWithParameters) {
    ValueChunkWriter writer;
    EXPECT_EQ(writer.init("test_measurement", TSDataType::DOUBLE,
                          TSEncoding::PLAIN, CompressionType::UNCOMPRESSED),
              E_OK);
    writer.destroy();
}

TEST_F(ValueChunkWriterTest, WriteBoolean) {
    EXPECT_EQ(value_chunk_writer.write(1234567890, true, false),
              E_TYPE_NOT_MATCH);
}

TEST_F(ValueChunkWriterTest, WriteInt32) {
    EXPECT_EQ(value_chunk_writer.write(1234567890, int32_t(42), false),
              E_TYPE_NOT_MATCH);
}

TEST_F(ValueChunkWriterTest, WriteInt64) {
    EXPECT_EQ(value_chunk_writer.write(1234567890, int64_t(42), false),
              E_TYPE_NOT_MATCH);
}

TEST_F(ValueChunkWriterTest, WriteFloat) {
    EXPECT_EQ(value_chunk_writer.write(1234567890, float(42.0), false),
              E_TYPE_NOT_MATCH);
}

TEST_F(ValueChunkWriterTest, WriteDouble) {
    EXPECT_EQ(value_chunk_writer.write(1234567890, double(42.0), false), E_OK);
}

TEST_F(ValueChunkWriterTest, WriteLargeDataSet) {
    for (int i = 0; i < 10000; ++i) {
        value_chunk_writer.write(i, double(i * 0.1), false);
    }
    EXPECT_EQ(value_chunk_writer.get_chunk_statistic()->count_, 10000);
}

TEST_F(ValueChunkWriterTest, EndEncodeChunk) {
    value_chunk_writer.write(1234567890, double(42.0), false);
    EXPECT_EQ(value_chunk_writer.end_encode_chunk(), E_OK);
    EXPECT_GT(value_chunk_writer.get_chunk_data().total_size(), 0);
}

TEST_F(ValueChunkWriterTest, DestroyChunkWriter) {
    value_chunk_writer.write(1234567890, double(42.0), false);
    value_chunk_writer.destroy();
    EXPECT_EQ(value_chunk_writer.get_chunk_statistic(), nullptr);
    EXPECT_EQ(value_chunk_writer.get_chunk_data().total_size(), 0);
}

TEST_F(ValueChunkWriterTest, EmptyPagesDoNotHaveStatisticsOrData) {
    const std::vector<std::vector<bool>> patterns{
        {true},
        {true, true},
        {true, false},
        {false, true},
        {true, true, false, true, false, true}};
    for (const auto& pattern : patterns) {
        for (bool seal_last_page : {false, true}) {
            SCOPED_TRACE(::testing::Message()
                         << "pages=" << pattern.size()
                         << " seal_last_page=" << seal_last_page);
            ValueChunkWriter writer;
            ASSERT_EQ(writer.init("value", DOUBLE, GORILLA, UNCOMPRESSED),
                      E_OK);
            writer.set_enable_page_seal_if_full(false);
            int64_t timestamp = 0;
            for (size_t page = 0; page < pattern.size(); ++page) {
                for (int row = 0; row < 9; ++row) {
                    ASSERT_EQ(writer.write(timestamp++, 42.0, pattern[page]),
                              E_OK);
                }
                if (page + 1 < pattern.size() || seal_last_page) {
                    ASSERT_EQ(writer.seal_current_page(), E_OK);
                }
            }
            ASSERT_EQ(writer.end_encode_chunk(), E_OK);
            ASSERT_EQ(writer.num_of_pages(), pattern.size());
            ByteStream& data = writer.get_chunk_data();
            for (bool all_null : pattern) {
                const auto start = data.read_pos();
                PageHeader header;
                ASSERT_EQ(
                    header.deserialize_from(data, pattern.size() > 1, DOUBLE),
                    E_OK);
                if (all_null) {
                    EXPECT_EQ(data.read_pos() - start, 1u);
                    EXPECT_EQ(header.uncompressed_size_, 0u);
                    EXPECT_EQ(header.compressed_size_, 0u);
                    EXPECT_EQ(header.statistic_, nullptr);
                } else {
                    ASSERT_GT(header.compressed_size_, 0u);
                    if (pattern.size() > 1) {
                        ASSERT_NE(header.statistic_, nullptr);
                        EXPECT_EQ(header.statistic_->count_, 9u);
                    }
                    std::vector<char> payload(header.compressed_size_);
                    uint32_t read = 0;
                    ASSERT_EQ(
                        data.read_buf(payload.data(), payload.size(), read),
                        E_OK);
                    ASSERT_EQ(read, payload.size());
                }
            }
            EXPECT_EQ(data.remaining_size(), 0u);
        }
    }
}
