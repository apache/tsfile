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
#include "writer/time_page_writer.h"

#include <gtest/gtest.h>

#include "common/allocator/byte_stream.h"
#include "common/statistic.h"
#include "encoding/plain_encoder.h"

using namespace storage;
using namespace common;

class TimePageWriterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        page_writer_ = new TimePageWriter();
        page_writer_->init(TSEncoding::PLAIN, UNCOMPRESSED);
    }

    void TearDown() override { delete page_writer_; }

    TimePageWriter* page_writer_;
};

TEST_F(TimePageWriterTest, WriteSuccess) {
    TimePageWriter page_writer;
    page_writer.init(TSEncoding::PLAIN, UNCOMPRESSED);
    int result = page_writer.write(1234567890);
    EXPECT_EQ(result, E_OK);
}

TEST_F(TimePageWriterTest, WriteLargeDataSet) {
    TimePageWriter page_writer;
    page_writer.init(TSEncoding::PLAIN, UNCOMPRESSED);
    for (int i = 0; i < 10000; ++i) {
        page_writer.write(i);
    }
    EXPECT_EQ(page_writer.get_point_numer(), 10000);
}

TEST_F(TimePageWriterTest, ResetTimePageWriter) {
    TimePageWriter page_writer;
    page_writer.init(TSEncoding::PLAIN, UNCOMPRESSED);
    page_writer.write(1234567890);
    page_writer.reset();
    EXPECT_EQ(page_writer.get_point_numer(), 0);
    EXPECT_EQ(page_writer.get_time_out_stream_size(), 0);
}

TEST_F(TimePageWriterTest, DestroyTimePageWriter) {
    TimePageWriter page_writer;
    page_writer.init(TSEncoding::PLAIN, UNCOMPRESSED);
    page_writer.write(1234567890);
    TimeStatistic* stat = (TimeStatistic*)page_writer.get_statistic();
    EXPECT_NE(stat, nullptr);
    EXPECT_EQ(stat->count_, 1);
}

TEST_F(TimePageWriterTest, WritePageHeaderAndData) {
    EXPECT_EQ(page_writer_->write(1L), common::E_OK);
    EXPECT_EQ(page_writer_->get_page_memory_size(), 8);
    EXPECT_EQ(page_writer_->write(2L), common::E_OK);
    EXPECT_EQ(page_writer_->write(3L), common::E_OK);
    common::ByteStream byte_stream(1024, common::MOD_DEFAULT);
    EXPECT_EQ(page_writer_->write_to_chunk(byte_stream, true, true, true),
              common::E_OK);
}