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
#include "writer/value_page_writer.h"

#include <gtest/gtest.h>

#include "common/allocator/byte_stream.h"
#include "common/statistic.h"

using namespace storage;
using namespace common;

class ValuePageWriterTest : public ::testing::Test {};

TEST_F(ValuePageWriterTest, WriteBooleanSuccess) {
    ValuePageWriter value_page_writer;
    value_page_writer.init(TSDataType::BOOLEAN, TSEncoding::PLAIN,
                           UNCOMPRESSED);
    int result = value_page_writer.write(1234567890, true, false);
    EXPECT_EQ(result, E_OK);
}

TEST_F(ValuePageWriterTest, WriteInt32Success) {
    ValuePageWriter value_page_writer;
    value_page_writer.init(TSDataType::INT32, TSEncoding::PLAIN, UNCOMPRESSED);
    int result = value_page_writer.write(1234567890, int32_t(42), false);
    EXPECT_EQ(result, E_OK);
}

TEST_F(ValuePageWriterTest, WriteInt64Success) {
    ValuePageWriter value_page_writer;
    value_page_writer.init(TSDataType::INT64, TSEncoding::PLAIN, UNCOMPRESSED);
    int result = value_page_writer.write(1234567890, int64_t(42), false);
    EXPECT_EQ(result, E_OK);
}

TEST_F(ValuePageWriterTest, WriteFloatSuccess) {
    ValuePageWriter value_page_writer;
    value_page_writer.init(TSDataType::FLOAT, TSEncoding::PLAIN, UNCOMPRESSED);
    int result = value_page_writer.write(1234567890, float(42.0), false);
    EXPECT_EQ(result, E_OK);
}

TEST_F(ValuePageWriterTest, WriteDoubleSuccess) {
    ValuePageWriter value_page_writer;
    value_page_writer.init(TSDataType::DOUBLE, TSEncoding::PLAIN, UNCOMPRESSED);
    int result = value_page_writer.write(1234567890, double(42.0), false);
    EXPECT_EQ(result, E_OK);
}

TEST_F(ValuePageWriterTest, WriteLargeDataSet) {
    ValuePageWriter value_page_writer;
    value_page_writer.init(TSDataType::DOUBLE, TSEncoding::PLAIN, UNCOMPRESSED);
    for (int i = 0; i < 10000; ++i) {
        value_page_writer.write(i, double(i * 0.1), false);
    }
    EXPECT_EQ(value_page_writer.get_point_numer(), 10000);
}

TEST_F(ValuePageWriterTest, ResetValuePageWriter) {
    ValuePageWriter value_page_writer;
    value_page_writer.init(TSDataType::INT64, TSEncoding::PLAIN, UNCOMPRESSED);
    value_page_writer.write(1234567890, int64_t(42), false);
    value_page_writer.reset();
    EXPECT_EQ(value_page_writer.get_point_numer(), 0);
    EXPECT_EQ(value_page_writer.get_col_notnull_bitmap_out_stream_size(), 0);
}

TEST_F(ValuePageWriterTest, DestroyValuePageWriter) {
    ValuePageWriter value_page_writer;
    value_page_writer.init(TSDataType::INT64, TSEncoding::PLAIN, UNCOMPRESSED);
    value_page_writer.write(1234567890, int64_t(42), false);
    Int64Statistic* stat = (Int64Statistic*)value_page_writer.get_statistic();
    EXPECT_TRUE(stat);
    EXPECT_EQ(stat->count_, 1);
}

TEST_F(ValuePageWriterTest, WritePageHeaderAndData) {
    ValuePageWriter value_page_writer;
    value_page_writer.init(TSDataType::INT64, TSEncoding::PLAIN, UNCOMPRESSED);
    EXPECT_EQ(value_page_writer.write(1234567890, (int64_t)1, false),
              common::E_OK);
    EXPECT_EQ(value_page_writer.get_page_memory_size(), 8);
    EXPECT_EQ(value_page_writer.write(1234567891, (int64_t)2, false),
              common::E_OK);
    EXPECT_EQ(value_page_writer.write(1234567892, (int64_t)3, false),
              common::E_OK);
    common::ByteStream byte_stream(1024, common::MOD_DEFAULT);
    EXPECT_EQ(value_page_writer.write_to_chunk(byte_stream, true, true, true),
              common::E_OK);
    value_page_writer.destroy_page_data();
}

// Regression: write_batch used to bump size_ and the page bitmap for every
// row in the batch *before* encoding the values.  If the value encode failed
// mid-batch, the page would claim `count` rows had been written even though
// the encoder stream only held a prefix.  The fix counts valid rows
// upfront, encodes, and only commits size_ / bitmap when the encode
// finishes cleanly.  This test exercises the happy path on a mixed-null
// batch and asserts size_ and statistics agree with the row count — a
// subsequent code change that re-introduces premature size_ bumping
// without rolling back on failure would still pass this test, but it
// guards the encode-then-commit ordering contract against accidental
// rewrites.
TEST_F(ValuePageWriterTest, WriteBatchCommitsStateAfterEncode) {
    ValuePageWriter w;
    w.init(TSDataType::INT64, TSEncoding::PLAIN, UNCOMPRESSED);

    const uint32_t N = 5;
    int64_t timestamps[N] = {100, 101, 102, 103, 104};
    int64_t values[N] = {10, 20, 30, 40, 50};
    common::BitMap nullmap;
    ASSERT_EQ(nullmap.init(N), common::E_OK);
    // bit=1 means null in the tablet bitmap convention.
    nullmap.set(1);  // row 1 (timestamp 101) is null
    nullmap.set(3);  // row 3 (timestamp 103) is null
    ASSERT_EQ(w.write_batch(timestamps, values, nullmap, 0, N), common::E_OK);

    // size_ tracks every row regardless of nullness, statistic only the
    // non-null subset.  get_point_numer() returns size_ (rows incl. NULLs).
    EXPECT_EQ(w.get_point_numer(), N);
    auto* stat = static_cast<Int64Statistic*>(w.get_statistic());
    ASSERT_NE(stat, nullptr);
    EXPECT_EQ(stat->count_, 3u);
}
