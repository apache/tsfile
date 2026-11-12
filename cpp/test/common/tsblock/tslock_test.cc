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

#include <cstring>

#include "common/container/byte_buffer.h"
#include "common/tsblock/tsblock.h"

namespace common {

TEST(TsBlockTest, Initialization) {
    TupleDesc tuple_desc;
    TsBlock ts_block(&tuple_desc);

    EXPECT_EQ(ts_block.get_row_count(), 0);
    EXPECT_EQ(ts_block.get_column_count(), 0);
    EXPECT_EQ(ts_block.get_max_row_count(), 0);
}

TEST(TsBlockTest, RowAppender_AddRow) {
    TupleDesc tuple_desc;
    ColumnSchema col;
    tuple_desc.push_back(col);
    TsBlock ts_block(&tuple_desc, 100);
    RowAppender row_appender(&ts_block);

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(row_appender.add_row());
        EXPECT_EQ(ts_block.get_row_count(), i + 1);
    }
}

TEST(TsBlockTest, ColAppender_AddRowAndAppend) {
    TupleDesc tuple_desc;
    ColumnSchema col("test_col", INT32, SNAPPY, RLE);
    tuple_desc.push_back(col);
    TsBlock ts_block(&tuple_desc, 50);
    ts_block.init();
    RowAppender row_appender(&ts_block);
    ColAppender col_appender(0, &ts_block);

    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(row_appender.add_row());
        EXPECT_TRUE(col_appender.add_row());
        int32_t val = i;
        col_appender.append((const char*)&val, sizeof(int32_t));
    }

    EXPECT_EQ(col_appender.get_col_row_count(), 50);
}

TEST(TsBlockTest, RowAppenderBulkAppendFixedAtExactCapacity) {
    TupleDesc tuple_desc;
    tuple_desc.push_back(ColumnSchema("time", TIMESTAMP, UNCOMPRESSED, PLAIN));
    tuple_desc.push_back(ColumnSchema("value", FLOAT, UNCOMPRESSED, PLAIN));
    TsBlock ts_block(&tuple_desc, 4);
    ASSERT_EQ(ts_block.init(), E_OK);
    RowAppender row_appender(&ts_block);

    const int64_t times[] = {101, 103, 107, 109};
    const float values[] = {1.25f, 2.5f, 3.75f, 5.0f};
    ASSERT_TRUE(row_appender.can_bulk_append_fixed(0, sizeof(int64_t)));
    ASSERT_TRUE(row_appender.can_bulk_append_fixed(1, sizeof(float)));
    EXPECT_FALSE(row_appender.can_bulk_append_fixed(1, sizeof(double)));

    row_appender.bulk_append_fixed(0, reinterpret_cast<const char*>(times), 4);
    row_appender.bulk_append_fixed(1, reinterpret_cast<const char*>(values), 4);
    row_appender.add_rows(4);

    EXPECT_EQ(ts_block.get_row_count(), 4u);
    EXPECT_EQ(row_appender.remaining(), 0u);
    EXPECT_EQ(ts_block.get_vector(0)->get_row_num(), 4u);
    EXPECT_EQ(ts_block.get_vector(1)->get_row_num(), 4u);

    ColIterator time_iter(0, &ts_block);
    ColIterator value_iter(1, &ts_block);
    for (uint32_t i = 0; i < 4; ++i) {
        uint32_t len = 0;
        EXPECT_EQ(*reinterpret_cast<int64_t*>(time_iter.read(&len)), times[i]);
        EXPECT_EQ(len, sizeof(int64_t));
        EXPECT_FLOAT_EQ(*reinterpret_cast<float*>(value_iter.read(&len)),
                        values[i]);
        EXPECT_EQ(len, sizeof(float));
        time_iter.next();
        value_iter.next();
    }
}

TEST(TsBlockTest, RowIterator_ReadAndNext) {
    TupleDesc tuple_desc;
    ColumnSchema col1("test_col1", INT32, SNAPPY, RLE);
    ColumnSchema col2("test_col2", INT32, SNAPPY, RLE);
    tuple_desc.push_back(col1);
    tuple_desc.push_back(col2);
    TsBlock ts_block(&tuple_desc, 1000000);
    ts_block.init();
    RowAppender row_appender(&ts_block);
    ColAppender col_appender(0, &ts_block);

    for (int i = 0; i < 10000; ++i) {
        EXPECT_TRUE(row_appender.add_row());
        EXPECT_TRUE(col_appender.add_row());
        int32_t val = i;
        col_appender.append((const char*)&val, sizeof(int32_t));
    }

    RowIterator row_iterator(&ts_block);
    EXPECT_FALSE(row_iterator.end());

    uint32_t len;
    bool null;
    char* value = row_iterator.read(0, &len, &null);
    EXPECT_EQ(len, sizeof(int32_t));
    EXPECT_FALSE(null);
    EXPECT_EQ(*((int32_t*)value), 0);

    row_iterator.next();
    EXPECT_FALSE(row_iterator.end());
    value = row_iterator.read(0, &len, &null);
    EXPECT_EQ(len, sizeof(int32_t));
    EXPECT_FALSE(null);
    EXPECT_EQ(*((int32_t*)value), 1);

    for (int i = 1; i < 10000; ++i) {
        row_iterator.next();
    }
    EXPECT_TRUE(row_iterator.end());
}

TEST(TsBlockTest, ColIterator_ReadAndNext) {
    TupleDesc tuple_desc;
    ColumnSchema col1("test_col1", INT32, SNAPPY, RLE);
    ColumnSchema col2("test_col2", INT32, SNAPPY, RLE);
    tuple_desc.push_back(col1);
    tuple_desc.push_back(col2);
    TsBlock ts_block(&tuple_desc, 100000);
    ts_block.init();
    RowAppender row_appender(&ts_block);
    ColAppender col_appender(0, &ts_block);

    for (int i = 0; i < 10000; ++i) {
        EXPECT_TRUE(row_appender.add_row());
        EXPECT_TRUE(col_appender.add_row());
        int32_t val = i;
        col_appender.append((const char*)&val, sizeof(int32_t));
    }

    ColIterator col_iterator(0, &ts_block);
    EXPECT_FALSE(col_iterator.end());

    uint32_t len;
    bool null;
    char* value = col_iterator.read(&len, &null);
    EXPECT_EQ(len, sizeof(int32_t));
    EXPECT_FALSE(null);
    EXPECT_EQ(*((int32_t*)value), 0);

    col_iterator.next();
    EXPECT_FALSE(col_iterator.end());
    value = col_iterator.read(&len, &null);
    EXPECT_EQ(len, sizeof(int32_t));
    EXPECT_FALSE(null);
    EXPECT_EQ(*((int32_t*)value), 1);

    for (int i = 1; i < 10000; ++i) {
        col_iterator.next();
    }
    EXPECT_TRUE(col_iterator.end());
}

TEST(TsBlockTest, ColAppenderFillRepeatedFixed) {
    TupleDesc tuple_desc;
    tuple_desc.push_back(ColumnSchema("value", INT32, UNCOMPRESSED, PLAIN));
    TsBlock ts_block(&tuple_desc, 10);
    ASSERT_EQ(ts_block.init(), E_OK);
    ColAppender col_appender(0, &ts_block);

    int32_t value = 42;
    ASSERT_EQ(col_appender.fill(reinterpret_cast<const char*>(&value),
                                sizeof(value), 5),
              E_OK);
    EXPECT_EQ(col_appender.get_col_row_count(), 5u);
    EXPECT_EQ(ts_block.get_vector(0)->get_row_num(), 5u);

    ColIterator iter(0, &ts_block);
    for (int i = 0; i < 5; ++i) {
        uint32_t len = 0;
        char* read = iter.read(&len);
        ASSERT_NE(read, nullptr);
        EXPECT_EQ(len, sizeof(int32_t));
        EXPECT_EQ(*reinterpret_cast<int32_t*>(read), value);
        iter.next();
    }
}

TEST(TsBlockTest, ColAppenderFillRepeatedVariable) {
    TupleDesc tuple_desc;
    tuple_desc.push_back(ColumnSchema("tag", STRING, UNCOMPRESSED, PLAIN));
    TsBlock ts_block(&tuple_desc, 10);
    ASSERT_EQ(ts_block.init(), E_OK);
    ColAppender col_appender(0, &ts_block);

    const std::string tag = "device-a";
    ASSERT_EQ(col_appender.fill(tag.data(), tag.size(), 4), E_OK);
    // A second fill continues after the rows already present.
    ASSERT_EQ(col_appender.fill(tag.data(), tag.size(), 7), E_OK);
    EXPECT_EQ(col_appender.get_col_row_count(), 7u);
    EXPECT_EQ(ts_block.get_vector(0)->get_row_num(), 7u);

    ColIterator iter(0, &ts_block);
    for (int i = 0; i < 7; ++i) {
        uint32_t len = 0;
        char* read = iter.read(&len);
        ASSERT_NE(read, nullptr);
        EXPECT_EQ(std::string(read, len), tag);
        iter.next();
    }
}

TEST(TsBlockTest, ColAppenderFillRepeatedRejectsOverflow) {
    TupleDesc tuple_desc;
    tuple_desc.push_back(ColumnSchema("value", INT32, UNCOMPRESSED, PLAIN));
    TsBlock ts_block(&tuple_desc, 3);
    ASSERT_EQ(ts_block.init(), E_OK);
    ColAppender col_appender(0, &ts_block);

    int32_t value = 1;
    EXPECT_EQ(col_appender.fill(reinterpret_cast<const char*>(&value),
                                sizeof(value), 4),
              E_INVALID_ARG);
    EXPECT_EQ(col_appender.get_col_row_count(), 0u);
    EXPECT_EQ(ts_block.get_vector(0)->get_row_num(), 0u);
}

// ByteBuffer-level zero-count guard: the helpers used to write the pattern
// before checking count, which writes past the end of an exactly-full buffer.
TEST(TsBlockTest, ByteBufferFillRepeatedZeroCountWritesNothing) {
    ByteBuffer buffer;
    buffer.init(64);
    char payload[16] = {'a', 'b', 'c'};
    ASSERT_EQ(buffer.append_variable_value(payload, 16), E_OK);
    const uint32_t size_before = buffer.get_data_size();
    char* data = buffer.get_data();
    data[size_before] = '\x5a';  // sentinel in the slack after the data

    EXPECT_EQ(buffer.fill_repeated_variable("wxyz", 4, 0), E_OK);
    EXPECT_EQ(buffer.fill_repeated_fixed("ABCD", 4, 0), E_OK);
    EXPECT_EQ(buffer.get_data_size(), size_before);
    EXPECT_EQ(data[size_before], '\x5a');
}

// Fill a tag longer than the per-row reservation so extend_memory() has to
// grow (and copy) the buffer mid-fill.
TEST(TsBlockTest, ColAppenderFillRepeatedVariableGrows) {
    TupleDesc tuple_desc;
    tuple_desc.push_back(ColumnSchema("tag", STRING, UNCOMPRESSED, PLAIN));
    TsBlock ts_block(&tuple_desc, 100);
    ASSERT_EQ(ts_block.init(), E_OK);
    ColAppender col_appender(0, &ts_block);

    const std::string tag(64, 'g');
    ASSERT_EQ(col_appender.fill(tag.data(), tag.size(), 100), E_OK);
    EXPECT_EQ(ts_block.get_vector(0)->get_row_num(), 100u);

    ColIterator iter(0, &ts_block);
    for (int i = 0; i < 100; ++i) {
        uint32_t len = 0;
        char* read = iter.read(&len);
        ASSERT_NE(read, nullptr);
        EXPECT_EQ(std::string(read, len), tag);
        iter.next();
    }
    EXPECT_TRUE(iter.end());
}

TEST(TsBlockTest, ColAppenderFillRepeatedZeroCountIsNoOp) {
    TupleDesc tuple_desc;
    tuple_desc.push_back(ColumnSchema("value", INT32, UNCOMPRESSED, PLAIN));
    TsBlock ts_block(&tuple_desc, 3);
    ASSERT_EQ(ts_block.init(), E_OK);
    ColAppender col_appender(0, &ts_block);

    int32_t value = 1;
    EXPECT_EQ(col_appender.fill(reinterpret_cast<const char*>(&value),
                                sizeof(value), 0),
              E_OK);
    EXPECT_EQ(col_appender.get_col_row_count(), 0u);
    EXPECT_EQ(ts_block.get_vector(0)->get_row_num(), 0u);
}

}  // namespace common
