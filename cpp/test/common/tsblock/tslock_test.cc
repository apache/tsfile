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
    ColumnDesc col;
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
    TsID ts_id(1, 2, 3);
    ColumnDesc col(INT32, RLE, SNAPPY, 1000, "test_col", ts_id);
    tuple_desc.push_back(col);
    TsBlock ts_block(&tuple_desc, 50);
    ts_block.init();
    RowAppender row_appender(&ts_block);
    ColAppender col_appender(0, &ts_block);

    for (int i = 0; i < 50; ++i) {
        EXPECT_TRUE(row_appender.add_row());
        EXPECT_TRUE(col_appender.add_row());
        int32_t val = i;
        col_appender.append((const char *)&val, sizeof(int32_t));
    }

    EXPECT_EQ(col_appender.get_col_row_count(), 50);
}

TEST(TsBlockTest, RowIterator_ReadAndNext) {
    TupleDesc tuple_desc;
    TsID ts_id1(1, 2, 3);
    ColumnDesc col1(INT32, RLE, SNAPPY, 1000, "test_col1", ts_id1);
    TsID ts_id2(1, 2, 4);
    ColumnDesc col2(INT32, RLE, SNAPPY, 1000, "test_col2", ts_id2);
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
        col_appender.append((const char *)&val, sizeof(int32_t));
    }

    RowIterator row_iterator(&ts_block);
    EXPECT_FALSE(row_iterator.end());

    uint32_t len;
    bool null;
    char *value = row_iterator.read(0, &len, &null);
    EXPECT_EQ(len, sizeof(int32_t));
    EXPECT_FALSE(null);
    EXPECT_EQ(*((int32_t *)value), 0);

    row_iterator.next();
    EXPECT_FALSE(row_iterator.end());
    value = row_iterator.read(0, &len, &null);
    EXPECT_EQ(len, sizeof(int32_t));
    EXPECT_FALSE(null);
    EXPECT_EQ(*((int32_t *)value), 1);

    for (int i = 1; i < 10000; ++i) {
        row_iterator.next();
    }
    EXPECT_TRUE(row_iterator.end());
}

TEST(TsBlockTest, ColIterator_ReadAndNext) {
    TupleDesc tuple_desc;
    TsID ts_id1(1, 2, 3);
    ColumnDesc col1(INT32, RLE, SNAPPY, 1000, "test_col1", ts_id1);
    TsID ts_id2(1, 2, 4);
    ColumnDesc col2(INT32, RLE, SNAPPY, 1000, "test_col2", ts_id2);
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
        col_appender.append((const char *)&val, sizeof(int32_t));
    }

    ColIterator col_iterator(0, &ts_block);
    EXPECT_FALSE(col_iterator.end());

    uint32_t len;
    bool null;
    char *value = col_iterator.read(&len, &null);
    EXPECT_EQ(len, sizeof(int32_t));
    EXPECT_FALSE(null);
    EXPECT_EQ(*((int32_t *)value), 0);

    col_iterator.next();
    EXPECT_FALSE(col_iterator.end());
    value = col_iterator.read(&len, &null);
    EXPECT_EQ(len, sizeof(int32_t));
    EXPECT_FALSE(null);
    EXPECT_EQ(*((int32_t *)value), 1);

    for (int i = 1; i < 10000; ++i) {
        col_iterator.next();
    }
    EXPECT_TRUE(col_iterator.end());
}

}  // namespace common
