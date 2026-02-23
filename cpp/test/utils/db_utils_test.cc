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
#include "utils/db_utils.h"

#include <gtest/gtest.h>

#include <sstream>

namespace common {

TEST(ColumnSchemaTest, Constructor) {
    ColumnSchema col_schema;
    EXPECT_EQ(col_schema.data_type_, INVALID_DATATYPE);
    EXPECT_EQ(col_schema.encoding_, PLAIN);
    EXPECT_EQ(col_schema.compression_, UNCOMPRESSED);
    EXPECT_EQ(col_schema.column_name_, "");
}

TEST(ColumnSchemaTest, ParameterizedConstructor) {
    ColumnSchema col_schema("test_col", INT32, SNAPPY, RLE);
    EXPECT_EQ(col_schema.data_type_, INT32);
    EXPECT_EQ(col_schema.encoding_, RLE);
    EXPECT_EQ(col_schema.compression_, SNAPPY);
    EXPECT_EQ(col_schema.column_name_, "test_col");
}

TEST(ColumnSchemaTest, OperatorEqual) {
    ColumnSchema col_schema1("test_col", INT32, SNAPPY, RLE);
    ColumnSchema col_schema2("test_col", INT32, SNAPPY, RLE);
    EXPECT_TRUE(col_schema1 == col_schema2);
}

TEST(ColumnSchemaTest, OperatorNotEqual) {
    ColumnSchema col_schema1("test_col", INT32, SNAPPY, RLE);
    ColumnSchema col_schema2("test_col2", INT32, SNAPPY, RLE);
    EXPECT_TRUE(col_schema1 != col_schema2);
}

TEST(ColumnSchemaTest, IsValid) {
    ColumnSchema col_schema("test_col", INT32, SNAPPY, RLE);
    EXPECT_TRUE(col_schema.is_valid());
    col_schema.data_type_ = INVALID_DATATYPE;
    EXPECT_FALSE(col_schema.is_valid());
}

TEST(UtilTest, GetCurTimestamp) {
    int64_t timestamp = get_cur_timestamp();
    EXPECT_GT(timestamp, 0);
}

}  // namespace common
