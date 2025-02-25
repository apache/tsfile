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

TEST(FileIDTest, Constructor) {
    FileID file_id;
    EXPECT_EQ(file_id.seq_, 0);
    EXPECT_EQ(file_id.version_, 0);
    EXPECT_EQ(file_id.merge_, 0);
}

TEST(FileIDTest, Reset) {
    FileID file_id;
    file_id.seq_ = 123;
    file_id.version_ = 1;
    file_id.merge_ = 2;
    file_id.reset();
    EXPECT_EQ(file_id.seq_, 0);
    EXPECT_EQ(file_id.version_, 0);
    EXPECT_EQ(file_id.merge_, 0);
}

TEST(FileIDTest, IsValid) {
    FileID file_id;
    EXPECT_FALSE(file_id.is_valid());
    file_id.seq_ = 123;
    EXPECT_TRUE(file_id.is_valid());
}

TEST(FileIDTest, OperatorLess) {
    FileID file_id1, file_id2;
    file_id1.seq_ = 123;
    file_id2.seq_ = 456;
    EXPECT_TRUE(file_id1 < file_id2);
    EXPECT_FALSE(file_id2 < file_id1);
}

TEST(FileIDTest, OperatorEqual) {
    FileID file_id1, file_id2;
    file_id1.seq_ = 123;
    file_id2.seq_ = 123;
    EXPECT_TRUE(file_id1 == file_id2);
    file_id2.seq_ = 456;
    EXPECT_FALSE(file_id1 == file_id2);
}

TEST(TsIDTest, Constructor) {
    TsID ts_id;
    EXPECT_EQ(ts_id.db_nid_, 0);
    EXPECT_EQ(ts_id.device_nid_, 0);
    EXPECT_EQ(ts_id.measurement_nid_, 0);
}

TEST(TsIDTest, ParameterizedConstructor) {
    TsID ts_id(1, 2, 3);
    EXPECT_EQ(ts_id.db_nid_, 1);
    EXPECT_EQ(ts_id.device_nid_, 2);
    EXPECT_EQ(ts_id.measurement_nid_, 3);
}

TEST(TsIDTest, Reset) {
    TsID ts_id(1, 2, 3);
    ts_id.reset();
    EXPECT_EQ(ts_id.db_nid_, 0);
    EXPECT_EQ(ts_id.device_nid_, 0);
    EXPECT_EQ(ts_id.measurement_nid_, 0);
}

TEST(TsIDTest, OperatorEqual) {
    TsID ts_id1(1, 2, 3);
    TsID ts_id2(1, 2, 3);
    EXPECT_TRUE(ts_id1 == ts_id2);
    ts_id2.db_nid_ = 4;
    EXPECT_FALSE(ts_id1 == ts_id2);
}

TEST(TsIDTest, OperatorNotEqual) {
    TsID ts_id1(1, 2, 3);
    TsID ts_id2(1, 2, 3);
    EXPECT_FALSE(ts_id1 != ts_id2);
    ts_id2.db_nid_ = 4;
    EXPECT_TRUE(ts_id1 != ts_id2);
}

TEST(TsIDTest, ToInt64) {
    TsID ts_id(1, 2, 3);
    int64_t expected = (1LL << 32) | (2 << 16) | 3;
    EXPECT_EQ(ts_id.to_int64(), expected);
}

TEST(TsIDTest, OperatorLess) {
    TsID ts_id1(1, 2, 3);
    TsID ts_id2(1, 2, 4);
    EXPECT_TRUE(ts_id1 < ts_id2);
    EXPECT_FALSE(ts_id2 < ts_id1);
}

TEST(DeviceIDTest, Constructor) {
    DeviceID device_id;
    EXPECT_EQ(device_id.db_nid_, 0);
    EXPECT_EQ(device_id.device_nid_, 0);
}

TEST(DeviceIDTest, ParameterizedConstructor) {
    DeviceID device_id(1, 2);
    EXPECT_EQ(device_id.db_nid_, 1);
    EXPECT_EQ(device_id.device_nid_, 2);
}

TEST(DeviceIDTest, TsIDConstructor) {
    TsID ts_id(1, 2, 3);
    DeviceID device_id(ts_id);
    EXPECT_EQ(device_id.db_nid_, 1);
    EXPECT_EQ(device_id.device_nid_, 2);
}

TEST(DeviceIDTest, OperatorEqual) {
    DeviceID device_id1(1, 2);
    DeviceID device_id2(1, 2);
    EXPECT_TRUE(device_id1 == device_id2);
    device_id2.db_nid_ = 3;
    EXPECT_FALSE(device_id1 == device_id2);
}

TEST(DeviceIDTest, OperatorNotEqual) {
    DeviceID device_id1(1, 2);
    DeviceID device_id2(1, 2);
    EXPECT_FALSE(device_id1 != device_id2);
    device_id2.db_nid_ = 3;
    EXPECT_TRUE(device_id1 != device_id2);
}

TEST(DatabaseDescTest, Constructor) {
    DatabaseDesc db_desc;
    EXPECT_EQ(db_desc.ttl_, INVALID_TTL);
    EXPECT_EQ(db_desc.db_name_, "");
    EXPECT_EQ(db_desc.ts_id_.db_nid_, 0);
}

TEST(DatabaseDescTest, ParameterizedConstructor) {
    TsID ts_id(1, 2, 3);
    DatabaseDesc db_desc(1000, "test_db", ts_id);
    EXPECT_EQ(db_desc.ttl_, 1000);
    EXPECT_EQ(db_desc.db_name_, "test_db");
    EXPECT_EQ(db_desc.ts_id_, ts_id);
}

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
