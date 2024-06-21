#include "utils/db_utils.h"

#include <gtest/gtest.h>
#include <string.h>
#include <sys/time.h>

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

TEST(ColumnDescTest, Constructor) {
    ColumnDesc col_desc;
    EXPECT_EQ(col_desc.type_, INVALID_DATATYPE);
    EXPECT_EQ(col_desc.encoding_, PLAIN);
    EXPECT_EQ(col_desc.compression_, UNCOMPRESSED);
    EXPECT_EQ(col_desc.ttl_, INVALID_TTL);
    EXPECT_EQ(col_desc.column_name_, "");
    EXPECT_EQ(col_desc.ts_id_.db_nid_, 0);
}

TEST(ColumnDescTest, ParameterizedConstructor) {
    TsID ts_id(1, 2, 3);
    ColumnDesc col_desc(INT32, RLE, SNAPPY, 1000, "test_col", ts_id);
    EXPECT_EQ(col_desc.type_, INT32);
    EXPECT_EQ(col_desc.encoding_, RLE);
    EXPECT_EQ(col_desc.compression_, SNAPPY);
    EXPECT_EQ(col_desc.ttl_, 1000);
    EXPECT_EQ(col_desc.column_name_, "test_col");
    EXPECT_EQ(col_desc.ts_id_, ts_id);
}

TEST(ColumnDescTest, OperatorEqual) {
    TsID ts_id(1, 2, 3);
    ColumnDesc col_desc1(INT32, RLE, SNAPPY, 1000, "test_col", ts_id);
    ColumnDesc col_desc2(INT32, RLE, SNAPPY, 1000, "test_col", ts_id);
    EXPECT_TRUE(col_desc1 == col_desc2);
}

TEST(ColumnDescTest, OperatorNotEqual) {
    TsID ts_id(1, 2, 3);
    ColumnDesc col_desc1(INT32, RLE, SNAPPY, 1000, "test_col", ts_id);
    ColumnDesc col_desc2(INT32, RLE, SNAPPY, 1000, "test_col2", ts_id);
    EXPECT_TRUE(col_desc1 != col_desc2);
}

TEST(ColumnDescTest, IsValid) {
    TsID ts_id(1, 2, 3);
    ColumnDesc col_desc(INT32, RLE, SNAPPY, 1000, "test_col", ts_id);
    EXPECT_TRUE(col_desc.is_valid());
    col_desc.type_ = INVALID_DATATYPE;
    EXPECT_FALSE(col_desc.is_valid());
}

TEST(UtilTest, GetCurTimestamp) {
    int64_t timestamp = get_cur_timestamp();
    EXPECT_GT(timestamp, 0);
}

}  // namespace common
