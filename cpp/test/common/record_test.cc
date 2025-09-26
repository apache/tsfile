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
#include "common/record.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace storage {

TEST(DataPointTest, BoolConstructor) {
    DataPoint dp("touch_sensor", true);
    EXPECT_EQ(dp.measurement_name_, "touch_sensor");
    EXPECT_EQ(dp.data_type_, common::BOOLEAN);
    EXPECT_TRUE(dp.u_.bool_val_);
}

TEST(DataPointTest, Int32Constructor) {
    DataPoint dp("temperature", int32_t(100));
    EXPECT_EQ(dp.measurement_name_, "temperature");
    EXPECT_EQ(dp.data_type_, common::INT32);
    EXPECT_EQ(dp.u_.i32_val_, 100);
}

TEST(DataPointTest, Int64Constructor) {
    DataPoint dp("temperature", int64_t(100000));
    EXPECT_EQ(dp.measurement_name_, "temperature");
    EXPECT_EQ(dp.data_type_, common::INT64);
    EXPECT_EQ(dp.u_.i64_val_, 100000);
}

TEST(DataPointTest, FloatConstructor) {
    DataPoint dp("temperature", float(36.6));
    EXPECT_EQ(dp.measurement_name_, "temperature");
    EXPECT_EQ(dp.data_type_, common::FLOAT);
    EXPECT_FLOAT_EQ(dp.u_.float_val_, 36.6);
}

TEST(DataPointTest, DoubleConstructor) {
    DataPoint dp("temperature", double(36.6));
    EXPECT_EQ(dp.measurement_name_, "temperature");
    EXPECT_EQ(dp.data_type_, common::DOUBLE);
    EXPECT_DOUBLE_EQ(dp.u_.double_val_, 36.6);
}

TEST(DataPointTest, SetInt32) {
    DataPoint dp("temperature");
    dp.set_i32(100);
    EXPECT_EQ(dp.data_type_, common::INT32);
    EXPECT_EQ(dp.u_.i32_val_, 100);
}

TEST(DataPointTest, SetInt64) {
    DataPoint dp("temperature");
    dp.set_i64(100000);
    EXPECT_EQ(dp.data_type_, common::INT64);
    EXPECT_EQ(dp.u_.i64_val_, 100000);
}

TEST(DataPointTest, SetFloat) {
    DataPoint dp("temperature");
    dp.set_float(36.6);
    EXPECT_EQ(dp.data_type_, common::FLOAT);
    EXPECT_FLOAT_EQ(dp.u_.float_val_, 36.6);
}

TEST(DataPointTest, SetDouble) {
    DataPoint dp("temperature");
    dp.set_double(36.6);
    EXPECT_EQ(dp.data_type_, common::DOUBLE);
    EXPECT_DOUBLE_EQ(dp.u_.double_val_, 36.6);
}

TEST(TsRecordTest, ConstructorWithDeviceName) {
    TsRecord ts_record("device1");
    EXPECT_EQ(ts_record.device_name_, "device1");
    EXPECT_EQ(ts_record.points_.size(), 0);
}

TEST(TsRecordTest, ConstructorWithTimestamp) {
    TsRecord ts_record(1625140800, "device1", 5);
    EXPECT_EQ(ts_record.timestamp_, 1625140800);
    EXPECT_EQ(ts_record.device_name_, "device1");
    EXPECT_EQ(ts_record.points_.capacity(), 5);
}

TEST(TsRecordTest, AppendDataPoint) {
    TsRecord ts_record("device1");
    DataPoint dp("temperature", 36.6);
    ts_record.append_data_point(dp);
    ASSERT_EQ(ts_record.points_.size(), 1);
    EXPECT_EQ(ts_record.points_[0].measurement_name_, "temperature");
    EXPECT_EQ(ts_record.points_[0].data_type_, common::DOUBLE);
    EXPECT_DOUBLE_EQ(ts_record.points_[0].u_.double_val_, 36.6);
}

TEST(TsRecordTest, LargeQuantities) {
    TsRecord ts_record("device1");
    for (int i = 0; i < 10000; i++) {
        DataPoint dp(std::to_string(i), 36.6);
        ts_record.append_data_point(dp);
    }

    ASSERT_EQ(ts_record.points_.size(), 10000);
    for (int i = 0; i < 10000; i++) {
        EXPECT_EQ(ts_record.points_[i].measurement_name_, std::to_string(i));
        EXPECT_EQ(ts_record.points_[i].data_type_, common::DOUBLE);
        EXPECT_DOUBLE_EQ(ts_record.points_[i].u_.double_val_, 36.6);
    }
}

}  // namespace storage
