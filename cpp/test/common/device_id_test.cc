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

#include "common/device_id.h"

#include <gtest/gtest.h>

#include "common/tablet.h"

namespace storage {
using namespace ::common;
TEST(DeviceIdTest, NormalTest) {
    std::string device_id_string = "root.db.tb.device1";
    StringArrayDeviceID device_id = StringArrayDeviceID(device_id_string);
    ASSERT_EQ("root.db.tb.device1", device_id.get_device_name());
}

TEST(DeviceIdTest, TabletDeviceId) {
    std::vector<TSDataType> measurement_types{
        TSDataType::STRING, TSDataType::STRING, TSDataType::STRING,
        TSDataType::INT32};
    std::vector<ColumnCategory> column_categories{
        ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::TAG,
        ColumnCategory::FIELD};
    std::vector<std::string> measurement_names{"tag1", "tag2", "tag3", "value"};

    Tablet tablet("test_device0", measurement_names, measurement_types,
                  column_categories);
    tablet.add_timestamp(0, 1);
    tablet.add_value(0, 0, "t1");
    tablet.add_value(0, 1, "t2");
    tablet.add_value(0, 2, "t3");
    tablet.add_value(1, 0, "");
    tablet.add_value(1, 1, "t2");
    tablet.add_value(1, 2, "t3");
    tablet.add_value(2, 1, "t2");
    tablet.add_value(2, 2, "t3");
    auto device_id = std::make_shared<StringArrayDeviceID>(
        std::vector<std::string>({"test_device0", "t1", "t2", "t3"}));
    auto device_id2 = tablet.get_device_id(0);
    ASSERT_TRUE(*device_id2 == *device_id);

    ASSERT_EQ("test_device0..t2.t3",
              tablet.get_device_id(1)->get_device_name());
    ASSERT_EQ("test_device0.null.t2.t3",
              tablet.get_device_id(2)->get_device_name());
}
}  // namespace storage
