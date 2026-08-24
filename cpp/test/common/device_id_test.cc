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

TEST(DeviceIdTest, DeviceIdStringFallbackSemantic) {
    std::string device_id_string = "root.sg1.FeederA";
    StringArrayDeviceID device_id = StringArrayDeviceID(device_id_string);

    // For a 3-level identifier, table name should be merged as "root.sg1".
    ASSERT_EQ("root.sg1", device_id.get_table_name());
    ASSERT_EQ(2, device_id.segment_num());
    ASSERT_EQ("root.sg1.FeederA", device_id.get_device_name());
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

// Regression: a device whose first tag is a real null and a device whose first
// tag is the literal string "null" render to the SAME get_device_name()
// ("t.null.b"), so anything that keys a per-device map/cache by the device name
// aliases the two — the second device silently reads the first device's chunks.
// The device-node cache in TsFileIOReader hit exactly this, conflating the two
// devices' data on a reused reader.  The reliable discriminator is the segment
// vector (operator==), which keeps nullptr distinct from the string "null".
TEST(DeviceIdTest, NullTagVsLiteralNullAreDistinct) {
    // Real null first tag: segment pointer is nullptr.
    std::vector<std::string*> null_first_segs{new std::string("t"), nullptr,
                                              new std::string("b")};
    StringArrayDeviceID null_first(null_first_segs);
    for (auto* s : null_first_segs) delete s;

    // Literal string "null" as the first tag value.
    StringArrayDeviceID literal_null(
        std::vector<std::string>({"t", "null", "b"}));

    // The names collide — this is the trap the cache used to fall into.
    ASSERT_EQ(null_first.get_device_name(), literal_null.get_device_name());
    ASSERT_EQ("t.null.b", null_first.get_device_name());

    // But the devices are genuinely different, and the segment-based equality
    // used by DeviceIDComparable / the cache key must reflect that.
    ASSERT_FALSE(null_first == literal_null);
    ASSERT_TRUE(null_first != literal_null);
}

// Regression: cached device IDs are reused across queries, so
// split_table_name() must be idempotent and not accumulate prefix segments.
TEST(DeviceIdTest, SplitTableNameIsIdempotent) {
    StringArrayDeviceID device_id("root.ln.wf01.wt01");

    const std::vector<std::string> expected = {"root", "ln", "wf01", "wt01"};

    for (int round = 0; round < 3; ++round) {
        device_id.split_table_name();

        ASSERT_EQ(static_cast<int>(expected.size()),
                  device_id.get_split_seg_num());
        for (int i = 0; i < device_id.get_split_seg_num(); ++i) {
            std::string* seg = device_id.get_split_segname_at(i);
            ASSERT_NE(nullptr, seg);
            ASSERT_EQ(expected[static_cast<size_t>(i)], *seg);
        }
    }
}
}  // namespace storage
