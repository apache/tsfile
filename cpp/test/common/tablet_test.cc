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
#include "common/tablet.h"

#include <gtest/gtest.h>

#include "common/schema.h"

namespace storage {

TEST(TabletTest, BasicFunctionality) {
    std::string device_name = "test_device";
    std::vector<MeasurementSchema> schema_vec;

    schema_vec.push_back(MeasurementSchema(
        "measurement1", common::TSDataType::BOOLEAN, common::TSEncoding::RLE,
        common::CompressionType::SNAPPY));
    schema_vec.push_back(MeasurementSchema(
        "measurement2", common::TSDataType::BOOLEAN, common::TSEncoding::RLE,
        common::CompressionType::SNAPPY));
    Tablet tablet(device_name, &schema_vec);

    EXPECT_EQ(tablet.get_column_count(), schema_vec.size());

    EXPECT_EQ(tablet.init(), common::E_OK);

    EXPECT_EQ(tablet.set_value(0, "measurement1", true), common::E_OK);
    EXPECT_EQ(tablet.set_value(0, "measurement2", false), common::E_OK);

    EXPECT_EQ(tablet.set_value(1, 0, false), common::E_OK);
    EXPECT_EQ(tablet.set_value(1, 1, true), common::E_OK);
}

TEST(TabletTest, LargeQuantities) {
    std::string device_name = "test_device";
    std::vector<MeasurementSchema> schema_vec;

    for (int i = 0; i < 10000; i++) {
        schema_vec.push_back(MeasurementSchema(
            "measurement" + std::to_string(i), common::TSDataType::BOOLEAN,
            common::TSEncoding::RLE, common::CompressionType::SNAPPY));
    }
    Tablet tablet(device_name, &schema_vec);

    EXPECT_EQ(tablet.get_column_count(), schema_vec.size());
}

}  // namespace storage