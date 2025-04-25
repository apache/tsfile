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
    Tablet tablet(device_name,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec));

    EXPECT_EQ(tablet.get_column_count(), schema_vec.size());

    EXPECT_EQ(tablet.add_value(0, "measurement1", true), common::E_OK);
    EXPECT_EQ(tablet.add_value(0, "measurement2", false), common::E_OK);

    EXPECT_EQ(tablet.add_value(1, 0, false), common::E_OK);
    EXPECT_EQ(tablet.add_value(1, 1, true), common::E_OK);
}

TEST(TabletTest, LargeQuantities) {
    std::string device_name = "test_device";
    std::vector<MeasurementSchema> schema_vec;

    for (int i = 0; i < 10000; i++) {
        schema_vec.push_back(MeasurementSchema(
            "measurement" + std::to_string(i), common::TSDataType::BOOLEAN,
            common::TSEncoding::RLE, common::CompressionType::SNAPPY));
    }
    Tablet tablet(device_name,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec));

    EXPECT_EQ(tablet.get_column_count(), schema_vec.size());
}

TEST(TabletTest, TabletBatchReadWrite) {
    std::vector<std::string> column_names = {
        "id1", "id2", "id3", "id4","id5","id6"
    };
    std::vector<common::TSDataType> datatypes = {
        common::TSDataType::BOOLEAN, common::TSDataType::INT32,
        common::TSDataType::INT64, common::TSDataType::FLOAT,
        common::TSDataType::DOUBLE, common::TSDataType::STRING
    };
    Tablet tablet(column_names, datatypes, 100);
    bool bool_vec[100] = {false};
    bool_vec[10] = true;

    common::TSDataType datatype;
    char* mask = new char[(100 + 7)/8];
    for (int i = 0; i < (100 + 7)/8; i++) {
        mask[i] = 0xff;
    }
    tablet.set_batch_data(0, bool_vec, mask);
    ASSERT_TRUE(*(bool*)(tablet.get_value(10, 0, datatype)));
    ASSERT_EQ(common::TSDataType::BOOLEAN, datatype);
    int32_t i32_vec[100] = {false};
    i32_vec[99] = 123;
    for (int i = 0; i < (100 + 7)/8; i++) {
        mask[i] = 0xff;
    }
    tablet.set_batch_data(1, i32_vec, mask);
    ASSERT_EQ(0, *(int32_t *)(tablet.get_value(10, 1, datatype)));
    ASSERT_EQ(123, *(int32_t *)(tablet.get_value(99, 1, datatype)));
    char** str = (char**) malloc(100 * sizeof(char*));
    for (int i = 0; i < 100; i++) {
        str[i] = strdup(std::string("val" + std::to_string(i)).c_str());
    }
    tablet.set_batch_data(5, str, nullptr);
    ASSERT_EQ(common::String("val10"), *(common::String*)tablet.get_value(10, 5, datatype));

    tablet.set_null_value(5, 20);
    ASSERT_EQ(nullptr, tablet.get_value(20, 5, datatype));
}

}  // namespace storage