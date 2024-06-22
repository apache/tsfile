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
#include "common/schema.h"

#include <gtest/gtest.h>

namespace storage {

TEST(MeasurementSchemaTest, DefaultConstructor) {
    MeasurementSchema schema;

    EXPECT_EQ(schema.measurement_name_, "");
    EXPECT_EQ(schema.data_type_, common::INVALID_DATATYPE);
    EXPECT_EQ(schema.encoding_, common::INVALID_ENCODING);
    EXPECT_EQ(schema.compression_type_, common::INVALID_COMPRESSION);
    EXPECT_EQ(schema.chunk_writer_, nullptr);
}

TEST(MeasurementSchemaTest, ParameterizedConstructor) {
    MeasurementSchema schema("test_measurement", common::TSDataType::BOOLEAN,
                             common::TSEncoding::RLE,
                             common::CompressionType::SNAPPY);

    EXPECT_EQ(schema.measurement_name_, "test_measurement");
    EXPECT_EQ(schema.data_type_, common::TSDataType::BOOLEAN);
    EXPECT_EQ(schema.encoding_, common::TSEncoding::RLE);
    EXPECT_EQ(schema.compression_type_, common::CompressionType::SNAPPY);
    EXPECT_EQ(schema.chunk_writer_, nullptr);
}

TEST(MeasurementSchemaGroupTest, DefaultConstructor) {
    MeasurementSchemaGroup group;

    EXPECT_TRUE(group.measurement_schema_map_.empty());
    EXPECT_FALSE(group.is_aligned_);
}

}  // namespace storage