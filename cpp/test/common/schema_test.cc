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

#if DEBUG_SE
TEST(MeasurementSchemaTest, JavaCppGap) {
    MeasurementSchema* measurement = new MeasurementSchema("measurement_name",
        common::INT64, common::PLAIN, common::UNCOMPRESSED);
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    measurement->serialize_to(stream);
    auto buf_len = stream.total_size();
    auto buf = new char[buf_len];
    common::copy_bs_to_buf(stream, buf, buf_len);
    const ssize_t expected_size = 27;
    uint8_t expected_buf[expected_size] = {0, 0, 0, 16, 109, 101, 97, 115, 117,
                                           114, 101,
                                           109, 101, 110, 116, 95, 110, 97, 109,
                                           101, 2, 0,
                                           0, 0, 0, 0, 0};
    for (int i = 0; i < expected_size; i++) {
        EXPECT_EQ(buf[i], expected_buf[i]);
    }
    delete [] buf;
    delete measurement;
}
#endif

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

#if DEBUG_SE
TEST(TableSchemaTest, BasicTest) {
    using namespace storage;
    using namespace common;
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    int id_schema_num = 5;
    int measurement_schema_num = 5;
    for (int i = 0; i < id_schema_num; i++) {
        measurement_schemas.emplace_back(
            new MeasurementSchema(
                "__level" + to_string(i), TSDataType::TEXT, TSEncoding::PLAIN,
                CompressionType::UNCOMPRESSED));
        column_categories.emplace_back(ColumnCategory::TAG);
    }
    for (int i = 0; i < measurement_schema_num; i++) {
        measurement_schemas.emplace_back(
            new MeasurementSchema(
                "s" + to_string(i), TSDataType::INT64, TSEncoding::PLAIN,
                CompressionType::UNCOMPRESSED));
        column_categories.emplace_back(ColumnCategory::FIELD);
    }
    auto table_schema = new TableSchema("test_table",
                                        measurement_schemas,
                                        column_categories);
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    table_schema->serialize_to(stream);
    delete table_schema;

    auto buf_len = stream.total_size();
    auto buf = new char[buf_len];
    common::copy_bs_to_buf(stream, buf, buf_len);
    const ssize_t expected_size = 201;
    uint8_t expected_buf[expected_size] = {10, 0, 0, 0, 8, 95, 95, 108, 101,
                                           118, 101, 108, 48, 5, 0, 0,
                                           0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8,
                                           95, 95, 108, 101,
                                           118, 101, 108, 49, 5, 0, 0, 0, 0, 0,
                                           0, 0, 0, 0, 0, 0,
                                           0, 0, 8, 95, 95, 108, 101, 118, 101,
                                           108, 50, 5, 0, 0, 0, 0,
                                           0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 95, 95,
                                           108, 101, 118, 101,
                                           108, 51, 5, 0, 0, 0, 0, 0, 0, 0, 0,
                                           0, 0, 0, 0, 0,
                                           8, 95, 95, 108, 101, 118, 101, 108,
                                           52, 5, 0, 0, 0, 0, 0, 0,
                                           0, 0, 0, 0, 0, 0, 0, 2, 115, 48, 2,
                                           0, 0, 0, 0, 0,
                                           0, 0, 0, 0, 1, 0, 0, 0, 2, 115, 49,
                                           2, 0, 0, 0, 0,
                                           0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 115,
                                           50, 2, 0, 0, 0,
                                           0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 115,
                                           51, 2, 0, 0,
                                           0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2,
                                           115, 52, 2, 0,
                                           0, 0, 0, 0, 0, 0, 0, 0, 1};
    for (int i = 0; i < expected_size; i++) {
        EXPECT_EQ(buf[i], expected_buf[i]);
    }
    delete [] buf;
}
#endif
} // namespace storage
