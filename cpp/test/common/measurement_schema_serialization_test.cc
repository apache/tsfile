/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

#include <gtest/gtest.h>

#include <vector>

#include "common/schema.h"

namespace storage {
namespace {

void expect_same_schema(const MeasurementSchema& expected,
                        const MeasurementSchema& actual) {
    EXPECT_EQ(expected.measurement_name_, actual.measurement_name_);
    EXPECT_EQ(expected.data_type_, actual.data_type_);
    EXPECT_EQ(expected.encoding_, actual.encoding_);
    EXPECT_EQ(expected.compression_type_, actual.compression_type_);
    EXPECT_EQ(expected.props_, actual.props_);
}

std::vector<uint8_t> serialized_bytes(MeasurementSchema& schema) {
    common::ByteStream stream(16, common::MOD_DEFAULT);
    EXPECT_EQ(common::E_OK, schema.serialize_to(stream));
    std::vector<uint8_t> bytes(stream.total_size());
    common::copy_bs_to_buf(stream, reinterpret_cast<char*>(bytes.data()),
                           bytes.size());
    return bytes;
}

}  // namespace

TEST(MeasurementSchemaSerializationTest, EmptyPropertiesMatchJavaBytes) {
    MeasurementSchema schema("temperature", common::FLOAT, common::TS_2DIFF,
                             common::UNCOMPRESSED);
    // MeasurementSchema.serializeTo(OutputStream) in Java, with empty props.
    const std::vector<uint8_t> expected = {
        0x00, 0x00, 0x00, 0x0b, 0x74, 0x65, 0x6d, 0x70, 0x65, 0x72, 0x61,
        0x74, 0x75, 0x72, 0x65, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00};
    EXPECT_EQ(expected, serialized_bytes(schema));
}

TEST(MeasurementSchemaSerializationTest, NonEmptyPropertiesMatchJavaBytes) {
    MeasurementSchema schema("temperature", common::FLOAT, common::TS_2DIFF,
                             common::UNCOMPRESSED);
    schema.props_["max_point_number"] = "0";
    // Java's bytes for the reproducer in apache/tsfile#1017.
    const std::vector<uint8_t> expected = {
        0x00, 0x00, 0x00, 0x0b, 0x74, 0x65, 0x6d, 0x70, 0x65, 0x72, 0x61, 0x74,
        0x75, 0x72, 0x65, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x10, 0x6d, 0x61, 0x78, 0x5f, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x5f,
        0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x00, 0x00, 0x00, 0x01, 0x30};
    EXPECT_EQ(expected, serialized_bytes(schema));
}

TEST(MeasurementSchemaSerializationTest, EmbeddedNulPropertiesMatchJavaBytes) {
    MeasurementSchema schema("temperature", common::FLOAT, common::TS_2DIFF,
                             common::UNCOMPRESSED);
    schema.props_[std::string("a\0b", 3)] = std::string("x\0y", 3);
    // Java writes the complete strings, including their embedded NUL bytes.
    const std::vector<uint8_t> expected = {
        0x00, 0x00, 0x00, 0x0b, 0x74, 0x65, 0x6d, 0x70, 0x65, 0x72, 0x61, 0x74,
        0x75, 0x72, 0x65, 0x03, 0x04, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x03, 0x61, 0x00, 0x62, 0x00, 0x00, 0x00, 0x03, 0x78, 0x00, 0x79};
    EXPECT_EQ(expected, serialized_bytes(schema));
}

TEST(MeasurementSchemaSerializationTest, MultiplePropertiesRoundTrip) {
    MeasurementSchema original("temperature", common::FLOAT, common::TS_2DIFF,
                               common::UNCOMPRESSED);
    original.props_ = {
        {"", ""},
        {"max_point_number", "0"},
        {"unit", "celsius"},
        {"\xe6\xb8\xa9\xe5\xba\xa6", "\xe6\x91\x84\xe6\xb0\x8f"}};
    common::ByteStream stream(16, common::MOD_DEFAULT);
    ASSERT_EQ(common::E_OK, original.serialize_to(stream));
    MeasurementSchema decoded;
    ASSERT_EQ(common::E_OK, decoded.deserialize_from(stream));
    expect_same_schema(original, decoded);
    EXPECT_EQ(0u, stream.remaining_size());
}

TEST(MeasurementSchemaSerializationTest, LongPropertiesCrossPageBoundaries) {
    MeasurementSchema original("temperature", common::FLOAT, common::TS_2DIFF,
                               common::UNCOMPRESSED);
    original.props_[std::string(131, 'k')] = std::string(4097, 'v');
    original.props_["max_point_number"] = "0";
    common::ByteStream stream(16, common::MOD_DEFAULT);
    ASSERT_EQ(common::E_OK, original.serialize_to(stream));
    MeasurementSchema decoded;
    ASSERT_EQ(common::E_OK, decoded.deserialize_from(stream));
    expect_same_schema(original, decoded);
    EXPECT_EQ(0u, stream.remaining_size());
}

TEST(MeasurementSchemaSerializationTest,
     ConsecutiveSchemasPreserveStreamPosition) {
    MeasurementSchema first("temperature", common::FLOAT, common::TS_2DIFF,
                            common::UNCOMPRESSED);
    first.props_["max_point_number"] = "0";
    MeasurementSchema second("humidity", common::DOUBLE, common::GORILLA,
                             common::UNCOMPRESSED);
    common::ByteStream stream(16, common::MOD_DEFAULT);
    ASSERT_EQ(common::E_OK, first.serialize_to(stream));
    ASSERT_EQ(common::E_OK, second.serialize_to(stream));
    ASSERT_EQ(common::E_OK,
              common::SerializationUtil::write_ui32(0x12345678, stream));
    MeasurementSchema decoded_first, decoded_second;
    ASSERT_EQ(common::E_OK, decoded_first.deserialize_from(stream));
    ASSERT_EQ(common::E_OK, decoded_second.deserialize_from(stream));
    expect_same_schema(first, decoded_first);
    expect_same_schema(second, decoded_second);
    uint32_t marker = 0;
    ASSERT_EQ(common::E_OK,
              common::SerializationUtil::read_ui32(marker, stream));
    EXPECT_EQ(0x12345678u, marker);
    EXPECT_EQ(0u, stream.remaining_size());
}

TEST(MeasurementSchemaSerializationTest, TableSchemaPreservesColumnProperties) {
    auto* tag = new MeasurementSchema("device", common::STRING, common::PLAIN,
                                      common::UNCOMPRESSED);
    auto* field = new MeasurementSchema("temperature", common::FLOAT,
                                        common::TS_2DIFF, common::UNCOMPRESSED);
    field->props_["max_point_number"] = "0";
    TableSchema original(
        "weather", {tag, field},
        {common::ColumnCategory::TAG, common::ColumnCategory::FIELD});
    common::ByteStream stream(16, common::MOD_DEFAULT);
    ASSERT_EQ(common::E_OK, original.serialize_to(stream));
    TableSchema decoded;
    ASSERT_EQ(common::E_OK, decoded.deserialize(stream));
    ASSERT_EQ(2u, decoded.get_measurement_schemas().size());
    expect_same_schema(*tag, *decoded.get_measurement_schemas()[0]);
    expect_same_schema(*field, *decoded.get_measurement_schemas()[1]);
    EXPECT_EQ(original.get_column_categories(),
              decoded.get_column_categories());
    EXPECT_EQ(0u, stream.remaining_size());
}

TEST(MeasurementSchemaSerializationTest, TruncatedPropertyPayloadFails) {
    MeasurementSchema original("temperature", common::FLOAT, common::TS_2DIFF,
                               common::UNCOMPRESSED);
    original.props_["max_point_number"] = "0";
    const auto bytes = serialized_bytes(original);
    ASSERT_EQ(47u, bytes.size());
    for (size_t length = 22; length < bytes.size(); ++length) {
        SCOPED_TRACE(length);
        common::ByteStream stream;
        stream.wrap_from(reinterpret_cast<const char*>(bytes.data()), length);
        MeasurementSchema decoded;
        EXPECT_EQ(common::E_PARTIAL_READ, decoded.deserialize_from(stream));
    }
}

}  // namespace storage
