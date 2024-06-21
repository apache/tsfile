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