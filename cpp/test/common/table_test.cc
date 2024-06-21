#include <gtest/gtest.h>

#include "common/schema.h"
#include "common/tablet.h"

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