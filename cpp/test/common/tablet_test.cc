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

// Regression: reset() must restore each column's bitmap to all-null. If the
// previous batch left some cells with non-null bits cleared and the next batch
// does not re-fill those cells, get_value() must report them as null so the
// writer does not emit stale leftover values.
TEST(TabletTest, ResetClearsBitmap) {
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.push_back(MeasurementSchema(
        "m_int", common::TSDataType::INT32, common::TSEncoding::PLAIN,
        common::CompressionType::UNCOMPRESSED));
    schema_vec.push_back(MeasurementSchema(
        "m_double", common::TSDataType::DOUBLE, common::TSEncoding::PLAIN,
        common::CompressionType::UNCOMPRESSED));
    Tablet tablet("dev",
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec));

    // First batch fills row 5 in both columns.
    ASSERT_EQ(tablet.add_value(5u, 0u, static_cast<int32_t>(42)), common::E_OK);
    ASSERT_EQ(tablet.add_value(5u, 1u, 3.14), common::E_OK);

    common::TSDataType ty;
    EXPECT_NE(tablet.get_value(5, 0u, ty), nullptr);
    EXPECT_NE(tablet.get_value(5, 1u, ty), nullptr);

    // Reuse the tablet: reset and write a fresh, smaller batch that does not
    // touch row 5 at all. Row 5 must come back as null, not as the stale 42.
    tablet.reset();
    ASSERT_EQ(tablet.add_value(0u, 0u, static_cast<int32_t>(7)), common::E_OK);
    EXPECT_NE(tablet.get_value(0, 0u, ty), nullptr);
    EXPECT_EQ(tablet.get_value(5, 0u, ty), nullptr);
    EXPECT_EQ(tablet.get_value(5, 1u, ty), nullptr);
}

// Regression: set_column_values() with a non-null bitmap must update
// has_set_bits_, otherwise downstream may_have_set_bits() shortcuts treat the
// column as having no nulls and the writer emits stale/garbage values for the
// rows the bitmap was meant to mark null.
TEST(TabletTest, SetColumnValuesBitmapPreservesNullFlag) {
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.push_back(MeasurementSchema(
        "m_int", common::TSDataType::INT32, common::TSEncoding::PLAIN,
        common::CompressionType::UNCOMPRESSED));
    Tablet tablet("dev",
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec));

    int32_t buf[8] = {1, 2, 3, 4, 5, 6, 7, 8};

    // Step 1: write all 8 rows with no nulls -> clear_all() inside the tablet
    // sets has_set_bits_=false, matching the state a real workload leaves
    // behind for a fully-populated column.
    ASSERT_EQ(tablet.set_column_values(0u, buf, /*bitmap=*/nullptr, 8u),
              common::E_OK);

    // Step 2: rewrite with a bitmap that marks rows 0 and 7 as NULL.  Tablet's
    // BitMap layout is LSB-first within each byte (row i -> bit 1<<(i%8)).
    uint8_t external_bitmap[] = {0x81};  // bit 0 (row 0) + bit 7 (row 7) set
    ASSERT_EQ(tablet.set_column_values(0u, buf, external_bitmap, 8u),
              common::E_OK);

    common::TSDataType ty;
    EXPECT_EQ(tablet.get_value(0, 0u, ty), nullptr);
    EXPECT_NE(tablet.get_value(1, 0u, ty), nullptr);
    EXPECT_EQ(tablet.get_value(7, 0u, ty), nullptr);
}

// Regression: set_column_string_values / set_column_string_repeated used to
// reinterpret value_matrix_[c].string_col without checking the schema type.
// Calling them on a numeric column would corrupt that column's numeric
// buffer.  Verify both reject non-string columns with E_TYPE_NOT_MATCH.
TEST(TabletTest, StringApisRejectNonStringColumn) {
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.push_back(MeasurementSchema(
        "m_int", common::TSDataType::INT32, common::TSEncoding::PLAIN,
        common::CompressionType::UNCOMPRESSED));
    Tablet tablet("dev",
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec));

    const char data[] = "hello";
    int32_t offsets[2] = {0, 5};
    EXPECT_EQ(tablet.set_column_string_values(0u, offsets, data, nullptr, 1u),
              common::E_TYPE_NOT_MATCH);
    EXPECT_EQ(tablet.set_column_string_repeated(0u, "x", 1u, 4u),
              common::E_TYPE_NOT_MATCH);
}

// Regression: str_len * count used to be computed in uint32_t and would wrap
// silently, leaving the loop to write past the truncated allocation.
// 65536 * 65537 = 4295032832 → wraps to 65536 in uint32_t.
TEST(TabletTest, StringRepeatedTotalBytesOverflowRejected) {
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.push_back(MeasurementSchema(
        "m_str", common::TSDataType::STRING, common::TSEncoding::PLAIN,
        common::CompressionType::UNCOMPRESSED));
    Tablet tablet("dev",
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  100000u);
    std::string big_str(65536, 'a');
    EXPECT_EQ(tablet.set_column_string_repeated(0u, big_str.c_str(),
                                                /*str_len=*/65536u,
                                                /*count=*/65537u),
              common::E_OVERFLOW);
}

TEST(TabletTest, StringReallocFailureReturnsOomAndPreservesColumn) {
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.push_back(MeasurementSchema(
        "m_str", common::TSDataType::STRING, common::TSEncoding::PLAIN,
        common::CompressionType::UNCOMPRESSED));
    Tablet tablet("dev",
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  1u);

    std::string oversized_value(64, 'x');
    common::TEST_fail_next_mem_realloc();
    EXPECT_EQ(tablet.add_value(0u, 0u, common::String(oversized_value)),
              common::E_OOM);

    common::String value("ok", 2);
    ASSERT_EQ(tablet.add_value(0u, 0u, value), common::E_OK);
    common::TSDataType type;
    auto* stored = static_cast<common::String*>(tablet.get_value(0u, 0u, type));
    ASSERT_NE(stored, nullptr);
    EXPECT_EQ(stored->len_, 2u);
    EXPECT_EQ(memcmp(stored->buf_, "ok", 2), 0);
}

// Regression: set_column_string_values only checked offsets[count] before;
// non-monotonic / negative / non-zero-start offsets would underflow the
// downstream `offsets[i+1] - offsets[i]` length calc and trigger wild
// memcpy.  Verify each malformed input is rejected with E_INVALID_ARG.
TEST(TabletTest, StringValuesRejectsMalformedOffsets) {
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.push_back(MeasurementSchema(
        "m_str", common::TSDataType::STRING, common::TSEncoding::PLAIN,
        common::CompressionType::UNCOMPRESSED));
    Tablet tablet("dev",
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec));
    const char data[] = "abcdefghij";

    // Non-zero start offset.
    int32_t off_bad_start[3] = {1, 5, 10};
    EXPECT_EQ(
        tablet.set_column_string_values(0u, off_bad_start, data, nullptr, 2u),
        common::E_INVALID_ARG);

    // Non-monotonic: {0, 10, 5}.
    int32_t off_non_mono[3] = {0, 10, 5};
    EXPECT_EQ(
        tablet.set_column_string_values(0u, off_non_mono, data, nullptr, 2u),
        common::E_INVALID_ARG);

    // Negative offset somewhere in the middle.
    int32_t off_neg[3] = {0, -1, 5};
    EXPECT_EQ(tablet.set_column_string_values(0u, off_neg, data, nullptr, 2u),
              common::E_INVALID_ARG);

    // Sanity: well-formed offsets succeed.
    int32_t off_ok[3] = {0, 3, 7};
    EXPECT_EQ(tablet.set_column_string_values(0u, off_ok, data, nullptr, 2u),
              common::E_OK);
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

}  // namespace storage
