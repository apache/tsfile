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

// Fail the first allocation so large capacities can be checked without
// reserving GiBs. The allocator must receive the full timestamp byte count.
TEST(TabletTest, LargeCapacityAttemptsFullAllocationAndReturnsOom) {
    if (sizeof(size_t) < sizeof(uint64_t)) GTEST_SKIP();
    const std::vector<common::TSDataType> types = {
        common::BOOLEAN, common::INT32,     common::DATE,   common::FLOAT,
        common::INT64,   common::TIMESTAMP, common::DOUBLE, common::TEXT,
        common::BLOB,    common::STRING};
    const uint32_t capacities[] = {1u << 29, (1u << 30) - 2, (1u << 30) - 1,
                                   UINT32_MAX};
    for (auto type : types) {
        for (auto capacity : capacities) {
            SCOPED_TRACE(::testing::Message()
                         << "type=" << static_cast<int>(type)
                         << " rows=" << capacity);
            common::TEST_fail_mem_alloc_after(common::MOD_TABLET, 0);
            Tablet tablet({"value"}, {type}, capacity);
            ASSERT_EQ(tablet.err_code_, common::E_OOM);
            EXPECT_EQ(common::TEST_get_failed_mem_alloc_size(),
                      static_cast<uint64_t>(capacity) * sizeof(int64_t));
            EXPECT_EQ(tablet.get_cur_row_size(), 0u);
            EXPECT_EQ(tablet.add_timestamp(0, 1), common::E_OOM);
            // The allocation failure is one-shot.
            Tablet next({"value"}, {type}, 1u);
            EXPECT_EQ(next.err_code_, common::E_OK);
        }
    }
}

TEST(TabletTest, CapacityCheckedByEveryConstructor) {
    std::vector<std::string> names = {"value"};
    std::vector<common::TSDataType> types = {common::DOUBLE};
    auto schema = std::make_shared<std::vector<MeasurementSchema>>();
    schema->emplace_back("value", common::DOUBLE, common::PLAIN,
                         common::UNCOMPRESSED);
    for (int capacity : {0, -1, 1 << 29, (1 << 30) - 1, INT32_MAX}) {
        if (capacity > 0 && sizeof(size_t) < sizeof(uint64_t)) continue;
        const int expected =
            capacity <= 0 ? common::E_INVALID_ARG : common::E_OOM;
        if (capacity > 0)
            common::TEST_fail_mem_alloc_after(common::MOD_TABLET, 0);
        Tablet from_schema("dev", schema, capacity);
        EXPECT_EQ(from_schema.err_code_, expected);
        if (capacity > 0)
            common::TEST_fail_mem_alloc_after(common::MOD_TABLET, 0);
        Tablet from_lists("dev", &names, &types, capacity);
        EXPECT_EQ(from_lists.err_code_, expected);
        if (capacity > 0)
            common::TEST_fail_mem_alloc_after(common::MOD_TABLET, 0);
        Tablet with_categories("table", names, types,
                               {common::ColumnCategory::FIELD}, capacity);
        EXPECT_EQ(with_categories.err_code_, expected);
    }
    Tablet zero(names, types, 0u);
    EXPECT_EQ(zero.err_code_, common::E_INVALID_ARG);
}

TEST(TabletTest, StringColumnsPreallocate32BytesPerRow) {
    const uint32_t rows = 1u << 16;
    for (auto type : {common::TEXT, common::BLOB, common::STRING}) {
        const int64_t before =
            common::ModStat::get_instance().get_stat(common::MOD_TABLET);
        // timestamps, matrix, StringColumn, offsets, then the data buffer.
        common::TEST_fail_mem_alloc_after(common::MOD_TABLET, 4);
        {
            Tablet tablet({"value"}, {type}, rows);
            EXPECT_EQ(tablet.err_code_, common::E_OOM);
            EXPECT_EQ(common::TEST_get_failed_mem_alloc_size(),
                      static_cast<size_t>(rows) * 32);
        }
        EXPECT_EQ(common::ModStat::get_instance().get_stat(common::MOD_TABLET),
                  before);
    }
}

TEST(TabletTest, AllocationFailuresReturnOomAndReleasePartialBuffers) {
    // timestamps, matrix, INT32, STRING object/offsets/data, DOUBLE,
    // BLOB object/offsets/data, bitmap array, and four bitmap buffers.
    for (uint32_t fail_after = 0; fail_after < 15; ++fail_after) {
        SCOPED_TRACE(fail_after);
        const int64_t before =
            common::ModStat::get_instance().get_stat(common::MOD_TABLET);
        common::TEST_fail_mem_alloc_after(common::MOD_TABLET, fail_after);
        {
            Tablet tablet(
                {"i", "s", "d", "b"},
                {common::INT32, common::STRING, common::DOUBLE, common::BLOB},
                8u);
            EXPECT_EQ(tablet.err_code_, common::E_OOM);
            EXPECT_EQ(tablet.get_cur_row_size(), 0u);
            EXPECT_EQ(tablet.add_timestamp(0, 1), common::E_OOM);
            common::TSDataType type;
            EXPECT_EQ(tablet.get_value(0, 0u, type), nullptr);
            tablet.reset();
            EXPECT_EQ(tablet.get_cur_row_size(), 0u);
        }
        EXPECT_EQ(common::ModStat::get_instance().get_stat(common::MOD_TABLET),
                  before);
        // Each injection is one-shot; a subsequent tablet must work normally.
        Tablet next({"value"}, {common::INT64}, 1u);
        EXPECT_EQ(next.err_code_, common::E_OK);
    }
}

TEST(TabletTest, MillionRowsSupportsNumericAndStringColumns) {
    const uint32_t rows = 1u << 20;
    Tablet tablet({"d", "text", "blob", "string"},
                  {common::DOUBLE, common::TEXT, common::BLOB, common::STRING},
                  rows);
    ASSERT_EQ(tablet.err_code_, common::E_OK);
    const std::string text = "value-中文";
    const common::String value(text);
    for (uint32_t row = 0; row < rows; ++row) {
        ASSERT_EQ(tablet.add_timestamp(row, row), common::E_OK);
        ASSERT_EQ(tablet.add_value(row, 0u, static_cast<double>(row)),
                  common::E_OK);
        for (uint32_t col = 1; col < 4; ++col) {
            ASSERT_EQ(tablet.add_value(row, col, value), common::E_OK);
        }
    }
    ASSERT_EQ(tablet.get_cur_row_size(), rows);
    for (uint32_t row = 0; row < rows; ++row) {
        common::TSDataType type;
        auto* number = static_cast<double*>(tablet.get_value(row, 0u, type));
        ASSERT_NE(number, nullptr);
        ASSERT_EQ(*number, static_cast<double>(row));
        for (uint32_t col = 1; col < 4; ++col) {
            auto* stored =
                static_cast<common::String*>(tablet.get_value(row, col, type));
            ASSERT_NE(stored, nullptr);
            ASSERT_EQ(stored->len_, text.size());
            ASSERT_EQ(memcmp(stored->buf_, text.data(), text.size()), 0);
        }
    }
}

TEST(TabletTest, StringBytesMustFitSignedOffsets) {
    Tablet tablet({"value"}, {common::STRING}, 2u);
    ASSERT_EQ(tablet.err_code_, common::E_OK);
    ASSERT_EQ(tablet.add_value(0u, 0u, common::String("x", 1)), common::E_OK);
    // This length used to wrap buf_used + len and skip the growth check.
    EXPECT_EQ(tablet.add_value(1u, 0u, common::String("x", UINT32_MAX)),
              common::E_OVERFLOW);
    EXPECT_EQ(tablet.set_column_string_repeated(0u, "x", 1u << 30, 2u),
              common::E_OVERFLOW);
    common::TSDataType type;
    auto* stored = static_cast<common::String*>(tablet.get_value(0u, 0u, type));
    ASSERT_NE(stored, nullptr);
    ASSERT_EQ(stored->len_, 1u);
    EXPECT_EQ(stored->buf_[0], 'x');
    EXPECT_EQ(tablet.add_value(1u, 0u, common::String("ok", 2)), common::E_OK);
}

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
