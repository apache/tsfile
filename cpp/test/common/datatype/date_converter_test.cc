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
#include "common/datatype/date_converter.h"

#include <gtest/gtest.h>

#include "common/datatype/value.h"
#include "common/record.h"

namespace common {

class DateConverterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize a valid date (2025-07-03)
        valid_tm_ = {0, 0,  12, 3,
                     6, 125};  // tm_mday=3, tm_mon=6 (July), tm_year=125 (2025)
        valid_int_ = 20250703;
    }

    std::tm valid_tm_{};
    int32_t valid_int_{};
};

// Test normal date conversion
TEST_F(DateConverterTest, DateToIntValidDate) {
    int32_t result;
    ASSERT_EQ(DateConverter::date_to_int(valid_tm_, result), common::E_OK);
    EXPECT_EQ(result, valid_int_);
}

TEST_F(DateConverterTest, IntToDateValidDate) {
    std::tm result = {0};
    ASSERT_EQ(DateConverter::int_to_date(valid_int_, result), common::E_OK);
    EXPECT_EQ(result.tm_year, valid_tm_.tm_year);
    EXPECT_EQ(result.tm_mon, valid_tm_.tm_mon);
    EXPECT_EQ(result.tm_mday, valid_tm_.tm_mday);
}

// Test round-trip conversion consistency
TEST_F(DateConverterTest, RoundTripConversion) {
    std::tm tm_result = {0};
    int32_t int_result;

    // Forward conversion then backward conversion
    ASSERT_EQ(DateConverter::date_to_int(valid_tm_, int_result), common::E_OK);
    ASSERT_EQ(DateConverter::int_to_date(int_result, tm_result), common::E_OK);
    EXPECT_EQ(tm_result.tm_year, valid_tm_.tm_year);
    EXPECT_EQ(tm_result.tm_mon, valid_tm_.tm_mon);
    EXPECT_EQ(tm_result.tm_mday, valid_tm_.tm_mday);
}

// Test boundary conditions (leap years, month days)
TEST_F(DateConverterTest, BoundaryConditions) {
    // Leap day (Feb 29, 2024)
    std::tm leap_day = {0, 0, 12, 29, 1, 124};  // 2024-02-29
    int32_t leap_int;
    EXPECT_EQ(DateConverter::date_to_int(leap_day, leap_int), common::E_OK);

    // Invalid leap day (Feb 29, 2025 - not a leap year)
    std::tm invalid_leap = {0, 0, 12, 29, 1, 125};  // 2025-02-29
    EXPECT_EQ(DateConverter::date_to_int(invalid_leap, leap_int),
              common::E_INVALID_ARG);

    // First and last day of month
    std::tm first_day = {0, 0, 12, 1, 0, 125};   // 2025-01-01
    std::tm last_day = {0, 0, 12, 31, 11, 125};  // 2025-12-31
    EXPECT_EQ(DateConverter::date_to_int(first_day, leap_int), common::E_OK);
    EXPECT_EQ(DateConverter::date_to_int(last_day, leap_int), common::E_OK);
}

// Test invalid inputs
TEST_F(DateConverterTest, InvalidInputs) {
    std::tm invalid_tm = {0, 0, 12, 32, 6, 125};  // 2025-07-32 (invalid day)
    int32_t out_int;
    EXPECT_EQ(DateConverter::date_to_int(invalid_tm, out_int),
              common::E_INVALID_ARG);

    // Year out of range
    std::tm year_out_of_range = {0, 0, 12,
                                 3, 6, -901};  // 0999-07-03 (year < 1000)
    EXPECT_EQ(DateConverter::date_to_int(year_out_of_range, out_int),
              common::E_INVALID_ARG);

    // Invalid integer format
    std::tm tm_result = {0};
    EXPECT_EQ(DateConverter::int_to_date(20251301, tm_result),
              common::E_INVALID_ARG);  // month=13
    EXPECT_EQ(DateConverter::int_to_date(20250015, tm_result),
              common::E_INVALID_ARG);  // month=0
}

// Test uninitialized fields
TEST_F(DateConverterTest, UninitializedFields) {
    std::tm uninitialized = {
        0};  // tm_year etc. are 0 (not explicitly initialized)
    uninitialized.tm_year = -1;  // Mark as invalid
    int32_t out_int;
    EXPECT_EQ(DateConverter::date_to_int(uninitialized, out_int),
              common::E_INVALID_ARG);
}

}  // namespace common