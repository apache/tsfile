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

#include "cwrapper/tsfile_cwrapper.h"
#include "utils/errno_define.h"

namespace cwrapper {

class CWrapperNumericBlockTest : public testing::Test {
   protected:
    void SetUp() override {
        remove(kPath);
        char* names[] = {name_a_, name_b_};
        TSDataType types[] = {TS_DATATYPE_DOUBLE, TS_DATATYPE_DOUBLE};
        ColumnSchema columns[] = {{name_a_, TS_DATATYPE_DOUBLE, FIELD},
                                  {name_b_, TS_DATATYPE_DOUBLE, FIELD}};
        TableSchema schema = {table_name_, columns, 2};

        ERRNO err = common::E_OK;
        WriteFile file = write_file_new(kPath, &err);
        ASSERT_NE(nullptr, file);
        ASSERT_EQ(common::E_OK, err);
        TsFileWriter writer = tsfile_writer_new(file, &schema, &err);
        ASSERT_NE(nullptr, writer);
        ASSERT_EQ(common::E_OK, err);

        Tablet tablet = tablet_new(names, types, 2, 7);
        ASSERT_NE(nullptr, tablet);
        const int64_t timestamps[] = {10, 20, 30, 40, 50, 60, 70};
        const double values_a[] = {1, 2, 3, 4, 5, 6, 7};
        const double values_b[] = {11, 12, 13, 14, 15, 16, 17};
        const uint8_t nulls_a[] = {0x22};
        const uint8_t nulls_b[] = {0x04};
        ASSERT_EQ(common::E_OK, tablet_set_timestamps(tablet, timestamps, 7));
        ASSERT_EQ(common::E_OK,
                  tablet_set_column_values(tablet, 0, values_a, nulls_a, 7));
        ASSERT_EQ(common::E_OK,
                  tablet_set_column_values(tablet, 1, values_b, nulls_b, 7));
        ASSERT_EQ(common::E_OK, tsfile_writer_write(writer, tablet));
        free_tablet(&tablet);
        ASSERT_EQ(common::E_OK, tsfile_writer_close(writer));
        free_write_file(&file);
    }

    void TearDown() override { remove(kPath); }

    ResultSet QueryBatch(int batch_size, ERRNO* err) {
        char* names[] = {name_a_, name_b_};
        reader_ = tsfile_reader_new(kPath, err);
        EXPECT_NE(nullptr, reader_);
        return tsfile_query_table_batch(reader_, table_name_, names, 2, 0, 100,
                                        nullptr, batch_size, err);
    }

    ResultSet QueryRows(ERRNO* err) {
        char* names[] = {name_a_, name_b_};
        reader_ = tsfile_reader_new(kPath, err);
        EXPECT_NE(nullptr, reader_);
        return tsfile_query_table(reader_, table_name_, names, 2, 0, 100, err);
    }

    void Close(ResultSet* result) {
        free_tsfile_result_set(result);
        ASSERT_EQ(common::E_OK, tsfile_reader_close(reader_));
        reader_ = nullptr;
    }

    static constexpr const char* kPath = "cwrapper_numeric_block.tsfile";
    char table_name_[16] = "signals";
    char name_a_[2] = "a";
    char name_b_[2] = "b";
    TsFileReader reader_ = nullptr;
};

constexpr const char* CWrapperNumericBlockTest::kPath;

TEST_F(CWrapperNumericBlockTest, CopiesMultipleBlocksAndNulls) {
    ERRNO err = common::E_OK;
    ResultSet result = QueryBatch(3, &err);
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(common::E_OK, err);

    int64_t timestamps[3] = {};
    double values[6] = {};
    uint8_t nulls[6] = {};
    const uint32_t expected_rows[] = {3, 3, 1, 0};
    uint32_t global_row = 0;
    for (uint32_t expected : expected_rows) {
        uint32_t rows = 99;
        ASSERT_EQ(common::E_OK, tsfile_result_set_read_numeric_block(
                                    result, TS_DATATYPE_DOUBLE, timestamps,
                                    values, nulls, 3, 2, &rows));
        ASSERT_EQ(expected, rows);
        for (uint32_t row = 0; row < rows; ++row, ++global_row) {
            EXPECT_EQ(static_cast<int64_t>((global_row + 1) * 10),
                      timestamps[row]);
            for (uint32_t col = 0; col < 2; ++col) {
                const bool is_null =
                    (col == 0 && (global_row == 1 || global_row == 5)) ||
                    (col == 1 && global_row == 2);
                EXPECT_EQ(is_null ? 1 : 0, nulls[row * 2 + col]);
                const double expected_value =
                    is_null ? 0.0 : global_row + 1 + col * 10;
                EXPECT_DOUBLE_EQ(expected_value, values[row * 2 + col]);
            }
        }
    }
    EXPECT_EQ(7u, global_row);
    Close(&result);
}

TEST_F(CWrapperNumericBlockTest, SupportsOptionalNullOutput) {
    ERRNO err = common::E_OK;
    ResultSet result = QueryBatch(3, &err);
    int64_t timestamps[3] = {};
    double values[6] = {};
    uint32_t rows = 0;
    ASSERT_EQ(common::E_OK, tsfile_result_set_read_numeric_block(
                                result, TS_DATATYPE_DOUBLE, timestamps, values,
                                nullptr, 3, 2, &rows));
    EXPECT_EQ(3u, rows);
    EXPECT_DOUBLE_EQ(0.0, values[2]);
    Close(&result);
}

TEST_F(CWrapperNumericBlockTest, RejectsInvalidArgumentsAndRowMode) {
    int64_t timestamps[3] = {};
    double values[6] = {};
    uint8_t nulls[6] = {};
    uint32_t rows = 99;
    EXPECT_EQ(common::E_INVALID_ARG,
              tsfile_result_set_read_numeric_block(nullptr, TS_DATATYPE_DOUBLE,
                                                   timestamps, values, nulls, 3,
                                                   2, &rows));
    EXPECT_EQ(0u, rows);

    ERRNO err = common::E_OK;
    ResultSet row_result = QueryRows(&err);
    rows = 99;
    EXPECT_EQ(common::E_INVALID_ARG,
              tsfile_result_set_read_numeric_block(
                  row_result, TS_DATATYPE_DOUBLE, timestamps, values, nulls, 3,
                  2, &rows));
    EXPECT_EQ(0u, rows);
    Close(&row_result);

    ResultSet result = QueryBatch(3, &err);
    EXPECT_EQ(common::E_INVALID_ARG, tsfile_result_set_read_numeric_block(
                                         result, TS_DATATYPE_DOUBLE, nullptr,
                                         values, nulls, 3, 2, &rows));
    EXPECT_EQ(common::E_INVALID_ARG, tsfile_result_set_read_numeric_block(
                                         result, TS_DATATYPE_DOUBLE, timestamps,
                                         nullptr, nulls, 3, 2, &rows));
    EXPECT_EQ(common::E_INVALID_ARG, tsfile_result_set_read_numeric_block(
                                         result, TS_DATATYPE_DOUBLE, timestamps,
                                         values, nulls, 3, 2, nullptr));
    EXPECT_EQ(common::E_INVALID_ARG, tsfile_result_set_read_numeric_block(
                                         result, TS_DATATYPE_DOUBLE, timestamps,
                                         values, nulls, 0, 2, &rows));
    Close(&result);
}

TEST_F(CWrapperNumericBlockTest, RejectsCapacityTypeAndColumnMismatch) {
    ERRNO err = common::E_OK;
    int64_t timestamps[3] = {};
    double values[6] = {};
    uint8_t nulls[6] = {};
    uint32_t rows = 99;

    ResultSet result = QueryBatch(3, &err);
    EXPECT_EQ(common::E_OUT_OF_RANGE,
              tsfile_result_set_read_numeric_block(result, TS_DATATYPE_DOUBLE,
                                                   timestamps, values, nulls, 2,
                                                   2, &rows));
    EXPECT_EQ(0u, rows);
    Close(&result);

    result = QueryBatch(3, &err);
    EXPECT_EQ(
        common::E_TYPE_NOT_MATCH,
        tsfile_result_set_read_numeric_block(
            result, TS_DATATYPE_FLOAT, timestamps, values, nulls, 3, 2, &rows));
    EXPECT_EQ(0u, rows);
    Close(&result);

    result = QueryBatch(3, &err);
    EXPECT_EQ(common::E_INVALID_ARG, tsfile_result_set_read_numeric_block(
                                         result, TS_DATATYPE_DOUBLE, timestamps,
                                         values, nulls, 3, 1, &rows));
    EXPECT_EQ(0u, rows);
    Close(&result);
}

}  // namespace cwrapper
