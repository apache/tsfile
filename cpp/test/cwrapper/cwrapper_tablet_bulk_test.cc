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

TEST(CWrapperTabletBulkTest, CopiesTimestampsValuesAndNullBitmap) {
    const char* path = "cwrapper_tablet_bulk.tsfile";
    remove(path);

    char name_a[] = "a";
    char name_b[] = "b";
    char* names[] = {name_a, name_b};
    TSDataType types[] = {TS_DATATYPE_DOUBLE, TS_DATATYPE_DOUBLE};
    ColumnSchema columns[] = {{name_a, TS_DATATYPE_DOUBLE, FIELD},
                              {name_b, TS_DATATYPE_DOUBLE, FIELD}};
    TableSchema schema = {const_cast<char*>("signals"), columns, 2};

    ERRNO err = common::E_OK;
    WriteFile file = write_file_new(path, &err);
    ASSERT_NE(nullptr, file);
    ASSERT_EQ(common::E_OK, err);
    TsFileWriter writer = tsfile_writer_new(file, &schema, &err);
    ASSERT_NE(nullptr, writer);
    ASSERT_EQ(common::E_OK, err);

    Tablet tablet = tablet_new(names, types, 2, 3);
    ASSERT_NE(nullptr, tablet);
    const int64_t timestamps[] = {10, 20, 30};
    const double values_a[] = {1.5, 2.5, 3.5};
    const double values_b[] = {10.5, 20.5, 30.5};
    const uint8_t nulls_a[] = {0x02};
    ASSERT_EQ(common::E_OK, tablet_reset(tablet, 0));
    ASSERT_EQ(common::E_OK, tablet_set_timestamps(tablet, timestamps, 3));
    ASSERT_EQ(common::E_OK,
              tablet_set_column_values(tablet, 0, values_a, nulls_a, 3));
    ASSERT_EQ(common::E_OK,
              tablet_set_column_values(tablet, 1, values_b, nullptr, 3));
    ASSERT_EQ(3u, tablet_get_cur_row_size(tablet));
    ASSERT_EQ(common::E_OK, tsfile_writer_write(writer, tablet));
    free_tablet(&tablet);
    ASSERT_EQ(common::E_OK, tsfile_writer_close(writer));
    free_write_file(&file);

    TsFileReader reader = tsfile_reader_new(path, &err);
    ASSERT_NE(nullptr, reader);
    ResultSet result =
        tsfile_query_table(reader, "signals", names, 2, 0, 100, &err);
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(common::E_OK, err);

    int row = 0;
    while (tsfile_result_set_next(result, &err)) {
        ASSERT_EQ(common::E_OK, err);
        EXPECT_EQ(timestamps[row],
                  tsfile_result_set_get_value_by_index_int64_t(result, 1));
        EXPECT_EQ(row == 1, tsfile_result_set_is_null_by_index(result, 2));
        if (row != 1) {
            EXPECT_DOUBLE_EQ(
                values_a[row],
                tsfile_result_set_get_value_by_index_double(result, 2));
        }
        EXPECT_FALSE(tsfile_result_set_is_null_by_index(result, 3));
        EXPECT_DOUBLE_EQ(
            values_b[row],
            tsfile_result_set_get_value_by_index_double(result, 3));
        ++row;
    }
    EXPECT_EQ(common::E_OK, err);
    EXPECT_EQ(3, row);

    free_tsfile_result_set(&result);
    EXPECT_EQ(common::E_OK, tsfile_reader_close(reader));
    remove(path);
}

TEST(CWrapperTabletBulkTest, RejectsInvalidArguments) {
    char name[] = "value";
    char* names[] = {name};
    TSDataType types[] = {TS_DATATYPE_DOUBLE};
    Tablet tablet = tablet_new(names, types, 1, 2);
    ASSERT_NE(nullptr, tablet);
    const int64_t timestamps[] = {1, 2, 3};
    const double values[] = {1.0, 2.0, 3.0};

    EXPECT_EQ(common::E_INVALID_ARG, tablet_reset(nullptr, 0));
    EXPECT_EQ(common::E_INVALID_ARG,
              tablet_set_timestamps(nullptr, timestamps, 2));
    EXPECT_EQ(common::E_INVALID_ARG, tablet_set_timestamps(tablet, nullptr, 1));
    EXPECT_EQ(common::E_OUT_OF_RANGE,
              tablet_set_timestamps(tablet, timestamps, 3));
    EXPECT_EQ(common::E_INVALID_ARG,
              tablet_set_column_values(nullptr, 0, values, nullptr, 2));
    EXPECT_EQ(common::E_INVALID_ARG,
              tablet_set_column_values(tablet, 0, nullptr, nullptr, 1));
    EXPECT_EQ(common::E_OUT_OF_RANGE,
              tablet_set_column_values(tablet, 1, values, nullptr, 2));
    EXPECT_EQ(common::E_OUT_OF_RANGE,
              tablet_set_column_values(tablet, 0, values, nullptr, 3));
    EXPECT_EQ(common::E_OUT_OF_RANGE, tablet_reset(tablet, 3));

    free_tablet(&tablet);
}

}  // namespace cwrapper
