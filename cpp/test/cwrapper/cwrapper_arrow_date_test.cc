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

#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
}

#include "common/tsblock/tsblock.h"

namespace arrow {
int TsBlockToArrowStruct(common::TsBlock& tsblock, ::ArrowArray* out_array,
                         ::ArrowSchema* out_schema);
}

namespace {

const char* kTableName = "arrow_date_table";

bool arrow_bit_set(const void* bitmap, int64_t index) {
    const uint8_t* bytes = static_cast<const uint8_t*>(bitmap);
    return (bytes[index / 8] >> (index % 8)) & 1;
}

void build_date_tsblock(common::TsBlock* tsblock, bool with_null_date) {
    common::RowAppender appender(tsblock);
    // YYYYMMDD values and their days-since-epoch equivalents.
    const int32_t kDates[3] = {19700101, 20250115, 19991231};
    for (int32_t row = 0; row < 4; ++row) {
        ASSERT_TRUE(appender.add_row());
        int64_t time = row;
        appender.append(0, reinterpret_cast<const char*>(&time), sizeof(time));
        const std::string tag = "devA";
        appender.append(1, tag.c_str(), static_cast<uint32_t>(tag.size()));
        if (with_null_date && row == 3) {
            appender.append_null(2);
            continue;
        }
        const int32_t date_value = row < 3 ? kDates[row] : kDates[0];
        appender.append(2, reinterpret_cast<const char*>(&date_value),
                        sizeof(date_value));
    }
}

common::TupleDesc make_tuple_desc() {
    common::TupleDesc tuple_desc;
    tuple_desc.push_back(common::ColumnSchema("time", common::INT64));
    tuple_desc.push_back(common::ColumnSchema("TAG", common::STRING));
    tuple_desc.push_back(common::ColumnSchema("DATE", common::DATE));
    return tuple_desc;
}

}  // namespace

TEST(CWrapperArrowDateTest, DateColumnIsEncodedAsDaysSinceEpoch) {
    common::TupleDesc tuple_desc = make_tuple_desc();
    common::TsBlock tsblock(&tuple_desc, 4);
    ASSERT_EQ(common::E_OK, tsblock.init());
    build_date_tsblock(&tsblock, true);

    ArrowArray array{};
    ArrowSchema schema{};
    ASSERT_EQ(common::E_OK,
              arrow::TsBlockToArrowStruct(tsblock, &array, &schema));
    ASSERT_EQ(4, array.length);
    ASSERT_EQ(3, array.n_children);
    ASSERT_NE(nullptr, schema.children);
    EXPECT_STREQ("tdD", schema.children[2]->format);

    const ArrowArray* date_child = array.children[2];
    ASSERT_NE(nullptr, date_child);
    EXPECT_EQ(4, date_child->length);
    ASSERT_NE(nullptr, date_child->buffers[1]);
    const int32_t* days = static_cast<const int32_t*>(date_child->buffers[1]);
    EXPECT_EQ(0, days[0]);
    EXPECT_EQ(20103, days[1]);
    EXPECT_EQ(10956, days[2]);

    // The fourth row is NULL: marked in the validity bitmap and zero filled.
    ASSERT_NE(nullptr, date_child->buffers[0]);
    EXPECT_FALSE(arrow_bit_set(date_child->buffers[0], 3));
    EXPECT_TRUE(arrow_bit_set(date_child->buffers[0], 0));
    EXPECT_EQ(0, days[3]);

    if (array.release != nullptr) {
        array.release(&array);
    }
    if (schema.release != nullptr) {
        schema.release(&schema);
    }
}

TEST(CWrapperArrowDateTest, ArrowDatesRoundTripThroughTsFile) {
    common::TupleDesc tuple_desc = make_tuple_desc();
    common::TsBlock tsblock(&tuple_desc, 4);
    ASSERT_EQ(common::E_OK, tsblock.init());
    build_date_tsblock(&tsblock, false);

    ArrowArray array{};
    ArrowSchema schema{};
    ASSERT_EQ(common::E_OK,
              arrow::TsBlockToArrowStruct(tsblock, &array, &schema));

    const std::string path = "cwrapper_arrow_date_roundtrip.tsfile";
    std::remove(path.c_str());
    ERRNO err = RET_OK;
    WriteFile file = write_file_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, file);

    TableSchema table_schema{};
    table_schema.table_name = strdup(kTableName);
    table_schema.column_num = 2;
    table_schema.column_schemas =
        static_cast<ColumnSchema*>(malloc(sizeof(ColumnSchema) * 2));
    table_schema.column_schemas[0].column_name = strdup("TAG");
    table_schema.column_schemas[0].column_category = TAG;
    table_schema.column_schemas[0].data_type = TS_DATATYPE_STRING;
    table_schema.column_schemas[1].column_name = strdup("DATE");
    table_schema.column_schemas[1].column_category = FIELD;
    table_schema.column_schemas[1].data_type = TS_DATATYPE_DATE;

    TsFileWriter writer = tsfile_writer_new(file, &table_schema, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, writer);
    free_table_schema(table_schema);

    ASSERT_EQ(RET_OK, tsfile_writer_write_arrow(writer, &array, &schema, 0));
    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);

    if (array.release != nullptr) {
        array.release(&array);
    }
    if (schema.release != nullptr) {
        schema.release(&schema);
    }

    TsFileReader reader = tsfile_reader_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, reader);
    char tag_column[] = "TAG";
    char date_column[] = "DATE";
    char* columns[2] = {tag_column, date_column};
    ResultSet result_set =
        tsfile_query_table(reader, kTableName, columns, 2, 0, 100, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, result_set);

    // Column 1 is time, column 3 is the DATE field.
    const int32_t expected_dates[4] = {19700101, 20250115, 19991231, 19700101};
    int rows = 0;
    while (tsfile_result_set_next(result_set, &err) && err == RET_OK) {
        EXPECT_FALSE(tsfile_result_set_is_null_by_index(result_set, 3)) << rows;
        EXPECT_EQ(expected_dates[rows],
                  tsfile_result_set_get_value_by_index_int32_t(result_set, 3))
            << rows;
        ++rows;
    }
    EXPECT_EQ(RET_OK, err);
    EXPECT_EQ(4, rows);

    free_tsfile_result_set(&result_set);
    tsfile_reader_close(reader);
    std::remove(path.c_str());
}
