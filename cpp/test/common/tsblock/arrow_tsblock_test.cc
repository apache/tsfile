/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#include <cstring>

#include "common/tsblock/tsblock.h"
#include "cwrapper/tsfile_cwrapper.h"
#include "utils/db_utils.h"

// Forward declarations for arrow namespace (functions are defined in
// arrow_c.cc)
namespace arrow {
// Type aliases for Arrow types (defined in tsfile_cwrapper.h)
using ArrowArray = ::ArrowArray;
using ArrowSchema = ::ArrowSchema;
#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

// Function declaration (defined in arrow_c.cc)
int TsBlockToArrowStruct(common::TsBlock& tsblock, ArrowArray* out_array,
                         ArrowSchema* out_schema);
}  // namespace arrow

static void VerifyArrowSchema(
    const ::arrow::ArrowSchema* schema,
    const std::vector<std::string>& expected_names,
    const std::vector<const char*>& expected_formats) {
    ASSERT_NE(schema, nullptr);
    EXPECT_STREQ(schema->format, "+s");
    EXPECT_EQ(schema->n_children, expected_names.size());
    ASSERT_NE(schema->children, nullptr);

    for (size_t i = 0; i < expected_names.size(); ++i) {
        const arrow::ArrowSchema* child = schema->children[i];
        ASSERT_NE(child, nullptr);
        EXPECT_STREQ(child->name, expected_names[i].c_str());
        EXPECT_STREQ(child->format, expected_formats[i]);
        EXPECT_EQ(child->flags, ARROW_FLAG_NULLABLE);
    }
}

static void VerifyArrowArrayData(const arrow::ArrowArray* array,
                                 uint32_t expected_length) {
    ASSERT_NE(array, nullptr);
    EXPECT_EQ(array->length, expected_length);
    EXPECT_EQ(array->n_children, 3);
    ASSERT_NE(array->children, nullptr);
}

TEST(ArrowTsBlockTest, NormalTsBlock_NoNulls) {
    common::TupleDesc tuple_desc;
    common::ColumnSchema col1("int_col", common::INT32, common::SNAPPY,
                              common::RLE);
    common::ColumnSchema col2("double_col", common::DOUBLE, common::SNAPPY,
                              common::RLE);
    common::ColumnSchema col3("string_col", common::STRING, common::SNAPPY,
                              common::RLE);
    tuple_desc.push_back(col1);
    tuple_desc.push_back(col2);
    tuple_desc.push_back(col3);

    common::TsBlock tsblock(&tuple_desc, 10);
    ASSERT_EQ(tsblock.init(), common::E_OK);

    common::RowAppender row_appender(&tsblock);

    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(row_appender.add_row());

        int32_t int_val = 100 + i;
        row_appender.append(0, reinterpret_cast<const char*>(&int_val),
                            sizeof(int32_t));
        double double_val = 3.14 + i;
        row_appender.append(1, reinterpret_cast<const char*>(&double_val),
                            sizeof(double));
        std::string str_val = "test" + std::to_string(i);
        row_appender.append(2, str_val.c_str(), str_val.length());
    }

    EXPECT_EQ(tsblock.get_row_count(), 5);

    arrow::ArrowArray array;
    arrow::ArrowSchema schema;
    int ret = arrow::TsBlockToArrowStruct(tsblock, &array, &schema);
    ASSERT_EQ(ret, common::E_OK);

    std::vector<std::string> expected_names = {"int_col", "double_col",
                                               "string_col"};
    std::vector<const char*> expected_formats = {"i", "g", "u"};
    VerifyArrowSchema(&schema, expected_names, expected_formats);

    VerifyArrowArrayData(&array, 5);

    ASSERT_NE(array.children, nullptr);
    ASSERT_NE(array.children[0], nullptr);
    ASSERT_NE(array.children[1], nullptr);
    ASSERT_NE(array.children[2], nullptr);

    const ArrowArray* int_array = array.children[0];
    EXPECT_EQ(int_array->length, 5);
    EXPECT_EQ(int_array->null_count, 0);
    ASSERT_NE(int_array->buffers, nullptr);
    const int32_t* int_data = reinterpret_cast<const int32_t*>(
        int_array->buffers[int_array->n_buffers - 1]);
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(int_data[i], 100 + i);
    }

    const arrow::ArrowArray* double_array = array.children[1];
    EXPECT_EQ(double_array->length, 5);
    EXPECT_EQ(double_array->null_count, 0);
    const double* double_data = reinterpret_cast<const double*>(
        double_array->buffers[double_array->n_buffers - 1]);
    for (int i = 0; i < 5; ++i) {
        EXPECT_DOUBLE_EQ(double_data[i], 3.14 + i);
    }
    const arrow::ArrowArray* string_array = array.children[2];
    EXPECT_EQ(string_array->length, 5);
    EXPECT_EQ(string_array->null_count, 0);
    ASSERT_NE(string_array->buffers, nullptr);
    const int32_t* offsets =
        reinterpret_cast<const int32_t*>(string_array->buffers[1]);
    const char* string_data =
        reinterpret_cast<const char*>(string_array->buffers[2]);

    for (int i = 0; i < 5; ++i) {
        int32_t start = offsets[i];
        int32_t end = offsets[i + 1];
        std::string expected_str = "test" + std::to_string(i);
        std::string actual_str(string_data + start, end - start);
        EXPECT_EQ(actual_str, expected_str);
    }

    if (array.release != nullptr) {
        array.release(&array);
    }
    if (schema.release != nullptr) {
        schema.release(&schema);
    }
}

TEST(ArrowTsBlockTest, TsBlock_WithNulls) {
    common::TupleDesc tuple_desc;
    common::ColumnSchema col1("int_col", common::INT32, common::SNAPPY,
                              common::RLE);
    common::ColumnSchema col2("double_col", common::DOUBLE, common::SNAPPY,
                              common::RLE);
    common::ColumnSchema col3("string_col", common::STRING, common::SNAPPY,
                              common::RLE);
    tuple_desc.push_back(col1);
    tuple_desc.push_back(col2);
    tuple_desc.push_back(col3);

    common::TsBlock tsblock(&tuple_desc, 10);
    ASSERT_EQ(tsblock.init(), common::E_OK);

    common::RowAppender row_appender(&tsblock);
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(row_appender.add_row());

        if (i == 1) {
            row_appender.append_null(0);
            row_appender.append_null(1);
            row_appender.append_null(2);
        } else if (i == 3) {
            row_appender.append_null(0);
            double double_val = 3.14 + i;
            row_appender.append(1, reinterpret_cast<const char*>(&double_val),
                                sizeof(double));
            std::string str_val = "test" + std::to_string(i);
            row_appender.append(2, str_val.c_str(), str_val.length());
        } else {
            int32_t int_val = 100 + i;
            row_appender.append(0, reinterpret_cast<const char*>(&int_val),
                                sizeof(int32_t));
            double double_val = 3.14 + i;
            row_appender.append(1, reinterpret_cast<const char*>(&double_val),
                                sizeof(double));
            std::string str_val = "test" + std::to_string(i);
            row_appender.append(2, str_val.c_str(), str_val.length());
        }
    }

    EXPECT_EQ(tsblock.get_row_count(), 5);

    arrow::ArrowArray array;
    arrow::ArrowSchema schema;
    int ret = arrow::TsBlockToArrowStruct(tsblock, &array, &schema);
    ASSERT_EQ(ret, common::E_OK);

    std::vector<std::string> expected_names = {"int_col", "double_col",
                                               "string_col"};
    std::vector<const char*> expected_formats = {"i", "g", "u"};
    VerifyArrowSchema(&schema, expected_names, expected_formats);

    VerifyArrowArrayData(&array, 5);

    const arrow::ArrowArray* int_array = array.children[0];
    EXPECT_EQ(int_array->null_count, 2);

    const arrow::ArrowArray* double_array = array.children[1];
    EXPECT_EQ(double_array->null_count, 1);

    const arrow::ArrowArray* string_array = array.children[2];
    EXPECT_EQ(string_array->null_count, 1);

    ASSERT_NE(int_array->buffers[0], nullptr);
    const uint8_t* null_bitmap =
        reinterpret_cast<const uint8_t*>(int_array->buffers[0]);
    EXPECT_FALSE(null_bitmap[0] & (1 << 1));
    EXPECT_FALSE(null_bitmap[0] & (1 << 3));
    EXPECT_TRUE(null_bitmap[0] & (1 << 0));
    EXPECT_TRUE(null_bitmap[0] & (1 << 2));
    EXPECT_TRUE(null_bitmap[0] & (1 << 4));

    const int32_t* int_data = reinterpret_cast<const int32_t*>(
        int_array->buffers[int_array->n_buffers - 1]);
    EXPECT_NE(int_data, nullptr);
    if (array.release != nullptr) {
        array.release(&array);
    }
    if (schema.release != nullptr) {
        schema.release(&schema);
    }
}

TEST(ArrowTsBlockTest, TsBlock_EdgeCases) {
    {
        common::TupleDesc tuple_desc;
        common::ColumnSchema col1("single_col", common::INT64, common::SNAPPY,
                                  common::RLE);
        tuple_desc.push_back(col1);

        common::TsBlock tsblock(&tuple_desc, 5);
        ASSERT_EQ(tsblock.init(), common::E_OK);

        common::RowAppender row_appender(&tsblock);
        for (int i = 0; i < 3; ++i) {
            ASSERT_TRUE(row_appender.add_row());
            int64_t val = 1000 + i;
            row_appender.append(0, reinterpret_cast<const char*>(&val),
                                sizeof(int64_t));
        }

        arrow::ArrowArray array;
        arrow::ArrowSchema schema;
        int ret = arrow::TsBlockToArrowStruct(tsblock, &array, &schema);
        ASSERT_EQ(ret, common::E_OK);

        EXPECT_STREQ(schema.format, "+s");
        EXPECT_EQ(schema.n_children, 1);
        EXPECT_STREQ(schema.children[0]->name, "single_col");
        EXPECT_STREQ(schema.children[0]->format, "l");

        EXPECT_EQ(array.length, 3);
        EXPECT_EQ(array.n_children, 1);
        if (array.release != nullptr) {
            array.release(&array);
        }
        if (schema.release != nullptr) {
            schema.release(&schema);
        }
    }

    {
        common::TupleDesc tuple_desc;
        common::ColumnSchema col1("int_col", common::INT32, common::SNAPPY,
                                  common::RLE);
        common::ColumnSchema col2("double_col", common::DOUBLE, common::SNAPPY,
                                  common::RLE);
        tuple_desc.push_back(col1);
        tuple_desc.push_back(col2);

        const int row_count = 1000;
        common::TsBlock tsblock(&tuple_desc, row_count);
        ASSERT_EQ(tsblock.init(), common::E_OK);

        common::RowAppender row_appender(&tsblock);
        for (int i = 0; i < row_count; ++i) {
            ASSERT_TRUE(row_appender.add_row());
            int32_t int_val = i;
            row_appender.append(0, reinterpret_cast<const char*>(&int_val),
                                sizeof(int32_t));
            double double_val = i * 0.5;
            row_appender.append(1, reinterpret_cast<const char*>(&double_val),
                                sizeof(double));
        }

        arrow::ArrowArray array;
        arrow::ArrowSchema schema;
        int ret = arrow::TsBlockToArrowStruct(tsblock, &array, &schema);
        ASSERT_EQ(ret, common::E_OK);

        EXPECT_EQ(array.length, row_count);
        EXPECT_EQ(array.n_children, 2);

        const arrow::ArrowArray* int_array = array.children[0];
        const int32_t* int_data =
            reinterpret_cast<const int32_t*>(int_array->buffers[1]);
        EXPECT_EQ(int_data[0], 0);
        EXPECT_EQ(int_data[row_count - 1], row_count - 1);

        const arrow::ArrowArray* double_array = array.children[1];
        const double* double_data =
            reinterpret_cast<const double*>(double_array->buffers[1]);
        EXPECT_DOUBLE_EQ(double_data[0], 0.0);
        EXPECT_DOUBLE_EQ(double_data[row_count - 1], (row_count - 1) * 0.5);

        if (array.release != nullptr) {
            array.release(&array);
        }
        if (schema.release != nullptr) {
            schema.release(&schema);
        }
    }
}
