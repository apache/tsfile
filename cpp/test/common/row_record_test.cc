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
#include "common/row_record.h"

#include <gtest/gtest.h>

#include <vector>

namespace storage {

TEST(FieldTest, DefaultConstructor) {
    Field field;
    EXPECT_EQ(field.type_, common::INVALID_DATATYPE);
}

TEST(FieldTest, TypeConstructor) {
    Field field(common::BOOLEAN);
    EXPECT_EQ(field.type_, common::BOOLEAN);
}

TEST(FieldTest, FreeMemory) {
    Field field(common::TEXT);
    field.value_.sval_ = strdup("test");
    field.free_memory();
    EXPECT_EQ(field.value_.sval_, nullptr);
}

TEST(FieldTest, IsType) {
    Field field(common::BOOLEAN);
    EXPECT_TRUE(field.is_type(common::BOOLEAN));
    EXPECT_FALSE(field.is_type(common::INT32));
}

TEST(FieldTest, IsLiteral) {
    Field field1(common::BOOLEAN);
    EXPECT_TRUE(field1.is_literal());

    Field field2(common::INT64);
    EXPECT_TRUE(field2.is_literal());

    Field field3(common::INVALID_DATATYPE);
    EXPECT_FALSE(field3.is_literal());
}

TEST(FieldTest, SetValue) {
    Field field;
    int32_t i32_val = 123;
    field.set_value(common::INT32, &i32_val);
    EXPECT_EQ(field.type_, common::INT32);
    EXPECT_EQ(field.value_.ival_, 123);

    double d_val = 3.14;
    field.set_value(common::DOUBLE, &d_val);
    EXPECT_EQ(field.type_, common::DOUBLE);
    EXPECT_DOUBLE_EQ(field.value_.dval_, 3.14);
}

TEST(FieldTest, MakeField) {
    Field* field = make(common::BOOLEAN);
    EXPECT_EQ(field->type_, common::BOOLEAN);
    delete field;
}

TEST(FieldTest, MakeLiteralInt64) {
    Field* field = make_literal(int64_t(12345));
    EXPECT_EQ(field->type_, common::INT64);
    EXPECT_EQ(field->value_.lval_, 12345);
    delete field;
}

TEST(FieldTest, MakeLiteralDouble) {
    Field* field = make_literal(3.14);
    EXPECT_EQ(field->type_, common::DOUBLE);
    EXPECT_DOUBLE_EQ(field->value_.dval_, 3.14);
    delete field;
}

TEST(FieldTest, MakeLiteralString) {
    char* text = "test\0";
    Field* field = make_literal(text);
    EXPECT_EQ(field->type_, common::TEXT);
    field->free_memory();
    delete field;
}

TEST(FieldTest, MakeLiteralBool) {
    Field* field = make_literal(true);
    EXPECT_EQ(field->type_, common::BOOLEAN);
    EXPECT_TRUE(field->value_.bval_);
    delete field;
}

TEST(FieldTest, MakeNullLiteral) {
    Field* field = make_null_literal();
    EXPECT_EQ(field->type_, common::NULL_TYPE);
    delete field;
}

TEST(RowRecordTest, ConstructorWithColNum) {
    RowRecord row_record(5);
    EXPECT_EQ(row_record.get_fields()->size(), 5);
    for (Field* field : *row_record.get_fields()) {
        EXPECT_EQ(field->type_, common::NULL_TYPE);
    }
}

TEST(RowRecordTest, ConstructorWithTimestamp) {
    RowRecord row_record(1625140800, 5);
    EXPECT_EQ(row_record.get_timestamp(), 1625140800);
    EXPECT_EQ(row_record.get_fields()->size(), 5);
    for (Field* field : *row_record.get_fields()) {
        EXPECT_EQ(field->type_, common::NULL_TYPE);
    }
}

TEST(RowRecordTest, AddField) {
    RowRecord row_record(5);
    Field* field = make_literal(int64_t(12345));
    row_record.add_field(field);
    EXPECT_EQ(row_record.get_fields()->size(), 6);
    EXPECT_EQ(row_record.get_field(5)->value_.lval_, 12345);
}

TEST(RowRecordTest, AddFieldLargeQuantities) {
    RowRecord row_record(5);
    for (int i = 0; i < 10000; i++) {
        Field* field = make_literal(int64_t(12345));
        row_record.add_field(field);
    }
    EXPECT_EQ(row_record.get_fields()->size(), 10000 + 5);
}

TEST(RowRecordTest, SetAndGetTimestamp) {
    RowRecord row_record(5);
    row_record.set_timestamp(1625140800);
    EXPECT_EQ(row_record.get_timestamp(), 1625140800);
}

}  // namespace storage
