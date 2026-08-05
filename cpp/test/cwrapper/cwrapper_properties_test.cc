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

#include <cstdio>
#include <cstring>
#include <string>

#include "cwrapper/tsfile_cwrapper.h"
#include "utils/errno_define.h"

namespace {

const TsFileProperty* FindProperty(const TsFileProperty* properties,
                                   uint32_t property_count,
                                   const std::string& key) {
    for (uint32_t i = 0; i < property_count; i++) {
        if (properties[i].key_len == key.size() &&
            std::memcmp(properties[i].key, key.data(), key.size()) == 0) {
            return &properties[i];
        }
    }
    return nullptr;
}

TEST(CWrapperPropertiesTest, GenericWriterRoundTripsLengthAwareValues) {
    const char* file_name = "cwrapper_properties_test.tsfile";
    std::remove(file_name);

    ERRNO error_code = common::E_OK;
    TsFileWriter writer =
        _tsfile_writer_new(file_name, 128 * 1024 * 1024, &error_code);
    ASSERT_NE(nullptr, writer);
    ASSERT_EQ(common::E_OK, error_code);

    const uint8_t first[] = {'f', 'i', 'r', 's', 't'};
    const uint8_t binary[] = {0x00, 0xFF, 0x80, 0x01, 0x00};
    const char embedded_null_key[] = {'k', '\0', 'y'};
    const uint8_t empty_marker = 0;
    EXPECT_EQ(common::E_INVALID_ARG,
              _tsfile_writer_add_tsfile_property(nullptr, "key", 3, binary,
                                                 sizeof(binary)));
    EXPECT_EQ(common::E_INVALID_ARG,
              _tsfile_writer_add_tsfile_property(writer, nullptr, 0, binary,
                                                 sizeof(binary)));
    EXPECT_EQ(common::E_INVALID_ARG,
              _tsfile_writer_add_tsfile_property(writer, "key", 3, nullptr, 1));
    ASSERT_EQ(common::E_OK,
              _tsfile_writer_add_tsfile_property(writer, "overwritten", 11,
                                                 first, sizeof(first)));
    ASSERT_EQ(common::E_OK, _tsfile_writer_flush(writer));
    ASSERT_EQ(common::E_OK,
              _tsfile_writer_add_tsfile_property(writer, "overwritten", 11,
                                                 binary, sizeof(binary)));
    ASSERT_EQ(common::E_OK,
              _tsfile_writer_add_tsfile_property(writer, embedded_null_key,
                                                 sizeof(embedded_null_key),
                                                 binary, sizeof(binary)));
    ASSERT_EQ(common::E_OK, _tsfile_writer_add_tsfile_property(
                                writer, "empty", 5, &empty_marker, 0));
    ASSERT_EQ(common::E_OK, _tsfile_writer_add_tsfile_property(writer, "null",
                                                               4, nullptr, 0));
    ASSERT_EQ(common::E_OK, _tsfile_writer_close(writer));

    TsFileReader reader = tsfile_reader_new(file_name, &error_code);
    ASSERT_NE(nullptr, reader);
    ASSERT_EQ(common::E_OK, error_code);
    TsFileProperty* properties = nullptr;
    uint32_t property_count = 0;
    ASSERT_EQ(common::E_OK, tsfile_reader_get_tsfile_properties(
                                reader, &properties, &property_count));

    const TsFileProperty* overwritten =
        FindProperty(properties, property_count, "overwritten");
    ASSERT_NE(nullptr, overwritten);
    EXPECT_FALSE(overwritten->is_null);
    ASSERT_EQ(sizeof(binary), overwritten->value_len);
    EXPECT_EQ(0, std::memcmp(binary, overwritten->value, sizeof(binary)));

    const TsFileProperty* embedded_key_property =
        FindProperty(properties, property_count,
                     std::string(embedded_null_key, sizeof(embedded_null_key)));
    ASSERT_NE(nullptr, embedded_key_property);
    ASSERT_EQ(sizeof(binary), embedded_key_property->value_len);
    EXPECT_EQ(
        0, std::memcmp(binary, embedded_key_property->value, sizeof(binary)));

    const TsFileProperty* empty =
        FindProperty(properties, property_count, "empty");
    ASSERT_NE(nullptr, empty);
    EXPECT_FALSE(empty->is_null);
    EXPECT_EQ(0U, empty->value_len);

    const TsFileProperty* null_value =
        FindProperty(properties, property_count, "null");
    ASSERT_NE(nullptr, null_value);
    EXPECT_TRUE(null_value->is_null);
    EXPECT_EQ(0U, null_value->value_len);

    tsfile_free_tsfile_properties(properties, property_count);
    TsFileProperty sentinel{};
    properties = &sentinel;
    property_count = 1;
    EXPECT_EQ(common::E_INVALID_ARG,
              tsfile_reader_get_tsfile_properties(nullptr, &properties,
                                                  &property_count));
    EXPECT_EQ(nullptr, properties);
    EXPECT_EQ(0U, property_count);
    EXPECT_EQ(common::E_OK, tsfile_reader_close(reader));
    EXPECT_EQ(0, std::remove(file_name));
}

TEST(CWrapperPropertiesTest, TableWriterSetterUsesExplicitLengths) {
    const char* file_name = "cwrapper_table_properties_test.tsfile";
    std::remove(file_name);

    ERRNO error_code = common::E_OK;
    WriteFile file = write_file_new(file_name, &error_code);
    ASSERT_NE(nullptr, file);
    ASSERT_EQ(common::E_OK, error_code);

    ColumnSchema column = {const_cast<char*>("value"), TS_DATATYPE_INT64,
                           FIELD};
    TableSchema schema = {const_cast<char*>("table"), &column, 1};
    TsFileWriter writer = tsfile_writer_new(file, &schema, &error_code);
    ASSERT_NE(nullptr, writer);
    const uint8_t binary[] = {0xAA, 0x00, 0xBB};
    ASSERT_EQ(common::E_OK, tsfile_writer_add_tsfile_property(
                                writer, "binary", 6, binary, sizeof(binary)));
    ASSERT_EQ(common::E_OK, tsfile_writer_close(writer));
    free_write_file(&file);

    TsFileReader reader = tsfile_reader_new(file_name, &error_code);
    ASSERT_NE(nullptr, reader);
    TsFileProperty* properties = nullptr;
    uint32_t property_count = 0;
    ASSERT_EQ(common::E_OK, tsfile_reader_get_tsfile_properties(
                                reader, &properties, &property_count));
    const TsFileProperty* property =
        FindProperty(properties, property_count, "binary");
    ASSERT_NE(nullptr, property);
    ASSERT_EQ(sizeof(binary), property->value_len);
    EXPECT_EQ(0, std::memcmp(binary, property->value, sizeof(binary)));
    tsfile_free_tsfile_properties(properties, property_count);
    EXPECT_EQ(common::E_OK, tsfile_reader_close(reader));
    EXPECT_EQ(0, std::remove(file_name));
}

}  // namespace
