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
#include <vector>

#include "common/global.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace storage {

namespace {

std::vector<uint8_t> Bytes(const std::string& value) {
    return std::vector<uint8_t>(value.begin(), value.end());
}

class TsFilePropertiesTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = "tsfile_properties_test.tsfile";
        std::remove(file_name_.c_str());
    }

    void TearDown() override {
        std::remove(file_name_.c_str());
        libtsfile_destroy();
    }

    std::string file_name_;
};

TEST_F(TsFilePropertiesTest, WriterPreservesBinaryNullAndEmptyValues) {
    TsFileWriter writer;
    ASSERT_EQ(common::E_OK, writer.open(file_name_));

    const std::vector<uint8_t> first_value = {'f', 'i', 'r', 's', 't'};
    ASSERT_EQ(common::E_OK,
              writer.add_tsfile_property("overwritten", first_value));
    ASSERT_EQ(common::E_OK, writer.flush());

    const std::vector<uint8_t> binary_value = {0x00, 0x7F, 0x80, 0xFF, 0x00};
    ASSERT_EQ(common::E_OK,
              writer.add_tsfile_property("overwritten", binary_value));
    std::vector<uint8_t> copied_value = {0x10, 0x20, 0x30};
    ASSERT_EQ(common::E_OK, writer.add_tsfile_property("copied", copied_value));
    copied_value[0] = 0xFF;
    const std::string embedded_null_key("embedded\0key", 12);
    ASSERT_EQ(common::E_OK,
              writer.add_tsfile_property(embedded_null_key, binary_value));
    ASSERT_EQ(common::E_OK,
              writer.add_tsfile_property("empty", std::vector<uint8_t>()));
    ASSERT_EQ(common::E_OK, writer.add_tsfile_property("null", nullptr, 0));
    ASSERT_EQ(common::E_INVALID_ARG,
              writer.add_tsfile_property("invalid", nullptr, 1));
    ASSERT_EQ(common::E_OK,
              writer.add_tsfile_property("encryptLevel", Bytes("custom")));
    ASSERT_EQ(common::E_OK,
              writer.add_tsfile_property("encryptType", Bytes("custom")));
    ASSERT_EQ(common::E_OK,
              writer.add_tsfile_property("encryptKey", Bytes("custom")));
    ASSERT_EQ(common::E_OK, writer.close());
    ASSERT_EQ(common::E_FILE_WRITE_ERR,
              writer.add_tsfile_property("closed", binary_value));

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(file_name_));
    TsFileProperties properties = reader.get_tsfile_properties();

    ASSERT_FALSE(properties.at("overwritten").is_null);
    EXPECT_EQ(binary_value, properties.at("overwritten").value);
    ASSERT_FALSE(properties.at("empty").is_null);
    EXPECT_TRUE(properties.at("empty").value.empty());
    EXPECT_EQ((std::vector<uint8_t>{0x10, 0x20, 0x30}),
              properties.at("copied").value);
    EXPECT_EQ(binary_value, properties.at(embedded_null_key).value);
    EXPECT_TRUE(properties.at("null").is_null);
    EXPECT_TRUE(properties.at("null").value.empty());

    ASSERT_FALSE(properties.at("encryptLevel").is_null);
    EXPECT_EQ(Bytes("0"), properties.at("encryptLevel").value);
    ASSERT_FALSE(properties.at("encryptType").is_null);
    EXPECT_EQ(Bytes("org.apache.tsfile.encrypt.UNENCRYPTED"),
              properties.at("encryptType").value);
    EXPECT_TRUE(properties.at("encryptKey").is_null);
    EXPECT_TRUE(properties.at("encryptKey").value.empty());
    EXPECT_EQ(common::E_OK, reader.close());
}

}  // namespace

}  // namespace storage
