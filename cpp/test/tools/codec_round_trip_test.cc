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
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "cli/run_cli.h"
#include "cli_test_util.h"
#include "common/config/config.h"
#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"
#include "format/result_set_format.h"

namespace {

const common::TSDataType kTypes[] = {
    common::BOOLEAN, common::INT32, common::INT64,     common::FLOAT,
    common::DOUBLE,  common::DATE,  common::TIMESTAMP, common::STRING,
    common::TEXT,    common::BLOB};
const char* const kEncodings[] = {
    "PLAIN",      "DICTIONARY", "RLE",     "DIFF",   "TS_2DIFF", "BITMAP",
    "GORILLA_V1", "REGULAR",    "GORILLA", "ZIGZAG", "FREQ",     "CHIMP",
    "SPRINTZ",    "RLBE",       "CAMEL",   "ALP"};

struct TempPath {
    explicit TempPath(const std::string& ext)
        : path(tsfile_cli_test::unique_temp_path("cli_codec", ext)) {}
    ~TempPath() { std::remove(path.c_str()); }
    std::string path;
};

std::string value_for(common::TSDataType type, int row) {
    switch (type) {
        case common::BOOLEAN:
            return row % 2 ? "true" : "false";
        case common::DATE:
            return "2024-02-29";
        case common::FLOAT:
        case common::DOUBLE:
            return std::to_string(row % 17 + 1) + ".5";
        case common::STRING:
        case common::TEXT:
            return "value_" + std::to_string(row % 17);
        case common::BLOB:
            return "abc";
        default:
            return std::to_string(row % 17 - 8);
    }
}

using CodecCase = std::pair<common::TSDataType, int>;

std::vector<CodecCase> codec_cases() {
    std::vector<CodecCase> cases;
    for (auto type : kTypes) {
        for (int encoding = common::PLAIN; encoding <= common::ALP;
             ++encoding) {
            cases.emplace_back(type, encoding);
        }
    }
    return cases;
}

class CliCodec : public ::testing::TestWithParam<CodecCase> {
   protected:
    void SetUp() override {
        storage::libtsfile_init();
        saved_page_limit_ = common::g_config_value_.page_writer_max_point_num_;
        common::config_set_page_max_point_count(2048);
    }
    void TearDown() override {
        common::config_set_page_max_point_count(saved_page_limit_);
    }

   private:
    uint32_t saved_page_limit_;
};

TEST_P(CliCodec, WriteMatchesSdkFactoriesAndRoundTrips) {
    const auto type = GetParam().first;
    const auto encoding = static_cast<common::TSEncoding>(GetParam().second);
    const std::string type_name = tsfile_cli::tsdatatype_name(type);
    const std::string encoding_name = kEncodings[GetParam().second];
    auto* encoder =
        storage::EncoderFactory::alloc_value_encoder(encoding, type);
    auto* decoder =
        storage::DecoderFactory::alloc_value_decoder(encoding, type);
    const bool supported = encoder != nullptr && decoder != nullptr;
    storage::EncoderFactory::free(encoder);
    storage::DecoderFactory::free(decoder);

    // Cross ALP blocks and the configured page boundary, retaining null rows.
    const int row_count = 2051;
    TempPath csv(".csv");
    TempPath file(".tsfile");
    std::ostringstream expected;
    std::ostringstream expected_csv;
    expected << "time,value\n";
    expected_csv << "time,value\n";
    for (int row = 0; row < row_count; ++row) {
        const std::string value =
            row % 7 == 1
                ? "\\N"
                : value_for(type, encoding == common::ALP ? row - 1024 : row);
        expected << row << ',' << value << '\n';
        expected_csv << row << ','
                     << (type == common::BLOB && row % 7 != 1 ? "0x616263"
                                                              : value)
                     << '\n';
    }
    {
        std::ofstream input(csv.path.c_str());
        input << expected.str();
        ASSERT_TRUE(input.good());
    }
    std::ostringstream out;
    std::ostringstream err;
    const int code = tsfile_cli::run_cli(
        {"write", "--table", "t", "--field", "value", type_name, "--encoding",
         type_name, encoding_name, "--compression", type_name, "UNCOMPRESSED",
         "-i", csv.path, "-o", file.path, "-v"},
        out, err);
    ASSERT_EQ(code, supported ? 0 : 1) << err.str();
    if (!supported) {
        EXPECT_FALSE(std::ifstream(file.path.c_str()).good());
        return;
    }
    EXPECT_NE(err.str().find("encoding=" + encoding_name), std::string::npos)
        << err.str();

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file.path), common::E_OK);
    storage::ResultSet* result = nullptr;
    ASSERT_EQ(reader.query("t", {"value"}, INT64_MIN, INT64_MAX, result),
              common::E_OK);
    ASSERT_NE(result, nullptr);
    std::ostringstream sdk_rows;
    sdk_rows << "time,value\n";
    int row = 0;
    bool has_next = false;
    int ret = common::E_OK;
    while ((ret = result->next(has_next)) == common::E_OK && has_next) {
        EXPECT_EQ(result->get_value<int64_t>(1), row);
        EXPECT_EQ(result->is_null(2), row % 7 == 1);
        sdk_rows << row << ','
                 << (result->is_null(2)
                         ? "\\N"
                         : tsfile_cli::cell_to_string(result, 2, type))
                 << '\n';
        ++row;
    }
    reader.destroy_query_data_set(result);
    EXPECT_EQ(ret, common::E_OK);
    EXPECT_EQ(row, row_count);
    EXPECT_EQ(sdk_rows.str(), expected.str());

    std::ostringstream schema_out;
    std::ostringstream schema_err;
    EXPECT_EQ(tsfile_cli::run_cli({"schema", "-f", "csv", file.path},
                                  schema_out, schema_err),
              0)
        << schema_err.str();
    EXPECT_NE(schema_out.str().find(",FIELD," + type_name + "," +
                                    encoding_name + ",UNCOMPRESSED\n"),
              std::string::npos)
        << schema_out.str();
    std::ostringstream cat_out;
    std::ostringstream cat_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"cat", "-f", "csv", file.path}, cat_out, cat_err),
        0)
        << cat_err.str();
    EXPECT_EQ(cat_out.str(), expected_csv.str());
}

INSTANTIATE_TEST_SUITE_P(SdkMatrix, CliCodec,
                         ::testing::ValuesIn(codec_cases()),
                         [](const ::testing::TestParamInfo<CodecCase>& info) {
                             return std::string(tsfile_cli::tsdatatype_name(
                                        info.param.first)) +
                                    "_" + kEncodings[info.param.second];
                         });

}  // namespace
