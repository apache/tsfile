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

#include <cmath>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "cli/exit_codes.h"
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

class CliGorillaV1Input : public ::testing::TestWithParam<common::TSDataType> {
   protected:
    void SetUp() override {
        storage::libtsfile_init();
        saved_float_encoding_ = common::g_config_value_.float_encoding_type_;
        saved_double_encoding_ = common::g_config_value_.double_encoding_type_;
    }
    void TearDown() override {
        common::g_config_value_.float_encoding_type_ = saved_float_encoding_;
        common::g_config_value_.double_encoding_type_ = saved_double_encoding_;
    }

    void expect_nan_rejected(const std::string& token, int nan_row,
                             bool override_encoding) {
        SCOPED_TRACE(token + " at row " + std::to_string(nan_row));
        TempPath csv(".csv");
        TempPath file(".tsfile");
        {
            std::ofstream input(csv.path.c_str());
            input << "time,value\n";
            const int row_count = nan_row == 3 ? 4 : nan_row + 3;
            for (int row = 0; row < row_count; ++row) {
                input << row << ',' << (row == nan_row ? token : "1.5") << '\n';
            }
            ASSERT_TRUE(input.good());
        }
        const std::string type_name = tsfile_cli::tsdatatype_name(GetParam());
        std::vector<std::string> args = {"write",
                                         "--table",
                                         "t",
                                         "--field",
                                         "value",
                                         type_name,
                                         "--compression",
                                         type_name,
                                         "UNCOMPRESSED",
                                         "-i",
                                         csv.path,
                                         "-o",
                                         file.path};
        if (override_encoding) {
            args.insert(args.end(), {"--encoding", type_name, "GORILLA_V1"});
        }
        std::ostringstream out;
        std::ostringstream err;
        EXPECT_EQ(tsfile_cli::run_cli(args, out, err), tsfile_cli::kExitFile)
            << err.str();
        EXPECT_TRUE(out.str().empty());
        EXPECT_NE(err.str().find("NaN"), std::string::npos) << err.str();
        EXPECT_NE(err.str().find("GORILLA_V1"), std::string::npos) << err.str();
        EXPECT_NE(err.str().find("value"), std::string::npos) << err.str();
        EXPECT_NE(err.str().find("line " + std::to_string(nan_row + 2)),
                  std::string::npos)
            << err.str();
        EXPECT_FALSE(std::ifstream(file.path.c_str()).good());
    }

   private:
    common::TSEncoding saved_float_encoding_;
    common::TSEncoding saved_double_encoding_;
};

TEST_P(CliGorillaV1Input, RejectsNaNIncludingAfterWrittenBatch) {
    for (const char* token :
         {"nan", "NAN", "+nan", "-NaN", "nan(123)", "\"NaN\""}) {
        for (int row : {0, 1, 3, 1025}) {
            expect_nan_rejected(token, row, true);
        }
    }
}

TEST_P(CliGorillaV1Input, RejectsNaNWithSdkDefaultEncoding) {
    common::g_config_value_.float_encoding_type_ = common::GORILLA_V1;
    common::g_config_value_.double_encoding_type_ = common::GORILLA_V1;
    expect_nan_rejected("nan", 1, false);
}

INSTANTIATE_TEST_SUITE_P(FloatingTypes, CliGorillaV1Input,
                         ::testing::Values(common::FLOAT, common::DOUBLE));

class CliFloatingSpecial : public ::testing::TestWithParam<CodecCase> {};

TEST_P(CliFloatingSpecial, PreservesSupportedSpecialValues) {
    storage::libtsfile_init();
    const auto type = GetParam().first;
    const auto encoding = static_cast<common::TSEncoding>(GetParam().second);
    const std::string type_name = tsfile_cli::tsdatatype_name(type);
    const bool scaled = encoding == common::RLE || encoding == common::TS_2DIFF;
    const std::vector<std::string> values = {
        "1.5", encoding == common::GORILLA_V1 ? "2.5" : "nan",
        "3.5", "\\N",
        "inf", "-inf",
        "0",   "-0",
        "4.5"};
    TempPath csv(".csv");
    TempPath file(".tsfile");
    std::ostringstream expected_csv;
    expected_csv << "time,value\n";
    {
        std::ofstream input(csv.path.c_str());
        input << "time,value\n";
        for (size_t row = 0; row < values.size(); ++row) {
            input << row << ',' << values[row] << '\n';
            expected_csv << row << ','
                         << (scaled && values[row] == "-0" ? "0" : values[row])
                         << '\n';
        }
        ASSERT_TRUE(input.good());
    }
    std::ostringstream out;
    std::ostringstream err;
    ASSERT_EQ(
        tsfile_cli::run_cli(
            {"write", "--table", "t", "--field", "value", type_name,
             "--encoding", type_name, kEncodings[encoding], "--compression",
             type_name, "UNCOMPRESSED", "-i", csv.path, "-o", file.path},
            out, err),
        0)
        << err.str();
    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file.path), common::E_OK);
    storage::ResultSet* result = nullptr;
    ASSERT_EQ(reader.query("t", {"value"}, INT64_MIN, INT64_MAX, result),
              common::E_OK);
    ASSERT_NE(result, nullptr);
    bool has_next = false;
    int ret = common::E_OK;
    size_t row = 0;
    while ((ret = result->next(has_next)) == common::E_OK && has_next) {
        ASSERT_LT(row, values.size());
        EXPECT_EQ(result->get_value<int64_t>(1), static_cast<int64_t>(row));
        EXPECT_EQ(result->is_null(2), values[row] == "\\N");
        if (!result->is_null(2)) {
            const double actual = type == common::FLOAT
                                      ? result->get_value<float>(2)
                                      : result->get_value<double>(2);
            if (values[row] == "nan") {
                EXPECT_TRUE(std::isnan(actual));
            } else {
                const double expected = std::stod(values[row]);
                EXPECT_EQ(actual, expected);
                if (!scaled)
                    EXPECT_EQ(std::signbit(actual), std::signbit(expected));
            }
        }
        ++row;
    }
    reader.destroy_query_data_set(result);
    EXPECT_EQ(ret, common::E_OK);
    EXPECT_EQ(row, values.size());
    std::ostringstream cat_out;
    std::ostringstream cat_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"cat", "-f", "csv", file.path}, cat_out, cat_err),
        0)
        << cat_err.str();
    EXPECT_EQ(cat_out.str(), expected_csv.str());
}

INSTANTIATE_TEST_SUITE_P(
    FloatingCodecs, CliFloatingSpecial, ::testing::ValuesIn([] {
        std::vector<CodecCase> cases;
        for (auto type : {common::FLOAT, common::DOUBLE}) {
            for (auto encoding : {common::PLAIN, common::RLE, common::TS_2DIFF,
                                  common::GORILLA_V1, common::GORILLA,
                                  common::CHIMP, common::RLBE, common::ALP}) {
                cases.emplace_back(type, encoding);
            }
        }
        return cases;
    }()));

TEST(CliCodecSpecialValues, PreservesIntegerEndMarkers) {
    storage::libtsfile_init();
    for (auto type : {common::INT32, common::INT64, common::TIMESTAMP}) {
        for (const char* encoding : {"GORILLA", "CHIMP"}) {
            const std::string type_name = tsfile_cli::tsdatatype_name(type);
            SCOPED_TRACE(type_name + " " + encoding);
            const std::string minimum =
                std::to_string(type == common::INT32 ? INT32_MIN : INT64_MIN);
            const std::string maximum =
                std::to_string(type == common::INT32 ? INT32_MAX : INT64_MAX);
            const std::string rows = "time,value\n0,1\n1," + minimum +
                                     "\n2,2\n3," + maximum + "\n4,3\n";
            TempPath csv(".csv");
            TempPath file(".tsfile");
            {
                std::ofstream input(csv.path.c_str());
                input << rows;
                ASSERT_TRUE(input.good());
            }
            std::ostringstream out;
            std::ostringstream err;
            ASSERT_EQ(tsfile_cli::run_cli(
                          {"write", "--table", "t", "--field", "value",
                           type_name, "--encoding", type_name, encoding,
                           "--compression", type_name, "UNCOMPRESSED", "-i",
                           csv.path, "-o", file.path},
                          out, err),
                      0)
                << err.str();
            EXPECT_EQ(
                tsfile_cli::run_cli({"cat", "-f", "csv", file.path}, out, err),
                0)
                << err.str();
            EXPECT_EQ(out.str(), rows);
            out.str("");
            EXPECT_EQ(tsfile_cli::run_cli({"cat", "-f", "csv", "--offset", "2",
                                           "-n", "2", file.path},
                                          out, err),
                      0)
                << err.str();
            EXPECT_EQ(out.str(), "time,value\n2,2\n3," + maximum + "\n");
        }
    }
}

}  // namespace
