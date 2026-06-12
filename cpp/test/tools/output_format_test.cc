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

#include "format/output_format.h"

#include <gtest/gtest.h>

#include <sstream>
#include <vector>

#include "common/db_common.h"
#include "utils/errno_define.h"

using tsfile_cli::OutputFormat;
using tsfile_cli::ParsedArgs;
using tsfile_cli::RowWriter;

TEST(ErrorCodeMessageTest, KnownCodesMapToReadablePhrases) {
    EXPECT_STREQ(tsfile_cli::error_code_message(common::E_TABLE_NOT_EXIST),
                 "table does not exist");
    EXPECT_STREQ(tsfile_cli::error_code_message(common::E_DEVICE_NOT_EXIST),
                 "device does not exist");
    EXPECT_STREQ(
        tsfile_cli::error_code_message(common::E_MEASUREMENT_NOT_EXIST),
        "measurement does not exist");
    EXPECT_STREQ(tsfile_cli::error_code_message(common::E_TSFILE_CORRUPTED),
                 "file is corrupted");
    EXPECT_STREQ(tsfile_cli::error_code_message(common::E_OUT_OF_ORDER),
                 "data is out of order");
    EXPECT_STREQ(tsfile_cli::error_code_message(common::E_DECODE_ERR),
                 "failed to decode data");
}

TEST(ErrorCodeMessageTest, UnknownCodeFallsBackToInternalError) {
    EXPECT_STREQ(tsfile_cli::error_code_message(987654), "internal error");
    // The phrase is always a non-empty, printable string (never a bare code).
    EXPECT_GT(std::string(tsfile_cli::error_code_message(-1)).size(), 0u);
}

TEST(ResolveFormatTest, AutoUsesTableOnTtyTsvOtherwise) {
    EXPECT_EQ(tsfile_cli::resolve_format(ParsedArgs::Format::kAuto, true),
              OutputFormat::kTable);
    EXPECT_EQ(tsfile_cli::resolve_format(ParsedArgs::Format::kAuto, false),
              OutputFormat::kTsv);
    EXPECT_EQ(tsfile_cli::resolve_format(ParsedArgs::Format::kJson, true),
              OutputFormat::kJson);
}

TEST(CsvEscapeTest, QuotesWhenSpecialCharsPresent) {
    EXPECT_EQ(tsfile_cli::csv_escape("plain"), "plain");
    EXPECT_EQ(tsfile_cli::csv_escape("a,b"), "\"a,b\"");
    EXPECT_EQ(tsfile_cli::csv_escape("she said \"hi\""),
              "\"she said \"\"hi\"\"\"");
    EXPECT_EQ(tsfile_cli::csv_escape("line\nbreak"), "\"line\nbreak\"");
}

TEST(JsonEscapeTest, EscapesQuotesBackslashAndControls) {
    EXPECT_EQ(tsfile_cli::json_escape("a\"b\\c"), "a\\\"b\\\\c");
    EXPECT_EQ(tsfile_cli::json_escape("tab\there"), "tab\\there");
}

TEST(TypeNameTest, KnownTypesMapToNames) {
    EXPECT_STREQ(tsfile_cli::tsdatatype_name(common::INT64), "INT64");
    EXPECT_STREQ(tsfile_cli::tsdatatype_name(common::STRING), "STRING");
    EXPECT_STREQ(tsfile_cli::tsdatatype_name(common::BOOLEAN), "BOOLEAN");
}

TEST(EncodingNameTest, KnownEncodings) {
    EXPECT_STREQ(tsfile_cli::tsencoding_name(common::PLAIN), "PLAIN");
    EXPECT_STREQ(tsfile_cli::tsencoding_name(common::TS_2DIFF), "TS_2DIFF");
    EXPECT_STREQ(tsfile_cli::tsencoding_name(common::SPRINTZ), "SPRINTZ");
}

TEST(CompressionNameTest, KnownCompressors) {
    EXPECT_STREQ(tsfile_cli::compression_name(common::UNCOMPRESSED),
                 "UNCOMPRESSED");
    EXPECT_STREQ(tsfile_cli::compression_name(common::SNAPPY), "SNAPPY");
    EXPECT_STREQ(tsfile_cli::compression_name(common::LZ4), "LZ4");
}

TEST(RowWriterTest, TsvWritesHeaderThenRows) {
    std::ostringstream out;
    RowWriter w(out, OutputFormat::kTsv, {"time", "s1"},
                {common::INT64, common::INT64}, false);
    w.write({"1", "10"}, {false, false});
    w.write({"2", ""}, {false, true});
    w.finish();
    EXPECT_EQ(out.str(), "time\ts1\n1\t10\n2\t\n");
}

TEST(RowWriterTest, NoHeaderSuppressesHeader) {
    std::ostringstream out;
    RowWriter w(out, OutputFormat::kTsv, {"name"}, {common::STRING}, true);
    w.write({"table1"}, {false});
    w.finish();
    EXPECT_EQ(out.str(), "table1\n");
}

TEST(RowWriterTest, CsvEscapesCells) {
    std::ostringstream out;
    RowWriter w(out, OutputFormat::kCsv, {"name"}, {common::STRING}, false);
    w.write({"a,b"}, {false});
    w.finish();
    EXPECT_EQ(out.str(), "name\n\"a,b\"\n");
}

TEST(RowWriterTest, JsonNumbersUnquotedStringsQuotedNullEmitted) {
    std::ostringstream out;
    RowWriter w(out, OutputFormat::kJson, {"time", "name"},
                {common::INT64, common::STRING}, false);
    w.write({"5", "dev1"}, {false, false});
    w.write({"6", ""}, {false, true});
    w.finish();
    EXPECT_EQ(out.str(),
              "{\"time\":5,\"name\":\"dev1\"}\n"
              "{\"time\":6,\"name\":null}\n");
}

TEST(RowWriterTest, TableAlignsColumns) {
    std::ostringstream out;
    RowWriter w(out, OutputFormat::kTable, {"name", "type"},
                {common::STRING, common::STRING}, false);
    w.write({"s1", "INT64"}, {false, false});
    w.write({"longname", "BOOLEAN"}, {false, false});
    w.finish();
    EXPECT_EQ(out.str(),
              "name      type\n"
              "s1        INT64\n"
              "longname  BOOLEAN\n");
}
