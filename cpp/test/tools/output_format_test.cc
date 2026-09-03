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
#include <streambuf>
#include <vector>

#include "common/db_common.h"
#include "utils/errno_define.h"

using tsfile_cli::OutputFormat;
using tsfile_cli::ParsedArgs;
using tsfile_cli::RowWriter;

namespace {

class FailingStreamBuf : public std::streambuf {
   protected:
    std::streamsize xsputn(const char*, std::streamsize) override { return 0; }
    int_type overflow(int_type) override { return traits_type::eof(); }
};

class FlushFailingStreamBuf : public std::stringbuf {
   protected:
    int sync() override { return -1; }
};

}  // namespace

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
    EXPECT_STREQ(tsfile_cli::error_code_message(common::E_FILE_MAP_ERR),
                 "failed to memory-map file");
}

TEST(ErrorCodeMessageTest, UnknownCodeFallsBackToInternalError) {
    EXPECT_STREQ(tsfile_cli::error_code_message(987654), "internal error");
    // The phrase is always a non-empty, printable string (never a bare code).
    EXPECT_GT(std::string(tsfile_cli::error_code_message(-1)).size(), 0u);
}

TEST(ResolveFormatTest, AutoAlwaysUsesTable) {
    EXPECT_EQ(tsfile_cli::resolve_format(ParsedArgs::Format::kAuto, true),
              OutputFormat::kTable);
    EXPECT_EQ(tsfile_cli::resolve_format(ParsedArgs::Format::kAuto, false),
              OutputFormat::kTable);
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
    EXPECT_STREQ(tsfile_cli::tsencoding_name(common::CHIMP), "CHIMP");
    EXPECT_STREQ(tsfile_cli::tsencoding_name(common::RLBE), "RLBE");
    EXPECT_STREQ(tsfile_cli::tsencoding_name(common::CAMEL), "CAMEL");
}

TEST(CompressionNameTest, KnownCompressors) {
    EXPECT_STREQ(tsfile_cli::compression_name(common::UNCOMPRESSED),
                 "UNCOMPRESSED");
    EXPECT_STREQ(tsfile_cli::compression_name(common::SNAPPY), "SNAPPY");
    EXPECT_STREQ(tsfile_cli::compression_name(common::LZ4), "LZ4");
    EXPECT_STREQ(tsfile_cli::compression_name(common::ZSTD), "ZSTD");
    EXPECT_STREQ(tsfile_cli::compression_name(common::LZMA2), "LZMA2");
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
    RowWriter w(out, OutputFormat::kCsv, {"name", "note"},
                {common::STRING, common::STRING}, false);
    w.write({"a,b", ""}, {false, true});
    w.write({"", ""}, {false, false});
    w.finish();
    EXPECT_EQ(out.str(), "name,note\n\"a,b\",\\N\n\"\",\"\"\n");
}

TEST(RowWriterTest, JsonQuotesInt64TimestampAndLeavesSmallNumbersBare) {
    std::ostringstream out;
    RowWriter w(
        out, OutputFormat::kJson, {"time", "small", "ts", "name"},
        {common::INT64, common::INT32, common::TIMESTAMP, common::STRING},
        false);
    w.write({"5", "10", "1700000000000", "dev1"}, {false, false, false, false});
    w.write({"6", "11", "1700000000001", ""}, {false, false, false, true});
    w.finish();
    EXPECT_EQ(out.str(),
              "{\"time\":\"5\",\"small\":10,\"ts\":\"1700000000000\","
              "\"name\":\"dev1\"}\n"
              "{\"time\":\"6\",\"small\":11,\"ts\":\"1700000000001\","
              "\"name\":null}\n");
}

TEST(RowWriterTest, BlobCellsUseLowercaseHexLexeme) {
    std::ostringstream json;
    RowWriter jw(json, OutputFormat::kJson, {"payload"}, {common::BLOB}, false);
    jw.write({std::string("A\0z", 3)}, {false});
    jw.finish();
    EXPECT_EQ(json.str(), "{\"payload\":\"0x41007a\"}\n");

    std::ostringstream csv;
    RowWriter cw(csv, OutputFormat::kCsv, {"payload"}, {common::BLOB}, false);
    cw.write({"hello"}, {false});
    cw.finish();
    EXPECT_EQ(csv.str(), "payload\n0x68656c6c6f\n");
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

TEST(RowWriterTest, ReportsStreamWriteFailure) {
    FailingStreamBuf buffer;
    std::ostream out(&buffer);
    RowWriter writer(out, OutputFormat::kCsv, {"name"}, {common::STRING},
                     false);
    EXPECT_FALSE(writer.write({"value"}, {false}));
    EXPECT_FALSE(writer.finish());
}

TEST(RowWriterTest, ReportsFlushFailure) {
    FlushFailingStreamBuf buffer;
    std::ostream out(&buffer);
    RowWriter writer(out, OutputFormat::kCsv, {"name"}, {common::STRING},
                     false);
    ASSERT_TRUE(writer.write({"value"}, {false}));
    EXPECT_FALSE(writer.finish());
}
