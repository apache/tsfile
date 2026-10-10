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
#include <utility>
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

TEST(Utf8OutputTest, PreservesValidSequencesAcrossFormats) {
    const std::string text =
        "ASCII\xc2\x80\xdf\xbf\xe0\xa0\x80\xe4\xb8\xad"
        "\xed\x9f\xbf\xee\x80\x80\xef\xbb\xbf\xf0\x90\x80\x80"
        "\xf0\x9f\x98\x80\xf4\x8f\xbf\xbf";
    EXPECT_EQ(tsfile_cli::csv_escape(text), text);
    EXPECT_EQ(tsfile_cli::json_escape(text), text);
    EXPECT_EQ(tsfile_cli::table_escape(text), text);
}

TEST(Utf8OutputTest, ReplacesMaximalInvalidSubpartsAcrossFormats) {
    const std::string replacement = "\xef\xbf\xbd";
    const std::vector<std::pair<std::string, std::string>> cases = {
        {"\xff", replacement},
        {"\x80\xbf", replacement + replacement},
        {"\xc0\xaf", replacement + replacement},
        {"\xe0\x80\xbf", replacement + replacement + replacement},
        {"\xed\xa0\x80", replacement + replacement + replacement},
        {"\xf4\x91\x92\x93",
         replacement + replacement + replacement + replacement},
        {"\xc2", replacement},
        {"\xe1\x80", replacement},
        {"\xf0\x91\x92", replacement},
        {"a\xc2"
         "b",
         "a" + replacement + "b"},
        {"\xe1\x80"
         "A",
         replacement + "A"},
        {"\xf1\xbf\xe4\xb8\xad", replacement + "\xe4\xb8\xad"},
        {"\xe1\x80\xe2\xf0\x91\x92\xf1\xbf"
         "A",
         replacement + replacement + replacement + replacement + "A"},
    };
    for (const auto& test : cases) {
        SCOPED_TRACE(::testing::PrintToString(test.first));
        EXPECT_EQ(tsfile_cli::csv_escape(test.first), test.second);
        EXPECT_EQ(tsfile_cli::json_escape(test.first), test.second);
        EXPECT_EQ(tsfile_cli::table_escape(test.first), test.second);
    }
}

TEST(Utf8OutputTest, PreservesDelimitersAfterAnInvalidSequence) {
    const std::string text = "\xe1\x80\",\n";
    const std::string replacement = "\xef\xbf\xbd";
    EXPECT_EQ(tsfile_cli::csv_escape(text), "\"" + replacement + "\"\",\n\"");
    EXPECT_EQ(tsfile_cli::json_escape(text), replacement + "\\\",\\n");
    EXPECT_EQ(tsfile_cli::table_escape(text), replacement + "\",\\n");
}

TEST(TableEscapeTest, EscapesBackslashAndNamedControls) {
    EXPECT_EQ(tsfile_cli::table_escape("a\\b"), "a\\\\b");
    EXPECT_EQ(tsfile_cli::table_escape("line\nbreak"), "line\\nbreak");
    EXPECT_EQ(tsfile_cli::table_escape("car\rret"), "car\\rret");
    EXPECT_EQ(tsfile_cli::table_escape("tab\there"), "tab\\there");
}

TEST(TableEscapeTest, OtherControlsBecomeUnicodeEscapes) {
    EXPECT_EQ(tsfile_cli::table_escape(std::string("a\bb", 3)), "a\\u0008b");
    EXPECT_EQ(tsfile_cli::table_escape(std::string("a\fb", 3)), "a\\u000cb");
    EXPECT_EQ(tsfile_cli::table_escape(std::string("\x01\x1f", 2)),
              "\\u0001\\u001f");
}

TEST(TableEscapeTest, ControlByteBoundaries) {
    // NUL is the lowest C0 control; DEL (0x7f) is the only non-C0 control in
    // the C iscntrl() set and must also become \uXXXX rather than pass through.
    EXPECT_EQ(tsfile_cli::table_escape(std::string("\x00", 1)), "\\u0000");
    EXPECT_EQ(tsfile_cli::table_escape(std::string("\x7f", 1)), "\\u007f");
    EXPECT_EQ(tsfile_cli::table_escape(std::string("a\177b", 3)), "a\\u007fb");

    // Space (0x20) is printable: it must survive verbatim, never be escaped.
    EXPECT_EQ(tsfile_cli::table_escape("a b"), "a b");

    // Well-formed UTF-8 must pass through unchanged.
    EXPECT_EQ(tsfile_cli::table_escape("\xe4\xb8\xad"), "\xe4\xb8\xad");
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

TEST(RowWriterTest, TableEscapesControlCharactersOnOneLine) {
    std::ostringstream out;
    RowWriter w(out, OutputFormat::kTable, {"time", "note"},
                {common::INT64, common::STRING}, false);
    w.write({"1000", "line1\nline2"}, {false, false});
    w.write({"2000", "tab\there"}, {false, false});
    w.write({"3000", "back\\slash"}, {false, false});
    w.finish();
    EXPECT_EQ(out.str(),
              "time  note\n"
              "1000  line1\\nline2\n"
              "2000  tab\\there\n"
              "3000  back\\\\slash\n");
}

TEST(RowWriterTest, TableEscapingKeepsColumnAlignment) {
    // When several cells in one row contain controls, each expanded escape
    // must still be padded to the same visual width computed at write time.
    std::ostringstream out;
    RowWriter w(out, OutputFormat::kTable, {"a", "b"},
                {common::STRING, common::STRING}, false);
    w.write({"x\ty", "long\nvalue"}, {false, false});
    w.write({"p", "q"}, {false, false});
    w.finish();
    EXPECT_EQ(out.str(),
              "a     b\n"
              "x\\ty  long\\nvalue\n"
              "p     q\n");
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

TEST(RowWriterTest, ReplacesInvalidUtf8InHeadersAndValues) {
    const std::string header = "name\xff";
    const std::string value = "value\xe1\x80";
    const std::string replacement = "\xef\xbf\xbd";
    const OutputFormat formats[] = {OutputFormat::kCsv, OutputFormat::kJson,
                                    OutputFormat::kTable};
    for (OutputFormat format : formats) {
        std::ostringstream out;
        RowWriter writer(out, format, {header}, {common::STRING}, false);
        ASSERT_TRUE(writer.write({value}, {false}));
        ASSERT_TRUE(writer.finish());
        const std::string expected =
            format == OutputFormat::kJson
                ? "{\"name" + replacement + "\":\"value" + replacement + "\"}\n"
                : "name" + replacement + "\nvalue" + replacement + "\n";
        EXPECT_EQ(out.str(), expected);
    }
}

TEST(RowWriterTest, PreservesNonUtf8BlobBytesAsHex) {
    const OutputFormat formats[] = {OutputFormat::kCsv, OutputFormat::kJson,
                                    OutputFormat::kTable};
    for (OutputFormat format : formats) {
        std::ostringstream out;
        RowWriter writer(out, format, {"payload"}, {common::BLOB}, false);
        ASSERT_TRUE(writer.write({std::string("\xff\0\x80", 3)}, {false}));
        ASSERT_TRUE(writer.finish());
        EXPECT_NE(out.str().find("0xff0080"), std::string::npos);
    }
}

TEST(RowWriterTest, RejectsJsonKeyCollisionsAfterUtf8Replacement) {
    const std::vector<std::vector<std::string>> headers = {
        {"bad\xff", "bad\xfe"},
        {"bad\xff", "bad\xef\xbf\xbd"},
    };
    for (const auto& header : headers) {
        for (bool no_header : {false, true}) {
            std::ostringstream out;
            RowWriter writer(out, OutputFormat::kJson, header,
                             {common::INT64, common::INT64}, no_header);
            EXPECT_FALSE(writer.write({"42", "7"}, {false, false}));
            EXPECT_FALSE(writer.finish());
            EXPECT_TRUE(out.str().empty());
        }
    }
}

TEST(RowWriterTest, RejectsJsonKeyCollisionsWithoutRows) {
    std::ostringstream out;
    RowWriter writer(out, OutputFormat::kJson, {"bad\xff", "bad\xfe"},
                     {common::STRING, common::STRING}, false);
    EXPECT_FALSE(writer.finish());
    EXPECT_TRUE(out.str().empty());
}

TEST(RowWriterTest, CsvEscapesLeadingBackslashesOnlyInTextValues) {
    std::ostringstream out;
    RowWriter writer(out, OutputFormat::kCsv, {R"(\header)", "note"},
                     {common::STRING, common::TEXT}, false);
    ASSERT_TRUE(writer.write({R"(\N)", R"(\a,b)"}, {false, false}));
    ASSERT_TRUE(writer.write({R"(back\slash)", ""}, {false, true}));
    ASSERT_TRUE(writer.finish());
    EXPECT_EQ(out.str(), R"csv(\header,note
\\N,"\\a,b"
back\slash,\N
)csv");
}
