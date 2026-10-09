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

#include "format/input_format.h"

#include <gtest/gtest.h>

#include <sstream>

#include "common/db_common.h"
#include "utils/db_utils.h"

TEST(InputFormatTest, NormalizeWriteColumnsValid) {
    const std::vector<tsfile_cli::WriteColumnSpec> specs = {
        {"id1", "STRING", true}, {"s1", "INT64", false}};
    std::vector<tsfile_cli::ColumnDef> cols;
    std::string err;
    EXPECT_TRUE(tsfile_cli::normalize_write_columns(specs, cols, err));
    ASSERT_EQ(cols.size(), 2u);
    EXPECT_EQ(cols[0].name, "id1");
    EXPECT_EQ(cols[0].type, common::STRING);
    EXPECT_EQ(cols[0].category, common::ColumnCategory::TAG);
    EXPECT_EQ(cols[1].type, common::INT64);
    EXPECT_EQ(cols[1].category, common::ColumnCategory::FIELD);
}

TEST(InputFormatTest, NormalizeWriteColumnsRejectsNonCanonicalType) {
    const std::vector<tsfile_cli::WriteColumnSpec> specs = {
        {"s1", "int64", false}};
    std::vector<tsfile_cli::ColumnDef> cols;
    std::string err;
    EXPECT_FALSE(tsfile_cli::normalize_write_columns(specs, cols, err));
    EXPECT_NE(err.find("unknown type"), std::string::npos) << err;
}

TEST(InputFormatTest, NormalizeWriteColumnsPreservesCategories) {
    const std::vector<tsfile_cli::WriteColumnSpec> specs = {
        {"id1", "STRING", true}, {"s1", "INT64", false}};
    std::vector<tsfile_cli::ColumnDef> cols;
    std::string err;
    EXPECT_TRUE(tsfile_cli::normalize_write_columns(specs, cols, err)) << err;
    ASSERT_EQ(cols.size(), 2u);
    EXPECT_EQ(cols[0].category, common::ColumnCategory::TAG);
    EXPECT_EQ(cols[1].category, common::ColumnCategory::FIELD);
}

TEST(InputFormatTest, NormalizeWriteColumnsExtendedTypes) {
    const std::vector<tsfile_cli::WriteColumnSpec> specs = {
        {"ts", "TIMESTAMP", false}, {"d", "DATE", false}, {"b", "BLOB", false}};
    std::vector<tsfile_cli::ColumnDef> cols;
    std::string err;
    EXPECT_TRUE(tsfile_cli::normalize_write_columns(specs, cols, err)) << err;
    ASSERT_EQ(cols.size(), 3u);
    EXPECT_EQ(cols[0].type, common::TIMESTAMP);
    EXPECT_EQ(cols[1].type, common::DATE);
    EXPECT_EQ(cols[2].type, common::BLOB);
}

TEST(InputFormatTest, NormalizeWriteColumnsErrors) {
    const std::vector<tsfile_cli::WriteColumnSpec> bad_type = {
        {"s1", "NOPE", false}};
    const std::vector<tsfile_cli::WriteColumnSpec> empty_name = {
        {"", "INT64", false}};
    std::vector<tsfile_cli::ColumnDef> cols;
    std::string err;
    EXPECT_FALSE(tsfile_cli::normalize_write_columns(bad_type, cols, err));
    EXPECT_FALSE(tsfile_cli::normalize_write_columns(empty_name, cols, err));
}

TEST(InputFormatTest, NormalizeWriteColumnsRejectsDuplicateNames) {
    const std::vector<tsfile_cli::WriteColumnSpec> specs = {
        {"s1", "INT64", false}, {"s1", "INT32", false}};
    std::vector<tsfile_cli::ColumnDef> cols;
    std::string err;
    EXPECT_FALSE(tsfile_cli::normalize_write_columns(specs, cols, err));
    EXPECT_NE(err.find("duplicate column"), std::string::npos) << err;
}

TEST(InputFormatTest, SplitLineTsv) {
    std::vector<std::string> f =
        tsfile_cli::split_line("0\t10\t20", '\t', false);
    ASSERT_EQ(f.size(), 3u);
    EXPECT_EQ(f[0], "0");
    EXPECT_EQ(f[2], "20");
}

TEST(InputFormatTest, SplitLineCsvQuotes) {
    std::vector<std::string> f =
        tsfile_cli::split_line("1,\"a,b\",\"she \"\"hi\"\"\"", ',', true);
    ASSERT_EQ(f.size(), 3u);
    EXPECT_EQ(f[1], "a,b");
    EXPECT_EQ(f[2], "she \"hi\"");
}

TEST(InputFormatTest, SplitLineEmptyFields) {
    std::vector<std::string> f = tsfile_cli::split_line("0,,5", ',', true);
    ASSERT_EQ(f.size(), 3u);
    EXPECT_EQ(f[1], "");
}

TEST(InputFormatTest, ParseBoolCell) {
    bool b = false;
    EXPECT_TRUE(tsfile_cli::parse_bool_cell("true", b));
    EXPECT_TRUE(b);
    EXPECT_TRUE(tsfile_cli::parse_bool_cell("0", b));
    EXPECT_FALSE(b);
    EXPECT_FALSE(tsfile_cli::parse_bool_cell("maybe", b));
}

TEST(InputFormatTest, ReadRecordSingleLinePlain) {
    std::istringstream in("a,b,c\nd,e,f\n");
    std::string rec;
    long long n = 0;
    ASSERT_TRUE(tsfile_cli::read_record(in, true, rec, n));
    EXPECT_EQ(rec, "a,b,c");
    EXPECT_EQ(n, 1);
    ASSERT_TRUE(tsfile_cli::read_record(in, true, rec, n));
    EXPECT_EQ(rec, "d,e,f");
    EXPECT_EQ(n, 1);
    EXPECT_FALSE(tsfile_cli::read_record(in, true, rec, n));
}

TEST(InputFormatTest, ReadRecordJoinsQuotedNewline) {
    std::istringstream in("1,\"line one\nline two\",x\n2,plain,y\n");
    std::string rec;
    long long n = 0;
    ASSERT_TRUE(tsfile_cli::read_record(in, true, rec, n));
    EXPECT_EQ(rec, "1,\"line one\nline two\",x");
    EXPECT_EQ(n, 2);  // two physical lines made one record
    // The embedded newline survives field splitting inside the quotes.
    std::vector<std::string> f = tsfile_cli::split_line(rec, ',', true);
    ASSERT_EQ(f.size(), 3u);
    EXPECT_EQ(f[1], "line one\nline two");
    ASSERT_TRUE(tsfile_cli::read_record(in, true, rec, n));
    EXPECT_EQ(rec, "2,plain,y");
    EXPECT_EQ(n, 1);
}

TEST(InputFormatTest, ReadRecordStripsCarriageReturn) {
    std::istringstream in("a,b\r\nc,d\r\n");
    std::string rec;
    long long n = 0;
    ASSERT_TRUE(tsfile_cli::read_record(in, true, rec, n));
    EXPECT_EQ(rec, "a,b");
}

TEST(InputFormatTest, ReadRecordTsvIgnoresQuotes) {
    // With csv_quotes false a quote is just data; no line joining happens.
    std::istringstream in("1\t\"open\n2\tclosed\n");
    std::string rec;
    long long n = 0;
    ASSERT_TRUE(tsfile_cli::read_record(in, false, rec, n));
    EXPECT_EQ(rec, "1\t\"open");
    EXPECT_EQ(n, 1);
}

TEST(InputFormatTest, ReadRecordUnterminatedQuoteAtEof) {
    std::istringstream in("1,\"never closed\n");
    std::string rec;
    long long n = 0;
    ASSERT_TRUE(tsfile_cli::read_record(in, true, rec, n));
    EXPECT_EQ(rec, "1,\"never closed");
    EXPECT_EQ(n, 1);
    EXPECT_FALSE(tsfile_cli::read_record(in, true, rec, n));
}
