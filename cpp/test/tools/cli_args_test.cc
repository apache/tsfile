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

#include "cli/cli_args.h"

#include <gtest/gtest.h>

#include <sstream>

TEST(ParseArgsTest, CommandAndFilePositional) {
    auto p = tsfile_cli::parse_args({"ls", "data.tsfile"});
    EXPECT_TRUE(p.error.empty());
    EXPECT_EQ(p.command, "ls");
    EXPECT_EQ(p.file, "data.tsfile");
}

TEST(ParseArgsTest, FormatFlagParsed) {
    auto p = tsfile_cli::parse_args({"cat", "-f", "ndjson", "data.tsfile"});
    EXPECT_TRUE(p.error.empty());
    EXPECT_EQ(p.format, tsfile_cli::ParsedArgs::Format::kJson);
}

TEST(ParseArgsTest, MeasurementsRepeatOneNamePerOption) {
    auto p =
        tsfile_cli::parse_args({"cat", "-m", "s1", "-m", "s2", "data.tsfile"});
    ASSERT_EQ(p.measurements.size(), 2u);
    EXPECT_EQ(p.measurements[1], "s2");
}

TEST(ParseArgsTest, LimitOffsetAndTimeRange) {
    auto p =
        tsfile_cli::parse_args({"head", "-n", "5", "--offset", "2", "--start",
                                "100", "--end", "200", "data.tsfile"});
    EXPECT_EQ(p.limit, 5);
    EXPECT_EQ(p.offset, 2);
    EXPECT_TRUE(p.has_start);
    EXPECT_EQ(p.start, 100);
    EXPECT_TRUE(p.has_end);
    EXPECT_EQ(p.end, 200);
}

TEST(ParseArgsTest, TagFilterParsed) {
    auto p = tsfile_cli::parse_args(
        {"cat", "--tag-filter", "id1", "eq", "dev_a", "data.tsfile"});
    EXPECT_TRUE(p.error.empty());
    EXPECT_TRUE(p.has_tag_filter);
    ASSERT_EQ(p.tag_filters.size(), 1u);
    EXPECT_EQ(p.tag_filters[0].op, tsfile_cli::ParsedArgs::TagFilterOp::kEq);
    EXPECT_EQ(p.tag_filters[0].column, "id1");
    EXPECT_EQ(p.tag_filters[0].value, "dev_a");
}

TEST(ParseArgsTest, UnknownFlagIsError) {
    auto p = tsfile_cli::parse_args({"ls", "--bogus", "data.tsfile"});
    EXPECT_FALSE(p.error.empty());
}

TEST(ParseArgsTest, BadFormatValueIsError) {
    auto p = tsfile_cli::parse_args({"cat", "-f", "yaml", "data.tsfile"});
    EXPECT_FALSE(p.error.empty());
}

TEST(ParseArgsTest, MissingFileIsAllowedAtParseTime) {
    auto p = tsfile_cli::parse_args({"ls"});
    EXPECT_TRUE(p.error.empty());
    EXPECT_EQ(p.command, "ls");
    EXPECT_TRUE(p.file.empty());
}

TEST(ParseArgsTest, WriteFlagsParsed) {
    auto p = tsfile_cli::parse_args({"write", "--table", "t1", "--field", "s1",
                                     "INT64", "-i", "in.csv", "-o",
                                     "out.tsfile", "-v", "--header-match"});
    EXPECT_TRUE(p.error.empty());
    EXPECT_EQ(p.command, "write");
    EXPECT_EQ(p.table, "t1");
    ASSERT_EQ(p.columns.size(), 1u);
    EXPECT_EQ(p.columns[0].name, "s1");
    EXPECT_EQ(p.columns[0].type_name, "INT64");
    EXPECT_FALSE(p.columns[0].is_tag);
    EXPECT_EQ(p.output, "out.tsfile");
    EXPECT_TRUE(p.verbose);
    EXPECT_TRUE(p.header_match);
    EXPECT_EQ(p.file, "in.csv");
    EXPECT_TRUE(p.input_set);
}

TEST(ParseArgsTest, OutputFlagNeedsValue) {
    auto p = tsfile_cli::parse_args({"write", "-o"});
    EXPECT_FALSE(p.error.empty());
}

TEST(ParseArgsTest, StdinFlagParsed) {
    auto p = tsfile_cli::parse_args({"write", "--table", "t1", "--field", "s1",
                                     "INT64", "--stdin", "-o", "out.tsfile"});
    EXPECT_TRUE(p.error.empty());
    EXPECT_EQ(p.file, "-");
    EXPECT_TRUE(p.input_set);
}
