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

#include <gtest/gtest.h>

#include <cstdio>
#include <sstream>
#include <string>

#include "cli/run_cli.h"
#include "cli_test_util.h"

namespace {

struct Fixture {
    std::string path = tsfile_cli_test::write_table_fixture();
    ~Fixture() { std::remove(path.c_str()); }
};

size_t count_lines(const std::string& s) {
    size_t n = 0;
    for (char c : s) {
        if (c == '\n') {
            ++n;
        }
    }
    return n;
}

}  // namespace

TEST(CliE2E, LsListsTableNameTsv) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "name\ntable1\n");
    EXPECT_TRUE(err.str().empty());
}

TEST(CliE2E, LsNoHeaderJustName) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-f", "tsv", "--no-header", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "table1\n");
}

TEST(CliE2E, OpenMissingFileReturnsFileError) {
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"ls", "definitely_missing.tsfile"}, out, err);
    EXPECT_EQ(code, 2);
    EXPECT_FALSE(err.str().empty());
}

TEST(CliE2E, SchemaShowsFieldColumnAndType) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"schema", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(
        out.str().find("target\tmeasurement\tdatatype\tencoding\tcompression"),
        std::string::npos);
    EXPECT_NE(out.str().find("s1"), std::string::npos);
    EXPECT_NE(out.str().find("INT64"), std::string::npos);
}

TEST(CliE2E, SchemaTableMeasurementFilterOnlyShowsRequestedColumn) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"schema", "-m", "s1", "-f", "tsv", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find("table1\ts1\tINT64"), std::string::npos);
    EXPECT_EQ(out.str().find("table1\tid1"), std::string::npos);
    EXPECT_EQ(out.str().find("table1\tid2"), std::string::npos);
}

TEST(CliE2E, StatsReportsCountAndTimeRange) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"stats", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find("target\tmeasurement\tcount\tstart_time\tend_"
                             "time\tmin\tmax\tfirst\tlast\tsum"),
              std::string::npos);
    EXPECT_NE(out.str().find("s1\t5\t0\t4\t0\t40\t0\t40\t100"),
              std::string::npos);
}

TEST(CliE2E, HeadProjectsAndLimits) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"head", "-m", "s1", "-n", "2", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "time\ts1\n0\t0\n1\t10\n");
}

TEST(CliE2E, CatReturnsAllRows) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(count_lines(out.str()), 6u);
    EXPECT_NE(out.str().find("time\ts1\n"), std::string::npos);
}

TEST(CliE2E, CatWithTimeRange) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "2", "--end",
                                    "3", "-f", "tsv", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "time\ts1\n2\t20\n3\t30\n");
}

TEST(CliE2E, CatJsonIsNdjson) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "0", "--end",
                                    "0", "-f", "json", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "{\"time\":0,\"s1\":0}\n");
}

TEST(CliE2E, MetaReportsFileSummary) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"meta", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("file\tmodel\tversion\tdevice_count\ttable_"
                             "count\tseries_count\tstart_time\tend_time\tbloom_"
                             "filter\tfile_size_bytes"),
              std::string::npos);
    EXPECT_NE(out.str().find("\ttable\t"), std::string::npos);
}

TEST(CliE2E, CountReportsSeriesCountsAndTotal) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"count", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("target\tmeasurement\tcount"), std::string::npos);
    EXPECT_NE(out.str().find("\ts1\t5"), std::string::npos);
    EXPECT_NE(out.str().find("total\t\t"), std::string::npos);
}
