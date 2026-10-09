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
#include <fstream>
#include <sstream>
#include <string>

#include "cli/run_cli.h"
#include "cli_test_util.h"

namespace {

struct Fixture {
    std::string path = tsfile_cli_test::write_table_fixture();
    ~Fixture() { std::remove(path.c_str()); }
};

struct TagFilterFixture {
    std::string path = tsfile_cli_test::write_tag_filter_fixture();
    ~TagFilterFixture() { std::remove(path.c_str()); }
};

struct MultiTableFixture {
    std::string path = tsfile_cli_test::write_multi_table_fixture();
    ~MultiTableFixture() { std::remove(path.c_str()); }
};

struct DisjointTableFixture {
    std::string path = tsfile_cli_test::write_disjoint_table_fixture();
    ~DisjointTableFixture() { std::remove(path.c_str()); }
};

struct SparseTreeFixture {
    std::string path = tsfile_cli_test::write_sparse_tree_fixture();
    ~SparseTreeFixture() { std::remove(path.c_str()); }
};

struct DisjointTreeFixture {
    std::string path = tsfile_cli_test::write_disjoint_tree_fixture();
    ~DisjointTreeFixture() { std::remove(path.c_str()); }
};

class FlushFailingStreamBuf : public std::stringbuf {
   protected:
    int sync() override { return -1; }
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
    int code = tsfile_cli::run_cli({"ls", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "model,object\ntable,table1\n");
    EXPECT_TRUE(err.str().empty());
}

TEST(CliE2E, LsRejectsNoHeader) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-f", "csv", "--no-header", f.path},
                                   out, err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("--no-header"), std::string::npos) << err.str();
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
    int code = tsfile_cli::run_cli({"schema", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find("model,object,column,category,data_type,encoding,"
                             "compression"),
              std::string::npos);
    EXPECT_NE(out.str().find("s1"), std::string::npos);
    EXPECT_NE(out.str().find("INT64"), std::string::npos);
}

TEST(CliE2E, SchemaTableMeasurementFilterOnlyShowsRequestedColumn) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"schema", "-m", "s1", "-f", "csv", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find("table,table1,s1,FIELD,INT64"), std::string::npos);
    EXPECT_EQ(out.str().find("table1,id1"), std::string::npos);
    EXPECT_EQ(out.str().find("table1,id2"), std::string::npos);
}

TEST(CliE2E, SchemaTableMeasurementFilterIsCaseInsensitive) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"schema", "-m", "S1", "-f", "csv", f.path},
                                   out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_NE(out.str().find("table,table1,s1,FIELD,INT64"), std::string::npos)
        << out.str();
}

TEST(CliE2E, SchemaRejectsMissingTableBeforeWritingOutput) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"schema", "-t", "missing", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("does not exist"), std::string::npos) << err.str();
}

TEST(CliE2E, StatsReportsCountAndTimeRange) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"stats", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find("model,object,tag.id1,tag.id2,field,data_type,"
                             "non_null_count,null_count,min_time,max_time,min,"
                             "max,first,last,sum,stats_source"),
              std::string::npos)
        << out.str();
    EXPECT_NE(out.str().find("table,table1,id1_field_1,id2_field_2,s1,INT64,"
                             "5,\\N,0,4,0,40,0,40,\\N,statistics"),
              std::string::npos)
        << out.str();
}

TEST(CliE2E, HeadProjectsAndLimits) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"head", "-m", "s1", "-n", "2", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(),
              "time,id1,id2,s1\n0,id1_field_1,id2_field_2,0\n1,id1_field_1,"
              "id2_field_2,10\n");
}

TEST(CliE2E, CatReturnsAllRows) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(count_lines(out.str()), 6u);
    EXPECT_NE(out.str().find("time,id1,id2,s1\n"), std::string::npos);
}

TEST(CliE2E, CatRejectsMissingOrNonFieldProjectionBeforeQuery) {
    Fixture f;
    for (const char* measurement : {"missing", "id1"}) {
        std::ostringstream out;
        std::ostringstream err;
        int code = tsfile_cli::run_cli(
            {"cat", "-m", measurement, "-f", "csv", f.path}, out, err);
        EXPECT_EQ(code, 1) << measurement << " " << err.str();
        EXPECT_TRUE(out.str().empty()) << measurement;
        EXPECT_NE(err.str().find("FIELD"), std::string::npos)
            << measurement << " " << err.str();
    }
}

TEST(CliE2E, CatTableProjectionIsCaseInsensitive) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"cat", "-m", "S1", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_EQ(out.str(),
              "time,id1,id2,s1\n0,id1_field_1,id2_field_2,0\n1,id1_field_1,"
              "id2_field_2,10\n2,id1_field_1,id2_field_2,20\n3,id1_field_1,"
              "id2_field_2,30\n4,id1_field_1,id2_field_2,40\n");
}

TEST(CliE2E, CatPushesDownOffsetAndLimit) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"cat", "-m", "s1", "--offset", "2", "-n", "2", "-f", "csv", f.path},
        out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(),
              "time,id1,id2,s1\n2,id1_field_1,id2_field_2,20\n3,id1_field_1,"
              "id2_field_2,30\n");
}

TEST(CliE2E, HeadPushesDownOffsetAndLimit) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"head", "-m", "s1", "--offset", "1", "-n", "3", "-f", "csv", f.path},
        out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(),
              "time,id1,id2,s1\n1,id1_field_1,id2_field_2,10\n2,id1_field_1,"
              "id2_field_2,20\n3,id1_field_1,id2_field_2,30\n");
}

TEST(CliE2E, CatWithTimeRange) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"cat", "-m", "s1", "--start", "2", "--end", "3", "-f", "csv", f.path},
        out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(),
              "time,id1,id2,s1\n2,id1_field_1,id2_field_2,20\n3,id1_field_1,"
              "id2_field_2,30\n");
}

TEST(CliE2E, CatAppliesOffsetAfterTimeRange) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "1", "--end", "4",
                             "--offset", "1", "-n", "2", "-f", "csv", f.path},
                            out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(),
              "time,id1,id2,s1\n2,id1_field_1,id2_field_2,20\n3,id1_field_1,"
              "id2_field_2,30\n");
}

TEST(CliE2E, CatFiltersRowsByTagEq) {
    TagFilterFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1",
                                    "eq", "dev_b", "-f", "csv", f.path},
                                   out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_EQ(out.str(), "time,id1,s1\n1,dev_b,20\n2,dev_b,30\n");
}

TEST(CliE2E, HeadFiltersRowsByTagRegexp) {
    TagFilterFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"head", "-m", "s1", "--tag-filter", "id1", "regexp", "dev_[bc]", "-n",
         "10", "-f", "csv", f.path},
        out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_EQ(out.str(), "time,id1,s1\n1,dev_b,20\n2,dev_b,30\n3,dev_c,40\n");
}

TEST(CliE2E, TagFilterRejectsFieldColumn) {
    TagFilterFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "s1",
                                    "eq", "20", "-f", "csv", f.path},
                                   out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("invalid tag filter column"), std::string::npos)
        << err.str();
}

TEST(CliE2E, TagFiltersDistinguishNullEmptyAndLiteralNull) {
    std::string path = tsfile_cli_test::write_nullable_tag_filter_fixture();

    struct Case {
        std::vector<std::string> filter;
        std::string expected_row;
    };
    const std::vector<Case> cases = {
        {{"id1", "is-null"}, "0,\\N,10\n"},
        {{"id1", "eq", ""}, "1,\"\",20\n"},
        {{"id1", "eq", "null"}, "2,null,30\n"},
    };
    for (const Case& test_case : cases) {
        std::vector<std::string> args = {"cat", "-m", "s1", "--tag-filter"};
        args.insert(args.end(), test_case.filter.begin(),
                    test_case.filter.end());
        args.insert(args.end(), {"-f", "csv", path});
        std::ostringstream out;
        std::ostringstream err;
        ASSERT_EQ(tsfile_cli::run_cli(args, out, err), 0) << err.str();
        EXPECT_EQ(out.str(), "time,id1,s1\n" + test_case.expected_row);
    }

    std::ostringstream not_null_out;
    std::ostringstream not_null_err;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1",
                                   "not-null", "-f", "csv", path},
                                  not_null_out, not_null_err),
              0)
        << not_null_err.str();
    EXPECT_EQ(not_null_out.str().find("0,\\N,10"), std::string::npos)
        << not_null_out.str();
    EXPECT_NE(not_null_out.str().find("1,\"\",20"), std::string::npos)
        << not_null_out.str();

    std::ostringstream neq_out;
    std::ostringstream neq_err;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1",
                                   "neq", "dev_a", "-f", "csv", path},
                                  neq_out, neq_err),
              0)
        << neq_err.str();
    EXPECT_EQ(neq_out.str().find("0,\\N,10"), std::string::npos)
        << neq_out.str();
    EXPECT_EQ(neq_out.str().find("3,dev_a,40"), std::string::npos)
        << neq_out.str();
    EXPECT_NE(neq_out.str().find("4,dev_b,50"), std::string::npos)
        << neq_out.str();

    std::remove(path.c_str());
}

TEST(CliE2E, TagRegexpUsesFullValueAndRejectsInvalidPatterns) {
    std::string path = tsfile_cli_test::write_nullable_tag_filter_fixture();

    std::ostringstream substring_out;
    std::ostringstream substring_err;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1",
                                   "regexp", "dev", "-f", "csv", path},
                                  substring_out, substring_err),
              0)
        << substring_err.str();
    EXPECT_EQ(substring_out.str(), "time,id1,s1\n");

    std::ostringstream full_out;
    std::ostringstream full_err;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1",
                                   "regexp", "dev_.*", "-f", "csv", path},
                                  full_out, full_err),
              0)
        << full_err.str();
    EXPECT_NE(full_out.str().find("3,dev_a,40"), std::string::npos)
        << full_out.str();
    EXPECT_NE(full_out.str().find("4,dev_b,50"), std::string::npos)
        << full_out.str();

    std::ostringstream invalid_out;
    std::ostringstream invalid_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1",
                                   "regexp", "[", "-f", "csv", path},
                                  invalid_out, invalid_err),
              1);
    EXPECT_TRUE(invalid_out.str().empty());
    EXPECT_NE(invalid_err.str().find("invalid regular expression"),
              std::string::npos)
        << invalid_err.str();

    std::remove(path.c_str());
}

TEST(CliE2E, MultipleTagFiltersHonorAll) {
    std::string path = tsfile_cli_test::write_nullable_tag_filter_fixture();
    std::ostringstream out;
    std::ostringstream err;
    ASSERT_EQ(
        tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1", "neq",
                             "dev_a", "--tag-filter", "id1", "regexp", "dev_.*",
                             "--tag-match", "all", "-f", "csv", path},
                            out, err),
        0)
        << err.str();
    EXPECT_EQ(out.str(), "time,id1,s1\n4,dev_b,50\n");
    std::remove(path.c_str());
}

TEST(CliE2E, RowWindowDistinguishesExactAndExcessiveOffset) {
    Fixture f;
    std::ostringstream exact_out;
    std::ostringstream exact_err;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"cat", "-m", "s1", "--offset", "5", "-f", "csv", f.path},
                  exact_out, exact_err),
              0)
        << exact_err.str();
    EXPECT_EQ(exact_out.str(), "time,id1,id2,s1\n");

    std::ostringstream excessive_out;
    std::ostringstream excessive_err;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"cat", "-m", "s1", "--offset", "6", "-f", "csv", f.path},
                  excessive_out, excessive_err),
              1);
    EXPECT_TRUE(excessive_out.str().empty());
    EXPECT_NE(excessive_err.str().find("offset exceeds matched row count"),
              std::string::npos)
        << excessive_err.str();
}

TEST(CliE2E, ZeroLimitUsesEachFormatsZeroRowContract) {
    Fixture f;
    std::ostringstream csv_out;
    std::ostringstream csv_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"cat", "-m", "s1", "-n", "0", "-f", "csv", f.path},
                            csv_out, csv_err),
        0)
        << csv_err.str();
    EXPECT_EQ(csv_out.str(), "time,id1,id2,s1\n");

    std::ostringstream ndjson_out;
    std::ostringstream ndjson_err;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"cat", "-m", "s1", "-n", "0", "-f", "ndjson", f.path},
                  ndjson_out, ndjson_err),
              0)
        << ndjson_err.str();
    EXPECT_TRUE(ndjson_out.str().empty());

    std::ostringstream table_out;
    std::ostringstream table_err;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"cat", "-m", "s1", "-n", "0", "-f", "table", f.path},
                  table_out, table_err),
              0)
        << table_err.str();
    EXPECT_NE(table_out.str().find("time"), std::string::npos)
        << table_out.str();
    EXPECT_NE(table_out.str().find("s1"), std::string::npos) << table_out.str();
    EXPECT_EQ(table_out.str().find("id1_field_1"), std::string::npos)
        << table_out.str();
}

TEST(CliE2E, CatJsonIsNdjson) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "0", "--end",
                                    "0", "-f", "ndjson", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(),
              "{\"time\":\"0\",\"id1\":\"id1_field_1\",\"id2\":"
              "\"id2_field_2\",\"s1\":\"0\"}\n");
}

TEST(CliE2E, StdoutFlushFailureReturnsRuntimeError) {
    Fixture f;
    FlushFailingStreamBuf buffer;
    std::ostream out(&buffer);
    std::ostringstream err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-f", "csv", f.path}, out, err), 3);
    EXPECT_NE(err.str().find("failed to read rows"), std::string::npos)
        << err.str();
}

TEST(CliE2E, MetaReportsFileSummary) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"meta", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("size_bytes,format_version,model\n"),
              std::string::npos)
        << out.str();
    EXPECT_NE(out.str().find(",4,table\n"), std::string::npos) << out.str();
}

TEST(CliE2E, MetaReportsFileHeaderVersionWithoutConstrainingReader) {
    SparseTreeFixture f;
    std::fstream file(f.path.c_str(),
                      std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.good());
    file.seekp(storage::MAGIC_STRING_TSFILE_LEN);
    file.put(static_cast<char>(3));
    file.close();

    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(tsfile_cli::run_cli({"meta", "-f", "csv", f.path}, out, err), 0)
        << err.str();
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find(",3,tree\n"), std::string::npos) << out.str();
}

TEST(CliE2E, CountReportsColumnCountsWithoutSummaryRows) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"count", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("model,object,column,category,row_count,entity_"
                             "count,non_null_count,null_count,min_time,"
                             "max_time,time_source"),
              std::string::npos)
        << out.str();
    EXPECT_NE(out.str().find("table,table1,id1,TAG,5,1,5,0,0,4,scan"),
              std::string::npos)
        << out.str();
    EXPECT_NE(out.str().find("table,table1,s1,FIELD,5,1,5,0,0,4,scan"),
              std::string::npos)
        << out.str();
    EXPECT_EQ(out.str().find("total"), std::string::npos) << out.str();
}

TEST(CliE2E, CountAndStatsDefaultToAllTables) {
    MultiTableFixture f;
    std::ostringstream count_out;
    std::ostringstream count_err;
    EXPECT_EQ(tsfile_cli::run_cli({"count", "-f", "csv", f.path}, count_out,
                                  count_err),
              0)
        << count_err.str();
    EXPECT_NE(count_out.str().find("table,sensors_a,s1,FIELD,1,1,1,0,0,0,scan"),
              std::string::npos)
        << count_out.str();
    EXPECT_NE(count_out.str().find("table,sensors_b,s1,FIELD,1,1,1,0,0,0,scan"),
              std::string::npos)
        << count_out.str();

    std::ostringstream stats_out;
    std::ostringstream stats_err;
    EXPECT_EQ(tsfile_cli::run_cli({"stats", "-f", "csv", f.path}, stats_out,
                                  stats_err),
              0)
        << stats_err.str();
    EXPECT_NE(stats_out.str().find("table,sensors_a,sensors_a_tag,s1,INT64"),
              std::string::npos)
        << stats_out.str();
    EXPECT_NE(stats_out.str().find("table,sensors_b,sensors_b_tag,s1,INT64"),
              std::string::npos)
        << stats_out.str();
}

TEST(CliE2E, CountAndStatsProjectionMayMatchOnlySomeTables) {
    DisjointTableFixture f;
    std::ostringstream count_out;
    std::ostringstream count_err;
    ASSERT_EQ(tsfile_cli::run_cli({"count", "-m", "S1", "-f", "csv", f.path},
                                  count_out, count_err),
              0)
        << count_err.str();
    EXPECT_NE(count_out.str().find("table,sensors_a,s1,FIELD"),
              std::string::npos)
        << count_out.str();
    EXPECT_EQ(count_out.str().find("table,sensors_b"), std::string::npos)
        << count_out.str();

    std::ostringstream stats_out;
    std::ostringstream stats_err;
    ASSERT_EQ(tsfile_cli::run_cli({"stats", "-m", "S1", "-f", "csv", f.path},
                                  stats_out, stats_err),
              0)
        << stats_err.str();
    EXPECT_NE(stats_out.str().find("table,sensors_a,sensors_a_tag,s1,INT64"),
              std::string::npos)
        << stats_out.str();
    EXPECT_EQ(stats_out.str().find("table,sensors_b"), std::string::npos)
        << stats_out.str();
}

TEST(CliE2E, CountAndStatsProjectionMayMatchOnlySomeTreeDevices) {
    DisjointTreeFixture f;
    std::ostringstream count_out;
    std::ostringstream count_err;
    ASSERT_EQ(tsfile_cli::run_cli({"count", "-m", "left", "-f", "csv", f.path},
                                  count_out, count_err),
              0)
        << count_err.str();
    EXPECT_NE(count_out.str().find("tree,root.test.d1,left,FIELD"),
              std::string::npos)
        << count_out.str();
    EXPECT_EQ(count_out.str().find("tree,root.test.d2"), std::string::npos)
        << count_out.str();

    std::ostringstream stats_out;
    std::ostringstream stats_err;
    ASSERT_EQ(tsfile_cli::run_cli({"stats", "-m", "left", "-f", "csv", f.path},
                                  stats_out, stats_err),
              0)
        << stats_err.str();
    EXPECT_NE(stats_out.str().find("tree,root.test.d1,left,INT32"),
              std::string::npos)
        << stats_out.str();
    EXPECT_EQ(stats_out.str().find("tree,root.test.d2"), std::string::npos)
        << stats_out.str();
}

TEST(CliE2E, TreeCountAndStatsUseDeviceTimestampUnion) {
    SparseTreeFixture f;
    std::ostringstream count_out;
    std::ostringstream count_err;
    ASSERT_EQ(tsfile_cli::run_cli({"count", "-f", "csv", f.path}, count_out,
                                  count_err),
              0)
        << count_err.str();
    EXPECT_NE(count_out.str().find(
                  "tree,root.test.d1,left,FIELD,3,\\N,2,1,0,2,statistics"),
              std::string::npos)
        << count_out.str();
    EXPECT_NE(count_out.str().find(
                  "tree,root.test.d1,right,FIELD,3,\\N,2,1,1,2,statistics"),
              std::string::npos)
        << count_out.str();

    std::ostringstream stats_out;
    std::ostringstream stats_err;
    ASSERT_EQ(tsfile_cli::run_cli({"stats", "-f", "csv", f.path}, stats_out,
                                  stats_err),
              0)
        << stats_err.str();
    EXPECT_NE(stats_out.str().find(
                  "tree,root.test.d1,left,INT32,2,1,0,2,10,20,10,20,30,"
                  "statistics"),
              std::string::npos)
        << stats_out.str();
    EXPECT_NE(stats_out.str().find(
                  "tree,root.test.d1,right,BOOLEAN,2,1,1,2,\\N,\\N,true,false,"
                  "1,statistics"),
              std::string::npos)
        << stats_out.str();
}

TEST(CliE2E, MetadataTableFilterIsCaseInsensitive) {
    Fixture f;

    std::ostringstream schema_out;
    std::ostringstream schema_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"schema", "-t", "TABLE1", "-f", "csv", f.path},
                            schema_out, schema_err),
        0);
    EXPECT_NE(schema_out.str().find("table,table1,s1,FIELD,INT64"),
              std::string::npos)
        << schema_out.str();

    std::ostringstream count_out;
    std::ostringstream count_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"count", "-t", "TABLE1", "-f", "csv", f.path},
                            count_out, count_err),
        0);
    EXPECT_NE(count_out.str().find("table,table1,s1,FIELD,5,1,5,0,0,4,scan"),
              std::string::npos)
        << count_out.str();

    std::ostringstream stats_out;
    std::ostringstream stats_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"stats", "-t", "TABLE1", "-f", "csv", f.path},
                            stats_out, stats_err),
        0);
    EXPECT_NE(stats_out.str().find("table,table1,id1_field_1,id2_field_2,s1,"
                                   "INT64,5,\\N,0,4,0,40,0,40,\\N,statistics"),
              std::string::npos)
        << stats_out.str();
}

TEST(CliE2E, WriteThenReadRoundTrip) {
    std::string csv_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_write_in", ".csv");
    {
        std::ofstream o(csv_path.c_str());
        o << "time,id1,s1\n0,dev,0\n1,dev,10\n2,dev,20\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_write_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--tag", "id1", "STRING", "--field", "s1",
         "INT64", "-i", csv_path, "-o", out_path},
        wout, werr);
    EXPECT_EQ(wc, 0) << werr.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    int cc =
        tsfile_cli::run_cli({"count", "-f", "csv", out_path}, cout_, cerr_);
    EXPECT_EQ(cc, 0);
    EXPECT_NE(cout_.str().find("table,t1,s1,FIELD,3,1,3,0,0,2,scan"),
              std::string::npos)
        << cout_.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    int rc = tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "csv", out_path},
                                 rout, rerr);
    EXPECT_EQ(rc, 0);
    EXPECT_EQ(rout.str(), "time,id1,s1\n0,dev,0\n1,dev,10\n2,dev,20\n");

    std::remove(csv_path.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteThenReadFloatDoubleRoundTripLossless) {
    std::string csv_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_fp_in", ".csv");
    {
        std::ofstream o(csv_path.c_str());
        o << "time,id1,f1,d1\n0,dev,0.1,0.1\n1,dev,3.4028235,"
             "3.141592653589793\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_fp_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--tag", "id1", "STRING", "--field", "f1",
         "FLOAT", "--field", "d1", "DOUBLE", "-i", csv_path, "-o", out_path},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    int rc = tsfile_cli::run_cli({"cat", "-f", "ndjson", out_path}, rout, rerr);
    ASSERT_EQ(rc, 0) << rerr.str();
    // Default ostream precision (6 sig digits) would print 0.1 / 3.40282 and
    // lose bits; max_digits10 keeps every digit needed to round-trip.
    EXPECT_NE(rout.str().find("0.100000001"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("3.40282345"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("3.1415926535897931"), std::string::npos)
        << rout.str();

    std::remove(csv_path.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteImportsQuotedFieldWithEmbeddedNewline) {
    std::string csv_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_nl_in", ".csv");
    {
        std::ofstream o(csv_path.c_str());
        // The note field on the first row spans two physical lines inside
        // quotes; it must import as a single row, not be split into two.
        o << "time,id1,note\n0,dev,\"line one\nline two\"\n1,dev,plain\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_nl_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--tag", "id1", "STRING", "--field", "note",
         "TEXT", "-i", csv_path, "-o", out_path},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    ASSERT_EQ(
        tsfile_cli::run_cli({"count", "-f", "csv", out_path}, cout_, cerr_), 0);
    EXPECT_NE(cout_.str().find("table,t1,note,FIELD,2,1,2,0,0,1,scan"),
              std::string::npos)
        << cout_.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(
        tsfile_cli::run_cli({"cat", "-f", "ndjson", out_path}, rout, rerr), 0);
    EXPECT_NE(rout.str().find("line one\\nline two"), std::string::npos)
        << rout.str();

    std::remove(csv_path.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteMissingColumnsIsUsageError) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t1", "-o", "x.tsfile", "in.csv"}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--field"), std::string::npos);
}

namespace {
bool path_exists(const std::string& p) {
    std::ifstream in(p.c_str());
    return in.good();
}
}  // namespace

TEST(CliE2E, WriteRejectsOutOfOrderTimestampsAndLeavesNoOutput) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_ooo", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n5,50\n1,10\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_ooo_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--field", "s1",
                                    "INT64", "-i", csv, "-o", out_path},
                                   out, err);
    EXPECT_EQ(code, 2);
    EXPECT_NE(err.str().find("strictly increasing"), std::string::npos)
        << err.str();
    EXPECT_NE(err.str().find("line 3"), std::string::npos) << err.str();
    EXPECT_FALSE(path_exists(out_path)) << "failed import must leave no output";

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteAllowsSameTimestampAcrossDevices) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_md", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,id,s1\n1,A,10\n1,B,20\n2,A,30\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_md_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t", "--tag", "id", "STRING", "--field", "s1",
         "INT64", "-i", csv, "-o", out_path},
        out, err);
    EXPECT_EQ(code, 0) << err.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    tsfile_cli::run_cli({"count", "-f", "csv", out_path}, cout_, cerr_);
    EXPECT_NE(cout_.str().find("table,t,s1,FIELD,3,2,3,0,1,2,scan"),
              std::string::npos)
        << cout_.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteRejectsOutputEqualsInput) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_alias", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0,1\n";
    }
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--field", "s1",
                                    "INT64", "-i", csv, "-o", csv},
                                   out, err);
    EXPECT_EQ(code, 3);
    EXPECT_NE(err.str().find("same as the input"), std::string::npos)
        << err.str();
    // The input file must be untouched.
    std::ifstream in(csv.c_str());
    std::stringstream buf;
    buf << in.rdbuf();
    EXPECT_EQ(buf.str(), "time,s1\n0,1\n");

    std::remove(csv.c_str());
}

TEST(CliE2E, WriteFailureOnBadValueLeavesNoOutput) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_badval", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0,notanumber\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_badval_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--field", "s1",
                                    "INT64", "-i", csv, "-o", out_path},
                                   out, err);
    EXPECT_EQ(code, 2);
    EXPECT_FALSE(path_exists(out_path));

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteRejectsDuplicateColumnNames) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t", "--field", "s1", "INT64", "--field", "s1",
         "INT64", "--stdin", "-o", "x.tsfile"},
        out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("duplicate column"), std::string::npos)
        << err.str();
}

TEST(CliE2E, WriteRejectsTagOnlySchema) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--tag", "id",
                                    "STRING", "--stdin", "-o", "x.tsfile"},
                                   out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--field"), std::string::npos) << err.str();
}

TEST(CliE2E, WriteRejectsNonStringTag) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t", "--tag", "id", "INT64", "--field", "s1",
         "INT64", "--stdin", "-o", "x.tsfile"},
        out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("must use STRING"), std::string::npos)
        << err.str();
}

TEST(CliE2E, WriteNormalizesNamesAndHeaderCase) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_case", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,ID,S1\n0,a,1\n";
    }
    std::string output =
        tsfile_cli_test::unique_temp_path("tsfile_cli_case_out", ".tsfile");
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"write", "--table", "Mixed", "--tag", "Id", "STRING",
                   "--field", "s1", "INT64", "-i", csv, "-o", output},
                  out, err),
              0)
        << err.str();
    std::ostringstream schema_out;
    std::ostringstream schema_err;
    EXPECT_EQ(tsfile_cli::run_cli({"schema", "-f", "csv", output}, schema_out,
                                  schema_err),
              0);
    EXPECT_NE(schema_out.str().find("table,mixed,id,TAG,STRING"),
              std::string::npos);
    std::remove(csv.c_str());
    std::remove(output.c_str());
}

TEST(CliE2E, WriteRejectsHeaderMatchWithNoHeader) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t", "--field", "s1", "INT64", "-o", "x.tsfile",
         "--stdin", "--no-header", "--header-match"},
        out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--header-match"), std::string::npos) << err.str();
}

TEST(CliE2E, ReadRejectsWriteOnlyFlag) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-o", "x.tsfile", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("only valid for write"), std::string::npos)
        << err.str();
}

TEST(CliE2E, ReadRejectsWriteColumnFlags) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"ls", "--field", "s1", "INT64", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--tag/--field are only valid for write"),
              std::string::npos)
        << err.str();
}

TEST(CliE2E, MetaRejectsDeviceScopeFlag) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"meta", "-d", "dev", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("not valid for meta"), std::string::npos)
        << err.str();
}

TEST(CliE2E, SchemaTableShowsEncodingAndCompression) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"schema", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    // Table-model schema must report the fixture's configured encoding and
    // compression rather than blanks.
    EXPECT_NE(out.str().find(",s1,FIELD,INT64,PLAIN,UNCOMPRESSED\n"),
              std::string::npos)
        << out.str();
}

namespace {
// Run a one-row `write` whose single value cell is `value`, declaring the
// column as `type`. Returns the exit code; captures stderr into `err`.
int write_one_value(const std::string& type, const std::string& value,
                    std::string& err_out) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_ovf", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0," << value << "\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_ovf_out", ".tsfile");
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--field", "s1",
                                    type, "-i", csv, "-o", out_path},
                                   out, err);
    err_out = err.str();
    std::remove(csv.c_str());
    std::remove(out_path.c_str());
    return code;
}
}  // namespace

TEST(CliE2E, WriteRejectsInt32Overflow) {
    std::string err;
    EXPECT_EQ(write_one_value("INT32", "3000000000", err), 2);
    EXPECT_NE(err.find("INT32 out of range"), std::string::npos) << err;
}

TEST(CliE2E, WriteAcceptsInt32Boundary) {
    std::string err;
    EXPECT_EQ(write_one_value("INT32", "2147483647", err), 0) << err;
}

TEST(CliE2E, WriteRejectsInt64Overflow) {
    std::string err;
    EXPECT_EQ(write_one_value("INT64", "99999999999999999999999999", err), 2);
    EXPECT_NE(err.find("INT64 out of range"), std::string::npos) << err;
}

TEST(CliE2E, WriteRejectsDoubleOverflow) {
    std::string err;
    EXPECT_EQ(write_one_value("DOUBLE", "1e400", err), 2);
    EXPECT_NE(err.find("DOUBLE out of range"), std::string::npos) << err;
}

TEST(CliE2E, WriteRejectsNonNumericInt64) {
    std::string err;
    EXPECT_EQ(write_one_value("INT64", "12abc", err), 2);
    EXPECT_NE(err.find("bad INT64"), std::string::npos) << err;
}

TEST(CliE2E, WriteDateRequiresStrictIsoLexicalForm) {
    std::string err;
    EXPECT_EQ(write_one_value("DATE", "2024-1-1", err), 2);
    EXPECT_NE(err.find("want YYYY-MM-DD"), std::string::npos) << err;
    EXPECT_EQ(write_one_value("DATE", "2024-02-30", err), 2);
    EXPECT_EQ(write_one_value("DATE", "2024-02-29", err), 0) << err;
}

TEST(CliE2E, WriteAcceptsOneLeadingUtf8Bom) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_bom", ".csv");
    {
        std::ofstream o(csv.c_str(), std::ios::binary);
        o << "\xEF\xBB\xBFtime,s1\n0,1\n";
    }
    std::string output =
        tsfile_cli_test::unique_temp_path("tsfile_cli_bom_out", ".tsfile");
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "t", "--field", "s1",
                                   "INT64", "-i", csv, "-o", output},
                                  out, err),
              0)
        << err.str();
    std::remove(csv.c_str());
    std::remove(output.c_str());
}

TEST(CliE2E, WriteRejectsInvalidUtf8AndMisplacedBomWithoutOutput) {
    const std::string invalid_utf8 =
        std::string("time,s1\n0,") + static_cast<char>(0xC3) + "(\n";
    const std::string misplaced_bom =
        "time,s1\n0,\xEF\xBB\xBF"
        "1\n";
    const std::string inputs[] = {invalid_utf8, misplaced_bom};

    for (size_t i = 0; i < 2; ++i) {
        std::string csv =
            tsfile_cli_test::unique_temp_path("tsfile_cli_bad_utf8", ".csv");
        std::string output = tsfile_cli_test::unique_temp_path(
            "tsfile_cli_bad_utf8_out", ".tsfile");
        {
            std::ofstream file(csv.c_str(), std::ios::binary);
            file.write(inputs[i].data(),
                       static_cast<std::streamsize>(inputs[i].size()));
        }
        std::ostringstream out;
        std::ostringstream err;
        EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "t", "--field", "s1",
                                       "INT64", "-i", csv, "-o", output},
                                      out, err),
                  2)
            << err.str();
        std::ifstream target(output.c_str(), std::ios::binary);
        EXPECT_FALSE(target.good());
        std::remove(csv.c_str());
        std::remove(output.c_str());
    }
}

TEST(CliE2E, WriteRejectsReservedAndControlCharacterNames) {
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "t", "--field", "time",
                                   "INT64", "--stdin", "-o", "unused.tsfile"},
                                  out, err),
              1);
    EXPECT_NE(err.str().find("reserved"), std::string::npos) << err.str();

    out.str("");
    out.clear();
    err.str("");
    err.clear();
    EXPECT_EQ(
        tsfile_cli::run_cli({"write", "--table", "bad\nname", "--field", "s1",
                             "INT64", "--stdin", "-o", "unused.tsfile"},
                            out, err),
        1);
    EXPECT_NE(err.str().find("control"), std::string::npos) << err.str();
}

TEST(CliE2E, WriteFailurePreservesExistingTarget) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_existing", ".csv");
    std::string output =
        tsfile_cli_test::unique_temp_path("tsfile_cli_existing_out", ".tsfile");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0,1\n";
    }
    {
        std::ofstream o(output.c_str());
        o << "sentinel";
    }
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "t", "--field", "s1",
                                   "INT64", "-i", csv, "-o", output},
                                  out, err),
              3);
    std::ifstream existing(output.c_str());
    std::stringstream content;
    content << existing.rdbuf();
    EXPECT_EQ(content.str(), "sentinel");
    std::remove(csv.c_str());
    std::remove(output.c_str());
}

TEST(CliE2E, WriteRejectsOutOfOrderAcrossBatches) {
    // More than one 1024-row batch of ascending rows, then a violating
    // timestamp. The first batch is already flushed by the time the bad row is
    // read, so this proves both that per-device tracking survives a batch flush
    // and that the already-written output is removed on failure.
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_xbatch", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n";
        for (int i = 1; i <= 1100; ++i) {
            o << i << "," << i << "\n";
        }
        o << "500,999\n";  // <= the last timestamp for the tag-less device
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_xbatch_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--field", "s1",
                                    "INT64", "-i", csv, "-o", out_path},
                                   out, err);
    EXPECT_EQ(code, 2);
    EXPECT_NE(err.str().find("strictly increasing"), std::string::npos)
        << err.str();
    EXPECT_FALSE(path_exists(out_path));

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteStreamsLargeInputRoundTrips) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_large", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n";
        for (int i = 1; i <= 3000; ++i) {
            o << i << "," << (i * 2) << "\n";
        }
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_large_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "big", "--field", "s1",
                                    "INT64", "-i", csv, "-o", out_path},
                                   out, err);
    EXPECT_EQ(code, 0) << err.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    tsfile_cli::run_cli({"count", "-f", "csv", out_path}, cout_, cerr_);
    EXPECT_NE(cout_.str().find("table,big,s1,FIELD,3000,1,3000,0,1,3000,scan"),
              std::string::npos)
        << cout_.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, HelpWithPositionalFileIsUsageError) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "--help", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("--help"), std::string::npos) << err.str();
}

TEST(CliE2E, StatsRejectsRowOnlyFlag) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"stats", "--start", "1", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("only valid for head/cat"), std::string::npos)
        << err.str();
}

TEST(CliE2E, LsRejectsMeasurementsFlag) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-m", "s1", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("not valid for ls"), std::string::npos)
        << err.str();
}

TEST(CliE2E, WriteRoundTripsTimestampDateBlob) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_tdb", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,id1,ts1,d1,b1\n"
             "0,dev,1700000000000,2024-01-15,hello\n"
             "1,dev,1700000000001,2024-12-31,world\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_tdb_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--tag", "id1", "STRING", "--field", "ts1",
         "TIMESTAMP", "--field", "d1", "DATE", "--field", "b1", "BLOB", "-o",
         out_path, "-i", csv},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-f", "csv", out_path}, rout, rerr),
              0)
        << rerr.str();
    // TIMESTAMP stays a decimal string, DATE uses YYYY-MM-DD, and BLOB uses
    // the external 0x-prefixed lowercase hex lexeme.
    EXPECT_NE(rout.str().find("1700000000000"), std::string::npos)
        << rout.str();
    EXPECT_NE(rout.str().find("2024-01-15"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("2024-12-31"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("0x68656c6c6f"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("0x776f726c64"), std::string::npos) << rout.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteRejectsBadDate) {
    std::string err;
    EXPECT_EQ(write_one_value("DATE", "not-a-date", err), 2);
    EXPECT_NE(err.find("bad DATE"), std::string::npos) << err;
}

TEST(CliE2E, WriteVerboseEchoesConfig) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_vb", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0,1\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_vb_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "vt", "--field", "s1",
                                    "INT64", "-v", "-i", csv, "-o", out_path},
                                   out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_NE(err.str().find("created model=table object=vt rows=1 output="),
              std::string::npos)
        << err.str();
    EXPECT_NE(err.str().find("column=s1 category=FIELD data_type=INT64"),
              std::string::npos)
        << err.str();
    EXPECT_NE(err.str().find("source=default"), std::string::npos) << err.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteRejectsHeaderMatch) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_hm", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,wrong\n0,1\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_hm_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"write", "--table", "t", "--field", "s1", "INT64",
                             "--header-match", "-i", csv, "-o", out_path},
                            out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--header-match"), std::string::npos) << err.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

// Every column carries a distinct, type-specific value, so the JSON output pins
// each column name to exactly one value. This is what guards the by-index
// add_value mapping in cmd_write: if any two columns were written to the wrong
// slot, a key/value pair below would mismatch.
TEST(CliE2E, WriteMapsEachColumnToItsOwnValue) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_colmap", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,a_bool,b_int,c_long,d_float,e_double,f_str,g_ts,h_date\n"
             "10,true,42,9000000000,1.5,3.25,hello,1700000000000,2024-06-15\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_colmap_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write",   "--table", "t1",     "--field", "a_bool",   "BOOLEAN",
         "--field", "b_int",   "INT32",  "--field", "c_long",   "INT64",
         "--field", "d_float", "FLOAT",  "--field", "e_double", "DOUBLE",
         "--field", "f_str",   "STRING", "--field", "g_ts",     "TIMESTAMP",
         "--field", "h_date",  "DATE",   "-i",      csv,        "-o",
         out_path},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(
        tsfile_cli::run_cli({"cat", "-f", "ndjson", out_path}, rout, rerr), 0)
        << rerr.str();
    const std::string& j = rout.str();
    EXPECT_NE(j.find("\"a_bool\":true"), std::string::npos) << j;
    EXPECT_NE(j.find("\"b_int\":42"), std::string::npos) << j;
    EXPECT_NE(j.find("\"c_long\":\"9000000000\""), std::string::npos) << j;
    EXPECT_NE(j.find("\"d_float\":1.5"), std::string::npos) << j;
    EXPECT_NE(j.find("\"e_double\":3.25"), std::string::npos) << j;
    EXPECT_NE(j.find("\"f_str\":\"hello\""), std::string::npos) << j;
    EXPECT_NE(j.find("\"g_ts\":\"1700000000000\""), std::string::npos) << j;
    EXPECT_NE(j.find("\"h_date\":\"2024-06-15\""), std::string::npos) << j;

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

// A multi-type import spanning several batches must keep every value in its own
// column after each flush. Tags vary so timestamps may repeat across devices.
TEST(CliE2E, WriteMultiTypeAcrossBatchesRoundTrips) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_mtb", ".csv");
    const int kRows = 2500;  // > 2 batches of 1024
    {
        std::ofstream o(csv.c_str());
        o << "time,id,n,note\n";
        for (int i = 0; i < kRows; ++i) {
            o << i << ",dev," << (i * 3) << ",row" << i << "\n";
        }
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_mtb_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t", "--tag", "id", "STRING", "--field", "n",
         "INT64", "--field", "note", "TEXT", "-i", csv, "-o", out_path},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    ASSERT_EQ(
        tsfile_cli::run_cli({"count", "-f", "csv", out_path}, cout_, cerr_), 0);
    EXPECT_NE(cout_.str().find("table,t,n,FIELD,2500,1,2500,0,0,2499,scan"),
              std::string::npos)
        << cout_.str();

    // Spot-check a row from the last batch keeps n and note paired correctly.
    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "--start", "2400", "--end", "2400",
                                   "-f", "ndjson", out_path},
                                  rout, rerr),
              0)
        << rerr.str();
    EXPECT_NE(rout.str().find("\"n\":\"7200\""), std::string::npos)
        << rout.str();
    EXPECT_NE(rout.str().find("\"note\":\"row2400\""), std::string::npos)
        << rout.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

// A quoted STRING field containing the delimiter and escaped quotes must
// survive import and re-export unchanged (RFC 4180 round-trip through the
// writer).
TEST(CliE2E, WriteRoundTripsQuotedSpecialChars) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_special", ".csv");
    {
        std::ofstream o(csv.c_str());
        // note = a,b "q" c  (comma + embedded quotes)
        o << "time,id,note\n0,dev,\"a,b \"\"q\"\" c\"\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_special_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t", "--tag", "id", "STRING", "--field", "note",
         "STRING", "-i", csv, "-o", out_path},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    // JSON escapes the embedded quotes; the comma is preserved verbatim.
    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(
        tsfile_cli::run_cli({"cat", "-f", "ndjson", out_path}, rout, rerr), 0)
        << rerr.str();
    EXPECT_NE(rout.str().find("\"note\":\"a,b \\\"q\\\" c\""),
              std::string::npos)
        << rout.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteRejectsTimestampOverflow) {
    std::string err;
    EXPECT_EQ(write_one_value("TIMESTAMP", "99999999999999999999999999", err),
              2);
    EXPECT_NE(err.find("TIMESTAMP out of range"), std::string::npos) << err;
}

TEST(CliE2E, WriteRejectsNonNumericTimestampColumn) {
    std::string err;
    EXPECT_EQ(write_one_value("TIMESTAMP", "not-a-number", err), 2);
    EXPECT_NE(err.find("bad TIMESTAMP"), std::string::npos) << err;
}

TEST(CliE2E, WriteRejectsImpossibleDate) {
    // Syntactically YYYY-MM-DD but not a real calendar date.
    std::string err;
    EXPECT_EQ(write_one_value("DATE", "2024-13-40", err), 2);
    EXPECT_NE(err.find("bad DATE"), std::string::npos) << err;
}

TEST(CliE2E, WriteAcceptsDateBoundary) {
    std::string err;
    EXPECT_EQ(write_one_value("DATE", "2024-02-29", err), 0)
        << err;  // leap day
}

// CSV nulls use unquoted \N; quoted empty strings stay distinct from null.
TEST(CliE2E, WriteDistinguishesCsvNullAndEmptyString) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_null", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,id,n\n0,dev,\\N\n1,\"\",7\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_null_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t", "--tag", "id", "STRING", "--field", "n",
         "INT64", "-i", csv, "-o", out_path},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(
        tsfile_cli::run_cli({"cat", "-f", "ndjson", out_path}, rout, rerr), 0)
        << rerr.str();
    EXPECT_NE(rout.str().find("\"n\":null"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("\"id\":\"\""), std::string::npos) << rout.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}
