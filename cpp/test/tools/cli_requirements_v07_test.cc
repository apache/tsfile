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
#ifndef _WIN32
#include <unistd.h>
#endif

#include "cli/run_cli.h"
#include "cli_test_util.h"

namespace {

struct TableFixture {
    std::string path = tsfile_cli_test::write_table_fixture();
    ~TableFixture() { std::remove(path.c_str()); }
};

struct MultiTableFixture {
    std::string path = tsfile_cli_test::write_multi_table_fixture();
    ~MultiTableFixture() { std::remove(path.c_str()); }
};

bool file_exists(const std::string& path) {
    std::ifstream in(path.c_str());
    return in.good();
}

std::string read_file(const std::string& path) {
    std::ifstream in(path.c_str());
    std::ostringstream buf;
    buf << in.rdbuf();
    return buf.str();
}

}  // namespace

TEST(CliRequirementsV07, HelpListsExactlyCurrentCommandSurface) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"--help"}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find("ls schema meta stats count sketch head cat export write"),
              std::string::npos)
        << out.str();
    EXPECT_EQ(out.str().find("sample"), std::string::npos) << out.str();
    EXPECT_TRUE(err.str().empty());
}

TEST(CliRequirementsV07, SampleIsNotACommand) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"sample", "x.tsfile"}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("Unknown command"), std::string::npos) << err.str();
}

TEST(CliRequirementsV07, FormatVocabularyIsTableNdjsonCsvOnly) {
    TableFixture f;

    std::ostringstream ndjson_out;
    std::ostringstream ndjson_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "0",
                                   "--end", "0", "-f", "ndjson", f.path},
                                  ndjson_out, ndjson_err),
              0)
        << ndjson_err.str();
    EXPECT_EQ(ndjson_out.str(), "{\"time\":0,\"s1\":0}\n");

    std::ostringstream json_out;
    std::ostringstream json_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-f", "json", f.path}, json_out,
                                  json_err),
              1);
    EXPECT_TRUE(json_out.str().empty());

    std::ostringstream tsv_out;
    std::ostringstream tsv_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-f", "tsv", f.path}, tsv_out,
                                  tsv_err),
              1);
    EXPECT_TRUE(tsv_out.str().empty());
}

TEST(CliRequirementsV07, DuplicateSingletonOptionsAreUsageErrors) {
    TableFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"cat", "-f", "csv", "-f", "ndjson", f.path}, out,
                            err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("--format specified more than once"),
              std::string::npos)
        << err.str();
}

TEST(CliRequirementsV07, MeasurementOptionRepeatsAndRejectsCommaLists) {
    TableFixture f;
    std::ostringstream comma_out;
    std::ostringstream comma_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1,s2", f.path}, comma_out,
                                  comma_err),
              1);

    std::ostringstream dup_out;
    std::ostringstream dup_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "-m", "s1", f.path},
                                  dup_out, dup_err),
              1);
    EXPECT_NE(dup_err.str().find("specified more than once"), std::string::npos)
        << dup_err.str();
}

TEST(CliRequirementsV07, MetaOnlyReturnsSizeFormatVersionAndModel) {
    TableFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"meta", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_TRUE(err.str().empty());
    EXPECT_EQ(out.str().substr(0, std::string("size_bytes,format_version,model\n").size()),
              "size_bytes,format_version,model\n")
        << out.str();
    EXPECT_EQ(out.str().find("path"), std::string::npos) << out.str();
    EXPECT_EQ(out.str().find("device_count"), std::string::npos) << out.str();
}

TEST(CliRequirementsV07, LsReturnsModelAndObjectFields) {
    TableFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_EQ(out.str(), "model,object\ntable,table1\n");
}

TEST(CliRequirementsV07, WriteUsesExplicitTagAndFieldOptions) {
    std::string csv = tsfile_cli_test::unique_temp_path("tsfile_cli_v07_in", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,id1,s1\n0,dev,0\n1,dev,10\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_v07_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli({"write", "--table", "t1", "--tag", "id1",
                                  "STRING", "--field", "s1", "INT64", "-i",
                                  csv, "-o", out_path},
                                 wout, werr);
    EXPECT_EQ(wc, 0) << werr.str();
    EXPECT_TRUE(file_exists(out_path));

    std::ostringstream rout;
    std::ostringstream rerr;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "csv", out_path},
                                  rout, rerr),
              0)
        << rerr.str();
    EXPECT_EQ(rout.str(), "time,s1\n0,0\n1,10\n");

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliRequirementsV07, LegacyColumnsOptionIsRejected) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t1", "--columns",
                                    "s1:INT64:field", "-o", "x.tsfile",
                                    "--stdin"},
                                   out, err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("Unknown flag: --columns"), std::string::npos)
        << err.str();
}

TEST(CliRequirementsV07, WriteRejectsImplicitInputAndFormatFlag) {
    std::ostringstream implicit_out;
    std::ostringstream implicit_err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "t1", "--field", "s1",
                                   "INT64", "-o", "x.tsfile", "in.csv"},
                                  implicit_out, implicit_err),
              1);
    EXPECT_NE(implicit_err.str().find("choose exactly one of --input or --stdin"),
              std::string::npos)
        << implicit_err.str();

    std::ostringstream format_out;
    std::ostringstream format_err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "t1", "--field", "s1",
                                   "INT64", "-f", "csv", "--stdin", "-o",
                                   "x.tsfile"},
                                  format_out, format_err),
              1);
    EXPECT_NE(format_err.str().find("--format is not valid"),
              std::string::npos)
        << format_err.str();
}

TEST(CliRequirementsV07, ExportWritesSingleObjectAtomically) {
    TableFixture f;
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_export", ".csv");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"export", "-t", "table1", "-o", out_path,
                                    "--type", "csv", "-m", "s1", f.path},
                                   out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_TRUE(out.str().empty());
    EXPECT_EQ(read_file(out_path), "time,s1\n0,0\n1,10\n2,20\n3,30\n4,40\n");

    std::remove(out_path.c_str());
}

TEST(CliRequirementsV07, ExportWritesMultiObjectManifestAndNumberedFiles) {
    MultiTableFixture f;
    std::string dir =
        tsfile_cli_test::unique_temp_path("tsfile_cli_multi_export", "");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"export", "-t", "sensors_a", "-t", "sensors_b", "--output-dir", dir,
         "--type", "csv", "-m", "s1", f.path},
        out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_TRUE(out.str().empty());
    EXPECT_EQ(read_file(dir + "/0001.csv"), "time,s1\n0,10\n");
    EXPECT_EQ(read_file(dir + "/0002.csv"), "time,s1\n0,20\n");
    std::string manifest = read_file(dir + "/_manifest.json");
    EXPECT_NE(manifest.find("\"complete\": true"), std::string::npos)
        << manifest;
    EXPECT_NE(manifest.find("\"file\":\"0001.csv\""), std::string::npos)
        << manifest;
    EXPECT_NE(manifest.find("\"object\":\"sensors_b\""), std::string::npos)
        << manifest;
    EXPECT_NE(manifest.find("\"rows\":\"1\""), std::string::npos)
        << manifest;

    std::remove((dir + "/0001.csv").c_str());
    std::remove((dir + "/0002.csv").c_str());
    std::remove((dir + "/_manifest.json").c_str());
#ifndef _WIN32
    rmdir(dir.c_str());
#endif
}

TEST(CliRequirementsV07, MultipleTagFiltersRequireAndHonorTagMatch) {
    std::string path = tsfile_cli_test::write_tag_filter_fixture();
    std::ostringstream missing_out;
    std::ostringstream missing_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1",
                                   "eq", "dev_a", "--tag-filter", "id1",
                                   "eq", "dev_c", "-f", "csv", path},
                                  missing_out, missing_err),
              1);

    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1",
                                   "eq", "dev_a", "--tag-filter", "id1",
                                   "eq", "dev_c", "--tag-match", "any", "-f",
                                   "csv", path},
                                  out, err),
              0)
        << err.str();
    EXPECT_EQ(out.str(), "time,s1\n0,10\n3,40\n");
    std::remove(path.c_str());
}

TEST(CliRequirementsV07, SketchRejectsRegularResultFormat) {
    TableFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"sketch", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("sketch does not accept --format"),
              std::string::npos)
        << err.str();
}
