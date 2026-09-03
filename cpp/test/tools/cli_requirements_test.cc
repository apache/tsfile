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
#include <sys/stat.h>
#include <unistd.h>
#endif

#include "cli/run_cli.h"
#include "cli_test_util.h"
#include "reader/tsfile_reader.h"

#ifndef TSFILE_CPP_SOURCE_DIR
#define TSFILE_CPP_SOURCE_DIR "."
#endif

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
    std::ifstream in(path.c_str(), std::ios::binary);
    std::ostringstream buf;
    buf << in.rdbuf();
    return buf.str();
}

}  // namespace

TEST(CliRequirements, HelpListsExactlyCurrentCommandSurface) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"--help"}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find(
                  "ls schema meta stats count sketch head cat export write"),
              std::string::npos)
        << out.str();
    EXPECT_EQ(out.str().find("sample"), std::string::npos) << out.str();
    EXPECT_TRUE(err.str().empty());
}

TEST(CliRequirements, CommandHelpIsSpecificAndDocumentsSyntaxFieldsExamples) {
    const std::vector<std::string> commands = {
        "ls",     "schema", "meta", "stats",  "count",
        "sketch", "head",   "cat",  "export", "write"};
    for (const auto& command : commands) {
        std::ostringstream out;
        std::ostringstream err;
        EXPECT_EQ(tsfile_cli::run_cli({command, "--help"}, out, err), 0)
            << command;
        EXPECT_TRUE(err.str().empty()) << command << ": " << err.str();
        EXPECT_NE(out.str().find("Usage: tsfile-cli " + command),
                  std::string::npos)
            << command << ": " << out.str();
        EXPECT_NE(out.str().find("Result fields:"), std::string::npos)
            << command << ": " << out.str();
        EXPECT_NE(out.str().find("Examples:"), std::string::npos)
            << command << ": " << out.str();
        EXPECT_EQ(out.str().find("Commands:"), std::string::npos)
            << command << ": " << out.str();
    }

    std::ostringstream meta_out;
    std::ostringstream meta_err;
    EXPECT_EQ(tsfile_cli::run_cli({"meta", "--help"}, meta_out, meta_err), 0);
    EXPECT_TRUE(meta_err.str().empty());
    EXPECT_NE(meta_out.str().find("Usage: tsfile-cli meta"), std::string::npos)
        << meta_out.str();
    EXPECT_NE(meta_out.str().find("Result fields:"), std::string::npos)
        << meta_out.str();
    EXPECT_NE(meta_out.str().find("size_bytes,format_version,model"),
              std::string::npos)
        << meta_out.str();
    EXPECT_NE(meta_out.str().find("Examples:"), std::string::npos)
        << meta_out.str();
    EXPECT_EQ(meta_out.str().find("Commands:"), std::string::npos)
        << meta_out.str();

    std::ostringstream write_out;
    std::ostringstream write_err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "-h"}, write_out, write_err), 0);
    EXPECT_TRUE(write_err.str().empty());
    EXPECT_NE(write_out.str().find("Usage: tsfile-cli write"),
              std::string::npos)
        << write_out.str();
    EXPECT_NE(write_out.str().find("--field <name> <type>"), std::string::npos)
        << write_out.str();
    EXPECT_NE(write_out.str().find("Default: success is silent"),
              std::string::npos)
        << write_out.str();
    EXPECT_NE(write_out.str().find("Examples:"), std::string::npos)
        << write_out.str();
    EXPECT_EQ(write_out.str().find("Commands:"), std::string::npos)
        << write_out.str();
}

TEST(CliRequirements, SkillShipsRequiredReferenceFiles) {
    const std::string root = std::string(TSFILE_CPP_SOURCE_DIR) +
                             "/tools/skills/tsfile-cli/references/";
    const std::vector<std::string> refs = {"commands.md", "errors.md",
                                           "examples.md"};
    for (const auto& ref : refs) {
        std::ifstream in(root + ref);
        ASSERT_TRUE(in.good()) << ref;
        std::ostringstream body;
        body << in.rdbuf();
        EXPECT_NE(body.str().find("tsfile-cli"), std::string::npos)
            << root + ref;
    }
}

TEST(CliRequirements, SampleIsNotACommand) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"sample", "x.tsfile"}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("Unknown command"), std::string::npos)
        << err.str();
}

TEST(CliRequirements, VersionIncludesConcreteBuildMetadata) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"--version"}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("tsfile-cli "), std::string::npos) << out.str();
    EXPECT_NE(out.str().find(" tsfile="), std::string::npos) << out.str();
    EXPECT_NE(out.str().find(" commit="), std::string::npos) << out.str();
    EXPECT_NE(out.str().find(" built="), std::string::npos) << out.str();
    EXPECT_EQ(out.str().find("unknown"), std::string::npos) << out.str();
}

TEST(CliRequirements, FormatVocabularyIsTableNdjsonCsvOnly) {
    TableFixture f;

    std::ostringstream ndjson_out;
    std::ostringstream ndjson_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "--start", "0", "--end",
                                   "0", "-f", "ndjson", f.path},
                                  ndjson_out, ndjson_err),
              0)
        << ndjson_err.str();
    EXPECT_EQ(ndjson_out.str(),
              "{\"time\":\"0\",\"id1\":\"id1_field_1\",\"id2\":"
              "\"id2_field_2\",\"s1\":\"0\"}\n");

    std::ostringstream json_out;
    std::ostringstream json_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"cat", "-f", "json", f.path}, json_out, json_err),
        1);
    EXPECT_TRUE(json_out.str().empty());

    std::ostringstream tsv_out;
    std::ostringstream tsv_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"cat", "-f", "tsv", f.path}, tsv_out, tsv_err), 1);
    EXPECT_TRUE(tsv_out.str().empty());
}

TEST(CliRequirements, DuplicateSingletonOptionsAreUsageErrors) {
    TableFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "-f", "csv", "-f", "ndjson", f.path},
                                   out, err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("--format specified more than once"),
              std::string::npos)
        << err.str();

    const std::vector<std::vector<std::string>> duplicate_singletons = {
        {"cat", "-n", "1", "--limit", "2", f.path},
        {"cat", "--offset", "0", "--offset", "1", f.path},
        {"cat", "--start", "0", "--start", "1", f.path},
        {"cat", "--end", "1", "--end", "2", f.path},
        {"export", "-t", "table1", "--type", "csv", "-o", "a.csv", "--output",
         "b.csv", f.path},
        {"export", "-t", "table1", "--type", "csv", "--output-dir", "a",
         "--output-dir", "b", f.path},
        {"export", "-t", "table1", "--type", "csv", "-o", "a.csv", "--force",
         "--force", f.path},
    };
    for (const std::vector<std::string>& args : duplicate_singletons) {
        std::ostringstream dup_out;
        std::ostringstream dup_err;
        EXPECT_EQ(tsfile_cli::run_cli(args, dup_out, dup_err), 1)
            << dup_err.str();
        EXPECT_TRUE(dup_out.str().empty());
        EXPECT_NE(dup_err.str().find("specified more than once"),
                  std::string::npos)
            << dup_err.str();
    }
}

TEST(CliRequirements, PositionalFileMustBeFinalUnlessAfterDoubleDash) {
    TableFixture f;

    std::ostringstream bad_out;
    std::ostringstream bad_err;
    int bad_code =
        tsfile_cli::run_cli({"cat", f.path, "-f", "csv"}, bad_out, bad_err);
    EXPECT_EQ(bad_code, 1);
    EXPECT_TRUE(bad_out.str().empty());
    EXPECT_NE(bad_err.str().find("Unexpected argument after file"),
              std::string::npos)
        << bad_err.str();

    std::ostringstream ok_out;
    std::ostringstream ok_err;
    int ok_code =
        tsfile_cli::run_cli({"cat", "-m", "s1", "--", f.path}, ok_out, ok_err);
    EXPECT_EQ(ok_code, 0) << ok_err.str();
    EXPECT_TRUE(ok_err.str().empty());
}

TEST(CliRequirements, MeasurementOptionRepeatsAndRejectsCommaLists) {
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

TEST(CliRequirements, MetaOnlyReturnsSizeFormatVersionAndModel) {
    TableFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"meta", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_TRUE(err.str().empty());
    EXPECT_EQ(out.str().substr(
                  0, std::string("size_bytes,format_version,model\n").size()),
              "size_bytes,format_version,model\n")
        << out.str();
    EXPECT_EQ(out.str().find("path"), std::string::npos) << out.str();
    EXPECT_EQ(out.str().find("device_count"), std::string::npos) << out.str();
}

TEST(CliRequirements, LsReturnsModelAndObjectFields) {
    TableFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_EQ(out.str(), "model,object\ntable,table1\n");
}

TEST(CliRequirements, SchemaReturnsFixedSevenFieldContract) {
    TableFixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"schema", "-f", "csv", f.path}, out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_EQ(out.str().substr(
                  0, std::string("model,object,column,category,data_type,"
                                 "encoding,compression\n")
                         .size()),
              "model,object,column,category,data_type,encoding,compression\n")
        << out.str();
    EXPECT_NE(out.str().find("table,table1,id1,TAG,STRING"), std::string::npos)
        << out.str();
    EXPECT_NE(out.str().find("table,table1,s1,FIELD,INT64"), std::string::npos)
        << out.str();
}

TEST(CliRequirements, HeadCatAndExportRejectOffsetWithZeroLimit) {
    TableFixture f;
    std::string out_path = tsfile_cli_test::unique_temp_path(
        "tsfile_cli_zero_limit_export", ".csv");

    std::ostringstream head_out;
    std::ostringstream head_err;
    EXPECT_EQ(tsfile_cli::run_cli({"head", "-n", "0", "--offset", "1", f.path},
                                  head_out, head_err),
              1);
    EXPECT_TRUE(head_out.str().empty());

    std::ostringstream cat_out;
    std::ostringstream cat_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-n", "0", "--offset", "1", f.path},
                                  cat_out, cat_err),
              1);
    EXPECT_TRUE(cat_out.str().empty());

    std::ostringstream export_out;
    std::ostringstream export_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"export", "-t", "table1", "--type", "csv", "-o",
                             out_path, "-n", "0", "--offset", "1", f.path},
                            export_out, export_err),
        1);
    EXPECT_TRUE(export_out.str().empty());
    EXPECT_FALSE(file_exists(out_path));
    std::remove(out_path.c_str());
}

TEST(CliRequirements, HeadAndCatRequireScopeForMultiObjectFiles) {
    MultiTableFixture f;

    std::ostringstream cat_out;
    std::ostringstream cat_err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "csv", f.path},
                                  cat_out, cat_err),
              1);
    EXPECT_TRUE(cat_out.str().empty());
    EXPECT_NE(cat_err.str().find("requires -t/--table"), std::string::npos)
        << cat_err.str();

    std::ostringstream head_out;
    std::ostringstream head_err;
    EXPECT_EQ(tsfile_cli::run_cli({"head", "-m", "s1", "-f", "csv", f.path},
                                  head_out, head_err),
              1);
    EXPECT_TRUE(head_out.str().empty());
    EXPECT_NE(head_err.str().find("requires -t/--table"), std::string::npos)
        << head_err.str();
}

TEST(CliRequirements, WriteUsesExplicitTagAndFieldOptions) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_req_in", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,id1,s1\n0,dev,0\n1,dev,10\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_req_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--tag", "id1", "STRING", "--field", "s1",
         "INT64", "-i", csv, "-o", out_path},
        wout, werr);
    EXPECT_EQ(wc, 0) << werr.str();
    EXPECT_TRUE(file_exists(out_path));

    std::ostringstream rout;
    std::ostringstream rerr;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "csv", out_path},
                                  rout, rerr),
              0)
        << rerr.str();
    EXPECT_EQ(rout.str(), "time,id1,s1\n0,dev,0\n1,dev,10\n");

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliRequirements, WriteMapsInputByHeaderName) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_header_order", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "temp,time,site\n21.5,1000,beijing\n22.0,2000,shanghai\n";
    }
    std::string out_path = tsfile_cli_test::unique_temp_path(
        "tsfile_cli_header_order_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "sensors", "--tag", "site", "STRING", "--field",
         "temp", "FLOAT", "-i", csv, "-o", out_path},
        wout, werr);
    EXPECT_EQ(wc, 0) << werr.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"cat", "-t", "sensors", "-f", "csv", out_path}, rout, rerr),
              0)
        << rerr.str();
    EXPECT_EQ(rout.str(),
              "time,site,temp\n1000,beijing,21.5\n2000,shanghai,22\n");

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliRequirements, WriteRejectsHeaderShapeErrorsBeforeCreatingOutput) {
    struct Case {
        const char* name;
        const char* content;
        const char* needle;
    };
    const Case cases[] = {
        {"missing", "time,site\n1000,beijing\n", "missing required column"},
        {"undeclared", "time,site,temp,status\n1000,beijing,21.5,true\n",
         "undeclared column"},
        {"duplicate", "time,site,temp,TEMP\n1000,beijing,21.5,22.0\n",
         "conflict"},
    };
    for (const Case& c : cases) {
        std::string csv = tsfile_cli_test::unique_temp_path(
            std::string("tsfile_cli_header_") + c.name, ".csv");
        {
            std::ofstream o(csv.c_str());
            o << c.content;
        }
        std::string out_path = tsfile_cli_test::unique_temp_path(
            std::string("tsfile_cli_header_out_") + c.name, ".tsfile");

        std::ostringstream out;
        std::ostringstream err;
        int code = tsfile_cli::run_cli(
            {"write", "--table", "sensors", "--tag", "site", "STRING",
             "--field", "temp", "FLOAT", "-i", csv, "-o", out_path},
            out, err);
        EXPECT_EQ(code, 2) << c.name << " " << err.str();
        EXPECT_TRUE(out.str().empty());
        EXPECT_NE(err.str().find(c.needle), std::string::npos)
            << c.name << " " << err.str();
        EXPECT_FALSE(file_exists(out_path));

        std::remove(csv.c_str());
        std::remove(out_path.c_str());
    }
}

TEST(CliRequirements, WriteAppliesAndValidatesTypePhysicalOverrides) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_type_override", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,site,temp,humidity\n"
             "1000,beijing,21.0,40.5\n"
             "2000,beijing,21.5,41.0\n";
    }
    std::string out_path = tsfile_cli_test::unique_temp_path(
        "tsfile_cli_type_override_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write",      "--table",      "sensors",  "--tag",
         "site",       "STRING",       "--field",  "temp",
         "FLOAT",      "--field",      "humidity", "FLOAT",
         "--encoding", "FLOAT",        "GORILLA",  "--compression",
         "FLOAT",      "UNCOMPRESSED", "-i",       csv,
         "-o",         out_path},
        wout, werr);
    EXPECT_EQ(wc, 0) << werr.str();

    std::ostringstream schema_out;
    std::ostringstream schema_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"schema", "-t", "sensors", "-f", "csv", out_path},
                            schema_out, schema_err),
        0)
        << schema_err.str();
    EXPECT_NE(schema_out.str().find(
                  "table,sensors,temp,FIELD,FLOAT,GORILLA,UNCOMPRESSED\n"),
              std::string::npos)
        << schema_out.str();
    EXPECT_NE(schema_out.str().find(
                  "table,sensors,humidity,FIELD,FLOAT,GORILLA,UNCOMPRESSED\n"),
              std::string::npos)
        << schema_out.str();

    std::ostringstream dup_out;
    std::ostringstream dup_err;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"write", "--table", "sensors", "--field", "temp", "FLOAT",
                   "--encoding", "FLOAT", "GORILLA", "--encoding", "FLOAT",
                   "PLAIN", "--stdin", "-o", "unused.tsfile"},
                  dup_out, dup_err),
              1);
    EXPECT_NE(dup_err.str().find("specified more than once"), std::string::npos)
        << dup_err.str();

    std::ostringstream unused_out;
    std::ostringstream unused_err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "sensors", "--field",
                                   "temp", "FLOAT", "--encoding", "DOUBLE",
                                   "GORILLA", "--stdin", "-o", "unused.tsfile"},
                                  unused_out, unused_err),
              1);
    EXPECT_NE(unused_err.str().find("not used"), std::string::npos)
        << unused_err.str();

    std::ostringstream bad_out;
    std::ostringstream bad_err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "binary_data", "--field",
                                   "payload", "BLOB", "--encoding", "BLOB",
                                   "GORILLA", "--stdin", "-o", "unused.tsfile"},
                                  bad_out, bad_err),
              1);
    EXPECT_NE(bad_err.str().find("not supported"), std::string::npos)
        << bad_err.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliRequirements, WriteRejectsExistingOutputWithoutTruncating) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_existing_in", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0,10\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_existing_out", ".tsfile");
    {
        std::ofstream o(out_path.c_str());
        o << "keep-me";
    }

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t1", "--field", "s1",
                                    "INT64", "-i", csv, "-o", out_path},
                                   out, err);
    EXPECT_EQ(code, 3);
    EXPECT_EQ(read_file(out_path), "keep-me");

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliRequirements, WriteRejectsNonRegularInputBeforeCreatingOutput) {
#ifndef _WIN32
    std::string dir =
        tsfile_cli_test::unique_temp_path("tsfile_cli_input_dir", "");
    ASSERT_EQ(mkdir(dir.c_str(), 0777), 0);
    std::string out_path = tsfile_cli_test::unique_temp_path(
        "tsfile_cli_input_dir_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t1", "--field", "s1",
                                    "INT64", "-i", dir, "-o", out_path},
                                   out, err);
    EXPECT_EQ(code, 2);
    EXPECT_FALSE(file_exists(out_path));

    rmdir(dir.c_str());
#endif
}

TEST(CliRequirements, WriteRejectsNonCanonicalTimeLexemesAsInputErrors) {
    const char* bad_times[] = {"+1", "01", "-0"};
    for (const char* bad_time : bad_times) {
        std::string csv =
            tsfile_cli_test::unique_temp_path("tsfile_cli_bad_time", ".csv");
        {
            std::ofstream o(csv.c_str());
            o << "time,s1\n" << bad_time << ",10\n";
        }
        std::string out_path = tsfile_cli_test::unique_temp_path(
            "tsfile_cli_bad_time_out", ".tsfile");
        std::ostringstream out;
        std::ostringstream err;
        int code =
            tsfile_cli::run_cli({"write", "--table", "t1", "--field", "s1",
                                 "INT64", "-i", csv, "-o", out_path},
                                out, err);
        EXPECT_EQ(code, 2) << bad_time << " " << err.str();
        EXPECT_FALSE(file_exists(out_path)) << bad_time;
        std::remove(csv.c_str());
        std::remove(out_path.c_str());
    }
}

TEST(CliRequirements, WriteTargetFailuresAreRuntimeErrors) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_target", ".csv");
    {
        std::ofstream o(csv.c_str(), std::ios::binary);
        o << "time,s1\n0,1\n";
    }

    std::ostringstream same_out;
    std::ostringstream same_err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "t1", "--field", "s1",
                                   "INT64", "-i", csv, "-o", csv},
                                  same_out, same_err),
              3);
    EXPECT_TRUE(same_out.str().empty());
    EXPECT_NE(same_err.str().find("same as the input"), std::string::npos)
        << same_err.str();
    EXPECT_EQ(read_file(csv), "time,s1\n0,1\n");

    std::string parent =
        tsfile_cli_test::unique_temp_path("tsfile_cli_missing_parent", "");
    std::string child = parent + "/out.tsfile";
    std::ostringstream parent_out;
    std::ostringstream parent_err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "t1", "--field", "s1",
                                   "INT64", "-i", csv, "-o", child},
                                  parent_out, parent_err),
              3);
    EXPECT_TRUE(parent_out.str().empty());
    EXPECT_NE(parent_err.str().find("cannot create output"), std::string::npos)
        << parent_err.str();
    EXPECT_FALSE(file_exists(child));

    std::remove(csv.c_str());
}

TEST(CliRequirements, WriteCsvNullAndEmptyStringRemainDistinctTags) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_tag_null", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,site,temp\n1000,\\N,21.0\n1000,\"\",22.0\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_tag_null_out", ".tsfile");
    std::ostringstream out;
    std::ostringstream err;
    ASSERT_EQ(tsfile_cli::run_cli(
                  {"write", "--table", "sensors", "--tag", "site", "STRING",
                   "--field", "temp", "FLOAT", "-i", csv, "-o", out_path},
                  out, err),
              0)
        << err.str();

    std::ostringstream cat_out;
    std::ostringstream cat_err;
    ASSERT_EQ(
        tsfile_cli::run_cli({"cat", "-t", "sensors", "-f", "ndjson", out_path},
                            cat_out, cat_err),
        0)
        << cat_err.str();
    EXPECT_NE(cat_out.str().find("\"site\":null"), std::string::npos)
        << cat_out.str();
    EXPECT_NE(cat_out.str().find("\"site\":\"\""), std::string::npos)
        << cat_out.str();
    EXPECT_NE(cat_out.str().find("\"temp\":21"), std::string::npos)
        << cat_out.str();
    EXPECT_NE(cat_out.str().find("\"temp\":22"), std::string::npos)
        << cat_out.str();

    std::ostringstream count_out;
    std::ostringstream count_err;
    ASSERT_EQ(tsfile_cli::run_cli({"count", "-t", "sensors", "-m", "site", "-f",
                                   "csv", out_path},
                                  count_out, count_err),
              0)
        << count_err.str();
    EXPECT_NE(count_out.str().find("table,sensors,site,TAG,2,2"),
              std::string::npos)
        << count_out.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliRequirements, WriteRejectsUnterminatedQuotedCsvField) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_unclosed_quote", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,site,temp\n1000,\"beijing,21.0\n";
    }
    std::string out_path = tsfile_cli_test::unique_temp_path(
        "tsfile_cli_unclosed_quote_out", ".tsfile");
    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"write", "--table", "sensors", "--tag", "site", "STRING",
                   "--field", "temp", "FLOAT", "-i", csv, "-o", out_path},
                  out, err),
              2);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("unterminated quoted CSV field"),
              std::string::npos)
        << err.str();
    EXPECT_FALSE(file_exists(out_path));

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliRequirements, LegacyColumnsOptionIsRejected) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t1", "--columns",
                                    "legacy", "-o", "x.tsfile", "--stdin"},
                                   out, err);
    EXPECT_EQ(code, 1);
    EXPECT_TRUE(out.str().empty());
    EXPECT_NE(err.str().find("Unknown flag: --columns"), std::string::npos)
        << err.str();
}

TEST(CliRequirements, WriteRejectsImplicitInputAndFormatFlag) {
    std::ostringstream implicit_out;
    std::ostringstream implicit_err;
    EXPECT_EQ(tsfile_cli::run_cli({"write", "--table", "t1", "--field", "s1",
                                   "INT64", "-o", "x.tsfile", "in.csv"},
                                  implicit_out, implicit_err),
              1);
    EXPECT_NE(
        implicit_err.str().find("choose exactly one of --input or --stdin"),
        std::string::npos)
        << implicit_err.str();

    std::ostringstream format_out;
    std::ostringstream format_err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"write", "--table", "t1", "--field", "s1", "INT64",
                             "-f", "csv", "--stdin", "-o", "x.tsfile"},
                            format_out, format_err),
        1);
    EXPECT_NE(format_err.str().find("--format is not valid"), std::string::npos)
        << format_err.str();
}

TEST(CliRequirements, ExportWritesSingleObjectAtomically) {
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

    std::ostringstream cat_out;
    std::ostringstream cat_err;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"cat", "-t", "table1", "-m", "s1", "-f", "csv", f.path},
                  cat_out, cat_err),
              0)
        << cat_err.str();
    EXPECT_EQ(read_file(out_path), cat_out.str());

    std::remove(out_path.c_str());
}

#ifndef _WIN32
TEST(CliRequirements, ExportForceRejectsSymlinkAndSpecialTargets) {
    TableFixture f;
    std::string sentinel =
        tsfile_cli_test::unique_temp_path("tsfile_cli_export_sentinel", ".txt");
    std::string symlink_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_export_link", ".csv");
    {
        std::ofstream file(sentinel.c_str());
        file << "sentinel";
    }
    ASSERT_EQ(symlink(sentinel.c_str(), symlink_path.c_str()), 0);

    std::ostringstream link_out;
    std::ostringstream link_err;
    EXPECT_EQ(tsfile_cli::run_cli({"export", "-t", "table1", "-o", symlink_path,
                                   "--type", "csv", "--force", f.path},
                                  link_out, link_err),
              3);
    EXPECT_EQ(read_file(sentinel), "sentinel");

    std::string fifo_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_export_fifo", ".csv");
    ASSERT_EQ(mkfifo(fifo_path.c_str(), 0600), 0);
    std::ostringstream fifo_out;
    std::ostringstream fifo_err;
    EXPECT_EQ(tsfile_cli::run_cli({"export", "-t", "table1", "-o", fifo_path,
                                   "--type", "csv", "--force", f.path},
                                  fifo_out, fifo_err),
              3);

    std::remove(symlink_path.c_str());
    std::remove(sentinel.c_str());
    std::remove(fifo_path.c_str());
}
#endif

TEST(CliRequirements, ExportWritesMultiObjectManifestAndNumberedFiles) {
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
    EXPECT_EQ(read_file(dir + "/0001.csv"),
              "time,id1,s1\n0,sensors_a_tag,10\n");
    EXPECT_EQ(read_file(dir + "/0002.csv"),
              "time,id1,s1\n0,sensors_b_tag,20\n");
    std::string manifest = read_file(dir + "/_manifest.json");
    EXPECT_NE(manifest.find("\"complete\": true"), std::string::npos)
        << manifest;
    EXPECT_NE(manifest.find("\"file\":\"0001.csv\""), std::string::npos)
        << manifest;
    EXPECT_NE(manifest.find("\"object\":\"sensors_b\""), std::string::npos)
        << manifest;
    EXPECT_NE(manifest.find("\"rows\":\"1\""), std::string::npos) << manifest;

    std::remove((dir + "/0001.csv").c_str());
    std::remove((dir + "/0002.csv").c_str());
    std::remove((dir + "/_manifest.json").c_str());
#ifndef _WIN32
    rmdir(dir.c_str());
#endif
}

TEST(CliRequirements, MultipleTagFiltersRequireAndHonorTagMatch) {
    std::string path = tsfile_cli_test::write_tag_filter_fixture();
    std::ostringstream missing_out;
    std::ostringstream missing_err;
    EXPECT_EQ(tsfile_cli::run_cli(
                  {"cat", "-m", "s1", "--tag-filter", "id1", "eq", "dev_a",
                   "--tag-filter", "id1", "eq", "dev_c", "-f", "csv", path},
                  missing_out, missing_err),
              1);

    std::ostringstream out;
    std::ostringstream err;
    EXPECT_EQ(
        tsfile_cli::run_cli({"cat", "-m", "s1", "--tag-filter", "id1", "eq",
                             "dev_a", "--tag-filter", "id1", "eq", "dev_c",
                             "--tag-match", "any", "-f", "csv", path},
                            out, err),
        0)
        << err.str();
    EXPECT_EQ(out.str(), "time,id1,s1\n0,dev_a,10\n3,dev_c,40\n");
    std::remove(path.c_str());
}

TEST(CliRequirements, SketchRejectsRegularResultFormat) {
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

TEST(CliRequirements, SketchWritesStdoutAndAtomicOutput) {
    TableFixture f;
    std::ostringstream stdout_out;
    std::ostringstream stdout_err;
    ASSERT_EQ(tsfile_cli::run_cli({"sketch", f.path}, stdout_out, stdout_err),
              0)
        << stdout_err.str();
    EXPECT_NE(stdout_out.str().find("TsFile Sketch"), std::string::npos)
        << stdout_out.str();
    EXPECT_NE(stdout_out.str().find("model: table"), std::string::npos)
        << stdout_out.str();

    std::string output =
        tsfile_cli_test::unique_temp_path("tsfile_cli_sketch", ".txt");
    std::ostringstream file_out;
    std::ostringstream file_err;
    ASSERT_EQ(tsfile_cli::run_cli({"sketch", "-o", output, f.path}, file_out,
                                  file_err),
              0)
        << file_err.str();
    EXPECT_TRUE(file_out.str().empty());
    EXPECT_EQ(read_file(output), stdout_out.str());

    const std::string sentinel = "do not replace";
    {
        std::ofstream existing(output.c_str(), std::ios::binary);
        existing << sentinel;
    }
    std::ostringstream no_force_out;
    std::ostringstream no_force_err;
    EXPECT_EQ(tsfile_cli::run_cli({"sketch", "-o", output, f.path},
                                  no_force_out, no_force_err),
              3);
    EXPECT_EQ(read_file(output), sentinel);

    std::ostringstream force_out;
    std::ostringstream force_err;
    ASSERT_EQ(tsfile_cli::run_cli({"sketch", "-o", output, "--force", f.path},
                                  force_out, force_err),
              0)
        << force_err.str();
    EXPECT_EQ(read_file(output), stdout_out.str());

    const std::string source_before = read_file(f.path);
    std::ostringstream alias_out;
    std::ostringstream alias_err;
    EXPECT_EQ(tsfile_cli::run_cli({"sketch", "-o", f.path, "--force", f.path},
                                  alias_out, alias_err),
              3);
    EXPECT_EQ(read_file(f.path), source_before);
    std::remove(output.c_str());
}

TEST(CliRequirements, ModelDetectionPrefersTableSchemaAndFallsBackToTree) {
    TableFixture table;
    storage::TsFileReader table_reader;
    ASSERT_EQ(table_reader.open(table.path), common::E_OK);
    EXPECT_FALSE(table_reader.get_all_table_schemas().empty());
    EXPECT_FALSE(table_reader.get_all_device_ids().empty());
    table_reader.close();

    std::ostringstream table_out;
    std::ostringstream table_err;
    ASSERT_EQ(tsfile_cli::run_cli({"ls", "-f", "csv", table.path}, table_out,
                                  table_err),
              0)
        << table_err.str();
    EXPECT_EQ(table_out.str(), "model,object\ntable,table1\n");

    std::string tree_path = tsfile_cli_test::write_sparse_tree_fixture();
    storage::TsFileReader tree_reader;
    ASSERT_EQ(tree_reader.open(tree_path), common::E_OK);
    EXPECT_TRUE(tree_reader.get_all_table_schemas().empty());
    EXPECT_FALSE(tree_reader.get_all_device_ids().empty());
    tree_reader.close();

    std::ostringstream tree_out;
    std::ostringstream tree_err;
    ASSERT_EQ(
        tsfile_cli::run_cli({"ls", "-f", "csv", tree_path}, tree_out, tree_err),
        0)
        << tree_err.str();
    EXPECT_EQ(tree_out.str(), "model,object\ntree,root.test.d1\n");
    std::remove(tree_path.c_str());
}
