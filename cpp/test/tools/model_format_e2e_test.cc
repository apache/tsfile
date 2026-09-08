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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#ifdef _WIN32
#include <direct.h>
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

#include "cli/run_cli.h"
#include "cli_test_util.h"

#ifndef TSFILE_CPP_SOURCE_DIR
#define TSFILE_CPP_SOURCE_DIR "."
#endif

namespace {

struct ModelFile {
    bool tree;
    std::vector<std::string> scope;
};

struct CliResult {
    int code;
    std::string out;
    std::string err;
};

class BothModels : public ::testing::TestWithParam<ModelFile> {
   protected:
    void SetUp() override {
        char current_directory[4096];
#ifdef _WIN32
        ASSERT_NE(_getcwd(current_directory, sizeof(current_directory)),
                  nullptr);
#else
        ASSERT_NE(getcwd(current_directory, sizeof(current_directory)),
                  nullptr);
#endif
        original_directory_ = current_directory;
        fixture_directory_ =
            tsfile_cli_test::unique_temp_path("tsfile_cli_model_work", "");
#ifdef _WIN32
        ASSERT_EQ(_mkdir(fixture_directory_.c_str()), 0);
        ASSERT_EQ(_chdir(fixture_directory_.c_str()), 0);
#else
        ASSERT_EQ(mkdir(fixture_directory_.c_str(), 0700), 0);
        ASSERT_EQ(chdir(fixture_directory_.c_str()), 0);
#endif

        const std::string generated =
            GetParam().tree ? tsfile_cli_test::write_complex_tree_fixture()
                            : tsfile_cli_test::write_complex_table_fixture();
        path_ = GetParam().tree ? "tsfile_cli_complex_tree_golden.tsfile"
                                : "tsfile_cli_complex_table_golden.tsfile";
        ASSERT_EQ(std::rename(generated.c_str(), path_.c_str()), 0);
    }
    void TearDown() override {
        std::remove(path_.c_str());
#ifdef _WIN32
        ASSERT_EQ(_chdir(original_directory_.c_str()), 0);
        ASSERT_EQ(_rmdir(fixture_directory_.c_str()), 0);
#else
        ASSERT_EQ(chdir(original_directory_.c_str()), 0);
        ASSERT_EQ(rmdir(fixture_directory_.c_str()), 0);
#endif
    }

    std::string path_;
    std::string original_directory_;
    std::string fixture_directory_;
};

std::string run(const std::vector<std::string>& args, int* code,
                std::string* err = nullptr) {
    std::ostringstream out;
    std::ostringstream diagnostics;
    *code = tsfile_cli::run_cli(args, out, diagnostics);
    if (err != nullptr) *err = diagnostics.str();
    return out.str();
}

CliResult execute(const std::vector<std::string>& args) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(args, out, err);
    return {code, out.str(), err.str()};
}

void expect_cli_exact(const std::vector<std::string>& args, int expected_code,
                      const std::string& expected_out,
                      const std::string& expected_err) {
    CliResult actual = execute(args);
    EXPECT_EQ(actual.code, expected_code);
    EXPECT_EQ(actual.out, expected_out);
    EXPECT_EQ(actual.err, expected_err);
}

std::string read_bytes(const std::string& path) {
    std::ifstream in(path.c_str(), std::ios::binary);
    std::ostringstream bytes;
    bytes << in.rdbuf();
    return bytes.str();
}

void expect_or_update_golden(const std::string& name,
                             const std::string& actual) {
    const std::string path = std::string(TSFILE_CPP_SOURCE_DIR) +
                             "/test/tools/golden/" + name + ".txt";
    if (std::getenv("TSFILE_UPDATE_CLI_GOLDENS") != nullptr) {
        std::ofstream out(path.c_str(), std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.good()) << path;
        out.write(actual.data(), static_cast<std::streamsize>(actual.size()));
        ASSERT_TRUE(out.good()) << path;
        return;
    }
    std::ifstream expected_file(path.c_str(), std::ios::binary);
    ASSERT_TRUE(expected_file.good())
        << "missing golden " << path
        << "; regenerate with TSFILE_UPDATE_CLI_GOLDENS=1";
    std::ostringstream expected;
    expected << expected_file.rdbuf();
    EXPECT_EQ(actual, expected.str()) << name;
}

}  // namespace

TEST_P(BothModels, MetadataCommandsImplementAllPublicFormats) {
    const std::string model = GetParam().tree ? "tree" : "table";
    const std::vector<std::string> commands = {"ls", "schema", "meta", "stats",
                                               "count"};
    const std::vector<std::string> formats = {"table", "ndjson", "csv"};
    for (const auto& command : commands) {
        for (const auto& format : formats) {
            int code = -1;
            std::string err;
            std::string output =
                run({command, "-f", format, path_}, &code, &err);
            EXPECT_EQ(code, 0) << command << " " << format << ": " << err;
            expect_or_update_golden(model + "_" + command + "_" + format,
                                    output);
            EXPECT_EQ(err, "") << command << " " << format;
        }
    }
}

TEST_P(BothModels, RowCommandsPreserveRowsAcrossFormats) {
    const std::string model = GetParam().tree ? "tree" : "table";
    const auto& scope = GetParam().scope;
    const std::vector<std::string> formats = {"table", "ndjson", "csv"};
    for (const auto& command : {std::string("head"), std::string("cat")}) {
        for (const auto& format : formats) {
            std::vector<std::string> args = {
                command, scope[0], scope[1], "-n", "2", "-f", format, path_};
            int code = -1;
            std::string err;
            std::string output = run(args, &code, &err);
            EXPECT_EQ(code, 0) << command << " " << format << ": " << err;
            expect_or_update_golden(model + "_" + command + "_" + format,
                                    output);
            EXPECT_EQ(err, "") << command << " " << format;
        }
    }
}

TEST_P(BothModels, SketchAndExportSupportBothModelKinds) {
    const std::string model = GetParam().tree ? "tree" : "table";
    int sketch_code = -1;
    std::string sketch_err;
    std::string sketch = run({"sketch", path_}, &sketch_code, &sketch_err);
    EXPECT_EQ(sketch_code, 0) << sketch_err;
    expect_or_update_golden(model + "_sketch", sketch);
    EXPECT_EQ(sketch_err, "");

    const auto& scope = GetParam().scope;
    std::string exported =
        tsfile_cli_test::unique_temp_path("tsfile_cli_model_export", ".csv");
    int export_code = -1;
    std::string export_err;
    std::string export_stdout = run(
        {"export", scope[0], scope[1], "--type", "csv", "-o", exported, path_},
        &export_code, &export_err);
    EXPECT_EQ(export_code, 0) << export_err;
    EXPECT_TRUE(export_stdout.empty());
    std::ifstream exported_file(exported.c_str());
    EXPECT_TRUE(exported_file.good());
    exported_file.close();
    expect_or_update_golden(model + "_export_csv", read_bytes(exported));
    std::remove(exported.c_str());
}

TEST_P(BothModels, EverySuccessfulCommandHasExactGoldenOutput) {
    const std::string model = GetParam().tree ? "tree" : "table";
    for (const auto& command :
         {std::string("ls"), std::string("schema"), std::string("meta"),
          std::string("stats"), std::string("count")}) {
        for (const auto& format : {std::string("table"), std::string("ndjson"),
                                   std::string("csv")}) {
            CliResult result = execute({command, "-f", format, path_});
            EXPECT_EQ(result.code, 0) << command << " " << format;
            expect_or_update_golden(model + "_" + command + "_" + format,
                                    result.out);
            EXPECT_EQ(result.err, "") << command << " " << format;
        }
    }

    const auto& scope = GetParam().scope;
    for (const auto& command : {std::string("head"), std::string("cat")}) {
        for (const auto& format : {std::string("table"), std::string("ndjson"),
                                   std::string("csv")}) {
            CliResult result = execute(
                {command, scope[0], scope[1], "-n", "2", "-f", format, path_});
            EXPECT_EQ(result.code, 0) << command << " " << format;
            expect_or_update_golden(model + "_" + command + "_" + format,
                                    result.out);
            EXPECT_EQ(result.err, "") << command << " " << format;
        }
    }

    CliResult sketch = execute({"sketch", path_});
    EXPECT_EQ(sketch.code, 0);
    expect_or_update_golden(model + "_sketch", sketch.out);
    EXPECT_EQ(sketch.err, "");

    for (const auto& format :
         {std::string("table"), std::string("ndjson"), std::string("csv")}) {
        const std::string target = tsfile_cli_test::unique_temp_path(
            "tsfile_cli_export_golden_" + model + "_" + format, ".out");
        CliResult result = execute({"export", scope[0], scope[1], "--type",
                                    format, "-o", target, path_});
        EXPECT_EQ(result.code, 0) << format;
        EXPECT_EQ(result.out, "");
        EXPECT_EQ(result.err, "");
        expect_or_update_golden(model + "_export_" + format,
                                read_bytes(target));
        std::remove(target.c_str());
    }
}

TEST(ComplexTableFixture, ContainsBothTablesAndNullValues) {
    const std::string path = tsfile_cli_test::write_complex_table_fixture();
    expect_cli_exact({"ls", "-f", "csv", path}, 0,
                     "model,object\n"
                     "table,table1\n"
                     "table,table2\n",
                     "");
    std::remove(path.c_str());
}

TEST(ComplexTableFixture, CsvAnswersMatchByteForByte) {
    const std::string path = tsfile_cli_test::write_complex_table_fixture();
    expect_cli_exact({"head", "-t", "table2", "-n", "3", "-f", "csv", path}, 0,
                     "time,site,rack,sensor,reading\n"
                     "0,\\N,rack0,sensor0,\\N\n"
                     "1,\"\",rack1,sensor1,1.25\n"
                     "2,null,rack2,sensor2,2.5\n",
                     "");
    std::remove(path.c_str());
}

TEST(ComplexTreeFixture, CsvAnswersMatchByteForByte) {
    const std::string path = tsfile_cli_test::write_complex_tree_fixture();
    expect_cli_exact(
        {"head", "-d", "root.test.d1", "-n", "2", "-f", "csv", path}, 0,
        "time,root.test.d1.m1,root.test.d1.m2,root.test.d1.m3,"
        "root.test.d1.m4,root.test.d1.m5\n"
        "0,0,0.5,value_0,0,100.5\n"
        "1,1,1.5,value_1,2,101.5\n",
        "");
    std::remove(path.c_str());
}

TEST(WriteCommand, SuccessfulWriteHasExactSilentOutput) {
    const std::string input = "tsfile_cli_write_golden.csv";
    const std::string output = "tsfile_cli_write_golden.tsfile";
    std::remove(input.c_str());
    std::remove(output.c_str());
    {
        std::ofstream csv(input.c_str(), std::ios::binary);
        ASSERT_TRUE(csv.good());
        csv << "time,id,value\n0,dev,10\n1,dev,20\n";
    }
    expect_cli_exact({"write", "--table", "written", "--tag", "id", "STRING",
                      "--field", "value", "INT64", "-i", input, "-o", output},
                     0, "", "");
    EXPECT_TRUE(std::ifstream(output.c_str()).good());
    std::remove(input.c_str());
    std::remove(output.c_str());
}

void rename_fixture(const std::string& generated, const std::string& stable) {
    std::remove(stable.c_str());
    ASSERT_EQ(std::rename(generated.c_str(), stable.c_str()), 0);
}

void expect_success_golden(const std::string& name,
                           const std::vector<std::string>& args) {
    CliResult result = execute(args);
    ASSERT_EQ(result.code, 0) << name << ": " << result.err;
    ASSERT_TRUE(result.err.empty()) << name << ": " << result.err;
    expect_or_update_golden(name, result.out);
}

TEST(IndependentFixtures, EmptyAndCategoryFilesHaveExactOutputs) {
    struct Fixture {
        std::string name;
        std::string path;
        std::string scope;
    };
    const std::string empty_table = "tsfile_cli_empty_table_golden.tsfile";
    const std::string category_edge = "tsfile_cli_category_edge_golden.tsfile";
    const std::string empty_field = "tsfile_cli_empty_field_golden.tsfile";
    rename_fixture(tsfile_cli_test::write_empty_table_fixture(), empty_table);
    rename_fixture(tsfile_cli_test::write_category_edge_fixture(),
                   category_edge);
    rename_fixture(tsfile_cli_test::write_empty_field_fixture(), empty_field);

    const std::vector<Fixture> fixtures = {
        {"empty_table", empty_table, "sensors"},
        {"category_edge", category_edge, "category_edge"},
        {"empty_field", empty_field, "sensors"}};
    for (const Fixture& fixture : fixtures) {
        for (const std::string& command :
             {"ls", "schema", "meta", "stats", "count"}) {
            for (const std::string& format : {"table", "ndjson", "csv"}) {
                expect_success_golden(
                    fixture.name + "_" + command + "_" + format,
                    {command, "-f", format, fixture.path});
            }
        }
        for (const std::string& command : {"head", "cat"}) {
            for (const std::string& format : {"table", "ndjson", "csv"}) {
                expect_success_golden(
                    fixture.name + "_" + command + "_" + format,
                    {command, "-t", fixture.scope, "-f", format, fixture.path});
            }
        }
        expect_success_golden(fixture.name + "_sketch",
                              {"sketch", fixture.path});
        for (const std::string& format : {"table", "ndjson", "csv"}) {
            const std::string target =
                "tsfile_cli_" + fixture.name + "_export_" + format + ".out";
            std::remove(target.c_str());
            CliResult result = execute({"export", "-t", fixture.scope, "--type",
                                        format, "-o", target, fixture.path});
            ASSERT_EQ(result.code, 0)
                << fixture.name << " " << format << ": " << result.err;
            ASSERT_TRUE(result.out.empty());
            ASSERT_TRUE(result.err.empty());
            ASSERT_TRUE(std::ifstream(target.c_str(), std::ios::binary).good());
            expect_or_update_golden(fixture.name + "_export_" + format,
                                    read_bytes(target));
            std::remove(target.c_str());
        }
    }
    std::remove(empty_table.c_str());
    std::remove(category_edge.c_str());
    std::remove(empty_field.c_str());
}

TEST(IndependentFixtures, EmptyTreeAndInputFailuresHaveExactDiagnostics) {
    const std::string empty_tree = "tsfile_cli_empty_tree_golden.tsfile";
    rename_fixture(tsfile_cli_test::write_empty_tree_fixture(), empty_tree);

    // A schema-only tree is a legal empty TsFile, but it has no device index;
    // metadata commands still succeed with deterministic header-only output.
    for (const std::string& command :
         {"ls", "schema", "meta", "stats", "count"}) {
        for (const std::string& format : {"table", "ndjson", "csv"}) {
            expect_success_golden("empty_tree_" + command + "_" + format,
                                  {command, "-f", format, empty_tree});
        }
    }
    expect_success_golden("empty_tree_sketch", {"sketch", empty_tree});

    // Row/export commands cannot select an absent device; verify their
    // user-facing usage failure exactly instead of merely checking a substring.
    for (const std::string& command : {"head", "cat"}) {
        for (const std::string& format : {"table", "ndjson", "csv"}) {
            expect_cli_exact({command, "-f", format, empty_tree}, 3, "",
                             "Error: no device found in file\n");
        }
    }
    for (const std::string& format : {"table", "ndjson", "csv"}) {
        const std::string target = "tsfile_cli_empty_tree_export_" + format;
        expect_cli_exact({"export", "-d", "root.empty.d1", "--type", format,
                          "-o", target, empty_tree},
                         1, "",
                         "Error: device 'root.empty.d1' does not exist\n");
        std::remove(target.c_str());
    }
    std::remove(empty_tree.c_str());

    const std::vector<std::string> commands = {"ls",    "schema", "meta",
                                               "stats", "count",  "head",
                                               "cat",   "sketch", "export"};
    const auto input_args = [](const std::string& command,
                               const std::string& path) {
        if (command == "export") {
            return std::vector<std::string>{"export",
                                            "-t",
                                            "missing",
                                            "--type",
                                            "csv",
                                            "-o",
                                            "tsfile_cli_input_error.csv",
                                            path};
        }
        std::vector<std::string> args = {command};
        if (command != "sketch" && command != "head" && command != "cat") {
            args.insert(args.end(), {"-f", "csv"});
        }
        args.push_back(path);
        return args;
    };
    const std::string missing = "tsfile_cli_missing_input.tsfile";
    std::remove(missing.c_str());
    for (const std::string& command : commands) {
        std::remove("tsfile_cli_input_error.csv");
        expect_cli_exact(
            input_args(command, missing), 2, "",
            "Error: cannot open " + missing + ": cannot open file (code 28)\n");
    }
    const std::string damaged = "tsfile_cli_damaged_input.tsfile";
    {
        std::ofstream out(damaged.c_str(), std::ios::binary | std::ios::trunc);
        ASSERT_TRUE(out.good());
        out << "not a TsFile";
    }
    for (const std::string& command : commands) {
        std::remove("tsfile_cli_input_error.csv");
        expect_cli_exact(input_args(command, damaged), 2, "",
                         "Error: cannot open " + damaged +
                             ": file is corrupted (code 35)\n");
    }
    std::remove(damaged.c_str());

    const std::string unsupported = tsfile_cli_test::write_empty_tree_fixture();
    ASSERT_FALSE(unsupported.empty());
    {
        std::fstream file(unsupported.c_str(),
                          std::ios::in | std::ios::out | std::ios::binary);
        ASSERT_TRUE(file.good());
        file.seekp(storage::MAGIC_STRING_TSFILE_LEN);
        file.put(static_cast<char>(0xff));
    }
    for (const std::string& command : commands) {
        std::remove("tsfile_cli_input_error.csv");
        expect_cli_exact(input_args(command, unsupported), 2, "",
                         "Error: cannot open " + unsupported +
                             ": unsupported TsFile format version (code 56)\n");
    }
    std::remove(unsupported.c_str());

    // Directories are special input paths, not TsFiles.  The check is made in
    // ReadFile before magic parsing, so this diagnostic is stable across all
    // read commands and does not leak parser output to stdout.
    for (const std::string& command : commands) {
        std::remove("tsfile_cli_input_error.csv");
        expect_cli_exact(input_args(command, "."), 2, "",
                         "Error: cannot open .: invalid path (code 37)\n");
    }
    std::remove("tsfile_cli_input_error.csv");
}

TEST(IndependentFixtures, NonPureFilesAreRejectedWithExactDiagnostics) {
    const std::string path = tsfile_cli_test::write_non_pure_fixture();
    ASSERT_FALSE(path.empty());

    const std::vector<std::string> commands = {"ls",    "schema", "meta",
                                               "stats", "count",  "head",
                                               "cat",   "sketch", "export"};
    const auto input_args = [](const std::string& command,
                               const std::string& file) {
        if (command == "export") {
            return std::vector<std::string>{"export",
                                            "-t",
                                            "hybrid_table",
                                            "--type",
                                            "csv",
                                            "-o",
                                            "tsfile_cli_non_pure_export.csv",
                                            file};
        }
        std::vector<std::string> args = {command};
        if (command != "sketch" && command != "head" && command != "cat") {
            args.insert(args.end(), {"-f", "csv"});
        }
        args.push_back(file);
        return args;
    };
    const std::string expected =
        "Error: input TsFile must be a pure tree-model or pure table-model "
        "file\n";
    for (const std::string& command : commands) {
        std::remove("tsfile_cli_non_pure_export.csv");
        expect_cli_exact(input_args(command, path), 2, "", expected);
    }
    std::remove("tsfile_cli_non_pure_export.csv");
    std::remove(path.c_str());
}

TEST(IndependentFixtures, MultiTableCorruptionStopsExportAfterFirstObject) {
    const std::string path =
        tsfile_cli_test::write_multi_table_corrupt_fixture();
    ASSERT_FALSE(path.empty());
    const std::string output_dir =
        tsfile_cli_test::unique_temp_path("tsfile_cli_corrupt_export_dir", "");
    std::remove((output_dir + "/0001.csv").c_str());
    std::remove((output_dir + "/0002.csv").c_str());
    std::remove((output_dir + "/_manifest.json").c_str());

    CliResult result =
        execute({"export", "-t", "sensors_a", "-t", "sensors_b", "--type",
                 "csv", "--output-dir", output_dir, path});
    const std::string expected_error =
        "Error: failed to read rows: file is corrupted\n";
    expect_cli_exact({"cat", "-t", "sensors_b", "-f", "csv", path}, 2, "",
                     expected_error);
    EXPECT_EQ(result.code, 2) << result.out << result.err;
    EXPECT_TRUE(result.out.empty());
    EXPECT_EQ(result.err, expected_error);
    EXPECT_EQ(read_bytes(output_dir + "/0001.csv"),
              "time,id1,s1\n0,sensors_a_tag,10\n");
    EXPECT_FALSE(std::ifstream(output_dir + "/0002.csv").good());
    EXPECT_EQ(read_bytes(output_dir + "/_manifest.json"),
              "{\n  \"complete\": false,\n  \"files\": [\n"
              "    {\"file\":\"0001.csv\",\"model\":\"table\","
              "\"object\":\"sensors_a\",\"type\":\"csv\","
              "\"rows\":\"1\"}\n  ]\n}\n");

    std::remove((output_dir + "/0001.csv").c_str());
    std::remove((output_dir + "/0002.csv").c_str());
    std::remove((output_dir + "/_manifest.json").c_str());
    std::remove(output_dir.c_str());
    std::remove(path.c_str());
}

TEST(IndependentFixtures, NullableTagsHaveExactPredicateResults) {
    const std::string path =
        tsfile_cli_test::write_nullable_tag_filter_fixture();
    ASSERT_FALSE(path.empty());

    const auto cat = [&path](const std::vector<std::string>& filter) {
        std::vector<std::string> args = {"cat", "-t", "t1",
                                         "-m",  "s1", "--tag-filter"};
        args.insert(args.end(), filter.begin(), filter.end());
        args.insert(args.end(), {"-f", "csv", path});
        return args;
    };
    expect_cli_exact(cat({"id1", "is-null"}), 0, "time,id1,s1\n0,\\N,10\n", "");
    expect_cli_exact(cat({"id1", "not-null"}), 0,
                     "time,id1,s1\n1,\"\",20\n3,dev_a,40\n4,dev_b,50\n"
                     "2,null,30\n",
                     "");
    expect_cli_exact(cat({"id1", "eq", ""}), 0, "time,id1,s1\n1,\"\",20\n", "");
    expect_cli_exact(cat({"id1", "eq", "null"}), 0, "time,id1,s1\n2,null,30\n",
                     "");
    expect_cli_exact(cat({"id1", "neq", "dev_a"}), 0,
                     "time,id1,s1\n1,\"\",20\n4,dev_b,50\n2,null,30\n", "");
    expect_cli_exact(cat({"id1", "regexp", "dev"}), 0, "time,id1,s1\n", "");
    expect_cli_exact(cat({"id1", "regexp", "dev_.*"}), 0,
                     "time,id1,s1\n3,dev_a,40\n4,dev_b,50\n", "");

    std::remove(path.c_str());
}

TEST(IndependentFixtures, SpecialCsvInputsHaveExactCodesAndErrors) {
    struct Case {
        const char* name;
        const char* csv;
        const char* expected;
    };
    const Case cases[] = {
        {"invalid_time", "time,site,temp\n01,dev,1\n",
         "Error: bad timestamp '01' (line 2)\n"},
        {"out_of_order", "time,site,temp\n2000,dev,1\n1000,dev,2\n",
         "Error: timestamps must be strictly increasing per device (line 3: "
         "1000 <= previous 2000)\n"},
        {"undeclared", "time,site,temp,status\n0,dev,1,ok\n",
         "Error: CSV contains undeclared column 'status'\n"},
        {"missing_column", "time,site\n0,beijing\n",
         "Error: CSV header is missing required column 'temp'\n"},
        {"unclosed_quote", "time,site,temp\n0,dev,\"unterminated\n",
         "Error: unterminated quoted CSV field (line 2)\n"},
        {"comment", "time,site,temp\n# comment\n",
         "Error: expected 3 fields, got 1 (line 2)\n"},
    };
    for (const Case& test_case : cases) {
        const std::string csv_path =
            std::string("tsfile_cli_") + test_case.name + ".csv";
        const std::string output_path =
            std::string("tsfile_cli_") + test_case.name + ".tsfile";
        std::remove(csv_path.c_str());
        std::remove(output_path.c_str());
        {
            std::ofstream csv(csv_path.c_str(),
                              std::ios::binary | std::ios::trunc);
            ASSERT_TRUE(csv.good());
            csv << test_case.csv;
        }
        expect_cli_exact(
            {"write", "--table", "sensors", "--tag", "site", "STRING",
             "--field", "temp", "INT64", "-i", csv_path, "-o", output_path},
            2, "", test_case.expected);
        EXPECT_FALSE(std::ifstream(output_path.c_str()).good());
        std::remove(csv_path.c_str());
        std::remove(output_path.c_str());
    }
}

INSTANTIATE_TEST_SUITE_P(TreeAndTable, BothModels,
                         ::testing::Values(ModelFile{true,
                                                     {"-d", "root.test.d1"}},
                                           ModelFile{false, {"-t", "table2"}}),
                         [](const ::testing::TestParamInfo<ModelFile>& info) {
                             return info.index == 0 ? "Tree" : "Table";
                         });
