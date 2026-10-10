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
#include <sys/stat.h>

#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "cli/run_cli.h"
#include "cli_test_util.h"
#include "format/input_format.h"

namespace {

const std::vector<std::string> kFormats = {"csv", "table", "ndjson"};
const std::vector<std::string> kNames = {"bad\xff", "bad\xfe",
                                         "bad\xef\xbf\xbd"};

struct TreeDefinition {
    std::string device;
    std::vector<std::string> fields;
};

struct TableDefinition {
    std::string table;
    std::vector<std::string> tags;
    std::vector<std::string> fields;
};

class CliUtf8CollisionTest : public ::testing::Test {
   protected:
    void SetUp() override { storage::libtsfile_init(); }

    void TearDown() override {
        for (const auto& path : files_) std::remove(path.c_str());
        for (const auto& path : directories_) {
            for (const auto& name :
                 {"_manifest.json", "0001.csv", "0002.csv", "0001.ndjson",
                  "0002.ndjson", "0001.txt", "0002.txt"}) {
                std::remove((path + "/" + name).c_str());
            }
#ifdef _WIN32
            _rmdir(path.c_str());
#else
            rmdir(path.c_str());
#endif
        }
    }

    std::string temp_file(const std::string& extension = ".tsfile") {
        std::string path =
            tsfile_cli_test::unique_temp_path("utf8_collision", extension);
        files_.push_back(path);
        return path;
    }

    std::string temp_directory() {
        std::string path =
            tsfile_cli_test::unique_temp_path("utf8_collision_export", "");
        directories_.push_back(path);
        return path;
    }

    bool exists(const std::string& path) {
        struct stat st;
        return stat(path.c_str(), &st) == 0;
    }

    void write_tree(const std::string& path,
                    const std::vector<TreeDefinition>& definitions,
                    bool aligned = false) {
        storage::WriteFile file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(file.create(path, flags, 0666), common::E_OK);
        storage::TsFileTreeWriter writer(&file);
        for (const auto& definition : definitions) {
            std::string device = definition.device;
            std::vector<storage::MeasurementSchema*> schemas;
            for (const auto& field : definition.fields) {
                schemas.push_back(new storage::MeasurementSchema(
                    field, common::INT64, common::PLAIN, common::UNCOMPRESSED));
                if (!aligned) {
                    ASSERT_EQ(
                        writer.register_timeseries(device, schemas.back()),
                        common::E_OK);
                }
            }
            if (aligned) {
                ASSERT_EQ(writer.register_timeseries(device, schemas),
                          common::E_OK);
            }
            storage::TsRecord record(device, 1000);
            for (const auto& field : definition.fields) {
                record.add_point(field, static_cast<int64_t>(42));
            }
            ASSERT_EQ(writer.write(record), common::E_OK);
            if (!aligned) {
                for (auto* schema : schemas) delete schema;
            }
        }
        ASSERT_EQ(writer.flush(), common::E_OK);
        ASSERT_EQ(writer.close(), common::E_OK);
    }

    void write_tables(const std::string& path,
                      const std::vector<TableDefinition>& definitions,
                      int rows = 2) {
        storage::TsFileWriter writer;
        ASSERT_EQ(writer.open(path), common::E_OK);
        for (const auto& definition : definitions) {
            std::vector<common::ColumnSchema> columns;
            std::vector<std::string> names;
            std::vector<common::TSDataType> types;
            std::vector<common::ColumnCategory> categories;
            for (const auto& tag : definition.tags) {
                columns.push_back(common::ColumnSchema(
                    tag, common::STRING, common::UNCOMPRESSED, common::PLAIN,
                    common::ColumnCategory::TAG));
                names.push_back(tag);
                types.push_back(common::STRING);
                categories.push_back(common::ColumnCategory::TAG);
            }
            for (const auto& field : definition.fields) {
                columns.push_back(common::ColumnSchema(
                    field, common::INT64, common::UNCOMPRESSED, common::PLAIN,
                    common::ColumnCategory::FIELD));
                names.push_back(field);
                types.push_back(common::INT64);
                categories.push_back(common::ColumnCategory::FIELD);
            }
            ASSERT_EQ(
                writer.register_table(std::make_shared<storage::TableSchema>(
                    definition.table, columns)),
                common::E_OK);
            if (rows == 0) continue;
            storage::Tablet tablet(definition.table, names, types, categories,
                                   rows);
            for (int row = 0; row < rows; ++row) {
                tablet.add_timestamp(row, static_cast<int64_t>(1000 + row));
                for (const auto& tag : definition.tags) {
                    tablet.add_value(row, tag, row == 0 ? "first" : "second");
                }
                for (const auto& field : definition.fields) {
                    tablet.add_value(row, field, static_cast<int64_t>(42));
                }
            }
            ASSERT_EQ(writer.write_table(tablet), common::E_OK);
        }
        ASSERT_EQ(writer.flush(), common::E_OK);
        ASSERT_EQ(writer.close(), common::E_OK);
    }

    void expect_collision(const std::vector<std::string>& args) {
        std::ostringstream out, err;
        EXPECT_EQ(tsfile_cli::run_cli(args, out, err), 2) << err.str();
        EXPECT_TRUE(out.str().empty()) << out.str();
        EXPECT_NE(err.str().find("after UTF-8 replacement"), std::string::npos)
            << err.str();
        EXPECT_TRUE(tsfile_cli::is_valid_utf8(err.str()));
    }

    void expect_success(const std::vector<std::string>& args) {
        std::ostringstream out, err;
        EXPECT_EQ(tsfile_cli::run_cli(args, out, err), 0) << err.str();
        EXPECT_TRUE(err.str().empty()) << err.str();
        EXPECT_TRUE(tsfile_cli::is_valid_utf8(out.str()));
    }

   private:
    std::vector<std::string> files_;
    std::vector<std::string> directories_;
};

TEST_F(CliUtf8CollisionTest, TreeColumnsFailBeforeOutputAcrossFormats) {
    for (bool aligned : {false, true}) {
        const std::string path = temp_file();
        write_tree(path, {{"root.d", kNames}}, aligned);
        ASSERT_FALSE(HasFatalFailure());
        for (const auto& format : kFormats) {
            for (const auto& command :
                 {"schema", "count", "stats", "head", "cat"}) {
                SCOPED_TRACE(std::string(command) + " " + format +
                             " aligned=" + std::to_string(aligned));
                expect_collision({command, "-f", format, path});
            }
            expect_success({"ls", "-f", format, path});
        }
        // Sketch deliberately follows printSketch without collision checking.
        expect_success({"sketch", path});
    }
}

TEST_F(CliUtf8CollisionTest, TableFieldsAndTagsFailBeforeOutputAcrossFormats) {
    const std::vector<TableDefinition> definitions = {
        {"t", {"id"}, kNames},
        {"t", kNames, {"value"}},
        {"t", {kNames[0]}, {kNames[1]}},
        {"t", {"id"}, {kNames[0], kNames[2]}}};
    for (const auto& definition : definitions) {
        for (int rows : {0, 2}) {
            const std::string path = temp_file();
            write_tables(path, {definition}, rows);
            ASSERT_FALSE(HasFatalFailure());
            for (const auto& format : kFormats) {
                for (const auto& command :
                     {"schema", "count", "stats", "head", "cat"}) {
                    SCOPED_TRACE(std::string(command) + " " + format +
                                 " rows=" + std::to_string(rows));
                    expect_collision({command, "-f", format, path});
                }
                expect_success({"ls", "-f", format, path});
            }
        }
    }
}

TEST_F(CliUtf8CollisionTest, ObjectCollisionsDoNotRequireMatchingColumnNames) {
    for (bool table : {false, true}) {
        for (size_t second : {size_t(1), size_t(2)}) {
            const std::string path = temp_file();
            if (table) {
                write_tables(path, {{kNames[0], {"id"}, {"left"}},
                                    {kNames[second], {"id"}, {"right"}}});
            } else {
                write_tree(
                    path, {{kNames[0], {"left"}}, {kNames[second], {"right"}}});
            }
            ASSERT_FALSE(HasFatalFailure());
            for (const auto& format : kFormats) {
                for (const auto& command : {"ls", "schema", "count", "stats"}) {
                    SCOPED_TRACE(std::string(command) + " " + format);
                    expect_collision({command, "-f", format, path});
                    if (std::string(command) != "ls") {
                        expect_success({command, table ? "-t" : "-d", kNames[0],
                                        "-f", format, path});
                        expect_success(
                            {command, "-m", "left", "-f", format, path});
                    }
                }
            }
        }
    }
}

TEST_F(CliUtf8CollisionTest, ProjectionAndSeparateColumnScopesRemainValid) {
    for (bool table : {false, true}) {
        const std::string path = temp_file();
        const std::string scoped = temp_file();
        if (table) {
            write_tables(path, {{"a", {"id"}, kNames}});
            write_tables(scoped, {{"a", {"id"}, {kNames[0], "value"}},
                                  {"b", {"id"}, {kNames[1], "value"}}});
        } else {
            write_tree(path, {{"a", kNames}});
            write_tree(scoped, {{"a", {kNames[0], "value"}},
                                {"b", {kNames[1], "value"}}});
        }
        ASSERT_FALSE(HasFatalFailure());
        for (const auto& format : kFormats) {
            for (const auto& command :
                 {"schema", "count", "stats", "head", "cat"}) {
                expect_success({command, "-m", kNames[0], "-f", format, path});
                if (std::string(command) != "head" &&
                    std::string(command) != "cat") {
                    expect_success({command, "-f", format, scoped});
                }
            }
            const std::string output = temp_file(".out");
            expect_success({"export", table ? "-t" : "-d", "a", "-m", kNames[0],
                            "--type", format, "-o", output, path});
            EXPECT_TRUE(exists(output));
            const std::string dir = temp_directory();
            expect_success({"export", table ? "-t" : "-d", "a",
                            table ? "-t" : "-d", "b", "--type", format,
                            "--output-dir", dir, scoped});
            EXPECT_TRUE(exists(dir + "/_manifest.json"));
        }
    }
}

TEST_F(CliUtf8CollisionTest, StatsChecksMergedTagHeadersAcrossTables) {
    const std::string path = temp_file();
    write_tables(
        path, {{"a", {kNames[0]}, {"value"}}, {"b", {kNames[1]}, {"value"}}});
    ASSERT_FALSE(HasFatalFailure());
    for (const auto& format : kFormats) {
        expect_collision({"stats", "-f", format, path});
        for (const auto& command : {"schema", "count"}) {
            expect_success({command, "-f", format, path});
        }
        expect_success({"stats", "-t", "a", "-f", format, path});
    }
}

TEST_F(CliUtf8CollisionTest, EmptyRowWindowsCannotHideSelectedNameCollisions) {
    const std::vector<std::vector<std::string>> windows = {
        {"-n", "0"},
        {"--start", "0", "-n", "0"},
        {"--start", "2000"},
        {"--end", "0"},
        {"--offset", "1"}};
    for (bool table : {false, true}) {
        const std::string path = temp_file();
        if (table)
            write_tables(path, {{"t", {"id"}, kNames}});
        else
            write_tree(path, {{"root.d", kNames}});
        ASSERT_FALSE(HasFatalFailure());
        for (const auto& format : kFormats) {
            for (const auto& window : windows) {
                for (const auto& command : {"head", "cat", "export"}) {
                    SCOPED_TRACE(std::string(command) + " " + format);
                    std::vector<std::string> args = {command};
                    args.insert(args.end(), window.begin(), window.end());
                    const std::string output = temp_file(".out");
                    if (std::string(command) == "export") {
                        args.insert(
                            args.end(),
                            {table ? "-t" : "-d", table ? "t" : "root.d",
                             "--type", format, "-o", output});
                    } else {
                        args.insert(args.end(), {"-f", format});
                    }
                    args.push_back(path);
                    expect_collision(args);
                    EXPECT_FALSE(exists(output));
                }
            }
        }
    }
}

TEST_F(CliUtf8CollisionTest, ExportCollisionPreservesExistingTargets) {
    for (bool table : {false, true}) {
        const std::string path = temp_file();
        if (table)
            write_tables(path, {{"t", {"id"}, kNames}});
        else
            write_tree(path, {{"root.d", kNames}});
        ASSERT_FALSE(HasFatalFailure());
        for (const auto& format : kFormats) {
            const std::string output = temp_file(".out");
            expect_collision({"export", table ? "-t" : "-d",
                              table ? "t" : "root.d", "--type", format, "-o",
                              output, path});
            EXPECT_FALSE(exists(output));
            {
                std::ofstream file(output.c_str(), std::ios::binary);
                file << "keep existing output\n";
            }
            expect_collision({"export", table ? "-t" : "-d",
                              table ? "t" : "root.d", "--type", format, "-o",
                              output, path});
            expect_collision({"export", table ? "-t" : "-d",
                              table ? "t" : "root.d", "--type", format, "-o",
                              output + "/unavailable", path});
            expect_collision({"export", table ? "-t" : "-d",
                              table ? "t" : "root.d", "--type", format,
                              "--force", "-o", output, path});
            std::ifstream file(output.c_str(), std::ios::binary);
            std::ostringstream content;
            content << file.rdbuf();
            EXPECT_EQ(content.str(), "keep existing output\n");
        }
    }
}

TEST_F(CliUtf8CollisionTest,
       MultiExportChecksEveryObjectBeforeCreatingDirectory) {
    for (bool table : {false, true}) {
        for (bool object_collision : {false, true}) {
            const std::string first = object_collision ? kNames[0] : "a";
            const std::string second = object_collision ? kNames[1] : "b";
            const std::string path = temp_file();
            if (table) {
                write_tables(
                    path, {{first, {"id"}, {"left"}},
                           {second,
                            {"id"},
                            object_collision ? std::vector<std::string>{"right"}
                                             : kNames}});
            } else {
                write_tree(path,
                           {{first, {"left"}},
                            {second, object_collision
                                         ? std::vector<std::string>{"right"}
                                         : kNames}});
            }
            ASSERT_FALSE(HasFatalFailure());
            for (const auto& format : kFormats) {
                const std::string dir = temp_directory();
                expect_collision({"export", table ? "-t" : "-d", first,
                                  table ? "-t" : "-d", second, "--type", format,
                                  "--output-dir", dir, path});
                EXPECT_FALSE(exists(dir));
                // A collision takes precedence over output-path errors as
                // well, proving it is checked before publication is prepared.
                const std::string occupied = temp_file(".out");
                {
                    std::ofstream file(occupied.c_str());
                    file << "keep existing output\n";
                }
                expect_collision({"export", table ? "-t" : "-d", first,
                                  table ? "-t" : "-d", second, "--type", format,
                                  "--output-dir", occupied, path});
                std::ifstream file(occupied.c_str());
                std::ostringstream content;
                content << file.rdbuf();
                EXPECT_EQ(content.str(), "keep existing output\n");
            }
        }
    }
}

}  // namespace
