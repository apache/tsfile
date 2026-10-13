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
#include "commands/commands.h"
#include "commands/statistics.h"
#include "common/config/config.h"
#include "file/local_random_access_read_file.h"
#include "reader/tsfile_reader.h"

#ifndef TSFILE_CPP_SOURCE_DIR
#define TSFILE_CPP_SOURCE_DIR "."
#endif

namespace {

struct TableFixture {
    std::string path = tsfile_cli_test::write_table_fixture();
    ~TableFixture() { std::remove(path.c_str()); }
};

class FailingMetadataReadFile : public storage::LocalRandomAccessReadFile {
   public:
    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override {
        ++reads;
        if (fail_reads || (fail_at > 0 && (persistent ? reads >= fail_at
                                                      : reads == fail_at))) {
            failed = true;
            fail_reads = persistent;
            read_size = 0;
            return short_read ? common::E_OK : common::E_FILE_READ_ERR;
        }
        return LocalRandomAccessReadFile::read(offset, buffer, size, read_size);
    }

    bool fail_reads = false;
    bool persistent = false;
    bool failed = false;
    bool short_read = false;
    int reads = 0;
    int fail_at = 0;
};

class FlushFailingStreamBuf : public std::stringbuf {
   protected:
    int sync() override { return -1; }
};

}  // namespace

TEST(CliInternal, FileSummaryPropagatesEveryMetadataReadFailure) {
    for (bool internal_indexes : {false, true}) {
        TableFixture fixture;
        if (internal_indexes) {
            const uint32_t saved =
                common::g_config_value_.max_degree_of_index_node_;
            ASSERT_EQ(storage::set_max_degree_of_index_node(2), common::E_OK);
            std::remove(fixture.path.c_str());
            fixture.path = tsfile_cli_test::write_tag_filter_fixture();
            ASSERT_EQ(storage::set_max_degree_of_index_node(saved),
                      common::E_OK);
        }
        tsfile_cli::ParsedArgs args;
        args.file = fixture.path;
        int first_read = 0;
        int last_read = 0;
        {
            auto* source = new FailingMetadataReadFile;
            ASSERT_EQ(source->open(fixture.path), common::E_OK);
            storage::TsFileReader reader;
            ASSERT_EQ(
                reader.open(
                    std::unique_ptr<storage::RandomAccessReadFile>(source)),
                common::E_OK);
            first_read = source->reads + 1;
            tsfile_cli::FileSummary summary;
            std::ostringstream err;
            ASSERT_EQ(
                tsfile_cli::collect_file_summary(args, reader, summary, err), 0)
                << err.str();
            EXPECT_EQ(summary.device_count, internal_indexes ? 3 : 1);
            EXPECT_GT(summary.series_count, 0);
            last_read = source->reads;
        }
        for (bool persistent : {false, true}) {
            for (bool short_read : {false, true}) {
                for (int fail_at = first_read; fail_at <= last_read;
                     ++fail_at) {
                    SCOPED_TRACE(::testing::Message()
                                 << internal_indexes << ":" << persistent << ":"
                                 << short_read << ":" << fail_at);
                    auto* source = new FailingMetadataReadFile;
                    ASSERT_EQ(source->open(fixture.path), common::E_OK);
                    storage::TsFileReader reader;
                    ASSERT_EQ(
                        reader.open(
                            std::unique_ptr<storage::RandomAccessReadFile>(
                                source)),
                        common::E_OK);
                    source->fail_at = fail_at;
                    source->persistent = persistent;
                    source->short_read = short_read;
                    tsfile_cli::FileSummary summary;
                    summary.file = "unchanged";
                    summary.device_count = -1;
                    std::ostringstream err;
                    EXPECT_EQ(tsfile_cli::collect_file_summary(args, reader,
                                                               summary, err),
                              2);
                    EXPECT_TRUE(source->failed);
                    EXPECT_EQ(summary.file, "unchanged");
                    EXPECT_EQ(summary.device_count, -1);
                    EXPECT_NE(err.str().find("failed to read"),
                              std::string::npos)
                        << err.str();
                }
            }
        }
    }
}

TEST(CliInternal, TableStatsPropagateEveryMetadataReadFailure) {
    TableFixture fixture;
    const uint32_t saved = common::g_config_value_.max_degree_of_index_node_;
    ASSERT_EQ(storage::set_max_degree_of_index_node(2), common::E_OK);
    std::remove(fixture.path.c_str());
    fixture.path = tsfile_cli_test::write_tag_filter_fixture();
    ASSERT_EQ(storage::set_max_degree_of_index_node(saved), common::E_OK);
    tsfile_cli::ParsedArgs args;
    args.file = fixture.path;
    args.table = "t1";
    int first_read = 0;
    int last_read = 0;
    for (int fail_at = 0; fail_at <= last_read; ++fail_at) {
        if (fail_at > 0 && fail_at < first_read) {
            continue;
        }
        for (bool persistent : {false, true}) {
            for (bool short_read : {false, true}) {
                SCOPED_TRACE(::testing::Message()
                             << persistent << ":" << short_read << ":"
                             << fail_at);
                auto* source = new FailingMetadataReadFile;
                ASSERT_EQ(source->open(fixture.path), common::E_OK);
                storage::TsFileReader reader;
                ASSERT_EQ(
                    reader.open(
                        std::unique_ptr<storage::RandomAccessReadFile>(source)),
                    common::E_OK);
                // Match run_cli's model validation before command dispatch.
                std::vector<std::shared_ptr<storage::TableSchema>> schemas;
                std::vector<std::shared_ptr<storage::IDeviceID>> devices;
                ASSERT_EQ(reader.get_all_table_schemas(schemas), common::E_OK);
                ASSERT_EQ(reader.get_all_devices(devices), common::E_OK);
                first_read = source->reads + 1;
                source->fail_at = fail_at;
                source->persistent = persistent;
                source->short_read = short_read;
                std::ostringstream out;
                std::ostringstream err;
                const int ret = tsfile_cli::cmd_stats(
                    args, reader, tsfile_cli::OutputFormat::kCsv, out, err);
                if (fail_at == 0) {
                    ASSERT_EQ(ret, 0) << err.str();
                    EXPECT_TRUE(err.str().empty()) << err.str();
                    EXPECT_NE(out.str().find("table,t1,dev_a,s1,INT64,1,"),
                              std::string::npos);
                    EXPECT_NE(out.str().find("table,t1,dev_b,s1,INT64,2,"),
                              std::string::npos);
                    EXPECT_NE(out.str().find("table,t1,dev_c,s1,INT64,1,"),
                              std::string::npos);
                    last_read = source->reads;
                    ASSERT_GE(last_read, first_read);
                } else {
                    EXPECT_EQ(ret, 2);
                    EXPECT_TRUE(source->failed);
                    EXPECT_TRUE(out.str().empty()) << out.str();
                    EXPECT_NE(err.str().find("failed to read"),
                              std::string::npos)
                        << err.str();
                }
            }
        }
    }
}

TEST(CliInternal, ModelDetectionPropagatesMetadataReadErrors) {
    TableFixture fixture;
    using Command =
        int (*)(const tsfile_cli::ParsedArgs&, storage::TsFileReader&,
                tsfile_cli::OutputFormat, std::ostream&, std::ostream&);
    const Command commands[] = {tsfile_cli::cmd_ls,    tsfile_cli::cmd_schema,
                                tsfile_cli::cmd_meta,  tsfile_cli::cmd_stats,
                                tsfile_cli::cmd_count, tsfile_cli::cmd_head,
                                tsfile_cli::cmd_cat};
    for (size_t i = 0; i < sizeof(commands) / sizeof(commands[0]); ++i) {
        for (bool persistent : {false, true}) {
            SCOPED_TRACE(::testing::Message() << i << ":" << persistent);
            auto* source = new FailingMetadataReadFile;
            ASSERT_EQ(source->open(fixture.path), common::E_OK);
            storage::TsFileReader reader;
            ASSERT_EQ(
                reader.open(
                    std::unique_ptr<storage::RandomAccessReadFile>(source)),
                common::E_OK);
            source->fail_reads = true;
            source->persistent = persistent;
            tsfile_cli::ParsedArgs args;
            args.file = fixture.path;
            std::ostringstream out;
            std::ostringstream err;
            EXPECT_EQ(commands[i](args, reader, tsfile_cli::OutputFormat::kCsv,
                                  out, err),
                      2);
            EXPECT_TRUE(source->failed);
            EXPECT_TRUE(out.str().empty()) << out.str();
            EXPECT_NE(err.str().find("failed to read"), std::string::npos)
                << err.str();
        }
    }
}

TEST(CliInternal, SkillShipsRequiredReferenceFiles) {
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

TEST(CliInternal, StdoutFlushFailureReturnsRuntimeError) {
    TableFixture f;
    FlushFailingStreamBuf buffer;
    std::ostream out(&buffer);
    std::ostringstream err;
    EXPECT_EQ(tsfile_cli::run_cli({"cat", "-f", "csv", f.path}, out, err), 3);
    EXPECT_NE(err.str().find("failed to read rows"), std::string::npos)
        << err.str();
}
