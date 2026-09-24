/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "common/config/config.h"
#include "common/tablet.h"
#include "cwrapper/tsfile_cwrapper.h"
#include "file/write_file.h"
#include "reader/filter/tag_filter.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

namespace {

class FailingReadFile : public storage::RandomAccessReadFile {
   public:
    explicit FailingReadFile(const std::vector<char>& bytes) : bytes_(bytes) {}
    bool is_opened() const override { return opened_; }
    int64_t file_size() const override { return bytes_.size(); }
    const std::string& file_path() const override { return path_; }
    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        size = bytes_.size();
        fingerprint = 0;
        return common::E_OK;
    }
    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override {
        ++reads;
        read_size = 0;
        if (fail_at > 0 && (persistent ? reads >= fail_at : reads == fail_at)) {
            failed = true;
            return short_read ? common::E_OK : common::E_FILE_READ_ERR;
        }
        if (offset < 0 || size < 0) return common::E_INVALID_ARG;
        if (offset >= static_cast<int64_t>(bytes_.size())) return common::E_OK;
        read_size = static_cast<int32_t>(
            std::min<int64_t>(size, bytes_.size() - offset));
        std::memcpy(buffer, bytes_.data() + offset, read_size);
        return common::E_OK;
    }
    void close() override { opened_ = false; }

    int reads = 0;
    int fail_at = 0;
    bool persistent = false;
    bool short_read = false;
    bool failed = false;

   private:
    const std::vector<char>& bytes_;
    bool opened_ = true;
    std::string path_ = "memory://table-read-failure";
};

// One tag exercises TagEq's direct lookup; two tags exercise filtered
// traversal. Two devices fit in a leaf; five force multiple levels of internal
// index nodes.
class TableReadFailureTest
    : public ::testing::TestWithParam<std::tuple<int, int>> {
   protected:
    void SetUp() override {
        storage::libtsfile_init();
        saved_index_degree_ = common::g_config_value_.max_degree_of_index_node_;
        ASSERT_EQ(storage::set_max_degree_of_index_node(2), common::E_OK);
        filename_ =
            "table_read_failure_" +
            std::to_string(
                std::chrono::steady_clock::now().time_since_epoch().count()) +
            ".tsfile";
        storage::WriteFile file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(file.create(filename_, flags, 0666), common::E_OK);
        std::vector<common::ColumnSchema> columns;
        for (int i = 0; i < std::get<0>(GetParam()); ++i) {
            columns.emplace_back("id" + std::to_string(i), common::STRING,
                                 common::UNCOMPRESSED, common::PLAIN,
                                 common::ColumnCategory::TAG);
        }
        columns.emplace_back("value", common::INT64, common::UNCOMPRESSED,
                             common::PLAIN, common::ColumnCategory::FIELD);
        storage::TableSchema schema("test", columns);
        storage::TsFileTableWriter writer(&file, &schema);
        storage::Tablet tablet(
            "test", schema.get_measurement_names(), schema.get_data_types(),
            schema.get_column_categories(), std::get<1>(GetParam()) * 30);
        for (int device = 0; device < std::get<1>(GetParam()); ++device) {
            for (int t = 0; t < 30; ++t) {
                const int row = device * 30 + t;
                const std::string id = "d" + std::to_string(device);
                ASSERT_EQ(tablet.add_timestamp(row, t), common::E_OK);
                ASSERT_EQ(tablet.add_value(row, "id0", id.c_str()),
                          common::E_OK);
                if (std::get<0>(GetParam()) == 2) {
                    ASSERT_EQ(tablet.add_value(row, "id1", "tag"),
                              common::E_OK);
                }
                ASSERT_EQ(
                    tablet.add_value(row, "value", static_cast<int64_t>(row)),
                    common::E_OK);
            }
        }
        ASSERT_EQ(writer.write_table(tablet), common::E_OK);
        ASSERT_EQ(writer.flush(), common::E_OK);
        ASSERT_EQ(writer.close(), common::E_OK);
        std::ifstream input(filename_, std::ios::binary);
        ASSERT_TRUE(input.is_open());
        bytes_.assign(std::istreambuf_iterator<char>(input),
                      std::istreambuf_iterator<char>());
    }

    void TearDown() override {
        storage::set_max_degree_of_index_node(saved_index_degree_);
        std::remove(filename_.c_str());
        storage::libtsfile_destroy();
    }

    struct ScanResult {
        int ret = common::E_OK;
        int rows = 0;
        int open_reads = 0;
        int reads = 0;
        bool failed = false;
    };

    void scan(bool filter, int batch_size, int fail_at, bool persistent,
              bool short_read, ScanResult& out) {
        storage::TsFileReader reader;
        auto* source = new FailingReadFile(bytes_);
        source->fail_at = fail_at;
        source->persistent = persistent;
        source->short_read = short_read;
        ASSERT_EQ(
            reader.open(std::unique_ptr<storage::RandomAccessReadFile>(source)),
            common::E_OK);
        out.open_reads = source->reads;
        storage::TagEq eq(1, "d0");
        storage::ResultSet* result = nullptr;
        out.ret = reader.query("test", {"id0", "value"}, 0, INT64_MAX, result,
                               filter ? &eq : nullptr, batch_size);
        if (out.ret == common::E_OK) {
            ASSERT_NE(result, nullptr);
            bool next = false;
            while ((out.ret = result->next(next)) == common::E_OK && next) {
                if (batch_size == 0) {
                    ++out.rows;
                } else {
                    common::TsBlock* block = nullptr;
                    out.ret = result->get_next_tsblock(block);
                    if (out.ret != common::E_OK) break;
                    ASSERT_NE(block, nullptr);
                    out.rows += block->get_row_count();
                }
            }
        }
        out.reads = source->reads;
        out.failed = source->failed;
        if (out.ret != common::E_OK && result != nullptr) {
            source->fail_at = 0;
            bool next = true;
            EXPECT_EQ(result->next(next), out.ret);
            EXPECT_FALSE(next);
            if (batch_size != 0) {
                common::TsBlock* block = nullptr;
                EXPECT_EQ(result->get_next_tsblock(block), out.ret);
                EXPECT_EQ(block, nullptr);
            }
            EXPECT_EQ(source->reads, out.reads);
        }
        if (result != nullptr) reader.destroy_query_data_set(result);
    }

    std::vector<char> bytes_;
    std::string filename_;
    uint32_t saved_index_degree_ = 0;
};

TEST_P(TableReadFailureTest, EveryReadFailureReachesCaller) {
    for (bool filter : {false, true}) {
        for (int batch_size : {0, 16}) {
            ScanResult baseline;
            ASSERT_NO_FATAL_FAILURE(
                scan(filter, batch_size, 0, false, false, baseline));
            ASSERT_EQ(baseline.ret, common::E_OK);
            ASSERT_EQ(baseline.rows,
                      filter ? 30 : std::get<1>(GetParam()) * 30);
            for (bool persistent : {false, true}) {
                for (bool short_read : {false, true}) {
                    for (int fail_at = baseline.open_reads + 1;
                         fail_at <= baseline.reads; ++fail_at) {
                        SCOPED_TRACE(::testing::Message()
                                     << filter << ":" << batch_size << ":"
                                     << persistent << ":" << short_read << ":"
                                     << fail_at);
                        ScanResult failed;
                        ASSERT_NO_FATAL_FAILURE(scan(filter, batch_size,
                                                     fail_at, persistent,
                                                     short_read, failed));
                        EXPECT_TRUE(failed.failed);
                        EXPECT_EQ(failed.ret, common::E_FILE_READ_ERR);
                    }
                }
            }
        }
    }
}

TEST_P(TableReadFailureTest, MetadataApisPreserveReadErrors) {
    for (int operation = 0; operation < 4; ++operation) {
        storage::TsFileReader reader;
        auto* source = new FailingReadFile(bytes_);
        ASSERT_EQ(
            reader.open(std::unique_ptr<storage::RandomAccessReadFile>(source)),
            common::E_OK);
        source->fail_at = source->reads + 1;
        source->persistent = true;
        TableSchema* schema = nullptr;
        ERRNO schema_error = common::E_OK;
        if (operation == 0) {
            schema =
                tsfile_reader_get_table_schema(&reader, "test", &schema_error);
            EXPECT_EQ(schema_error, common::E_FILE_READ_ERR);
            EXPECT_EQ(schema, nullptr);
        } else if (operation == 3) {
            uint32_t count = 0;
            DeviceSchema* device_schemas =
                tsfile_reader_get_all_timeseries_schemas(&reader, &count,
                                                         &schema_error);
            EXPECT_EQ(schema_error, common::E_FILE_READ_ERR);
            EXPECT_EQ(count, 0u);
            EXPECT_EQ(device_schemas, nullptr);
        } else {
            ERRNO error = common::E_OK;
            TagFilterHandle filter =
                operation == 1
                    ? tsfile_tag_filter_create(&reader, "test", "id0", "d0",
                                               TAG_FILTER_EQ, &error)
                    : tsfile_tag_filter_between(&reader, "test", "id0", "d0",
                                                "d0", false, &error);
            EXPECT_EQ(error, common::E_FILE_READ_ERR);
            EXPECT_EQ(filter, nullptr);
            if (filter != nullptr) tsfile_tag_filter_free(filter);
        }
        EXPECT_TRUE(source->failed);
        // Metadata failures must not poison the reader or cache an empty
        // schema.
        source->fail_at = 0;
        if (operation == 3) {
            uint32_t count = 0;
            DeviceSchema* device_schemas =
                tsfile_reader_get_all_timeseries_schemas(&reader, &count,
                                                         &schema_error);
            ASSERT_EQ(schema_error, common::E_OK);
            ASSERT_NE(device_schemas, nullptr);
            ASSERT_GT(count, 0u);
            for (uint32_t i = 0; i < count; ++i) {
                free_device_schema(device_schemas[i]);
            }
            free(device_schemas);
        } else {
            schema =
                tsfile_reader_get_table_schema(&reader, "test", &schema_error);
            ASSERT_EQ(schema_error, common::E_OK);
            ASSERT_NE(schema, nullptr);
            EXPECT_STREQ(schema->table_name, "test");
            free_table_schema(*schema);
            free(schema);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(LeafAndInternalDeviceIndexes, TableReadFailureTest,
                         ::testing::Combine(::testing::Values(1, 2),
                                            ::testing::Values(2, 5)));

class FailingBlockReader : public storage::TsBlockReader {
   public:
    int has_next(bool& next) override {
        next = true;
        return common::E_OK;
    }
    int next(common::TsBlock*& block) override {
        ++reads;
        block = nullptr;
        return common::E_FILE_READ_ERR;
    }
    void close() override {}
    int reads = 0;
};

TEST(TableResultReadFailureTest, FailureWhileFetchingBlockIsTerminal) {
    storage::libtsfile_init();
    for (int mode : {storage::RETURN_ROW, storage::RETURN_BATCH}) {
        auto* source = new FailingBlockReader();
        storage::TableResultSet result(
            std::unique_ptr<storage::TsBlockReader>(source), {"value"},
            {common::INT64}, mode);
        bool next = true;
        common::TsBlock* block = nullptr;
        if (mode == storage::RETURN_ROW) {
            EXPECT_EQ(result.next(next), common::E_FILE_READ_ERR);
            EXPECT_FALSE(next);
        } else {
            EXPECT_EQ(result.get_next_tsblock(block), common::E_FILE_READ_ERR);
            EXPECT_EQ(block, nullptr);
        }
        EXPECT_EQ(result.next(next), common::E_FILE_READ_ERR);
        EXPECT_FALSE(next);
        EXPECT_EQ(result.get_next_tsblock(block), common::E_FILE_READ_ERR);
        EXPECT_EQ(block, nullptr);
        EXPECT_EQ(source->reads, 1);
    }
    storage::libtsfile_destroy();
}

}  // namespace
