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

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iterator>
#include <memory>
#include <new>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/config/config.h"
#include "common/tablet.h"
#include "cwrapper/tsfile_cwrapper.h"
#include "file/tsfile_io_reader.h"
#include "file/write_file.h"
#include "reader/filter/tag_filter.h"
#include "reader/meta_data_querier.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

namespace {

class FailingReadFile : public storage::RandomAccessReadFile {
   public:
    enum class ReadException { None, BadAlloc, Other };

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
            if (read_exception == ReadException::BadAlloc) {
                throw std::bad_alloc();
            }
            if (read_exception == ReadException::Other) {
                throw std::runtime_error("injected read exception");
            }
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
    ReadException read_exception = ReadException::None;

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
    for (int operation = 0; operation < 5; ++operation) {
        storage::TsFileReader reader;
        auto* source = new FailingReadFile(bytes_);
        ASSERT_EQ(
            reader.open(std::unique_ptr<storage::RandomAccessReadFile>(source)),
            common::E_OK);
        source->fail_at = source->reads + 1;
        source->persistent = true;
        TableSchema schema{};
        ERRNO schema_error = common::E_OK;
        if (operation == 0) {
            schema_error = tsfile_reader_get_table_schema_checked(
                &reader, "test", &schema);
            EXPECT_EQ(schema_error, common::E_FILE_READ_ERR);
            EXPECT_EQ(schema.table_name, nullptr);
            EXPECT_EQ(schema.column_num, 0);
            EXPECT_EQ(schema.column_schemas, nullptr);
        } else if (operation == 3) {
            uint32_t count = 0;
            DeviceSchema* device_schemas = nullptr;
            schema_error = tsfile_reader_get_all_timeseries_schemas_checked(
                &reader, &device_schemas, &count);
            EXPECT_EQ(schema_error, common::E_FILE_READ_ERR);
            EXPECT_EQ(count, 0u);
            EXPECT_EQ(device_schemas, nullptr);
        } else if (operation == 4) {
            uint32_t count = 0;
            TableSchema* schemas = nullptr;
            schema_error = tsfile_reader_get_all_table_schemas_checked(
                &reader, &schemas, &count);
            EXPECT_EQ(schema_error, common::E_FILE_READ_ERR);
            EXPECT_EQ(count, 0u);
            EXPECT_EQ(schemas, nullptr);
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
            DeviceSchema* device_schemas = nullptr;
            schema_error = tsfile_reader_get_all_timeseries_schemas_checked(
                &reader, &device_schemas, &count);
            ASSERT_EQ(schema_error, common::E_OK);
            ASSERT_NE(device_schemas, nullptr);
            ASSERT_GT(count, 0u);
            for (uint32_t i = 0; i < count; ++i) {
                free_device_schema(device_schemas[i]);
            }
            free(device_schemas);
        } else if (operation == 4) {
            uint32_t count = 0;
            TableSchema* schemas = nullptr;
            schema_error = tsfile_reader_get_all_table_schemas_checked(
                &reader, &schemas, &count);
            ASSERT_EQ(schema_error, common::E_OK);
            ASSERT_NE(schemas, nullptr);
            ASSERT_GT(count, 0u);
            for (uint32_t i = 0; i < count; ++i) {
                free_table_schema(schemas[i]);
            }
            free(schemas);
        } else {
            schema_error = tsfile_reader_get_table_schema_checked(
                &reader, "test", &schema);
            ASSERT_EQ(schema_error, common::E_OK);
            EXPECT_STREQ(schema.table_name, "test");
            free_table_schema(schema);
        }
    }
}

TEST_P(TableReadFailureTest, CheckedMetadataApisContainReadExceptions) {
    using ReadException = FailingReadFile::ReadException;
    // Enumerating every read also covers exceptions after device-schema
    // entries have been partially initialized.
    for (int operation = 0; operation < 3; ++operation) {
        auto invoke = [operation](storage::TsFileReader& reader,
                                  bool expect_failure) {
            if (operation == 0) {
                auto* schemas = reinterpret_cast<DeviceSchema*>(&reader);
                uint32_t count = 7;
                const ERRNO ret =
                    tsfile_reader_get_all_timeseries_schemas_checked(
                        &reader, &schemas, &count);
                if (expect_failure) {
                    EXPECT_EQ(schemas, nullptr);
                    EXPECT_EQ(count, 0u);
                } else {
                    EXPECT_NE(schemas, nullptr);
                    EXPECT_GT(count, 0u);
                }
                if (schemas != nullptr) {
                    for (uint32_t i = 0; i < count; ++i) {
                        free_device_schema(schemas[i]);
                    }
                    free(schemas);
                }
                return ret;
            }
            TagFilterHandle filter = &reader;
            const ERRNO ret =
                operation == 1
                    ? tsfile_tag_filter_create_checked(
                          &reader, "test", "id0", "d0", TAG_FILTER_EQ, &filter)
                    : tsfile_tag_filter_between_checked(
                          &reader, "test", "id0", "d0", "d1", false, &filter);
            if (expect_failure) {
                EXPECT_EQ(filter, nullptr);
            } else {
                EXPECT_NE(filter, nullptr);
            }
            tsfile_tag_filter_free(filter);
            return ret;
        };

        int first_read = 0;
        int last_read = 0;
        {
            storage::TsFileReader reader;
            auto* source = new FailingReadFile(bytes_);
            ASSERT_EQ(
                reader.open(
                    std::unique_ptr<storage::RandomAccessReadFile>(source)),
                common::E_OK);
            first_read = source->reads + 1;
            ASSERT_EQ(invoke(reader, false), common::E_OK);
            last_read = source->reads;
            ASSERT_GE(last_read, first_read);
        }
        for (ReadException exception :
             {ReadException::BadAlloc, ReadException::Other}) {
            const int expected = exception == ReadException::BadAlloc
                                     ? common::E_OOM
                                     : common::E_FILE_READ_ERR;
            for (int fail_at = first_read; fail_at <= last_read; ++fail_at) {
                SCOPED_TRACE(::testing::Message()
                             << operation << ":" << expected << ":" << fail_at);
                const int64_t allocated_before =
                    common::ModStat::get_instance().get_stat(
                        common::MOD_TSFILE_READER);
                {
                    storage::TsFileReader reader;
                    auto* source = new FailingReadFile(bytes_);
                    ASSERT_EQ(
                        reader.open(
                            std::unique_ptr<storage::RandomAccessReadFile>(
                                source)),
                        common::E_OK);
                    source->fail_at = fail_at;
                    source->read_exception = exception;
                    ERRNO ret = common::E_OK;
                    EXPECT_NO_THROW(ret = invoke(reader, true));
                    EXPECT_TRUE(source->failed);
                    EXPECT_EQ(ret, expected);
                }
                EXPECT_EQ(common::ModStat::get_instance().get_stat(
                              common::MOD_TSFILE_READER),
                          allocated_before);
            }
        }
    }
}

TEST_P(TableReadFailureTest, LegacySchemaApisReturnEmptyOnReadFailure) {
    for (int operation = 0; operation < 4; ++operation) {
        SCOPED_TRACE(operation);
        storage::TsFileReader reader;
        auto* source = new FailingReadFile(bytes_);
        ASSERT_EQ(
            reader.open(std::unique_ptr<storage::RandomAccessReadFile>(source)),
            common::E_OK);
        source->fail_at = source->reads + 1;
        source->persistent = true;
        if (operation == 0) {
            TableSchema schema =
                tsfile_reader_get_table_schema(&reader, "test");
            EXPECT_EQ(schema.table_name, nullptr);
            EXPECT_EQ(schema.column_num, 0);
            EXPECT_EQ(schema.column_schemas, nullptr);
            free_table_schema(schema);
        } else if (operation == 1) {
            uint32_t count = 7;
            TableSchema* schemas =
                tsfile_reader_get_all_table_schemas(&reader, &count);
            EXPECT_EQ(schemas, nullptr);
            EXPECT_EQ(count, 0u);
            if (schemas != nullptr) {
                for (uint32_t i = 0; i < count; ++i)
                    free_table_schema(schemas[i]);
                free(schemas);
            }
        } else if (operation == 2) {
            uint32_t count = 7;
            DeviceSchema* schemas =
                tsfile_reader_get_all_timeseries_schemas(&reader, &count);
            EXPECT_EQ(schemas, nullptr);
            EXPECT_EQ(count, 0u);
            if (schemas != nullptr) {
                for (uint32_t i = 0; i < count; ++i)
                    free_device_schema(schemas[i]);
                free(schemas);
            }
        } else {
            EXPECT_EQ(reader.get_table_schema("test"), nullptr);
        }
        EXPECT_TRUE(source->failed);
        source->fail_at = 0;
        auto schema = reader.get_table_schema("test");
        ASSERT_NE(schema, nullptr);
        EXPECT_EQ(schema->get_table_name(), "test");
    }
}

TEST_P(TableReadFailureTest, WholeFileMetadataUpdatesCallerOutput) {
    for (bool fail : {false, true}) {
        SCOPED_TRACE(fail);
        FailingReadFile source(bytes_);
        source.fail_at = fail ? 1 : 0;
        storage::TsFileIOReader io_reader;
        ASSERT_EQ(io_reader.init(&source), common::E_OK);
        storage::MetadataQuerier querier(&io_reader);
        storage::IMetadataQuerier& interface = querier;
        storage::TsFileMeta* metadata = nullptr;
        if (fail) {
            metadata = reinterpret_cast<storage::TsFileMeta*>(&querier);
            EXPECT_EQ(interface.get_whole_file_metadata(metadata),
                      common::E_FILE_READ_ERR);
            EXPECT_EQ(metadata, nullptr);
        } else {
            ASSERT_EQ(interface.get_whole_file_metadata(metadata),
                      common::E_OK);
            ASSERT_NE(metadata, nullptr);
            EXPECT_EQ(metadata->table_schemas_.count("test"), 1u);
        }
    }
}

TEST_P(TableReadFailureTest, NullTagOperatorsAcceptNullValues) {
    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(std::unique_ptr<storage::RandomAccessReadFile>(
                  new FailingReadFile(bytes_))),
              common::E_OK);
    for (TagFilterOp op : {TAG_FILTER_IS_NULL, TAG_FILTER_IS_NOT_NULL}) {
        for (bool checked : {false, true}) {
            SCOPED_TRACE(::testing::Message() << op << ":" << checked);
            TagFilterHandle filter = nullptr;
            ERRNO ret = common::E_OK;
            if (checked) {
                ret = tsfile_tag_filter_create_checked(&reader, "test", "id0",
                                                       nullptr, op, &filter);
            } else {
                filter = tsfile_tag_filter_create(&reader, "test", "id0",
                                                  nullptr, op, &ret);
            }
            ASSERT_EQ(ret, common::E_OK);
            ASSERT_NE(filter, nullptr);
            storage::ResultSet* result = nullptr;
            ret = reader.query("test", {"value"}, 0, INT64_MAX, result,
                               static_cast<storage::Filter*>(filter));
            EXPECT_EQ(ret, common::E_OK);
            int rows = 0;
            bool next = false;
            if (result != nullptr) {
                while ((ret = result->next(next)) == common::E_OK && next) {
                    ++rows;
                }
                reader.destroy_query_data_set(result);
            }
            tsfile_tag_filter_free(filter);
            EXPECT_EQ(ret, common::E_OK);
            EXPECT_EQ(rows, op == TAG_FILTER_IS_NULL
                                ? 0
                                : std::get<1>(GetParam()) * 30);
        }
    }
    for (TagFilterOp op : {TAG_FILTER_EQ, TAG_FILTER_NEQ, TAG_FILTER_LT,
                           TAG_FILTER_LTEQ, TAG_FILTER_GT, TAG_FILTER_GTEQ,
                           TAG_FILTER_REGEXP, TAG_FILTER_NOT_REGEXP}) {
        TagFilterHandle filter = &reader;
        EXPECT_EQ(tsfile_tag_filter_create_checked(&reader, "test", "id0",
                                                   nullptr, op, &filter),
                  common::E_INVALID_ARG);
        EXPECT_EQ(filter, nullptr);
    }
}

TEST_P(TableReadFailureTest, LegacyTagFactoriesReturnNullOnReadFailure) {
    using Factory = TagFilterHandle (*)(TsFileReader, const char*, const char*,
                                        const char*);
    const Factory factories[] = {tsfile_tag_filter_eq, tsfile_tag_filter_neq,
                                 tsfile_tag_filter_lt, tsfile_tag_filter_lteq,
                                 tsfile_tag_filter_gt, tsfile_tag_filter_gteq};
    for (auto factory : factories) {
        storage::TsFileReader reader;
        auto* source = new FailingReadFile(bytes_);
        ASSERT_EQ(
            reader.open(std::unique_ptr<storage::RandomAccessReadFile>(source)),
            common::E_OK);
        source->fail_at = source->reads + 1;
        source->persistent = true;
        TagFilterHandle filter = factory(&reader, "test", "id0", "d0");
        EXPECT_EQ(filter, nullptr);
        tsfile_tag_filter_free(filter);
        EXPECT_TRUE(source->failed);
        source->fail_at = 0;
        filter = factory(&reader, "test", "id0", "d0");
        ASSERT_NE(filter, nullptr);
        tsfile_tag_filter_free(filter);
    }
}

TEST_P(TableReadFailureTest, CheckedTagFactoriesPreserveMissingTableError) {
    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(std::unique_ptr<storage::RandomAccessReadFile>(
                  new FailingReadFile(bytes_))),
              common::E_OK);
    TagFilterHandle filter = &reader;
    EXPECT_EQ(tsfile_tag_filter_create_checked(&reader, "missing", "id0", "d0",
                                               TAG_FILTER_EQ, &filter),
              common::E_TABLE_NOT_EXIST);
    ASSERT_EQ(filter, nullptr);
    filter = &reader;
    EXPECT_EQ(tsfile_tag_filter_between_checked(&reader, "missing", "id0", "d0",
                                                "d1", false, &filter),
              common::E_TABLE_NOT_EXIST);
    ASSERT_EQ(filter, nullptr);

    ERRNO code = common::E_OK;
    EXPECT_EQ(tsfile_tag_filter_create(&reader, "missing", "id0", "d0",
                                       TAG_FILTER_EQ, &code),
              nullptr);
    EXPECT_EQ(code, common::E_INVALID_ARG);
    code = common::E_OK;
    EXPECT_EQ(tsfile_tag_filter_between(&reader, "missing", "id0", "d0", "d1",
                                        false, &code),
              nullptr);
    EXPECT_EQ(code, common::E_INVALID_ARG);
}

INSTANTIATE_TEST_SUITE_P(LeafAndInternalDeviceIndexes, TableReadFailureTest,
                         ::testing::Combine(::testing::Values(1, 2),
                                            ::testing::Values(2, 5)));

TEST(TableMetadataReadFailureTest, ResizedBufferIsReleasedOnReadFailure) {
    // A metadata tail larger than the initial 1 KiB buffer forces another
    // read before deserialization. Inject the failure into that second read.
    std::vector<char> bytes(2048, 0);
    const uint32_t metadata_size = 1536;
    for (int i = 0; i < 4; ++i) {
        bytes[bytes.size() - 10 + i] =
            static_cast<char>(metadata_size >> (8 * (3 - i)));
    }
    for (int failure = 0; failure < 4; ++failure) {
        SCOPED_TRACE(failure);
        const int64_t allocated_before =
            common::ModStat::get_instance().get_stat(common::MOD_TSFILE_READER);
        {
            FailingReadFile source(bytes);
            source.fail_at = 2;
            storage::TsFileIOReader reader;
            ASSERT_EQ(reader.init(&source), common::E_OK);
            storage::TsFileMeta* metadata = nullptr;
            if (failure == 0) {
                EXPECT_EQ(reader.get_tsfile_meta(metadata),
                          common::E_FILE_READ_ERR);
            } else if (failure == 1) {
                source.read_exception =
                    FailingReadFile::ReadException::BadAlloc;
                EXPECT_THROW(reader.get_tsfile_meta(metadata), std::bad_alloc);
            } else if (failure == 2) {
                source.read_exception = FailingReadFile::ReadException::Other;
                EXPECT_THROW(reader.get_tsfile_meta(metadata),
                             std::runtime_error);
            } else {
                common::TEST_fail_next_mem_realloc();
                EXPECT_EQ(reader.get_tsfile_meta(metadata), common::E_OOM);
            }
            EXPECT_EQ(source.reads, failure == 3 ? 1 : 2);
        }
        EXPECT_EQ(
            common::ModStat::get_instance().get_stat(common::MOD_TSFILE_READER),
            allocated_before);
    }
}

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
