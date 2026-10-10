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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "reader/prepared_series.h"

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <algorithm>
#include <cstdio>
#include <vector>

#include "common/global.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"

namespace storage {
namespace {

class PagePointGuard {
   public:
    explicit PagePointGuard(uint32_t page_points, uint32_t page_bytes = 0)
        : saved_(common::g_config_value_.page_writer_max_point_num_),
          saved_bytes_(common::g_config_value_.page_writer_max_memory_bytes_) {
        common::g_config_value_.page_writer_max_point_num_ = page_points;
        if (page_bytes != 0) {
            common::g_config_value_.page_writer_max_memory_bytes_ = page_bytes;
        }
    }
    ~PagePointGuard() {
        common::g_config_value_.page_writer_max_point_num_ = saved_;
        common::g_config_value_.page_writer_max_memory_bytes_ = saved_bytes_;
    }

   private:
    uint32_t saved_;
    uint32_t saved_bytes_;
};

class PreparedSeriesBatchTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        const auto* test_info =
            ::testing::UnitTest::GetInstance()->current_test_info();
        file_name_ = std::string("prepared_series_batch_test_") +
                     (test_info == nullptr ? "unknown" : test_info->name()) +
                     ".tsfile";
        std::replace(file_name_.begin(), file_name_.end(), '/', '_');
        std::remove(file_name_.c_str());
    }

    void TearDown() override {
        std::remove(file_name_.c_str());
        libtsfile_destroy();
    }

    void write_nullable_table() {
        PagePointGuard guard(10000);
        WriteFile write_file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(common::E_OK, write_file.create(file_name_, flags, 0666));

        std::vector<common::ColumnSchema> columns = {
            common::ColumnSchema("device", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("value", common::DOUBLE,
                                 common::ColumnCategory::FIELD),
            common::ColumnSchema("value2", common::DOUBLE,
                                 common::ColumnCategory::FIELD),
        };
        auto* schema = new TableSchema("weather", columns);
        TsFileTableWriter writer(&write_file, schema);
        Tablet tablet(
            "weather", {"device", "value", "value2"},
            {common::STRING, common::DOUBLE, common::DOUBLE},
            {common::ColumnCategory::TAG, common::ColumnCategory::FIELD,
             common::ColumnCategory::FIELD},
            70000);
        for (int row = 0; row < 70000; ++row) {
            tablet.add_timestamp(row, row);
            tablet.add_value(row, "device", "d0");
            if (row != 2 && row != 6) {
                tablet.add_value(row, "value", static_cast<double>(row));
            }
            if (row != 4 && row != 8) {
                tablet.add_value(row, "value2", static_cast<double>(row * 10));
            }
        }
        ASSERT_EQ(common::E_OK, writer.write_table(tablet));
        ASSERT_EQ(common::E_OK, writer.flush());
        ASSERT_EQ(common::E_OK, writer.close());
        delete schema;
    }

    std::string file_name_ = "prepared_series_batch_test.tsfile";
};

class NonAlignedPreparedSeriesOffsetTest
    : public PreparedSeriesBatchTest,
      public ::testing::WithParamInterface<uint32_t> {};

TEST_P(NonAlignedPreparedSeriesOffsetTest, AppliesRowOffset) {
    // Include pages larger than the 65536-row output block so the decoder
    // resumes an unfinished page on the next read.
    PagePointGuard guard(GetParam(), 16 * 1024 * 1024);
    const std::string device = "root.offset";
    const std::vector<std::string> names = {"boolean", "int32",  "int64",
                                            "float",   "double", "string"};
    const std::vector<common::TSDataType> types = {
        common::BOOLEAN, common::INT32,  common::INT64,
        common::FLOAT,   common::DOUBLE, common::STRING};
    auto schemas = std::make_shared<std::vector<MeasurementSchema>>();
    TsFileWriter writer;
    ASSERT_EQ(common::E_OK, writer.open(file_name_));
    for (size_t i = 0; i < names.size(); ++i) {
        schemas->emplace_back(names[i], types[i], common::PLAIN,
                              common::UNCOMPRESSED);
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(device, schemas->back()));
    }
    // Flush halfway through to exercise both page and chunk skipping.
    for (int start : {0, 70000}) {
        Tablet tablet(device, schemas, 70000);
        for (int row = 0; row < 70000; ++row) {
            const int value = start + row;
            ASSERT_EQ(common::E_OK, tablet.add_timestamp(row, value));
            ASSERT_EQ(common::E_OK, tablet.add_value(row, 0u, value % 2 == 0));
            ASSERT_EQ(common::E_OK, tablet.add_value(row, 1u, int32_t(value)));
            ASSERT_EQ(common::E_OK, tablet.add_value(row, 2u, int64_t(value)));
            ASSERT_EQ(common::E_OK,
                      tablet.add_value(row, 3u, float(value) + 0.5f));
            ASSERT_EQ(common::E_OK,
                      tablet.add_value(row, 4u, double(value) + 0.5));
            const std::string text = std::to_string(value);
            ASSERT_EQ(common::E_OK, tablet.add_value(row, 5u, text.c_str()));
        }
        ASSERT_EQ(common::E_OK, writer.write_tablet(tablet));
        ASSERT_EQ(common::E_OK, writer.flush());
    }
    ASSERT_EQ(common::E_OK, writer.close());

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(file_name_));
    FileGeneration generation;
    generation.mapped_index_identity = 1;
    generation.file_id = 0;
    struct stat file_stat {};
    ASSERT_EQ(0, stat(file_name_.c_str(), &file_stat));
    generation.file_size = static_cast<uint64_t>(file_stat.st_size);
    generation.file_fingerprint = 0;

    struct Window {
        int64_t start;
        int64_t end;
        int offset;
        int limit;
    };
    const std::vector<Window> windows = {
        {0, 139999, 30, 10},     // Offset inside the first page.
        {0, 139999, 10003, 1},   // Output capacity below the decode batch size.
        {0, 139999, 70003, 7},   // Whole chunk plus a residual.
        {0, 139999, 13, 65537},  // Resumes decoding a large page.
        {0, 139999, 69995, 10},  // Window crossing a chunk boundary.
        {0, 139999, 139990, -1},  // Unlimited tail.
        {0, 139999, 140001, 5},   // Offset beyond the data.
        {99, 150, 5, 7},          // Count only rows passing the time filter.
        {99, 150, 60, 7},         // Offset beyond the filtered rows.
        {9998, 10005, 3, 4},      // Filter and offset crossing a page boundary.
        {0, 139999, 30, 0},
    };
    auto metadata = reader.get_timeseries_metadata();
    ASSERT_EQ(1U, metadata.size());
    ASSERT_EQ(names.size(), metadata.begin()->second.size());
    for (const auto& device_entry : metadata) {
        for (const auto& index : device_entry.second) {
            auto* series_index = dynamic_cast<TimeseriesIndex*>(index.get());
            ASSERT_NE(nullptr, series_index);
            PreparedLocator locator;
            locator.layout = 0;
            locator.value_metadata_offset = series_index->get_metadata_offset();
            locator.value_metadata_length = series_index->get_metadata_length();
            std::shared_ptr<PreparedSeries> prepared;
            ASSERT_EQ(common::E_OK,
                      reader.prepare_series(generation, locator, prepared));
            for (const auto& window : windows) {
                SCOPED_TRACE(index->get_measurement_name().to_std_string());
                SCOPED_TRACE(window.offset);
                ResultSet* result = nullptr;
                ASSERT_EQ(
                    common::E_OK,
                    reader.query_prepared(prepared, window.start, window.end,
                                          window.offset, window.limit, result));
                auto* table = dynamic_cast<TableResultSet*>(result);
                ASSERT_NE(nullptr, table);
                const int available = std::max(
                    0, int(window.end - window.start + 1) - window.offset);
                const int expected_count =
                    window.limit < 0 ? available
                                     : std::min(available, window.limit);
                int count = 0;
                common::TsBlock* block = nullptr;
                int ret = common::E_OK;
                while ((ret = table->get_next_tsblock(block)) == common::E_OK) {
                    common::RowIterator rows(block);
                    while (rows.has_next()) {
                        const int64_t expected =
                            window.start + window.offset + count;
                        uint32_t len = 0;
                        bool is_null = false;
                        const char* time = rows.read(0, &len, &is_null);
                        ASSERT_FALSE(is_null);
                        ASSERT_EQ(expected,
                                  *reinterpret_cast<const int64_t*>(time));
                        const char* value = rows.read(1, &len, &is_null);
                        ASSERT_FALSE(is_null);
                        switch (index->get_data_type()) {
                            case common::BOOLEAN:
                                EXPECT_EQ(
                                    expected % 2 == 0,
                                    *reinterpret_cast<const bool*>(value));
                                break;
                            case common::INT32:
                                EXPECT_EQ(
                                    expected,
                                    *reinterpret_cast<const int32_t*>(value));
                                break;
                            case common::INT64:
                                EXPECT_EQ(
                                    expected,
                                    *reinterpret_cast<const int64_t*>(value));
                                break;
                            case common::FLOAT:
                                EXPECT_FLOAT_EQ(
                                    expected + 0.5f,
                                    *reinterpret_cast<const float*>(value));
                                break;
                            case common::DOUBLE:
                                EXPECT_DOUBLE_EQ(
                                    expected + 0.5,
                                    *reinterpret_cast<const double*>(value));
                                break;
                            case common::STRING:
                                EXPECT_EQ(std::to_string(expected),
                                          std::string(value, len));
                                break;
                            default:
                                FAIL() << "Unexpected type";
                        }
                        ++count;
                        rows.next();
                    }
                }
                EXPECT_EQ(common::E_NO_MORE_DATA, ret);
                EXPECT_EQ(expected_count, count);
                reader.destroy_query_data_set(result);
            }
        }
    }
    EXPECT_EQ(common::E_OK, reader.close());
}

INSTANTIATE_TEST_SUITE_P(PageSizes, NonAlignedPreparedSeriesOffsetTest,
                         ::testing::Values(10000U, 100000U));

TEST_F(PreparedSeriesBatchTest,
       PreparedQueryReturnsDirectTableResultSetBatches) {
    write_nullable_table();

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(file_name_));

    ResultSet* fixture_result = nullptr;
    ASSERT_EQ(common::E_OK, reader.query("weather", {"device", "value"}, 0,
                                         69999, fixture_result, 4096));
    auto* fixture_table = dynamic_cast<TableResultSet*>(fixture_result);
    ASSERT_NE(nullptr, fixture_table);
    uint32_t fixture_row_count = 0;
    common::TsBlock* fixture_block = nullptr;
    while (fixture_table->get_next_tsblock(fixture_block) == common::E_OK) {
        ASSERT_NE(nullptr, fixture_block);
        fixture_row_count += fixture_block->get_row_count();
    }
    EXPECT_EQ(70000U, fixture_row_count);
    reader.destroy_query_data_set(fixture_result);

    auto metadata = reader.get_timeseries_metadata();
    AlignedTimeseriesIndex* aligned = nullptr;
    for (const auto& device_entry : metadata) {
        for (const auto& index : device_entry.second) {
            auto* candidate =
                dynamic_cast<AlignedTimeseriesIndex*>(index.get());
            if (candidate != nullptr && candidate->value_ts_idx_ != nullptr &&
                candidate->value_ts_idx_->get_measurement_name()
                        .to_std_string() == "value") {
                aligned = candidate;
                break;
            }
        }
    }
    ASSERT_NE(nullptr, aligned);
    ASSERT_NE(nullptr, aligned->time_ts_idx_);
    ASSERT_NE(nullptr, aligned->value_ts_idx_);

    FileGeneration generation;
    generation.mapped_index_identity = 1;
    generation.file_id = 0;
    struct stat file_stat {};
    ASSERT_EQ(0, stat(file_name_.c_str(), &file_stat));
    generation.file_size = static_cast<uint64_t>(file_stat.st_size);
    generation.file_fingerprint = 0;

    PreparedLocator locator;
    locator.locator_id = 0;
    locator.layout = 1;
    locator.flags = 1;
    locator.value_metadata_offset =
        aligned->value_ts_idx_->get_metadata_offset();
    locator.value_metadata_length =
        aligned->value_ts_idx_->get_metadata_length();
    locator.time_metadata_offset = aligned->time_ts_idx_->get_metadata_offset();
    locator.time_metadata_length = aligned->time_ts_idx_->get_metadata_length();

    std::shared_ptr<PreparedSeries> prepared;
    ASSERT_EQ(common::E_OK,
              reader.prepare_series(generation, locator, prepared));
    ASSERT_NE(nullptr, prepared);
    auto* prepared_aligned =
        dynamic_cast<AlignedTimeseriesIndex*>(prepared->index());
    ASSERT_NE(nullptr, prepared_aligned);
    ASSERT_NE(nullptr, prepared_aligned->time_ts_idx_);
    ASSERT_NE(nullptr, prepared_aligned->value_ts_idx_);
    EXPECT_EQ(70000,
              prepared_aligned->time_ts_idx_->get_statistic()->get_count());
    EXPECT_EQ(aligned->time_ts_idx_->get_chunk_meta_list()->size(),
              prepared_aligned->time_ts_idx_->get_chunk_meta_list()->size());

    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query_prepared(prepared, 0, 9, 1, 7, result));
    auto* table_result = dynamic_cast<TableResultSet*>(result);
    ASSERT_NE(nullptr, table_result);

    std::vector<int64_t> timestamps;
    std::vector<double> values;
    std::vector<bool> nulls;
    int block_count = 0;
    common::TsBlock* block = nullptr;
    int ret = common::E_OK;
    while ((ret = table_result->get_next_tsblock(block)) == common::E_OK) {
        ASSERT_NE(nullptr, block);
        ++block_count;
        common::RowIterator rows(block);
        while (rows.has_next()) {
            uint32_t len = 0;
            bool is_null = false;
            const char* timestamp = rows.read(0, &len, &is_null);
            ASSERT_FALSE(is_null);
            timestamps.push_back(*reinterpret_cast<const int64_t*>(timestamp));

            const char* value = rows.read(1, &len, &is_null);
            nulls.push_back(is_null);
            values.push_back(is_null ? 0.0
                                     : *reinterpret_cast<const double*>(value));
            rows.next();
        }
    }
    EXPECT_EQ(common::E_NO_MORE_DATA, ret);
    EXPECT_EQ(1, block_count);
    ASSERT_EQ(7U, timestamps.size());
    for (int64_t index = 0; index < 7; ++index) {
        EXPECT_EQ(index + 1, timestamps[index]);
        const bool expected_null = index + 1 == 2 || index + 1 == 6;
        EXPECT_EQ(expected_null, nulls[index]);
        if (!expected_null) {
            EXPECT_DOUBLE_EQ(static_cast<double>(index + 1), values[index]);
        }
    }
    reader.destroy_query_data_set(result);

    ResultSet* multi_batch = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query_prepared(prepared, 0, 69999, 0, 65537, multi_batch));
    auto* multi_batch_table = dynamic_cast<TableResultSet*>(multi_batch);
    ASSERT_NE(nullptr, multi_batch_table);
    uint32_t multi_batch_rows = 0;
    uint32_t multi_batch_count = 0;
    block = nullptr;
    while (multi_batch_table->get_next_tsblock(block) == common::E_OK) {
        ASSERT_NE(nullptr, block);
        ++multi_batch_count;
        multi_batch_rows += block->get_row_count();
    }
    EXPECT_GT(multi_batch_count, 1U);
    EXPECT_EQ(65537U, multi_batch_rows);
    reader.destroy_query_data_set(multi_batch);

    ResultSet* empty = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query_prepared(prepared, 200000, 100000, 0, -1, empty));
    auto* empty_table = dynamic_cast<TableResultSet*>(empty);
    ASSERT_NE(nullptr, empty_table);
    block = nullptr;
    EXPECT_EQ(common::E_NO_MORE_DATA, empty_table->get_next_tsblock(block));
    EXPECT_EQ(nullptr, block);
    reader.destroy_query_data_set(empty);
    EXPECT_EQ(common::E_OK, reader.close());
}

TEST_F(PreparedSeriesBatchTest,
       MultiPreparedQuerySharesAlignedTimeAxisAndPreservesColumnOrder) {
    write_nullable_table();

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(file_name_));
    auto metadata = reader.get_timeseries_metadata();
    AlignedTimeseriesIndex* value_index = nullptr;
    AlignedTimeseriesIndex* value2_index = nullptr;
    for (const auto& device_entry : metadata) {
        for (const auto& index : device_entry.second) {
            auto* aligned = dynamic_cast<AlignedTimeseriesIndex*>(index.get());
            if (aligned == nullptr || aligned->value_ts_idx_ == nullptr) {
                continue;
            }
            const std::string name =
                aligned->value_ts_idx_->get_measurement_name().to_std_string();
            if (name == "value") {
                value_index = aligned;
            } else if (name == "value2") {
                value2_index = aligned;
            }
        }
    }
    ASSERT_NE(nullptr, value_index);
    ASSERT_NE(nullptr, value2_index);

    FileGeneration generation;
    generation.mapped_index_identity = 1;
    generation.file_id = 0;
    struct stat file_stat {};
    ASSERT_EQ(0, stat(file_name_.c_str(), &file_stat));
    generation.file_size = static_cast<uint64_t>(file_stat.st_size);

    auto prepare = [&](uint32_t locator_id, AlignedTimeseriesIndex* aligned,
                       const std::shared_ptr<PreparedSeries>& time_owner) {
        PreparedLocator locator;
        locator.locator_id = locator_id;
        locator.layout = 1;
        locator.flags = 1;
        locator.value_metadata_offset =
            aligned->value_ts_idx_->get_metadata_offset();
        locator.value_metadata_length =
            aligned->value_ts_idx_->get_metadata_length();
        locator.time_metadata_offset =
            aligned->time_ts_idx_->get_metadata_offset();
        locator.time_metadata_length =
            aligned->time_ts_idx_->get_metadata_length();
        std::shared_ptr<PreparedSeries> result;
        EXPECT_EQ(common::E_OK,
                  time_owner == nullptr
                      ? reader.prepare_series(generation, locator, result)
                      : reader.prepare_series(generation, locator, time_owner,
                                              result));
        return result;
    };

    std::shared_ptr<PreparedSeries> prepared_value =
        prepare(0, value_index, nullptr);
    std::shared_ptr<PreparedSeries> prepared_value2 =
        prepare(1, value2_index, prepared_value);
    ASSERT_NE(nullptr, prepared_value);
    ASSERT_NE(nullptr, prepared_value2);
    auto* first_aligned =
        dynamic_cast<AlignedTimeseriesIndex*>(prepared_value->index());
    auto* second_aligned =
        dynamic_cast<AlignedTimeseriesIndex*>(prepared_value2->index());
    ASSERT_NE(nullptr, first_aligned);
    ASSERT_NE(nullptr, second_aligned);
    EXPECT_EQ(first_aligned->time_ts_idx_, second_aligned->time_ts_idx_);

    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query_prepared_multi({prepared_value2, prepared_value}, 0,
                                          9, 0, -1, result));
    auto* table_result = dynamic_cast<TableResultSet*>(result);
    ASSERT_NE(nullptr, table_result);
    auto result_metadata = table_result->get_metadata();
    ASSERT_NE(nullptr, result_metadata);
    EXPECT_EQ("time", result_metadata->get_column_name(1));
    EXPECT_EQ("value2", result_metadata->get_column_name(2));
    EXPECT_EQ("value", result_metadata->get_column_name(3));

    uint32_t row = 0;
    common::TsBlock* block = nullptr;
    while (table_result->get_next_tsblock(block) == common::E_OK) {
        ASSERT_NE(nullptr, block);
        common::RowIterator rows(block);
        while (rows.has_next()) {
            uint32_t len = 0;
            bool is_null = false;
            const char* timestamp = rows.read(0, &len, &is_null);
            ASSERT_FALSE(is_null);
            ASSERT_EQ(row, *reinterpret_cast<const int64_t*>(timestamp));

            const char* value2 = rows.read(1, &len, &is_null);
            EXPECT_EQ(row == 4 || row == 8, is_null);
            if (!is_null) {
                EXPECT_DOUBLE_EQ(static_cast<double>(row * 10),
                                 *reinterpret_cast<const double*>(value2));
            }

            const char* value = rows.read(2, &len, &is_null);
            EXPECT_EQ(row == 2 || row == 6, is_null);
            if (!is_null) {
                EXPECT_DOUBLE_EQ(static_cast<double>(row),
                                 *reinterpret_cast<const double*>(value));
            }
            ++row;
            rows.next();
        }
    }
    EXPECT_EQ(10U, row);
    reader.destroy_query_data_set(result);

    ResultSet* empty_multi = nullptr;
    ASSERT_EQ(common::E_OK,
              reader.query_prepared_multi({prepared_value2, prepared_value}, 9,
                                          0, 0, -1, empty_multi));
    auto* empty_multi_table = dynamic_cast<TableResultSet*>(empty_multi);
    ASSERT_NE(nullptr, empty_multi_table);
    block = nullptr;
    EXPECT_EQ(common::E_NO_MORE_DATA,
              empty_multi_table->get_next_tsblock(block));
    EXPECT_EQ(nullptr, block);
    reader.destroy_query_data_set(empty_multi);
    EXPECT_EQ(common::E_OK, reader.close());
}

}  // namespace
}  // namespace storage
