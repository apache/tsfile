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

#include <chrono>
#include <random>
#include <vector>

#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/tsfile_io_writer.h"
#include "file/write_file.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/chunk_writer.h"
#include "writer/tsfile_table_writer.h"

using namespace storage;
using namespace common;

class TsFileTableReaderBatchTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("tsfile_reader_table_batch_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        mode_t mode = 0666;
        write_file_.create(file_name_, flags, mode);
    }
    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }
    std::string file_name_;
    WriteFile write_file_;

   public:
    static std::string generate_random_string(int length) {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, 61);

        const std::string chars =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        std::string random_string;

        for (int i = 0; i < length; ++i) {
            random_string += chars[dis(gen)];
        }

        return random_string;
    }

    static TableSchema* gen_table_schema_no_tag() {
        // Generate table schema with only FIELD columns (no TAG columns)
        std::vector<MeasurementSchema*> measurement_schemas;
        std::vector<ColumnCategory> column_categories;
        int measurement_schema_num = 5;  // 5 field columns
        for (int i = 0; i < measurement_schema_num; i++) {
            measurement_schemas.emplace_back(new MeasurementSchema(
                "s" + std::to_string(i), TSDataType::INT64, TSEncoding::PLAIN,
                CompressionType::UNCOMPRESSED));
            column_categories.emplace_back(ColumnCategory::FIELD);
        }
        return new TableSchema("testTableNoTag", measurement_schemas,
                               column_categories);
    }

    static storage::Tablet gen_tablet_no_tag(TableSchema* table_schema,
                                             int num_rows) {
        // Generate tablet without tags (only field columns)
        storage::Tablet tablet(table_schema->get_table_name(),
                               table_schema->get_measurement_names(),
                               table_schema->get_data_types(),
                               table_schema->get_column_categories(), num_rows);

        for (int i = 0; i < num_rows; i++) {
            tablet.add_timestamp(i, i);
            auto column_schemas = table_schema->get_measurement_schemas();
            for (const auto& column_schema : column_schemas) {
                if (column_schema->data_type_ == TSDataType::INT64) {
                    tablet.add_value(i, column_schema->measurement_name_,
                                     static_cast<int64_t>(i));
                }
            }
        }
        return tablet;
    }

    static TableSchema* gen_table_schema() {
        std::vector<MeasurementSchema*> measurement_schemas;
        std::vector<ColumnCategory> column_categories;
        int id_schema_num = 2;
        int measurement_schema_num = 3;
        for (int i = 0; i < id_schema_num; i++) {
            measurement_schemas.emplace_back(new MeasurementSchema(
                "id" + std::to_string(i), TSDataType::STRING, TSEncoding::PLAIN,
                CompressionType::UNCOMPRESSED));
            column_categories.emplace_back(ColumnCategory::TAG);
        }
        for (int i = 0; i < measurement_schema_num; i++) {
            measurement_schemas.emplace_back(new MeasurementSchema(
                "s" + std::to_string(i), TSDataType::INT64, TSEncoding::PLAIN,
                CompressionType::UNCOMPRESSED));
            column_categories.emplace_back(ColumnCategory::FIELD);
        }
        return new TableSchema("testTable", measurement_schemas,
                               column_categories);
    }

    static storage::Tablet gen_tablet(TableSchema* table_schema, int offset,
                                      int device_num,
                                      int num_timestamp_per_device = 10) {
        storage::Tablet tablet(table_schema->get_table_name(),
                               table_schema->get_measurement_names(),
                               table_schema->get_data_types(),
                               table_schema->get_column_categories(),
                               device_num * num_timestamp_per_device);

        char* literal = new char[std::strlen("device_id") + 1];
        std::strcpy(literal, "device_id");
        String literal_str(literal, std::strlen("device_id"));
        for (int i = 0; i < device_num; i++) {
            for (int l = 0; l < num_timestamp_per_device; l++) {
                int row_index = i * num_timestamp_per_device + l;
                tablet.add_timestamp(row_index, row_index);
                auto column_schemas = table_schema->get_measurement_schemas();
                for (const auto& column_schema : column_schemas) {
                    switch (column_schema->data_type_) {
                        case TSDataType::INT64:
                            tablet.add_value(row_index,
                                             column_schema->measurement_name_,
                                             static_cast<int64_t>(i));
                            break;
                        case TSDataType::STRING:
                            tablet.add_value(row_index,
                                             column_schema->measurement_name_,
                                             literal_str);
                            break;
                        default:
                            break;
                    }
                }
            }
        }
        delete[] literal;
        return tablet;
    }
};

TEST_F(TsFileTableReaderBatchTest, BatchQueryWithSmallBatchSize) {
    auto table_schema = gen_table_schema();
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);

    const int device_num = 2;
    const int points_per_device = 50;
    auto tablet = gen_tablet(table_schema, 0, device_num, points_per_device);
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);

    ResultSet* tmp_result_set = nullptr;
    const int batch_size = 20;
    ret = reader.query(table_schema->get_table_name(),
                       table_schema->get_measurement_names(), 0, 1000000000000,
                       tmp_result_set, batch_size);
    ASSERT_EQ(ret, common::E_OK);
    ASSERT_NE(tmp_result_set, nullptr);

    auto* table_result_set = dynamic_cast<TableResultSet*>(tmp_result_set);
    ASSERT_NE(table_result_set, nullptr);

    int total_rows = 0;
    int block_count = 0;
    common::TsBlock* block = nullptr;

    char* literal = new char[std::strlen("device_id") + 1];
    std::strcpy(literal, "device_id");
    String expected_string(literal, std::strlen("device_id"));
    std::vector<int64_t> int64_sums(3, 0);
    while ((ret = table_result_set->get_next_tsblock(block)) == common::E_OK) {
        ASSERT_NE(block, nullptr);
        block_count++;
        uint32_t row_count = block->get_row_count();
        total_rows += row_count;
        ASSERT_EQ(row_count, batch_size);

        common::RowIterator row_iterator(block);
        while (row_iterator.has_next()) {
            uint32_t len;
            bool null;

            int int64_col_idx = 0;
            for (uint32_t col_idx = 1;
                 col_idx < row_iterator.get_column_count(); ++col_idx) {
                const char* value = row_iterator.read(col_idx, &len, &null);
                ASSERT_FALSE(null);
                TSDataType data_type = row_iterator.get_data_type(col_idx);
                if (data_type == TSDataType::INT64) {
                    int64_t int_val = *reinterpret_cast<const int64_t*>(value);
                    int64_sums[int64_col_idx] += int_val;
                    int64_col_idx++;
                } else if (data_type == TSDataType::STRING) {
                    String str_value(value, len);
                    ASSERT_EQ(str_value.compare(expected_string), 0);
                }
            }
            row_iterator.next();
        }
    }
    EXPECT_EQ(total_rows, device_num * points_per_device);
    EXPECT_GT(block_count, 1);
    for (size_t i = 0; i < int64_sums.size(); i++) {
        EXPECT_EQ(int64_sums[i], 50);
    }

    delete[] literal;

    reader.destroy_query_data_set(table_result_set);
    ASSERT_EQ(reader.close(), common::E_OK);
    delete table_schema;
}

TEST_F(TsFileTableReaderBatchTest, BatchQueryWithLargeBatchSize) {
    auto table_schema = gen_table_schema();
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);

    const int device_num = 1;
    const int points_per_device = 120;
    auto tablet = gen_tablet(table_schema, 0, device_num, points_per_device);
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);

    ResultSet* tmp_result_set = nullptr;
    const int batch_size = 100;
    ret = reader.query(table_schema->get_table_name(),
                       table_schema->get_measurement_names(), 0, 1000000000000,
                       tmp_result_set, batch_size);
    ASSERT_EQ(ret, common::E_OK);
    ASSERT_NE(tmp_result_set, nullptr);

    auto* table_result_set = dynamic_cast<TableResultSet*>(tmp_result_set);
    ASSERT_NE(table_result_set, nullptr);

    int total_rows = 0;
    int block_count = 0;
    common::TsBlock* block = nullptr;

    while (table_result_set->get_next_tsblock(block) == common::E_OK) {
        ASSERT_NE(block, nullptr);
        block_count++;
        uint32_t row_count = block->get_row_count();
        total_rows += row_count;

        ASSERT_EQ(row_count, block_count == 1 ? batch_size : 20);
    }

    EXPECT_EQ(total_rows, device_num * points_per_device);
    EXPECT_GE(block_count, 2);

    reader.destroy_query_data_set(table_result_set);
    ASSERT_EQ(reader.close(), common::E_OK);
    delete table_schema;
}

TEST_F(TsFileTableReaderBatchTest, BatchQueryVerifyDataCorrectness) {
    auto table_schema = gen_table_schema();
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);

    const int device_num = 1;
    const int points_per_device = 30;
    auto tablet = gen_tablet(table_schema, 0, device_num, points_per_device);
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);

    ResultSet* tmp_result_set = nullptr;
    const int batch_size = 10;
    ret = reader.query(table_schema->get_table_name(),
                       table_schema->get_measurement_names(), 0, 1000000000000,
                       tmp_result_set, batch_size);
    ASSERT_EQ(ret, common::E_OK);
    ASSERT_NE(tmp_result_set, nullptr);

    auto* table_result_set = dynamic_cast<TableResultSet*>(tmp_result_set);
    ASSERT_NE(table_result_set, nullptr);

    int expected_timestamp = 0;
    common::TsBlock* block = nullptr;

    while (table_result_set->get_next_tsblock(block) == common::E_OK) {
        ASSERT_NE(block, nullptr);

        common::RowIterator row_iterator(block);
        while (row_iterator.has_next()) {
            uint32_t len;
            bool null;
            int64_t timestamp = *reinterpret_cast<const int64_t*>(
                row_iterator.read(0, &len, &null));
            ASSERT_FALSE(null);
            EXPECT_EQ(timestamp, expected_timestamp);

            for (uint32_t col_idx = 2;
                 col_idx < row_iterator.get_column_count(); ++col_idx) {
                const char* value = row_iterator.read(col_idx, &len, &null);
                if (!null && row_iterator.get_data_type(col_idx) == INT64) {
                    int64_t int_val = *reinterpret_cast<const int64_t*>(value);
                    EXPECT_EQ(int_val, 0);
                }
            }
            row_iterator.next();
            expected_timestamp++;
        }
    }

    EXPECT_EQ(expected_timestamp, device_num * points_per_device);

    reader.destroy_query_data_set(table_result_set);
    ASSERT_EQ(reader.close(), common::E_OK);
    delete table_schema;
}

TEST_F(TsFileTableReaderBatchTest, PerformanceComparisonSinglePointVsBatch) {
    // Create table schema without tags (only fields)
    auto table_schema = gen_table_schema_no_tag();
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);

    // Write a large amount of data
    const int total_rows = 1000000;
    auto tablet = gen_tablet_no_tag(table_schema, total_rows);
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);

    // Test 1: Single point query (using next() method)
    {
        storage::TsFileReader reader;
        int ret = reader.open(file_name_);
        ASSERT_EQ(ret, common::E_OK);

        ResultSet* tmp_result_set = nullptr;
        // Single point query: don't specify batch_size (or use 0)
        auto start_time = std::chrono::high_resolution_clock::now();

        ret = reader.query(table_schema->get_table_name(),
                           table_schema->get_measurement_names(), 0,
                           1000000000000, tmp_result_set);
        ASSERT_EQ(ret, common::E_OK);
        ASSERT_NE(tmp_result_set, nullptr);

        auto* table_result_set = dynamic_cast<TableResultSet*>(tmp_result_set);
        ASSERT_NE(table_result_set, nullptr);

        int total_rows_read = 0;
        bool has_next = false;

        // Use next() method for single point query
        while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
            total_rows_read++;
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        EXPECT_EQ(total_rows_read, total_rows);
        std::cout << "\n=== Single Point Query (using next() method) ==="
                  << std::endl;
        std::cout << "Total rows read: " << total_rows_read << std::endl;
        std::cout << "Time taken: " << duration.count() << " ms" << std::endl;
        std::cout << "Throughput: "
                  << (total_rows_read * 5 * 1000.0 / duration.count())
                  << " rows/sec" << std::endl;

        reader.destroy_query_data_set(table_result_set);
        ASSERT_EQ(reader.close(), common::E_OK);
    }

    // Test 2: Batch query (batch_size = 1000)
    {
        storage::TsFileReader reader;
        int ret = reader.open(file_name_);
        ASSERT_EQ(ret, common::E_OK);

        ResultSet* tmp_result_set = nullptr;
        const int batch_size = 10000;  // Batch query
        auto start_time = std::chrono::high_resolution_clock::now();

        ret = reader.query(table_schema->get_table_name(),
                           table_schema->get_measurement_names(), 0,
                           1000000000000, tmp_result_set, batch_size);
        ASSERT_EQ(ret, common::E_OK);
        ASSERT_NE(tmp_result_set, nullptr);

        auto* table_result_set = dynamic_cast<TableResultSet*>(tmp_result_set);
        ASSERT_NE(table_result_set, nullptr);

        int total_rows_read = 0;
        common::TsBlock* block = nullptr;
        int block_count = 0;

        while ((ret = table_result_set->get_next_tsblock(block)) ==
               common::E_OK) {
            ASSERT_NE(block, nullptr);
            block_count++;
            total_rows_read += block->get_row_count();
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time);

        EXPECT_EQ(total_rows_read, total_rows);
        std::cout << "\n=== Batch Query (batch_size=10000) ===" << std::endl;
        std::cout << "Total rows read: " << total_rows_read << std::endl;
        std::cout << "Block count: " << block_count << std::endl;
        std::cout << "Time taken: " << duration.count() << " ms" << std::endl;
        std::cout << "Throughput: "
                  << (total_rows_read * 5 * 1000.0 / duration.count())
                  << " rows/sec" << std::endl;

        reader.destroy_query_data_set(table_result_set);
        ASSERT_EQ(reader.close(), common::E_OK);
    }

    delete table_schema;
}
