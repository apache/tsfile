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

#include <random>

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

class TsFileTableReaderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("tsfile_writer_table_test_") +
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
        write_file_.sync();
        write_file_.close();
        remove(file_name_.c_str());
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

    static TableSchema* gen_table_schema(int table_num) {
        std::vector<MeasurementSchema*> measurement_schemas;
        std::vector<ColumnCategory> column_categories;
        int id_schema_num = 5;
        int measurement_schema_num = 5;
        for (int i = 0; i < id_schema_num; i++) {
            measurement_schemas.emplace_back(new MeasurementSchema(
                "id" + to_string(i), TSDataType::STRING, TSEncoding::PLAIN,
                CompressionType::UNCOMPRESSED));
            column_categories.emplace_back(ColumnCategory::TAG);
        }
        for (int i = 0; i < measurement_schema_num; i++) {
            measurement_schemas.emplace_back(new MeasurementSchema(
                "s" + to_string(i), TSDataType::INT64, TSEncoding::PLAIN,
                CompressionType::UNCOMPRESSED));
            column_categories.emplace_back(ColumnCategory::FIELD);
        }
        return new TableSchema("testTable" + to_string(table_num),
                               measurement_schemas, column_categories);
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
                tablet.add_timestamp(row_index, offset + l);
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

    void test_table_model_query(uint32_t points_per_device = 10, uint32_t device_num = 1) {
        auto table_schema = gen_table_schema(0);
        auto tsfile_table_writer_ =
            std::make_shared<TsFileTableWriter>(&write_file_, table_schema);

        auto tablet = gen_tablet(table_schema, 0, device_num, points_per_device);
        ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
        ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
        ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);
        storage::TsFileReader reader;
        int ret = reader.open(file_name_);
        ASSERT_EQ(ret, common::E_OK);

        ResultSet* tmp_result_set = nullptr;
        ret = reader.query(table_schema->get_table_name(),
                           table_schema->get_measurement_names(), 0,
                           1000000000000, tmp_result_set);
        auto* table_result_set = (TableResultSet*)tmp_result_set;
        char* literal = new char[std::strlen("device_id") + 1];
        std::strcpy(literal, "device_id");
        String literal_str(literal, std::strlen("device_id"));
        bool has_next = false;
        int64_t row_num = 0;
        while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
            auto column_schemas = table_schema->get_measurement_schemas();
            for (const auto& column_schema : column_schemas) {
                switch (column_schema->data_type_) {
                    case TSDataType::INT64:
                        ASSERT_EQ(table_result_set->get_value<int64_t>(
                                      column_schema->measurement_name_),
                                  (row_num / points_per_device) % device_num);
                        break;
                    case TSDataType::STRING:
                        ASSERT_EQ(table_result_set
                                      ->get_value<common::String*>(
                                          column_schema->measurement_name_)
                                      ->compare(literal_str),
                                  0);
                        break;
                    default:
                        break;
                }
            }
            for (int i = 2; i <= 6; i++) {
                ASSERT_EQ(
                    table_result_set->get_value<common::String*>(i)->compare(
                        literal_str),
                    0);
            }
            for (int i = 7; i <= 11; i++) {
                ASSERT_EQ(table_result_set->get_value<int64_t>(i),  (row_num / points_per_device) % device_num);
            }
            ASSERT_EQ(table_result_set->get_value<int64_t>(1), row_num % points_per_device);
            row_num++;
        }
        ASSERT_EQ(row_num, points_per_device * device_num);
        reader.destroy_query_data_set(table_result_set);
        delete[] literal;
        ASSERT_EQ(reader.close(), common::E_OK);
        delete table_schema;
    }
};

TEST_F(TsFileTableReaderTest, TableModelQuery) { test_table_model_query(); }

TEST_F(TsFileTableReaderTest, TableModelQueryOneSmallPage) {
    int prev_config = g_config_value_.page_writer_max_point_num_;
    g_config_value_.page_writer_max_point_num_ = 5;
    test_table_model_query(g_config_value_.page_writer_max_point_num_);
    g_config_value_.page_writer_max_point_num_ = prev_config;
}

TEST_F(TsFileTableReaderTest, TableModelQueryOneLargePage) {
    int prev_config = g_config_value_.page_writer_max_point_num_;
    g_config_value_.page_writer_max_point_num_ = 10000;
    test_table_model_query(g_config_value_.page_writer_max_point_num_);
    g_config_value_.page_writer_max_point_num_ = prev_config;
}

TEST_F(TsFileTableReaderTest, TableModelQueryMultiLargePage) {
    int prev_config = g_config_value_.page_writer_max_point_num_;
    g_config_value_.page_writer_max_point_num_ = 10000;
    test_table_model_query(1000000);
    g_config_value_.page_writer_max_point_num_ = prev_config;
}

TEST_F(TsFileTableReaderTest, TableModelQueryMultiDevices) {
    int prev_config = g_config_value_.page_writer_max_point_num_;
    g_config_value_.page_writer_max_point_num_ = 10000;
    test_table_model_query(g_config_value_.page_writer_max_point_num_, 10);
    g_config_value_.page_writer_max_point_num_ = prev_config;
}

TEST_F(TsFileTableReaderTest, TableModelResultMetadata) {
    auto table_schema = gen_table_schema(0);
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);

    auto tablet = gen_tablet(table_schema, 0, 1);
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);
    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);

    ResultSet* tmp_result_set = nullptr;
    ret = reader.query(table_schema->get_table_name(),
                       table_schema->get_measurement_names(), 0, 1000000000000,
                       tmp_result_set);
    auto* table_result_set = (TableResultSet*)tmp_result_set;
    auto result_set_metadata = table_result_set->get_metadata();
    ASSERT_EQ(result_set_metadata->get_column_count(), 11);
    ASSERT_EQ(result_set_metadata->get_column_name(1), "time");
    ASSERT_EQ(result_set_metadata->get_column_type(1), INT64);
    for (int i = 2; i <= 6; i++) {
        ASSERT_EQ(result_set_metadata->get_column_name(i),
                  "id" + to_string(i - 2));
        ASSERT_EQ(result_set_metadata->get_column_type(i), TSDataType::STRING);
    }
    for (int i = 7; i <= 11; i++) {
        ASSERT_EQ(result_set_metadata->get_column_name(i),
                  "s" + to_string(i - 7));
        ASSERT_EQ(result_set_metadata->get_column_type(i), TSDataType::INT64);
    }
    reader.destroy_query_data_set(table_result_set);
    ASSERT_EQ(reader.close(), common::E_OK);
    delete table_schema;
}

TEST_F(TsFileTableReaderTest, TableModelGetSchema) {
    auto tmp_table_schema = gen_table_schema(0);
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, tmp_table_schema);
    auto tmp_tablet = gen_tablet(tmp_table_schema, 0, 1);
    ASSERT_EQ(tsfile_table_writer_->write_table(tmp_tablet), common::E_OK);
    for (int i = 1; i < 10; i++) {
        auto table_schema = gen_table_schema(i);
        auto tablet = gen_tablet(table_schema, 0, 1);
        auto table_schema_ptr = std::shared_ptr<TableSchema>(table_schema);
        tsfile_table_writer_->register_table(table_schema_ptr);
        ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
    }
    ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);
    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);

    auto table_schemas = reader.get_all_table_schemas();
    std::sort(table_schemas.begin(), table_schemas.end(),
              [](const std::shared_ptr<TableSchema>& a,
                 const std::shared_ptr<TableSchema>& b) {
                  return a->get_table_name() < b->get_table_name();
              });  // The table_schema returned is not guaranteed to be sorted
                   // by table_name
    ASSERT_EQ(table_schemas.size(), 10);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(table_schemas[i]->get_table_name(),
                  "testtable" + to_string(i));
        for (int j = 0; j < 5; j++) {
            ASSERT_EQ(table_schemas[i]->get_data_types()[j],
                      TSDataType::STRING);
            ASSERT_EQ(table_schemas[i]->get_column_categories()[j],
                      ColumnCategory::TAG);
        }
        for (int j = 5; j < 10; j++) {
            ASSERT_EQ(table_schemas[i]->get_data_types()[j], TSDataType::INT64);
            ASSERT_EQ(table_schemas[i]->get_column_categories()[j],
                      ColumnCategory::FIELD);
        }
    }

    auto table_schema = reader.get_table_schema("testtable0");
    ASSERT_EQ(table_schema->get_table_name(), "testtable0");
    for (int i = 0; i < 5; i++) {
        ASSERT_EQ(table_schema->get_data_types()[i], TSDataType::STRING);
        ASSERT_EQ(table_schema->get_column_categories()[i],
                  ColumnCategory::TAG);
    }
    for (int i = 5; i < 10; i++) {
        ASSERT_EQ(table_schema->get_data_types()[i], TSDataType::INT64);
        ASSERT_EQ(table_schema->get_column_categories()[i],
                  ColumnCategory::FIELD);
    }

    ASSERT_EQ(reader.close(), common::E_OK);
    delete tmp_table_schema;
}
