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
#include "reader/tsfile_reader.h"
#include "writer/chunk_writer.h"
#include "writer/tsfile_table_writer.h"
using namespace storage;
using namespace common;

class TsFileWriterTableTest : public ::testing::Test {
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
                                      int device_num) {
        storage::Tablet tablet(table_schema->get_measurement_names(),
                               table_schema->get_data_types());

        int num_timestamp_per_device = 10;
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
};

TEST_F(TsFileWriterTableTest, WriteTableTest) {
    auto table_schema = gen_table_schema(0);
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    auto tablet = gen_tablet(table_schema, 0, 1);
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, WriteNonExistColumnTest) {
    auto table_schema = gen_table_schema(0);
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);

    auto measurment_schemas = table_schema->get_measurement_schemas();
    auto column_categories = table_schema->get_column_categories();
    measurment_schemas.emplace_back(
        std::make_shared<MeasurementSchema>("non_exist", TSDataType::INT64));
    column_categories.emplace_back(ColumnCategory::FIELD);
    std::vector<ColumnSchema> column_schemas;
    for (size_t i = 0; i < measurment_schemas.size(); ++i) {
        column_schemas.emplace_back(measurment_schemas[i]->measurement_name_,
                                    measurment_schemas[i]->data_type_,
                                    measurment_schemas[i]->compression_type_,
                                    measurment_schemas[i]->encoding_,
                                    column_categories[i]);
    }
    auto write_table_schema =
        TableSchema(table_schema->get_table_name(), column_schemas);

    auto tablet = gen_tablet(&write_table_schema, 0, 1);
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet),
              common::E_COLUMN_NOT_EXIST);
    tsfile_table_writer_->close();
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, WriteNonExistTableTest) {
    auto table_schema = gen_table_schema(0);
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    auto tablet = gen_tablet(table_schema, 0, 1);
    tablet.set_table_name("non_exist");
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet),
              common::E_TABLE_NOT_EXIST);
    tsfile_table_writer_->close();
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, DISABLED_WriteAndReadSimple) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    measurement_schemas.resize(2);
    measurement_schemas[0] = new MeasurementSchema("device", STRING);
    measurement_schemas[1] = new MeasurementSchema("value", DOUBLE);
    column_categories.emplace_back(ColumnCategory::TAG);
    column_categories.emplace_back(ColumnCategory::FIELD);
    TableSchema* table_schema =
        new TableSchema("test_table", measurement_schemas, column_categories);
    auto tsfile_table_writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    Tablet tablet = Tablet(table_schema->get_measurement_names(),
                           table_schema->get_data_types());
    tablet.set_table_name("test_table");
    for (int i = 0; i < 100; i++) {
        tablet.add_timestamp(i, static_cast<int64_t>(i));
        tablet.add_value(i, "device",
                         std::string("device" + std::to_string(i)).c_str());
        tablet.add_value(i, "value", i * 1.1);
    }
    tsfile_table_writer->write_table(tablet);
    tsfile_table_writer->flush();
    tsfile_table_writer->close();

    TsFileReader reader = TsFileReader();
    reader.open(write_file_.get_file_path());
    ResultSet* ret = nullptr;
    int ret_value =
        reader.query("test_table", {"device", "value"}, 10, 50, ret);

    auto* table_result_set = (TableResultSet*)ret;
    bool has_next = false;
    // There may be error in AlignedChunkReader::read_from_file_and_rewrap.
    // read_from_file_and_rewrap::read_file_->read 308 may get E_FILE_READ_ERR.
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        std::cout << table_result_set->get_value<double>("value");
        std::cout << table_result_set->get_value<common::String*>("device");
    }
    reader.destroy_query_data_set(table_result_set);
    reader.close();
    delete table_schema;
}