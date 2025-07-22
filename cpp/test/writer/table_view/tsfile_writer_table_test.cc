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
    void TearDown() override { remove(file_name_.c_str()); }
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

    static TableSchema* gen_table_schema(int table_num, int id_col_num = 5,
                                         int field_col_num = 5) {
        std::vector<MeasurementSchema*> measurement_schemas;
        std::vector<ColumnCategory> column_categories;
        int id_schema_num = id_col_num;
        int measurement_schema_num = field_col_num;
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
        return new TableSchema("testTable" + std::to_string(table_num),
                               measurement_schemas, column_categories);
    }

    static storage::Tablet gen_tablet(TableSchema* table_schema, int offset,
                                      int device_num,
                                      int num_timestamp_per_device = 10) {
        storage::Tablet tablet(table_schema->get_measurement_names(),
                               table_schema->get_data_types(),
                               device_num * num_timestamp_per_device);
        static int timestamp = 0;
        for (int i = 0; i < device_num; i++) {
            PageArena pa;
            pa.init(512, MOD_DEFAULT);
            std::string device_str =
                std::string("device_id_") + std::to_string(i);
            String literal_str(device_str, pa);
            for (int l = 0; l < num_timestamp_per_device; l++) {
                int row_index = i * num_timestamp_per_device + l;
                tablet.add_timestamp(row_index, timestamp++);
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

TEST_F(TsFileWriterTableTest, WithoutTagAndMultiPage) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    measurement_schemas.resize(1);
    measurement_schemas[0] = new MeasurementSchema("value", DOUBLE);
    column_categories.emplace_back(ColumnCategory::FIELD);
    TableSchema* table_schema =
        new TableSchema("test_table", measurement_schemas, column_categories);
    auto tsfile_table_writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);

    int cur_line = 0;
    for (int j = 0; j < 100; j++) {
        Tablet tablet = Tablet(table_schema->get_measurement_names(),
                               table_schema->get_data_types(), 10001);
        tablet.set_table_name("test_table");
        for (int i = 0; i < 10001; i++) {
            tablet.add_timestamp(i, static_cast<int64_t>(cur_line++));
            tablet.add_value(i, "value", i * 1.1);
        }
        tsfile_table_writer->write_table(tablet);
    }

    tsfile_table_writer->flush();
    tsfile_table_writer->close();

    TsFileReader reader = TsFileReader();
    reader.open(write_file_.get_file_path());
    ResultSet* ret = nullptr;
    int ret_value = reader.query("test_table", {"value"}, 0, 50, ret);
    ASSERT_EQ(common::E_OK, ret_value);
    auto* table_result_set = (TableResultSet*)ret;
    bool has_next = false;
    cur_line = 0;
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        cur_line++;
        int64_t timestamp = table_result_set->get_value<int64_t>("time");
        ASSERT_EQ(table_result_set->get_value<double>("value"),
                  timestamp * 1.1);
    }
    ASSERT_EQ(cur_line, 51);
    table_result_set->close();
    reader.destroy_query_data_set(table_result_set);

    reader.close();
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, WriteDisorderTest) {
    auto table_schema = gen_table_schema(0);
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);

    int device_num = 1;
    int num_timestamp_per_device = 10;
    int offset = 0;
    storage::Tablet tablet(table_schema->get_measurement_names(),
                           table_schema->get_data_types(),
                           device_num * num_timestamp_per_device);

    char* literal = new char[std::strlen("device_id") + 1];
    std::strcpy(literal, "device_id");
    String literal_str(literal, std::strlen("device_id"));
    for (int i = 0; i < device_num; i++) {
        for (int l = 0; l < num_timestamp_per_device; l++) {
            int row_index = i * num_timestamp_per_device + l;
            // disordered timestamp.
            tablet.add_timestamp(row_index, l > num_timestamp_per_device / 2
                                                ? l - num_timestamp_per_device
                                                : offset + l);
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

    ASSERT_EQ(tsfile_table_writer_->write_table(tablet),
              common::E_OUT_OF_ORDER);
    ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, WriteTableTestMultiFlush) {
    auto table_schema = gen_table_schema(0);
    auto tsfile_table_writer_ = std::make_shared<TsFileTableWriter>(
        &write_file_, table_schema, 2 * 1024);
    for (int i = 0; i < 100; i++) {
        auto tablet = gen_tablet(table_schema, i * 10000, 1, 10000);
        ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
    }
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

TEST_F(TsFileWriterTableTest, WriterWithMemoryThreshold) {
    auto table_schema = gen_table_schema(0);
    auto tsfile_table_writer_ = std::make_shared<TsFileTableWriter>(
        &write_file_, table_schema, 256 * 1024 * 1024);
    ASSERT_EQ(common::g_config_value_.chunk_group_size_threshold_,
              256 * 1024 * 1024);
    tsfile_table_writer_->close();
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, EmptyTagWrite) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    measurement_schemas.resize(3);
    measurement_schemas[0] = new MeasurementSchema("device1", STRING);
    measurement_schemas[1] = new MeasurementSchema("device2", STRING);
    measurement_schemas[2] = new MeasurementSchema("value", DOUBLE);
    column_categories.emplace_back(ColumnCategory::TAG);
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
        tablet.add_value(i, "device1",
                         std::string("device" + std::to_string(i)).c_str());
        tablet.add_value(i, "device2", "");
        tablet.add_value(i, "value", i * 1.1);
    }
    tsfile_table_writer->write_table(tablet);
    tsfile_table_writer->flush();
    tsfile_table_writer->close();

    TsFileReader reader = TsFileReader();
    reader.open(write_file_.get_file_path());
    ResultSet* ret = nullptr;
    int ret_value =
        reader.query("test_table", {"device1", "device2", "value"}, 0, 50, ret);
    ASSERT_EQ(common::E_OK, ret_value);

    ASSERT_EQ(ret_value, 0);
    auto* table_result_set = (TableResultSet*)ret;
    bool has_next = false;
    int cur_line = 0;
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        cur_line++;
        int64_t timestamp = table_result_set->get_value<int64_t>("time");
        ASSERT_EQ(table_result_set->get_value<common::String*>("device1")
                      ->to_std_string(),
                  "device" + std::to_string(timestamp));
        ASSERT_EQ(table_result_set->get_value<double>("value"),
                  timestamp * 1.1);
    }
    ASSERT_EQ(cur_line, 51);
    table_result_set->close();
    reader.destroy_query_data_set(table_result_set);

    reader.close();
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, WritehDataTypeMisMatch) {
    auto table_schema = gen_table_schema(0);
    auto tsfile_table_writer_ = std::make_shared<TsFileTableWriter>(
        &write_file_, table_schema, 256 * 1024 * 1024);
    int device_num = 3;
    int num_timestamp_per_device = 10;
    int offset = 0;
    auto datatypes = table_schema->get_data_types();

    datatypes[6] = TSDataType::INT32;
    storage::Tablet tablet(table_schema->get_measurement_names(), datatypes,
                           device_num * num_timestamp_per_device);

    char* literal = new char[std::strlen("device_id") + 1];
    std::strcpy(literal, "device_id");
    String literal_str(literal, std::strlen("device_id"));
    for (int i = 0; i < device_num; i++) {
        for (int l = 0; l < num_timestamp_per_device; l++) {
            int row_index = i * num_timestamp_per_device + l;
            tablet.add_timestamp(row_index, offset + l);
            auto column_schemas = table_schema->get_measurement_schemas();
            for (int idx = 0; idx < column_schemas.size(); idx++) {
                switch (datatypes[idx]) {
                    case TSDataType::INT64:
                        tablet.add_value(row_index,
                                         column_schemas[idx]->measurement_name_,
                                         static_cast<int64_t>(i));
                        break;
                    case TSDataType::INT32:
                        tablet.add_value(row_index,
                                         column_schemas[idx]->measurement_name_,
                                         static_cast<int32_t>(i));
                        break;
                    case TSDataType::STRING:
                        tablet.add_value(row_index,
                                         column_schemas[idx]->measurement_name_,
                                         literal_str);
                        break;
                    default:
                        break;
                }
            }
        }
    }
    delete[] literal;
    delete table_schema;

    ASSERT_EQ(E_TYPE_NOT_MATCH, tsfile_table_writer_->write_table(tablet));
    tsfile_table_writer_->close();
}

TEST_F(TsFileWriterTableTest, WriteAndReadSimple) {
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
    std::vector<std::string> column_names = {"device", "VALUE"};
    int ret_value = reader.query("test_table", column_names, 0, 50, ret);
    ASSERT_EQ(common::E_OK, ret_value);

    ASSERT_EQ(ret_value, 0);
    auto* table_result_set = (TableResultSet*)ret;
    auto metadata = ret->get_metadata();
    ASSERT_EQ(metadata->get_column_name(column_names.size() + 1), "VALUE");
    bool has_next = false;
    int cur_line = 0;
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        cur_line++;
        int64_t timestamp = table_result_set->get_value<int64_t>("time");
        ASSERT_EQ(table_result_set->get_value<common::String*>("device")
                      ->to_std_string(),
                  "device" + std::to_string(timestamp));
        ASSERT_EQ(table_result_set->get_value<double>("VaLue"),
                  timestamp * 1.1);
    }
    ASSERT_EQ(cur_line, 51);
    table_result_set->close();
    reader.destroy_query_data_set(table_result_set);

    reader.close();
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, DuplicateColumnName) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    measurement_schemas.resize(3);
    measurement_schemas[0] = new MeasurementSchema("device", STRING);
    column_categories.emplace_back(ColumnCategory::TAG);
    measurement_schemas[1] = new MeasurementSchema("Device", STRING);
    column_categories.emplace_back(ColumnCategory::TAG);
    measurement_schemas[2] = new MeasurementSchema("value", DOUBLE);
    column_categories.emplace_back(ColumnCategory::FIELD);
    TableSchema* table_schema =
        new TableSchema("test_table", measurement_schemas, column_categories);
    auto tsfile_table_writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    Tablet tablet = Tablet(table_schema->get_measurement_names(),
                           table_schema->get_data_types());
    tablet.set_table_name("test_table");
    ASSERT_EQ(E_INVALID_ARG, tablet.add_timestamp(0, 10));
    ASSERT_EQ(E_INVALID_ARG, tablet.add_value(1, 1, 10));
    ASSERT_EQ(E_INVALID_ARG, tablet.add_value(1, "test", 10));
    std::vector<MeasurementSchema> measurement_schemas2;
    for (int i = 0; i < 2; i++) {
        measurement_schemas2.push_back(*measurement_schemas[i]);
    }
    Tablet tablet1 = Tablet(
        "test_table",
        std::make_shared<std::vector<MeasurementSchema>>(measurement_schemas2));
    tablet1.set_table_name("test_table");
    ASSERT_EQ(E_INVALID_ARG, tablet1.add_timestamp(0, 10));
    ASSERT_EQ(E_INVALID_ARG, tablet1.add_value(1, 1, 10));
    ASSERT_EQ(E_INVALID_ARG, tablet1.add_value(1, "test", 10));

    ASSERT_EQ(E_INVALID_ARG, tsfile_table_writer->write_table(tablet));
    ASSERT_EQ(E_INVALID_ARG, tsfile_table_writer->register_table(
                                 std::make_shared<TableSchema>(*table_schema)));
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, WriteWithNullAndEmptyTag) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    for (int i = 0; i < 3; i++) {
        measurement_schemas.emplace_back(new MeasurementSchema(
            "id" + std::to_string(i), TSDataType::STRING));
        column_categories.emplace_back(ColumnCategory::TAG);
    }
    measurement_schemas.emplace_back(new MeasurementSchema("value", DOUBLE));
    column_categories.emplace_back(ColumnCategory::FIELD);
    auto table_schema =
        new TableSchema("testTable", measurement_schemas, column_categories);
    auto tsfile_table_writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    int time = 0;
    Tablet tablet = Tablet(table_schema->get_measurement_names(),
                           table_schema->get_data_types(), 10);

    for (int i = 0; i < 10; i++) {
        tablet.add_timestamp(i, static_cast<int64_t>(time++));
        tablet.add_value(i, "ID0", "tag1");
        tablet.add_value(i, 1, "tag2");
        tablet.add_value(i, 2, "tag3");
        tablet.add_value(i, 3, 100.0f);
    }

    tsfile_table_writer->write_table(tablet);
    Tablet tablet2 = Tablet(table_schema->get_measurement_names(),
                            table_schema->get_data_types(), 10);

    for (int i = 0; i < 10; i++) {
        tablet2.add_timestamp(i, static_cast<int64_t>(time++));
        tablet2.add_value(i, 0, i % 2 == 0 ? "" : "tag4");
        tablet2.add_value(i, 1, i % 2 == 1 ? "" : "tag5");
        tablet2.add_value(i, 2, i % 3 == 0 ? "" : "tag6");
        tablet2.add_value(i, 3, 101.0f);
    }
    tsfile_table_writer->write_table(tablet2);

    Tablet tablet3 = Tablet(table_schema->get_measurement_names(),
                            table_schema->get_data_types(), 10);
    for (int i = 0; i < 10; i++) {
        tablet3.add_timestamp(i, static_cast<int64_t>(time++));
        tablet3.add_value(i, 0, "tag7");
        if (i % 2 == 0) {
            tablet3.add_value(i, 1, "tag8\0ta");
        } else {
            tablet3.add_value(i, 2, "tag9");
        }
        tablet3.add_value(i, 3, 102.0f);
    }

    tsfile_table_writer->write_table(tablet3);
    tsfile_table_writer->flush();
    tsfile_table_writer->close();

    delete table_schema;

    auto reader = TsFileReader();
    reader.open(write_file_.get_file_path());
    ResultSet* ret = nullptr;
    int ret_value =
        reader.query("testTable", {"id0", "id1", "id2", "value"}, 0, 50, ret);
    ASSERT_EQ(common::E_OK, ret_value);

    auto table_result_set = (TableResultSet*)ret;
    bool has_next = false;
    auto schema = table_result_set->get_metadata();
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        int64_t timestamp = table_result_set->get_value<int64_t>(1);
        switch (timestamp) {
            case 0: {
                // All tag fields have valid values.
                ASSERT_EQ(common::String(std::string("tag1")),
                          *table_result_set->get_value<common::String*>(2));
                ASSERT_EQ(common::String(std::string("tag2")),
                          *table_result_set->get_value<common::String*>(3));
                ASSERT_EQ(common::String(std::string("tag3")),
                          *table_result_set->get_value<common::String*>(4));
                ASSERT_EQ(100.0f, table_result_set->get_value<double>(5));
                break;
            }
            case 10: {
                // The first and last tag fields are empty strings.
                ASSERT_EQ(common::String(std::string("")),
                          *table_result_set->get_value<common::String*>(2));
                ASSERT_EQ(common::String(std::string("tag5")),
                          *table_result_set->get_value<common::String*>(3));
                ASSERT_EQ(common::String(std::string("")),
                          *table_result_set->get_value<common::String*>(4));
                ASSERT_EQ(101.0f, table_result_set->get_value<double>(5));
                break;
            }
            case 11: {
                // The middle tag field is an empty string.
                ASSERT_EQ(common::String(std::string("tag4")),
                          *table_result_set->get_value<common::String*>(2));
                ASSERT_EQ(common::String(std::string("")),
                          *table_result_set->get_value<common::String*>(3));
                ASSERT_EQ(common::String(std::string("tag6")),
                          *table_result_set->get_value<common::String*>(4));
                ASSERT_EQ(101.0f, table_result_set->get_value<double>(5));
                break;
            }
            case 20: {
                // The last tag field is null.
                ASSERT_EQ(common::String(std::string("tag7")),
                          *table_result_set->get_value<common::String*>(2));
                ASSERT_EQ(common::String(std::string("tag8\0ta")),
                          *table_result_set->get_value<common::String*>(3));
                ASSERT_TRUE(table_result_set->is_null(4));
                ASSERT_EQ(102.0f, table_result_set->get_value<double>(5));
                break;
            }
            case 21: {
                // The middle tag field is null.
                ASSERT_EQ(common::String(std::string("tag7")),
                          *table_result_set->get_value<common::String*>(2));
                ASSERT_EQ(common::String(std::string("tag9")),
                          *table_result_set->get_value<common::String*>(4));
                ASSERT_TRUE(table_result_set->is_null(3));
                ASSERT_EQ(102.0f, table_result_set->get_value<double>(5));
                break;
            }
            default:
                break;
        }
    }
    reader.destroy_query_data_set(table_result_set);
    ASSERT_EQ(reader.close(), common::E_OK);
}

TEST_F(TsFileWriterTableTest, MultiDeviceMultiFields) {
    common::config_set_max_degree_of_index_node(5);
    auto table_schema = gen_table_schema(0, 1, 100);
    auto tsfile_table_writer_ =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    int num_row_per_device = 10;
    auto tablet = gen_tablet(table_schema, 0, 100, num_row_per_device);
    ASSERT_EQ(tsfile_table_writer_->write_table(tablet), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->flush(), common::E_OK);
    ASSERT_EQ(tsfile_table_writer_->close(), common::E_OK);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);

    ResultSet* tmp_result_set = nullptr;
    ret = reader.query(table_schema->get_table_name(),
                       table_schema->get_measurement_names(), 0, INT32_MAX,
                       tmp_result_set);
    auto* table_result_set = (TableResultSet*)tmp_result_set;
    bool has_next = false;
    int64_t row_num = 0;
    auto result_set_meta = table_result_set->get_metadata();
    ASSERT_EQ(result_set_meta->get_column_count(),
              table_schema->get_columns_num() + 1);  // +1: time column
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        auto column_schemas = table_schema->get_measurement_schemas();
        std::string tag_col_val;  // "device_id_[num]"
        std::string tag_col_val_prefix = "device_id_";
        for (const auto& column_schema : column_schemas) {
            switch (column_schema->data_type_) {
                case TSDataType::INT64:
                    if (!table_result_set->is_null(
                            column_schema->measurement_name_)) {
                        std::string num = tag_col_val.substr(
                            tag_col_val_prefix.length(),
                            tag_col_val.length() - tag_col_val_prefix.length());
                        EXPECT_EQ(table_result_set->get_value<int64_t>(
                                      column_schema->measurement_name_),
                                  std::stoi(num));
                    }
                    break;
                case TSDataType::STRING:
                    tag_col_val = table_result_set
                                      ->get_value<common::String*>(
                                          column_schema->measurement_name_)
                                      ->to_std_string();
                default:
                    break;
            }
        }
        row_num++;
    }
    ASSERT_EQ(row_num, tablet.get_cur_row_size());
    reader.destroy_query_data_set(table_result_set);
    ASSERT_EQ(reader.close(), common::E_OK);
    delete table_schema;
}

TEST_F(TsFileWriterTableTest, WriteDataWithEmptyField) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    for (int i = 0; i < 3; i++) {
        measurement_schemas.emplace_back(new MeasurementSchema(
            "id" + std::to_string(i), TSDataType::STRING));
        column_categories.emplace_back(ColumnCategory::TAG);
    }
    measurement_schemas.emplace_back(new MeasurementSchema("value", DOUBLE));
    measurement_schemas.emplace_back(new MeasurementSchema("value1", INT32));
    column_categories.emplace_back(ColumnCategory::FIELD);
    column_categories.emplace_back(ColumnCategory::FIELD);
    auto table_schema =
        new TableSchema("testTable", measurement_schemas, column_categories);
    auto tsfile_table_writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    int time = 0;
    Tablet tablet = Tablet(table_schema->get_measurement_names(),
                           table_schema->get_data_types(), 100);

    for (int i = 0; i < 100; i++) {
        tablet.add_timestamp(i, static_cast<int64_t>(time++));
        tablet.add_value(i, 0, "tag1");
        tablet.add_value(i, 1, "tag2");
        if (i % 3 == 0) {
            // all device has no data
            tablet.add_value(i, 2, "tag_null");
        } else {
            tablet.add_value(i, 2, "tag3");
            tablet.add_value(i, 3, 100.0f);
            if (i % 5 == 0) {
                tablet.add_value(i, 4, 100);
            }
        }
    }
    tsfile_table_writer->write_table(tablet);
    tsfile_table_writer->flush();
    tsfile_table_writer->close();

    delete table_schema;

    auto reader = TsFileReader();
    reader.open(write_file_.get_file_path());
    ResultSet* ret = nullptr;
    int ret_value = reader.query(
        "testTable", {"id0", "id1", "id2", "value", "value1"}, 0, 100, ret);
    ASSERT_EQ(common::E_OK, ret_value);

    auto table_result_set = (TableResultSet*)ret;
    bool has_next = false;
    auto schema = table_result_set->get_metadata();
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        int64_t timestamp = table_result_set->get_value<int64_t>(1);
        ASSERT_EQ(common::String("tag1"),
                  *table_result_set->get_value<common::String*>(2));
        ASSERT_EQ(common::String("tag2"),
                  *table_result_set->get_value<common::String*>(3));
        if (timestamp % 3 == 0) {
            ASSERT_EQ(common::String("tag_null"),
                      *table_result_set->get_value<common::String*>(4));
            ASSERT_TRUE(table_result_set->is_null(5));
            ASSERT_TRUE(table_result_set->is_null(6));
        } else {
            ASSERT_EQ(common::String("tag3"),
                      *table_result_set->get_value<common::String*>(4));
            ASSERT_EQ(100.0f, table_result_set->get_value<double>(5));
            if (timestamp % 5 == 0) {
                ASSERT_EQ(100, table_result_set->get_value<int32_t>(6));
            } else {
                ASSERT_TRUE(table_result_set->is_null(6));
            }
        }
    }
    reader.destroy_query_data_set(table_result_set);
    ASSERT_EQ(reader.close(), common::E_OK);
}

TEST_F(TsFileWriterTableTest, MultiDatatypes) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;

    std::vector<std::string> measurement_names = {
        "level", "num", "bools", "double", "id", "ts", "text", "blob", "date"};
    std::vector<common::TSDataType> data_types = {
        FLOAT, INT64, BOOLEAN, DOUBLE, STRING, TIMESTAMP, TEXT, BLOB, DATE};

    for (int i = 0; i < measurement_names.size(); i++) {
        measurement_schemas.emplace_back(
            new MeasurementSchema(measurement_names[i], data_types[i]));
        column_categories.emplace_back(ColumnCategory::FIELD);
    }
    auto table_schema =
        new TableSchema("testTable", measurement_schemas, column_categories);
    auto tsfile_table_writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    int time = 0;
    Tablet tablet = Tablet(table_schema->get_measurement_names(),
                           table_schema->get_data_types(), 100);

    char* literal = new char[std::strlen("device_id") + 1];
    std::strcpy(literal, "device_id");
    String literal_str(literal, std::strlen("device_id"));
    std::time_t now = std::time(nullptr);
    std::tm* local_time = std::localtime(&now);
    std::tm today = {};
    today.tm_year = local_time->tm_year;
    today.tm_mon = local_time->tm_mon;
    today.tm_mday = local_time->tm_mday;
    for (int i = 0; i < 100; i++) {
        tablet.add_timestamp(i, static_cast<int64_t>(time++));
        for (int j = 0; j < measurement_schemas.size(); j++) {
            switch (data_types[j]) {
                case BOOLEAN:
                    ASSERT_EQ(tablet.add_value(i, j, true), E_OK);
                    break;
                case INT64:
                    ASSERT_EQ(tablet.add_value(i, j, (int64_t)415412), E_OK);
                    break;
                case FLOAT:
                    ASSERT_EQ(tablet.add_value(i, j, (float)1.0), E_OK);
                    break;
                case DOUBLE:
                    ASSERT_EQ(tablet.add_value(i, j, (double)2.0), E_OK);
                    break;
                case STRING:
                    ASSERT_EQ(tablet.add_value(i, j, literal_str), E_OK);
                    break;
                case TEXT:
                    ASSERT_EQ(tablet.add_value(i, j, literal_str), E_OK);
                    break;
                case BLOB:
                    ASSERT_EQ(tablet.add_value(i, j, literal_str), E_OK);
                    break;
                case TIMESTAMP:
                    ASSERT_EQ(tablet.add_value(i, j, (int64_t)415412), E_OK);
                    break;
                case DATE:
                    ASSERT_EQ(tablet.add_value(i, j, today), E_OK);
                default:
                    break;
            }
        }
    }
    ASSERT_EQ(tsfile_table_writer->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_table_writer->flush(), E_OK);
    ASSERT_EQ(tsfile_table_writer->close(), E_OK);

    delete table_schema;

    auto reader = TsFileReader();
    reader.open(write_file_.get_file_path());
    ResultSet* ret = nullptr;
    int ret_value = reader.query("testTable", measurement_names, 0, 100, ret);
    ASSERT_EQ(common::E_OK, ret_value);

    auto table_result_set = (TableResultSet*)ret;
    bool has_next = false;
    int cur_line = 0;
    auto schema = table_result_set->get_metadata();
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        int64_t timestamp = table_result_set->get_value<int64_t>(1);
        ASSERT_EQ(table_result_set->get_value<float>(2), (float)1.0);
        ASSERT_EQ(table_result_set->get_value<int64_t>(3), (int64_t)415412);
        ASSERT_EQ(table_result_set->get_value<bool>(4), true);
        ASSERT_EQ(table_result_set->get_value<double>(5), (double)2.0);
        ASSERT_EQ(table_result_set->get_value<common::String*>(6)->compare(
                      literal_str),
                  0);
        ASSERT_EQ(table_result_set->get_value<int64_t>(7), (int64_t)415412);
        ASSERT_EQ(table_result_set->get_value<common::String*>(8)->compare(
                      literal_str),
                  0);
        ASSERT_EQ(table_result_set->get_value<common::String*>(9)->compare(
                      literal_str),
                  0);
        ASSERT_TRUE(DateConverter::is_tm_ymd_equal(
            table_result_set->get_value<std::tm>(10), today));
    }
    reader.destroy_query_data_set(table_result_set);
    ASSERT_EQ(reader.close(), common::E_OK);
    delete[] literal;
}

TEST_F(TsFileWriterTableTest, DiffCodecTypes) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;

    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    std::vector<std::string> measurement_names = {
        "int32_zigzag",  "int64_zigzag",   "string_dic",    "text_dic",
        "float_gorilla", "double_gorilla", "int32_ts2diff", "int64_ts2diff",
        "int32_rle",     "int64_rle",      "int32_sprintz", "int64_sprintz",
        "float_sprintz", "double_sprintz",
    };
    std::vector<common::TSDataType> data_types = {
        INT32, INT64, STRING, TEXT,  FLOAT, DOUBLE, INT32,
        INT64, INT32, INT64,  INT32, INT64, FLOAT,  DOUBLE};
    std::vector<common::TSEncoding> encodings = {
        ZIGZAG,   ZIGZAG, DICTIONARY, DICTIONARY, GORILLA, GORILLA, TS_2DIFF,
        TS_2DIFF, RLE,    RLE,        SPRINTZ,    SPRINTZ, SPRINTZ, SPRINTZ};

    for (int i = 0; i < measurement_names.size(); i++) {
        measurement_schemas.emplace_back(new MeasurementSchema(
            measurement_names[i], data_types[i], encodings[i], UNCOMPRESSED));
        column_categories.emplace_back(ColumnCategory::FIELD);
    }
    auto table_schema =
        new TableSchema("testTable", measurement_schemas, column_categories);
    auto tsfile_table_writer =
        std::make_shared<TsFileTableWriter>(&write_file_, table_schema);
    int time = 0;
    Tablet tablet = Tablet(table_schema->get_measurement_names(),
                           table_schema->get_data_types(), 100);

    char* literal = new char[std::strlen("device_id") + 1];
    std::strcpy(literal, "device_id");
    String literal_str(literal, std::strlen("device_id"));
    for (int i = 0; i < 100; i++) {
        tablet.add_timestamp(i, static_cast<int64_t>(time++));
        for (int j = 0; j < measurement_schemas.size(); j++) {
            std::string measurement_name = measurement_names[j];
            switch (data_types[j]) {
                case BOOLEAN:
                    ASSERT_EQ(tablet.add_value(i, j, true), E_OK);
                    break;
                case INT32:
                    ASSERT_EQ(tablet.add_value(i, j, (int32_t)32), E_OK);
                    break;
                case INT64:
                    ASSERT_EQ(tablet.add_value(i, j, (int64_t)64), E_OK);
                    break;
                case FLOAT:
                    ASSERT_EQ(tablet.add_value(i, j, (float)1.0), E_OK);
                    break;
                case DOUBLE:
                    ASSERT_EQ(tablet.add_value(i, j, (double)2.0), E_OK);
                    break;
                case TEXT:
                case STRING:
                case BLOB:
                    ASSERT_EQ(tablet.add_value(i, j, literal_str), E_OK);
                    break;
                default:
                    break;
            }
        }
    }
    ASSERT_EQ(tsfile_table_writer->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_table_writer->flush(), E_OK);
    ASSERT_EQ(tsfile_table_writer->close(), E_OK);

    delete table_schema;

    auto reader = TsFileReader();
    reader.open(write_file_.get_file_path());
    ResultSet* ret = nullptr;
    int ret_value = reader.query("testTable", measurement_names, 0, 100, ret);
    ASSERT_EQ(common::E_OK, ret_value);

    auto table_result_set = (TableResultSet*)ret;
    bool has_next = false;
    int cur_line = 0;
    auto schema = table_result_set->get_metadata();
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        int64_t timestamp = table_result_set->get_value<int64_t>(1);
        ASSERT_EQ(table_result_set->get_value<int32_t>(2), 32);
        ASSERT_EQ(table_result_set->get_value<int64_t>(3), 64);

        ASSERT_EQ(table_result_set->get_value<common::String*>(4)->compare(
                      literal_str),
                  0);
        ASSERT_EQ(table_result_set->get_value<common::String*>(5)->compare(
                      literal_str),
                  0);

        ASSERT_EQ(table_result_set->get_value<float>(6), (float)1.0);
        ASSERT_EQ(table_result_set->get_value<double>(7), (double)2.0);

        ASSERT_EQ(table_result_set->get_value<int32_t>(8), 32);
        ASSERT_EQ(table_result_set->get_value<int64_t>(9), 64);

        ASSERT_EQ(table_result_set->get_value<int32_t>(10), 32);
        ASSERT_EQ(table_result_set->get_value<int64_t>(11), 64);
        // SPRINTZ
        ASSERT_EQ(table_result_set->get_value<int32_t>(12), 32);
        ASSERT_EQ(table_result_set->get_value<int64_t>(13), 64);
        ASSERT_FLOAT_EQ(table_result_set->get_value<float>(14), (float)1.0);
        ASSERT_DOUBLE_EQ(table_result_set->get_value<double>(15), (double)2.0);
    }
    reader.destroy_query_data_set(table_result_set);
    ASSERT_EQ(reader.close(), common::E_OK);
    delete[] literal;
}
