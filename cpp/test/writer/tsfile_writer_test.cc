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
#include "writer/tsfile_writer.h"

#include <gtest/gtest.h>

#include <random>

#include "common/path.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/tsfile_io_writer.h"
#include "file/write_file.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/chunk_writer.h"
using namespace storage;
using namespace common;

class TsFileWriterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        tsfile_writer_ = new TsFileWriter();
        file_name_ = std::string("tsfile_writer_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        mode_t mode = 0666;
        EXPECT_EQ(tsfile_writer_->open(file_name_, flags, mode), common::E_OK);
    }
    void TearDown() override {
        delete tsfile_writer_;
        remove(file_name_.c_str());
    }

    std::string file_name_;
    TsFileWriter *tsfile_writer_ = nullptr;

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

    static std::string field_to_string(storage::Field *value) {
        if (value->type_ == common::TEXT) {
            return std::string(value->value_.sval_);
        } else {
            std::stringstream ss;
            switch (value->type_) {
                case common::BOOLEAN:
                    ss << (value->value_.bval_ ? "true" : "false");
                    break;
                case common::INT32:
                    ss << value->value_.ival_;
                    break;
                case common::INT64:
                    ss << value->value_.lval_;
                    break;
                case common::FLOAT:
                    ss << value->value_.fval_;
                    break;
                case common::DOUBLE:
                    ss << value->value_.dval_;
                    break;
                case common::NULL_TYPE:
                    ss << "NULL";
                    break;
                default:
                    ASSERT(false);
                    break;
            }
            return ss.str();
        }
    }
};

TEST_F(TsFileWriterTest, InitWithNullWriteFile) {
    TsFileWriter writer;
    ASSERT_EQ(writer.init(nullptr), E_INVALID_ARG);
}

TEST_F(TsFileWriterTest, WriteDiffDataType) {
    std::string device_name = "test_table";
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    std::vector<std::string> measurement_names = {"level", "num", "bools",
                                                  "double", "id"};
    std::vector<common::TSDataType> data_types = {FLOAT, INT64, BOOLEAN, DOUBLE,
                                                  STRING};
    for (uint32_t i = 0; i < measurement_names.size(); i++) {
        std::string measurement_name = measurement_names[i];
        common::TSDataType data_type = data_types[i];
        tsfile_writer_->register_timeseries(
            device_name,
            storage::MeasurementSchema(measurement_name, data_type, encoding,
                                       compression_type));
    }

    char *literal = new char[std::strlen("device_id") + 1];
    std::strcpy(literal, "device_id");
    String literal_str(literal, std::strlen("device_id"));

    int row_num = 1000;
    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i * 100, device_name);
        for (uint32_t j = 0; j < measurement_names.size(); j++) {
            std::string measurement_name = measurement_names[j];
            common::TSDataType data_type = data_types[j];
            switch (data_type) {
                case BOOLEAN:
                    record.add_point(measurement_name, true);
                    break;
                case INT64:
                    record.add_point(measurement_name, (int64_t)415412);
                    break;
                case FLOAT:
                    record.add_point(measurement_name, (float)1.0);
                    break;
                case DOUBLE:
                    record.add_point(measurement_name, (double)2.0);
                    break;
                case STRING:
                    record.add_point(measurement_name, literal_str);
                    break;
                default:
                    break;
            }
        }
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<std::string> select_list;
    select_list.reserve(measurement_names.size());
    for (uint32_t i = 0; i < measurement_names.size(); ++i) {
        std::string measurement_name = measurement_names[i];
        std::string path_name = device_name + "." + measurement_name;
        select_list.emplace_back(path_name);
    }

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet *tmp_qds = nullptr;
    ret = reader.query(select_list, 1622505600000,
                       1622505600000 + row_num * 100, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    int64_t cur_record_num = 0;
    bool has_next = false;
    do {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        cur_record_num++;
        ASSERT_EQ(qds->get_value<float>(1), (float)1.0);
        ASSERT_EQ(qds->get_value<int64_t>(2), (int64_t)415412);
        ASSERT_EQ(qds->get_value<bool>(3), true);
        ASSERT_EQ(qds->get_value<double>(4), (double)2.0);
        ASSERT_EQ(qds->get_value<common::String *>(5)->compare(literal_str), 0);

        ASSERT_EQ(qds->get_value<float>(measurement_names[0]), (float)1.0);
        ASSERT_EQ(qds->get_value<int64_t>(measurement_names[1]),
                  (int64_t)415412);
        ASSERT_EQ(qds->get_value<bool>(measurement_names[2]), true);
        ASSERT_EQ(qds->get_value<double>(measurement_names[3]), (double)2.0);
        ASSERT_EQ(qds->get_value<common::String *>(measurement_names[4])
                      ->compare(literal_str),
                  0);
    } while (true);
    delete[] literal;
    EXPECT_EQ(cur_record_num, row_num);
    reader.destroy_query_data_set(qds);
    reader.close();
}

TEST_F(TsFileWriterTest, RegisterTimeSeries) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;

    ASSERT_EQ(tsfile_writer_->register_timeseries(
                  device_path,
                  storage::MeasurementSchema(measurement_name, data_type,
                                             encoding, compression_type)),
              E_OK);
}

TEST_F(TsFileWriterTest, WriteMultipleRecords) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    tsfile_writer_->register_timeseries(
        device_path, storage::MeasurementSchema(measurement_name, data_type,
                                                encoding, compression_type));

    for (int i = 0; i < 50000; ++i) {
        TsRecord record(1622505600000 + i * 1000, device_path);
        record.add_point(measurement_name, (int32_t)i);
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}

TEST_F(TsFileWriterTest, WriteDiffrentTypeCombination) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    std::vector<TSDataType> data_types = {
        TSDataType::INT32, TSDataType::INT64, TSDataType::FLOAT, TSDataType::DOUBLE};
    std::vector<TSEncoding> encodings = {TSEncoding::PLAIN,
                                         TSEncoding::TS_2DIFF};
    std::vector<CompressionType> compression_types = {
        CompressionType::UNCOMPRESSED, CompressionType::SNAPPY,
        CompressionType::GZIP, CompressionType::LZ4};

    std::vector<MeasurementSchema> schema_vecs;
    schema_vecs.reserve(data_types.size() * encodings.size() * compression_types.size());
    int idx = 0;
    for (auto data_type : data_types) {
        for (auto encoding_type : encodings) {
            for (auto compression_type : compression_types) {
                schema_vecs.emplace_back(MeasurementSchema(measurement_name + std::to_string(idx),
                                      data_type, encoding_type, compression_type));
                tsfile_writer_->register_timeseries(
                    device_path, schema_vecs[idx++]);
            }
        }
    }

    char *literal = new char[std::strlen("literal") + 1];
    std::strcpy(literal, "literal");
    String literal_str(literal, std::strlen("literal"));

    for (int i = 0; i < schema_vecs.size(); ++i) {
        TsRecord record(1622505600000 + i * 1000, device_path);
        if (schema_vecs[i].data_type_ == TSDataType::INT32) {
            record.add_point(schema_vecs[i].measurement_name_, (int32_t)i);
        } else if (schema_vecs[i].data_type_ == TSDataType::FLOAT) {
            record.add_point(schema_vecs[i].measurement_name_, 3.14);
        } else if (schema_vecs[i].data_type_ == TSDataType::DOUBLE) {
            record.add_point(schema_vecs[i].measurement_name_, 3.1415926);
        } else if (schema_vecs[i].data_type_ == TSDataType::BOOLEAN) {
            record.add_point(schema_vecs[i].measurement_name_, true);
        } else if (schema_vecs[i].data_type_ == TSDataType::STRING) {
            record.add_point(schema_vecs[i].measurement_name_, literal_str);
        }
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    delete[] literal;
}

TEST_F(TsFileWriterTest, WriteMultipleTabletsMultiFlush) {
    const int device_num = 20;
    const int measurement_num = 20;
    int max_tablet_num = 100;
    std::vector<std::vector<MeasurementSchema>> schema_vecs(
        device_num, std::vector<MeasurementSchema>(measurement_num));
    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        for (int j = 0; j < measurement_num; j++) {
            std::string measure_name = "measurement" + std::to_string(j);
            schema_vecs[i][j] =
                MeasurementSchema(measure_name, common::TSDataType::INT32,
                                  common::TSEncoding::PLAIN,
                                  common::CompressionType::UNCOMPRESSED);
            tsfile_writer_->register_timeseries(
                device_name, storage::MeasurementSchema(
                                 measure_name, common::TSDataType::INT32,
                                 common::TSEncoding::PLAIN,
                                 common::CompressionType::UNCOMPRESSED));
        }
    }

    for (int tablet_num = 0; tablet_num < max_tablet_num; tablet_num++) {
        for (int i = 0; i < device_num; i++) {
            std::string device_name = "test_device" + std::to_string(i);
            storage::Tablet tablet(
                device_name,
                std::make_shared<std::vector<MeasurementSchema>>(
                    schema_vecs[i]),
                1);
            for (int j = 0; j < measurement_num; j++) {
                tablet.add_timestamp(0, 16225600000 + tablet_num * 100);
                tablet.add_value(0, j, static_cast<int32_t>(tablet_num));
            }
            ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
        }
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (int i = 0; i < device_num; i++) {
        for (int j = 0; j < measurement_num; ++j) {
            std::string device_name = "test_device" + std::to_string(i);
            std::string measure_name = "measurement" + std::to_string(j);
            storage::Path path(device_name, measure_name);
            select_list.push_back(path);
        }
    }
    storage::QueryExpression *query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    int max_rows = max_tablet_num * 1;
    bool has_next = false;
    for (int cur_row = 0; cur_row < max_rows; cur_row++) {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, WriteMultipleTabletsAlignedMultiFlush) {
    const int device_num = 20;
    const int measurement_num = 20;
    int max_tablet_num = 100;
    std::vector<std::vector<MeasurementSchema>> schema_vecs(
        device_num, std::vector<MeasurementSchema>(measurement_num));
    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        for (int j = 0; j < measurement_num; j++) {
            std::string measure_name = "measurement" + std::to_string(j);
            schema_vecs[i][j] =
                MeasurementSchema(measure_name, common::TSDataType::INT32,
                                  common::TSEncoding::PLAIN,
                                  common::CompressionType::UNCOMPRESSED);
            tsfile_writer_->register_aligned_timeseries(
                device_name, storage::MeasurementSchema(
                                 measure_name, common::TSDataType::INT32,
                                 common::TSEncoding::PLAIN,
                                 common::CompressionType::UNCOMPRESSED));
        }
    }

    for (int tablet_num = 0; tablet_num < max_tablet_num; tablet_num++) {
        for (int i = 0; i < device_num; i++) {
            std::string device_name = "test_device" + std::to_string(i);
            storage::Tablet tablet(
                device_name,
                std::make_shared<std::vector<MeasurementSchema>>(
                    schema_vecs[i]),
                1);
            for (int j = 0; j < measurement_num; j++) {
                tablet.add_timestamp(0, 16225600000 + tablet_num * 100);
                tablet.add_value(0, j, static_cast<int32_t>(tablet_num));
            }
            ASSERT_EQ(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
        }
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (int i = 0; i < device_num; i++) {
        for (int j = 0; j < measurement_num; ++j) {
            std::string device_name = "test_device" + std::to_string(i);
            std::string measure_name = "measurement" + std::to_string(j);
            storage::Path path(device_name, measure_name);
            select_list.push_back(path);
        }
    }
    storage::QueryExpression *query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    int max_rows = max_tablet_num * 1;
    bool has_next = false;
    for (int cur_row = 0; cur_row < max_rows; cur_row++) {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, WriteMultipleTabletsInt64) {
    const int device_num = 50;
    const int measurement_num = 50;
    std::vector<MeasurementSchema> schema_vec[50];

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        for (int j = 0; j < measurement_num; j++) {
            std::string measure_name = "measurement" + std::to_string(j);
            schema_vec[i].push_back(
                MeasurementSchema(measure_name, common::TSDataType::INT64,
                                  common::TSEncoding::PLAIN,
                                  common::CompressionType::UNCOMPRESSED));
            tsfile_writer_->register_timeseries(
                device_name, storage::MeasurementSchema(
                                 measure_name, common::TSDataType::INT64,
                                 common::TSEncoding::PLAIN,
                                 common::CompressionType::UNCOMPRESSED));
        }
    }

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        int max_rows = 100;
        storage::Tablet tablet(
            device_name,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec[i]),
            max_rows);
        for (int j = 0; j < measurement_num; j++) {
            for (int row = 0; row < max_rows; row++) {
                tablet.add_timestamp(row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet.add_value(row, j, static_cast<int64_t>(row));
            }
        }
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    }

    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}

TEST_F(TsFileWriterTest, WriteMultipleTabletsDouble) {
    const int device_num = 50;
    const int measurement_num = 50;
    std::vector<MeasurementSchema> schema_vec[50];

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        for (int j = 0; j < measurement_num; j++) {
            std::string measure_name = "measurement" + std::to_string(j);
            schema_vec[i].push_back(
                MeasurementSchema(measure_name, common::TSDataType::DOUBLE,
                                  common::TSEncoding::PLAIN,
                                  common::CompressionType::UNCOMPRESSED));
            tsfile_writer_->register_timeseries(
                device_name, storage::MeasurementSchema(
                                 measure_name, common::TSDataType::DOUBLE,
                                 common::TSEncoding::PLAIN,
                                 common::CompressionType::UNCOMPRESSED));
        }
    }

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        int max_rows = 200;
        storage::Tablet tablet(
            device_name,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec[i]),
            max_rows);
        for (int j = 0; j < measurement_num; j++) {
            for (int row = 0; row < max_rows; row++) {
                tablet.add_timestamp(row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet.add_value(row, j, static_cast<double>(row) + 1.0);
            }
        }
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    }

    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}

TEST_F(TsFileWriterTest, FlushMultipleDevice) {
    const int device_num = 50;
    const int measurement_num = 50;
    const int max_rows = 100;
    std::vector<MeasurementSchema> schema_vec[50];

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        for (int j = 0; j < measurement_num; j++) {
            std::string measure_name = "measurement" + std::to_string(j);
            schema_vec[i].emplace_back(measure_name, common::TSDataType::INT64,
                                       common::TSEncoding::PLAIN,
                                       common::CompressionType::UNCOMPRESSED);
            tsfile_writer_->register_timeseries(
                device_name,
                MeasurementSchema(measure_name, common::TSDataType::INT64,
                                  common::TSEncoding::PLAIN,
                                  common::CompressionType::UNCOMPRESSED));
        }
    }

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        storage::Tablet tablet(
            device_name,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec[i]),
            max_rows);
        for (int j = 0; j < measurement_num; j++) {
            for (int row = 0; row < max_rows; row++) {
                tablet.add_timestamp(row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet.add_value(row, j, static_cast<int64_t>(row));
            }
        }
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
        // flush after write tablet to check whether write empty chunk
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        for (int j = 0; j < measurement_num; j++) {
            std::string measurement_name = "measurement" + std::to_string(j);
            storage::Path path(device_name, measurement_name);
            select_list.push_back(path);
        }
    }
    storage::QueryExpression *query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    int64_t cur_record_num = 0;
    bool has_next = false;
    do {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        // if empty chunk is writen, the timestamp should be NULL
        if (!record) {
            break;
        }
        EXPECT_EQ(record->get_timestamp(), 16225600 + cur_record_num);
        cur_record_num++;
    } while (true);
    EXPECT_EQ(cur_record_num, max_rows);
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, AnalyzeTsfileForload) {
    const int device_num = 50;
    const int measurement_num = 50;
    const int max_rows = 100;
    std::vector<MeasurementSchema> schema_vec[50];

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        for (int j = 0; j < measurement_num; j++) {
            std::string measure_name = "measurement" + std::to_string(j);
            schema_vec[i].push_back(
                MeasurementSchema(measure_name, common::TSDataType::INT64,
                                  common::TSEncoding::PLAIN,
                                  common::CompressionType::UNCOMPRESSED));
            tsfile_writer_->register_timeseries(
                device_name,
                MeasurementSchema(measure_name, common::TSDataType::INT64,
                                  common::TSEncoding::PLAIN,
                                  common::CompressionType::UNCOMPRESSED));
        }
    }

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        storage::Tablet tablet(
            device_name,
            std::make_shared<std::vector<MeasurementSchema>>(schema_vec[i]),
            max_rows);
        for (int j = 0; j < measurement_num; j++) {
            for (int row = 0; row < max_rows; row++) {
                tablet.add_timestamp(row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet.add_value(row, j, static_cast<int64_t>(row));
            }
        }
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    }
    auto schemas = tsfile_writer_->get_schema_group_map();
    ASSERT_EQ(schemas->size(), 50);
    for (const auto &device_iter : *schemas) {
        for (const auto &chunk_iter :
             device_iter.second->measurement_schema_map_) {
            ASSERT_NE(chunk_iter.second->chunk_writer_, nullptr);
            ASSERT_TRUE(chunk_iter.second->chunk_writer_->hasData());
        }
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}
TEST_F(TsFileWriterTest, FlushWithoutWriteAfterRegisterTS) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;

    ASSERT_EQ(tsfile_writer_->register_timeseries(
                  device_path,
                  storage::MeasurementSchema(measurement_name, data_type,
                                             encoding, compression_type)),
              E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}

TEST_F(TsFileWriterTest, WriteAlignedTimeseries) {
    int measurement_num = 100, row_num = 150;
    std::string device_name = "device";
    std::vector<std::string> measurement_names;
    for (int i = 0; i < measurement_num; i++) {
        measurement_names.emplace_back("temperature" + to_string(i));
    }

    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    std::vector<MeasurementSchema *> measurement_schema_vec;
    for (const auto &measurement_name : measurement_names) {
        auto *ms = new MeasurementSchema(measurement_name, data_type, encoding,
                                         compression_type);
        measurement_schema_vec.push_back(ms);
    }
    tsfile_writer_->register_aligned_timeseries(device_name,
                                                measurement_schema_vec);

    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i * 1000, device_name);
        for (const auto &measurement_name : measurement_names) {
            record.add_point(measurement_name, (int32_t)i);
        }
        ASSERT_EQ(tsfile_writer_->write_record_aligned(record), E_OK);
    }

    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (int i = 0; i < measurement_num; ++i) {
        std::string measurement_name = "temperature" + to_string(i);
        storage::Path path(device_name, measurement_name);
        select_list.push_back(path);
    }
    storage::QueryExpression *query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    bool has_next = false;
    for (int cur_row = 0; cur_row < row_num; cur_row++) {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, WriteAlignedMultiFlush) {
    int measurement_num = 100, row_num = 100;
    std::string device_name = "device";
    std::vector<std::string> measurement_names;
    for (int i = 0; i < measurement_num; i++) {
        measurement_names.emplace_back("temperature" + to_string(i));
    }

    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    std::vector<MeasurementSchema *> measurement_schema_vec;
    for (const auto &measurement_name : measurement_names) {
        auto *ms = new MeasurementSchema(measurement_name, data_type, encoding,
                                         compression_type);
        measurement_schema_vec.push_back(ms);
    }
    tsfile_writer_->register_aligned_timeseries(device_name,
                                                measurement_schema_vec);

    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i * 1000, device_name);
        for (const auto &measurement_name : measurement_names) {
            record.add_point(measurement_name, (int32_t)i);
        }
        ASSERT_EQ(tsfile_writer_->write_record_aligned(record), E_OK);
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }

    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (int i = 0; i < measurement_num; ++i) {
        std::string measurement_name = "temperature" + to_string(i);
        storage::Path path(device_name, measurement_name);
        select_list.push_back(path);
    }
    storage::QueryExpression *query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    bool has_next = false;
    for (int cur_row = 0; cur_row < row_num; cur_row++) {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, WriteBenchmark) {
    std::vector<CompressionType> compress_types = {UNCOMPRESSED, SNAPPY, GZIP, LZ4};
    std::vector<std::string> compress_names = {"UNCOMPRESSED", "SNAPPY", "GZIP", "LZ4"}; ;
    for (int compress_idx = 0; compress_idx < compress_names.size(); compress_idx++) {
        TsFileWriter* tsfile_writer = new TsFileWriter();
        std::string tsfile_path = compress_names[compress_idx] + std::string("tsfile");
        remove(tsfile_path.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        mode_t mode = 0666;
        int ret = tsfile_writer->open(tsfile_path.c_str(), flags, mode);
        ASSERT_EQ(ret, common::E_OK);
        int device_num = 50, measurement_num = 50;

        std::vector<MeasurementSchema> schema_vec;
        for (int j = 0; j < measurement_num; j++) {
            std::string measure_name = "sensor_" + to_string(j);
            schema_vec.push_back(MeasurementSchema("sensor_" + to_string(j),
                INT32, TS_2DIFF, compress_types[compress_idx]));
        }
        std::string device_name = "device";
        for (int i = 0; i < device_num; i++) {
            for (int j = 0; j < measurement_num; j++) {
                tsfile_writer->register_timeseries(device_name + to_string(i), schema_vec[j]);
            }
        }
        auto start_time = std::chrono::high_resolution_clock::now();
        int max_rows = 1000000;
        int tablet_size = 100000;
        int cur = 0;
        std::cout << "start writing" << std::endl;
        while (cur < max_rows) {
            if (cur + tablet_size > max_rows) {
                tablet_size = max_rows - cur;
            }
            for (int i = 0; i < device_num; i++) {
                Tablet tablet(device_name + to_string(i),
                    std::make_shared<std::vector<MeasurementSchema>>(schema_vec), tablet_size);
                for (int row = 0; row < tablet_size; row++) {
                    tablet.add_timestamp(row, 12345 + cur + row);
                }
                for (int j = 0; j < measurement_num; j++) {
                    for (int row = 0; row < tablet_size; row++) {
                        tablet.add_value(row, schema_vec[j].measurement_name_, cur + row);
                    }
                }
                tsfile_writer->write_tablet(tablet);
                tsfile_writer->flush();
            }
            std::cout << "cur write:" << cur << std::endl;
            cur = cur + tablet_size;
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        auto compression_duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                              start_time)
            .count();
        std::cout << "write cost: " << compression_duration << " ms"
                  << std::endl;
        tsfile_writer->close();
        delete tsfile_writer;

        std::ifstream file(tsfile_path, std::ifstream::ate | std::ifstream::binary);
        std::streamsize size = file.tellg();
        file.close();
        std::cout << "file size: " << size / 1024 / 1024 << " MB" << std::endl;
        std::cout << "write finish " << compress_names[compress_idx] << std::endl;

        //
        // std::vector<storage::Path> select_list;
        //
        // storage::Path path("device0", "sensor_0");
        // select_list.push_back(path);
        //
        // storage::QueryExpression *query_expr =
        //     storage::QueryExpression::create(select_list, nullptr);
        //
        // storage::TsFileReader reader;
        // ret = reader.open(tsfile_path);
        // ASSERT_EQ(ret, common::E_OK);
        // storage::ResultSet *tmp_qds = nullptr;
        //
        // ret = reader.query(query_expr, tmp_qds);
        // auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;
        //
        // storage::RowRecord *record;
        // bool has_next = false;
        // for (int cur_row = 0; cur_row < max_rows; cur_row++) {
        //     if (IS_FAIL(qds->next(has_next)) || !has_next) {
        //         break;
        //     }
        //     record = qds->get_row_record();
        //     int size = record->get_fields()->size();
        //     for (int i = 0; i < size; ++i) {
        //         EXPECT_EQ(std::to_string(cur_row),
        //                   field_to_string(record->get_field(i)));
        //     }
        // }
        // reader.destroy_query_data_set(qds);
    }
}
