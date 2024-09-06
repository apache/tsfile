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

using namespace storage;
using namespace common;

class TsFileWriterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        tsfile_writer_ = new TsFileWriter();
        libtsfile_init();
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
                                                  "double"};
    std::vector<common::TSDataType> data_types = {FLOAT, INT64, BOOLEAN,
                                                  DOUBLE};
    for (uint32_t i = 0; i < measurement_names.size(); i++) {
        std::string measurement_name = measurement_names[i];
        common::TSDataType data_type = data_types[i];
        tsfile_writer_->register_timeseries(device_name, measurement_name,
                                            data_type, encoding,
                                            compression_type);
    }

    int row_num = 1000;
    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i * 100, device_name);
        for (uint32_t j = 0; j < measurement_names.size(); j++) {
            std::string measurement_name = measurement_names[j];
            common::TSDataType data_type = data_types[j];
            switch (data_type) {
                case BOOLEAN:
                    record.append_data_point(DataPoint(measurement_name, true));
                    break;
                case INT64:
                    record.append_data_point(
                        DataPoint(measurement_name, (int64_t)415412));
                    break;
                case FLOAT:
                    record.append_data_point(
                        DataPoint(measurement_name, (float)1.0));
                    break;
                case DOUBLE:
                    record.append_data_point(
                        DataPoint(measurement_name, (double)2.0));
                    break;
                default:
                    break;
            }
        }
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (uint32_t i = 0; i < measurement_names.size(); ++i) {
        std::string measurement_name = measurement_names[i];
        storage::Path path(device_name, measurement_name);
        select_list.push_back(path);
    }
    storage::QueryExpression *query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::QueryDataSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    int64_t cur_record_num = 0;
    do {
        record = qds->get_next();
        if (!record) {
            break;
        }
        cur_record_num++;
    } while (true);
    EXPECT_EQ(cur_record_num, row_num);
    storage::QueryExpression::destory(query_expr);
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, RegisterTimeSeries) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;

    ASSERT_EQ(tsfile_writer_->register_timeseries(device_path, measurement_name,
                                                  data_type, encoding,
                                                  compression_type),
              E_OK);
}

TEST_F(TsFileWriterTest, WriteMultipleRecords) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    tsfile_writer_->register_timeseries(device_path, measurement_name,
                                        data_type, encoding, compression_type);

    for (int i = 0; i < 50000; ++i) {
        TsRecord record(1622505600000 + i * 1000, device_path);
        DataPoint point(measurement_name, (int32_t)i);
        record.append_data_point(point);
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
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
                device_name, measure_name, common::TSDataType::INT32,
                common::TSEncoding::PLAIN,
                common::CompressionType::UNCOMPRESSED);
        }
    }

    for (int tablet_num = 0; tablet_num < max_tablet_num; tablet_num++) {
        for (int i = 0; i < device_num; i++) {
            std::string device_name = "test_device" + std::to_string(i);
            Tablet tablet(device_name, &schema_vecs[i], 1);
            tablet.init();
            for (int j = 0; j < measurement_num; j++) {
                tablet.set_timestamp(0, 16225600000 + tablet_num * 100);
                tablet.set_value(0, j, static_cast<int32_t>(tablet_num));
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
    storage::QueryDataSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    int max_rows = max_tablet_num * 1;
    for (int cur_row = 0; cur_row < max_rows; cur_row++) {
        record = qds->get_next();
        if (!record) {
            break;
        }
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    storage::QueryExpression::destory(query_expr);
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
                device_name, measure_name, common::TSDataType::INT64,
                common::TSEncoding::PLAIN,
                common::CompressionType::UNCOMPRESSED);
        }
    }

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        int max_rows = 100;
        Tablet tablet(device_name, &schema_vec[i], max_rows);
        tablet.init();
        for (int j = 0; j < measurement_num; j++) {
            for (int row = 0; row < max_rows; row++) {
                tablet.set_timestamp(row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet.set_value(row, j, static_cast<int64_t>(row));
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
                device_name, measure_name, common::TSDataType::DOUBLE,
                common::TSEncoding::PLAIN,
                common::CompressionType::UNCOMPRESSED);
        }
    }

    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        int max_rows = 200;
        Tablet tablet(device_name, &schema_vec[i], max_rows);
        tablet.init();
        for (int j = 0; j < measurement_num; j++) {
            for (int row = 0; row < max_rows; row++) {
                tablet.set_timestamp(row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet.set_value(row, j, static_cast<double>(row) + 1.0);
            }
        }
        ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
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

    ASSERT_EQ(tsfile_writer_->register_timeseries(device_path, measurement_name,
                                                  data_type, encoding,
                                                  compression_type),
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
            DataPoint point(measurement_name, (int32_t)i);
            record.append_data_point(point);
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
    storage::QueryDataSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    for (int cur_row = 0; cur_row < row_num; cur_row++) {
        record = qds->get_next();
        if (!record) {
            break;
        }
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    storage::QueryExpression::destory(query_expr);
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
            DataPoint point(measurement_name, (int32_t)i);
            record.append_data_point(point);
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
    storage::QueryDataSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    for (int cur_row = 0; cur_row < row_num; cur_row++) {
        record = qds->get_next();
        if (!record) {
            break;
        }
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    storage::QueryExpression::destory(query_expr);
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, WriteAlignedPartialData) {
    int measurement_num = 100, row_num = 200;
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
            DataPoint point(measurement_name, (int32_t)i);
            if (i % 2 == 0) {
                point.isnull = true;
            }
            record.append_data_point(point);
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
    storage::QueryDataSet *tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    storage::RowRecord *record;
    int64_t cur_row = 1;
    do {
        record = qds->get_next();
        if (!record) {
            break;
        }
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
        cur_row += 2;
    } while (true);
    storage::QueryExpression::destory(query_expr);
    reader.destroy_query_data_set(qds);
}