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
#include "reader/tsfile_reader.h"

#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "common/path.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/tsfile_io_writer.h"
#include "file/write_file.h"
#include "reader/qds_without_timegenerator.h"
#include "writer/tsfile_writer.h"

using namespace storage;
using namespace common;

class TsFileReaderTest : public ::testing::Test {
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

TEST_F(TsFileReaderTest, ResultSetMetadata) {
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

    std::vector<std::string> select_list = {"device1.temperature"};

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet *tmp_qds = nullptr;

    ret = reader.query(select_list, 1622505600000, 1622505600000 + 50000 * 1000,
                       tmp_qds);
    auto *qds = (QDSWithoutTimeGenerator *)tmp_qds;

    ResultSetMetadata *result_set_metadaa = qds->get_metadata();
    ASSERT_EQ(result_set_metadaa->get_column_type(0), data_type);
    ASSERT_EQ(result_set_metadaa->get_column_name(0),
              device_path + "." + measurement_name);
    reader.destroy_query_data_set(qds);
    reader.close();
}

TEST_F(TsFileReaderTest, GetAllDevice) {
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;

    for (size_t i = 0; i < 1024; i++) {
        tsfile_writer_->register_timeseries(
            "device.ln" + to_string(i),
            storage::MeasurementSchema(measurement_name, data_type, encoding,
                                    compression_type));
    }

    for (size_t i = 0; i < 1024; i++) {
        TsRecord record(1622505600000, "device.ln" + to_string(i));
        record.add_point(measurement_name, (int32_t)0);
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    std::vector<std::string> devices = reader.get_all_devices();
    ASSERT_EQ(devices.size(), 1024);
    std::vector<std::string> devices_name_expected;
    for (size_t i = 0; i < 1024; i++) {
        devices_name_expected.push_back("device.ln" + std::to_string(i));
    }
    std::sort(devices_name_expected.begin(), devices_name_expected.end(), [](const std::string& left_str, const std::string& right_str) {
        return left_str < right_str;
    });

    for (size_t i = 0; i < devices.size(); i++) {
        ASSERT_EQ(devices[i], devices_name_expected[i]);
    }
}

TEST_F(TsFileReaderTest, GetTimeseriesSchema) {
    std::vector<std::string> device_path = {"device", "device.ln"};
    std::vector<std::string> measurement_name = {"temperature", "humidity"};
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    tsfile_writer_->register_timeseries(
        device_path[0],
        storage::MeasurementSchema(measurement_name[0], data_type, encoding,
                                   compression_type));
    tsfile_writer_->register_timeseries(
        device_path[1],
        storage::MeasurementSchema(measurement_name[1], data_type, encoding,
                                   compression_type));
    TsRecord record_0(1622505600000, device_path[0]);
    record_0.add_point(measurement_name[0], (int32_t)0);
    TsRecord record_1(1622505600000, device_path[1]);
    record_1.add_point(measurement_name[1], (int32_t)1);
    ASSERT_EQ(tsfile_writer_->write_record(record_0), E_OK);
    ASSERT_EQ(tsfile_writer_->write_record(record_1), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    std::vector<MeasurementSchema> measurement_schemas;
    reader.get_timeseries_schema(device_path[0], measurement_schemas);
    ASSERT_EQ(measurement_schemas[0].measurement_name_, measurement_name[0]);
    ASSERT_EQ(measurement_schemas[0].data_type_, TSDataType::INT32);

    reader.get_timeseries_schema(device_path[1], measurement_schemas);
    ASSERT_EQ(measurement_schemas[1].measurement_name_, measurement_name[1]);
    ASSERT_EQ(measurement_schemas[1].data_type_, TSDataType::INT32);
    reader.close();
}