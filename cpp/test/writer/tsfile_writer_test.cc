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

using namespace storage;
using namespace common;

class TsFileWriterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        tsfile_writer_ = new TsFileWriter();
        libtsfile_init();
        file_name_ = std::string("tsfile_writer_test_") +
                     generate_random_string(10) + std::string(".dat");
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
    TsFileWriter* tsfile_writer_ = nullptr;

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
};

TEST_F(TsFileWriterTest, InitWithNullWriteFile) {
    TsFileWriter writer;
    ASSERT_EQ(writer.init(nullptr), E_INVALID_ARG);
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
    }
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
        for (int j = 0; j < measurement_num; j++) {
            int max_rows = 100;
            Tablet tablet(device_name, &schema_vec[i], max_rows);
            tablet.init();
            for (int row = 0; row < max_rows; row++) {
                tablet.set_timestamp(row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet.set_value(row, j, row);
            }
            ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
        }
    }

    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    tsfile_writer_->close();
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
        for (int j = 0; j < measurement_num; j++) {
            int max_rows = 200;
            Tablet tablet(device_name, &schema_vec[i], max_rows);
            tablet.init();
            for (int row = 0; row < max_rows; row++) {
                tablet.set_timestamp(row, 16225600 + row);
            }
            for (int row = 0; row < max_rows; row++) {
                tablet.set_value(row, j, (double)row + 1.0);
            }
            ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
        }
    }

    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    tsfile_writer_->close();
}
/*
// TODO: Flushing without writing after registering a timeseries will cause a
core
// dump
TEST_F(TsFileWriterTest, FlushWithoutWriteAfterRegisterTS) {
    TsFileWriter writer;
    writer.init(file_);

    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;

    ASSERT_EQ(writer.register_timeseries(device_path, measurement_name,
                                         data_type, encoding, compression_type),
              E_OK);
    ASSERT_EQ(writer.flush(), E_OK);
    ASSERT_EQ(writer.close(), E_OK);
}
*/
