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
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"

namespace storage {
class QDSWithoutTimeGenerator;
}
using namespace storage;
using namespace common;

class TsFileTreeReaderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("tsfile_writer_tree_test_") +
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
};

TEST_F(TsFileTreeReaderTest, BasicTest) {
    TsFileTreeWriter writer(&write_file_);
    std::string device_id = "test_device";
    std::string measurement_id = "test_measurement";
    auto* measurement = new MeasurementSchema(measurement_id, INT64);
    writer.register_timeseries(device_id, measurement);
    TsRecord record(device_id, 0);
    record.add_point(measurement_id, static_cast<int64_t>(1));
    writer.write(record);
    writer.flush();
    writer.close();

    delete measurement;
    TsFileTreeReader reader;
    reader.open(file_name_);
    auto device_ids = reader.get_all_device_ids();
    ASSERT_EQ(device_ids.size(), 1);
    ASSERT_EQ(device_ids[0], device_id);

    std::vector<std::string> measurement_ids{measurement_id};
    ResultSet* result;
    int ret =
        reader.query(device_ids, measurement_ids, INT64_MIN, INT64_MAX, result);
    ASSERT_EQ(ret, E_OK);
    auto iter = result->iterator();
    RowRecord* read_record;
    while (iter.hasNext()) {
        read_record = iter.next();
        EXPECT_EQ(read_record->get_field(1)->type_, INT64);
    }
    reader.destroy_query_data_set(result);
    reader.close();
}

TEST_F(TsFileTreeReaderTest, ExtendedRowsAndColumnsTest) {
    TsFileTreeWriter writer(&write_file_);
    std::vector<std::string> device_ids = {"device_1", "device_2", "device_3"};
    std::vector<std::string> measurement_ids = {"temperature", "humidity",
                                                "pressure", "voltage"};
    std::vector<TSDataType> data_types = {INT64, DOUBLE, FLOAT, INT32};
    std::vector<MeasurementSchema*> measurements;
    for (size_t i = 0; i < measurement_ids.size(); ++i) {
        auto* measurement =
            new MeasurementSchema(measurement_ids[i], data_types[i]);
        measurements.push_back(measurement);
    }
    for (auto& device_id : device_ids) {
        for (auto measurement : measurements) {
            writer.register_timeseries(device_id, measurement);
        }
    }

    const int NUM_ROWS = 100;
    for (int row = 0; row < NUM_ROWS; ++row) {
        for (const auto& device_id : device_ids) {
            TsRecord record(device_id, row * 1000);
            for (size_t i = 0; i < measurement_ids.size(); ++i) {
                switch (data_types[i]) {
                    case INT64:
                        record.add_point(measurement_ids[i],
                                         static_cast<int64_t>(row + i));
                        break;
                    case DOUBLE:
                        record.add_point(measurement_ids[i],
                                         static_cast<double>(row * 1.5 + i));
                        break;
                    case FLOAT:
                        record.add_point(measurement_ids[i],
                                         static_cast<float>(row * 0.8f + i));
                        break;
                    case INT32:
                        record.add_point(measurement_ids[i],
                                         static_cast<int32_t>(row * 2 + i));
                        break;
                    default:
                        break;
                }
            }
            writer.write(record);
        }
    }

    writer.flush();
    writer.close();

    TsFileTreeReader reader;
    reader.open(file_name_);

    auto read_device_ids = reader.get_all_device_ids();
    ASSERT_EQ(read_device_ids.size(), device_ids.size());
    for (size_t i = 0; i < device_ids.size(); ++i) {
        EXPECT_EQ(read_device_ids[i], device_ids[i]);
    }

    ResultSet* result;
    int ret =
        reader.query(device_ids, measurement_ids, 0, NUM_ROWS * 1000, result);
    ASSERT_EQ(ret, E_OK);

    auto iter = result->iterator();
    int row_count = 0;

    while (iter.hasNext()) {
        RowRecord* read_record = iter.next();
        row_count++;
        EXPECT_EQ(read_record->get_fields()->size(),
                  device_ids.size() * measurement_ids.size() + 1);

        // device_id1
        for (size_t i = 0; i < measurement_ids.size(); ++i) {
            Field* field = read_record->get_field(i + 1);
            ASSERT_NE(field, nullptr);
            EXPECT_EQ(field->type_, data_types[i]);

            int64_t timestamp = read_record->get_timestamp();
            int row_index = timestamp / 1000;

            switch (data_types[i]) {
                case INT64: {
                    EXPECT_EQ(field->get_value<int64_t>(),
                              static_cast<int64_t>(row_index + i));
                    break;
                }
                case DOUBLE: {
                    EXPECT_NEAR(field->get_value<double>(), row_index * 1.5 + i,
                                0.001);
                    break;
                }
                case FLOAT: {
                    EXPECT_NEAR(field->get_value<float>(), row_index * 0.8f + i,
                                0.001f);
                    break;
                }
                case INT32: {
                    EXPECT_EQ(field->get_value<int32_t>(),
                              static_cast<int32_t>(row_index * 2 + i));
                    break;
                }
                default:
                    break;
            }
        }
    }
    EXPECT_EQ(row_count * device_ids.size(), NUM_ROWS * device_ids.size());
    reader.destroy_query_data_set(result);
    reader.close();
    for (auto* measurement : measurements) {
        delete measurement;
    }
}
