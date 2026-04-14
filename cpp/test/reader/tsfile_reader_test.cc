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
#include <sys/stat.h>

#include <map>
#include <random>
#include <unordered_map>
#include <vector>

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
        // remove(file_name_.c_str());
        libtsfile_destroy();
    }

    std::string file_name_;
    TsFileWriter* tsfile_writer_ = nullptr;

   public:
    static std::string generate_random_string(int length) {
        std::mt19937 gen(static_cast<unsigned int>(
            std::chrono::system_clock::now().time_since_epoch().count()));
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

    static std::string field_to_string(storage::Field* value) {
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
    storage::ResultSet* tmp_qds = nullptr;

    ret = reader.query(select_list, 1622505600000, 1622505600000 + 50000 * 1000,
                       tmp_qds);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    std::shared_ptr<ResultSetMetadata> result_set_metadata =
        qds->get_metadata();
    ASSERT_EQ(result_set_metadata->get_column_type(1), INT64);
    ASSERT_EQ(result_set_metadata->get_column_name(1), "time");
    ASSERT_EQ(result_set_metadata->get_column_type(2), data_type);
    ASSERT_EQ(result_set_metadata->get_column_name(2),
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
            "device.ln" + std::to_string(i),
            storage::MeasurementSchema(measurement_name, data_type, encoding,
                                       compression_type));
    }

    for (size_t i = 0; i < 1024; i++) {
        TsRecord record(1622505600000, "device.ln" + std::to_string(i));
        record.add_point(measurement_name, (int32_t)0);
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    auto devices = reader.get_all_devices("device");
    ASSERT_EQ(devices.size(), 1024);
    std::vector<std::shared_ptr<IDeviceID>> devices_name_expected;
    for (size_t i = 0; i < 1024; i++) {
        devices_name_expected.push_back(std::make_shared<StringArrayDeviceID>(
            "device.ln" + std::to_string(i)));
    }
    std::sort(devices_name_expected.begin(), devices_name_expected.end(),
              [](const std::shared_ptr<IDeviceID>& left_str,
                 const std::shared_ptr<IDeviceID>& right_str) {
                  return left_str->operator<(*right_str);
              });

    for (size_t i = 0; i < devices.size(); i++) {
        ASSERT_TRUE(devices[i]->operator==(*devices_name_expected[i]));
    }
}

TEST_F(TsFileReaderTest, GetTimeseriesSchema) {
    std::vector<std::string> device_path = {"device.ln1", "device.ln2 "};
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
    reader.get_timeseries_schema(
        std::make_shared<StringArrayDeviceID>(device_path[0]),
        measurement_schemas);
    ASSERT_EQ(measurement_schemas[0].measurement_name_, measurement_name[0]);
    ASSERT_EQ(measurement_schemas[0].data_type_, TSDataType::INT32);

    reader.get_timeseries_schema(
        std::make_shared<StringArrayDeviceID>(device_path[1]),
        measurement_schemas);
    ASSERT_EQ(measurement_schemas[1].measurement_name_, measurement_name[1]);
    ASSERT_EQ(measurement_schemas[1].data_type_, TSDataType::INT32);

    std::vector<std::shared_ptr<IDeviceID>> one_device = {
        std::make_shared<StringArrayDeviceID>(device_path[0])};
    auto one_meta = reader.get_timeseries_metadata(one_device);
    ASSERT_EQ(one_meta.size(), 1u);
    auto timeseries_list = one_meta.begin()->second;
    ASSERT_EQ(timeseries_list.size(), 1u);
    ASSERT_EQ(timeseries_list[0]->get_measurement_name().to_std_string(),
              measurement_name[0]);
    ASSERT_EQ(timeseries_list[0]->get_statistic()->start_time_, 1622505600000);
    ASSERT_EQ(timeseries_list[0]->get_statistic()->end_time_, 1622505600000);
    ASSERT_EQ(timeseries_list[0]->get_statistic()->count_, 1);

    auto device_timeseries_map = reader.get_timeseries_metadata();
    ASSERT_EQ(device_timeseries_map.size(), 2u);
    auto device_timeseries_1 = device_timeseries_map.at(
        std::make_shared<StringArrayDeviceID>(device_path[1]));
    ASSERT_EQ(device_timeseries_1.size(), 1u);
    ASSERT_EQ(device_timeseries_1[0]->get_measurement_name().to_std_string(),
              measurement_name[1]);
    ASSERT_EQ(device_timeseries_1[0]->get_statistic()->start_time_,
              1622505600000);
    ASSERT_EQ(device_timeseries_1[0]->get_statistic()->end_time_,
              1622505600000);
    ASSERT_EQ(device_timeseries_1[0]->get_statistic()->count_, 1);
    reader.close();
}

TEST_F(TsFileReaderTest, GetTimeseriesMetadataTableModelTypeAndDeviceFilter) {
    std::vector<MeasurementSchema*> measurement_schemas = {
        new MeasurementSchema("deviceid1", TSDataType::STRING),
        new MeasurementSchema("deviceid2", TSDataType::STRING),
        new MeasurementSchema("temperature", TSDataType::FLOAT),
        new MeasurementSchema("pressure", TSDataType::DOUBLE),
        new MeasurementSchema("humidity", TSDataType::INT32)};
    std::vector<ColumnCategory> column_categories = {
        ColumnCategory::TAG, ColumnCategory::TAG, ColumnCategory::FIELD,
        ColumnCategory::FIELD, ColumnCategory::FIELD};
    auto table_schema = std::make_shared<TableSchema>(
        "testtable", measurement_schemas, column_categories);

    ASSERT_EQ(tsfile_writer_->register_table(table_schema), E_OK);

    Tablet tablet(table_schema->get_table_name(),
                  table_schema->get_measurement_names(),
                  table_schema->get_data_types(),
                  table_schema->get_column_categories(), 10);
    for (int row = 0; row < 5; row++) {
        ASSERT_EQ(tablet.add_timestamp(row, row), E_OK);
        ASSERT_EQ(tablet.add_value(row, "deviceid1", "device_a"), E_OK);
        ASSERT_EQ(tablet.add_value(row, "deviceid2", "device_b"), E_OK);
        ASSERT_EQ(tablet.add_value(row, "temperature", static_cast<float>(row)),
                  E_OK);
        ASSERT_EQ(tablet.add_value(row, "pressure", static_cast<double>(row)),
                  E_OK);
        ASSERT_EQ(tablet.add_value(row, "humidity", static_cast<int32_t>(row)),
                  E_OK);
    }
    for (int row = 5; row < 10; row++) {
        ASSERT_EQ(tablet.add_timestamp(row, row), E_OK);
        ASSERT_EQ(tablet.add_value(row, "deviceid1", "device_b"), E_OK);
        ASSERT_EQ(tablet.add_value(row, "deviceid2", "device_a"), E_OK);
        ASSERT_EQ(tablet.add_value(row, "temperature", static_cast<float>(row)),
                  E_OK);
        ASSERT_EQ(tablet.add_value(row, "pressure", static_cast<double>(row)),
                  E_OK);
        ASSERT_EQ(tablet.add_value(row, "humidity", static_cast<int32_t>(row)),
                  E_OK);
    }

    // Append one row whose middle TAG segment is null.
    Tablet null_tag_tablet(table_schema->get_table_name(),
                           table_schema->get_measurement_names(),
                           table_schema->get_data_types(),
                           table_schema->get_column_categories(), 1);
    int64_t null_tag_ts[1] = {10};
    int32_t null_tag_humidity[1] = {10};
    float null_tag_temperature[1] = {10.0F};
    double null_tag_pressure[1] = {10.0};
    // deviceid1 = null
    int32_t id1_offsets[2] = {0, 0};
    uint8_t id1_bitmap[1] = {0x01};  // row0 is null
    // deviceid2 = "device_b"
    int32_t id2_offsets[2] = {0, 8};
    const char id2_data[] = "device_b";
    ASSERT_EQ(null_tag_tablet.set_timestamps(null_tag_ts, 1), E_OK);
    ASSERT_EQ(null_tag_tablet.set_column_string_values(0, id1_offsets, "",
                                                       id1_bitmap, 1),
              E_OK);
    ASSERT_EQ(null_tag_tablet.set_column_string_values(1, id2_offsets, id2_data,
                                                       nullptr, 1),
              E_OK);
    ASSERT_EQ(
        null_tag_tablet.set_column_values(2, null_tag_temperature, nullptr, 1),
        E_OK);
    ASSERT_EQ(
        null_tag_tablet.set_column_values(3, null_tag_pressure, nullptr, 1),
        E_OK);
    ASSERT_EQ(
        null_tag_tablet.set_column_values(4, null_tag_humidity, nullptr, 1),
        E_OK);

    ASSERT_EQ(tsfile_writer_->write_table(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->write_table(null_tag_tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), common::E_OK);

    auto all_meta = reader.get_timeseries_metadata();
    ASSERT_EQ(all_meta.size(), 3u);

    std::vector<std::string> selected_device_segments = {
        "testtable", "device_a", "device_b"};
    std::vector<std::shared_ptr<IDeviceID>> selected_devices = {
        std::make_shared<StringArrayDeviceID>(selected_device_segments)};
    auto selected_meta = reader.get_timeseries_metadata(selected_devices);
    ASSERT_EQ(selected_meta.size(), 1u);

    auto selected_list = selected_meta.begin()->second;
    std::unordered_map<std::string, TSDataType> type_by_measurement;
    for (const auto& index : selected_list) {
        type_by_measurement[index->get_measurement_name().to_std_string()] =
            index->get_data_type();
    }
    ASSERT_EQ(type_by_measurement.at("temperature"), TSDataType::FLOAT);
    ASSERT_EQ(type_by_measurement.at("pressure"), TSDataType::DOUBLE);
    ASSERT_EQ(type_by_measurement.at("humidity"), TSDataType::INT32);

    // Query metadata for the device with null middle TAG segment.
    std::vector<std::string*> null_seg_device = {
        new std::string("testtable"), nullptr, new std::string("device_b")};
    std::vector<std::shared_ptr<IDeviceID>> null_seg_devices = {
        std::make_shared<StringArrayDeviceID>(null_seg_device)};
    for (auto* seg : null_seg_device) {
        if (seg != nullptr) {
            delete seg;
        }
    }
    auto null_seg_meta = reader.get_timeseries_metadata(null_seg_devices);
    ASSERT_EQ(null_seg_meta.size(), 1u);
    auto null_seg_list = null_seg_meta.begin()->second;
    ASSERT_EQ(null_seg_list.size(), 3u);
    std::unordered_map<std::string, TSDataType> null_seg_type_by_measurement;
    for (const auto& index : null_seg_list) {
        null_seg_type_by_measurement[index->get_measurement_name()
                                         .to_std_string()] =
            index->get_data_type();
    }
    ASSERT_EQ(null_seg_type_by_measurement.at("temperature"),
              TSDataType::FLOAT);
    ASSERT_EQ(null_seg_type_by_measurement.at("pressure"), TSDataType::DOUBLE);
    ASSERT_EQ(null_seg_type_by_measurement.at("humidity"), TSDataType::INT32);

    reader.close();
}

static const int64_t kLargeFileNumRecords = 300000000;
static const int64_t kLargeFileFlushBatch = 100000;

TEST_F(TsFileReaderTest,
       DISABLED_LargeFileNoEncodingNoCompression_WriteAndRead) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    common::TSDataType data_type = common::TSDataType::INT64;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;

    tsfile_writer_->register_timeseries(
        device_path, storage::MeasurementSchema(measurement_name, data_type,
                                                encoding, compression_type));

    const int64_t start_time = 1622505600000LL;
    for (int64_t i = 0; i < kLargeFileNumRecords; ++i) {
        TsRecord record(start_time + i * 1000, device_path);
        record.add_point(measurement_name, static_cast<int64_t>(i));
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
        if ((i + 1) % kLargeFileFlushBatch == 0) {
            ASSERT_EQ(tsfile_writer_->flush(), E_OK);
        }
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<std::string> select_list = {"device1.temperature"};
    const int64_t end_time = start_time + (kLargeFileNumRecords - 1) * 1000 + 1;

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);

    storage::ResultSet* tmp_qds = nullptr;
    ret = reader.query(select_list, start_time, end_time, tmp_qds);
    ASSERT_EQ(ret, common::E_OK);
    ASSERT_NE(tmp_qds, nullptr);

    auto* qds = static_cast<QDSWithoutTimeGenerator*>(tmp_qds);
    std::shared_ptr<ResultSetMetadata> meta = qds->get_metadata();
    ASSERT_NE(meta, nullptr);
    ASSERT_EQ(meta->get_column_type(1), INT64);
    ASSERT_EQ(meta->get_column_type(2), INT64);

    int64_t row_count = 0;
    bool has_next = false;

    while (true) {
        ret = qds->next(has_next);
        ASSERT_EQ(ret, common::E_OK);
        if (!has_next) break;
        row_count++;
    }

    ASSERT_EQ(row_count, kLargeFileNumRecords);

    reader.destroy_query_data_set(qds);
    reader.close();
}
