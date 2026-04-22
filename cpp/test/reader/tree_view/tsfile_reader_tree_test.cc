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
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"

using namespace storage;
using namespace common;

class TsFileTreeReaderTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = std::string("tsfile_writer_tree_reader_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        mode_t mode = 0666;
        write_file_.create(file_name_, flags, mode);
    }

    void TearDown() override { libtsfile_destroy(); }
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

TEST_F(TsFileTreeReaderTest, ReadTreeByTable) {
    TsFileTreeWriter writer(&write_file_);
    std::vector<std::string> device_ids = {"root.db1.t1", "root.db2.t1",
                                           "root.db3.t2.t3", "root.db3.t3",
                                           "device"};
    std::vector<std::string> measurement_ids = {"temperature", "hudi", "level"};
    for (auto& device_id : device_ids) {
        TsRecord record(device_id, 0);
        TsRecord record1(device_id, 1);
        for (auto const& measurement : measurement_ids) {
            auto schema =
                new storage::MeasurementSchema(measurement, TSDataType::INT32);
            ASSERT_EQ(E_OK, writer.register_timeseries(device_id, schema));
            delete schema;
            record.add_point(measurement, static_cast<int64_t>(1));
            record1.add_point(measurement, static_cast<int64_t>(2));
        }
        ASSERT_EQ(E_OK, writer.write(record));
        ASSERT_EQ(E_OK, writer.write(record1));
    }
    writer.flush();
    writer.close();

    TsFileReader reader;
    reader.open(file_name_);
    ResultSet* result;
    int ret = reader.query_table_on_tree({"temperature", "hudi"}, INT64_MIN,
                                         INT64_MAX, result);
    ASSERT_EQ(ret, E_OK);

    auto* table_result_set = (storage::TableResultSet*)result;
    bool has_next = false;
    int col_cnt = table_result_set->get_metadata()->get_column_count();
    std::unordered_map<std::string, std::string> res;
    std::unordered_set<std::string> result_set;
    result_set.insert("rootdb1t1null");
    result_set.insert("rootdb2t1null");
    result_set.insert("rootdb3t2t3");
    result_set.insert("rootdb3t3null");
    result_set.insert("devicenullnullnull");
    int row_cnt = 0;
    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        auto t = table_result_set->get_value<int64_t>(1);
        ASSERT_TRUE(t == 0 || t == 1);
        std::string device_id_string;
        for (int i = 1; i < col_cnt + 1; ++i) {
            switch (table_result_set->get_metadata()->get_column_type(i)) {
                case INT64:
                    ASSERT_TRUE(table_result_set->get_value<int64_t>(i) == 1 ||
                                table_result_set->get_value<int64_t>(i) == 0);
                    break;
                case INT32:
                    ASSERT_TRUE(table_result_set->get_value<int32_t>(i) == 1 ||
                                table_result_set->get_value<int32_t>(i) == 2);
                    break;
                case STRING: {
                    common::String* str =
                        table_result_set->get_value<common::String*>(i);
                    std::string device_id_str;
                    if (str == nullptr) {
                        device_id_str = "null";
                    } else {
                        device_id_str = std::string(str->buf_, str->len_);
                    }
                    device_id_string += device_id_str;
                } break;
                default:
                    break;
            }
        }
        ASSERT_TRUE(result_set.find(device_id_string) != result_set.end());
        row_cnt++;
    }
    ASSERT_EQ(row_cnt, 10);
    reader.destroy_query_data_set(result);
    reader.close();
}

TEST_F(TsFileTreeReaderTest, ReadTreeByTableIrrergular) {
    TsFileTreeWriter writer(&write_file_);
    std::vector<std::string> device_ids = {"root.db1.t1",
                                           "root.db2.t1",
                                           "root.db3.t2.t3",
                                           "root.db3.t3",
                                           "device",
                                           "device.ln",
                                           "device2.ln1.tmp",
                                           "device3.ln2.tmp.v1.v2",
                                           "device3.ln2.tmp.v1.v3"};
    std::vector<std::string> measurement_ids1 = {"temperature", "hudi",
                                                 "level"};
    std::vector<std::string> measurement_ids2 = {"level", "vol"};
    for (int i = 0; i < device_ids.size(); ++i) {
        std::string device_id = device_ids[i];
        TsRecord record(device_id, 0);
        TsRecord record1(device_id, 1);
        std::vector<std::string> measurements =
            (i % 2 == 0) ? measurement_ids1 : measurement_ids2;
        for (auto const& measurement : measurements) {
            auto schema =
                new storage::MeasurementSchema(measurement, TSDataType::INT32);
            ASSERT_EQ(E_OK, writer.register_timeseries(device_id, schema));
            delete schema;
            record.add_point(measurement, static_cast<int64_t>(1));
            record1.add_point(measurement, static_cast<int64_t>(2));
        }
        ASSERT_EQ(E_OK, writer.write(record));
        ASSERT_EQ(E_OK, writer.write(record1));
    }
    writer.flush();
    writer.close();

    TsFileReader reader;
    reader.open(file_name_);
    ResultSet* result;
    int ret = reader.query_table_on_tree({"level", "hudi"}, INT64_MIN,
                                         INT64_MAX, result);
    ASSERT_EQ(ret, E_OK);

    auto* table_result_set = (storage::TableResultSet*)result;
    bool has_next = false;
    int col_cnt = table_result_set->get_metadata()->get_column_count();
    ASSERT_EQ(col_cnt, 8);
    int row_cnt = 0;
    int null_count = 0;
    std::unordered_set<std::string> result_set;
    result_set.insert("rootdb1t1nullnull");
    result_set.insert("rootdb2t1nullnull");
    result_set.insert("rootdb3t2t3null");
    result_set.insert("rootdb3t3nullnull");
    result_set.insert("devicenullnullnullnull");
    result_set.insert("devicelnnullnullnull");
    result_set.insert("device2ln1tmpnullnull");
    result_set.insert("device3ln2tmpv1v2");
    result_set.insert("device3ln2tmpv1v3");

    while (IS_SUCC(table_result_set->next(has_next)) && has_next) {
        auto t = table_result_set->get_value<int64_t>(1);
        ASSERT_TRUE(t == 0 || t == 1);
        std::string device_id_string;
        for (int i = 1; i < col_cnt + 1; ++i) {
            if (table_result_set->is_null(i)) {
                null_count++;
                if (table_result_set->get_metadata()->get_column_type(i) !=
                    STRING) {
                    continue;
                }
            }
            switch (table_result_set->get_metadata()->get_column_type(i)) {
                case INT64:
                    ASSERT_TRUE(table_result_set->get_value<int64_t>(i) == 1 ||
                                table_result_set->get_value<int64_t>(i) == 0);
                    break;
                case INT32:
                    ASSERT_TRUE(table_result_set->get_value<int32_t>(i) == 1 ||
                                table_result_set->get_value<int32_t>(i) == 2);
                    break;
                case STRING: {
                    common::String* str =
                        table_result_set->get_value<common::String*>(i);
                    std::string device_id_str;
                    if (str == nullptr) {
                        device_id_str = "null";
                    } else {
                        device_id_str = std::string(str->buf_, str->len_);
                    }
                    device_id_string += device_id_str;
                } break;
                default:
                    break;
            }
        }
        ASSERT_TRUE(result_set.find(device_id_string) != result_set.end());
        row_cnt++;
    }
    ASSERT_EQ(null_count, 40);
    ASSERT_EQ(row_cnt, 18);
    reader.destroy_query_data_set(result);
    reader.close();
}

TEST_F(TsFileTreeReaderTest, ExtendedRowsAndColumnsTest) {
    TsFileTreeWriter writer(&write_file_);
    std::vector<std::string> device_ids = {"device_1", "device_2", "device_3"};
    std::vector<std::string> measurement_ids = {"temperature", "humidity",
                                                "pressure", "voltage"};
    std::sort(measurement_ids.begin(), measurement_ids.end());
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
    int start_time = 0, end_time = -1;
    for (int row = 0; row < NUM_ROWS; ++row) {
        for (const auto& device_id : device_ids) {
            int timestamp = row * 1000;
            TsRecord record(device_id, timestamp);
            end_time = timestamp;
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

    auto device_timeseries_map = reader.get_timeseries_metadata();
    ASSERT_EQ(device_timeseries_map.size(), device_ids.size());
    auto device_timeseries = device_timeseries_map.at(
        std::make_shared<StringArrayDeviceID>(device_ids[0]));
    ASSERT_EQ(device_timeseries.size(), measurement_ids.size());
    ASSERT_EQ(
        device_timeseries[0]->get_measurement_name().to_std_string(),
        *std::min_element(measurement_ids.begin(), measurement_ids.end()));
    ASSERT_EQ(device_timeseries[0]->get_statistic()->start_time_, start_time);
    ASSERT_EQ(device_timeseries[0]->get_statistic()->end_time_, end_time);
    ASSERT_EQ(device_timeseries[0]->get_statistic()->count_, NUM_ROWS);
    // Verify get_all_device_ids / get_all_devices
    auto read_device_ids = reader.get_all_device_ids();
    ASSERT_EQ(read_device_ids.size(), device_ids.size());
    for (size_t i = 0; i < device_ids.size(); ++i) {
        EXPECT_EQ(read_device_ids[i], device_ids[i]);
    }

    auto device_schema = reader.get_device_schema(device_ids[0]);
    for (int i = 0; i < measurements.size(); ++i) {
        EXPECT_EQ(measurements[i]->measurement_name_,
                  device_schema[i].measurement_name_);
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

// Regression test: query_table_on_tree on a device path with three or more
// dot-segments (e.g. "root.sensors.TH") previously SEGVed because:
// 1. StringArrayDeviceID split "root.sensors.TH" into ["root","sensors","TH"]
//    instead of the correct ["root.sensors","TH"], so get_table_name() returned
//    "root" instead of "root.sensors".
// 2. load_device_index_entry used operator[] on the table map which inserted a
//    null entry, then asserted on it.
TEST_F(TsFileTreeReaderTest, QueryTableOnTreeDeepDevicePath) {
    TsFileTreeWriter writer(&write_file_);
    // Device paths with 3 dot-segments: table_name="root.sensors", device="TH"
    std::string device_id = "root.sensors.TH";
    std::string m_temp = "temperature";
    std::string m_humi = "humidity";
    auto* ms_temp = new MeasurementSchema(m_temp, INT32);
    auto* ms_humi = new MeasurementSchema(m_humi, INT32);
    ASSERT_EQ(E_OK, writer.register_timeseries(device_id, ms_temp));
    ASSERT_EQ(E_OK, writer.register_timeseries(device_id, ms_humi));
    delete ms_temp;
    delete ms_humi;

    for (int ts = 0; ts < 5; ts++) {
        TsRecord rec(device_id, ts);
        rec.add_point(m_temp, static_cast<int32_t>(20 + ts));
        rec.add_point(m_humi, static_cast<int32_t>(50 + ts));
        ASSERT_EQ(E_OK, writer.write(rec));
    }
    writer.flush();
    writer.close();

    TsFileReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));
    ResultSet* result;
    // query_table_on_tree used to SEGV here due to wrong table-name lookup
    ASSERT_EQ(E_OK, reader.query_table_on_tree({m_temp, m_humi}, INT64_MIN,
                                               INT64_MAX, result));

    auto* trs = static_cast<storage::TableResultSet*>(result);
    bool has_next = false;
    int row_cnt = 0;
    while (IS_SUCC(trs->next(has_next)) && has_next) {
        row_cnt++;
    }
    EXPECT_EQ(row_cnt, 5);
    reader.destroy_query_data_set(result);
    reader.close();
}

// Regression test: load_device_index_entry previously used operator[] to look
// up the table node, which silently inserted a null entry and then asserted.
// After the fix it uses find() and returns E_DEVICE_NOT_EXIST gracefully.
// This is triggered when querying a measurement that no device in the file has.
TEST_F(TsFileTreeReaderTest, QueryTableOnTreeMissingMeasurement) {
    // Use the same multi-device setup as ReadTreeByTable to ensure a valid
    // file.
    TsFileTreeWriter writer(&write_file_);
    std::vector<std::string> device_ids = {"root.db1.t1", "root.db2.t1"};
    std::string m_temp = "temperature";
    for (auto dev : device_ids) {
        auto* ms = new MeasurementSchema(m_temp, INT32);
        ASSERT_EQ(E_OK, writer.register_timeseries(dev, ms));
        delete ms;
        TsRecord rec(dev, 0);
        rec.add_point(m_temp, static_cast<int32_t>(25));
        ASSERT_EQ(E_OK, writer.write(rec));
    }
    writer.flush();
    writer.close();

    TsFileReader reader;
    ASSERT_EQ(E_OK, reader.open(file_name_));
    ResultSet* result = nullptr;
    // "nonexistent" is not present in any device. Before the fix,
    // load_device_index_entry used operator[] which inserted null and crashed.
    // After the fix it returns E_DEVICE_NOT_EXIST or E_COLUMN_NOT_EXIST.
    int ret = reader.query_table_on_tree({"nonexistent"}, INT64_MIN, INT64_MAX,
                                         result);
    EXPECT_NE(ret, E_OK);  // Must not succeed (measurement not found)
    if (result != nullptr) {
        reader.destroy_query_data_set(result);
    }
    reader.close();
}
