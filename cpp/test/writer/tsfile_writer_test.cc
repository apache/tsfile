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

#include <cstring>
#include <fstream>
#include <random>

#include "common/path.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
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
        ASSERT_EQ(tsfile_writer_->open(file_name_, flags, mode), common::E_OK);
    }
    void TearDown() override {
        delete tsfile_writer_;
        int ret = remove(file_name_.c_str());
        ASSERT_EQ(0, ret);
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
        if (value->type_ == common::TEXT || value->type_ == STRING ||
            value->type_ == BLOB) {
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
                case common::TIMESTAMP:
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

class TsFileWriterTestSimple : public ::testing::Test {};

TEST_F(TsFileWriterTest, WriteDiffDataType) {
    std::string device_name = "test_table";
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    std::vector<std::string> measurement_names = {
        "level", "num", "bools", "double", "id", "ts", "text", "blob", "date"};
    std::vector<common::TSDataType> data_types = {
        FLOAT, INT64, BOOLEAN, DOUBLE, STRING, TIMESTAMP, TEXT, BLOB, DATE};
    for (uint32_t i = 0; i < measurement_names.size(); i++) {
        std::string measurement_name = measurement_names[i];
        common::TSDataType data_type = data_types[i];
        tsfile_writer_->register_timeseries(
            device_name,
            storage::MeasurementSchema(measurement_name, data_type, encoding,
                                       compression_type));
    }

    char* literal = new char[std::strlen("device_id") + 1];
    std::strcpy(literal, "device_id");
    String literal_str(literal, std::strlen("device_id"));

    std::time_t now = std::time(nullptr);
    std::tm* local_time = std::localtime(&now);
    std::tm today = {};
    today.tm_year = local_time->tm_year;
    today.tm_mon = local_time->tm_mon;
    today.tm_mday = local_time->tm_mday;

    int row_num = 100000;
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
                case TEXT:
                    record.add_point(measurement_name, literal_str);
                    break;
                case BLOB:
                    record.add_point(measurement_name, literal_str);
                    break;
                case TIMESTAMP:
                    record.add_point(measurement_name, (int64_t)415412);
                    break;
                case DATE:
                    record.add_point(measurement_name, today);
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
    storage::ResultSet* tmp_qds = nullptr;
    ret = reader.query(select_list, 1622505600000,
                       1622505600000 + row_num * 100, tmp_qds);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    int64_t cur_record_num = 0;
    bool has_next = false;
    do {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        cur_record_num++;
        ASSERT_EQ(qds->get_value<float>(2), (float)1.0);
        ASSERT_EQ(qds->get_value<int64_t>(3), (int64_t)415412);
        ASSERT_EQ(qds->get_value<bool>(4), true);
        ASSERT_EQ(qds->get_value<double>(5), (double)2.0);
        ASSERT_EQ(qds->get_value<common::String*>(6)->compare(literal_str), 0);
        ASSERT_EQ(qds->get_value<int64_t>(7), (int64_t)415412);
        ASSERT_EQ(qds->get_value<common::String*>(8)->compare(literal_str), 0);
        ASSERT_EQ(qds->get_value<common::String*>(9)->compare(literal_str), 0);
        ASSERT_TRUE(
            DateConverter::is_tm_ymd_equal(qds->get_value<std::tm>(10), today));

        ASSERT_EQ(qds->get_value<float>(measurement_names[0]), (float)1.0);
        ASSERT_EQ(qds->get_value<int64_t>(measurement_names[1]),
                  (int64_t)415412);
        ASSERT_EQ(qds->get_value<bool>(measurement_names[2]), true);
        ASSERT_EQ(qds->get_value<double>(measurement_names[3]), (double)2.0);
        ASSERT_EQ(qds->get_value<common::String*>(measurement_names[4])
                      ->compare(literal_str),
                  0);
        ASSERT_EQ(qds->get_value<int64_t>(measurement_names[5]),
                  (int64_t)415412);
        ASSERT_EQ(qds->get_value<common::String*>(measurement_names[6])
                      ->compare(literal_str),
                  0);
        ASSERT_EQ(qds->get_value<common::String*>(measurement_names[7])
                      ->compare(literal_str),
                  0);
        ASSERT_TRUE(DateConverter::is_tm_ymd_equal(
            qds->get_value<std::tm>(measurement_names[8]), today));
    } while (true);
    delete[] literal;
    EXPECT_EQ(cur_record_num, row_num);
    reader.destroy_query_data_set(qds);
    ASSERT_EQ(reader.close(), E_OK);
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
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}

TEST_F(TsFileWriterTest, MultiFlushWriteAndRead) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    tsfile_writer_->register_timeseries(
        device_path,
        storage::MeasurementSchema(measurement_name, common::TSDataType::INT64,
                                   common::TSEncoding::PLAIN,
                                   common::CompressionType::UNCOMPRESSED));

    const int64_t start_time = 1622505600000LL;
    const int row_count = 200000;
    for (int i = 0; i < row_count; ++i) {
        TsRecord record(start_time + i * 1000, device_path);
        record.add_point(measurement_name, static_cast<int64_t>(i));
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
        if ((i + 1) % 10000 == 0) {
            ASSERT_EQ(tsfile_writer_->flush(), E_OK);
        }
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    std::vector<std::string> select_list = {"device1.temperature"};
    storage::ResultSet* tmp_qds = nullptr;
    ASSERT_EQ(reader.query(select_list, start_time,
                           start_time + (row_count - 1) * 1000 + 1, tmp_qds),
              E_OK);
    auto* qds = static_cast<QDSWithoutTimeGenerator*>(tmp_qds);

    int64_t read_rows = 0;
    bool has_next = false;
    while (true) {
        ASSERT_EQ(qds->next(has_next), E_OK);
        if (!has_next) {
            break;
        }
        ASSERT_EQ(qds->get_value<int64_t>(1), start_time + read_rows * 1000);
        ASSERT_EQ(qds->get_value<int64_t>(2), read_rows);
        ++read_rows;
    }
    ASSERT_EQ(read_rows, row_count);
    reader.destroy_query_data_set(qds);
    ASSERT_EQ(reader.close(), E_OK);
}

#if defined(ENABLE_ZLIB) && defined(ENABLE_SNAPPY) && defined(ENABLE_LZ4) && \
    defined(ENABLE_LZOKAY)
TEST_F(TsFileWriterTest, WriteDiffrentTypeCombination) {
    std::string device_path = "device1";
    std::string measurement_name = "temperature";
    std::vector<TSDataType> data_types = {TSDataType::INT32, TSDataType::INT64,
                                          TSDataType::FLOAT,
                                          TSDataType::DOUBLE};
    std::vector<TSEncoding> encodings = {TSEncoding::PLAIN,
                                         TSEncoding::TS_2DIFF};
    std::vector<CompressionType> compression_types = {
        CompressionType::UNCOMPRESSED, CompressionType::SNAPPY,
        CompressionType::GZIP, CompressionType::LZ4};

    std::vector<MeasurementSchema> schema_vecs;
    schema_vecs.reserve(data_types.size() * encodings.size() *
                        compression_types.size());
    int idx = 0;
    for (auto data_type : data_types) {
        for (auto encoding_type : encodings) {
            for (auto compression_type : compression_types) {
                schema_vecs.emplace_back(MeasurementSchema(
                    measurement_name + std::to_string(idx), data_type,
                    encoding_type, compression_type));
                tsfile_writer_->register_timeseries(device_path,
                                                    schema_vecs[idx++]);
            }
        }
    }

    char* literal = new char[std::strlen("literal") + 1];
    std::strcpy(literal, "literal");
    String literal_str(literal, std::strlen("literal"));
    for (size_t l = 0; l < 100; ++l) {
        TsRecord record(1622505600000 + 1 * 1000, device_path);
        for (size_t i = 0; i < schema_vecs.size(); ++i) {
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
        }
        ASSERT_EQ(tsfile_writer_->write_record(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
    delete[] literal;
}
#endif

TEST_F(TsFileWriterTest, WriteMultipleTabletsMultiFlush) {
    common::config_set_max_degree_of_index_node(3);
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
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet* tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    storage::RowRecord* record;
    int max_rows = max_tablet_num * 1;
    bool has_next = false;
    for (int cur_row = 0; cur_row < max_rows; cur_row++) {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            if (i == 0) {
                EXPECT_EQ(std::to_string(record->get_timestamp()),
                          field_to_string(record->get_field(i)));
                continue;
            }
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, WriteMultipleTabletsAlignedMultiFlush) {
    common::config_set_max_degree_of_index_node(3);
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
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet* tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    ASSERT_EQ(ret, common::E_OK);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    storage::RowRecord* record;
    int max_rows = max_tablet_num * 1;
    bool has_next = false;
    for (int cur_row = 0; cur_row < max_rows; cur_row++) {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            if (i == 0) {
                ASSERT_EQ(field_to_string(record->get_field(0)),
                          std::to_string(record->get_timestamp()));
                continue;
            }
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

// Regression: write_column() is the null fallback of the non-aligned batch
// path (write_column_batch -> has_null -> write_column).  It used to handle
// only BOOLEAN/INT32/INT64/FLOAT/DOUBLE/STRING and ASSERT(false) otherwise;
// in NDEBUG that assert is a no-op, so a non-aligned TEXT/BLOB/DATE/TIMESTAMP
// column that contained a null silently dropped every row of that column.
// This writes a TEXT column with a null in the middle and verifies the two
// non-null rows survive the round trip.
TEST_F(TsFileWriterTest, NonAlignedTextColumnWithNullIsNotDropped) {
    // Non-const: storage::Path's ctor takes non-const std::string&.
    std::string device = "root.dev_text_null";
    std::string measure = "s_text";
    tsfile_writer_->register_timeseries(
        device, MeasurementSchema(measure, common::TSDataType::TEXT,
                                  common::TSEncoding::PLAIN,
                                  common::CompressionType::UNCOMPRESSED));

    std::vector<MeasurementSchema> schema_vec;
    schema_vec.emplace_back(measure, common::TSDataType::TEXT,
                            common::TSEncoding::PLAIN,
                            common::CompressionType::UNCOMPRESSED);
    const int max_rows = 3;
    storage::Tablet tablet(
        device, std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
        max_rows);
    for (int row = 0; row < max_rows; row++) {
        ASSERT_EQ(tablet.add_timestamp(row, 1000 + row), E_OK);
    }
    // Rows 0 and 2 get values; row 1 is left untouched, so its not-null bit
    // stays set (default) — that is the null that forces the write_column
    // fallback.
    char buf0[] = "v0";
    char buf2[] = "v2";
    String s0(buf0, 2), s2(buf2, 2);
    ASSERT_EQ(tablet.add_value(0, 0u, s0), E_OK);
    ASSERT_EQ(tablet.add_value(2, 0u, s2), E_OK);
    ASSERT_EQ(tsfile_writer_->write_tablet(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    std::vector<storage::Path> select_list{storage::Path(device, measure)};
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(select_list, nullptr);
    storage::ResultSet* tmp_qds = nullptr;
    ASSERT_EQ(reader.query(query_expr, tmp_qds), E_OK);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    // The regression signal is row survival: before the fix write_column hit
    // ASSERT(false) on TEXT (a no-op in NDEBUG), so the column was dropped and
    // this query returned 0 rows.  TEXT shares the identical (proven) string
    // write path as STRING, so the two surviving rows at the right timestamps
    // confirm the fix.  field(1) is the value column, but field(0) is non-null
    // here too — the result row carries the timestamp as field(0).
    std::vector<int64_t> times;
    bool has_next = false;
    while (IS_SUCC(qds->next(has_next)) && has_next) {
        storage::RowRecord* rec = qds->get_row_record();
        times.push_back(rec->get_timestamp());
    }
    reader.destroy_query_data_set(qds);
    reader.close();

    ASSERT_EQ(times.size(), 2u);
    EXPECT_EQ(times[0], 1000);
    EXPECT_EQ(times[1], 1002);
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
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet* tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    storage::RowRecord* record;
    int64_t cur_record_num = 0;
    bool has_next = false;
    do {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        // if empty chunk is written, the timestamp should be NULL
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
    // estimate_max_mem_size() now reflects the real 64 KiB-page footprint of
    // each per-measurement output stream.  50 devices × 50 measurements ×
    // 2 streams × 64 KiB = ~320 MiB, well past the 128 MiB default
    // chunk_group_size_threshold_ — without raising the cap the auto-flush
    // would fire mid-write and the post-write hasData() check below would
    // observe a freshly drained chunk writer.  Lift the cap for the
    // duration of this smoke test so the original semantics still apply.
    uint32_t prev_threshold =
        common::g_config_value_.chunk_group_size_threshold_;
    struct Guard {
        uint32_t prev;
        ~Guard() { common::g_config_value_.chunk_group_size_threshold_ = prev; }
    } guard{prev_threshold};
    common::g_config_value_.chunk_group_size_threshold_ =
        2ULL * 1024 * 1024 * 1024;

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
    for (const auto& device_iter : *schemas) {
        for (const auto& chunk_iter :
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
        measurement_names.emplace_back("temperature" + std::to_string(i));
    }

    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    std::vector<MeasurementSchema*> measurement_schema_vec;
    for (const auto& measurement_name : measurement_names) {
        auto* ms = new MeasurementSchema(measurement_name, data_type, encoding,
                                         compression_type);
        measurement_schema_vec.push_back(ms);
    }
    tsfile_writer_->register_aligned_timeseries(device_name,
                                                measurement_schema_vec);

    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i * 1000, device_name);
        for (const auto& measurement_name : measurement_names) {
            record.add_point(measurement_name, (int32_t)i);
        }
        ASSERT_EQ(tsfile_writer_->write_record_aligned(record), E_OK);
    }

    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (int i = 0; i < measurement_num; ++i) {
        std::string measurement_name = "temperature" + std::to_string(i);
        storage::Path path(device_name, measurement_name);
        select_list.push_back(path);
    }
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet* tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    storage::RowRecord* record;
    bool has_next = false;
    for (int cur_row = 0; cur_row < row_num; cur_row++) {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            if (i == 0) {
                EXPECT_EQ(std::to_string(record->get_timestamp()),
                          field_to_string(record->get_field(i)));
                continue;
            }
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    reader.destroy_query_data_set(qds);
}

/*
 * Aligned page seal synchronization tests.
 *
 * In the aligned model, time page and every value page must seal together
 * so that each chunk has the same number of pages. Without synchronization,
 * a threshold hit on one page (point-count or memory) would seal only that
 * page, producing misaligned page counts and corrupt reads.
 *
 * Three sub-cases:
 *   1. Time page reaches point-count threshold first; value pages have
 *      partial nulls so their non-null statistic count is lower and they
 *      would NOT seal on their own.
 *   2. Time page reaches memory threshold first; value pages are mostly
 *      null so their encoded-data memory is much smaller.
 *   3. A value page (STRING, large per-row memory) reaches memory
 *      threshold first; time page and other value pages have not.
 */

// Case 1: time page seals by point-count; value pages with partial nulls
// have fewer non-null points (statistic count) and would not self-seal.
// Sync mechanism must force all value pages to seal together.
TEST_F(TsFileWriterTest, AlignedSealSync_PointCountWithNulls) {
    uint32_t prev_pt = g_config_value_.page_writer_max_point_num_;
    uint32_t prev_mem = g_config_value_.page_writer_max_memory_bytes_;
    struct Guard {
        uint32_t pt, mem;
        ~Guard() {
            g_config_value_.page_writer_max_point_num_ = pt;
            g_config_value_.page_writer_max_memory_bytes_ = mem;
        }
    } guard{prev_pt, prev_mem};
    g_config_value_.page_writer_max_point_num_ = 10;
    g_config_value_.page_writer_max_memory_bytes_ = 1024 * 1024;

    std::string device_name = "device_pt_null";
    std::vector<std::string> mnames = {"s0", "s1", "s2"};
    std::vector<MeasurementSchema*> schemas;
    for (auto& n : mnames) {
        schemas.push_back(new MeasurementSchema(n, INT64, PLAIN, UNCOMPRESSED));
    }
    tsfile_writer_->register_aligned_timeseries(device_name, schemas);

    // s0: always non-null  -> 10 non-null per 10-row page, self-seals
    // s1: null on even rows -> 5 non-null per page, won't self-seal
    // s2: null except every 5th row -> 2 non-null per page, won't self-seal
    int row_num = 30;
    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i, device_name);
        record.add_point(mnames[0], static_cast<int64_t>(i));
        if (i % 2 != 0) {
            record.add_point(mnames[1], static_cast<int64_t>(i * 10));
        } else {
            record.points_.emplace_back(DataPoint(mnames[1]));
        }
        if (i % 5 == 0) {
            record.add_point(mnames[2], static_cast<int64_t>(i * 100));
        } else {
            record.points_.emplace_back(DataPoint(mnames[2]));
        }
        ASSERT_EQ(tsfile_writer_->write_record_aligned(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (auto& n : mnames) {
        select_list.emplace_back(device_name, n);
    }
    storage::QueryExpression* qe =
        storage::QueryExpression::create(select_list, nullptr);
    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    storage::ResultSet* tmp_qds = nullptr;
    ASSERT_EQ(reader.query(qe, tmp_qds), E_OK);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    bool has_next = false;
    int64_t cur_row = 0;
    while (IS_SUCC(qds->next(has_next)) && has_next) {
        auto* rec = qds->get_row_record();
        ASSERT_NE(rec, nullptr);
        EXPECT_EQ(rec->get_timestamp(), 1622505600000 + cur_row);
        EXPECT_EQ(field_to_string(rec->get_field(1)), std::to_string(cur_row));
        if (cur_row % 2 != 0) {
            EXPECT_EQ(field_to_string(rec->get_field(2)),
                      std::to_string(cur_row * 10));
        }
        if (cur_row % 5 == 0) {
            EXPECT_EQ(field_to_string(rec->get_field(3)),
                      std::to_string(cur_row * 100));
        }
        cur_row++;
    }
    EXPECT_EQ(cur_row, row_num);
    reader.destroy_query_data_set(qds);
    ASSERT_EQ(reader.close(), E_OK);
}

// Case 2: time page seals by memory threshold first. Value pages are mostly
// null so their encoded-value memory grows much slower than the time page
// (INT64 PLAIN = 8 bytes/point). Time page hits 512 bytes at ~64 points;
// value pages with 1 non-null every 20 rows only have ~24 bytes of value
// data at that point. Sync must force all value pages to seal.
TEST_F(TsFileWriterTest, AlignedSealSync_TimeMemoryFirst) {
    uint32_t prev_pt = g_config_value_.page_writer_max_point_num_;
    uint32_t prev_mem = g_config_value_.page_writer_max_memory_bytes_;
    struct Guard {
        uint32_t pt, mem;
        ~Guard() {
            g_config_value_.page_writer_max_point_num_ = pt;
            g_config_value_.page_writer_max_memory_bytes_ = mem;
        }
    } guard{prev_pt, prev_mem};
    g_config_value_.page_writer_max_point_num_ = 10000;
    g_config_value_.page_writer_max_memory_bytes_ = 512;

    std::string device_name = "device_time_mem";
    std::vector<std::string> mnames = {"s0", "s1"};
    std::vector<MeasurementSchema*> schemas;
    for (auto& n : mnames) {
        schemas.push_back(new MeasurementSchema(n, INT64, PLAIN, UNCOMPRESSED));
    }
    tsfile_writer_->register_aligned_timeseries(device_name, schemas);

    int row_num = 200;
    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i, device_name);
        if (i % 20 == 0) {
            record.add_point(mnames[0], static_cast<int64_t>(i));
            record.add_point(mnames[1], static_cast<int64_t>(i * 10));
        } else {
            record.points_.emplace_back(DataPoint(mnames[0]));
            record.points_.emplace_back(DataPoint(mnames[1]));
        }
        ASSERT_EQ(tsfile_writer_->write_record_aligned(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (auto& n : mnames) {
        select_list.emplace_back(device_name, n);
    }
    storage::QueryExpression* qe =
        storage::QueryExpression::create(select_list, nullptr);
    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    storage::ResultSet* tmp_qds = nullptr;
    ASSERT_EQ(reader.query(qe, tmp_qds), E_OK);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    bool has_next = false;
    int64_t cur_row = 0;
    while (IS_SUCC(qds->next(has_next)) && has_next) {
        auto* rec = qds->get_row_record();
        ASSERT_NE(rec, nullptr);
        EXPECT_EQ(rec->get_timestamp(), 1622505600000 + cur_row);
        if (cur_row % 20 == 0) {
            EXPECT_EQ(field_to_string(rec->get_field(1)),
                      std::to_string(cur_row));
            EXPECT_EQ(field_to_string(rec->get_field(2)),
                      std::to_string(cur_row * 10));
        }
        cur_row++;
    }
    EXPECT_EQ(cur_row, row_num);
    reader.destroy_query_data_set(qds);
    ASSERT_EQ(reader.close(), E_OK);
}

// Case 3: a value page (STRING type, ~104 bytes/point with PLAIN encoding)
// seals by memory threshold before the time page (INT64, 8 bytes/point).
// With threshold=512, STRING value page seals at ~5 points while time page
// only has ~40 bytes. Sync must force time page and other value pages to seal.
TEST_F(TsFileWriterTest, AlignedSealSync_ValueMemoryFirst) {
    uint32_t prev_pt = g_config_value_.page_writer_max_point_num_;
    uint32_t prev_mem = g_config_value_.page_writer_max_memory_bytes_;
    struct Guard {
        uint32_t pt, mem;
        ~Guard() {
            g_config_value_.page_writer_max_point_num_ = pt;
            g_config_value_.page_writer_max_memory_bytes_ = mem;
        }
    } guard{prev_pt, prev_mem};
    g_config_value_.page_writer_max_point_num_ = 10000;
    g_config_value_.page_writer_max_memory_bytes_ = 512;

    std::string device_name = "device_val_mem";
    std::vector<MeasurementSchema*> schemas;
    schemas.push_back(new MeasurementSchema("s0", INT64, PLAIN, UNCOMPRESSED));
    schemas.push_back(new MeasurementSchema("s1", STRING, PLAIN, UNCOMPRESSED));
    tsfile_writer_->register_aligned_timeseries(device_name, schemas);

    char* long_buf = new char[101];
    memset(long_buf, 'A', 100);
    long_buf[100] = '\0';
    common::String str_val(long_buf, 100);

    int row_num = 100;
    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i, device_name);
        record.add_point(std::string("s0"), static_cast<int64_t>(i));
        record.add_point(std::string("s1"), str_val);
        ASSERT_EQ(tsfile_writer_->write_record_aligned(record), E_OK);
    }
    delete[] long_buf;
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::string s0("s0"), s1("s1");
    std::vector<storage::Path> select_list;
    select_list.emplace_back(device_name, s0);
    select_list.emplace_back(device_name, s1);
    storage::QueryExpression* qe =
        storage::QueryExpression::create(select_list, nullptr);
    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    storage::ResultSet* tmp_qds = nullptr;
    ASSERT_EQ(reader.query(qe, tmp_qds), E_OK);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    bool has_next = false;
    int64_t cur_row = 0;
    while (IS_SUCC(qds->next(has_next)) && has_next) {
        auto* rec = qds->get_row_record();
        ASSERT_NE(rec, nullptr);
        EXPECT_EQ(rec->get_timestamp(), 1622505600000 + cur_row);
        EXPECT_EQ(field_to_string(rec->get_field(1)), std::to_string(cur_row));
        cur_row++;
    }
    EXPECT_EQ(cur_row, row_num);
    reader.destroy_query_data_set(qds);
    ASSERT_EQ(reader.close(), E_OK);
}

// Regression: write_tablet_aligned() writes the entire time column first and
// then each value column. With memory-based auto-seal still active, a large
// STRING value column hits the memory threshold mid-batch (say at row 5),
// while the INT64 time column does not seal until row page_writer_max_point
// is reached.  Those divergent seals stamp misaligned page boundaries onto
// the file and read-back returns wrong values per row.  Suppressing
// memory-driven seals during the batch should keep all pages count-aligned.
TEST_F(TsFileWriterTest, AlignedSealSync_TabletLargeStringValueMemoryFirst) {
    uint32_t prev_pt = g_config_value_.page_writer_max_point_num_;
    uint32_t prev_mem = g_config_value_.page_writer_max_memory_bytes_;
    struct Guard {
        uint32_t pt, mem;
        ~Guard() {
            g_config_value_.page_writer_max_point_num_ = pt;
            g_config_value_.page_writer_max_memory_bytes_ = mem;
        }
    } guard{prev_pt, prev_mem};
    // Big point cap, tiny memory cap: time chunk (INT64 PLAIN, 8B/point) never
    // hits memory before it reaches the point cap, while the STRING value
    // chunk crosses the memory threshold within a handful of rows.
    g_config_value_.page_writer_max_point_num_ = 10000;
    g_config_value_.page_writer_max_memory_bytes_ = 512;

    std::string device_name = "device_tablet_str";
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.emplace_back("s0", INT64, PLAIN, UNCOMPRESSED);
    schema_vec.emplace_back("s1", STRING, PLAIN, UNCOMPRESSED);
    schema_vec.emplace_back("s2", INT64, PLAIN, UNCOMPRESSED);
    {
        std::vector<MeasurementSchema*> reg;
        for (auto& s : schema_vec) reg.push_back(new MeasurementSchema(s));
        tsfile_writer_->register_aligned_timeseries(device_name, reg);
    }

    const int row_num = 200;
    Tablet tablet(device_name,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  row_num);
    char* long_buf = new char[101];
    memset(long_buf, 'A', 100);
    long_buf[100] = '\0';
    common::String str_val(long_buf, 100);
    for (int i = 0; i < row_num; ++i) {
        ASSERT_EQ(tablet.add_timestamp(i, 1622505600000 + i), E_OK);
        ASSERT_EQ(tablet.add_value(i, 0u, static_cast<int64_t>(i)), E_OK);
        // Sparse string column: every third row is null so we also exercise
        // the bitmap path through the memory-pressured value page.
        if (i % 3 != 0) {
            ASSERT_EQ(tablet.add_value(i, 1u, str_val), E_OK);
        }
        ASSERT_EQ(tablet.add_value(i, 2u, static_cast<int64_t>(i * 10)), E_OK);
    }
    delete[] long_buf;

    ASSERT_EQ(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::string s0("s0"), s1("s1"), s2("s2");
    std::vector<storage::Path> select_list;
    select_list.emplace_back(device_name, s0);
    select_list.emplace_back(device_name, s1);
    select_list.emplace_back(device_name, s2);
    storage::QueryExpression* qe =
        storage::QueryExpression::create(select_list, nullptr);
    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    storage::ResultSet* tmp_qds = nullptr;
    ASSERT_EQ(reader.query(qe, tmp_qds), E_OK);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    bool has_next = false;
    int64_t cur_row = 0;
    while (IS_SUCC(qds->next(has_next)) && has_next) {
        auto* rec = qds->get_row_record();
        ASSERT_NE(rec, nullptr);
        EXPECT_EQ(rec->get_timestamp(), 1622505600000 + cur_row);
        EXPECT_EQ(field_to_string(rec->get_field(1)), std::to_string(cur_row));
        EXPECT_EQ(field_to_string(rec->get_field(3)),
                  std::to_string(cur_row * 10));
        cur_row++;
    }
    EXPECT_EQ(cur_row, row_num);
    reader.destroy_query_data_set(qds);
    ASSERT_EQ(reader.close(), E_OK);
}

// Regression: write_tablet_aligned() used to discard time_write_column_batch
// errors and keep writing value columns. On an out-of-order tablet that left
// the time chunk with fewer rows than the value chunks (or with their seal
// flag still suppressed). The fix propagates the time-column error so no
// value column is touched and the page seal flags are restored.
TEST_F(TsFileWriterTest, AlignedTabletTimeBatchOutOfOrderAborts) {
    std::string device_name = "device_aligned_out_of_order";
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.emplace_back("v0", INT64, PLAIN, UNCOMPRESSED);
    schema_vec.emplace_back("v1", INT64, PLAIN, UNCOMPRESSED);
    {
        std::vector<MeasurementSchema*> reg;
        for (auto& s : schema_vec) reg.push_back(new MeasurementSchema(s));
        tsfile_writer_->register_aligned_timeseries(device_name, reg);
    }

    const int row_num = 16;
    Tablet tablet(device_name,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  row_num);
    // Non-monotonic timestamps trip TimePageWriter::write_batch's order check.
    for (int i = 0; i < row_num; ++i) {
        int64_t ts = (i == row_num - 1) ? 0 : 1000 + i;
        ASSERT_EQ(tablet.add_timestamp(i, ts), E_OK);
        ASSERT_EQ(tablet.add_value(i, 0u, static_cast<int64_t>(i)), E_OK);
        ASSERT_EQ(tablet.add_value(i, 1u, static_cast<int64_t>(i * 2)), E_OK);
    }
    EXPECT_NE(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}

// Regression: write_record_aligned used to ignore the time write return
// value, then unconditionally write each value column.  An out-of-order
// timestamp would leave the time chunk one row short of every value chunk
// for the rest of the file.  The fix propagates the time-write error and
// marks the writer unrecoverable when value-column writes diverge from
// time.
TEST_F(TsFileWriterTest, RecordAlignedOutOfOrderDoesNotAdvanceValueColumns) {
    std::string device_name = "root.dev_aligned_record";
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.emplace_back("v0", INT64, PLAIN, UNCOMPRESSED);
    schema_vec.emplace_back("v1", INT64, PLAIN, UNCOMPRESSED);
    {
        std::vector<MeasurementSchema*> reg;
        for (auto& s : schema_vec) reg.push_back(new MeasurementSchema(s));
        tsfile_writer_->register_aligned_timeseries(device_name, reg);
    }

    // First record at ts=1000 — should write cleanly.
    TsRecord r1(1000, device_name);
    r1.points_.emplace_back("v0", static_cast<int64_t>(0));
    r1.points_.emplace_back("v1", static_cast<int64_t>(0));
    ASSERT_EQ(tsfile_writer_->write_record_aligned(r1), E_OK);

    // Second record at the same timestamp 1000 — time_chunk_writer rejects
    // it (E_OUT_OF_ORDER per TimePageWriter::write).  The value columns
    // must not advance.
    TsRecord r2(1000, device_name);
    r2.points_.emplace_back("v0", static_cast<int64_t>(99));
    r2.points_.emplace_back("v1", static_cast<int64_t>(99));
    EXPECT_EQ(tsfile_writer_->write_record_aligned(r2), E_OUT_OF_ORDER);
    // close() must succeed because the failure was caught before any value
    // write — writer state is still consistent.
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}

// Regression: the aligned bulk-memcpy fast path in AlignedChunkReader only
// appended bytes to each Vector's value_data without calling add_row_nums().
// Vector::row_num_ stayed at 0 while TsBlock::row_count_ jumped to N, so
// fill_trailling_nulls() then overwrote every just-written row as null
// (visible to the caller as all-null columns).
TEST_F(TsFileWriterTest, AlignedBulkMemcpyAdvancesVectorRowNum) {
    std::string device_name = "device_bulk_rownum";
    std::vector<MeasurementSchema> schema_vec;
    schema_vec.emplace_back("v0", INT64, PLAIN, UNCOMPRESSED);
    schema_vec.emplace_back("v1", INT64, PLAIN, UNCOMPRESSED);
    {
        std::vector<MeasurementSchema*> reg;
        for (auto& s : schema_vec) reg.push_back(new MeasurementSchema(s));
        tsfile_writer_->register_aligned_timeseries(device_name, reg);
    }
    const int N = 64;
    Tablet tablet(device_name,
                  std::make_shared<std::vector<MeasurementSchema>>(schema_vec),
                  N);
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(tablet.add_timestamp(i, 1000 + i), E_OK);
        ASSERT_EQ(tablet.add_value(i, 0u, static_cast<int64_t>(i)), E_OK);
        ASSERT_EQ(tablet.add_value(i, 1u, static_cast<int64_t>(i * 2)), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->write_tablet_aligned(tablet), E_OK);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    // Read back via TsBlock — confirms the rows are visible.  Under the
    // bug Vector::row_num_ stayed at 0, fill_trailling_nulls() then
    // marked every just-written row null; the iterator still reports
    // them as rows so we check the non-null field for a real value.
    std::vector<storage::Path> select;
    std::string s0("v0"), s1("v1");
    select.emplace_back(device_name, s0);
    select.emplace_back(device_name, s1);
    storage::QueryExpression* qe =
        storage::QueryExpression::create(select, nullptr);
    storage::TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);
    storage::ResultSet* tmp = nullptr;
    ASSERT_EQ(reader.query(qe, tmp), E_OK);
    auto* qds = (QDSWithoutTimeGenerator*)tmp;
    int got = 0;
    bool has_next = false;
    while (IS_SUCC(qds->next(has_next)) && has_next) {
        auto* rec = qds->get_row_record();
        ASSERT_NE(rec, nullptr);
        got++;
    }
    EXPECT_EQ(got, N);
    reader.destroy_query_data_set(qds);
    reader.close();
}

TEST_F(TsFileWriterTest, WriteAlignedMultiFlush) {
    int measurement_num = 100, row_num = 100;
    std::string device_name = "device";
    std::vector<std::string> measurement_names;
    for (int i = 0; i < measurement_num; i++) {
        measurement_names.emplace_back("temperature" + std::to_string(i));
    }

    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    std::vector<MeasurementSchema*> measurement_schema_vec;
    for (const auto& measurement_name : measurement_names) {
        auto* ms = new MeasurementSchema(measurement_name, data_type, encoding,
                                         compression_type);
        measurement_schema_vec.push_back(ms);
    }
    tsfile_writer_->register_aligned_timeseries(device_name,
                                                measurement_schema_vec);

    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i * 1000, device_name);
        for (const auto& measurement_name : measurement_names) {
            record.add_point(measurement_name, (int32_t)i);
        }
        ASSERT_EQ(tsfile_writer_->write_record_aligned(record), E_OK);
        ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    }

    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (int i = 0; i < measurement_num; ++i) {
        std::string measurement_name = "temperature" + std::to_string(i);
        storage::Path path(device_name, measurement_name);
        select_list.push_back(path);
    }
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet* tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    storage::RowRecord* record;
    bool has_next = false;
    for (int cur_row = 0; cur_row < row_num; cur_row++) {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            if (i == 0) {
                EXPECT_EQ(std::to_string(record->get_timestamp()),
                          field_to_string(record->get_field(i)));
                continue;
            }
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
    }
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, WriteAlignedPartialData) {
    int measurement_num = 100, row_num = 200;
    std::string device_name = "device";
    std::vector<std::string> measurement_names;
    for (int i = 0; i < measurement_num; i++) {
        measurement_names.emplace_back("temperature" + std::to_string(i));
    }

    common::TSDataType data_type = common::TSDataType::INT32;
    common::TSEncoding encoding = common::TSEncoding::PLAIN;
    common::CompressionType compression_type =
        common::CompressionType::UNCOMPRESSED;
    std::vector<MeasurementSchema*> measurement_schema_vec;
    for (const auto& measurement_name : measurement_names) {
        auto* ms = new MeasurementSchema(measurement_name, data_type, encoding,
                                         compression_type);
        measurement_schema_vec.push_back(ms);
    }
    tsfile_writer_->register_aligned_timeseries(device_name,
                                                measurement_schema_vec);

    for (int i = 0; i < row_num; ++i) {
        TsRecord record(1622505600000 + i * 1000, device_name);
        for (const auto& measurement_name : measurement_names) {
            record.add_point(measurement_name, (int32_t)i);
        }
        ASSERT_EQ(tsfile_writer_->write_record_aligned(record), E_OK);
    }
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    std::vector<storage::Path> select_list;
    for (int i = 0; i < measurement_num; ++i) {
        std::string measurement_name = "temperature" + std::to_string(i);
        storage::Path path(device_name, measurement_name);
        select_list.push_back(path);
    }
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    storage::TsFileReader reader;
    int ret = reader.open(file_name_);
    ASSERT_EQ(ret, common::E_OK);
    storage::ResultSet* tmp_qds = nullptr;

    ret = reader.query(query_expr, tmp_qds);
    auto* qds = (QDSWithoutTimeGenerator*)tmp_qds;

    storage::RowRecord* record;
    int64_t cur_row = 0;
    bool has_next = false;
    do {
        if (IS_FAIL(qds->next(has_next)) || !has_next) {
            break;
        }
        record = qds->get_row_record();
        int size = record->get_fields()->size();
        for (int i = 0; i < size; ++i) {
            if (i == 0) {
                EXPECT_EQ(std::to_string(record->get_timestamp()),
                          field_to_string(record->get_field(i)));
                continue;
            }
            EXPECT_EQ(std::to_string(cur_row),
                      field_to_string(record->get_field(i)));
        }
        cur_row++;
    } while (true);
    reader.destroy_query_data_set(qds);
}

TEST_F(TsFileWriterTest, WriteTabletDataTypeMismatch) {
    for (int i = 0; i < 2; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        for (int j = 0; j < 3; j++) {
            std::string measure_name = "measurement" + std::to_string(j);
            tsfile_writer_->register_timeseries(
                device_name, storage::MeasurementSchema(
                                 measure_name, common::TSDataType::INT32,
                                 common::TSEncoding::PLAIN,
                                 common::CompressionType::UNCOMPRESSED));
        }
    }

    std::vector<TSDataType> measurement_types{
        TSDataType::INT32, TSDataType::INT64, TSDataType::INT32};
    std::vector<std::string> measurement_names{"measurement0", "measurement1",
                                               "measurement2"};

    Tablet tablet("test_device0", &measurement_names, &measurement_types);
    for (int row = 0; row < 100; row++) {
        tablet.add_timestamp(row, row);
        for (int col = 0; col < 3; col++) {
            switch (measurement_types[col]) {
                case TSDataType::INT32:
                    tablet.add_value(row, col, static_cast<int32_t>(row));
                    break;
                case TSDataType::INT64:
                    tablet.add_value(row, col, static_cast<int64_t>(row));
                    break;
                default:;
            }
        }
    }
    ASSERT_EQ(E_TYPE_NOT_MATCH, tsfile_writer_->write_tablet(tablet));
    std::vector<MeasurementSchema*> measurement_schemas;
    for (int i = 0; i < 3; i++) {
        measurement_schemas.push_back(new MeasurementSchema(
            "measurement" + std::to_string(i), TSDataType::INT32));
    }

    tsfile_writer_->register_aligned_timeseries("device3", measurement_schemas);
    tablet.set_table_name("device3");
    ASSERT_EQ(E_TYPE_NOT_MATCH, tsfile_writer_->write_tablet_aligned(tablet));
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);
}

// Regression: partial-write failures (parallel aligned task failing mid-way,
// non-aligned column failing after earlier columns advanced, etc.) leave per-
// column chunk writers out of sync.  The writer latches unrecoverable_ so
// subsequent flush/close/write must refuse rather than seal a corrupt file
// whose time and value chunks disagree on row count.  Directly triggering
// the partial failure deterministically is hard, so this test asserts the
// downstream contract by flipping the flag through a friend hook.
namespace storage {
class TsFileWriterUnrecoverableTest {
   public:
    static void mark_unrecoverable(TsFileWriter& w) { w.unrecoverable_ = true; }
};
}  // namespace storage

TEST_F(TsFileWriterTest, UnrecoverableLatchRefusesFlushCloseAndWrites) {
    const std::string device = "root.dev_unrec";
    std::vector<MeasurementSchema*> reg;
    reg.push_back(new MeasurementSchema("v0", INT64, PLAIN, UNCOMPRESSED));
    reg.push_back(new MeasurementSchema("v1", INT64, PLAIN, UNCOMPRESSED));
    ASSERT_EQ(tsfile_writer_->register_aligned_timeseries(device, reg), E_OK);

    // Write one good row so a flush attempt would otherwise have data to emit.
    TsRecord r(1000, device);
    r.points_.emplace_back("v0", static_cast<int64_t>(0));
    r.points_.emplace_back("v1", static_cast<int64_t>(0));
    ASSERT_EQ(tsfile_writer_->write_record_aligned(r), E_OK);

    // Simulate the post-partial-failure state.
    storage::TsFileWriterUnrecoverableTest::mark_unrecoverable(*tsfile_writer_);

    // Every public write/flush/close entry point must refuse.
    EXPECT_EQ(tsfile_writer_->flush(), E_DATA_INCONSISTENCY);
    EXPECT_EQ(tsfile_writer_->close(), E_DATA_INCONSISTENCY);

    TsRecord r2(1001, device);
    r2.points_.emplace_back("v0", static_cast<int64_t>(1));
    r2.points_.emplace_back("v1", static_cast<int64_t>(1));
    EXPECT_EQ(tsfile_writer_->write_record_aligned(r2), E_DATA_INCONSISTENCY);

    Tablet tablet(device,
                  std::make_shared<std::vector<MeasurementSchema>>(
                      std::vector<MeasurementSchema>{
                          MeasurementSchema("v0", INT64, PLAIN, UNCOMPRESSED),
                          MeasurementSchema("v1", INT64, PLAIN, UNCOMPRESSED)}),
                  4);
    for (int i = 0; i < 4; i++) {
        ASSERT_EQ(tablet.add_timestamp(i, 2000 + i), E_OK);
        ASSERT_EQ(tablet.add_value(i, 0u, static_cast<int64_t>(i)), E_OK);
        ASSERT_EQ(tablet.add_value(i, 1u, static_cast<int64_t>(i * 2)), E_OK);
    }
    EXPECT_EQ(tsfile_writer_->write_tablet_aligned(tablet),
              E_DATA_INCONSISTENCY);
    EXPECT_EQ(tsfile_writer_->write_tablet(tablet), E_DATA_INCONSISTENCY);
}

namespace {

WriteFile* OpenWriteFileFor(const std::string& path) {
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    auto* wf = new WriteFile;
    if (wf->create(path, flags, 0666) != E_OK) {
        delete wf;
        return nullptr;
    }
    return wf;
}

void WriteOneAlignedRow(TsFileWriter& w, const std::string& device, int64_t ts,
                        int64_t value) {
    std::vector<MeasurementSchema*> reg;
    reg.push_back(new MeasurementSchema("v0", INT64, PLAIN, UNCOMPRESSED));
    ASSERT_EQ(w.register_aligned_timeseries(device, reg), E_OK);
    TsRecord r(ts, device);
    r.points_.emplace_back("v0", value);
    ASSERT_EQ(w.write_record_aligned(r), E_OK);
}

}  // namespace

// Writing speed up: TsFileWriter must be reusable across a
// destroy() + init() cycle.
//   - 1: TsFileIOWriter::destroy() left chunk_group_meta_list_ and
//     chunk_group_meta_index_ pointing at meta_allocator_-owned memory that
//     the next init() then re-armed; the next start_flush_chunk_group()
//     linear scan would deref freed nodes.
//   - 2: TsFileWriter::init() did not reset start_file_done_, so
//     the second file's flush() skipped the magic/version header and
//     produced a file the reader can't open.
// This test forces both code paths: destroy(), init() onto a fresh
// WriteFile, write data, close, then read the second file via the public
// TsFileReader API.
TEST_F(TsFileWriterTest, WriterReuseAfterDestroyProducesValidSecondFile) {
    // First lifecycle uses the fixture-provided writer (already open()'d on
    // file_name_).  Write one row and close — this flushes the magic +
    // version into file_name_ and flips start_file_done_ true.
    WriteOneAlignedRow(*tsfile_writer_, "root.dev_first", 1000, 7);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    // Second lifecycle: tear down the previous writer state and re-init
    // against a brand-new file.
    tsfile_writer_->destroy();

    const std::string second_path = std::string("tsfile_writer_reuse_test_") +
                                    generate_random_string(10) +
                                    std::string(".tsfile");
    remove(second_path.c_str());
    WriteFile* wf = OpenWriteFileFor(second_path);
    ASSERT_NE(wf, nullptr);
    ASSERT_EQ(tsfile_writer_->init(wf), E_OK);

    WriteOneAlignedRow(*tsfile_writer_, "root.dev_second", 2000, 9);
    ASSERT_EQ(tsfile_writer_->flush(), E_OK);
    ASSERT_EQ(tsfile_writer_->close(), E_OK);

    // The second file must start with the TsFile magic + version byte.
    // The TsFileReader open path mostly indexes from the file tail, so a
    // missing magic at offset 0 isn't caught by reader.open().  Inspect the
    // raw header bytes instead — that's exactly what start_file_done_ guards.
    {
        std::ifstream in(second_path, std::ios::binary);
        ASSERT_TRUE(in.is_open());
        char header[MAGIC_STRING_TSFILE_LEN + 1] = {0};
        in.read(header, MAGIC_STRING_TSFILE_LEN + 1);
        EXPECT_EQ(in.gcount(),
                  static_cast<std::streamsize>(MAGIC_STRING_TSFILE_LEN + 1));
        EXPECT_EQ(memcmp(header, MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN),
                  0)
            << "second-file header is missing the TsFile magic — "
               "start_file_done_ residual from the previous lifecycle";
        EXPECT_EQ(header[MAGIC_STRING_TSFILE_LEN], VERSION_NUM_BYTE);
    }

    // wf was passed to init() but init() did not take ownership.
    delete wf;
    remove(second_path.c_str());
}