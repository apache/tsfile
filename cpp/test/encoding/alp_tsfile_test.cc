/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include "common/device_id.h"
#include "common/path.h"
#include "common/record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "gtest/gtest.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_writer.h"

namespace storage {

// Shared deterministic values for the table-model test: fixed-scale decimals
// that ALP encodes without exceptions, so a mismatch cannot be blamed on
// lossy encoding.
static float AlpTestFloatValue(int row) {
    return static_cast<float>(row % 1000) * 0.01f - 5.0f;
}

static double AlpTestDoubleValue(int row) {
    return static_cast<double>(row % 2000) * 0.0001 - 0.1;
}

class AlpTsFileTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        writer_ = new TsFileWriter();
#ifdef _WIN32
        const int pid = _getpid();
#else
        const int pid = static_cast<int>(getpid());
#endif
        file_name_ =
            std::string("alp_tsfile_test_") + std::to_string(pid) + ".tsfile";
        std::remove(file_name_.c_str());
        int flags = O_RDWR | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(common::E_OK, writer_->open(file_name_, flags, 0666));
    }

    void TearDown() override {
        delete writer_;
        writer_ = nullptr;
        std::remove(file_name_.c_str());
        std::remove((file_name_ + ".table").c_str());
        libtsfile_destroy();
    }

    TsFileWriter* writer_ = nullptr;
    std::string file_name_;
};

TEST_F(AlpTsFileTest, FloatAndDoubleRoundTrip) {
    const std::string device = "alp_device";
    const std::string float_name = "f";
    const std::string double_name = "d";
    ASSERT_EQ(
        common::E_OK,
        writer_->register_timeseries(
            device, MeasurementSchema(float_name, common::FLOAT, common::ALP,
                                      common::UNCOMPRESSED)));
    ASSERT_EQ(
        common::E_OK,
        writer_->register_timeseries(
            device, MeasurementSchema(double_name, common::DOUBLE, common::ALP,
                                      common::UNCOMPRESSED)));

    const int row_count = 20000;
    const int64_t base_time = 1700000000000LL;
    for (int i = 0; i < row_count; ++i) {
        TsRecord record(base_time + i, device);
        const float fv = static_cast<float>(i % 1000) * 0.01f - 5.0f;
        const double dv = static_cast<double>(i % 10000) * 0.0001 - 0.5;
        record.add_point(float_name, fv);
        record.add_point(double_name, dv);
        ASSERT_EQ(common::E_OK, writer_->write_record(record));
    }
    ASSERT_EQ(common::E_OK, writer_->flush());
    ASSERT_EQ(common::E_OK, writer_->close());

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(file_name_));
    std::vector<std::string> select_list = {device + "." + float_name,
                                            device + "." + double_name};
    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK, reader.query(select_list, base_time,
                                         base_time + row_count, result));
    ASSERT_NE(nullptr, result);
    auto* qds = static_cast<QDSWithoutTimeGenerator*>(result);

    int rows = 0;
    bool has_next = false;
    while (IS_SUCC(qds->next(has_next)) && has_next) {
        const int i = rows;
        const float expected_f = static_cast<float>(i % 1000) * 0.01f - 5.0f;
        const double expected_d = static_cast<double>(i % 10000) * 0.0001 - 0.5;
        EXPECT_EQ(expected_f, qds->get_value<float>(device + "." + float_name));
        EXPECT_EQ(expected_d,
                  qds->get_value<double>(device + "." + double_name));
        ++rows;
    }
    EXPECT_EQ(row_count, rows);
    reader.destroy_query_data_set(qds);
    ASSERT_EQ(common::E_OK, reader.close());
}

// ALP must also work for the value columns of an aligned chunk group: those
// are decoded by AlignedChunkReader, which validates the encoding byte and
// allocates its decoder through a different path than ChunkReader.
TEST_F(AlpTsFileTest, AlignedDeviceRoundTrip) {
    const std::string device = "alp_aligned_device";
    const std::string float_name = "f";
    const std::string double_name = "d";
    // An aligned device's schema is fixed at the first registration: every
    // column must be registered in one call.
    std::vector<MeasurementSchema*> aligned_schemas = {
        new MeasurementSchema(float_name, common::FLOAT, common::ALP,
                              common::UNCOMPRESSED),
        new MeasurementSchema(double_name, common::DOUBLE, common::ALP,
                              common::UNCOMPRESSED)};
    ASSERT_EQ(common::E_OK,
              writer_->register_aligned_timeseries(device, aligned_schemas));

    const int row_count = 5000;
    const int64_t base_time = 1700000000000LL;
    for (int i = 0; i < row_count; ++i) {
        TsRecord record(base_time + i, device);
        record.add_point(float_name,
                         static_cast<float>(i % 1000) * 0.01f - 5.0f);
        record.add_point(double_name,
                         static_cast<double>(i % 10000) * 0.0001 - 0.5);
        ASSERT_EQ(common::E_OK, writer_->write_record_aligned(record));
    }
    ASSERT_EQ(common::E_OK, writer_->flush());
    ASSERT_EQ(common::E_OK, writer_->close());

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(file_name_));
    std::vector<std::string> select_list = {device + "." + float_name,
                                            device + "." + double_name};
    ResultSet* result = nullptr;
    ASSERT_EQ(common::E_OK, reader.query(select_list, base_time,
                                         base_time + row_count, result));
    ASSERT_NE(nullptr, result);
    auto* qds = static_cast<QDSWithoutTimeGenerator*>(result);

    int rows = 0;
    bool has_next = false;
    while (IS_SUCC(qds->next(has_next)) && has_next) {
        const int i = rows;
        EXPECT_EQ(static_cast<float>(i % 1000) * 0.01f - 5.0f,
                  qds->get_value<float>(device + "." + float_name));
        EXPECT_EQ(static_cast<double>(i % 10000) * 0.0001 - 0.5,
                  qds->get_value<double>(device + "." + double_name));
        ++rows;
    }
    EXPECT_EQ(row_count, rows);
    reader.destroy_query_data_set(qds);
    ASSERT_EQ(common::E_OK, reader.close());
}

// Encoding and compression are stored only in the chunk header on disk, so a
// reader that reports defaults instead of the stored codec would still pass a
// round-trip test. Read the schema back from the file and assert the header
// really says ALP.
TEST_F(AlpTsFileTest, ChunkHeaderStoresAlpEncoding) {
    const std::string device = "alp_wire_device";
#ifdef ENABLE_SNAPPY
    const common::CompressionType double_compression = common::SNAPPY;
#else
    const common::CompressionType double_compression = common::UNCOMPRESSED;
#endif
    ASSERT_EQ(common::E_OK,
              writer_->register_timeseries(
                  device, MeasurementSchema("f", common::FLOAT, common::ALP,
                                            common::UNCOMPRESSED)));
    ASSERT_EQ(common::E_OK,
              writer_->register_timeseries(
                  device, MeasurementSchema("d", common::DOUBLE, common::ALP,
                                            double_compression)));
    for (int i = 0; i < 100; ++i) {
        TsRecord record(1700000000000LL + i, device);
        record.add_point("f", AlpTestFloatValue(i));
        record.add_point("d", AlpTestDoubleValue(i));
        ASSERT_EQ(common::E_OK, writer_->write_record(record));
    }
    ASSERT_EQ(common::E_OK, writer_->flush());
    ASSERT_EQ(common::E_OK, writer_->close());

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(file_name_));
    std::vector<MeasurementSchema> schemas;
    ASSERT_EQ(common::E_OK,
              reader.get_timeseries_schema(
                  std::make_shared<StringArrayDeviceID>(device), schemas));
    ASSERT_EQ(2u, schemas.size());
    for (const MeasurementSchema& schema : schemas) {
        EXPECT_EQ(common::ALP, schema.encoding_)
            << schema.measurement_name_ << " was not stored as ALP";
        if (schema.measurement_name_ == "f") {
            EXPECT_EQ(common::UNCOMPRESSED, schema.compression_type_);
        } else {
            EXPECT_EQ(double_compression, schema.compression_type_);
        }
    }
    ASSERT_EQ(common::E_OK, reader.close());
}

// The table model decodes through AlignedChunkReader's multi-column path,
// including its not-null bitmap and pre-decode logic, which the tree-model
// tests do not exercise. The value pages are compressed too (SNAPPY when the
// build provides it), so this covers ALP combined with compression and with
// NULL rows.
TEST_F(AlpTsFileTest, TableModelRoundTripWithNullsAndCompression) {
    const std::string table_name = "alp_table";
    const std::string table_path = file_name_ + ".table";
    std::remove(table_path.c_str());

    const int rows_per_device = 12000;  // crosses the 10k rows-per-page limit
    const int batch = 3000;
    const int64_t base_time = 1700000000000LL;
#ifdef ENABLE_SNAPPY
    const common::CompressionType value_compression = common::SNAPPY;
#else
    const common::CompressionType value_compression = common::UNCOMPRESSED;
#endif
    int expected_nulls = 0;
    for (int r = 0; r < rows_per_device; ++r) {
        if (r % 7 == 0) {
            ++expected_nulls;
        }
    }
    expected_nulls *= 2;
    {
        WriteFile write_file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(common::E_OK, write_file.create(table_path, flags, 0666));
        std::vector<MeasurementSchema*> column_schemas;
        column_schemas.push_back(new MeasurementSchema(
            "tag", common::STRING, common::PLAIN, common::UNCOMPRESSED));
        column_schemas.push_back(new MeasurementSchema(
            "f", common::FLOAT, common::ALP, value_compression));
        column_schemas.push_back(new MeasurementSchema(
            "d", common::DOUBLE, common::ALP, value_compression));
        std::vector<common::ColumnCategory> column_categories = {
            common::ColumnCategory::TAG, common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD};
        TableSchema table_schema(table_name, column_schemas, column_categories);
        TsFileTableWriter table_writer(&write_file, &table_schema);

        for (int device = 0; device < 2; ++device) {
            for (int start = 0; start < rows_per_device; start += batch) {
                const int end = std::min(start + batch, rows_per_device);
                Tablet tablet(table_schema.get_table_name(),
                              table_schema.get_measurement_names(),
                              table_schema.get_data_types(),
                              table_schema.get_column_categories(),
                              static_cast<uint32_t>(end - start));
                uint32_t index = 0;
                for (int r = start; r < end; ++r, ++index) {
                    ASSERT_EQ(
                        common::E_OK,
                        tablet.add_timestamp(
                            index, base_time +
                                       static_cast<int64_t>(device) * 1000000 +
                                       r));
                    ASSERT_EQ(
                        common::E_OK,
                        tablet.add_value(
                            index, "tag",
                            common::String("dev" + std::to_string(device))));
                    // Every 7th row omits both value columns, so the ALP pages
                    // interleave real values with NULL rows.
                    if (r % 7 != 0) {
                        ASSERT_EQ(
                            common::E_OK,
                            tablet.add_value(index, "f", AlpTestFloatValue(r)));
                        ASSERT_EQ(common::E_OK,
                                  tablet.add_value(index, "d",
                                                   AlpTestDoubleValue(r)));
                    }
                }
                ASSERT_EQ(common::E_OK, table_writer.write_table(tablet));
            }
        }
        ASSERT_EQ(common::E_OK, table_writer.close());
    }

    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(table_path));
    ResultSet* result = nullptr;
    std::vector<std::string> columns = {"tag", "f", "d"};
    ASSERT_EQ(common::E_OK, reader.query(table_name, columns, base_time,
                                         base_time + 2 * 1000000, result));
    ASSERT_NE(nullptr, result);
    int rows = 0;
    int nulls = 0;
    bool has_next = false;
    while (IS_SUCC(result->next(has_next)) && has_next) {
        const int64_t timestamp = result->get_int64_at(1);
        const int device = static_cast<int>((timestamp - base_time) / 1000000);
        const int r = static_cast<int>((timestamp - base_time) % 1000000);
        common::String* tag = result->get_value<common::String*>(2);
        ASSERT_NE(nullptr, tag);
        EXPECT_EQ("dev" + std::to_string(device), tag->to_std_string());
        const bool expect_null = (r % 7 == 0);
        EXPECT_EQ(expect_null, result->is_null(3))
            << "device=" << device << " row=" << r;
        EXPECT_EQ(expect_null, result->is_null(4))
            << "device=" << device << " row=" << r;
        if (expect_null) {
            ++nulls;
        } else {
            EXPECT_EQ(AlpTestFloatValue(r), result->get_float_at(3))
                << "device=" << device << " row=" << r;
            EXPECT_EQ(AlpTestDoubleValue(r), result->get_double_at(4))
                << "device=" << device << " row=" << r;
        }
        ++rows;
    }
    EXPECT_EQ(2 * rows_per_device, rows);
    EXPECT_EQ(expected_nulls, nulls);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(common::E_OK, reader.close());
    std::remove(table_path.c_str());
}

}  // namespace storage
