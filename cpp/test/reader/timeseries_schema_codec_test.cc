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

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "common/device_id.h"
#include "common/record.h"
#include "common/schema.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace storage {

// get_timeseries_schema() reports the encoding and the compressor of every
// series it returns. Those two fields are per-chunk properties recorded in the
// chunk header on disk, so the only way to report them correctly is to read
// them back from the file.
//
// The series below are deliberately registered with codecs that differ from
// the library defaults for their data type (INT32 defaults to TS_2DIFF, FLOAT
// to GORILLA, and the default compressor is LZ4 when it is compiled in), so a
// reader that reports defaults instead of the stored values is caught.
class TimeseriesSchemaCodecTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        writer_ = new TsFileWriter();
        file_name_ = std::string("timeseries_schema_codec_test_") +
                     generate_random_string(10) + std::string(".tsfile");
        remove(file_name_.c_str());
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        ASSERT_EQ(writer_->open(file_name_, flags, 0666), common::E_OK);
    }

    void TearDown() override {
        delete writer_;
        writer_ = nullptr;
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    static std::string generate_random_string(int length) {
        std::mt19937 gen(static_cast<unsigned int>(
            std::chrono::system_clock::now().time_since_epoch().count()));
        std::uniform_int_distribution<> dis(0, 61);
        const std::string chars =
            "0123456789"
            "abcdefghijklmnopqrstuvwxyz"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        std::string s;
        s.reserve(static_cast<size_t>(length));
        for (int i = 0; i < length; ++i) {
            s += chars[static_cast<size_t>(dis(gen))];
        }
        return s;
    }

    // Look a measurement up by name: get_timeseries_schema() makes no promise
    // about the order of its result.
    static const MeasurementSchema* find(
        const std::vector<MeasurementSchema>& schemas,
        const std::string& name) {
        for (const auto& s : schemas) {
            if (s.measurement_name_ == name) {
                return &s;
            }
        }
        return nullptr;
    }

    std::string file_name_;
    TsFileWriter* writer_ = nullptr;
};

TEST_F(TimeseriesSchemaCodecTest, ReportsStoredEncodingAndCompressor) {
    const std::string device = "root.codec.dev1";

    // PLAIN/UNCOMPRESSED for an INT32 series: both differ from the defaults
    // that INT32 would otherwise get (TS_2DIFF, and LZ4 where available).
    ASSERT_EQ(
        writer_->register_timeseries(
            device, MeasurementSchema("plain_i32", common::INT32, common::PLAIN,
                                      common::UNCOMPRESSED)),
        common::E_OK);
    // TS_2DIFF on a FLOAT series, whose default encoding is GORILLA.
    ASSERT_EQ(
        writer_->register_timeseries(
            device, MeasurementSchema("ts2diff_float", common::FLOAT,
                                      common::TS_2DIFF, common::UNCOMPRESSED)),
        common::E_OK);

    for (int i = 0; i < 100; ++i) {
        TsRecord record(1622505600000 + i * 1000, device);
        record.add_point("plain_i32", static_cast<int32_t>(i));
        record.add_point("ts2diff_float", static_cast<float>(i) + 0.5f);
        ASSERT_EQ(writer_->write_record(record), common::E_OK);
    }
    ASSERT_EQ(writer_->flush(), common::E_OK);
    ASSERT_EQ(writer_->close(), common::E_OK);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), common::E_OK);

    std::vector<MeasurementSchema> schemas;
    ASSERT_EQ(reader.get_timeseries_schema(
                  std::make_shared<StringArrayDeviceID>(device), schemas),
              common::E_OK);
    ASSERT_EQ(schemas.size(), 2u);

    const MeasurementSchema* i32 = find(schemas, "plain_i32");
    ASSERT_NE(i32, nullptr);
    EXPECT_EQ(i32->data_type_, common::INT32);
    EXPECT_EQ(i32->encoding_, common::PLAIN)
        << "encoding was not read back from the chunk header";
    EXPECT_EQ(i32->compression_type_, common::UNCOMPRESSED)
        << "compressor was not read back from the chunk header";

    const MeasurementSchema* flt = find(schemas, "ts2diff_float");
    ASSERT_NE(flt, nullptr);
    EXPECT_EQ(flt->data_type_, common::FLOAT);
    EXPECT_EQ(flt->encoding_, common::TS_2DIFF)
        << "encoding was not read back from the chunk header";
    EXPECT_EQ(flt->compression_type_, common::UNCOMPRESSED)
        << "compressor was not read back from the chunk header";

    reader.close();
}

// Same requirement for an aligned device: the value columns of an aligned
// chunk group carry their own chunk headers, so the codecs must survive the
// aligned write path too.
TEST_F(TimeseriesSchemaCodecTest, ReportsStoredCodecForAlignedDevice) {
    const std::string device = "root.codec.aligned1";

    ASSERT_EQ(
        writer_->register_aligned_timeseries(
            device, MeasurementSchema("plain_i32", common::INT32, common::PLAIN,
                                      common::UNCOMPRESSED)),
        common::E_OK);

    for (int i = 0; i < 100; ++i) {
        TsRecord record(1622505600000 + i * 1000, device);
        record.add_point("plain_i32", static_cast<int32_t>(i));
        ASSERT_EQ(writer_->write_record_aligned(record), common::E_OK);
    }
    ASSERT_EQ(writer_->flush(), common::E_OK);
    ASSERT_EQ(writer_->close(), common::E_OK);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), common::E_OK);

    std::vector<MeasurementSchema> schemas;
    ASSERT_EQ(reader.get_timeseries_schema(
                  std::make_shared<StringArrayDeviceID>(device), schemas),
              common::E_OK);
    ASSERT_EQ(schemas.size(), 1u);

    // The data type must be the value column type, not the VECTOR placeholder
    // that the aligned time column reports.
    EXPECT_EQ(schemas[0].data_type_, common::INT32);
    EXPECT_EQ(schemas[0].encoding_, common::PLAIN)
        << "encoding was not read back from the chunk header";
    EXPECT_EQ(schemas[0].compression_type_, common::UNCOMPRESSED)
        << "compressor was not read back from the chunk header";

    reader.close();
}

// Every series in the device above is UNCOMPRESSED, so an implementation that
// hard-coded UNCOMPRESSED would still pass. Register two series of the same
// data type that differ only in their codecs: each schema must report its own
// chunk header, not one value shared across the device.
#ifdef ENABLE_SNAPPY
TEST_F(TimeseriesSchemaCodecTest, ReportsCodecPerSeriesNotPerDevice) {
    const std::string device = "root.codec.dev2";

    ASSERT_EQ(
        writer_->register_timeseries(
            device, MeasurementSchema("plain_raw", common::INT32, common::PLAIN,
                                      common::UNCOMPRESSED)),
        common::E_OK);
    ASSERT_EQ(writer_->register_timeseries(
                  device, MeasurementSchema("ts2diff_snappy", common::INT32,
                                            common::TS_2DIFF, common::SNAPPY)),
              common::E_OK);

    for (int i = 0; i < 100; ++i) {
        TsRecord record(1622505600000 + i * 1000, device);
        record.add_point("plain_raw", static_cast<int32_t>(i));
        record.add_point("ts2diff_snappy", static_cast<int32_t>(i * 3));
        ASSERT_EQ(writer_->write_record(record), common::E_OK);
    }
    ASSERT_EQ(writer_->flush(), common::E_OK);
    ASSERT_EQ(writer_->close(), common::E_OK);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), common::E_OK);

    std::vector<MeasurementSchema> schemas;
    ASSERT_EQ(reader.get_timeseries_schema(
                  std::make_shared<StringArrayDeviceID>(device), schemas),
              common::E_OK);
    ASSERT_EQ(schemas.size(), 2u);

    const MeasurementSchema* raw = find(schemas, "plain_raw");
    ASSERT_NE(raw, nullptr);
    EXPECT_EQ(raw->encoding_, common::PLAIN);
    EXPECT_EQ(raw->compression_type_, common::UNCOMPRESSED);

    const MeasurementSchema* snappy = find(schemas, "ts2diff_snappy");
    ASSERT_NE(snappy, nullptr);
    EXPECT_EQ(snappy->encoding_, common::TS_2DIFF);
    EXPECT_EQ(snappy->compression_type_, common::SNAPPY)
        << "the compressor was not taken from this series' own chunk header";

    reader.close();
}
#endif  // ENABLE_SNAPPY

// The measurement name sits between the chunk type byte and the codec bytes in
// the chunk header, so a name long enough to push those bytes past a
// fixed-size read buffer would silently fall back to the defaults.
TEST_F(TimeseriesSchemaCodecTest, ReportsStoredCodecForLongMeasurementName) {
    const std::string device = "root.codec.dev3";
    const std::string long_name(600, 'm');

    ASSERT_EQ(
        writer_->register_timeseries(
            device, MeasurementSchema(long_name, common::INT32, common::PLAIN,
                                      common::UNCOMPRESSED)),
        common::E_OK);

    for (int i = 0; i < 100; ++i) {
        TsRecord record(1622505600000 + i * 1000, device);
        record.add_point(long_name, static_cast<int32_t>(i));
        ASSERT_EQ(writer_->write_record(record), common::E_OK);
    }
    ASSERT_EQ(writer_->flush(), common::E_OK);
    ASSERT_EQ(writer_->close(), common::E_OK);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), common::E_OK);

    std::vector<MeasurementSchema> schemas;
    ASSERT_EQ(reader.get_timeseries_schema(
                  std::make_shared<StringArrayDeviceID>(device), schemas),
              common::E_OK);
    ASSERT_EQ(schemas.size(), 1u);

    const MeasurementSchema* ms = find(schemas, long_name);
    ASSERT_NE(ms, nullptr);
    EXPECT_EQ(ms->encoding_, common::PLAIN)
        << "the chunk header read was truncated by the measurement name";
    EXPECT_EQ(ms->compression_type_, common::UNCOMPRESSED)
        << "the chunk header read was truncated by the measurement name";

    reader.close();
}

}  // namespace storage
