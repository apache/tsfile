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

#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "common/schema.h"
#include "common/tablet.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

using namespace storage;

namespace {

// Multi-byte UTF-8 in the device and measurement names reaches the bloom
// filter tail, where Java and C++ used to disagree on byte sign extension.
const char* const kDevice = "root.测试.设备1";
const char* const kTemperature = "温度";
const char* const kHumidity = "湿度";
const char* const kRoundTripFile = "non_ascii_path_roundtrip.tsfile";

// Returns the number of rows, or -1 when the query itself failed.
int64_t CountRows(TsFileReader& reader, const std::vector<std::string>& paths) {
    std::vector<std::string> query_paths(paths);
    ResultSet* result = nullptr;
    if (reader.query(query_paths, INT64_MIN, INT64_MAX, result) !=
            common::E_OK ||
        result == nullptr) {
        return -1;
    }
    int64_t rows = 0;
    bool has_next = false;
    while (result->next(has_next) == common::E_OK && has_next) {
        ++rows;
    }
    reader.destroy_query_data_set(result);
    return rows;
}

}  // namespace

// The fixture was written by the Java implementation (TsFile 2.5.0) with:
//   device       root.测试.设备1
//   measurements 温度, 湿度 (DOUBLE, GORILLA, LZ4)
//   rows         10, timestamps 1000..1009, values 20+i and 50+i
// The reader must not lose a series just because the path is non-ASCII.
TEST(NonAsciiPathTest, ReadsJavaWrittenNonAsciiPaths) {
    libtsfile_init();
    const std::string fixture = std::string(TSFILE_CPP_SOURCE_DIR) +
                                "/test/reader/fixtures/non_ascii_tree.tsfile";
    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(fixture));

    const std::string device(kDevice);
    EXPECT_EQ(10, CountRows(reader, {device + "." + kTemperature}));
    EXPECT_EQ(10, CountRows(reader, {device + "." + kHumidity}));
    EXPECT_EQ(10, CountRows(
                      reader, {device + "." + kTemperature,
                               device + "." + kHumidity}));
    reader.close();
}

TEST(NonAsciiPathTest, RoundTripsNonAsciiPaths) {
    libtsfile_init();
    std::remove(kRoundTripFile);
    {
        TsFileWriter writer;
        ASSERT_EQ(common::E_OK, writer.open(kRoundTripFile));
        MeasurementSchema temperature(kTemperature, common::DOUBLE,
                                      common::GORILLA, common::LZ4);
        MeasurementSchema humidity(kHumidity, common::DOUBLE, common::GORILLA,
                                   common::LZ4);
        ASSERT_EQ(common::E_OK,
                  writer.register_timeseries(kDevice, temperature));
        ASSERT_EQ(common::E_OK, writer.register_timeseries(kDevice, humidity));

        auto schemas = std::make_shared<std::vector<MeasurementSchema>>();
        schemas->push_back(temperature);
        schemas->push_back(humidity);
        Tablet tablet(kDevice, schemas);
        for (int i = 0; i < 10; i++) {
            tablet.add_timestamp(static_cast<uint32_t>(i), 1000 + i);
            tablet.add_value(static_cast<uint32_t>(i), 0U, 20.0 + i);
            tablet.add_value(static_cast<uint32_t>(i), 1U, 50.0 + i);
        }
        ASSERT_EQ(common::E_OK, writer.write_tablet(tablet));
        writer.close();
    }
    {
        TsFileReader reader;
        ASSERT_EQ(common::E_OK, reader.open(kRoundTripFile));
        const std::string device(kDevice);
        EXPECT_EQ(10, CountRows(reader, {device + "." + kTemperature,
                                         device + "." + kHumidity}));
        reader.close();
    }
    std::remove(kRoundTripFile);
}
