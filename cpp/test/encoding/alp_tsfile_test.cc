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

#include <cmath>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include "common/path.h"
#include "common/record.h"
#include "common/schema.h"
#include "gtest/gtest.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace storage {

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
        file_name_ = std::string("/tmp/alp_tsfile_test_") +
                     std::to_string(pid) + ".tsfile";
        std::remove(file_name_.c_str());
        ASSERT_EQ(common::E_OK,
                  writer_->open(file_name_, O_RDWR | O_CREAT | O_TRUNC, 0666));
    }

    void TearDown() override {
        delete writer_;
        writer_ = nullptr;
        std::remove(file_name_.c_str());
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

}  // namespace storage
