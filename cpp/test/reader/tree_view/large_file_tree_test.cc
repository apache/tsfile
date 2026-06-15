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

#include <memory>
#include <vector>

#include "common/schema.h"
#include "common/tablet.h"
#include "large_file_test_common.h"
#include "reader/qds_without_timegenerator.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

using namespace common;
using namespace storage;
using namespace large_file_test;

namespace {

bool VerifyTreeRecord(TsFileReader& reader, int64_t record_index) {
    std::vector<std::string> select_list = {"device1.temperature"};
    const int64_t timestamp = kStartTime + record_index * 1000;
    ResultSet* tmp_qds = nullptr;
    int ret = reader.query(select_list, timestamp, timestamp + 1, tmp_qds);
    if (ret != E_OK || tmp_qds == nullptr) {
        return false;
    }

    auto* qds = static_cast<QDSWithoutTimeGenerator*>(tmp_qds);
    bool has_next = false;
    ret = qds->next(has_next);
    if (ret != E_OK || !has_next) {
        reader.destroy_query_data_set(qds);
        return false;
    }

    const int64_t read_time = qds->get_value<int64_t>(1);
    const int64_t read_value = qds->get_value<int64_t>(2);
    reader.destroy_query_data_set(qds);
    return read_time == timestamp && read_value == record_index;
}

}  // namespace

class LargeFileTreeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = "large_file_tree_test_" + RandomSuffix() + ".tsfile";
        remove(file_name_.c_str());
    }

    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    std::string file_name_;
};

TEST_F(LargeFileTreeTest, DISABLED_LargeFile4GB_TreeWriteAndRead) {
    TsFileWriter writer;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(writer.open(file_name_, flags, 0666), E_OK);
    writer.register_timeseries(
        "device1",
        MeasurementSchema("temperature", TSDataType::INT64, TSEncoding::PLAIN,
                          CompressionType::UNCOMPRESSED));

    std::vector<MeasurementSchema> schema = {
        MeasurementSchema("temperature", TSDataType::INT64, TSEncoding::PLAIN,
                          CompressionType::UNCOMPRESSED)};
    auto schema_ptr = std::make_shared<std::vector<MeasurementSchema>>(schema);

    int64_t total_rows = 0;
    while (GetFileSize(file_name_) < kTargetFileSize) {
        Tablet tablet("device1", schema_ptr, kTabletRows);
        for (uint32_t row = 0; row < kTabletRows; ++row) {
            const int64_t record_index = total_rows + row;
            tablet.add_timestamp(row, kStartTime + record_index * 1000);
            tablet.add_value(row, 0, record_index);
        }
        ASSERT_EQ(writer.write_tablet(tablet), E_OK);
        total_rows += kTabletRows;
        if (total_rows % kFlushRows == 0) {
            ASSERT_EQ(writer.flush(), E_OK);
        }
    }

    ASSERT_EQ(writer.flush(), E_OK);
    ASSERT_EQ(writer.close(), E_OK);

    const int64_t final_size = GetFileSize(file_name_);
    ASSERT_GE(final_size, kMinAcceptableFileSize);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);

    const std::vector<int64_t> check_indexes = {0, total_rows / 2,
                                                total_rows - 1};
    for (int64_t index : check_indexes) {
        ASSERT_TRUE(VerifyTreeRecord(reader, index)) << "index=" << index;
    }

    ASSERT_EQ(reader.close(), E_OK);
}
