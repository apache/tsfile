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
#include "file/write_file.h"
#include "large_file_test_common.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

using namespace common;
using namespace storage;
using namespace large_file_test;

namespace {

constexpr char kTableName[] = "large_table";
constexpr char kDeviceTag[] = "device";
constexpr char kValueField[] = "value";

bool VerifyTableRecord(TsFileReader& reader, int64_t record_index) {
    const int64_t timestamp = kStartTime + record_index * 1000;
    ResultSet* tmp_result_set = nullptr;
    int ret = reader.query(kTableName, {kValueField}, timestamp, timestamp + 1,
                           tmp_result_set);
    if (ret != E_OK || tmp_result_set == nullptr) {
        return false;
    }

    auto* table_result_set = static_cast<TableResultSet*>(tmp_result_set);
    bool has_next = false;
    ret = table_result_set->next(has_next);
    if (ret != E_OK || !has_next) {
        reader.destroy_query_data_set(table_result_set);
        return false;
    }

    const int64_t read_time = table_result_set->get_value<int64_t>(1);
    const int64_t read_value = table_result_set->get_value<int64_t>(2);
    reader.destroy_query_data_set(table_result_set);
    return read_time == timestamp && read_value == record_index;
}

TableSchema* CreateTableSchema() {
    std::vector<MeasurementSchema*> measurement_schemas;
    measurement_schemas.push_back(
        new MeasurementSchema(kDeviceTag, TSDataType::STRING, TSEncoding::PLAIN,
                              CompressionType::UNCOMPRESSED));
    measurement_schemas.push_back(
        new MeasurementSchema(kValueField, TSDataType::INT64, TSEncoding::PLAIN,
                              CompressionType::UNCOMPRESSED));
    std::vector<ColumnCategory> column_categories = {ColumnCategory::TAG,
                                                     ColumnCategory::FIELD};
    return new TableSchema(kTableName, measurement_schemas, column_categories);
}

}  // namespace

class LargeFileTableTest : public ::testing::Test {
   protected:
    void SetUp() override {
        libtsfile_init();
        file_name_ = "large_file_table_test_" + RandomSuffix() + ".tsfile";
        remove(file_name_.c_str());
    }

    void TearDown() override {
        remove(file_name_.c_str());
        libtsfile_destroy();
    }

    std::string file_name_;
};

TEST_F(LargeFileTableTest, DISABLED_LargeFile4GB_TableWriteAndRead) {
    std::unique_ptr<TableSchema> table_schema(CreateTableSchema());

    WriteFile write_file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(write_file.create(file_name_, flags, 0666), E_OK);

    TsFileTableWriter table_writer(&write_file, table_schema.get());
    const std::vector<std::string> column_names = {kDeviceTag, kValueField};
    const std::vector<TSDataType> data_types = {TSDataType::STRING,
                                                TSDataType::INT64};

    int64_t total_rows = 0;
    while (GetFileSize(file_name_) < kTargetFileSize) {
        Tablet tablet(column_names, data_types, kTabletRows);
        tablet.set_table_name(kTableName);
        for (uint32_t row = 0; row < kTabletRows; ++row) {
            const int64_t record_index = total_rows + row;
            tablet.add_timestamp(row, kStartTime + record_index * 1000);
            tablet.add_value(row, kDeviceTag, "device0");
            tablet.add_value(row, kValueField, record_index);
        }
        ASSERT_EQ(table_writer.write_table(tablet), E_OK);
        total_rows += kTabletRows;
        if (total_rows % kFlushRows == 0) {
            ASSERT_EQ(table_writer.flush(), E_OK);
        }
    }

    ASSERT_EQ(table_writer.flush(), E_OK);
    ASSERT_EQ(table_writer.close(), E_OK);
    ASSERT_EQ(write_file.close(), E_OK);

    const int64_t final_size = GetFileSize(file_name_);
    ASSERT_GE(final_size, kMinAcceptableFileSize);

    TsFileReader reader;
    ASSERT_EQ(reader.open(file_name_), E_OK);

    const std::vector<int64_t> check_indexes = {0, total_rows / 2,
                                                total_rows - 1};
    for (int64_t index : check_indexes) {
        ASSERT_TRUE(VerifyTableRecord(reader, index)) << "index=" << index;
    }

    ASSERT_EQ(reader.close(), E_OK);
}
