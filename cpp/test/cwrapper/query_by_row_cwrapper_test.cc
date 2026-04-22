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
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "common/global.h"
#include "common/record.h"
#include "common/schema.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"

extern "C" {
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
}

class CWrapperQueryByRowTest : public ::testing::Test {
   protected:
    static void write_tree_tsfile(const char* filename,
                                  const std::vector<std::string>& device_ids,
                                  const std::vector<std::string>& measurements,
                                  int num_rows) {
        storage::WriteFile write_file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        mode_t mode = 0666;
        ASSERT_EQ(common::E_OK, write_file.create(filename, flags, mode));

        storage::TsFileTreeWriter writer(&write_file);
        for (size_t d = 0; d < device_ids.size(); d++) {
            std::string device_id = device_ids[d];
            for (size_t m = 0; m < measurements.size(); m++) {
                auto* schema = new storage::MeasurementSchema(
                    measurements[m], common::TSDataType::INT64);
                ASSERT_EQ(common::E_OK,
                          writer.register_timeseries(device_id, schema));
                delete schema;
            }
        }

        for (int t = 0; t < num_rows; t++) {
            for (size_t d = 0; d < device_ids.size(); d++) {
                storage::TsRecord record(device_ids[d],
                                         static_cast<int64_t>(t));
                for (size_t m = 0; m < measurements.size(); m++) {
                    int64_t value = static_cast<int64_t>(t) * 100 +
                                    static_cast<int64_t>(m) +
                                    static_cast<int64_t>(d) * 10000;
                    record.add_point(measurements[m], value);
                }
                ASSERT_EQ(common::E_OK, writer.write(record));
            }
        }
        ASSERT_EQ(common::E_OK, writer.flush());
        ASSERT_EQ(common::E_OK, writer.close());
    }

    static void write_table_tsfile(const char* filename,
                                   const std::string& table_name,
                                   const std::vector<std::string>& columns,
                                   int num_rows) {
        storage::WriteFile write_file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        mode_t mode = 0666;
        ASSERT_EQ(common::E_OK, write_file.create(filename, flags, mode));

        std::vector<common::ColumnSchema> col_schemas = {
            common::ColumnSchema(columns[0], common::TSDataType::STRING,
                                 common::CompressionType::UNCOMPRESSED,
                                 common::TSEncoding::PLAIN,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema(columns[1], common::TSDataType::INT64,
                                 common::CompressionType::UNCOMPRESSED,
                                 common::TSEncoding::PLAIN,
                                 common::ColumnCategory::FIELD),
        };
        auto* schema = new storage::TableSchema(table_name, col_schemas);
        auto* writer = new storage::TsFileTableWriter(&write_file, schema);

        storage::Tablet tablet(
            table_name, {columns[0], columns[1]},
            {common::TSDataType::STRING, common::TSDataType::INT64},
            {common::ColumnCategory::TAG, common::ColumnCategory::FIELD},
            num_rows);

        for (int t = 0; t < num_rows; t++) {
            tablet.add_timestamp(t, static_cast<int64_t>(t));
            tablet.add_value(t, columns[0],
                             std::string("device_") + std::to_string(t));
            tablet.add_value(t, columns[1], static_cast<int64_t>(t) * 10);
        }

        ASSERT_EQ(common::E_OK, writer->write_table(tablet));
        ASSERT_EQ(common::E_OK, writer->flush());
        ASSERT_EQ(common::E_OK, writer->close());
        delete writer;
        delete schema;
    }
};

TEST_F(CWrapperQueryByRowTest, TreeByRowOffsetLimit) {
    storage::libtsfile_init();

    const char* file_name = "cwrapper_tree_query_by_row_test.tsfile";
    remove(file_name);

    std::vector<std::string> device_ids = {"root.d1", "root.d2"};
    std::vector<std::string> measurements = {"s1", "s2"};
    const int num_rows = 10;
    write_tree_tsfile(file_name, device_ids, measurements, num_rows);

    ERRNO code = 0;
    TsFileReader reader = tsfile_reader_new(file_name, &code);
    ASSERT_EQ(code, RET_OK);
    ASSERT_NE(reader, nullptr);

    char* device_ids_c[2];
    device_ids_c[0] = strdup(device_ids[0].c_str());
    device_ids_c[1] = strdup(device_ids[1].c_str());

    char* measurement_ids_c[2];
    measurement_ids_c[0] = strdup(measurements[0].c_str());
    measurement_ids_c[1] = strdup(measurements[1].c_str());

    const int offset = 3;
    const int limit = 5;
    ResultSet rs = tsfile_reader_query_tree_by_row(
        reader, device_ids_c, 2, measurement_ids_c, 2, offset, limit, &code);
    ASSERT_EQ(code, RET_OK);
    ASSERT_NE(rs, nullptr);

    bool has_next = tsfile_result_set_next(rs, &code);
    ASSERT_EQ(code, RET_OK);
    int row = 0;
    while (has_next) {
        int64_t ts = tsfile_result_set_get_value_by_index_int64_t(rs, 1);
        ASSERT_EQ(ts, static_cast<int64_t>(offset + row));

        ASSERT_EQ(tsfile_result_set_get_value_by_index_int64_t(rs, 2),
                  ts * 100 + 0 + 0 * 10000);
        ASSERT_EQ(tsfile_result_set_get_value_by_index_int64_t(rs, 3),
                  ts * 100 + 1 + 0 * 10000);
        ASSERT_EQ(tsfile_result_set_get_value_by_index_int64_t(rs, 4),
                  ts * 100 + 0 + 1 * 10000);
        ASSERT_EQ(tsfile_result_set_get_value_by_index_int64_t(rs, 5),
                  ts * 100 + 1 + 1 * 10000);

        row++;
        has_next = tsfile_result_set_next(rs, &code);
        ASSERT_EQ(code, RET_OK);
    }

    ASSERT_EQ(row, limit);

    free_tsfile_result_set(&rs);
    ASSERT_EQ(tsfile_reader_close(reader), RET_OK);

    free(device_ids_c[0]);
    free(device_ids_c[1]);
    free(measurement_ids_c[0]);
    free(measurement_ids_c[1]);
    remove(file_name);

    storage::libtsfile_destroy();
}

TEST_F(CWrapperQueryByRowTest, TableByRowOffsetLimit) {
    storage::libtsfile_init();

    const char* file_name = "cwrapper_table_query_by_row_test.tsfile";
    remove(file_name);

    std::string table_name = "t1";
    std::vector<std::string> columns = {"device", "s1"};
    const int num_rows = 10;
    write_table_tsfile(file_name, table_name, columns, num_rows);

    ERRNO code = 0;
    TsFileReader reader = tsfile_reader_new(file_name, &code);
    ASSERT_EQ(code, RET_OK);
    ASSERT_NE(reader, nullptr);

    char* column_names_c[2];
    column_names_c[0] = strdup(columns[0].c_str());
    column_names_c[1] = strdup(columns[1].c_str());

    const int offset = 3;
    const int limit = 5;
    ResultSet rs = tsfile_reader_query_table_by_row(reader, table_name.c_str(),
                                                    column_names_c, 2, offset,
                                                    limit, NULL, 0, &code);
    ASSERT_EQ(code, RET_OK);
    ASSERT_NE(rs, nullptr);

    bool has_next = tsfile_result_set_next(rs, &code);
    ASSERT_EQ(code, RET_OK);

    int row = 0;
    while (has_next) {
        int64_t ts = tsfile_result_set_get_value_by_index_int64_t(rs, 1);
        ASSERT_EQ(ts, static_cast<int64_t>(offset + row));

        char* device = tsfile_result_set_get_value_by_index_string(rs, 2);
        ASSERT_NE(device, nullptr);
        ASSERT_EQ(std::string(device),
                  std::string("device_") + std::to_string(ts));
        free(device);

        ASSERT_EQ(tsfile_result_set_get_value_by_index_int64_t(rs, 3), ts * 10);

        row++;
        has_next = tsfile_result_set_next(rs, &code);
        ASSERT_EQ(code, RET_OK);
    }

    ASSERT_EQ(row, limit);

    free_tsfile_result_set(&rs);
    ASSERT_EQ(tsfile_reader_close(reader), RET_OK);

    free(column_names_c[0]);
    free(column_names_c[1]);
    remove(file_name);

    storage::libtsfile_destroy();
}
