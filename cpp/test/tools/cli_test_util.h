/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#ifndef TSFILE_CLI_TEST_UTIL_H
#define TSFILE_CLI_TEST_UTIL_H

#include <fcntl.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"

namespace tsfile_cli_test {

// Unique per-process path so tests stay isolated when ctest runs the
// gtest-discovered cases in parallel processes.
inline std::string unique_temp_path(const std::string& stem,
                                    const std::string& ext) {
    static unsigned counter = 0;
#ifdef _WIN32
    long pid = static_cast<long>(_getpid());
#else
    long pid = static_cast<long>(getpid());
#endif
    std::ostringstream ss;
    ss << stem << "_" << pid << "_" << counter++ << ext;
    return ss.str();
}

inline std::string write_table_fixture() {
    storage::libtsfile_init();
    std::string out_path = unique_temp_path("tsfile_cli_fixture", ".tsfile");
    std::string table_name = "table1";

    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(out_path, flags, 0666);

    auto* schema = new storage::TableSchema(
        table_name,
        {
            common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("id2", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("s1", common::INT64, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::FIELD),
        });

    auto* writer = new storage::TsFileTableWriter(&file, schema);
    storage::Tablet tablet(
        table_name, {"id1", "id2", "s1"},
        {common::STRING, common::STRING, common::INT64},
        {common::ColumnCategory::TAG, common::ColumnCategory::TAG,
         common::ColumnCategory::FIELD},
        10);

    for (int row = 0; row < 5; ++row) {
        tablet.add_timestamp(row, static_cast<int64_t>(row));
        tablet.add_value(row, "id1", "id1_field_1");
        tablet.add_value(row, "id2", "id2_field_2");
        tablet.add_value(row, "s1", static_cast<int64_t>(row * 10));
    }

    writer->write_table(tablet);
    writer->flush();
    writer->close();

    delete writer;
    delete schema;
    return out_path;
}

inline std::string write_tag_filter_fixture() {
    storage::libtsfile_init();
    std::string out_path =
        unique_temp_path("tsfile_cli_tag_filter_fixture", ".tsfile");
    std::string table_name = "t1";

    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(out_path, flags, 0666);

    auto* schema = new storage::TableSchema(
        table_name,
        {
            common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("s1", common::INT64, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::FIELD),
        });

    auto* writer = new storage::TsFileTableWriter(&file, schema);
    storage::Tablet tablet(
        table_name, {"id1", "s1"}, {common::STRING, common::INT64},
        {common::ColumnCategory::TAG, common::ColumnCategory::FIELD}, 10);

    const char* tags[] = {"dev_a", "dev_b", "dev_b", "dev_c"};
    for (int row = 0; row < 4; ++row) {
        tablet.add_timestamp(row, static_cast<int64_t>(row));
        tablet.add_value(row, "id1", tags[row]);
        tablet.add_value(row, "s1", static_cast<int64_t>((row + 1) * 10));
    }

    writer->write_table(tablet);
    writer->flush();
    writer->close();

    delete writer;
    delete schema;
    return out_path;
}

inline void write_one_table_row(storage::TsFileTableWriter* writer,
                                const std::string& table_name,
                                const std::string& field_name, int64_t time,
                                int64_t value) {
    storage::Tablet tablet(
        table_name, {"id1", field_name}, {common::STRING, common::INT64},
        {common::ColumnCategory::TAG, common::ColumnCategory::FIELD}, 1);
    tablet.add_timestamp(0, time);
    tablet.add_value(0, "id1", table_name + "_tag");
    tablet.add_value(0, field_name, value);
    writer->write_table(tablet);
}

inline std::string write_two_table_fixture(const std::string& fixture_name,
                                           const std::string& first_field,
                                           const std::string& second_field) {
    storage::libtsfile_init();
    std::string out_path = unique_temp_path(fixture_name, ".tsfile");

    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(out_path, flags, 0666);

    auto* schema_a = new storage::TableSchema(
        "sensors_a",
        {
            common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema(first_field, common::INT64,
                                 common::UNCOMPRESSED, common::PLAIN,
                                 common::ColumnCategory::FIELD),
        });
    auto* writer = new storage::TsFileTableWriter(&file, schema_a);
    auto schema_b = std::make_shared<storage::TableSchema>(
        "sensors_b",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema(second_field, common::INT64,
                                 common::UNCOMPRESSED, common::PLAIN,
                                 common::ColumnCategory::FIELD),
        });
    writer->register_table(schema_b);

    write_one_table_row(writer, "sensors_a", first_field, 0, 10);
    write_one_table_row(writer, "sensors_b", second_field, 0, 20);

    writer->flush();
    writer->close();

    delete writer;
    delete schema_a;
    return out_path;
}

inline std::string write_multi_table_fixture() {
    return write_two_table_fixture("tsfile_cli_multi_table_fixture", "s1",
                                   "s1");
}

inline std::string write_disjoint_table_fixture() {
    return write_two_table_fixture("tsfile_cli_disjoint_table_fixture", "s1",
                                   "s2");
}

inline std::string write_sparse_tree_fixture() {
    storage::libtsfile_init();
    std::string path = unique_temp_path("tsfile_cli_sparse_tree", ".tsfile");
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(path, flags, 0666);
    storage::TsFileTreeWriter writer(&file);
    std::string device = "root.test.d1";
    auto* left = new storage::MeasurementSchema("left", common::INT32);
    auto* right = new storage::MeasurementSchema("right", common::BOOLEAN);
    writer.register_timeseries(device, left);
    writer.register_timeseries(device, right);

    storage::TsRecord first(device, 0);
    first.add_point("left", static_cast<int32_t>(10));
    writer.write(first);
    storage::TsRecord second(device, 1);
    second.add_point("right", true);
    writer.write(second);
    storage::TsRecord third(device, 2);
    third.add_point("left", static_cast<int32_t>(20));
    third.add_point("right", false);
    writer.write(third);
    writer.flush();
    writer.close();
    delete left;
    delete right;
    return path;
}

inline std::string write_disjoint_tree_fixture() {
    storage::libtsfile_init();
    std::string path = unique_temp_path("tsfile_cli_disjoint_tree", ".tsfile");
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(path, flags, 0666);
    storage::TsFileTreeWriter writer(&file);
    auto* left = new storage::MeasurementSchema("left", common::INT32);
    auto* right = new storage::MeasurementSchema("right", common::BOOLEAN);
    std::string first_device = "root.test.d1";
    std::string second_device = "root.test.d2";
    writer.register_timeseries(first_device, left);
    writer.register_timeseries(second_device, right);

    storage::TsRecord first(first_device, 0);
    first.add_point("left", static_cast<int32_t>(10));
    writer.write(first);
    storage::TsRecord second(second_device, 1);
    second.add_point("right", true);
    writer.write(second);
    writer.flush();
    writer.close();
    delete left;
    delete right;
    return path;
}

}  // namespace tsfile_cli_test

#endif  // TSFILE_CLI_TEST_UTIL_H
