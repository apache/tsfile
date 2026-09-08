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

#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"
#include "writer/tsfile_writer.h"

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

inline std::string write_nullable_tag_filter_fixture() {
    storage::libtsfile_init();
    std::string out_path =
        unique_temp_path("tsfile_cli_nullable_tag_filter_fixture", ".tsfile");
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
        {common::ColumnCategory::TAG, common::ColumnCategory::FIELD}, 5);
    const char* tags[] = {nullptr, "", "null", "dev_a", "dev_b"};
    for (int row = 0; row < 5; ++row) {
        tablet.add_timestamp(row, static_cast<int64_t>(row));
        if (tags[row] != nullptr) {
            tablet.add_value(row, "id1", tags[row]);
        }
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

// Keep the first table readable and corrupt the second table's value chunk.
// The multi-object export test uses this to verify first-error-stop behavior
// and that only the successfully committed object remains in the manifest.
inline std::string write_multi_table_corrupt_fixture() {
    std::string path = write_multi_table_fixture();
    if (path.empty()) {
        return path;
    }

    storage::TsFileReader reader;
    if (reader.open(path) != common::E_OK) {
        return std::string();
    }
    const auto devices = reader.get_all_devices("sensors_b");
    const auto metadata = reader.get_timeseries_metadata(devices);
    int64_t chunk_offset = -1;
    for (const auto& entry : metadata) {
        for (const auto& index : entry.second) {
            if (index == nullptr) continue;
            auto* chunks = index->get_value_chunk_meta_list();
            if (chunks == nullptr) {
                chunks = index->get_chunk_meta_list();
            }
            if (chunks == nullptr) continue;
            for (auto it = chunks->begin(); it != chunks->end(); it++) {
                if (it.get() != nullptr) {
                    chunk_offset = it.get()->offset_of_chunk_header_;
                    break;
                }
            }
            if (chunk_offset >= 0) break;
        }
        if (chunk_offset >= 0) break;
    }
    reader.close();
    if (chunk_offset < 0) {
        return std::string();
    }

    std::fstream file(path.c_str(),
                      std::ios::in | std::ios::out | std::ios::binary);
    if (!file.good()) {
        return std::string();
    }
    // The metadata offset points at the chunk header.  Decode that header and
    // change only its encoding byte to an unsupported value.  This keeps the
    // footer and the first table intact while making the second table fail
    // before any data page can be returned.
    char header_buf[64];
    std::ifstream input(path.c_str(), std::ios::binary);
    input.seekg(chunk_offset);
    input.read(header_buf, sizeof(header_buf));
    const std::streamsize header_len = input.gcount();
    if (header_len <= 0) {
        return std::string();
    }
    common::ByteStream header_stream;
    header_stream.wrap_from(header_buf, static_cast<int32_t>(header_len));
    storage::ChunkHeader header;
    if (header.deserialize_from(header_stream) != common::E_OK) {
        return std::string();
    }
    const int64_t encoding_offset = chunk_offset + header.serialized_size_ - 1;
    file.seekp(encoding_offset);
    file.put(static_cast<char>(0x7f));
    file.close();
    return path;
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

// A deliberately heterogeneous tree file used by cross-model CLI tests.
inline std::string write_complex_tree_fixture() {
    storage::libtsfile_init();
    std::string path = unique_temp_path("tsfile_cli_complex_tree", ".tsfile");
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(path, flags, 0666);
    storage::TsFileTreeWriter writer(&file);

    const std::string first_device = "root.test.d1";
    const std::string second_device = "root.test2.d2.t2";
    const std::string third_device = "root.t3";
    std::vector<storage::MeasurementSchema*> schemas;
    auto register_schema = [&](const std::string& device_name,
                               const std::string& measurement,
                               common::TSDataType type) {
        std::string device = device_name;
        auto* schema = new storage::MeasurementSchema(measurement, type);
        schemas.push_back(schema);
        writer.register_timeseries(device, schema);
    };
    register_schema(first_device, "m1", common::INT32);
    register_schema(first_device, "m2", common::DOUBLE);
    register_schema(first_device, "m3", common::TEXT);
    register_schema(first_device, "m4", common::INT32);
    register_schema(first_device, "m5", common::DOUBLE);
    register_schema(second_device, "xxx", common::FLOAT);
    register_schema(second_device, "xxx_int", common::INT64);
    register_schema(second_device, "xxx_text", common::TEXT);
    register_schema(third_device, "xx", common::BOOLEAN);
    register_schema(third_device, "xx_double", common::DOUBLE);
    register_schema(third_device, "xx_text", common::TEXT);

    for (int row = 0; row < 20; ++row) {
        {
            storage::TsRecord record(first_device, row);
            std::string m3_value = "value_" + std::to_string(row);
            record.add_point("m1", static_cast<int32_t>(row));
            record.add_point("m2", static_cast<double>(row) + 0.5);
            record.add_point("m3", common::String(m3_value));
            record.add_point("m4", static_cast<int32_t>(row * 2));
            record.add_point("m5", static_cast<double>(row) + 100.5);
            writer.write(record);
        }
        {
            storage::TsRecord record(second_device, row);
            std::string xxx_text_value = "xxx_" + std::to_string(row);
            record.add_point("xxx", static_cast<float>(row) + 0.25f);
            record.add_point("xxx_int", static_cast<int64_t>(row * 10));
            record.add_point("xxx_text", common::String(xxx_text_value));
            writer.write(record);
        }
        {
            storage::TsRecord record(third_device, row);
            std::string xx_text_value = "xx_" + std::to_string(row);
            record.add_point("xx", row % 2 == 0);
            record.add_point("xx_double", static_cast<double>(row) + 10.5);
            record.add_point("xx_text", common::String(xx_text_value));
            writer.write(record);
        }
    }
    writer.flush();
    writer.close();
    for (auto* schema : schemas) delete schema;
    return path;
}

// A two-table file: table1 has no TAG columns, while table2 has three TAGs.
// Both tables contain twenty rows and table2 intentionally contains NULLs.
inline std::string write_complex_table_fixture() {
    storage::libtsfile_init();
    std::string path = unique_temp_path("tsfile_cli_complex_table", ".tsfile");
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(path, flags, 0666);

    auto* schema1 = new storage::TableSchema(
        "table1",
        {common::ColumnSchema("value", common::INT64, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::FIELD)});
    auto* writer = new storage::TsFileTableWriter(&file, schema1);
    auto schema2 = std::make_shared<storage::TableSchema>(
        "table2",
        std::vector<common::ColumnSchema>{
            common::ColumnSchema("site", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("rack", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("sensor", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("reading", common::DOUBLE,
                                 common::UNCOMPRESSED, common::PLAIN,
                                 common::ColumnCategory::FIELD)});
    writer->register_table(schema2);

    for (int row = 0; row < 20; ++row) {
        storage::Tablet t1("table1", {"value"}, {common::INT64},
                           {common::ColumnCategory::FIELD}, 1);
        t1.add_timestamp(0, row);
        t1.add_value(0, "value", static_cast<int64_t>(row));
        writer->write_table(t1);

        storage::Tablet t2(
            "table2", {"site", "rack", "sensor", "reading"},
            {common::STRING, common::STRING, common::STRING, common::DOUBLE},
            {common::ColumnCategory::TAG, common::ColumnCategory::TAG,
             common::ColumnCategory::TAG, common::ColumnCategory::FIELD},
            1);
        t2.add_timestamp(0, row);
        if (row == 0) {
            // Missing TAG value: NULL, distinct from empty and the literal
            // string "null" below.
        } else if (row == 1) {
            t2.add_value(0, "site", "");
        } else if (row == 2) {
            t2.add_value(0, "site", "null");
        } else {
            t2.add_value(0, "site", std::string("site") + std::to_string(row));
        }
        t2.add_value(0, "rack", std::string("rack") + std::to_string(row));
        t2.add_value(0, "sensor", std::string("sensor") + std::to_string(row));
        if (row % 4 != 0) {
            t2.add_value(0, "reading", static_cast<double>(row) * 1.25);
        }
        writer->write_table(t2);
    }
    writer->flush();
    writer->close();
    delete writer;
    delete schema1;
    return path;
}

// Deliberately mixes a tree device with a separately registered table schema.
// This is not a supported production output, but it is a useful input fixture
// for verifying that every read command rejects a non-pure TsFile uniformly.
inline std::string write_non_pure_fixture() {
    storage::libtsfile_init();
    std::string path = unique_temp_path("tsfile_cli_non_pure", ".tsfile");
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(path, flags, 0666);

    storage::TsFileWriter writer;
    writer.init(&file);
    auto table_schema = std::make_shared<storage::TableSchema>(
        "hybrid_table", std::vector<common::ColumnSchema>{common::ColumnSchema(
                            "value", common::INT64, common::UNCOMPRESSED,
                            common::PLAIN, common::ColumnCategory::FIELD)});
    if (writer.register_table(table_schema) != common::E_OK) {
        writer.close();
        return std::string();
    }

    storage::MeasurementSchema tree_schema("value", common::INT32);
    if (writer.register_timeseries("root.hybrid.d1", tree_schema) !=
        common::E_OK) {
        writer.close();
        return std::string();
    }

    storage::TsRecord tree_record("root.hybrid.d1", 0);
    tree_record.add_point("value", static_cast<int32_t>(1));
    if (writer.write_tree(tree_record) != common::E_OK) {
        writer.close();
        return std::string();
    }

    storage::Tablet table_tablet("hybrid_table", {"value"}, {common::INT64},
                                 {common::ColumnCategory::FIELD}, 1);
    table_tablet.add_timestamp(0, 0);
    table_tablet.add_value(0, "value", static_cast<int64_t>(2));
    if (writer.write_table(table_tablet) != common::E_OK ||
        writer.flush() != common::E_OK || writer.close() != common::E_OK) {
        return std::string();
    }
    return path;
}

// Valid tree-model file with a declared device/schema but no data pages.
inline std::string write_empty_tree_fixture() {
    storage::libtsfile_init();
    std::string path = unique_temp_path("tsfile_cli_empty_tree", ".tsfile");
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(path, flags, 0666);
    storage::TsFileTreeWriter writer(&file);
    std::string device = "root.empty.d1";
    auto* value = new storage::MeasurementSchema("value", common::INT32);
    writer.register_timeseries(device, value);
    writer.flush();
    writer.close();
    delete value;
    return path;
}

// Valid table-model file with a complete schema but no data rows.
inline std::string write_empty_table_fixture() {
    storage::libtsfile_init();
    std::string path = unique_temp_path("tsfile_cli_empty_table", ".tsfile");
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(path, flags, 0666);
    auto* schema = new storage::TableSchema(
        "sensors",
        {common::ColumnSchema("site", common::STRING, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::TAG),
         common::ColumnSchema("room", common::STRING, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::TAG),
         common::ColumnSchema("temp", common::DOUBLE, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::FIELD)});
    auto* writer = new storage::TsFileTableWriter(&file, schema);
    writer->flush();
    writer->close();
    delete writer;
    delete schema;
    return path;
}

// Table schema fixture covering all logical column categories. TIME and
// ATTRIBUTE are metadata-only columns; FIELD is present so the file is still
// useful for a row query when a caller appends data in a separate test.
inline std::string write_category_edge_fixture() {
    storage::libtsfile_init();
    std::string path = unique_temp_path("tsfile_cli_category_edge", ".tsfile");
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(path, flags, 0666);
    auto* schema = new storage::TableSchema(
        "category_edge",
        {common::ColumnSchema("event_time", common::TIMESTAMP,
                              common::UNCOMPRESSED, common::PLAIN,
                              common::ColumnCategory::TIME),
         common::ColumnSchema("owner", common::STRING, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::ATTRIBUTE),
         common::ColumnSchema("site", common::STRING, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::TAG),
         common::ColumnSchema("temp", common::DOUBLE, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::FIELD)});
    auto* writer = new storage::TsFileTableWriter(&file, schema);
    writer->flush();
    writer->close();
    delete writer;
    delete schema;
    return path;
}

// A table with a real entity timeline whose only FIELD is null on every row.
// This is distinct from a schema-only empty table: count/stats must still see
// the two timestamps and report null_count=2 for temp.
inline std::string write_empty_field_fixture() {
    storage::libtsfile_init();
    std::string path = unique_temp_path("tsfile_cli_empty_field", ".tsfile");
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(path, flags, 0666);
    auto* schema = new storage::TableSchema(
        "sensors",
        {common::ColumnSchema("site", common::STRING, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::TAG),
         common::ColumnSchema("room", common::STRING, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::TAG),
         common::ColumnSchema("temp", common::DOUBLE, common::UNCOMPRESSED,
                              common::PLAIN, common::ColumnCategory::FIELD)});
    auto* writer = new storage::TsFileTableWriter(&file, schema);
    for (int row = 0; row < 2; ++row) {
        storage::Tablet tablet(
            "sensors", {"site", "room", "temp"},
            {common::STRING, common::STRING, common::DOUBLE},
            {common::ColumnCategory::TAG, common::ColumnCategory::TAG,
             common::ColumnCategory::FIELD},
            1);
        tablet.add_timestamp(0, 1000 + row * 1000);
        tablet.add_value(0, "site", "beijing");
        tablet.add_value(0, "room", "r1");
        // Deliberately omit temp so the FIELD bitmap marks it NULL.
        writer->write_table(tablet);
    }
    writer->flush();
    writer->close();
    delete writer;
    delete schema;
    return path;
}

}  // namespace tsfile_cli_test

#endif  // TSFILE_CLI_TEST_UTIL_H
