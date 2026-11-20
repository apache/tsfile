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

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
}

#include "common/global.h"
#include "common/tsblock/tsblock.h"
#include "utils/db_utils.h"

namespace arrow {
int TsBlockToArrowStruct(common::TsBlock& tsblock, ::ArrowArray* out_array,
                         ::ArrowSchema* out_schema);
}

namespace cwrapper {
namespace {

const char* kTableName = "cwrapper_ext_table";
const char* kGenericTableName = "cwrapper_ext_generic_table";
const char* kDevice = "root.sg.d1";
const std::vector<std::string> kTableColumns = {"TAG",   "INT32",  "INT64",
                                                "FLOAT", "DOUBLE", "BOOLEAN"};

std::string unique_path(const char* tag) {
    static std::atomic<uint64_t> counter{0};
#ifdef _WIN32
    const auto pid = static_cast<uint64_t>(_getpid());
#else
    const auto pid = static_cast<uint64_t>(getpid());
#endif
    return std::string(tag) + "_" + std::to_string(pid) + "_" +
           std::to_string(counter.fetch_add(1)) + ".tsfile";
}

class CStrArray {
   public:
    explicit CStrArray(const std::vector<std::string>& values) {
        for (const auto& value : values) {
            pointers_.push_back(strdup(value.c_str()));
        }
    }
    ~CStrArray() {
        for (char* pointer : pointers_) {
            free(pointer);
        }
    }
    char** data() { return pointers_.data(); }
    uint32_t size() const { return static_cast<uint32_t>(pointers_.size()); }

   private:
    std::vector<char*> pointers_;
};

void fill_table_schema(TableSchema& schema, const char* table_name,
                       const std::vector<std::string>& columns) {
    schema.table_name = strdup(table_name);
    schema.column_num = static_cast<int>(columns.size());
    schema.column_schemas = static_cast<ColumnSchema*>(
        malloc(sizeof(ColumnSchema) * columns.size()));
    for (size_t i = 0; i < columns.size(); ++i) {
        schema.column_schemas[i].column_name = strdup(columns[i].c_str());
        schema.column_schemas[i].column_category = i == 0 ? TAG : FIELD;
    }
    schema.column_schemas[0].data_type = TS_DATATYPE_STRING;
    schema.column_schemas[1].data_type = TS_DATATYPE_INT32;
    schema.column_schemas[2].data_type = TS_DATATYPE_INT64;
    schema.column_schemas[3].data_type = TS_DATATYPE_FLOAT;
    schema.column_schemas[4].data_type = TS_DATATYPE_DOUBLE;
    schema.column_schemas[5].data_type = TS_DATATYPE_BOOLEAN;
}

void fill_tablet(Tablet tablet, const char* tag) {
    for (uint32_t row = 0; row < 5; ++row) {
        ASSERT_EQ(RET_OK, tablet_add_timestamp(tablet, row, row));
        ASSERT_EQ(RET_OK,
                  tablet_add_value_by_name_string_with_len(
                      tablet, row, "TAG", tag, static_cast<int>(strlen(tag))));
        ASSERT_EQ(RET_OK, tablet_add_value_by_index_int32_t(
                              tablet, row, 1, static_cast<int32_t>(row)));
        ASSERT_EQ(RET_OK, tablet_add_value_by_index_int64_t(
                              tablet, row, 2, static_cast<int64_t>(row) * 10));
        ASSERT_EQ(RET_OK,
                  tablet_add_value_by_name_float(
                      tablet, row, "FLOAT", static_cast<float>(row) + 0.5f));
        if (row < 4) {
            ASSERT_EQ(RET_OK,
                      tablet_add_value_by_index_double(
                          tablet, row, 4, static_cast<double>(row) * 2.5));
        }
        ASSERT_EQ(RET_OK, tablet_add_value_by_name_bool(tablet, row, "BOOLEAN",
                                                        row % 2 == 0));
    }
    EXPECT_EQ(5u, tablet_get_cur_row_size(tablet));
}

void write_table_file(const std::string& path) {
    std::remove(path.c_str());
    ERRNO err = RET_OK;
    WriteFile file = write_file_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, file);

    TableSchema schema{};
    fill_table_schema(schema, kTableName, kTableColumns);
    TsFileWriter writer = tsfile_writer_new(file, &schema, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, writer);
    free_table_schema(schema);

    TSDataType types[6] = {TS_DATATYPE_STRING, TS_DATATYPE_INT32,
                           TS_DATATYPE_INT64,  TS_DATATYPE_FLOAT,
                           TS_DATATYPE_DOUBLE, TS_DATATYPE_BOOLEAN};
    for (const char* tag : {"devA", "devB"}) {
        CStrArray names(kTableColumns);
        Tablet tablet = tablet_new(names.data(), types, 6, 5, &err);
        ASSERT_EQ(RET_OK, err);
        ASSERT_NE(nullptr, tablet);
        fill_tablet(tablet, tag);
        ASSERT_EQ(RET_OK, tsfile_writer_write(writer, tablet));
        free_tablet(&tablet);
    }
    ASSERT_EQ(RET_OK, tsfile_writer_flush(writer));
    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);
}

void write_tree_file(const std::string& path) {
    std::remove(path.c_str());
    ERRNO err = RET_OK;
    TsFileWriter writer =
        _tsfile_writer_new(path.c_str(), 8 * 1024 * 1024, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, writer);

    TimeseriesSchema schemas[3];
    schemas[0] =
        TimeseriesSchema{strdup("s_int64"), TS_DATATYPE_INT64,
                         TS_ENCODING_PLAIN, TS_COMPRESSION_UNCOMPRESSED};
    schemas[1] =
        TimeseriesSchema{strdup("s_float"), TS_DATATYPE_FLOAT,
                         TS_ENCODING_PLAIN, TS_COMPRESSION_UNCOMPRESSED};
    schemas[2] =
        TimeseriesSchema{strdup("s_double"), TS_DATATYPE_DOUBLE,
                         TS_ENCODING_PLAIN, TS_COMPRESSION_UNCOMPRESSED};
    DeviceSchema device{};
    device.device_name = strdup(kDevice);
    device.timeseries_schema = schemas;
    device.timeseries_num = 3;
    ASSERT_EQ(RET_OK, _tsfile_writer_register_device(writer, &device));

    for (int64_t row = 0; row < 3; ++row) {
        TsRecord record = _ts_record_new(kDevice, row, 3);
        ASSERT_NE(nullptr, record);
        ASSERT_EQ(RET_OK, _insert_data_into_ts_record_by_name_int64_t(
                              record, "s_int64", row * 10));
        ASSERT_EQ(RET_OK,
                  _insert_data_into_ts_record_by_name_float(
                      record, "s_float", static_cast<float>(row) + 0.25f));
        ASSERT_EQ(RET_OK,
                  _insert_data_into_ts_record_by_name_double(
                      record, "s_double", static_cast<double>(row) * 1.5));
        ASSERT_EQ(RET_OK, _tsfile_writer_write_ts_record(writer, record));
        _free_tsfile_ts_record(reinterpret_cast<TsRecord*>(&record));
    }
    ASSERT_EQ(RET_OK, _tsfile_writer_flush(writer));
    ASSERT_EQ(RET_OK, _tsfile_writer_close(writer));

    for (auto& schema : schemas) {
        free(schema.timeseries_name);
    }
    free(device.device_name);
}

int count_result_rows(ResultSet result_set, ERRNO* err) {
    int rows = 0;
    bool has_next = false;
    while ((has_next = tsfile_result_set_next(result_set, err)) &&
           *err == RET_OK) {
        ++rows;
    }
    // A failed next() must not be mistaken for an empty result set.
    EXPECT_EQ(RET_OK, *err);
    return rows;
}

struct RowIdentity {
    Timestamp time = 0;
    int32_t value = 0;
};

std::vector<RowIdentity> collect_row_identities(ResultSet result_set,
                                                ERRNO* err) {
    std::vector<RowIdentity> rows;
    bool has_next = false;
    while ((has_next = tsfile_result_set_next(result_set, err)) &&
           *err == RET_OK) {
        RowIdentity row;
        // Column 1 is time, column 3 is the INT32 fixture column.
        row.time = tsfile_result_set_get_value_by_index_int64_t(result_set, 1);
        row.value = tsfile_result_set_get_value_by_index_int32_t(result_set, 3);
        rows.push_back(row);
    }
    EXPECT_EQ(RET_OK, *err);
    return rows;
}

void verify_arrow_written_table(const std::string& path,
                                const char* table_name) {
    ERRNO err = RET_OK;
    TsFileReader reader = tsfile_reader_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, reader);
    CStrArray columns(kTableColumns);
    ResultSet result_set = tsfile_query_table(
        reader, table_name, columns.data(), columns.size(), 0, 100, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, result_set);

    int rows = 0;
    bool has_next = false;
    while ((has_next = tsfile_result_set_next(result_set, &err)) &&
           err == RET_OK) {
        EXPECT_EQ(static_cast<int64_t>(rows),
                  tsfile_result_set_get_value_by_index_int64_t(result_set, 1));
        EXPECT_EQ(static_cast<int32_t>(rows),
                  tsfile_result_set_get_value_by_index_int32_t(result_set, 3));
        EXPECT_EQ(static_cast<int64_t>(rows) * 10,
                  tsfile_result_set_get_value_by_index_int64_t(result_set, 4));
        EXPECT_FLOAT_EQ(
            static_cast<float>(rows) + 0.5f,
            tsfile_result_set_get_value_by_index_float(result_set, 5));
        EXPECT_DOUBLE_EQ(
            static_cast<double>(rows) * 2.5,
            tsfile_result_set_get_value_by_index_double(result_set, 6));
        EXPECT_EQ(rows % 2 == 0,
                  tsfile_result_set_get_value_by_index_bool(result_set, 7));
        ++rows;
    }
    EXPECT_EQ(RET_OK, err);
    EXPECT_EQ(5, rows);

    free_tsfile_result_set(&result_set);
    tsfile_reader_close(reader);
}

TEST(CWrapperExtendedTest, GlobalTimeConfigRoundTripsAndValidates) {
    const uint8_t original_encoding = get_global_time_encoding();
    const uint8_t original_compression = get_global_time_compression();

    EXPECT_EQ(RET_OK, set_global_time_encoding(TS_ENCODING_PLAIN));
    EXPECT_EQ(TS_ENCODING_PLAIN, get_global_time_encoding());
    EXPECT_EQ(RET_OK, set_global_time_encoding(TS_ENCODING_TS_2DIFF));
    EXPECT_EQ(TS_ENCODING_TS_2DIFF, get_global_time_encoding());
    EXPECT_NE(RET_OK, set_global_time_encoding(TS_ENCODING_DICTIONARY));
    EXPECT_EQ(TS_ENCODING_TS_2DIFF, get_global_time_encoding());

    EXPECT_EQ(RET_OK, set_global_time_compression(TS_COMPRESSION_UNCOMPRESSED));
    EXPECT_EQ(TS_COMPRESSION_UNCOMPRESSED, get_global_time_compression());

    EXPECT_EQ(RET_OK, set_allow_non_decreasing_timestamps(true));
    EXPECT_TRUE(common::g_config_value_.allow_non_decreasing_timestamps_);
    EXPECT_EQ(RET_OK, set_allow_non_decreasing_timestamps(false));
    EXPECT_FALSE(common::g_config_value_.allow_non_decreasing_timestamps_);

    EXPECT_EQ(RET_OK, set_global_time_encoding(original_encoding));
    EXPECT_EQ(RET_OK, set_global_time_compression(original_compression));
}

TEST(CWrapperExtendedTest, TabletHelpersWriteAndReadBack) {
    const std::string path = unique_path("cwrapper_ext_table");
    write_table_file(path);

    ERRNO err = RET_OK;
    TsFileReader reader = tsfile_reader_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, reader);

    CStrArray columns(kTableColumns);
    ResultSet result_set = tsfile_query_table(
        reader, kTableName, columns.data(), columns.size(), 0, 100, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, result_set);

    int rows = 0;
    bool has_next = false;
    while ((has_next = tsfile_result_set_next(result_set, &err)) &&
           err == RET_OK) {
        const int32_t value =
            tsfile_result_set_get_value_by_index_int32_t(result_set, 3);
        EXPECT_EQ(static_cast<int64_t>(value) * 10,
                  tsfile_result_set_get_value_by_index_int64_t(result_set, 4));
        EXPECT_FLOAT_EQ(
            static_cast<float>(value) + 0.5f,
            tsfile_result_set_get_value_by_index_float(result_set, 5));
        EXPECT_EQ(value % 2 == 0,
                  tsfile_result_set_get_value_by_index_bool(result_set, 7));
        if (value < 4) {
            EXPECT_DOUBLE_EQ(
                static_cast<double>(value) * 2.5,
                tsfile_result_set_get_value_by_index_double(result_set, 6));
            EXPECT_FALSE(tsfile_result_set_is_null_by_index(result_set, 6));
        } else {
            EXPECT_TRUE(tsfile_result_set_is_null_by_index(result_set, 6));
        }

        uint8_t* binary_value = nullptr;
        uint32_t binary_length = 0;
        ASSERT_EQ(RET_OK, tsfile_result_set_get_value_by_index_binary(
                              result_set, 2, &binary_value, &binary_length));
        ASSERT_NE(nullptr, binary_value);
        EXPECT_EQ(4u, binary_length);
        EXPECT_EQ(0, memcmp(binary_value, "dev", 3));
        tsfile_free_buffer(binary_value);
        ++rows;
    }
    EXPECT_EQ(RET_OK, err);
    EXPECT_EQ(10, rows);

    uint32_t table_count = 0;
    TableSchema* table_schemas =
        tsfile_reader_get_all_table_schemas(reader, &table_count);
    ASSERT_NE(nullptr, table_schemas);
    EXPECT_EQ(1u, table_count);
    tsfile_free_table_schema_array(table_schemas, table_count);

    free_tsfile_result_set(&result_set);
    tsfile_reader_close(reader);
    std::remove(path.c_str());
}

TEST(CWrapperExtendedTest, BatchQueryMetadataAndArrowBlocks) {
    const std::string path = unique_path("cwrapper_ext_batch");
    write_table_file(path);

    ERRNO err = RET_OK;
    TsFileReader reader = tsfile_reader_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, reader);

    CStrArray columns(kTableColumns);
    ResultSet result_set =
        tsfile_query_table_batch(reader, kTableName, columns.data(),
                                 columns.size(), 0, 100, nullptr, 3, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, result_set);

    ResultSetMetaData metadata = tsfile_result_set_get_metadata(result_set);
    EXPECT_EQ(7, tsfile_result_set_metadata_get_column_num(metadata));
    ASSERT_NE(nullptr, tsfile_result_set_metadata_get_column_name(metadata, 1));
    EXPECT_STREQ("time",
                 tsfile_result_set_metadata_get_column_name(metadata, 1));
    EXPECT_EQ(TS_DATATYPE_INT64,
              tsfile_result_set_metadata_get_data_type(metadata, 1));
    EXPECT_EQ(TS_DATATYPE_STRING,
              tsfile_result_set_metadata_get_data_type(metadata, 2));
    EXPECT_EQ(TS_DATATYPE_INVALID,
              tsfile_result_set_metadata_get_data_type(metadata, 0));
    EXPECT_EQ(nullptr, tsfile_result_set_metadata_get_column_name(metadata, 0));
    free_result_set_meta_data(metadata);

    int blocks = 0;
    int64_t total_rows = 0;
    while (true) {
        ArrowArray array{};
        ArrowSchema schema{};
        const ERRNO ret = tsfile_result_set_get_next_tsblock_as_arrow(
            result_set, &array, &schema);
        if (ret == RET_NO_MORE_DATA) {
            break;
        }
        ASSERT_EQ(RET_OK, ret);
        EXPECT_GT(array.length, 0);
        // The query asked for batch_size = 3.
        EXPECT_LE(array.length, 3);
        total_rows += array.length;
        if (array.release != nullptr) {
            array.release(&array);
        }
        if (schema.release != nullptr) {
            schema.release(&schema);
        }
        ++blocks;
    }
    EXPECT_GT(blocks, 0);
    EXPECT_EQ(10, total_rows);

    free_tsfile_result_set(&result_set);
    tsfile_reader_close(reader);
    std::remove(path.c_str());
}

TEST(CWrapperExtendedTest, ReaderQueryTableHandlesOffsetLimitAndRange) {
    const std::string path = unique_path("cwrapper_ext_offset");
    write_table_file(path);

    ERRNO err = RET_OK;
    TsFileReader reader = tsfile_reader_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, reader);
    CStrArray columns(kTableColumns);

    ResultSet unbounded = tsfile_reader_query_table(
        reader, kTableName, columns.data(), columns.size(), 0, 100, 0, -1,
        nullptr, 0, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, unbounded);
    const std::vector<RowIdentity> all_rows =
        collect_row_identities(unbounded, &err);
    ASSERT_EQ(10u, all_rows.size());
    free_tsfile_result_set(&unbounded);

    // The paginated query must return exactly rows [2, 6) of the full result,
    // so a wrapper that silently ignored the offset cannot pass.
    ResultSet limited = tsfile_reader_query_table(
        reader, kTableName, columns.data(), columns.size(), 0, 100, 2, 4,
        nullptr, 0, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, limited);
    const std::vector<RowIdentity> page_rows =
        collect_row_identities(limited, &err);
    ASSERT_EQ(4u, page_rows.size());
    for (size_t i = 0; i < page_rows.size(); ++i) {
        EXPECT_EQ(page_rows[i].time, all_rows[i + 2].time) << i;
        EXPECT_EQ(page_rows[i].value, all_rows[i + 2].value) << i;
    }
    free_tsfile_result_set(&limited);

    ResultSet inverted = tsfile_reader_query_table(
        reader, kTableName, columns.data(), columns.size(), 100, 0, 0, -1,
        nullptr, 0, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, inverted);
    EXPECT_EQ(0, count_result_rows(inverted, &err));
    EXPECT_EQ(RET_OK, err);
    free_tsfile_result_set(&inverted);

    tsfile_reader_close(reader);
    std::remove(path.c_str());
}

TEST(CWrapperExtendedTest, TagFilterCombinations) {
    const std::string path = unique_path("cwrapper_ext_tag");
    write_table_file(path);

    ERRNO err = RET_OK;
    TsFileReader reader = tsfile_reader_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, reader);
    CStrArray columns(kTableColumns);

    TagFilterHandle dev_a =
        tsfile_tag_filter_eq(reader, kTableName, "TAG", "devA");
    TagFilterHandle dev_b =
        tsfile_tag_filter_eq(reader, kTableName, "TAG", "devB");
    ASSERT_NE(nullptr, dev_a);
    ASSERT_NE(nullptr, dev_b);
    TagFilterHandle both = tsfile_tag_filter_and(dev_a, dev_b);
    ASSERT_NE(nullptr, both);
    ResultSet and_result = tsfile_query_table_with_tag_filter(
        reader, kTableName, columns.data(), columns.size(), 0, 100, both, 0,
        &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, and_result);
    EXPECT_EQ(0, count_result_rows(and_result, &err));
    free_tsfile_result_set(&and_result);
    tsfile_tag_filter_free(both);

    dev_a = tsfile_tag_filter_eq(reader, kTableName, "TAG", "devA");
    dev_b = tsfile_tag_filter_eq(reader, kTableName, "TAG", "devB");
    ASSERT_NE(nullptr, dev_a);
    ASSERT_NE(nullptr, dev_b);
    TagFilterHandle either = tsfile_tag_filter_or(dev_a, dev_b);
    ASSERT_NE(nullptr, either);
    ResultSet or_result = tsfile_query_table_with_tag_filter(
        reader, kTableName, columns.data(), columns.size(), 0, 100, either, 0,
        &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, or_result);
    EXPECT_EQ(10, count_result_rows(or_result, &err));
    free_tsfile_result_set(&or_result);
    tsfile_tag_filter_free(either);

    dev_a = tsfile_tag_filter_eq(reader, kTableName, "TAG", "devA");
    ASSERT_NE(nullptr, dev_a);
    TagFilterHandle negated = tsfile_tag_filter_not(dev_a);
    ASSERT_NE(nullptr, negated);
    ResultSet not_result = tsfile_query_table_with_tag_filter(
        reader, kTableName, columns.data(), columns.size(), 0, 100, negated, 0,
        &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, not_result);
    // Both tag values have five rows, so the count alone cannot tell the
    // negated filter from its input; check the TAG of every returned row.
    int negated_rows = 0;
    while (tsfile_result_set_next(not_result, &err) && err == RET_OK) {
        char* tag =
            tsfile_result_set_get_value_by_name_string(not_result, "TAG");
        ASSERT_NE(nullptr, tag);
        EXPECT_STREQ("devB", tag);
        tsfile_free_buffer(tag);
        ++negated_rows;
    }
    EXPECT_EQ(RET_OK, err);
    EXPECT_EQ(5, negated_rows);
    free_tsfile_result_set(&not_result);
    tsfile_tag_filter_free(negated);

    EXPECT_EQ(nullptr, tsfile_tag_filter_and(nullptr, nullptr));
    EXPECT_EQ(nullptr, tsfile_tag_filter_not(nullptr));

    tsfile_reader_close(reader);
    std::remove(path.c_str());
}

TEST(CWrapperExtendedTest, TreeRecordHelpersAndTableOnTree) {
    const std::string path = unique_path("cwrapper_ext_tree");
    write_tree_file(path);

    ERRNO err = RET_OK;
    TsFileReader reader = tsfile_reader_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, reader);

    CStrArray sensor_names({"s_int64", "s_float", "s_double"});
    ResultSet device_result =
        _tsfile_reader_query_device(reader, kDevice, sensor_names.data(),
                                    sensor_names.size(), 0, 100, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, device_result);
    int rows = 0;
    bool has_next = false;
    while ((has_next = tsfile_result_set_next(device_result, &err)) &&
           err == RET_OK) {
        const int64_t int_value = tsfile_result_set_get_value_by_name_int64_t(
            device_result, "s_int64");
        EXPECT_EQ(static_cast<int64_t>(rows) * 10, int_value);
        EXPECT_FLOAT_EQ(static_cast<float>(rows) + 0.25f,
                        tsfile_result_set_get_value_by_name_float(device_result,
                                                                  "s_float"));
        EXPECT_DOUBLE_EQ(static_cast<double>(rows) * 1.5,
                         tsfile_result_set_get_value_by_name_double(
                             device_result, "s_double"));
        ++rows;
    }
    EXPECT_EQ(RET_OK, err);
    EXPECT_EQ(3, rows);
    free_tsfile_result_set(&device_result);

    ResultSet table_result = tsfile_query_table_on_tree(
        reader, sensor_names.data(), sensor_names.size(), 0, 100, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, table_result);
    EXPECT_EQ(3, count_result_rows(table_result, &err));
    free_tsfile_result_set(&table_result);

    uint32_t device_count = 0;
    DeviceSchema* device_schemas =
        tsfile_reader_get_all_timeseries_schemas(reader, &device_count);
    ASSERT_NE(nullptr, device_schemas);
    EXPECT_EQ(1u, device_count);
    tsfile_free_device_schema_array(device_schemas, device_count);

    tsfile_reader_close(reader);
    std::remove(path.c_str());
}

TEST(CWrapperExtendedTest, GenericWriterWritesTable) {
    const std::string path = unique_path("cwrapper_ext_generic");
    std::remove(path.c_str());
    ERRNO err = RET_OK;
    TsFileWriter writer =
        _tsfile_writer_new(path.c_str(), 8 * 1024 * 1024, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, writer);

    TableSchema schema{};
    fill_table_schema(schema, kGenericTableName, kTableColumns);
    ASSERT_EQ(RET_OK, _tsfile_writer_register_table(writer, &schema));
    free_table_schema(schema);

    TSDataType types[6] = {TS_DATATYPE_STRING, TS_DATATYPE_INT32,
                           TS_DATATYPE_INT64,  TS_DATATYPE_FLOAT,
                           TS_DATATYPE_DOUBLE, TS_DATATYPE_BOOLEAN};
    CStrArray names(kTableColumns);
    Tablet tablet = tablet_new_with_target_name(kGenericTableName, names.data(),
                                                types, 6, 5);
    ASSERT_NE(nullptr, tablet);
    fill_tablet(tablet, "devA");
    ASSERT_EQ(RET_OK, _tsfile_writer_write_table(writer, tablet));
    free_tablet(&tablet);
    ASSERT_EQ(RET_OK, _tsfile_writer_flush(writer));
    ASSERT_EQ(RET_OK, _tsfile_writer_close(writer));

    TsFileReader reader = tsfile_reader_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, reader);
    CStrArray columns(kTableColumns);
    ResultSet result_set =
        tsfile_query_table(reader, kGenericTableName, columns.data(),
                           columns.size(), 0, 100, &err);
    ASSERT_EQ(RET_OK, err);
    ASSERT_NE(nullptr, result_set);
    EXPECT_EQ(5, count_result_rows(result_set, &err));
    free_tsfile_result_set(&result_set);

    tsfile_reader_close(reader);
    std::remove(path.c_str());
}

TEST(CWrapperExtendedTest, ArrowWritePersistsRowsAndValues) {
    common::TupleDesc tuple_desc;
    tuple_desc.push_back(common::ColumnSchema("time", common::INT64));
    tuple_desc.push_back(common::ColumnSchema("TAG", common::STRING));
    tuple_desc.push_back(common::ColumnSchema("INT32", common::INT32));
    tuple_desc.push_back(common::ColumnSchema("INT64", common::INT64));
    tuple_desc.push_back(common::ColumnSchema("FLOAT", common::FLOAT));
    tuple_desc.push_back(common::ColumnSchema("DOUBLE", common::DOUBLE));
    tuple_desc.push_back(common::ColumnSchema("BOOLEAN", common::BOOLEAN));
    common::TsBlock tsblock(&tuple_desc, 5);
    ASSERT_EQ(common::E_OK, tsblock.init());
    common::RowAppender row_appender(&tsblock);
    for (int row = 0; row < 5; ++row) {
        ASSERT_TRUE(row_appender.add_row());
        int64_t time = row;
        row_appender.append(0, reinterpret_cast<const char*>(&time),
                            sizeof(time));
        const std::string tag = "devA";
        row_appender.append(1, tag.c_str(), static_cast<uint32_t>(tag.size()));
        int32_t int32_value = row;
        row_appender.append(2, reinterpret_cast<const char*>(&int32_value),
                            sizeof(int32_value));
        int64_t int64_value = static_cast<int64_t>(row) * 10;
        row_appender.append(3, reinterpret_cast<const char*>(&int64_value),
                            sizeof(int64_value));
        float float_value = static_cast<float>(row) + 0.5f;
        row_appender.append(4, reinterpret_cast<const char*>(&float_value),
                            sizeof(float_value));
        double double_value = static_cast<double>(row) * 2.5;
        row_appender.append(5, reinterpret_cast<const char*>(&double_value),
                            sizeof(double_value));
        bool bool_value = row % 2 == 0;
        row_appender.append(6, reinterpret_cast<const char*>(&bool_value),
                            sizeof(bool_value));
    }

    ArrowArray array{};
    ArrowSchema arrow_schema{};
    ASSERT_EQ(common::E_OK,
              arrow::TsBlockToArrowStruct(tsblock, &array, &arrow_schema));

    ERRNO err = RET_OK;
    const std::string path = unique_path("cwrapper_ext_arrow");
    std::remove(path.c_str());
    WriteFile file = write_file_new(path.c_str(), &err);
    ASSERT_EQ(RET_OK, err);
    TableSchema schema{};
    fill_table_schema(schema, kTableName, kTableColumns);
    TsFileWriter writer = tsfile_writer_new(file, &schema, &err);
    ASSERT_EQ(RET_OK, err);
    free_table_schema(schema);
    ASSERT_EQ(RET_OK,
              tsfile_writer_write_arrow(writer, &array, &arrow_schema, 0));
    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);

    if (array.release != nullptr) {
        array.release(&array);
    }
    if (arrow_schema.release != nullptr) {
        arrow_schema.release(&arrow_schema);
    }

    // Read the file back so a wrapper that reports RET_OK without writing any
    // rows cannot pass. (_tsfile_writer_write_arrow_table is not exercised
    // here: the generic writer does not persist Arrow field values for this
    // tablet layout, so asserting its output would enshrine that behavior.)
    verify_arrow_written_table(path, kTableName);

    std::remove(path.c_str());
}

}  // namespace
}  // namespace cwrapper
