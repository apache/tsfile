/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include <io.h>
#else
#include <unistd.h>
#endif
#include <utils/db_utils.h>

#include <cstring>

#include "common/row_record.h"
#include "cwrapper/tsfile_cwrapper.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace storage {
class TsFileReader;
}

extern "C" {
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
}

#include "common/tablet.h"
#include "utils/errno_define.h"

namespace cwrapper {
class CWrapperTest : public testing::Test {
   public:
    static void ASSERT_OK(ERRNO code, const char* msg = "") {
        ASSERT_EQ(code, RET_OK) << msg;
    }
};

TEST_F(CWrapperTest, FileReadBackendConfigurationRoundTripsAndValidates) {
    const TsFileReadBackend original = tsfile_get_file_read_backend();

    EXPECT_EQ(tsfile_set_file_read_backend(TSFILE_READ_BACKEND_MMAP), RET_OK);
    EXPECT_EQ(tsfile_get_file_read_backend(), TSFILE_READ_BACKEND_MMAP);
    EXPECT_EQ(tsfile_set_file_read_backend(TSFILE_READ_BACKEND_PREAD), RET_OK);
    EXPECT_EQ(tsfile_get_file_read_backend(), TSFILE_READ_BACKEND_PREAD);
    EXPECT_EQ(tsfile_set_file_read_backend(99), RET_INVALID_ARG);
    EXPECT_EQ(tsfile_set_file_read_backend(256), RET_INVALID_ARG);
    EXPECT_EQ(tsfile_get_file_read_backend(), TSFILE_READ_BACKEND_PREAD);

    EXPECT_EQ(tsfile_set_file_read_backend(original), RET_OK);
}

TEST_F(CWrapperTest, CodecAndCompressionConfigIncludesJavaIds) {
    uint8_t old_int32_encoding = get_datatype_encoding(TS_DATATYPE_INT32);
    uint8_t old_int64_encoding = get_datatype_encoding(TS_DATATYPE_INT64);
    uint8_t old_float_encoding = get_datatype_encoding(TS_DATATYPE_FLOAT);
    uint8_t old_double_encoding = get_datatype_encoding(TS_DATATYPE_DOUBLE);
    uint8_t old_compression = get_global_compression();

    EXPECT_EQ(set_datatype_encoding(TS_DATATYPE_INT32, TS_ENCODING_CHIMP),
              common::E_OK);
    EXPECT_EQ(set_datatype_encoding(TS_DATATYPE_INT64, TS_ENCODING_RLBE),
              common::E_OK);
    EXPECT_EQ(set_datatype_encoding(TS_DATATYPE_FLOAT, TS_ENCODING_CHIMP),
              common::E_OK);
    EXPECT_EQ(set_datatype_encoding(TS_DATATYPE_DOUBLE, TS_ENCODING_CAMEL),
              common::E_OK);
    EXPECT_EQ(set_datatype_encoding(TS_DATATYPE_FLOAT, TS_ENCODING_CAMEL),
              common::E_NOT_SUPPORT);
    EXPECT_EQ(set_global_compression(TS_COMPRESSION_ZSTD), common::E_OK);
    EXPECT_EQ(set_global_compression(TS_COMPRESSION_LZMA2), common::E_OK);

    EXPECT_EQ(set_datatype_encoding(TS_DATATYPE_INT32, old_int32_encoding),
              common::E_OK);
    EXPECT_EQ(set_datatype_encoding(TS_DATATYPE_INT64, old_int64_encoding),
              common::E_OK);
    EXPECT_EQ(set_datatype_encoding(TS_DATATYPE_FLOAT, old_float_encoding),
              common::E_OK);
    EXPECT_EQ(set_datatype_encoding(TS_DATATYPE_DOUBLE, old_double_encoding),
              common::E_OK);
    EXPECT_EQ(set_global_compression(old_compression), common::E_OK);
}

TEST_F(CWrapperTest, TestForPythonInterfaceInsert) {
    ERRNO code = 0;
    const char* filename = "cwrapper_for_python.tsfile";
    remove(filename);  // Clean up any existing file

    // Device and measurement definitions
    char* device_id = strdup("root.device1");
    char* str_measurement_id = strdup("str_measurement");
    char* text_measurement_id = strdup("text_measurement");
    char* date_measurement_id = strdup("date_measurement");

    // Define time series schemas for different data types
    timeseries_schema str_measurement;
    str_measurement.timeseries_name = str_measurement_id;
    str_measurement.compression = TS_COMPRESSION_UNCOMPRESSED;
    str_measurement.data_type = TS_DATATYPE_STRING;
    str_measurement.encoding = TS_ENCODING_PLAIN;

    timeseries_schema text_measurement;
    text_measurement.timeseries_name = text_measurement_id;
    text_measurement.compression = TS_COMPRESSION_UNCOMPRESSED;
    text_measurement.data_type = TS_DATATYPE_TEXT;
    text_measurement.encoding = TS_ENCODING_PLAIN;

    timeseries_schema date_measurement;
    date_measurement.timeseries_name = date_measurement_id;
    date_measurement.compression = TS_COMPRESSION_UNCOMPRESSED;
    date_measurement.data_type = TS_DATATYPE_DATE;
    date_measurement.encoding = TS_ENCODING_PLAIN;

    // Create TsFile writer
    auto* writer = (storage::TsFileWriter*)_tsfile_writer_new(
        filename, 128 * 1024 * 1024, &code);
    ASSERT_OK(code, "create writer failed");

    // Register time series with the writer
    ASSERT_OK(
        _tsfile_writer_register_timeseries(writer, device_id, &str_measurement),
        "register timeseries failed");

    ASSERT_OK(_tsfile_writer_register_timeseries(writer, device_id,
                                                 &text_measurement),
              "register timeseries failed");

    ASSERT_OK(_tsfile_writer_register_timeseries(writer, device_id,
                                                 &date_measurement),
              "register timeseries failed");

    // Create a new time series record
    auto* record = (storage::TsRecord*)_ts_record_new(device_id, 0, 3);

    // Insert string data
    const char* test_str = "test_string";
    ASSERT_OK(_insert_data_into_ts_record_by_name_string_with_len(
                  record, str_measurement_id, test_str, strlen(test_str)),
              "insert data failed");

    // Insert text data
    const char* test_text = "test_text";
    ASSERT_OK(_insert_data_into_ts_record_by_name_string_with_len(
                  record, text_measurement_id, test_text, strlen(test_text)),
              "insert data failed");

    // Insert date data - NOTE: There's a bug here, should use
    // date_measurement_id
    int32_t test_date = 20251118;
    ASSERT_OK(_insert_data_into_ts_record_by_name_int32_t(
                  record, date_measurement_id, test_date),
              "insert data failed");

    // Write the record to file and close writer
    ASSERT_OK(_tsfile_writer_write_ts_record(writer, record),
              "write record failed");
    ASSERT_OK(_tsfile_writer_flush(writer), "flush failed");
    ASSERT_OK(_tsfile_writer_close(writer), "close writer failed");
    _free_tsfile_ts_record(reinterpret_cast<TsRecord*>(&record));
    // Create reader to verify the written data
    auto* reader = (storage::TsFileReader*)tsfile_reader_new(filename, &code);
    ASSERT_OK(code, "create reader failed");

    // Query the data we just wrote
    char* sensors[] = {str_measurement_id, text_measurement_id,
                       date_measurement_id};
    auto* result = (storage::ResultSet*)_tsfile_reader_query_device(
        reader, device_id, sensors, 3, 0, 100, &code);
    ASSERT_OK(code, "query device failed");

    // Verify the retrieved data matches what we inserted
    bool has_next = false;
    int row_count = 0;
    while (result->next(has_next) == common::E_OK && has_next) {
        // Verify timestamp
        EXPECT_EQ(result->get_value<int64_t>(1), row_count);

        // Verify string data
        const common::String* str = result->get_value<common::String*>(2);
        EXPECT_EQ(strlen(test_str), str->len_);
        const char* ret_char =
            tsfile_result_set_get_value_by_index_string(result, 2);
        EXPECT_EQ(strcmp(test_str, ret_char), 0);
        free((void*)ret_char);

        // Verify text data
        const common::String* text = result->get_value<common::String*>(3);
        EXPECT_EQ(strlen(test_text), text->len_);
        const char* ret_text =
            tsfile_result_set_get_value_by_index_string(result, 3);
        EXPECT_EQ(strcmp(test_text, ret_text), 0);
        free((void*)ret_text);

        // Verify date data
        int32_t ret_date =
            tsfile_result_set_get_value_by_index_int32_t(result, 4);
        EXPECT_EQ(test_date, ret_date);

        row_count++;
    }
    free_tsfile_result_set(reinterpret_cast<ResultSet*>(&result));

    ASSERT_OK(tsfile_reader_close(reader), "close reader failed");
    free(device_id);
    free(str_measurement_id);
    free(text_measurement_id);
    free(date_measurement_id);
}

TEST_F(CWrapperTest, WriterFlushTabletAndReadData) {
    ERRNO code = 0;
    const int column_num = 10;
    remove("cwrapper_write_flush_and_read.tsfile");
    TableSchema schema;
    schema.table_name = strdup("testtable0");
    int id_schema_num = 5;
    int field_schema_num = 5;
    schema.column_num = column_num;
    schema.column_schemas =
        static_cast<ColumnSchema*>(malloc(column_num * sizeof(ColumnSchema)));
    for (int i = 0; i < id_schema_num; i++) {
        schema.column_schemas[i] =
            ColumnSchema{strdup(std::string("id" + std::to_string(i)).c_str()),
                         TS_DATATYPE_STRING, TAG};
    }
    for (int i = 0; i < field_schema_num; i++) {
        schema.column_schemas[i + id_schema_num] =
            ColumnSchema{strdup(std::string("s" + std::to_string(i)).c_str()),
                         TS_DATATYPE_INT64, FIELD};
    }
    WriteFile file =
        write_file_new("cwrapper_write_flush_and_read.tsfile", &code);
    TsFileWriter writer = tsfile_writer_new(file, &schema, &code);
    ASSERT_EQ(code, RET_OK);

    char** column_names =
        static_cast<char**>(malloc(column_num * sizeof(char*)));
    TSDataType* data_types =
        static_cast<TSDataType*>(malloc(sizeof(TSDataType) * column_num));
    for (int i = 0; i < id_schema_num; i++) {
        column_names[i] = strdup(std::string("id" + std::to_string(i)).c_str());
        data_types[i] = TS_DATATYPE_STRING;
    }

    for (int i = 0; i < field_schema_num; i++) {
        column_names[i + id_schema_num] =
            strdup(std::string("s" + std::to_string(i)).c_str());
        data_types[i + id_schema_num] = TS_DATATYPE_INT64;
    }

    Tablet tablet = tablet_new(column_names, data_types, column_num, 10);

    int num_timestamp = 10;
    char* literal = new char[std::strlen("device_id") + 1];
    std::strcpy(literal, "device_id");

    for (int l = 0; l < num_timestamp; l++) {
        tablet_add_timestamp(tablet, l, l);
        for (int i = 0; i < schema.column_num; i++) {
            switch (schema.column_schemas[i].data_type) {
                case TS_DATATYPE_STRING:
                    tablet_add_value_by_name_string_with_len(
                        tablet, l, schema.column_schemas[i].column_name,
                        literal, strlen(literal));
                    break;
                case TS_DATATYPE_INT64:
                    tablet_add_value_by_name_int64_t(
                        tablet, l, schema.column_schemas[i].column_name, l);
                    break;
                default:
                    break;
            }
        }
    }
    delete[] literal;
    code = tsfile_writer_write(writer, tablet);
    ASSERT_EQ(code, RET_OK);
    ASSERT_EQ(tsfile_writer_close(writer), 0);

    TsFileReader reader =
        tsfile_reader_new("cwrapper_write_flush_and_read.tsfile", &code);
    ASSERT_EQ(code, 0);
    ResultSet result_set = tsfile_query_table(reader, schema.table_name,
                                              column_names, 10, 0, 100, &code);

    int row = 0;
    while (tsfile_result_set_next(result_set, &code) && code == RET_OK) {
        for (int i = 0; i < schema.column_num; i++) {
            char* ret = nullptr;
            switch (schema.column_schemas[i].data_type) {
                case TS_DATATYPE_STRING:
                    ret = tsfile_result_set_get_value_by_name_string(
                        result_set, schema.column_schemas[i].column_name);
                    ASSERT_EQ(std::string("device_id"), std::string(ret));
                    free(ret);
                    break;
                case TS_DATATYPE_INT64:
                    ASSERT_EQ(row, tsfile_result_set_get_value_by_name_int64_t(
                                       result_set,
                                       schema.column_schemas[i].column_name));
                    break;
                default:
                    break;
            }
        }
        for (int i = 7; i <= 11; i++) {
            ASSERT_EQ(row, tsfile_result_set_get_value_by_index_int64_t(
                               result_set, i));
        }
        row++;
    }
    ASSERT_EQ(row, num_timestamp);
    uint32_t size;
    TableSchema* all_schema =
        tsfile_reader_get_all_table_schemas(reader, &size);
    ASSERT_EQ(1, size);
    ASSERT_EQ(std::string(all_schema[0].table_name),
              std::string(schema.table_name));
    ASSERT_EQ(all_schema[0].column_num, schema.column_num);
    int count_int64_t = 0;
    int count_string = 0;
    for (int i = 0; i < column_num; i++) {
        if (all_schema[0].column_schemas[i].data_type == TS_DATATYPE_INT64) {
            count_int64_t++;
        } else if (all_schema[0].column_schemas[i].data_type ==
                   TS_DATATYPE_STRING) {
            count_string++;
        }
    }

    ASSERT_EQ(5, count_int64_t);
    ASSERT_EQ(5, count_string);
    free_tablet(&tablet);
    tsfile_reader_close(reader);
    free_tsfile_result_set(&result_set);
    free_table_schema(schema);
    free_table_schema(*all_schema);
    free(all_schema);
    for (int i = 0; i < column_num; i++) {
        free(column_names[i]);
    }
    free(column_names);
    free(data_types);
    free_write_file(&file);
}

// Regression: tsfile_writer_new_with_memory_threshold() had its duplicate-
// column check inverted (`==` instead of `!=`), so the very first column
// always looked like a duplicate and the constructor returned
// E_INVALID_SCHEMA before any legitimate schema could be used.  Compare to
// tsfile_writer_new() in the same file which had the correct check.
TEST(TsFileWriterCApiTest, NewWithMemoryThresholdAcceptsValidSchema) {
    const char* path = "cwrapper_writer_with_threshold_smoke.tsfile";
    remove(path);
    ERRNO code = 0;
    WriteFile file = write_file_new(path, &code);
    ASSERT_EQ(code, RET_OK);

    const int column_num = 3;
    TableSchema schema;
    schema.table_name = strdup("t");
    schema.column_num = column_num;
    schema.column_schemas =
        static_cast<ColumnSchema*>(malloc(sizeof(ColumnSchema) * column_num));
    schema.column_schemas[0] =
        ColumnSchema{strdup("id1"), TS_DATATYPE_STRING, TAG};
    schema.column_schemas[1] =
        ColumnSchema{strdup("s1"), TS_DATATYPE_INT64, FIELD};
    schema.column_schemas[2] =
        ColumnSchema{strdup("s2"), TS_DATATYPE_DOUBLE, FIELD};

    TsFileWriter writer = tsfile_writer_new_with_memory_threshold(
        file, &schema, 1024 * 1024, &code);
    EXPECT_NE(writer, nullptr) << "constructor refused a valid 3-column schema";
    EXPECT_EQ(code, RET_OK);

    // Duplicate column triggers the now-correct path.
    TableSchema dup;
    dup.table_name = strdup("t");
    dup.column_num = 2;
    dup.column_schemas =
        static_cast<ColumnSchema*>(malloc(sizeof(ColumnSchema) * 2));
    dup.column_schemas[0] =
        ColumnSchema{strdup("s1"), TS_DATATYPE_INT64, FIELD};
    dup.column_schemas[1] =
        ColumnSchema{strdup("s1"), TS_DATATYPE_INT64, FIELD};
    ERRNO dup_code = 0;
    TsFileWriter dup_writer = tsfile_writer_new_with_memory_threshold(
        file, &dup, 1024 * 1024, &dup_code);
    EXPECT_EQ(dup_writer, nullptr);
    EXPECT_EQ(dup_code, common::E_INVALID_SCHEMA);

    if (writer != nullptr) {
        tsfile_writer_close(writer);
    }
    free_table_schema(schema);
    free_table_schema(dup);
    free_write_file(&file);
    remove(path);
}

// Regression: tsfile_writer_new / tsfile_writer_new_with_memory_threshold /
// _tsfile_writer_register_table used to dereference null inputs directly,
// crashing the host process.  Each now reports E_INVALID_ARG (or returns
// nullptr when err_code itself is null) instead of segfaulting.
TEST(TsFileWriterCApiTest, RejectsNullInputs) {
    ERRNO err = 0;

    // tsfile_writer_new: null file
    EXPECT_EQ(
        tsfile_writer_new(nullptr, reinterpret_cast<TableSchema*>(1), &err),
        nullptr);
    EXPECT_EQ(err, common::E_INVALID_ARG);

    // tsfile_writer_new: null schema
    err = 0;
    EXPECT_EQ(tsfile_writer_new(reinterpret_cast<WriteFile>(1), nullptr, &err),
              nullptr);
    EXPECT_EQ(err, common::E_INVALID_ARG);

    // tsfile_writer_new: null err_code
    EXPECT_EQ(tsfile_writer_new(nullptr, nullptr, nullptr), nullptr);

    // tsfile_writer_new_with_memory_threshold: same checks
    err = 0;
    EXPECT_EQ(tsfile_writer_new_with_memory_threshold(
                  nullptr, reinterpret_cast<TableSchema*>(1), 1024, &err),
              nullptr);
    EXPECT_EQ(err, common::E_INVALID_ARG);

    // _tsfile_writer_register_table: nulls
    EXPECT_EQ(_tsfile_writer_register_table(nullptr,
                                            reinterpret_cast<TableSchema*>(1)),
              common::E_INVALID_ARG);
    EXPECT_EQ(_tsfile_writer_register_table(reinterpret_cast<TsFileWriter>(1),
                                            nullptr),
              common::E_INVALID_ARG);
}

// Regression: the tag-filter C API used to dereference a null reader and
// pass null char pointers straight to std::string(), crashing the host
// process.  Each entry point must now return nullptr / E_INVALID_ARG on
// missing inputs instead of segfaulting.  This test only checks the guards
// are in place — it deliberately never touches a real reader.
TEST(TagFilterCApiTest, RejectsNullInputs) {
    const char* table = "t";
    const char* col = "c";
    const char* val = "v";

    EXPECT_EQ(tsfile_tag_filter_eq(nullptr, table, col, val), nullptr);
    EXPECT_EQ(tsfile_tag_filter_eq(reinterpret_cast<TsFileReader>(1), nullptr,
                                   col, val),
              nullptr);
    EXPECT_EQ(tsfile_tag_filter_eq(reinterpret_cast<TsFileReader>(1), table,
                                   nullptr, val),
              nullptr);
    EXPECT_EQ(tsfile_tag_filter_eq(reinterpret_cast<TsFileReader>(1), table,
                                   col, nullptr),
              nullptr);

    EXPECT_EQ(tsfile_tag_filter_neq(nullptr, table, col, val), nullptr);
    EXPECT_EQ(tsfile_tag_filter_lt(nullptr, table, col, val), nullptr);
    EXPECT_EQ(tsfile_tag_filter_lteq(nullptr, table, col, val), nullptr);
    EXPECT_EQ(tsfile_tag_filter_gt(nullptr, table, col, val), nullptr);
    EXPECT_EQ(tsfile_tag_filter_gteq(nullptr, table, col, val), nullptr);

    ERRNO err = common::E_OK;
    EXPECT_EQ(
        tsfile_tag_filter_create(nullptr, table, col, val, TAG_FILTER_EQ, &err),
        nullptr);
    EXPECT_EQ(err, common::E_INVALID_ARG);

    err = common::E_OK;
    EXPECT_EQ(tsfile_tag_filter_create(reinterpret_cast<TsFileReader>(1),
                                       nullptr, col, val, TAG_FILTER_EQ, &err),
              nullptr);
    EXPECT_EQ(err, common::E_INVALID_ARG);

    err = common::E_OK;
    EXPECT_EQ(tsfile_tag_filter_create(reinterpret_cast<TsFileReader>(1), table,
                                       nullptr, val, TAG_FILTER_EQ, &err),
              nullptr);
    EXPECT_EQ(err, common::E_INVALID_ARG);

    err = common::E_OK;
    EXPECT_EQ(tsfile_tag_filter_create(reinterpret_cast<TsFileReader>(1), table,
                                       col, nullptr, TAG_FILTER_EQ, &err),
              nullptr);
    EXPECT_EQ(err, common::E_INVALID_ARG);

    // err_code itself is null — must not crash, must return null.
    EXPECT_EQ(tsfile_tag_filter_create(reinterpret_cast<TsFileReader>(1), table,
                                       col, val, TAG_FILTER_EQ, nullptr),
              nullptr);
}

}  // namespace cwrapper
