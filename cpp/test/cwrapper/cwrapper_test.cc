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
#include <unistd.h>
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
}  // namespace cwrapper