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
extern "C" {
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
}

#include "common/tablet.h"
#include "utils/errno_define.h"
namespace CReleaseTest {
class CReleaseTest : public testing::Test {};

TEST_F(CReleaseTest, TestCreateFile) {
    ERRNO error_no = RET_OK;
    // Create File and Get RET_OK
    WriteFile file = write_file_new("create_file1.tsfile", &error_no);
    ASSERT_EQ(RET_OK, error_no);
    free_write_file(&file);

    // Already exists
    file = write_file_new("create_file1.tsfile", &error_no);
    ASSERT_EQ(RET_ALREADY_EXIST, error_no);
    ASSERT_EQ(nullptr, file);

    // Folder
    file = write_file_new("test/", &error_no);
    ASSERT_EQ(RET_FILRET_OPEN_ERR, error_no);

    remove("create_file1.tsfile");
    free_write_file(&file);
}

TEST_F(CReleaseTest, TsFileWriterNew) {
    ERRNO error_code = RET_OK;

    TableSchema test_schema;
    test_schema.table_name = strdup("test_table");
    test_schema.column_num = 0;

    // Invalid schema
    WriteFile file = write_file_new("test_empty_schema.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);
    TsFileWriter writer = tsfile_writer_new(file, &test_schema, &error_code);
    ASSERT_EQ(RET_INVALID_SCHEMA, error_code);
    ASSERT_EQ(nullptr, writer);
    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);
    ASSERT_EQ(nullptr, file);
    remove("test_empty_schema.tsfile");

    // Invalid schema with memory threshold
    file = write_file_new("test_empty_schema_memory_threshold.tsfile",
                          &error_code);
    ASSERT_EQ(RET_OK, error_code);
    // Invalid schema
    writer = tsfile_writer_new_with_memory_threshold(file, &test_schema, 100,
                                                     &error_code);
    ASSERT_EQ(RET_INVALID_SCHEMA, error_code);
    ASSERT_EQ(nullptr, writer);
    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);
    ASSERT_EQ(nullptr, file);
    remove("test_empty_schema_memory_threshold.tsfile");

    // Normal schema
    file = write_file_new("test_empty_writer.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);

    TableSchema table_schema;
    table_schema.table_name = strdup("test_table");
    table_schema.column_num = 2;
    table_schema.column_schemas =
        static_cast<ColumnSchema *>(malloc(sizeof(ColumnSchema) * 2));
    table_schema.column_schemas[0] =
        (ColumnSchema){.column_name = strdup("col1"),
                       .data_type = TS_DATATYPE_STRING,
                       .column_category = TAG};
    table_schema.column_schemas[1] =
        (ColumnSchema){.column_name = strdup("col2"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};

    writer = tsfile_writer_new(file, &table_schema, &error_code);
    ASSERT_EQ(RET_OK, error_code);
    error_code = tsfile_writer_close(writer);
    ASSERT_EQ(RET_OK, error_code);
    free_write_file(&file);
    remove("test_empty_writer.tsfile");

    free_table_schema(table_schema);
    free_table_schema(test_schema);
}

TEST_F(CReleaseTest, TsFileWriterWriteDataAbnormalColumn) {
    remove("TsFileWriterWriteDataAbnormalColumn.tsfile");
    ERRNO error_code = RET_OK;
    WriteFile file = write_file_new(
        "TsFileWriterWriteDataAbnormalColumn.tsfile", &error_code);

    TableSchema abnormal_schema;
    abnormal_schema.table_name = strdup("!@#$%^*()_+-=");
    abnormal_schema.column_num = 3;
    abnormal_schema.column_schemas =
        static_cast<ColumnSchema *>(malloc(sizeof(ColumnSchema) * 4));
    abnormal_schema.column_schemas[0] =
        (ColumnSchema){.column_name = strdup("!@#$%^*()_+-="),
                       .data_type = TS_DATATYPE_STRING,
                       .column_category = TAG};

    // TAG's datatype is not correct
    abnormal_schema.column_schemas[1] =
        (ColumnSchema){.column_name = strdup("TAG2"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = TAG};

    // same column name with column[0]
    abnormal_schema.column_schemas[2] =
        (ColumnSchema){.column_name = strdup("!@#$%^*()_+-="),
                       .data_type = TS_DATATYPE_DOUBLE,
                       .column_category = FIELD};

    // column name conflict
    TsFileWriter writer =
        tsfile_writer_new(file, &abnormal_schema, &error_code);
    ASSERT_EQ(RET_INVALID_SCHEMA, error_code);
    free(abnormal_schema.column_schemas[2].column_name);

    abnormal_schema.column_schemas[2] =
        (ColumnSchema){.column_name = strdup("!@#$%^*()_+-=1"),
                       .data_type = TS_DATATYPE_DOUBLE,
                       .column_category = FIELD};

    // datatype conflict
    writer = tsfile_writer_new(file, &abnormal_schema, &error_code);
    ASSERT_EQ(RET_INVALID_SCHEMA, error_code);

    free(abnormal_schema.column_schemas[1].column_name);
    abnormal_schema.column_schemas[1] =
        (ColumnSchema){.column_name = strdup("TAG2"),
                       .data_type = TS_DATATYPE_STRING,
                       .column_category = TAG};

    writer = tsfile_writer_new(file, &abnormal_schema, &error_code);
    ASSERT_EQ(RET_OK, error_code);

    char **column_list = static_cast<char **>(malloc(sizeof(char *) * 3));
    column_list[0] = strdup("!@#$%^*()_+-=");
    column_list[1] = strdup("TAG2");
    column_list[2] = strdup("!@#$%^*()_+-=1");
    TSDataType *type_list =
        static_cast<TSDataType *>(malloc(sizeof(TSDataType) * 3));
    type_list[0] = TS_DATATYPE_STRING;
    type_list[1] = TS_DATATYPE_STRING;
    type_list[2] = TS_DATATYPE_DOUBLE;
    Tablet tablet = tablet_new(column_list, type_list, 3, 100);
    for (int i = 0; i < 100; i++) {
        tablet_add_timestamp(tablet, i, static_cast<int64_t>(i));
        tablet_add_value_by_name_string(tablet, i, "!@#$%^*()_+-=", "device1");
        tablet_add_value_by_index_string(
            tablet, i, 1, std::string("sensor" + std::to_string(i)).c_str());
        tablet_add_value_by_name_double(tablet, i, "!@#$%^*()_+-=1", i * 100.0);
    }
    ASSERT_EQ(RET_OK, tsfile_writer_write(writer, tablet));
    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);

    TsFileReader reader = tsfile_reader_new(
        "TsFileWriterWriteDataAbnormalColumn.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);
    int i = 0;
    ResultSet result_set = tsfile_query_table(
        reader, "!@#$%^*()_+-=", column_list, 3, 0, 100, &error_code);
    while (tsfile_result_set_next(result_set, &error_code) &&
           error_code == RET_OK) {
        Timestamp timestamp =
            tsfile_result_set_get_value_by_name_int64_t(result_set, "time");
        ASSERT_EQ(timestamp * 100.0, tsfile_result_set_get_value_by_name_double(
                                         result_set, "!@#$%^*()_+-=1"));
        char *value_str =
            tsfile_result_set_get_value_by_index_string(result_set, 2);
        ASSERT_EQ("device1", std::string(value_str));
        free(value_str);
        i++;
    }
    ASSERT_EQ(100, i);
    for (int i = 0; i < 3; i++) {
        free(column_list[i]);
    }
    free(column_list);
    free(type_list);
    free_write_file(&file);
    free_table_schema(abnormal_schema);
    free_tablet(&tablet);
    free_tsfile_result_set(&result_set);
    tsfile_reader_close(reader);
    remove("TsFileWriterWriteDataAbnormalColumn.tsfile");
}

TEST_F(CReleaseTest, TsFileWriterMultiDataType) {
    ERRNO error_code = RET_OK;
    remove("TsFileWriterMultiDataType.tsfile");
    WriteFile file =
        write_file_new("TsFileWriterMultiDataType.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);
    TableSchema all_type_schema;
    all_type_schema.table_name = strdup("All_Datatype");
    all_type_schema.column_num = 6;
    all_type_schema.column_schemas =
        static_cast<ColumnSchema *>(malloc(sizeof(ColumnSchema) * 6));
    all_type_schema.column_schemas[0] =
        (ColumnSchema){.column_name = strdup("TAG"),
                       .data_type = TS_DATATYPE_STRING,
                       .column_category = TAG};
    all_type_schema.column_schemas[1] =
        (ColumnSchema){.column_name = strdup("INT32"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};
    all_type_schema.column_schemas[2] =
        (ColumnSchema){.column_name = strdup("INT64"),
                       .data_type = TS_DATATYPE_INT64,
                       .column_category = FIELD};
    all_type_schema.column_schemas[3] =
        (ColumnSchema){.column_name = strdup("FLOAT"),
                       .data_type = TS_DATATYPE_FLOAT,
                       .column_category = FIELD};
    all_type_schema.column_schemas[4] =
        (ColumnSchema){.column_name = strdup("DOUBLE"),
                       .data_type = TS_DATATYPE_DOUBLE,
                       .column_category = FIELD};
    all_type_schema.column_schemas[5] =
        (ColumnSchema){.column_name = strdup("BOOLEAN"),
                       .data_type = TS_DATATYPE_BOOLEAN,
                       .column_category = FIELD};

    TsFileWriter writer =
        tsfile_writer_new(file, &all_type_schema, &error_code);
    ASSERT_EQ(RET_OK, error_code);

    free_table_schema(all_type_schema);
    char **column_list = static_cast<char **>(malloc(sizeof(char *) * 6));
    column_list[0] = strdup("TAG");
    column_list[1] = strdup("INT32");
    column_list[2] = strdup("INT64");
    column_list[3] = strdup("FLOAT");
    column_list[4] = strdup("DOUBLE");
    column_list[5] = strdup("BOOLEAN");
    TSDataType *type_list =
        static_cast<TSDataType *>(malloc(sizeof(TSDataType) * 6));
    type_list[0] = TS_DATATYPE_STRING;
    type_list[1] = TS_DATATYPE_INT32;
    type_list[2] = TS_DATATYPE_INT64;
    type_list[3] = TS_DATATYPE_FLOAT;
    type_list[4] = TS_DATATYPE_DOUBLE;
    type_list[5] = TS_DATATYPE_BOOLEAN;
    Tablet tablet = tablet_new(column_list, type_list, 6, 1000);
    for (int i = 0; i < 1000; i++) {
        // negative timestamp included
        tablet_add_timestamp(tablet, i, static_cast<int64_t>(i - 10));
        tablet_add_value_by_name_string(tablet, i, "TAG", "device1");
        tablet_add_value_by_name_int32_t(tablet, i, "INT32", i);
        tablet_add_value_by_index_int64_t(tablet, i, 2, i * 100);
        tablet_add_value_by_index_float(tablet, i, 3, i * 100.0);
        if (i > 900) {
            continue;
        }
        // Null value
        tablet_add_value_by_index_double(tablet, i, 4, i * 100.0);
        tablet_add_value_by_index_bool(tablet, i, 5, i % 2 == 0);
    }
    ASSERT_EQ(RET_OK, tsfile_writer_write(writer, tablet));
    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);

    TsFileReader reader =
        tsfile_reader_new("TsFileWriterMultiDataType.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);
    ResultSet result_set = tsfile_query_table(
        reader, "all_datatype", column_list, 6, 0, 1000, &error_code);
    int row_num = 0;
    while (tsfile_result_set_next(result_set, &error_code) &&
           error_code == RET_OK) {
        Timestamp timestamp =
            tsfile_result_set_get_value_by_name_int64_t(result_set, "time");
        int64_t value = timestamp + 10;
        char *str_value =
            tsfile_result_set_get_value_by_name_string(result_set, "TAG");
        ASSERT_EQ("device1", std::string(str_value));
        free(str_value);
        ASSERT_EQ(value, tsfile_result_set_get_value_by_name_int32_t(result_set,
                                                                     "int32"));
        ASSERT_EQ(value * 100, tsfile_result_set_get_value_by_name_int64_t(
                                   result_set, "int64"));
        ASSERT_EQ(value * 100.0, tsfile_result_set_get_value_by_name_float(
                                     result_set, "FLOAT"));

        if (value <= 900) {
            ASSERT_EQ(value * 100.0, tsfile_result_set_get_value_by_name_double(
                                         result_set, "DOUBLE"));
            ASSERT_EQ(value % 2 == 0, tsfile_result_set_get_value_by_name_bool(
                                          result_set, "BOOLEAN"));
        } else {
            ASSERT_TRUE(
                tsfile_result_set_is_null_by_name(result_set, "DOUBLE"));
        }
        row_num++;
    }
    ASSERT_EQ(990, row_num);
    free_tsfile_result_set(&result_set);
    tsfile_reader_close(reader);
    for (int i = 0; i < 6; i++) {
        free(column_list[i]);
    }
    free_tablet(&tablet);
    free(column_list);
    free(type_list);
    remove("TsFileWriterMultiDataType.tsfile");
}

}  // namespace CReleaseTest