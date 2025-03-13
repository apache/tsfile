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
}

TEST_F(CReleaseTest, TsFileWriterNew) {
    ERRNO error_code = RET_OK;

    remove("test.tsfile");
    TableSchema test_schema;
    test_schema.table_name = strdup("test_table");
    test_schema.column_num = 0;

    WriteFile file = write_file_new("test.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);
    // Invalid schema
    TsFileWriter writer = tsfile_writer_new(file, &test_schema, &error_code);
    ASSERT_EQ(RET_INVALID_SCHEMA, error_code);
    ASSERT_EQ(nullptr, writer);

    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);
    ASSERT_EQ(nullptr, file);
    remove("test.tsfile");
    file = write_file_new("test.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);
    // Invalid schema
    writer = tsfile_writer_new_with_memory_threshold(file, &test_schema, 100,
                                                     &error_code);
    ASSERT_EQ(RET_INVALID_SCHEMA, error_code);
    ASSERT_EQ(nullptr, writer);

    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);
    ASSERT_EQ(nullptr, file);
    remove("test.tsfile");

    file = write_file_new("test.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);

    TableSchema tableSchema;
    tableSchema.table_name = strdup("test_table");
    tableSchema.column_num = 2;
    tableSchema.column_schemas =
        static_cast<ColumnSchema *>(malloc(sizeof(ColumnSchema) * 2));
    tableSchema.column_schemas[0] =
        (ColumnSchema){.column_name = strdup("col1"),
                       .data_type = TS_DATATYPE_STRING,
                       .column_category = TAG};
    tableSchema.column_schemas[1] =
        (ColumnSchema){.column_name = strdup("col2"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};

    writer = tsfile_writer_new(file, &tableSchema, &error_code);
    ASSERT_EQ(RET_OK, error_code);

    // Close empty writer.
    error_code = tsfile_writer_close(writer);
    ASSERT_EQ(RET_OK, error_code);

    free_write_file(&file);
    remove("test.tsfile");
}

TEST_F(CReleaseTest, TsFileWriterWriteDataAbnormalColumn) {
    ERRNO error_code = RET_OK;
    remove("TsFileWriterWriteDataAbnormalColumn_3_100.tsfile");
    WriteFile file = write_file_new(
        "TsFileWriterWriteDataAbnormalColumn_3_100.tsfile", &error_code);

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

    abnormal_schema.column_schemas[2] =
        (ColumnSchema){.column_name = strdup("!@#$%^*()_+-=1"),
                       .data_type = TS_DATATYPE_DOUBLE,
                       .column_category = FIELD};

    // datatype conflict
    writer = tsfile_writer_new(file, &abnormal_schema, &error_code);
    ASSERT_EQ(RET_INVALID_SCHEMA, error_code);

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

    TsFileReader reader = tsfile_reader_new(
        "TsFileWriterWriteDataAbnormalColumn_3_100.tsfile", &error_code);
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
        ASSERT_EQ("device1",
                  std::string(tsfile_result_set_get_value_by_index_string(
                      result_set, 2)));
        i++;
    }
    ASSERT_EQ(100, i);
    free_tsfile_result_set(&result_set);
    tsfile_reader_close(reader);
}

TEST_F(CReleaseTest, TsFileWriterMultiDataType) {
    ERRNO error_code = RET_OK;
    remove("TsFileWriterWriteDataAbnormalColumn_3_100.tsfile");
    WriteFile file = write_file_new(
        "TsFileWriterWriteDataAbnormalColumn_3_100.tsfile", &error_code);
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

    TsFileReader reader = tsfile_reader_new(
        "TsFileWriterWriteDataAbnormalColumn_3_100.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);
    ResultSet result_set = tsfile_query_table(
        reader, "all_datatype", column_list, 6, 0, 1000, &error_code);
    while (tsfile_result_set_next(result_set, &error_code) &&
           error_code == RET_OK) {
        Timestamp timestamp =
            tsfile_result_set_get_value_by_name_int64_t(result_set, "time");
        int64_t value = timestamp + 10;
        ASSERT_EQ("device1",
                  std::string(tsfile_result_set_get_value_by_name_string(
                      result_set, "TAG")));
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
            ASSERT_TRUE(tsfile_result_set_is_null_by_name(result_set, "DOUBLE"));
        }
    }
    free_tsfile_result_set(&result_set);
    tsfile_reader_close(reader);
}

TEST_F(CReleaseTest, TsFileWriterSameDataType) {
    ERRNO error_code = RET_OK;
    std::vector<TSDataType> data_types = {
        TS_DATATYPE_INT32,  TS_DATATYPE_INT64,   TS_DATATYPE_FLOAT,
        TS_DATATYPE_DOUBLE, TS_DATATYPE_BOOLEAN, TS_DATATYPE_STRING};
    std::vector<std::string> data_type_name = {"INT32",  "INT64",   "FLOAT",
                                               "DOUBLE", "BOOLEAN", "STRING"};
    for (int i = 0; i < 6; i++) {
        remove(std::string("TsFileWriterSameDataType_" + data_type_name[i] +
                           ".tsfile")
                   .c_str());
        WriteFile file =
            write_file_new(std::string("TsFileWriterSameDataType_" +
                                       data_type_name[i] + ".tsfile")
                               .c_str(),
                           &error_code);
        TableSchema schema;
        schema.table_name =
            strdup(std::string("table_" + data_type_name[i]).c_str());
        schema.column_num = 3;
        schema.column_schemas =
            static_cast<ColumnSchema *>(malloc(sizeof(ColumnSchema) * 3));
        schema.column_schemas[0] =
            (ColumnSchema){.column_name = strdup("TAG"),
                           .data_type = TS_DATATYPE_STRING,
                           .column_category = TAG};
        schema.column_schemas[1] =
            (ColumnSchema){.column_name = strdup("VALUE"),
                           .data_type = data_types[i],
                           .column_category = FIELD};
        schema.column_schemas[2] =
            (ColumnSchema){.column_name = strdup("VALUE2"),
                           .data_type = data_types[i],
                           .column_category = FIELD};

        TsFileWriter writer = tsfile_writer_new(file, &schema, &error_code);
        ASSERT_EQ(RET_OK, error_code);
        char **column_name = static_cast<char **>(malloc(sizeof(char *) * 3));
        column_name[0] = strdup("TAG");
        column_name[1] = strdup("VALUE");
        column_name[2] = strdup("VALUE2");
        TSDataType *datatype =
            static_cast<TSDataType *>(malloc(sizeof(char *) * 3));
        datatype[0] = TS_DATATYPE_STRING;
        datatype[1] = data_types[i];
        datatype[2] = data_types[i];
        Tablet tablet = tablet_new(column_name, datatype, 3, 1000);
        for (int j = 0; j < 1000; j++) {
            tablet_add_timestamp(tablet, j, static_cast<int64_t>(j));
            tablet_add_value_by_name_string(tablet, j, "TAG", "device1");
            switch (data_types[i]) {
                case TS_DATATYPE_INT32:
                    tablet_add_value_by_index_int32_t(tablet, j, 1, j);
                    tablet_add_value_by_index_int32_t(tablet, j, 2, j * 2);
                    break;
                case TS_DATATYPE_INT64:
                    tablet_add_value_by_index_int64_t(tablet, j, 1, j * 100);
                    tablet_add_value_by_index_int64_t(tablet, j, 2, j * 200);
                    break;
                case TS_DATATYPE_FLOAT:
                    tablet_add_value_by_index_float(tablet, j, 1, j * 100.0);
                    tablet_add_value_by_index_float(tablet, j, 2, j * 200.0);
                    break;
                case TS_DATATYPE_DOUBLE:
                    tablet_add_value_by_index_double(tablet, j, 1, j * 100.0);
                    tablet_add_value_by_index_double(tablet, j, 2, j * 200.0);
                    break;
                case TS_DATATYPE_BOOLEAN:
                    tablet_add_value_by_index_bool(tablet, j, 1, j % 2 == 0);
                    tablet_add_value_by_index_bool(tablet, j, 2, j % 2 == 1);
                    break;
                case TS_DATATYPE_STRING:
                    tablet_add_value_by_index_string(
                        tablet, j, 1,
                        std::string("sensor" + std::to_string(j)).c_str());
                    tablet_add_value_by_index_string(
                        tablet, j, 2,
                        std::string("sensor" + std::to_string(j * 2)).c_str());
                    break;
                default:
                    break;
            }
        }
        ASSERT_EQ(RET_OK, tsfile_writer_write(writer, tablet));
        ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
        free_write_file(&file);
    }
}

}  // namespace CReleaseTest