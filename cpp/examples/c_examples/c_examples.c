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

#include "c_examples.h"

#include <fcntl.h>
#include <malloc.h>
#include <stdbool.h>
#include <stdio.h>
#include <unistd.h>

#define HANDLE_ERROR(err_no)                  \
    do {                                      \
        if (err_no != 0) {                    \
            printf("get err no: %d", err_no); \
            return err_no;                    \
        }                                     \
    } while (0)

ErrorCode write_tsfile() {
    ErrorCode err_code;
    CTsFileWriter writer;
    if (access("c_rw.tsfile", 0) == 0) {
        if (remove("test.tsfile") != 0) {
            printf("Failed to delete test.tsfile file\n");
            return -1;
        }
    }
    writer = ts_writer_open("c_rw.tsfile", &err_code);
    if (NULL == writer) {
        return err_code;
    }
    ColumnSchema columnSchema;
    columnSchema.name = "temperature";
    columnSchema.column_def = TS_TYPE_INT32;
    err_code =
        tsfile_register_table_column(writer, "test_table", &columnSchema);
    HANDLE_ERROR(err_code);
    TableSchema tableSchema;
    tableSchema.column_num = 3;
    tableSchema.table_name = "test_table";
    tableSchema.column_schema =
        (ColumnSchema **)malloc(tableSchema.column_num * sizeof(TableSchema *));
    tableSchema.column_schema[0] = (ColumnSchema *)malloc(sizeof(ColumnSchema));
    tableSchema.column_schema[0]->column_def = TS_TYPE_DOUBLE;
    tableSchema.column_schema[0]->name = "level";
    tableSchema.column_schema[1] = (ColumnSchema *)malloc(sizeof(ColumnSchema));
    tableSchema.column_schema[1]->column_def = TS_TYPE_BOOLEAN;
    tableSchema.column_schema[1]->name = "up";
    tableSchema.column_schema[2] = (ColumnSchema *)malloc(sizeof(ColumnSchema));
    tableSchema.column_schema[2]->column_def = TS_TYPE_FLOAT;
    tableSchema.column_schema[2]->name = "humi";
    err_code = tsfile_register_table(writer, &tableSchema);
    free(tableSchema.column_schema[0]);
    free(tableSchema.column_schema[1]);
    free(tableSchema.column_schema[2]);
    free(tableSchema.column_schema);
    HANDLE_ERROR(err_code);
    printf("register table success\n");
    TsFileRowData rowData = create_tsfile_row("test_table", 1, 4);
    insert_data_into_tsfile_row_double(rowData, "level", 10);
    insert_data_into_tsfile_row_float(rowData, "humi", 10.0f);
    insert_data_into_tsfile_row_boolean(rowData, "up", true);
    insert_data_into_tsfile_row_int32(rowData, "temperature", 10);
    err_code = tsfile_write_row_data(writer, rowData);

    rowData = create_tsfile_row("test_table", 2, 4);
    insert_data_into_tsfile_row_double(rowData, "level", 12);
    err_code = tsfile_write_row_data(writer, rowData);

    for (int ind = 10; ind < 2000; ind++) {
        rowData = create_tsfile_row("test_table", ind, 4);
        insert_data_into_tsfile_row_double(rowData, "level", 12 + ind);
        insert_data_into_tsfile_row_float(rowData, "humi", 12.0f + ind);
        insert_data_into_tsfile_row_boolean(rowData, "up", true);
        insert_data_into_tsfile_row_int32(rowData, "temperature", 12 + ind);
        err_code = tsfile_write_row_data(writer, rowData);
    }
    printf("writer row data success\n");
    HANDLE_ERROR(err_code);
    HANDLE_ERROR(tsfile_flush_data(writer));
    printf("flush data success\n");
    HANDLE_ERROR(ts_writer_close(writer));
    printf("close writer success\n");
    return 0;
}

ErrorCode read_tsfile() {
    ErrorCode err_code;
    CTsFileReader reader;
    reader = ts_reader_open("c_rw.tsfile", &err_code);
    if (NULL == reader) {
        return err_code;
    }
    const char *columns[] = {"temperature", "level", "up", "humi"};
    //  TimeFilterExpression* exp = create_andquery_timefilter();
    //  TimeFilterExpression* time_filter = create_time_filter("test_table",
    //  "temperature", GT, 11); TimeFilterExpression* time_filter2 =
    //  create_time_filter("test_table", "humi", GT, 10); TimeFilterExpression*
    //  time_filter3 = create_time_filter("test_table", "level", LE, 20);
    //  add_time_filter_to_and_query(exp, time_filter);
    //  add_time_filter_to_and_query(exp, time_filter2);
    //  add_time_filter_to_and_query(exp, time_filter3);

    QueryDataRet ret = ts_reader_query(reader, "test_table", columns, 4, NULL);
    printf("query success\n");
    DataResult *result = ts_next(ret, 20);
    if (result == NULL) {
        printf("get result failed\n");
        return -1;
    }
    print_data_result(result);
    //  destory_time_filter_query(exp);
    HANDLE_ERROR(destory_query_dataret(ret));
    HANDLE_ERROR(destory_tablet(result));
    return 0;
}
