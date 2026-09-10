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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "tsfile_labview.h"

#define CHECK(condition)                                                    \
    do {                                                                    \
        if (!(condition)) {                                                 \
            fprintf(stderr, "FAIL: %s at line %d\n", #condition, __LINE__); \
            return 1;                                                       \
        }                                                                   \
    } while (0)

#define CHECK_OK(expression) CHECK((expression) == 0)
#define CHECK_REJECTED(expression) CHECK((expression) != 0)

static int open_writer(const char* path, const char* table,
                       LV_Handle* out_writer) {
    remove(path);
    LV_Handle builder = lv_tsfile_schema_builder_new(table);
    CHECK(builder != 0);
    CHECK_OK(lv_tsfile_schema_builder_add_column(builder, "value",
                                                 LV_TYPE_DOUBLE, LV_CAT_FIELD));
    CHECK_OK(lv_tsfile_writer_open(path, builder, 0, out_writer));
    lv_tsfile_schema_builder_free(builder);
    return 0;
}

static int write_rows(LV_Handle writer) {
    const int64_t timestamps[3] = {1, 2, 3};
    const double values[3] = {1.25, 2.25, 3.25};
    CHECK_OK(lv_tsfile_write_block_f64(writer, timestamps, values, 3, 1));
    return 0;
}

static int verify_file(const char* path, const char* table) {
    LV_Handle reader = 0;
    LV_Handle result = 0;
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_OK(lv_tsfile_query_table(reader, table, "value", 0, 10, &result));

    LV_Status error = 0;
    int rows = 0;
    while (lv_tsfile_rs_next(result, &error) == 1 && error == 0) {
        CHECK(lv_tsfile_rs_get_i64(result, 0) == rows + 1);
        CHECK(lv_tsfile_rs_get_f64(result, 1) == rows + 1.25);
        ++rows;
    }
    CHECK(error == 0);
    CHECK(rows == 3);
    lv_tsfile_rs_free(result);
    CHECK_OK(lv_tsfile_reader_close(reader));
    return 0;
}

static int test_async_close(void) {
    static const char* path = "lv_async_close.tsfile";
    static const char* table = "async_close";
    LV_Handle writer = 0;
    LV_Handle task = 123;

    CHECK_REJECTED(lv_tsfile_writer_close_ex(0, 1, &task));
    CHECK(task == 0);
    CHECK(open_writer(path, table, &writer) == 0);

    task = 123;
    CHECK_REJECTED(lv_tsfile_writer_close_ex(writer, 2, &task));
    CHECK(task == 0);
    CHECK_REJECTED(lv_tsfile_writer_close_ex(writer, 1, NULL));
    CHECK(write_rows(writer) == 0);

    CHECK_OK(lv_tsfile_writer_close_ex(writer, 1, &task));
    CHECK(task != 0);
    CHECK_REJECTED(lv_tsfile_writer_flush(writer));
    CHECK_OK(lv_tsfile_close_task_wait(task));
    CHECK_REJECTED(lv_tsfile_close_task_wait(task));
    CHECK(verify_file(path, table) == 0);
    remove(path);
    return 0;
}

static int test_sync_close(void) {
    static const char* path = "lv_sync_close_ex.tsfile";
    static const char* table = "sync_close_ex";
    LV_Handle writer = 0;
    LV_Handle task = 123;

    CHECK(open_writer(path, table, &writer) == 0);
    CHECK(write_rows(writer) == 0);
    CHECK_OK(lv_tsfile_writer_close_ex(writer, 0, &task));
    CHECK(task == 0);
    CHECK(verify_file(path, table) == 0);
    remove(path);
    return 0;
}

int main(void) {
    if (test_async_close() != 0) {
        return 1;
    }
    if (test_sync_close() != 0) {
        return 1;
    }
    return 0;
}
