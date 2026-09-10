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

#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "tsfile_labview.h"

#define CHECK_OK(expr)                                                      \
    do {                                                                    \
        LV_Status status_ = (expr);                                         \
        if (status_ != 0) {                                                 \
            fprintf(stderr, "FAIL: %s returned %d\n", #expr, (int)status_); \
            return 1;                                                       \
        }                                                                   \
    } while (0)

#define CHECK_TRUE(expr)                                               \
    do {                                                               \
        if (!(expr)) {                                                 \
            fprintf(stderr, "FAIL: %s at line %d\n", #expr, __LINE__); \
            return 1;                                                  \
        }                                                              \
    } while (0)

static int create_file(const char* path, const char* table, uint8_t type,
                       int rows, int cols) {
    LV_Handle builder = 0;
    LV_Handle writer = 0;
    int64_t* timestamps = NULL;
    void* values = NULL;
    size_t cell_count = (size_t)rows * (size_t)cols;

    remove(path);
    builder = lv_tsfile_schema_builder_new(table);
    CHECK_TRUE(builder != 0);
    for (int col = 0; col < cols; ++col) {
        char name[16];
        snprintf(name, sizeof(name), "c%d", col);
        CHECK_OK(lv_tsfile_schema_builder_add_column(builder, name, type,
                                                     LV_CAT_FIELD));
    }
    CHECK_OK(lv_tsfile_writer_open(path, builder, 0, &writer));
    lv_tsfile_schema_builder_free(builder);

    timestamps = (int64_t*)malloc((size_t)rows * sizeof(*timestamps));
    if (type == LV_TYPE_INT32) {
        values = malloc(cell_count * sizeof(int32_t));
    } else if (type == LV_TYPE_FLOAT) {
        values = malloc(cell_count * sizeof(float));
    } else {
        values = malloc(cell_count * sizeof(double));
    }
    CHECK_TRUE(timestamps != NULL && values != NULL);

    for (int row = 0; row < rows; ++row) {
        timestamps[row] = 1000 + row;
        for (int col = 0; col < cols; ++col) {
            size_t index = (size_t)row * (size_t)cols + (size_t)col;
            if (type == LV_TYPE_INT32) {
                ((int32_t*)values)[index] = row * 100 + col;
            } else if (type == LV_TYPE_FLOAT) {
                ((float*)values)[index] = (float)(row * 100 + col) + 0.25f;
            } else {
                ((double*)values)[index] = (double)(row * 100 + col) + 0.5;
            }
        }
    }

    if (type == LV_TYPE_INT32) {
        CHECK_OK(lv_tsfile_write_block_i32(writer, timestamps,
                                           (const int32_t*)values, rows, cols));
    } else if (type == LV_TYPE_FLOAT) {
        CHECK_OK(lv_tsfile_write_block_f32(writer, timestamps,
                                           (const float*)values, rows, cols));
    } else {
        CHECK_OK(lv_tsfile_write_block_f64(writer, timestamps,
                                           (const double*)values, rows, cols));
    }
    CHECK_OK(lv_tsfile_writer_close(writer));
    free(values);
    free(timestamps);
    return 0;
}

static int verify_i32(void) {
    static const char* path = "lv_read_block_i32.tsfile";
    LV_Handle reader = 0;
    LV_Handle rs = 0;
    int64_t timestamps[4];
    int32_t values[4 * 3];
    uint8_t nulls[4 * 3];
    int32_t out_rows = -1;

    CHECK_TRUE(create_file(path, "read_i32", LV_TYPE_INT32, 7, 3) == 0);
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_OK(lv_tsfile_query_table_batch(reader, "read_i32", "c0\nc1\nc2", 0,
                                         10000, 4, &rs));

    LV_Status row_err = 0;
    CHECK_TRUE(lv_tsfile_rs_next(rs, &row_err) == 0 && row_err != 0);

    out_rows = 99;
    CHECK_TRUE(lv_tsfile_rs_read_block_i32(rs, timestamps, values, nulls, 3, 3,
                                           &out_rows) != 0);
    CHECK_TRUE(out_rows == 0);

    int total = 0;
    for (;;) {
        CHECK_OK(lv_tsfile_rs_read_block_i32(rs, timestamps, values, nulls, 4,
                                             3, &out_rows));
        if (out_rows == 0) {
            break;
        }
        CHECK_TRUE(out_rows == (total == 0 ? 4 : 3));
        for (int row = 0; row < out_rows; ++row) {
            CHECK_TRUE(timestamps[row] == 1000 + total + row);
            for (int col = 0; col < 3; ++col) {
                int index = row * 3 + col;
                CHECK_TRUE(values[index] == (total + row) * 100 + col);
                CHECK_TRUE(nulls[index] == 0);
            }
        }
        total += out_rows;
    }
    CHECK_TRUE(total == 7);
    lv_tsfile_rs_free(rs);
    CHECK_OK(lv_tsfile_reader_close(reader));
    remove(path);
    return 0;
}

static int verify_f32(void) {
    static const char* path = "lv_read_block_f32.tsfile";
    LV_Handle reader = 0;
    LV_Handle rs = 0;
    int64_t timestamps[5];
    float values[5 * 2];
    int32_t out_rows = 0;

    CHECK_TRUE(create_file(path, "read_f32", LV_TYPE_FLOAT, 6, 2) == 0);
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_OK(lv_tsfile_query_table_batch(reader, "read_f32", "c0\nc1", 0, 10000,
                                         5, &rs));
    CHECK_OK(lv_tsfile_rs_read_block_f32(rs, timestamps, values, NULL, 5, 2,
                                         &out_rows));
    CHECK_TRUE(out_rows == 5);
    for (int row = 0; row < out_rows; ++row) {
        for (int col = 0; col < 2; ++col) {
            CHECK_TRUE(fabsf(values[row * 2 + col] -
                             ((float)(row * 100 + col) + 0.25f)) < 1e-6f);
        }
    }
    lv_tsfile_rs_free(rs);
    CHECK_OK(lv_tsfile_reader_close(reader));
    remove(path);
    return 0;
}

static int verify_f64_large(void) {
    static const char* path = "lv_read_block_f64.tsfile";
    enum { ROWS = 150005, COLS = 9, BATCH = 4096 };
    LV_Handle reader = 0;
    LV_Handle rs = 0;
    int64_t* timestamps = NULL;
    double* values = NULL;
    int32_t out_rows = 0;
    int total = 0;

    CHECK_TRUE(create_file(path, "read_f64", LV_TYPE_DOUBLE, ROWS, COLS) == 0);
    timestamps = (int64_t*)malloc(BATCH * sizeof(*timestamps));
    values = (double*)malloc((size_t)BATCH * COLS * sizeof(*values));
    CHECK_TRUE(timestamps != NULL && values != NULL);
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_OK(lv_tsfile_query_table_batch(reader, "read_f64",
                                         "c0\nc1\nc2\nc3\nc4\nc5\nc6\nc7\nc8",
                                         0, 200000, BATCH, &rs));

    for (;;) {
        CHECK_OK(lv_tsfile_rs_read_block_f64(rs, timestamps, values, NULL,
                                             BATCH, COLS, &out_rows));
        if (out_rows == 0) {
            break;
        }
        for (int row = 0; row < out_rows; ++row) {
            CHECK_TRUE(timestamps[row] == 1000 + total + row);
            for (int col = 0; col < COLS; ++col) {
                double expected = (double)((total + row) * 100 + col) + 0.5;
                CHECK_TRUE(fabs(values[row * COLS + col] - expected) < 1e-12);
            }
        }
        total += out_rows;
    }
    CHECK_TRUE(total == ROWS);
    free(values);
    free(timestamps);
    lv_tsfile_rs_free(rs);
    CHECK_OK(lv_tsfile_reader_close(reader));
    remove(path);
    return 0;
}

static int verify_errors(void) {
    static const char* path = "lv_read_block_errors.tsfile";
    LV_Handle reader = 0;
    LV_Handle row_rs = 0;
    LV_Handle batch_rs = 123;
    int64_t timestamps[4];
    double values[8];
    int32_t out_rows = 99;

    CHECK_TRUE(create_file(path, "read_errors", LV_TYPE_DOUBLE, 4, 2) == 0);
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_TRUE(lv_tsfile_query_table_batch(reader, "read_errors", "c0\nc1", 0,
                                           10000, 0, &batch_rs) != 0);
    CHECK_TRUE(batch_rs == 0);
    CHECK_OK(lv_tsfile_query_table(reader, "read_errors", "c0\nc1", 0, 10000,
                                   &row_rs));
    CHECK_TRUE(lv_tsfile_rs_read_block_f64(row_rs, timestamps, values, NULL, 4,
                                           2, &out_rows) != 0);
    CHECK_TRUE(out_rows == 0);
    lv_tsfile_rs_free(row_rs);

    CHECK_OK(lv_tsfile_query_table_batch(reader, "read_errors", "c0\nc1", 0,
                                         10000, 4, &batch_rs));
    out_rows = 99;
    CHECK_TRUE(lv_tsfile_rs_read_block_f64(batch_rs, timestamps, values, NULL,
                                           4, 1, &out_rows) != 0);
    CHECK_TRUE(out_rows == 0);
    out_rows = 99;
    CHECK_TRUE(lv_tsfile_rs_read_block_i32(batch_rs, timestamps,
                                           (int32_t*)values, NULL, 4, 2,
                                           &out_rows) != 0);
    CHECK_TRUE(out_rows == 0);
    lv_tsfile_rs_free(batch_rs);
    CHECK_OK(lv_tsfile_reader_close(reader));
    remove(path);
    return 0;
}

int main(void) {
    if (verify_i32() != 0 || verify_f32() != 0 || verify_f64_large() != 0 ||
        verify_errors() != 0) {
        return 1;
    }
    puts("LabVIEW numeric block read test passed");
    return 0;
}
