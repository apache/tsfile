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
#include <string.h>

#include "tsfile_labview.h"

#define NROWS 4
#define NCOLS 3

#define CHECK_OK(expr)                                                      \
    do {                                                                    \
        LV_Status status_ = (expr);                                         \
        if (status_ != 0) {                                                 \
            fprintf(stderr, "FAIL: %s returned %d\n", #expr, (int)status_); \
            return 1;                                                       \
        }                                                                   \
    } while (0)

#define CHECK_REJECTED(expr)                                             \
    do {                                                                 \
        LV_Status status_ = (expr);                                      \
        if (status_ == 0) {                                              \
            fprintf(stderr, "FAIL: %s unexpectedly succeeded\n", #expr); \
            return 1;                                                    \
        }                                                                \
    } while (0)

static int open_writer(const char* path, const char* table, uint8_t data_type,
                       LV_Handle* writer) {
    static const char* names[NCOLS] = {"front_x", "front_y", "front_z"};
    LV_Handle builder = lv_tsfile_schema_builder_new(table);
    if (builder == 0) {
        fprintf(stderr, "FAIL: schema builder creation\n");
        return 1;
    }
    for (int col = 0; col < NCOLS; ++col) {
        CHECK_OK(lv_tsfile_schema_builder_add_column(builder, names[col],
                                                     data_type, LV_CAT_FIELD));
    }
    remove(path);
    CHECK_OK(lv_tsfile_writer_open(path, builder, 0, writer));
    lv_tsfile_schema_builder_free(builder);
    return 0;
}

static int verify_i32(const char* path, const int32_t* expected) {
    LV_Handle reader = 0;
    LV_Handle rs = 0;
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_OK(lv_tsfile_query_table(reader, "block_i32",
                                   "front_x\nfront_y\nfront_z", 0, 100, &rs));
    LV_Status err = 0;
    int row = 0;
    while (lv_tsfile_rs_next(rs, &err) == 1 && err == 0) {
        if (row >= NROWS || lv_tsfile_rs_get_i64(rs, 0) != 10 + row) {
            fprintf(stderr, "FAIL: i32 timestamp at row %d\n", row);
            return 1;
        }
        for (int col = 0; col < NCOLS; ++col) {
            if (lv_tsfile_rs_get_i32(rs, (uint32_t)(col + 1)) !=
                expected[row * NCOLS + col]) {
                fprintf(stderr, "FAIL: i32 value at row %d col %d\n", row, col);
                return 1;
            }
        }
        ++row;
    }
    lv_tsfile_rs_free(rs);
    CHECK_OK(lv_tsfile_reader_close(reader));
    if (err != 0 || row != NROWS) {
        fprintf(stderr, "FAIL: i32 row count=%d err=%d\n", row, (int)err);
        return 1;
    }
    return 0;
}

static int verify_f32(const char* path, const float* expected) {
    LV_Handle reader = 0;
    LV_Handle rs = 0;
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_OK(lv_tsfile_query_table(reader, "block_f32",
                                   "front_x\nfront_y\nfront_z", 0, 100, &rs));
    LV_Status err = 0;
    int row = 0;
    while (lv_tsfile_rs_next(rs, &err) == 1 && err == 0) {
        for (int col = 0; col < NCOLS; ++col) {
            float actual = lv_tsfile_rs_get_f32(rs, (uint32_t)(col + 1));
            if (fabsf(actual - expected[row * NCOLS + col]) > 1e-6f) {
                fprintf(stderr, "FAIL: f32 value at row %d col %d\n", row, col);
                return 1;
            }
        }
        ++row;
    }
    lv_tsfile_rs_free(rs);
    CHECK_OK(lv_tsfile_reader_close(reader));
    if (err != 0 || row != NROWS) {
        fprintf(stderr, "FAIL: f32 row count=%d err=%d\n", row, (int)err);
        return 1;
    }
    return 0;
}

static int verify_f64(const char* path, const double* expected) {
    LV_Handle reader = 0;
    LV_Handle rs = 0;
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_OK(lv_tsfile_query_table(reader, "block_f64",
                                   "front_x\nfront_y\nfront_z", 0, 100, &rs));
    LV_Status err = 0;
    int row = 0;
    while (lv_tsfile_rs_next(rs, &err) == 1 && err == 0) {
        for (int col = 0; col < NCOLS; ++col) {
            double actual = lv_tsfile_rs_get_f64(rs, (uint32_t)(col + 1));
            if (fabs(actual - expected[row * NCOLS + col]) > 1e-12) {
                fprintf(stderr, "FAIL: f64 value at row %d col %d\n", row, col);
                return 1;
            }
        }
        ++row;
    }
    lv_tsfile_rs_free(rs);
    CHECK_OK(lv_tsfile_reader_close(reader));
    if (err != 0 || row != NROWS) {
        fprintf(stderr, "FAIL: f64 row count=%d err=%d\n", row, (int)err);
        return 1;
    }
    return 0;
}

static int test_i32(void) {
    static const char* path = "lv_block_i32.tsfile";
    const int64_t ts[NROWS] = {10, 11, 12, 13};
    const int32_t values[NROWS * NCOLS] = {1,  2,  3,  11, 12, 13,
                                           21, 22, 23, 31, 32, 33};
    LV_Handle writer = 0;
    if (open_writer(path, "block_i32", LV_TYPE_INT32, &writer) != 0) {
        return 1;
    }
    CHECK_REJECTED(lv_tsfile_write_block_i32(writer, ts, NULL, NROWS, NCOLS));
    CHECK_REJECTED(
        lv_tsfile_write_block_i32(writer, ts, values, NROWS, NCOLS - 1));
    CHECK_OK(lv_tsfile_write_block_i32(writer, ts, values, NROWS, NCOLS));
    CHECK_OK(lv_tsfile_writer_close(writer));
    if (verify_i32(path, values) != 0) {
        return 1;
    }
    remove(path);
    return 0;
}

static int test_f32(void) {
    static const char* path = "lv_block_f32.tsfile";
    const int64_t ts[NROWS] = {10, 11, 12, 13};
    const float values[NROWS * NCOLS] = {1.25f,  2.25f,  3.25f,  11.25f,
                                         12.25f, 13.25f, 21.25f, 22.25f,
                                         23.25f, 31.25f, 32.25f, 33.25f};
    LV_Handle writer = 0;
    if (open_writer(path, "block_f32", LV_TYPE_FLOAT, &writer) != 0) {
        return 1;
    }
    CHECK_OK(lv_tsfile_write_block_f32(writer, ts, values, NROWS, NCOLS));
    CHECK_OK(lv_tsfile_writer_close(writer));
    if (verify_f32(path, values) != 0) {
        return 1;
    }
    remove(path);
    return 0;
}

static int test_f64(void) {
    static const char* path = "lv_block_f64.tsfile";
    const int64_t ts[NROWS] = {10, 11, 12, 13};
    const double values[NROWS * NCOLS] = {1.5,  2.5,  3.5,  11.5, 12.5, 13.5,
                                          21.5, 22.5, 23.5, 31.5, 32.5, 33.5};
    const int32_t wrong_type[NROWS * NCOLS] = {0};
    LV_Handle writer = 0;
    if (open_writer(path, "block_f64", LV_TYPE_DOUBLE, &writer) != 0) {
        return 1;
    }
    CHECK_REJECTED(
        lv_tsfile_write_block_i32(writer, ts, wrong_type, NROWS, NCOLS));
    CHECK_REJECTED(lv_tsfile_write_block_f64(writer, ts, values, 0, NCOLS));
    CHECK_OK(lv_tsfile_write_block_f64(writer, ts, values, 2, NCOLS));
    CHECK_OK(lv_tsfile_write_block_f64(writer, ts + 2, values + 2 * NCOLS, 2,
                                       NCOLS));
    CHECK_OK(lv_tsfile_writer_close(writer));
    if (verify_f64(path, values) != 0) {
        return 1;
    }
    remove(path);
    return 0;
}

static int test_f64_reuse_and_growth(void) {
    static const char* path = "lv_block_f64_reuse.tsfile";
    static const char* columns = "c0\nc1\nc2\nc3\nc4\nc5\nc6\nc7\nc8";
    enum { COLS = 9, FIRST_ROWS = 1024, SECOND_ROWS = 256, THIRD_ROWS = 2048 };
    const int row_counts[] = {FIRST_ROWS, SECOND_ROWS, THIRD_ROWS};
    const int total_rows = FIRST_ROWS + SECOND_ROWS + THIRD_ROWS;
    int64_t* timestamps =
        (int64_t*)malloc((size_t)THIRD_ROWS * sizeof(int64_t));
    double* values =
        (double*)malloc((size_t)THIRD_ROWS * COLS * sizeof(double));
    if (timestamps == NULL || values == NULL) {
        free(timestamps);
        free(values);
        return 1;
    }

    remove(path);
    LV_Handle builder = lv_tsfile_schema_builder_new("reuse_f64");
    if (builder == 0) {
        free(timestamps);
        free(values);
        return 1;
    }
    for (int col = 0; col < COLS; ++col) {
        char name[8];
        snprintf(name, sizeof(name), "c%d", col);
        CHECK_OK(lv_tsfile_schema_builder_add_column(
            builder, name, LV_TYPE_DOUBLE, LV_CAT_FIELD));
    }
    LV_Handle writer = 0;
    CHECK_OK(lv_tsfile_writer_open(path, builder, 0, &writer));
    lv_tsfile_schema_builder_free(builder);

    int global_row = 0;
    for (int batch = 0; batch < 3; ++batch) {
        for (int row = 0; row < row_counts[batch]; ++row) {
            timestamps[row] = 1000 + global_row + row;
            for (int col = 0; col < COLS; ++col) {
                values[row * COLS + col] =
                    (double)((global_row + row) * 100 + col) + 0.25;
            }
        }
        CHECK_OK(lv_tsfile_write_block_f64(writer, timestamps, values,
                                           row_counts[batch], COLS));
        global_row += row_counts[batch];
    }
    CHECK_OK(lv_tsfile_writer_close(writer));

    LV_Handle reader = 0;
    LV_Handle rs = 0;
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_OK(
        lv_tsfile_query_table(reader, "reuse_f64", columns, 0, 10000, &rs));
    LV_Status err = 0;
    int row = 0;
    while (lv_tsfile_rs_next(rs, &err) == 1 && err == 0) {
        if (row >= total_rows || lv_tsfile_rs_get_i64(rs, 0) != 1000 + row) {
            fprintf(stderr, "FAIL: reuse timestamp at row %d\n", row);
            return 1;
        }
        for (int col = 0; col < COLS; ++col) {
            double expected = (double)(row * 100 + col) + 0.25;
            double actual = lv_tsfile_rs_get_f64(rs, (uint32_t)(col + 1));
            if (fabs(actual - expected) > 1e-12) {
                fprintf(stderr, "FAIL: reuse value at row %d col %d\n", row,
                        col);
                return 1;
            }
        }
        ++row;
    }
    lv_tsfile_rs_free(rs);
    CHECK_OK(lv_tsfile_reader_close(reader));
    free(timestamps);
    free(values);
    remove(path);
    if (err != 0 || row != total_rows) {
        fprintf(stderr, "FAIL: reuse row count=%d err=%d\n", row, (int)err);
        return 1;
    }
    return 0;
}

static int test_file_f64(void) {
    static const char* path = "lv_file_f64.tsfile";
    const int64_t ts[NROWS] = {10, 11, 12, 13};
    const double values[NROWS * NCOLS] = {1.5,  2.5,  3.5,  11.5, 12.5, 13.5,
                                          21.5, 22.5, 23.5, 31.5, 32.5, 33.5};
    remove(path);
    CHECK_REJECTED(lv_tsfile_write_file_f64(
        path, "block_f64", "front_x\nfront_y", ts, values, NROWS, NCOLS));
    CHECK_OK(lv_tsfile_write_file_f64(path, "block_f64",
                                      "front_x\nfront_y\nfront_z", ts, values,
                                      NROWS, NCOLS));
    if (verify_f64(path, values) != 0) {
        return 1;
    }
    remove(path);
    return 0;
}

static int test_write_demo(void) {
    static const char* path = "lv_write_demo.tsfile";
    remove(path);
    CHECK_REJECTED(lv_tsfile_write_demo(path, 0));
    CHECK_OK(lv_tsfile_write_demo(path, NROWS));

    LV_Handle reader = 0;
    LV_Handle rs = 0;
    CHECK_OK(lv_tsfile_reader_open(path, &reader));
    CHECK_OK(
        lv_tsfile_query_table(reader, "demo", "device\ntemp\ncnt", 0, 10, &rs));
    LV_Status err = 0;
    int row = 0;
    while (lv_tsfile_rs_next(rs, &err) == 1 && err == 0) {
        char device[16] = {0};
        int32_t actual_len = 0;
        CHECK_OK(lv_tsfile_rs_get_str(rs, 1, device, (int32_t)sizeof(device),
                                      &actual_len));
        if (row >= NROWS || lv_tsfile_rs_get_i64(rs, 0) != row + 1 ||
            actual_len != 7 || strcmp(device, "sensorA") != 0 ||
            fabs(lv_tsfile_rs_get_f64(rs, 2) -
                 (20.0 + 0.5 * sin((double)row))) > 1e-12 ||
            lv_tsfile_rs_get_i32(rs, 3) != row * 10) {
            fprintf(stderr, "FAIL: demo data at row %d\n", row);
            return 1;
        }
        ++row;
    }
    lv_tsfile_rs_free(rs);
    CHECK_OK(lv_tsfile_reader_close(reader));
    if (err != 0 || row != NROWS) {
        fprintf(stderr, "FAIL: demo row count=%d err=%d\n", row, (int)err);
        return 1;
    }
    remove(path);
    return 0;
}

int main(void) {
    fprintf(stderr, "RUN test_i32\n");
    if (test_i32() != 0) {
        return 1;
    }
    fprintf(stderr, "RUN test_f32\n");
    if (test_f32() != 0) {
        return 1;
    }
    fprintf(stderr, "RUN test_f64\n");
    if (test_f64() != 0) {
        return 1;
    }
    fprintf(stderr, "RUN test_f64_reuse_and_growth\n");
    if (test_f64_reuse_and_growth() != 0) {
        return 1;
    }
    fprintf(stderr, "RUN test_file_f64\n");
    if (test_file_f64() != 0) {
        return 1;
    }
    fprintf(stderr, "RUN test_write_demo\n");
    if (test_write_demo() != 0) {
        return 1;
    }
    puts("LabVIEW block-write tests passed");
    return 0;
}
