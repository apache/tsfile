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

/*
 * Standalone smoke test for the LabVIEW shim. It writes a small table to a
 * .tsfile using only the lv_tsfile_* API, then reads it back and prints the
 * rows. Build/run instructions are in the header comment of CMakeLists.txt.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "tsfile_labview.h"

#define CHECK(expr)                                            \
    do {                                                       \
        LV_Status _s = (expr);                                 \
        if (_s != 0) {                                         \
            printf("FAIL: %s -> status %d\n", #expr, (int)_s); \
            return 1;                                          \
        }                                                      \
    } while (0)

static const char* kPath = "lv_smoke.tsfile";
static const char* kTable = "lv_table";
#define NROWS 5

static int do_write(void) {
    LV_Handle sb = lv_tsfile_schema_builder_new(kTable);
    CHECK(lv_tsfile_schema_builder_add_column(sb, "device", LV_TYPE_STRING,
                                              LV_CAT_TAG));
    CHECK(lv_tsfile_schema_builder_add_column(sb, "temp", LV_TYPE_DOUBLE,
                                              LV_CAT_FIELD));
    CHECK(lv_tsfile_schema_builder_add_column(sb, "cnt", LV_TYPE_INT32,
                                              LV_CAT_FIELD));

    remove(kPath);
    LV_Handle writer = 0;
    CHECK(lv_tsfile_writer_open(kPath, sb, 0, &writer));
    lv_tsfile_schema_builder_free(sb);

    LV_Handle tablet = lv_tsfile_tablet_new(NROWS);
    CHECK(lv_tsfile_tablet_add_column(tablet, "device", LV_TYPE_STRING));
    CHECK(lv_tsfile_tablet_add_column(tablet, "temp", LV_TYPE_DOUBLE));
    CHECK(lv_tsfile_tablet_add_column(tablet, "cnt", LV_TYPE_INT32));
    CHECK(lv_tsfile_tablet_finalize_columns(tablet));

    for (uint32_t row = 0; row < NROWS; ++row) {
        const char* dev = "sensorA";
        CHECK(lv_tsfile_tablet_set_timestamp(tablet, row, (int64_t)row));
        CHECK(lv_tsfile_tablet_set_str(tablet, row, 0, dev,
                                       (int32_t)strlen(dev)));
        CHECK(lv_tsfile_tablet_set_f64(tablet, row, 1, 20.0 + row * 0.5));
        CHECK(lv_tsfile_tablet_set_i32(tablet, row, 2, (int32_t)(row * 10)));
    }

    CHECK(lv_tsfile_writer_write(writer, tablet));
    lv_tsfile_tablet_free(tablet);
    CHECK(lv_tsfile_writer_close(writer));
    printf("write OK: %d rows -> %s\n", NROWS, kPath);
    return 0;
}

static int do_read(void) {
    LV_Handle reader = 0;
    CHECK(lv_tsfile_reader_open(kPath, &reader));

    LV_Handle rs = 0;
    CHECK(lv_tsfile_query_table(reader, kTable, "device\ntemp\ncnt", 0, 100,
                                &rs));

    int32_t ncols = lv_tsfile_rs_column_count(rs);
    printf("read: column_count = %d\n", ncols);
    for (int32_t c = 0; c < ncols; ++c) {
        printf("  col[%d] type=%u\n", c, lv_tsfile_rs_column_type(rs, c));
    }

    int rows = 0;
    LV_Status err = 0;
    while (lv_tsfile_rs_next(rs, &err) == 1 && err == 0) {
        int64_t ts = lv_tsfile_rs_get_i64(rs, 0);  // column 0 = timestamp
        printf("  ts=%lld", (long long)ts);
        for (int32_t c = 1; c < ncols; ++c) {
            if (lv_tsfile_rs_is_null(rs, c)) {
                printf(" | null");
                continue;
            }
            uint8_t t = lv_tsfile_rs_column_type(rs, c);
            switch (t) {
                case LV_TYPE_INT32:
                    printf(" | %d", lv_tsfile_rs_get_i32(rs, c));
                    break;
                case LV_TYPE_INT64:
                    printf(" | %lld", (long long)lv_tsfile_rs_get_i64(rs, c));
                    break;
                case LV_TYPE_FLOAT:
                    printf(" | %f", lv_tsfile_rs_get_f32(rs, c));
                    break;
                case LV_TYPE_DOUBLE:
                    printf(" | %lf", lv_tsfile_rs_get_f64(rs, c));
                    break;
                case LV_TYPE_BOOLEAN:
                    printf(" | %d", lv_tsfile_rs_get_bool(rs, c));
                    break;
                case LV_TYPE_STRING: {
                    char buf[256];
                    int32_t actual = 0;
                    lv_tsfile_rs_get_str(rs, c, buf, sizeof(buf), &actual);
                    printf(" | %s", buf);
                    break;
                }
                default:
                    printf(" | <type %u>", t);
                    break;
            }
        }
        printf("\n");
        ++rows;
    }
    printf("read OK: %d rows (err=%d)\n", rows, (int)err);

    lv_tsfile_rs_free(rs);
    CHECK(lv_tsfile_reader_close(reader));
    return 0;
}

int main(void) {
    setbuf(stdout, NULL);  // unbuffered, so output survives a crash
    if (do_write() != 0) {
        return 1;
    }
    if (do_read() != 0) {
        return 1;
    }
    printf("ALL OK\n");
    return 0;
}
