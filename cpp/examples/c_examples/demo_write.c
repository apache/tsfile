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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "c_examples.h"
/* 计时用：单调时钟，返回毫秒 */
static inline int64_t now_monotonic_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000LL + (int64_t)ts.tv_nsec / 1000000LL;
}

int64_t get_timestamp_int64(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (int64_t)ts.tv_sec * 1000000000LL
           + (int64_t)ts.tv_nsec;
}

static uint32_t g_rng_state = 0xA5A5A5A5u;

static inline void rng_seed(uint32_t seed) {
    /* 避免 0 种子导致退化 */
    g_rng_state = (seed == 0) ? 0xA5A5A5A5u : seed;
}

static inline uint32_t xorshift32_u32(void) {
    uint32_t x = g_rng_state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    g_rng_state = x;
    return x;
}

/* 返回 [center - range, center + range] 的均匀分布 int32 */
static inline int32_t rng_int32_around(int32_t center, int32_t range) {
    /* 2*range+1 个离散值：例如 center=25, range=5 -> [20..30] */
    uint32_t span = (uint32_t)(2 * range + 1);
    uint32_t r = xorshift32_u32();

    /* 为了避免取模偏差，用 rejection sampling */
    uint32_t limit = UINT32_MAX - (UINT32_MAX % span);
    while (r >= limit) {
        r = xorshift32_u32();
    }

    int32_t offset = (int32_t)(r % span) - range;
    return center + offset;
}

// This example shows you how to write tsfile.
ERRNO write_tsfile() {
    ERRNO code = 0;
    int64_t t_total_start = now_monotonic_ms();
    remove("test_c.tsfile");
    // Create a file with specify path to write tsfile.
    WriteFile file = write_file_new("test_c.tsfile", &code);
    HANDLE_ERROR(code);
    code = set_global_compression(TS_COMPRESSION_GZIP);
    if (code != RET_OK) {
        return code;
    }
    code = set_datatype_encoding(TS_DATATYPE_INT32, TS_ENCODING_TS_2DIFF);
    if (code != RET_OK) {
        return code;
    }
    char* table_name = "table1";

    // Create table schema to describe a table in a tsfile.
    TableSchema table_schema;
    table_schema.table_name = strdup(table_name);
    table_schema.column_num = 8;
    table_schema.column_schemas =
        (ColumnSchema*)malloc(sizeof(ColumnSchema) * 9);
    table_schema.column_schemas[0] =
        (ColumnSchema){.column_name = strdup("a1"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};
    table_schema.column_schemas[1] =
        (ColumnSchema){.column_name = strdup("a2"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};
    table_schema.column_schemas[2] =
        (ColumnSchema){.column_name = strdup("a3"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};
    table_schema.column_schemas[3] =
        (ColumnSchema){.column_name = strdup("a4"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};
    table_schema.column_schemas[4] =
        (ColumnSchema){.column_name = strdup("a5"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};
    table_schema.column_schemas[5] =
        (ColumnSchema){.column_name = strdup("a6"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};
    table_schema.column_schemas[6] =
        (ColumnSchema){.column_name = strdup("a7"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};
    table_schema.column_schemas[7] =
        (ColumnSchema){.column_name = strdup("a8"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};

    // Create tsfile writer with specify table schema.
    TsFileWriter writer = tsfile_writer_new(file, &table_schema, &code);
    HANDLE_ERROR(code);

    // Create tablet to insert data.
    int tablet_num = 1000;
    int max_len = 1000;
    Tablet tablet = tablet_new(
        (char*[]){"a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8"},
        (TSDataType[]){ TS_DATATYPE_INT32, TS_DATATYPE_INT32,
                       TS_DATATYPE_INT32, TS_DATATYPE_INT32, TS_DATATYPE_INT32,
                       TS_DATATYPE_INT32, TS_DATATYPE_INT32, TS_DATATYPE_INT32},
        8, max_len);
    printf("\ndebug %d.\n", 4);

    int64_t sum_fill_ms = 0;   /* tablet 填充耗时总和 */
    int64_t sum_write_ms = 0;  /* writer_write 耗时总和 */

    for (uint32_t tablet_idx = 0; tablet_idx < tablet_num; tablet_idx++) {
        int64_t t_fill_start = now_monotonic_ms();
        for (uint32_t row = 0; row < max_len; row++) {
            Timestamp timestamp = get_timestamp_int64();
            tablet_add_timestamp(tablet, row, timestamp);
            tablet_add_value_by_name_int32_t(tablet, row, "a1",
                                             rng_int32_around(25, 5));
            tablet_add_value_by_name_int32_t(tablet, row, "a2",
                                             rng_int32_around(25, 5));
            tablet_add_value_by_name_int32_t(tablet, row, "a3",
                                             rng_int32_around(25, 5));
            tablet_add_value_by_name_int32_t(tablet, row, "a4",
                                             rng_int32_around(25, 5));
            tablet_add_value_by_name_int32_t(tablet, row, "a5",
                                             rng_int32_around(25, 5));
            tablet_add_value_by_name_int32_t(tablet, row, "a6",
                                             rng_int32_around(25, 5));
            tablet_add_value_by_name_int32_t(tablet, row, "a7",
                                             rng_int32_around(25, 5));
            tablet_add_value_by_name_int32_t(tablet, row, "a8",
                                             rng_int32_around(25, 5));
        }
        int64_t t_fill_end = now_monotonic_ms();
        int64_t fill_ms = t_fill_end - t_fill_start;
        sum_fill_ms += fill_ms;


        int64_t t_write_start = now_monotonic_ms();
        HANDLE_ERROR(tsfile_writer_write(writer, tablet));
        int64_t t_write_end = now_monotonic_ms();

        int64_t write_ms = t_write_end - t_write_start;
        sum_write_ms += write_ms;
    }

    // Free tablet.
    free_tablet(&tablet);

    // Free table schema we used before.
    free_table_schema(table_schema);

    // Close writer.
    int64_t t_close_start = now_monotonic_ms();
    HANDLE_ERROR(tsfile_writer_close(writer));
    free_write_file(&file);
    int64_t t_close_end = now_monotonic_ms();
    int64_t close_ms = t_close_end - t_close_start;

    int64_t t_total_end = now_monotonic_ms();
    int64_t total_ms = t_total_end - t_total_start;

    printf("\n[PERF][SUMMARY]\n");
    printf("[PERF][SUMMARY] tablet_num=%d max_len=%d rows_total=%lld\n",
           tablet_num, max_len, (long long)tablet_num * (long long)max_len);
    printf("[PERF][SUMMARY] fill_total_ms=%lld avg_fill_ms=%.3f\n",
           (long long)sum_fill_ms, (double)sum_fill_ms / (double)tablet_num);
    printf("[PERF][SUMMARY] write_total_ms=%lld avg_write_ms=%.3f\n",
           (long long)sum_write_ms, (double)sum_write_ms / (double)tablet_num);
    printf("[PERF][SUMMARY] close_ms=%lld\n", (long long)close_ms);
    printf("[PERF][SUMMARY] total_ms=%lld\n\n", (long long)total_ms);

    return 0;
}