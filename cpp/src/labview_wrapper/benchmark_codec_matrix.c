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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Reproducible Windows-oriented benchmark for separating TsFile value
 * encoding, value compression, timestamp encoding/compression, and the
 * LabVIEW wrapper's block materialization cost.
 */

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#ifdef _WIN32
/* The TsFile C API names its opaque file handle WriteFile.  Rename the Win32
 * function declaration while including windows.h to avoid a C identifier
 * collision; this benchmark does not call the Win32 WriteFile function. */
#define WriteFile Win32WriteFile
#include <windows.h>
#undef WriteFile
#else
#include <time.h>
#endif

#include "cwrapper/tsfile_cwrapper.h"
#include "tsfile_labview.h"

typedef enum BenchmarkMode {
    MODE_WRAPPER,
    MODE_CORE,
} BenchmarkMode;

typedef struct PhaseTimes {
    double open_seconds;
    double prepare_seconds;
    double tablet_new_seconds;
    double tablet_fill_seconds;
    double writer_write_seconds;
    double tablet_free_seconds;
    double wrapper_block_seconds;
    double buffer_free_seconds;
    double flush_seconds;
    double close_seconds;
    double write_seconds;
    double full_seconds;
    double process_cpu_seconds;
    uint64_t process_write_bytes;
} PhaseTimes;

static double monotonic_seconds(void) {
#ifdef _WIN32
    LARGE_INTEGER counter;
    LARGE_INTEGER frequency;
    QueryPerformanceFrequency(&frequency);
    QueryPerformanceCounter(&counter);
    return (double)counter.QuadPart / (double)frequency.QuadPart;
#else
    struct timespec value;
    clock_gettime(CLOCK_MONOTONIC, &value);
    return (double)value.tv_sec + (double)value.tv_nsec / 1000000000.0;
#endif
}

static double process_cpu_seconds(void) {
#ifdef _WIN32
    FILETIME creation;
    FILETIME exit;
    FILETIME kernel;
    FILETIME user;
    ULARGE_INTEGER kernel_value;
    ULARGE_INTEGER user_value;
    if (!GetProcessTimes(GetCurrentProcess(), &creation, &exit, &kernel,
                         &user)) {
        return 0.0;
    }
    kernel_value.LowPart = kernel.dwLowDateTime;
    kernel_value.HighPart = kernel.dwHighDateTime;
    user_value.LowPart = user.dwLowDateTime;
    user_value.HighPart = user.dwHighDateTime;
    return (double)(kernel_value.QuadPart + user_value.QuadPart) / 10000000.0;
#else
    return (double)clock() / (double)CLOCKS_PER_SEC;
#endif
}

static uint64_t process_write_bytes(void) {
#ifdef _WIN32
    IO_COUNTERS counters;
    if (GetProcessIoCounters(GetCurrentProcess(), &counters)) {
        return (uint64_t)counters.WriteTransferCount;
    }
#endif
    return 0;
}

static int check_status(int status, const char* operation) {
    if (status != 0) {
        fprintf(stderr, "%s failed with status %d\n", operation, status);
        return 0;
    }
    return 1;
}

static int parse_mode(const char* text, BenchmarkMode* mode) {
    if (strcmp(text, "wrapper") == 0) {
        *mode = MODE_WRAPPER;
        return 1;
    }
    if (strcmp(text, "core") == 0) {
        *mode = MODE_CORE;
        return 1;
    }
    return 0;
}

static int parse_encoding(const char* text, uint8_t* encoding) {
    if (strcmp(text, "plain") == 0) {
        *encoding = TS_ENCODING_PLAIN;
        return 1;
    }
    if (strcmp(text, "ts2diff") == 0) {
        *encoding = TS_ENCODING_TS_2DIFF;
        return 1;
    }
    if (strcmp(text, "gorilla") == 0) {
        *encoding = TS_ENCODING_GORILLA;
        return 1;
    }
    return 0;
}

static int parse_compression(const char* text, uint8_t* compression) {
    if (strcmp(text, "uncompressed") == 0) {
        *compression = TS_COMPRESSION_UNCOMPRESSED;
        return 1;
    }
    if (strcmp(text, "lz4") == 0) {
        *compression = TS_COMPRESSION_LZ4;
        return 1;
    }
    return 0;
}

static void free_column_names(char** names, int32_t count) {
    int32_t i;
    if (names == NULL) {
        return;
    }
    for (i = 0; i < count; ++i) {
        free(names[i]);
    }
    free(names);
}

static char** make_column_names(int32_t count) {
    char** names = (char**)calloc((size_t)count, sizeof(char*));
    int32_t i;
    if (names == NULL) {
        return NULL;
    }
    for (i = 0; i < count; ++i) {
        names[i] = (char*)malloc(32);
        if (names[i] == NULL) {
            free_column_names(names, count);
            return NULL;
        }
        if (count == 1) {
            snprintf(names[i], 32, "channel");
        } else {
            snprintf(names[i], 32, "ch_%d", (int)i);
        }
    }
    return names;
}

static int configure_codecs(uint8_t value_encoding, uint8_t value_compression,
                            uint8_t time_encoding, uint8_t time_compression) {
    return check_status(
               set_datatype_encoding(TS_DATATYPE_DOUBLE, value_encoding),
               "set_datatype_encoding") &&
           check_status(set_global_compression(value_compression),
                        "set_global_compression") &&
           check_status(set_global_time_encoding(time_encoding),
                        "set_global_time_encoding") &&
           check_status(set_global_time_compression(time_compression),
                        "set_global_time_compression");
}

static LV_Handle wrapper_open(const char* output_path, char** column_names,
                              int32_t ncols) {
    LV_Handle builder = lv_tsfile_schema_builder_new("vib");
    LV_Handle writer = 0;
    int32_t col;
    if (builder == 0) {
        return 0;
    }
    for (col = 0; col < ncols; ++col) {
        if (!check_status(
                lv_tsfile_schema_builder_add_column(
                    builder, column_names[col], LV_TYPE_DOUBLE, LV_CAT_FIELD),
                "schema_builder_add_column")) {
            lv_tsfile_schema_builder_free(builder);
            return 0;
        }
    }
    if (!check_status(lv_tsfile_writer_open(output_path, builder, 0, &writer),
                      "writer_open")) {
        lv_tsfile_schema_builder_free(builder);
        return 0;
    }
    lv_tsfile_schema_builder_free(builder);
    return writer;
}

static TsFileWriter core_open(const char* output_path, char** column_names,
                              int32_t ncols, WriteFile* out_file) {
    ColumnSchema* columns = NULL;
    TableSchema schema;
    TsFileWriter writer = NULL;
    ERRNO status = 0;
    int32_t col;

    *out_file = write_file_new(output_path, &status);
    if (*out_file == NULL || status != 0) {
        fprintf(stderr, "write_file_new failed with status %d\n", status);
        return NULL;
    }
    columns = (ColumnSchema*)calloc((size_t)ncols, sizeof(ColumnSchema));
    if (columns == NULL) {
        free_write_file(out_file);
        return NULL;
    }
    for (col = 0; col < ncols; ++col) {
        columns[col].column_name = column_names[col];
        columns[col].data_type = TS_DATATYPE_DOUBLE;
        columns[col].column_category = FIELD;
    }
    schema.table_name = "vib";
    schema.column_schemas = columns;
    schema.column_num = ncols;
    writer = tsfile_writer_new(*out_file, &schema, &status);
    free(columns);
    if (writer == NULL || status != 0) {
        fprintf(stderr, "tsfile_writer_new failed with status %d\n", status);
        free_write_file(out_file);
        return NULL;
    }
    return writer;
}

int main(int argc, char** argv) {
    BenchmarkMode mode;
    const char* output_path;
    uint64_t total_value_bytes;
    uint64_t block_value_bytes;
    int32_t ncols;
    const char* raw_block_path;
    uint8_t value_encoding;
    uint8_t value_compression;
    uint8_t time_encoding;
    uint8_t time_compression;
    uint64_t values_per_block;
    int32_t rows_per_block;
    uint64_t block_count;
    uint64_t total_rows;
    double* source_values = NULL;
    FILE* source = NULL;
    char** column_names = NULL;
    TSDataType* column_types = NULL;
    PhaseTimes phases;
    LV_Handle wrapper_writer = 0;
    WriteFile core_file = NULL;
    TsFileWriter core_writer = NULL;
    double full_started;
    double write_started;
    double cpu_started;
    uint64_t io_started;
    uint64_t block;
    struct stat file_info;
    int exit_code = 1;

    memset(&phases, 0, sizeof(phases));
    if (argc != 11) {
        fprintf(stderr,
                "usage: %s <wrapper|core> <output.tsfile> "
                "<total-value-bytes> <block-value-bytes> <ncols> "
                "<raw-block.bin> <value-encoding> <value-compression> "
                "<time-encoding> <time-compression>\n",
                argv[0]);
        return 2;
    }
    if (!parse_mode(argv[1], &mode) ||
        !parse_encoding(argv[7], &value_encoding) ||
        !parse_compression(argv[8], &value_compression) ||
        !parse_encoding(argv[9], &time_encoding) ||
        !parse_compression(argv[10], &time_compression)) {
        fprintf(stderr, "invalid mode, encoding, or compression name\n");
        return 2;
    }
    output_path = argv[2];
    total_value_bytes = strtoull(argv[3], NULL, 10);
    block_value_bytes = strtoull(argv[4], NULL, 10);
    ncols = (int32_t)strtol(argv[5], NULL, 10);
    raw_block_path = argv[6];
    if (total_value_bytes == 0 || block_value_bytes == 0 || ncols <= 0 ||
        total_value_bytes % block_value_bytes != 0 ||
        block_value_bytes % sizeof(double) != 0) {
        fprintf(stderr, "invalid byte counts or column count\n");
        return 2;
    }
    values_per_block = block_value_bytes / sizeof(double);
    if (values_per_block % (uint64_t)ncols != 0 ||
        values_per_block / (uint64_t)ncols > INT32_MAX) {
        fprintf(stderr, "block size is not divisible into rows and columns\n");
        return 2;
    }
    rows_per_block = (int32_t)(values_per_block / (uint64_t)ncols);
    block_count = total_value_bytes / block_value_bytes;
    total_rows = (uint64_t)rows_per_block * block_count;
    if (total_rows > INT64_MAX) {
        fprintf(stderr, "timestamp range is too large\n");
        return 2;
    }

    source = fopen(raw_block_path, "rb");
    source_values = (double*)malloc((size_t)block_value_bytes);
    if (source == NULL || source_values == NULL ||
        fread(source_values, 1, (size_t)block_value_bytes, source) !=
            (size_t)block_value_bytes ||
        fgetc(source) != EOF) {
        fprintf(stderr, "could not read an exact raw source block\n");
        if (source != NULL) {
            fclose(source);
        }
        free(source_values);
        return 2;
    }
    fclose(source);
    source = NULL;

    column_names = make_column_names(ncols);
    column_types = (TSDataType*)malloc((size_t)ncols * sizeof(TSDataType));
    if (column_names == NULL || column_types == NULL) {
        fprintf(stderr, "column allocation failed\n");
        goto cleanup;
    }
    for (int32_t col = 0; col < ncols; ++col) {
        column_types[col] = TS_DATATYPE_DOUBLE;
    }
    if (!configure_codecs(value_encoding, value_compression, time_encoding,
                          time_compression)) {
        goto cleanup;
    }
    if (get_datatype_encoding(TS_DATATYPE_DOUBLE) != value_encoding ||
        get_global_compression() != value_compression ||
        get_global_time_encoding() != time_encoding ||
        get_global_time_compression() != time_compression) {
        fprintf(stderr, "effective codec configuration differs from request\n");
        goto cleanup;
    }

    remove(output_path);
    full_started = monotonic_seconds();
    {
        const double started = monotonic_seconds();
        if (mode == MODE_WRAPPER) {
            wrapper_writer = wrapper_open(output_path, column_names, ncols);
            if (wrapper_writer == 0) {
                goto cleanup;
            }
        } else {
            core_writer =
                core_open(output_path, column_names, ncols, &core_file);
            if (core_writer == NULL) {
                goto cleanup;
            }
        }
        phases.open_seconds = monotonic_seconds() - started;
    }

    write_started = monotonic_seconds();
    cpu_started = process_cpu_seconds();
    io_started = process_write_bytes();
    for (block = 0; block < block_count; ++block) {
        int64_t* timestamps = NULL;
        double* values = NULL;
        const int64_t base = (int64_t)(block * (uint64_t)rows_per_block);
        double started = monotonic_seconds();
        int32_t row;
        int32_t col;

        timestamps = (int64_t*)malloc((size_t)rows_per_block * sizeof(int64_t));
        values = (double*)malloc((size_t)block_value_bytes);
        if (timestamps == NULL || values == NULL) {
            free(timestamps);
            free(values);
            fprintf(stderr, "block buffer allocation failed\n");
            goto cleanup;
        }
        memcpy(values, source_values, (size_t)block_value_bytes);
        for (row = 0; row < rows_per_block; ++row) {
            timestamps[row] = base + row;
        }
        phases.prepare_seconds += monotonic_seconds() - started;

        if (mode == MODE_WRAPPER) {
            started = monotonic_seconds();
            if (!check_status(
                    lv_tsfile_write_block_f64(wrapper_writer, timestamps,
                                              values, rows_per_block, ncols),
                    "lv_tsfile_write_block_f64")) {
                free(timestamps);
                free(values);
                goto cleanup;
            }
            phases.wrapper_block_seconds += monotonic_seconds() - started;
        } else {
            Tablet tablet;
            started = monotonic_seconds();
            tablet = tablet_new(column_names, column_types, (uint32_t)ncols,
                                (uint32_t)rows_per_block);
            phases.tablet_new_seconds += monotonic_seconds() - started;
            if (tablet == NULL) {
                free(timestamps);
                free(values);
                fprintf(stderr, "tablet_new failed\n");
                goto cleanup;
            }

            started = monotonic_seconds();
            for (row = 0; row < rows_per_block; ++row) {
                const size_t offset = (size_t)row * (size_t)ncols;
                if (!check_status(tablet_add_timestamp(tablet, (uint32_t)row,
                                                       timestamps[row]),
                                  "tablet_add_timestamp")) {
                    free_tablet(&tablet);
                    free(timestamps);
                    free(values);
                    goto cleanup;
                }
                for (col = 0; col < ncols; ++col) {
                    if (!check_status(tablet_add_value_by_index_double(
                                          tablet, (uint32_t)row, (uint32_t)col,
                                          values[offset + (size_t)col]),
                                      "tablet_add_value_by_index_double")) {
                        free_tablet(&tablet);
                        free(timestamps);
                        free(values);
                        goto cleanup;
                    }
                }
            }
            phases.tablet_fill_seconds += monotonic_seconds() - started;

            started = monotonic_seconds();
            if (!check_status(tsfile_writer_write(core_writer, tablet),
                              "tsfile_writer_write")) {
                free_tablet(&tablet);
                free(timestamps);
                free(values);
                goto cleanup;
            }
            phases.writer_write_seconds += monotonic_seconds() - started;

            started = monotonic_seconds();
            free_tablet(&tablet);
            phases.tablet_free_seconds += monotonic_seconds() - started;
        }

        started = monotonic_seconds();
        free(timestamps);
        free(values);
        phases.buffer_free_seconds += monotonic_seconds() - started;
    }
    phases.process_cpu_seconds = process_cpu_seconds() - cpu_started;
    phases.process_write_bytes = process_write_bytes() - io_started;
    phases.write_seconds = monotonic_seconds() - write_started;

    {
        const double started = monotonic_seconds();
        if (mode == MODE_WRAPPER) {
            if (!check_status(lv_tsfile_writer_flush(wrapper_writer),
                              "lv_tsfile_writer_flush")) {
                goto cleanup;
            }
        } else if (!check_status(tsfile_writer_flush(core_writer),
                                 "tsfile_writer_flush")) {
            goto cleanup;
        }
        phases.flush_seconds = monotonic_seconds() - started;
    }

    {
        const double started = monotonic_seconds();
        if (mode == MODE_WRAPPER) {
            if (!check_status(lv_tsfile_writer_close(wrapper_writer),
                              "lv_tsfile_writer_close")) {
                wrapper_writer = 0;
                goto cleanup;
            }
            wrapper_writer = 0;
        } else {
            if (!check_status(tsfile_writer_close(core_writer),
                              "tsfile_writer_close")) {
                core_writer = NULL;
                goto cleanup;
            }
            core_writer = NULL;
            free_write_file(&core_file);
        }
        phases.close_seconds = monotonic_seconds() - started;
    }
    phases.full_seconds = monotonic_seconds() - full_started;

    if (stat(output_path, &file_info) != 0) {
        perror("stat");
        goto cleanup;
    }

    printf("MODE=%s\n", mode == MODE_WRAPPER ? "wrapper" : "core");
    printf("VALUE_ENCODING=%s\n", argv[7]);
    printf("VALUE_COMPRESSION=%s\n", argv[8]);
    printf("TIME_ENCODING=%s\n", argv[9]);
    printf("TIME_COMPRESSION=%s\n", argv[10]);
    printf("EFFECTIVE_VALUE_ENCODING=%u\n",
           (unsigned)get_datatype_encoding(TS_DATATYPE_DOUBLE));
    printf("EFFECTIVE_VALUE_COMPRESSION=%u\n",
           (unsigned)get_global_compression());
    printf("EFFECTIVE_TIME_ENCODING=%u\n",
           (unsigned)get_global_time_encoding());
    printf("EFFECTIVE_TIME_COMPRESSION=%u\n",
           (unsigned)get_global_time_compression());
    printf("TOTAL_VALUE_BYTES=%" PRIu64 "\n", total_value_bytes);
    printf("TOTAL_TIMESTAMP_BYTES=%" PRIu64 "\n", total_rows * sizeof(int64_t));
    printf("BLOCK_VALUE_BYTES=%" PRIu64 "\n", block_value_bytes);
    printf("ROWS_PER_BLOCK=%d\n", (int)rows_per_block);
    printf("COLS=%d\n", (int)ncols);
    printf("BLOCKS=%" PRIu64 "\n", block_count);
    printf("TOTAL_ROWS=%" PRIu64 "\n", total_rows);
    printf("TOTAL_POINTS=%" PRIu64 "\n", total_rows * (uint64_t)ncols);
    printf("OPEN_SECONDS=%.9f\n", phases.open_seconds);
    printf("PREPARE_SECONDS=%.9f\n", phases.prepare_seconds);
    printf("TABLET_NEW_SECONDS=%.9f\n", phases.tablet_new_seconds);
    printf("TABLET_FILL_SECONDS=%.9f\n", phases.tablet_fill_seconds);
    printf("WRITER_WRITE_SECONDS=%.9f\n", phases.writer_write_seconds);
    printf("TABLET_FREE_SECONDS=%.9f\n", phases.tablet_free_seconds);
    printf("WRAPPER_BLOCK_SECONDS=%.9f\n", phases.wrapper_block_seconds);
    printf("BUFFER_FREE_SECONDS=%.9f\n", phases.buffer_free_seconds);
    printf("WRITE_SECONDS=%.9f\n", phases.write_seconds);
    printf("FLUSH_SECONDS=%.9f\n", phases.flush_seconds);
    printf("CLOSE_SECONDS=%.9f\n", phases.close_seconds);
    printf("FULL_SECONDS=%.9f\n", phases.full_seconds);
    printf("PROCESS_CPU_SECONDS=%.9f\n", phases.process_cpu_seconds);
    printf("CPU_TO_WALL_RATIO=%.6f\n",
           phases.write_seconds > 0.0
               ? phases.process_cpu_seconds / phases.write_seconds
               : 0.0);
    printf("PROCESS_WRITE_BYTES=%" PRIu64 "\n", phases.process_write_bytes);
    printf("VALUE_BYTES_PER_SECOND=%.3f\n",
           (double)total_value_bytes / phases.write_seconds);
    printf("POINTS_PER_SECOND=%.3f\n",
           (double)(total_rows * (uint64_t)ncols) / phases.write_seconds);
    printf("OUTPUT_BYTES=%lld\n", (long long)file_info.st_size);
    printf("OUTPUT_TO_VALUE_RATIO=%.9f\n",
           (double)file_info.st_size / (double)total_value_bytes);
    printf("OUTPUT_TO_LOGICAL_INPUT_RATIO=%.9f\n",
           (double)file_info.st_size /
               (double)(total_value_bytes + total_rows * sizeof(int64_t)));
    exit_code = 0;

cleanup:
    if (wrapper_writer != 0) {
        lv_tsfile_writer_close(wrapper_writer);
    }
    if (core_writer != NULL) {
        tsfile_writer_close(core_writer);
    }
    if (core_file != NULL) {
        free_write_file(&core_file);
    }
    free(column_types);
    free_column_names(column_names, ncols > 0 ? ncols : 0);
    free(source_values);
    return exit_code;
}
