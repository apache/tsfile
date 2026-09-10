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
 * tsfile_labview.h - LabVIEW-friendly C shim over cpp/src/cwrapper.
 *
 * Design goals (driven by LabVIEW Call Library Function Node limits):
 *   - Every opaque handle is a plain uint64_t (LabVIEW: Unsigned 64-bit
 *     Integer / U64, including in 32-bit LabVIEW). 0 means invalid.
 *   - Every fallible call returns int32_t (0 = OK), reusing cwrapper ERRNO.
 *   - No struct with embedded pointers / variable arrays is ever passed.
 *   - No char** arguments: column lists are passed as a single newline
 *     ('\n') separated string and split inside the shim.
 *   - String outputs use caller-preallocated buffer + size + actual_len;
 *     the shim never hands a malloc'd pointer back to LabVIEW.
 */

#ifndef SRC_LABVIEW_WRAPPER_TSFILE_LABVIEW_H_
#define SRC_LABVIEW_WRAPPER_TSFILE_LABVIEW_H_

#include <stdint.h>

#ifdef _WIN32
#define LV_API __declspec(dllexport)
#else
#define LV_API
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t LV_Handle; /* opaque handle, 0 = invalid */
typedef int32_t LV_Status;  /* 0 = OK, otherwise cwrapper ERRNO */

/* Data type codes (mirror cwrapper TSDataType). */
#define LV_TYPE_BOOLEAN 0
#define LV_TYPE_INT32 1
#define LV_TYPE_INT64 2
#define LV_TYPE_FLOAT 3
#define LV_TYPE_DOUBLE 4
#define LV_TYPE_STRING 11

/* Column categories (mirror cwrapper ColumnCategory). */
#define LV_CAT_TAG 0
#define LV_CAT_FIELD 1
#define LV_CAT_ATTRIBUTE 2

/* ===================== global configuration ===================== */
LV_API LV_Status lv_tsfile_set_global_compression(uint8_t compression);
LV_API LV_Status lv_tsfile_set_datatype_encoding(uint8_t dtype, uint8_t enc);

/* ===================== schema builder ===================== */
/* Build a TableSchema (which contains a ColumnSchema* array) without ever
 * exposing the array to LabVIEW. */
LV_API LV_Handle lv_tsfile_schema_builder_new(const char* table_name);
LV_API LV_Status lv_tsfile_schema_builder_add_column(LV_Handle builder,
                                                     const char* name,
                                                     uint8_t data_type,
                                                     uint8_t category);
LV_API void lv_tsfile_schema_builder_free(LV_Handle builder);

/* ===================== writer ===================== */
LV_API LV_Status lv_tsfile_writer_open(const char* path,
                                       LV_Handle schema_builder,
                                       uint64_t mem_threshold_bytes,
                                       LV_Handle* out_writer);
LV_API LV_Status lv_tsfile_writer_write(LV_Handle writer, LV_Handle tablet);
/* Write one homogeneous numeric block in a single host-to-DLL call.
 * ts has nrows elements. data has nrows*ncols elements in row-major order:
 * data[row*ncols + col]. ncols and the value type must exactly match the
 * schema used to open writer. The temporary tablet is owned by this call. */
LV_API LV_Status lv_tsfile_write_block_i32(LV_Handle writer, const int64_t* ts,
                                           const int32_t* data, int32_t nrows,
                                           int32_t ncols);
LV_API LV_Status lv_tsfile_write_block_f32(LV_Handle writer, const int64_t* ts,
                                           const float* data, int32_t nrows,
                                           int32_t ncols);
LV_API LV_Status lv_tsfile_write_block_f64(LV_Handle writer, const int64_t* ts,
                                           const double* data, int32_t nrows,
                                           int32_t ncols);
LV_API LV_Status lv_tsfile_writer_close(LV_Handle writer);

/* ===================== tablet builder ===================== */
LV_API LV_Handle lv_tsfile_tablet_new(uint32_t max_rows);
LV_API LV_Status lv_tsfile_tablet_add_column(LV_Handle tablet, const char* name,
                                             uint8_t dtype);
/* Must be called once after all columns are added and before set_* calls. */
LV_API LV_Status lv_tsfile_tablet_finalize_columns(LV_Handle tablet);

LV_API LV_Status lv_tsfile_tablet_set_timestamp(LV_Handle tablet, uint32_t row,
                                                int64_t ts);
LV_API LV_Status lv_tsfile_tablet_set_i32(LV_Handle tablet, uint32_t row,
                                          uint32_t col, int32_t v);
LV_API LV_Status lv_tsfile_tablet_set_i64(LV_Handle tablet, uint32_t row,
                                          uint32_t col, int64_t v);
LV_API LV_Status lv_tsfile_tablet_set_f32(LV_Handle tablet, uint32_t row,
                                          uint32_t col, float v);
LV_API LV_Status lv_tsfile_tablet_set_f64(LV_Handle tablet, uint32_t row,
                                          uint32_t col, double v);
LV_API LV_Status lv_tsfile_tablet_set_bool(LV_Handle tablet, uint32_t row,
                                           uint32_t col, int32_t v);
LV_API LV_Status lv_tsfile_tablet_set_str(LV_Handle tablet, uint32_t row,
                                          uint32_t col, const char* v,
                                          int32_t len);
LV_API void lv_tsfile_tablet_free(LV_Handle tablet);

/* ===================== reader ===================== */
LV_API LV_Status lv_tsfile_reader_open(const char* path, LV_Handle* out_reader);
LV_API LV_Status lv_tsfile_reader_close(LV_Handle reader);

/* columns: newline ('\n') separated list, e.g. "id1\ns1\ns2". */
LV_API LV_Status lv_tsfile_query_table(LV_Handle reader, const char* table_name,
                                       const char* columns_newline_separated,
                                       int64_t start_time, int64_t end_time,
                                       LV_Handle* out_result_set);

/* ===================== result set ===================== */
/* Returns 1 if a row is available, 0 if end-of-data; *out_err carries ERRNO. */
LV_API int32_t lv_tsfile_rs_next(LV_Handle rs, LV_Status* out_err);
LV_API int32_t lv_tsfile_rs_column_count(LV_Handle rs);
/* Column data type code (mirror LV_TYPE_*); 255 on invalid index. */
LV_API uint8_t lv_tsfile_rs_column_type(LV_Handle rs, uint32_t col);
LV_API int32_t lv_tsfile_rs_is_null(LV_Handle rs, uint32_t col);
LV_API int32_t lv_tsfile_rs_get_i32(LV_Handle rs, uint32_t col);
LV_API int64_t lv_tsfile_rs_get_i64(LV_Handle rs, uint32_t col);
LV_API float lv_tsfile_rs_get_f32(LV_Handle rs, uint32_t col);
LV_API double lv_tsfile_rs_get_f64(LV_Handle rs, uint32_t col);
LV_API int32_t lv_tsfile_rs_get_bool(LV_Handle rs, uint32_t col);
/* String output into a caller-preallocated buffer. out_actual_len receives the
 * full string length (excluding NUL), even if it exceeds buf_size. */
LV_API LV_Status lv_tsfile_rs_get_str(LV_Handle rs, uint32_t col, char* out_buf,
                                      int32_t buf_size,
                                      int32_t* out_actual_len);
LV_API void lv_tsfile_rs_free(LV_Handle rs);

/* ===================== one-call convenience ===================== */
/* Create, write, and close a homogeneous DOUBLE TsFile in one call.
 * column_names_newline_separated contains exactly ncols non-empty names.
 * ts has nrows elements and data has nrows*ncols row-major elements.
 * This is convenient for a one-shot LabVIEW CLFN; use writer_open +
 * write_block_f64 + writer_close for a continuous acquisition stream. */
LV_API LV_Status lv_tsfile_write_file_f64(
    const char* tsfile_path, const char* table_name,
    const char* column_names_newline_separated, const int64_t* ts,
    const double* data, int32_t nrows, int32_t ncols);

/* Generate the small mixed-type file used by the bundled LabVIEW write demo.
 * Schema: device:STRING tag, temp:DOUBLE field, cnt:INT32 field. */
LV_API LV_Status lv_tsfile_write_demo(const char* tsfile_path, int32_t nrows);

/* All-in-one: open the .tsfile, auto-discover the first table and all of its
 * columns, read every row, and write the result as a comma-separated CSV file
 * (first row = column names; column 0 = timestamp). Returns 0 on success.
 *
 * This is the simplest possible entry point for hosts (e.g. LabVIEW) that do
 * not want to juggle 64-bit handles, pointer-by-reference outputs, or row
 * loops: the call takes two plain C strings and returns an int32 status. */
LV_API LV_Status lv_tsfile_dump_to_csv(const char* tsfile_path,
                                       const char* csv_out_path);

#ifdef __cplusplus
}
#endif

#endif  // SRC_LABVIEW_WRAPPER_TSFILE_LABVIEW_H_
