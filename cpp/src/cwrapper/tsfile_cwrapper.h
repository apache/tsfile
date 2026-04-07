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

#ifndef SRC_CWRAPPER_TSFILE_CWRAPPER_H_
#define SRC_CWRAPPER_TSFILE_CWRAPPER_H_
#ifdef __cplusplus

extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include <sys/stat.h>

typedef enum {
    TS_DATATYPE_BOOLEAN = 0,
    TS_DATATYPE_INT32 = 1,
    TS_DATATYPE_INT64 = 2,
    TS_DATATYPE_FLOAT = 3,
    TS_DATATYPE_DOUBLE = 4,
    TS_DATATYPE_TEXT = 5,
    TS_DATATYPE_VECTOR = 6,
    TS_DATATYPE_TIMESTAMP = 8,
    TS_DATATYPE_DATE = 9,
    TS_DATATYPE_BLOB = 10,
    TS_DATATYPE_STRING = 11,
    TS_DATATYPE_NULL_TYPE = 254,
    TS_DATATYPE_INVALID = 255
} TSDataType;

typedef enum {
    TS_ENCODING_PLAIN = 0,
    TS_ENCODING_DICTIONARY = 1,
    TS_ENCODING_RLE = 2,
    TS_ENCODING_DIFF = 3,
    TS_ENCODING_TS_2DIFF = 4,
    TS_ENCODING_BITMAP = 5,
    TS_ENCODING_GORILLA_V1 = 6,
    TS_ENCODING_REGULAR = 7,
    TS_ENCODING_GORILLA = 8,
    TS_ENCODING_ZIGZAG = 9,
    TS_ENCODING_FREQ = 10,
    TS_ENCODING_SPRINTZ = 12,
    TS_ENCODING_INVALID = 255
} TSEncoding;

typedef enum {
    TS_COMPRESSION_UNCOMPRESSED = 0,
    TS_COMPRESSION_SNAPPY = 1,
    TS_COMPRESSION_GZIP = 2,
    TS_COMPRESSION_LZO = 3,
    TS_COMPRESSION_SDT = 4,
    TS_COMPRESSION_PAA = 5,
    TS_COMPRESSION_PLA = 6,
    TS_COMPRESSION_LZ4 = 7,
    TS_COMPRESSION_INVALID = 255
} CompressionType;

typedef enum column_category {
    TAG = 0,
    FIELD = 1,
    ATTRIBUTE = 2,
    TIME = 3
} ColumnCategory;

typedef struct column_schema {
    char* column_name;
    TSDataType data_type;
    ColumnCategory column_category;
} ColumnSchema;

typedef struct table_schema {
    char* table_name;
    ColumnSchema* column_schemas;
    int column_num;
} TableSchema;

typedef struct timeseries_schema {
    char* timeseries_name;
    TSDataType data_type;
    TSEncoding encoding;
    CompressionType compression;
} TimeseriesSchema;

typedef struct device_schema {
    char* device_name;
    TimeseriesSchema* timeseries_schema;
    int timeseries_num;
} DeviceSchema;

/**
 * @brief Common header for all statistic variants (first member of each
 * TsFile*Statistic struct; also aliases the start of TimeseriesStatistic::u).
 *
 * When @p has_statistic is false, @p type is undefined. Otherwise @p type
 * selects which @ref TimeseriesStatisticUnion member is active (INT32/DATE/
 * INT64/TIMESTAMP share @c int_s). @c sum exists only on @c bool_s, @c int_s,
 * and @c float_s. Heap strings in string_s/text_s are
 * freed by tsfile_free_device_timeseries_metadata_map only.
 */
typedef struct TsFileStatisticBase {
    bool has_statistic;
    TSDataType type;
    int32_t row_count;
    int64_t start_time;
    int64_t end_time;
} TsFileStatisticBase;

typedef struct TsFileBoolStatistic {
    TsFileStatisticBase base;
    double sum;
    bool first_bool;
    bool last_bool;
} TsFileBoolStatistic;

typedef struct TsFileIntStatistic {
    TsFileStatisticBase base;
    double sum;
    int64_t min_int64;
    int64_t max_int64;
    int64_t first_int64;
    int64_t last_int64;
} TsFileIntStatistic;

typedef struct TsFileFloatStatistic {
    TsFileStatisticBase base;
    double sum;
    double min_float64;
    double max_float64;
    double first_float64;
    double last_float64;
} TsFileFloatStatistic;

typedef struct TsFileStringStatistic {
    TsFileStatisticBase base;
    char* str_min;
    char* str_max;
    char* str_first;
    char* str_last;
} TsFileStringStatistic;

typedef struct TsFileTextStatistic {
    TsFileStatisticBase base;
    char* str_first;
    char* str_last;
} TsFileTextStatistic;

/**
 * @brief One of the typed layouts; active member follows @c base.type.
 */
typedef union TimeseriesStatisticUnion {
    TsFileBoolStatistic bool_s;
    TsFileIntStatistic int_s;
    TsFileFloatStatistic float_s;
    TsFileStringStatistic string_s;
    TsFileTextStatistic text_s;
} TimeseriesStatisticUnion;

/**
 * @brief Aggregated statistic for one timeseries (subset of C++ Statistic).
 *
 * Read common fields via @c tsfile_statistic_base(s). Type-specific fields
 * via @c s->u.int_s, @c s->u.float_s, etc., per @c base.type.
 */
typedef struct TimeseriesStatistic {
    TimeseriesStatisticUnion u;
} TimeseriesStatistic;

/** Pointer to the common header at the start of @p s->u (any active arm). */
#define tsfile_statistic_base(s) ((TsFileStatisticBase*)&(s)->u)

/**
 * @brief One measurement's metadata as exposed to C.
 */
typedef struct TimeseriesMetadata {
    char* measurement_name;
    TSDataType data_type;
    int32_t chunk_meta_count;
    TimeseriesStatistic statistic;
} TimeseriesMetadata;

/**
 * @brief Device identity from IDeviceID (path, table name, segments).
 *
 * Heap fields are freed by tsfile_device_id_free_contents or
 * tsfile_free_device_id_array, or as part of
 * tsfile_free_device_timeseries_metadata_map for entries.
 */
typedef struct DeviceID {
    char* path;
    char* table_name;
    uint32_t segment_count;
    char** segments;
} DeviceID;

/**
 * @brief One device's timeseries metadata list plus DeviceID.
 *
 * @p device heap fields freed by tsfile_free_device_timeseries_metadata_map.
 */
typedef struct DeviceTimeseriesMetadataEntry {
    DeviceID device;
    TimeseriesMetadata* timeseries;
    uint32_t timeseries_count;
} DeviceTimeseriesMetadataEntry;

/**
 * @brief Map device -> list of TimeseriesMetadata (C layout with explicit
 * counts).
 */
typedef struct DeviceTimeseriesMetadataMap {
    DeviceTimeseriesMetadataEntry* entries;
    uint32_t device_count;
} DeviceTimeseriesMetadataMap;

/** Frees path, table_name, and segments inside @p d; zeros @p d. */
void tsfile_device_id_free_contents(DeviceID* d);

typedef struct result_set_meta_data {
    char** column_names;
    TSDataType* data_types;
    int column_num;
} ResultSetMetaData;

typedef struct tsfile_conf {
    int mem_threshold_kb;
} TsFileConf;

typedef void* WriteFile;

typedef void* TsFileReader;
typedef void* TsFileWriter;

// just reuse Tablet from c++
typedef void* Tablet;
typedef void* TsRecord;

typedef void* ResultSet;

typedef struct arrow_schema {
    // Array type description
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct arrow_schema** children;
    struct arrow_schema* dictionary;

    // Release callback
    void (*release)(struct arrow_schema*);
    // Opaque producer-specific data
    void* private_data;
} ArrowSchema;

typedef struct arrow_array {
    // Array data description
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct arrow_array** children;
    struct arrow_array* dictionary;

    // Release callback
    void (*release)(struct arrow_array*);
    // Opaque producer-specific data
    void* private_data;
} ArrowArray;

typedef int32_t ERRNO;
typedef int64_t Timestamp;

/**
 * @brief Get the encoding type for global time column
 *
 * @return uint8_t Time encoding type enum value (cast to uint8_t)
 */
uint8_t get_global_time_encoding();

/**
 * @brief Get the compression type for global time column
 *
 * @return uint8_t Time compression type enum value (cast to uint8_t)
 */
uint8_t get_global_time_compression();

/**
 * @brief Get the encoding type for specified data type
 *
 * @param data_type The data type to query encoding for
 * @return uint8_t Encoding type enum value (cast to uint8_t)
 */
uint8_t get_datatype_encoding(uint8_t data_type);

/**
 * @brief Get the global default compression type
 *
 * @return uint8_t Compression type enum value (cast to uint8_t)
 */
uint8_t get_global_compression();

/**
 * @brief Sets the global time column encoding method
 *
 * Validates and sets the encoding type for time series timestamps.
 * Supported encodings: TS_2DIFF, PLAIN, GORILLA, ZIGZAG, RLE, SPRINTZ
 *
 * @param encoding The encoding type to set (as uint8_t)
 * @return int E_OK on success, E_NOT_SUPPORT for invalid encoding
 */
int set_global_time_encoding(uint8_t encoding);

/**
 * @brief Sets the global time column compression method
 *
 * Validates and sets the compression type for time series timestamps.
 * Supported compressions: UNCOMPRESSED, SNAPPY, GZIP, LZO, LZ4
 *
 * @param compression The compression type to set (as uint8_t)
 * @return int E_OK on success, E_NOT_SUPPORT for invalid compression
 */
int set_global_time_compression(uint8_t compression);

/**
 * @brief Set encoding type for specific data type
 * @param data_type The data type to configure
 * @param encoding The encoding type to set
 * @return E_OK if success, E_NOT_SUPPORT if encoding is not supported for the
 * data type
 * @note Supported encodings per data type:
 *        - BOOLEAN: PLAIN only
 *        - INT32/INT64: PLAIN, TS_2DIFF, GORILLA, ZIGZAG, RLE, SPRINTZ
 *        - FLOAT/DOUBLE: PLAIN, TS_2DIFF, GORILLA, SPRINTZ
 *        - STRING: PLAIN, DICTIONARY
 */
int set_datatype_encoding(uint8_t data_type, uint8_t encoding);

/**
 * @brief Set the global default compression type
 * @param compression Compression type to set
 * @return E_OK if success, E_NOT_SUPPORT if compression is not supported
 * @note Supported compressions: UNCOMPRESSED, SNAPPY, GZIP, LZO, LZ4
 */
int set_global_compression(uint8_t compression);

/*--------------------------TsFile Reader and Writer------------------------ */

/**
 * @brief Creates a file for writing.
 *
 * @param pathname     Target file path to create.
 * @param err_code     [out] E_OK(0), or check error code in errno_define_c.h.
 *
 * @return WriteFile Valid handle on success.
 *
 * @note Call free_write_file() to release resources.
 * @note Before call free_write_file(), make sure TsFileWriter has been closed.
 */

WriteFile write_file_new(const char* pathname, ERRNO* err_code);

/**
 * @brief Creates a TsFileWriter for writing a TsFile.
 *
 * @param file     Target file where the table data will be written.
 * @param schema       Table schema definition.
 *                     - Ownership: Should be free it by Caller.
 * @param err_code     [out] E_OK(0), or check error code in errno_define_c.h.
 *
 * @return TsFileWriter Valid handle on success, NULL on failure.
 *
 * @note Call tsfile_writer_close() to release resources.
 */
TsFileWriter tsfile_writer_new(WriteFile file, TableSchema* schema,
                               ERRNO* err_code);

/**
 * @brief Creates a TsFileWriter for writing a TsFile.
 *
 * @param file     Target file where the table data will be written.
 * @param schema       Table schema definition.
 *                     - Ownership: Should be free it by Caller.
 * @param memory_threshold  When the size of written data exceeds
 * this value, the data will be automatically flushed to the disk.
 * @param err_code     [out] E_OK(0), or check error code in errno_define_c.h.
 *
 * @return TsFileWriter Valid handle on success, NULL on failure.
 *
 * @note Call tsfile_writer_close() to release resources.
 */
TsFileWriter tsfile_writer_new_with_memory_threshold(WriteFile file,
                                                     TableSchema* schema,
                                                     uint64_t memory_threshold,
                                                     ERRNO* err_code);

/**
 * @brief Creates a TsFileReader for reading a TsFile.
 *
 * @param pathname     Source TsFiles path. Must be a valid path.
 * @param err_code     E_OK(0), or check error code in errno_define_c.h.
 * @return TsFileReader Valid handle on success, NULL on failure.
 *
 * @note Call tsfile_reader_close() to release resources.
 */

TsFileReader tsfile_reader_new(const char* pathname, ERRNO* err_code);

/**
 * @brief Releases resources associated with a TsFileWriter.
 *
 * @param writer [in] Writer handle obtained from tsfile_writer_new().
 *                    After call: handle becomes invalid and must not be reused.
 * @return ERRNO - E_OK(0) on success, check error code in errno_define_c.h.
 */
ERRNO tsfile_writer_close(TsFileWriter writer);

/**
 * @brief Releases resources associated with a TsFileReader.
 *
 * @param reader [in] Reader handle obtained from tsfile_reader_new().
 *                    After call:
 *                      Handle becomes invalid and must not be reused.
 *                      Result_set obtained by this handle becomes invalid.
 * @return ERRNO - E_OK(0) on success, or check error code in errno_define_c.h.
 */
ERRNO tsfile_reader_close(TsFileReader reader);

/**
 * @brief Lists all devices (path, table name, segments from IDeviceID).
 *
 * @param out_devices [out] Allocated array; caller frees with
 * tsfile_free_device_id_array.
 */
ERRNO tsfile_reader_get_all_devices(TsFileReader reader, DeviceID** out_devices,
                                    uint32_t* out_length);

void tsfile_free_device_id_array(DeviceID* devices, uint32_t length);

/**
 * @brief Timeseries metadata for all devices in the file.
 */
ERRNO tsfile_reader_get_timeseries_metadata_all(
    TsFileReader reader, DeviceTimeseriesMetadataMap* out_map);

/**
 * @brief Timeseries metadata for a subset of devices.
 *
 * @param devices NULL and length>0 is E_INVALID_ARG. length==0: empty result
 * (E_OK); @p devices is not read.
 * For each entry, @p path must be non-NULL (canonical device path).
 */
ERRNO tsfile_reader_get_timeseries_metadata_for_devices(
    TsFileReader reader, const DeviceID* devices, uint32_t length,
    DeviceTimeseriesMetadataMap* out_map);

void tsfile_free_device_timeseries_metadata_map(
    DeviceTimeseriesMetadataMap* map);

/*--------------------------Tablet API------------------------ */

/**
 * @brief Creates a Tablet for batch data.
 *
 * @param column_name_list [in] Column names array. Size=column_num.
 * @param data_types [in] Data types array. Size=column_num.
 * @param column_num [in] Number of columns. Must be ≥1.
 * @param max_rows [in] Pre-allocated row capacity. Must be ≥1.
 * @return Tablet Valid handle.
 * @note Call free_tablet() to release resources.
 */
Tablet tablet_new(char** column_name_list, TSDataType* data_types,
                  uint32_t column_num, uint32_t max_rows);

/**
 * @brief Gets current row count in the Tablet.
 *
 * @param tablet [in] Valid Tablet handle.
 * @return uint32_t Row count (0 to max_rows-1).
 */
uint32_t tablet_get_cur_row_size(Tablet tablet);

/**
 * @brief Assigns timestamp to a row in the Tablet.
 *
 * @param tablet [in] Valid Tablet handle.
 * @param row_index [in] Target row (0 ≤ index < max_rows).
 * @param timestamp [in] Timestamp with int64_t type.
 * @return ERRNO - E_OK(0)/E_OUT_OF_RANGE(5) or check errno_define_c.h.
 */
ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           Timestamp timestamp);

/**
 * @brief Adds a value to a Tablet row by column name (generic types).
 *
 * @param tablet [in] Valid Tablet handle.
 * @param row_index [in] Target row (0 ≤ index < max_rows).
 * @param column_name [in] Existing column name from Tablet schema.
 * @param value [in] Value to add. Type must match column schema.
 * @return ERRNO - E_OK(0) or check errno_define_c.h.
 *
 * @note Generated for types: int32_t, int64_t, float, double, bool
 */
#define TABLET_ADD_VALUE_BY_NAME(type)                                       \
    ERRNO tablet_add_value_by_name_##type(Tablet tablet, uint32_t row_index, \
                                          const char* column_name,           \
                                          const type value);
TABLET_ADD_VALUE_BY_NAME(int32_t);
TABLET_ADD_VALUE_BY_NAME(int64_t);
TABLET_ADD_VALUE_BY_NAME(float);
TABLET_ADD_VALUE_BY_NAME(double);
TABLET_ADD_VALUE_BY_NAME(bool);

/**
 * @brief Adds a string value to a Tablet row by column name.
 *
 * @param value [in] Null-terminated string. Ownership remains with caller.
 * @return ERRNO.
 */
ERRNO tablet_add_value_by_name_string_with_len(Tablet tablet,
                                               uint32_t row_index,
                                               const char* column_name,
                                               const char* value,
                                               int value_len);

/**
 * @brief Adds a value to a Tablet row by column index (generic types).
 *
 * @param column_index [in] Column position (0 ≤ index < column_num).
 * @return ERRNO - E_OK(0) or check errno_define_c.h.
 *
 * @note Generated for types: int32_t, int64_t, float, double, bool
 */
#define TABLE_ADD_VALUE_BY_INDEX(type)                                        \
    ERRNO tablet_add_value_by_index_##type(Tablet tablet, uint32_t row_index, \
                                           uint32_t column_index,             \
                                           const type value);

TABLE_ADD_VALUE_BY_INDEX(int32_t);
TABLE_ADD_VALUE_BY_INDEX(int64_t);
TABLE_ADD_VALUE_BY_INDEX(float);
TABLE_ADD_VALUE_BY_INDEX(double);
TABLE_ADD_VALUE_BY_INDEX(bool);

/**
 * @brief Adds a string value to a Tablet row by column index.
 *
 * @param value [in] Null-terminated string. Copied internally.
 */
ERRNO tablet_add_value_by_index_string_with_len(Tablet tablet,
                                                uint32_t row_index,
                                                uint32_t column_index,
                                                const char* value,
                                                int value_len);

/*--------------------------TsRecord API------------------------ */
/*
TsRecord ts_record_new(const char* device_id, Timestamp timestamp,
                      int timeseries_num);

#define INSERT_DATA_INTO_TS_RECORD_BY_NAME(type)     \
   ERRNO insert_data_into_ts_record_by_name_##type( \
       TsRecord data, const char* measurement_name, type value);

INSERT_DATA_INTO_TS_RECORD_BY_NAME(int32_t);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(int64_t);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(bool);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(float);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(double);
*/

/*--------------------------TsFile Writer Register------------------------ */
/*
ERRNO tsfile_writer_register_table(TsFileWriter writer, TableSchema* schema);
ERRNO tsfile_writer_register_timeseries(TsFileWriter writer,
                                       const char* device_id,
                                       const TimeseriesSchema* schema);
ERRNO tsfile_writer_register_device(TsFileWriter writer,
                                   const DeviceSchema* device_schema);
                                   */

/*-------------------TsFile Writer write data------------------ */

/**
 * @brief Writes data from a Tablet to the TsFile.
 *
 * @param writer [in] Valid TsFileWriter handle. Must be initialized.
 * @param tablet [in] Tablet containing data. Should be freed after successful
 * write.
 * @return ERRNO - E_OK(0), or check error code in errno_define_c.h.
 *
 */

ERRNO tsfile_writer_write(TsFileWriter writer, Tablet tablet);
// ERRNO tsfile_writer_write_tablet(TsFileWriter writer, Tablet tablet);
// ERRNO tsfile_writer_write_ts_record(TsFileWriter writer, TsRecord record);
// ERRNO tsfile_writer_flush_data(TsFileWriter writer);

/*-------------------TsFile reader query data------------------ */

/**
 * @brief Queries time series data from a specific table within time range.
 *
 * @param reader [in] Valid TsFileReader handle from tsfile_reader_new().
 * @param table_name [in] Target table name. Must exist in the TS file.
 * @param columns [in] Array of column names to fetch.
 * @param column_num [in] Number of columns in array.
 * @param start_time [in] Start timestamp.
 * @param end_time [in] End timestamp. Must ≥ start_time.
 * @return ResultSet Query results handle. Must be freed with
 * free_tsfile_result_set().
 */
ResultSet tsfile_query_table(TsFileReader reader, const char* table_name,
                             char** columns, uint32_t column_num,
                             Timestamp start_time, Timestamp end_time,
                             ERRNO* err_code);

ResultSet tsfile_query_table_on_tree(TsFileReader reader, char** columns,
                                     uint32_t column_num, Timestamp start_time,
                                     Timestamp end_time, ERRNO* err_code);
/**
 * @brief Query time series (tree model) by row with offset/limit.
 *
 * For tree model, each (device_id, measurement_name) pair maps to a full path
 * "device_id.measurement_name". The result set merges multiple paths by
 * timestamp, applies the global offset/limit at merge layer, and returns
 * at most @p limit rows. < 0 limit means unlimited.
 *
 * @param reader [in] Valid TsFileReader handle obtained from
 * tsfile_reader_new().
 * @param device_ids [in] Array of device identifiers.
 * @param device_ids_len [in] Device id count.
 * @param measurement_names [in] Array of measurement (sensor) names.
 * @param measurement_names_len [in] Measurement name count.
 * @param offset [in] Number of leading rows to skip (>= 0).
 * @param limit [in] Maximum rows to return. < 0 means unlimited.
 * @param err_code [out] Error code. E_OK(0) on success.
 * @return ResultSet handle on success; NULL on failure.
 */
ResultSet tsfile_reader_query_tree_by_row(TsFileReader reader,
                                          char** device_ids, int device_ids_len,
                                          char** measurement_names,
                                          int measurement_names_len, int offset,
                                          int limit, ERRNO* err_code);

/**
 * @brief Query table-model data by row with offset/limit pushdown.
 *
 * @param reader [in] Valid TsFileReader handle obtained from
 * tsfile_reader_new().
 * @param table_name [in] Target table name.
 * @param column_names [in] Array of requested column names.
 * @param column_names_len [in] Requested column count.
 * @param offset [in] Number of leading rows to skip (>= 0).
 * @param limit [in] Maximum rows to return. < 0 means unlimited.
 * @param err_code [out] Error code. E_OK(0) on success.
 * @return ResultSet handle on success; NULL on failure.
 */
ResultSet tsfile_reader_query_table_by_row(TsFileReader reader,
                                           const char* table_name,
                                           char** column_names,
                                           int column_names_len, int offset,
                                           int limit, ERRNO* err_code);

ResultSet tsfile_query_table_batch(TsFileReader reader, const char* table_name,
                                   char** columns, uint32_t column_num,
                                   Timestamp start_time, Timestamp end_time,
                                   int batch_size, ERRNO* err_code);
// ResultSet tsfile_reader_query_device(TsFileReader reader,
//                                      const char* device_name,
//                                      char** sensor_name, uint32_t
//                                      sensor_num, Timestamp start_time,
//                                      Timestamp end_time);

/**
 * @brief Check and fetch the next row in the ResultSet.
 *
 * @param result_set [in] Valid ResultSet handle.
 * @return bool - true: Row available, false: End of data or error.
 */
bool tsfile_result_set_next(ResultSet result_set, ERRNO* error_code);

/**
 * @brief Gets the next TsBlock from batch ResultSet and converts it to Arrow
 * format.
 *
 * @param result_set [in] Valid ResultSet handle from batch query
 * (tsfile_query_table_batch).
 * @param out_array [out] Pointer to ArrowArray pointer. Will be set to the
 * converted Arrow array.
 * @param out_schema [out] Pointer to ArrowSchema pointer. Will be set to the
 * converted Arrow schema.
 * @return ERRNO - E_OK(0) on success, E_NO_MORE_DATA if no more blocks, or
 * other error codes.
 * @note Caller should release ArrowArray and ArrowSchema by calling their
 * release callbacks when done.
 * @note This function should only be called on ResultSet obtained from
 * tsfile_query_table_batch with batch_size > 0.
 */
ERRNO tsfile_result_set_get_next_tsblock_as_arrow(ResultSet result_set,
                                                  ArrowArray* out_array,
                                                  ArrowSchema* out_schema);

/**
 * @brief Gets value from current row by column name (generic types).
 *
 * @param result_set [in] Valid ResultSet with active row (after next()=true).
 * @param column_name [in] Existing column name in result schema.
 * @return type-value, return type-specific value.
 * @note Generated for: bool, int32_t, int64_t, float, double
 */
#define TSFILE_RESULT_SET_GET_VALUE_BY_NAME(type)                         \
    type tsfile_result_set_get_value_by_name_##type(ResultSet result_set, \
                                                    const char* column_name)
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(bool);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(int32_t);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(int64_t);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(float);
TSFILE_RESULT_SET_GET_VALUE_BY_NAME(double);

/**
 * @brief Gets string value from current row by column name.
 *
 * @return char* - String pointer. Caller must free this ptr after usage.
 */
char* tsfile_result_set_get_value_by_name_string(ResultSet result_set,
                                                 const char* column_name);

/**
 * @brief Gets value from current row by column_index[0 <= column_index <<
 * column_num] (generic types).
 *
 * @param result_set [in] Valid ResultSet with active row (after next()=true).
 * @param column_name [in] Existing column index in result schema.
 * @return type-value, return type-specific value.
 * @note Generated for: bool, int32_t, int64_t, float, double
 */

#define TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(type)                         \
    type tsfile_result_set_get_value_by_index_##type(ResultSet result_set, \
                                                     uint32_t column_index);

TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(int32_t);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(int64_t);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(float);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(double);
TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(bool);

/**
 * @brief Gets string value from current row by column index.
 *
 * @return char* - String pointer. Caller must free this ptr after usage.
 */
char* tsfile_result_set_get_value_by_index_string(ResultSet result_set,
                                                  uint32_t column_index);

/**
 * @brief Checks if the current row's column value is NULL by column name.
 *
 * @param result_set [in] Valid ResultSet with active row (after next()=true).
 * @param column_name [in] Existing column name in result schema.
 * @return bool - true: Value is NULL or column not found, false: Valid value.
 */
bool tsfile_result_set_is_null_by_name(ResultSet result_set,
                                       const char* column_name);

/**
 * @brief Checks if the current row's column value is NULL by column index.
 *
 * @param column_index [in] Column position (0 ≤ index < result_column_count).
 * @return bool - true: Value is NULL or index out of range, false: Valid value.
 */
bool tsfile_result_set_is_null_by_index(ResultSet result_set,
                                        uint32_t column_index);

/*-------------------TsFile reader query metadata------------------ */

/**
 * @brief Retrieves metadata describing the ResultSet's schema.
 *
 * @param result_set [in] Valid ResultSet handle.
 * @return ResultSetMetaData Metadata handle. Caller should free the
 * ResultSetMataData after usage.
 * @note Before calling this func, check if result_set is NULL, which means
 * the query may be not correct.
 */
ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set);

/**
 * @brief Gets column name by index from metadata.
 *
 * @param column_index [in] Column position (0 ≤ index < column_num).
 * @return const char* Read-only string. NULL if index invalid.
 */
char* tsfile_result_set_metadata_get_column_name(ResultSetMetaData result_set,
                                                 uint32_t column_index);

/**
 * @brief Gets column data type by index from metadata.
 *
 * @return TSDataType Returns TS_DATATYPE_INVALID(255) if index invalid.
 */
TSDataType tsfile_result_set_metadata_get_data_type(
    ResultSetMetaData result_set, uint32_t column_index);

/**
 * @brief Gets total number of columns in the result schema.
 *
 * @return column num in result set metadata.
 */
int tsfile_result_set_metadata_get_column_num(ResultSetMetaData result_set);

// Desc table schema.
// DeviceSchema tsfile_reader_get_device_schema(TsFileReader reader,
//                                              const char* device_id);

/**
 * @brief Gets specific table's schema in the tsfile.
 *
 * @return TableSchema, contains table and column info.
 * @note Caller should call free_table_schema to free the tableschema.
 */
TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                           const char* table_name);
/**
 * @brief Gets all table schema in the tsfile.
 *
 * @return TableSchema, contains table and column info.
 * @note Caller should call free_table_schema and free to free the ptr.
 */
TableSchema* tsfile_reader_get_all_table_schemas(TsFileReader reader,
                                                 uint32_t* size);

/**
 * @brief Gets all timeseries schema in the tsfile.
 *
 * @return DeviceSchema list, contains timeseries info.
 * @note Caller should call free_device_schema and free to free the ptr.
 */
DeviceSchema* tsfile_reader_get_all_timeseries_schemas(TsFileReader reader,
                                                       uint32_t* size);

// Close and free resource.
void free_tablet(Tablet* tablet);
void free_tsfile_result_set(ResultSet* result_set);
void free_result_set_meta_data(ResultSetMetaData result_set_meta_data);
void free_device_schema(DeviceSchema schema);
void free_timeseries_schema(TimeseriesSchema schema);
void free_table_schema(TableSchema schema);
void free_column_schema(ColumnSchema schema);
void free_write_file(WriteFile* write_file);

// ---------- !For Python API! ----------

/** WARN! Temporary internal method/interface.
 *  Avoid use: No compatibility/existence guarantees. */

// Create a tsfile writer.
TsFileWriter _tsfile_writer_new(const char* pathname, uint64_t memory_threshold,
                                ERRNO* err_code);

// Create a tablet with name, data_type and max_rows.
Tablet _tablet_new_with_target_name(const char* device_id,
                                    char** column_name_list,
                                    TSDataType* data_types, int column_num,
                                    int max_rows);

// Register a table with given table schema.
ERRNO _tsfile_writer_register_table(TsFileWriter writer, TableSchema* schema);

// Register a timeseries with given timeseries schema.
ERRNO _tsfile_writer_register_timeseries(TsFileWriter writer,
                                         const char* device_id,
                                         const TimeseriesSchema* schema);

// Register a device with given device schema.
ERRNO _tsfile_writer_register_device(TsFileWriter writer,
                                     const DeviceSchema* device_schema);

// Create a row record.
TsRecord _ts_record_new(const char* device_id, Timestamp timestamp,
                        int timeseries_num);

// Insert data into row record.
#define INSERT_DATA_INTO_TS_RECORD_BY_NAME(type)      \
    ERRNO _insert_data_into_ts_record_by_name_##type( \
        TsRecord data, const char* measurement_name, type value);

INSERT_DATA_INTO_TS_RECORD_BY_NAME(int32_t);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(int64_t);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(bool);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(float);
INSERT_DATA_INTO_TS_RECORD_BY_NAME(double);

ERRNO _insert_data_into_ts_record_by_name_string_with_len(
    TsRecord data, const char* measurement_name, const char* value,
    const uint32_t value_len);

// Write a tablet into a device.
ERRNO _tsfile_writer_write_tablet(TsFileWriter writer, Tablet tablet);

// Write a tablet into a table.
ERRNO _tsfile_writer_write_table(TsFileWriter writer, Tablet tablet);

// Write Arrow C Data Interface batch into a table (Arrow -> Tablet -> write).
// time_col_index: index of the time column in the Arrow struct.
// Caller should determine the correct time_col_index before calling.
ERRNO _tsfile_writer_write_arrow_table(TsFileWriter writer,
                                       const char* table_name,
                                       ArrowArray* array, ArrowSchema* schema,
                                       int time_col_index);

// Write a row record into a device.
ERRNO _tsfile_writer_write_ts_record(TsFileWriter writer, TsRecord record);

// Close a TsFile writer, automatically flush data.
ERRNO _tsfile_writer_close(TsFileWriter writer);

// Flush Chunk into tsfile from current tsFileWriter
ERRNO _tsfile_writer_flush(TsFileWriter writer);

// Queries time-series data for a specific device within a given time range.
ResultSet _tsfile_reader_query_device(TsFileReader reader,
                                      const char* device_name,
                                      char** sensor_name, uint32_t sensor_num,
                                      Timestamp start_time, Timestamp end_time,
                                      ERRNO* err_code);

// Free row record.
void _free_tsfile_ts_record(TsRecord* record);

#ifdef __cplusplus
}
#endif
#endif  // SRC_CWRAPPER_TSFILE_CWRAPPER_H_
