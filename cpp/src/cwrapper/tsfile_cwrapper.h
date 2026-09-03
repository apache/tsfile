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
    TS_ENCODING_CHIMP = 11,
    TS_ENCODING_SPRINTZ = 12,
    TS_ENCODING_RLBE = 13,
    TS_ENCODING_CAMEL = 14,
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
    TS_COMPRESSION_ZSTD = 8,
    TS_COMPRESSION_LZMA2 = 9,
    TS_COMPRESSION_INVALID = 255
} CompressionType;

/** Local-file read backend selected for subsequently opened readers. */
typedef enum {
    TSFILE_READ_BACKEND_AUTO = 0,
    TSFILE_READ_BACKEND_MMAP = 1,
    TSFILE_READ_BACKEND_PREAD = 2
} TsFileReadBackend;

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
    TimeseriesStatistic timeline_statistic;
    uint64_t value_metadata_offset;
    uint32_t value_metadata_length;
    uint64_t time_metadata_offset;
    uint32_t time_metadata_length;
    uint32_t time_chunk_meta_count;
    uint16_t layout;
    uint16_t locator_flags;
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

/**
 * @brief One file-level property with length-aware binary storage.
 *
 * @p key is allocated with one trailing NUL for convenience, while @p key_len
 * is authoritative and preserves embedded NUL bytes. @p is_null distinguishes
 * a null value from a non-null zero-length value.
 */
typedef struct TsFileProperty {
    char* key;
    uint32_t key_len;
    uint8_t* value;
    uint32_t value_len;
    bool is_null;
} TsFileProperty;

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
typedef void* TsFileGenericWriter;

// just reuse Tablet from c++
typedef void* Tablet;
typedef void* TsRecord;

typedef void* ResultSet;
typedef void* TagFilterHandle;
typedef void* PreparedSeriesHandle;

typedef struct TsFilePreparedLocator {
    uint64_t mapped_index_identity;
    uint32_t file_id;
    uint64_t file_size;
    uint64_t file_fingerprint;
    uint32_t locator_id;
    uint16_t layout;
    uint16_t flags;
    uint64_t value_metadata_offset;
    uint32_t value_metadata_length;
    uint64_t time_metadata_offset;
    uint32_t time_metadata_length;
} TsFilePreparedLocator;

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
 * @brief Select the backend used by subsequently opened local TsFile readers.
 *
 * AUTO prefers memory mapping and falls back to pread, MMAP requires memory
 * mapping, and PREAD preserves the traditional positioned-read path. Existing
 * readers are unaffected.
 *
 * @param backend One of TSFILE_READ_BACKEND_AUTO, TSFILE_READ_BACKEND_MMAP,
 * or TSFILE_READ_BACKEND_PREAD.
 * @return RET_OK on success, or RET_INVALID_ARG for any other value.
 */
ERRNO tsfile_set_file_read_backend(int32_t backend);

/** @return The backend configured for subsequently opened readers. */
TsFileReadBackend tsfile_get_file_read_backend(void);

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
 * Supported encodings: PLAIN, TS_2DIFF
 *
 * @param encoding The encoding type to set (as uint8_t)
 * @return int E_OK on success, E_NOT_SUPPORT for invalid encoding
 */
int set_global_time_encoding(uint8_t encoding);

/**
 * @brief Sets the global time column compression method
 *
 * Validates and sets the compression type for time series timestamps.
 * Supported compressions: UNCOMPRESSED, SNAPPY, GZIP, LZO, LZ4, ZSTD, LZMA2
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
 *        - INT32/DATE/INT64/TIMESTAMP: PLAIN, TS_2DIFF, GORILLA, ZIGZAG, RLE,
 *          SPRINTZ, CHIMP, RLBE
 *        - FLOAT: PLAIN, TS_2DIFF, GORILLA, SPRINTZ, CHIMP, RLBE
 *        - DOUBLE: PLAIN, TS_2DIFF, GORILLA, SPRINTZ, CHIMP, RLBE, CAMEL
 *        - STRING: PLAIN, DICTIONARY
 */
int set_datatype_encoding(uint8_t data_type, uint8_t encoding);

/**
 * @brief Set the global default compression type
 * @param compression Compression type to set
 * @return E_OK if success, E_NOT_SUPPORT if compression is not supported
 * @note Supported compressions: UNCOMPRESSED, SNAPPY, GZIP, LZO, LZ4, ZSTD,
 *       LZMA2
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
 * @brief Adds or replaces a file-level property while the table writer is open.
 *
 * The key and value are copied immediately. A NULL value with value_len == 0
 * represents a null property; a non-NULL value with value_len == 0 represents
 * an empty byte array.
 */
ERRNO tsfile_writer_add_tsfile_property(TsFileWriter writer, const char* key,
                                        uint32_t key_len, const uint8_t* value,
                                        uint32_t value_len);

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

/**
 * @brief Returns a heap-allocated array containing all file-level properties.
 *
 * Caller must release the result with tsfile_free_tsfile_properties().
 */
ERRNO tsfile_reader_get_tsfile_properties(TsFileReader reader,
                                          TsFileProperty** out_properties,
                                          uint32_t* out_length);

void tsfile_free_tsfile_properties(TsFileProperty* properties, uint32_t length);

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

/** Deserialize one exact Dataset Index locator into a reusable series. */
PreparedSeriesHandle tsfile_reader_prepare_series(
    TsFileReader reader, const TsFilePreparedLocator* locator, ERRNO* err_code);

/** Prepare an aligned value locator by sharing an existing parsed time index.
 */
PreparedSeriesHandle tsfile_reader_prepare_series_with_time_owner(
    TsFileReader reader, const TsFilePreparedLocator* locator,
    PreparedSeriesHandle aligned_time_owner, ERRNO* err_code);

/** Release a prepared handle. Existing result sets remain independently owned.
 */
void tsfile_prepared_series_free(PreparedSeriesHandle prepared);

/** Query a prepared series without traversing the TsFile footer index. */
ResultSet tsfile_reader_query_prepared(TsFileReader reader,
                                       PreparedSeriesHandle prepared,
                                       Timestamp start_time, Timestamp end_time,
                                       int offset, int limit, ERRNO* err_code);

/** Query multiple aligned prepared value columns sharing one time axis. */
ResultSet tsfile_reader_query_prepared_multi(
    TsFileReader reader, const PreparedSeriesHandle* prepared,
    uint32_t prepared_count, Timestamp start_time, Timestamp end_time,
    int offset, int limit, ERRNO* err_code);

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
 * @brief Query exact tree-model full paths within a time range.
 *
 * @param reader [in] Valid reader handle.
 * @param paths [in] Array of full paths such as root.device.measurement.
 * @param path_num [in] Number of paths; must be greater than zero.
 * @param start_time [in] Inclusive start timestamp.
 * @param end_time [in] Inclusive end timestamp.
 * @param err_code [out] Error code; must not be NULL.
 * @return ResultSet handle on success, or NULL on failure.
 */
ResultSet tsfile_reader_query_tree(TsFileReader reader, char** paths,
                                   uint32_t path_num, Timestamp start_time,
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
ResultSet tsfile_reader_query_table_by_row(
    TsFileReader reader, const char* table_name, char** column_names,
    int column_names_len, int offset, int limit, TagFilterHandle tag_filter,
    int batch_size, ERRNO* err_code);

ResultSet tsfile_query_table_batch(TsFileReader reader, const char* table_name,
                                   char** columns, uint32_t column_num,
                                   Timestamp start_time, Timestamp end_time,
                                   TagFilterHandle tag_filter, int batch_size,
                                   ERRNO* err_code);
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
 * @brief Gets a value from the current row by 1-based column index.
 *
 * @param result_set [in] Valid ResultSet with active row (after next()=true).
 * @param column_index [in] Existing column index in [1, column_num].
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
 * @param column_index [in] Column position in [1, result_column_count].
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
 * @param column_index [in] Column position in [1, column_num].
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

// ---------- Tag Filter API ----------

/**
 * @brief Tag filter comparison operators.
 */
typedef enum {
    TAG_FILTER_EQ = 0,
    TAG_FILTER_NEQ = 1,
    TAG_FILTER_LT = 2,
    TAG_FILTER_LTEQ = 3,
    TAG_FILTER_GT = 4,
    TAG_FILTER_GTEQ = 5,
    TAG_FILTER_REGEXP = 6,
    TAG_FILTER_NOT_REGEXP = 7,
    TAG_FILTER_IS_NULL = 8,
    TAG_FILTER_IS_NOT_NULL = 9,
} TagFilterOp;

/**
 * @brief Create a tag filter with a comparison operator.
 *
 * @param reader [in] TsFileReader handle (used to resolve column name to
 * index).
 * @param table_name [in] Table name whose schema defines the TAG columns.
 * @param column_name [in] Name of the TAG column to filter on.
 * @param value [in] Comparison value (string). Ignored for
 * TAG_FILTER_IS_NULL / TAG_FILTER_IS_NOT_NULL (may be NULL).
 * @param op [in] Comparison operator (TagFilterOp).
 * @param err_code [out] Error code. E_OK(0) on success.
 * @return TagFilterHandle on success; NULL on failure.
 */
TagFilterHandle tsfile_tag_filter_create(TsFileReader reader,
                                         const char* table_name,
                                         const char* column_name,
                                         const char* value, TagFilterOp op,
                                         ERRNO* err_code);

/**
 * @brief Create a BETWEEN tag filter (lower <= column <= upper).
 */
TagFilterHandle tsfile_tag_filter_between(TsFileReader reader,
                                          const char* table_name,
                                          const char* column_name,
                                          const char* lower, const char* upper,
                                          bool is_not, ERRNO* err_code);

/**
 * @brief Create a tag equality filter: column == value.
 *
 * @param reader [in] Valid TsFileReader handle (used to resolve column index).
 * @param table_name [in] Target table name.
 * @param column_name [in] Tag column name.
 * @param value [in] Value to compare against.
 * @return TagFilterHandle on success, NULL on failure.
 */
TagFilterHandle tsfile_tag_filter_eq(TsFileReader reader,
                                     const char* table_name,
                                     const char* column_name,
                                     const char* value);

TagFilterHandle tsfile_tag_filter_neq(TsFileReader reader,
                                      const char* table_name,
                                      const char* column_name,
                                      const char* value);

TagFilterHandle tsfile_tag_filter_lt(TsFileReader reader,
                                     const char* table_name,
                                     const char* column_name,
                                     const char* value);

TagFilterHandle tsfile_tag_filter_lteq(TsFileReader reader,
                                       const char* table_name,
                                       const char* column_name,
                                       const char* value);

TagFilterHandle tsfile_tag_filter_gt(TsFileReader reader,
                                     const char* table_name,
                                     const char* column_name,
                                     const char* value);

TagFilterHandle tsfile_tag_filter_gteq(TsFileReader reader,
                                       const char* table_name,
                                       const char* column_name,
                                       const char* value);

/**
 * @brief Logical AND of two tag filters. Takes ownership of left and right.
 */
TagFilterHandle tsfile_tag_filter_and(TagFilterHandle left,
                                      TagFilterHandle right);

/**
 * @brief Logical OR of two tag filters. Takes ownership of left and right.
 */
TagFilterHandle tsfile_tag_filter_or(TagFilterHandle left,
                                     TagFilterHandle right);

/**
 * @brief Logical NOT of a tag filter. Takes ownership of filter.
 */
TagFilterHandle tsfile_tag_filter_not(TagFilterHandle filter);

/**
 * @brief Free a tag filter handle.
 */
void tsfile_tag_filter_free(TagFilterHandle filter);

/**
 * @brief Batch query with tag filter support.
 */
ResultSet tsfile_query_table_with_tag_filter(
    TsFileReader reader, const char* table_name, char** columns,
    uint32_t column_num, Timestamp start_time, Timestamp end_time,
    TagFilterHandle tag_filter, int batch_size, ERRNO* err_code);

// Close and free resource.
void free_tablet(Tablet* tablet);
void free_tsfile_result_set(ResultSet* result_set);
void free_result_set_meta_data(ResultSetMetaData result_set_meta_data);
void free_device_schema(DeviceSchema schema);
void free_timeseries_schema(TimeseriesSchema schema);
void free_table_schema(TableSchema schema);
void free_column_schema(ColumnSchema schema);
void free_write_file(WriteFile* write_file);

// ---------- Generic Writer API ----------
//
// Safety rules:
// - No C++ exception (e.g. std::bad_alloc) is allowed to cross this API:
//   allocation failures are reported as RET_OOM, and null/invalid
//   arguments as RET_INVALID_ARG (handle constructors additionally return
//   NULL).
// - tsfile_generic_writer_close(NULL) is a no-op returning RET_OK.
//
// Ownership and lifetime rules:
// - tsfile_generic_writer_new returns an owning handle. The caller owns the
//   returned handle until it is passed to tsfile_generic_writer_close, which
//   flushes, closes and deletes it.
// - Input schemas (TableSchema, TimeseriesSchema, DeviceSchema) and strings
//   (pathname, target_name, device_id, keys) are borrowed for the duration of
//   the call only; the writer does not retain them after the call returns.
// - Tablets are not consumed by the writer: after
//   tsfile_generic_writer_write_tree_tablet or
//   tsfile_generic_writer_write_table_tablet the tablet remains caller-owned
//   and must be freed by the caller (e.g. via free_tablet).

/**
 * @brief Create a new generic writer.
 * @param pathname file path of the TsFile to write; must not be NULL
 * @param memory_threshold in-memory buffer threshold in bytes
 * @param err_code out parameter receiving the error code; may be NULL, in
 *        which case the call fails silently with a NULL return
 * @return an owning handle to the new writer, or NULL on failure with
 *         *err_code set (unless err_code was NULL)
 */
TsFileGenericWriter tsfile_generic_writer_new(const char* pathname,
                                              uint64_t memory_threshold,
                                              ERRNO* err_code);

/**
 * @brief Create a tablet whose rows target the named table.
 * @param target_name name of the target table (borrowed); NULL selects the
 *        table-less constructor
 * @param column_name_list column names (borrowed); must not be NULL when
 *        column_num > 0, and no entry may be NULL
 * @param data_types data types of the columns (borrowed); must not be NULL
 *        when column_num > 0
 * @param column_num number of columns; must be >= 0
 * @param max_rows maximum number of rows the tablet can hold; must satisfy
 *        0 < max_rows < 2^30
 * @return a caller-owned tablet, or NULL if the arguments are invalid or
 *         memory allocation fails
 */
Tablet tablet_new_with_target_name(const char* target_name,
                                   char** column_name_list,
                                   TSDataType* data_types, int column_num,
                                   int max_rows);

/**
 * @brief Register a table schema with the writer. The schema is borrowed for
 *        the duration of the call.
 */
ERRNO tsfile_generic_writer_register_table(TsFileGenericWriter writer,
                                           TableSchema* schema);

/**
 * @brief Register a timeseries schema for a device with the writer. The
 *        device_id string and schema are borrowed for the duration of the
 *        call.
 */
ERRNO tsfile_generic_writer_register_timeseries(TsFileGenericWriter writer,
                                                const char* device_id,
                                                const TimeseriesSchema* schema);

/**
 * @brief Register a device schema with the writer. The device schema is
 *        borrowed for the duration of the call.
 */
ERRNO tsfile_generic_writer_register_device(TsFileGenericWriter writer,
                                            const DeviceSchema* device_schema);

/**
 * @brief Write a tree-model tablet. The tablet remains caller-owned after the
 *        call returns.
 */
ERRNO tsfile_generic_writer_write_tree_tablet(TsFileGenericWriter writer,
                                              Tablet tablet);

/**
 * @brief Write a table-model tablet. The tablet remains caller-owned after
 *        the call returns.
 */
ERRNO tsfile_generic_writer_write_table_tablet(TsFileGenericWriter writer,
                                               Tablet tablet);

/**
 * @brief Flush pending data to the file.
 */
ERRNO tsfile_generic_writer_flush(TsFileGenericWriter writer);

/**
 * @brief Add a key/value TsFile property to the writer. The key and value
 *        buffers are borrowed for the duration of the call.
 */
ERRNO tsfile_generic_writer_add_tsfile_property(TsFileGenericWriter writer,
                                                const char* key,
                                                uint32_t key_len,
                                                const uint8_t* value,
                                                uint32_t value_len);

/**
 * @brief Flush, close and delete the writer. After this call the handle must
 *        not be used.
 */
ERRNO tsfile_generic_writer_close(TsFileGenericWriter writer);

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

// Add or replace a file-level property on the generic writer used by Python.
ERRNO _tsfile_writer_add_tsfile_property(TsFileWriter writer, const char* key,
                                         uint32_t key_len, const uint8_t* value,
                                         uint32_t value_len);

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
