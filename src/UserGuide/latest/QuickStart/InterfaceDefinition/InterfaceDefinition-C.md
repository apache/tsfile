<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Interface definition - C

The C API is declared in `cwrapper/tsfile_cwrapper.h`. Success is `RET_OK` (0); other codes are defined in `cwrapper/errno_define_c.h`.

- **Table model**: one `TableSchema`, batch rows in a `Tablet`, queries by `table_name` and column names (time range, tag filter, row window, or batch TsBlock/Arrow).
- **Tree model**: queries by **device** id strings and **measurement** names; device listing and per-device timeseries metadata are documented under tree-oriented reads.

**Column index convention**: `tsfile_result_set_get_value_by_index_*`, `tsfile_result_set_is_null_by_index`, and `tsfile_result_set_metadata_get_*` use **1-based** `column_index` (1 … column count), matching the underlying C++ `ResultSet` and the shipped C examples.

## Schema and common types

Excerpt; see the header for the full list and additional metadata structures (`DeviceID`, `TimeseriesMetadata`, `DeviceTimeseriesMetadataMap`, statistics unions, etc.).

```C
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

typedef struct result_set_meta_data {
    char** column_names;
    TSDataType* data_types;
    int column_num;
} ResultSetMetaData;
```

## Table model: write

### TsFile `WriteFile` and `TsFileWriter`

```C
/**
 * @brief Creates a file handle for TsFile writing.
 *
 * @param pathname Output file path.
 * @param[out] err_code `RET_OK` on success.
 * @return Valid `WriteFile` handle on success, otherwise NULL.
 */
WriteFile write_file_new(const char* pathname, ERRNO* err_code);
/**
 * @brief Frees `WriteFile` resources.
 *
 * @param[in,out] write_file Pointer to write-file handle.
 */
void free_write_file(WriteFile* write_file);

/**
 * @brief Creates a table-model TsFile writer.
 *
 * @param file Write file handle.
 * @param schema Table schema definition.
 * @param[out] err_code `RET_OK` on success.
 * @return Valid `TsFileWriter` on success, otherwise NULL.
 */
TsFileWriter tsfile_writer_new(WriteFile file, TableSchema* schema,
                               ERRNO* err_code);
/**
 * @brief Creates a writer with memory threshold.
 *
 * @param file Write file handle.
 * @param schema Table schema definition.
 * @param memory_threshold Threshold for buffered writes before flush.
 * @param[out] err_code `RET_OK` on success.
 * @return Valid `TsFileWriter` on success, otherwise NULL.
 */
TsFileWriter tsfile_writer_new_with_memory_threshold(
    WriteFile file, TableSchema* schema, uint64_t memory_threshold,
    ERRNO* err_code);

/**
 * @brief Closes a TsFile writer.
 *
 * @param writer Writer handle.
 * @return `RET_OK` on success.
 */
ERRNO tsfile_writer_close(TsFileWriter writer);
```

`memory_threshold` is passed through to the writer configuration (e.g. chunk group size threshold). Use a value appropriate for your write workload.

### `Tablet` (string columns use length)

```C
/**
 * @brief Creates a Tablet for batch writes.
 *
 * @param column_name_list Column names, length = `column_num`.
 * @param data_types Data types, length = `column_num`.
 * @param column_num Number of columns.
 * @param max_rows Tablet capacity in rows.
 * @return Valid `Tablet` handle.
 */
Tablet tablet_new(char** column_name_list, TSDataType* data_types,
                  uint32_t column_num, uint32_t max_rows);
/**
 * @brief Gets current number of rows in tablet.
 *
 * @param tablet Tablet handle.
 * @return Current row count.
 */
uint32_t tablet_get_cur_row_size(Tablet tablet);
/**
 * @brief Sets timestamp for a row.
 *
 * @param tablet Tablet handle.
 * @param row_index Target row index.
 * @param timestamp Timestamp value.
 * @return `RET_OK` on success.
 */
ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           Timestamp timestamp);

/* String: pass byte length (not necessarily null-terminated storage). */
/**
 * @brief Sets string value by column name.
 *
 * @param tablet Tablet handle.
 * @param row_index Target row index.
 * @param column_name Column name.
 * @param value String bytes.
 * @param value_len Byte length of `value`.
 * @return `RET_OK` on success.
 */
ERRNO tablet_add_value_by_name_string_with_len(
    Tablet tablet, uint32_t row_index, const char* column_name,
    const char* value, int value_len);
/**
 * @brief Sets string value by column index.
 *
 * @param tablet Tablet handle.
 * @param row_index Target row index.
 * @param column_index Target column index.
 * @param value String bytes.
 * @param value_len Byte length of `value`.
 * @return `RET_OK` on success.
 */
ERRNO tablet_add_value_by_index_string_with_len(
    Tablet tablet, uint32_t row_index, uint32_t column_index,
    const char* value, int value_len);

/* Other types: generated names tablet_add_value_by_name_<type>, etc. */
/**
 * @brief Writes one tablet to TsFile.
 *
 * @param writer Writer handle.
 * @param tablet Tablet handle.
 * @return `RET_OK` on success.
 */
ERRNO tsfile_writer_write(TsFileWriter writer, Tablet tablet);
/**
 * @brief Frees tablet resources.
 *
 * @param[in,out] tablet Tablet pointer.
 */
void free_tablet(Tablet* tablet);
```

## Table model: read (time range and row window)

```C
/**
 * @brief Queries table-model data by time range.
 *
 * @param reader Reader handle.
 * @param table_name Table name.
 * @param columns Selected columns.
 * @param column_num Number of selected columns.
 * @param start_time Start timestamp.
 * @param end_time End timestamp.
 * @param[out] err_code `RET_OK` on success.
 * @return ResultSet handle on success.
 */
ResultSet tsfile_query_table(
    TsFileReader reader, const char* table_name, char** columns,
    uint32_t column_num, Timestamp start_time, Timestamp end_time,
    ERRNO* err_code);

/**
 * @brief Queries table-model data with optional tag filter and batch mode.
 *
 * @param reader Reader handle.
 * @param table_name Table name.
 * @param columns Selected columns.
 * @param column_num Number of selected columns.
 * @param start_time Start timestamp.
 * @param end_time End timestamp.
 * @param tag_filter Optional tag filter.
 * @param batch_size <=0 row mode, >0 batch TsBlock mode.
 * @param[out] err_code `RET_OK` on success.
 * @return ResultSet handle on success.
 */
ResultSet tsfile_query_table_batch(
    TsFileReader reader, const char* table_name, char** columns,
    uint32_t column_num, Timestamp start_time, Timestamp end_time,
    TagFilterHandle tag_filter, int batch_size, ERRNO* err_code);

/**
 * @brief Queries table-model data by row window (offset/limit).
 *
 * @param reader Reader handle.
 * @param table_name Table name.
 * @param column_names Selected columns.
 * @param column_names_len Number of selected columns.
 * @param offset Rows to skip.
 * @param limit Max rows to return. <0 means unlimited.
 * @param tag_filter Optional tag filter.
 * @param batch_size <=0 row mode, >0 batch TsBlock mode.
 * @param[out] err_code `RET_OK` on success.
 * @return ResultSet handle on success.
 */
ResultSet tsfile_reader_query_table_by_row(
    TsFileReader reader, const char* table_name, char** column_names,
    int column_names_len, int offset, int limit, TagFilterHandle tag_filter,
    int batch_size, ERRNO* err_code);

/**
 * @brief Moves to next row in result set.
 *
 * @param result_set ResultSet handle.
 * @param[out] error_code `RET_OK` while iteration is valid.
 * @return `true` if next row exists.
 */
bool tsfile_result_set_next(ResultSet result_set, ERRNO* error_code);
/**
 * @brief Frees result set resources.
 *
 * @param[in,out] result_set ResultSet pointer.
 */
void free_tsfile_result_set(ResultSet* result_set);
```

For batch / Arrow output from a batch `ResultSet` (`batch_size` > 0 in the query that produced it):

```C
/**
 * @brief Gets next TsBlock from batch result and converts to Arrow.
 *
 * @param result_set Batch-mode ResultSet.
 * @param[out] out_array Output ArrowArray.
 * @param[out] out_schema Output ArrowSchema.
 * @return `RET_OK` on success, no-more-data or error code otherwise.
 */
ERRNO tsfile_result_set_get_next_tsblock_as_arrow(
    ResultSet result_set, ArrowArray* out_array, ArrowSchema* out_schema);
```

Row iteration and `get_value_by_index` / `is_null_by_index` / metadata accessors are shared with the tree model; see [Result set and metadata](#result-set-and-metadata) below.

## Table model: tag filter

```C
typedef enum {
    TAG_FILTER_EQ = 0, TAG_FILTER_NEQ = 1, TAG_FILTER_LT = 2,
    TAG_FILTER_LTEQ = 3, TAG_FILTER_GT = 4, TAG_FILTER_GTEQ = 5,
    TAG_FILTER_REGEXP = 6, TAG_FILTER_NOT_REGEXP = 7,
} TagFilterOp;

/**
 * @brief Creates a simple comparison tag filter.
 *
 * @param reader Reader handle.
 * @param table_name Table name.
 * @param column_name TAG column name.
 * @param value Comparison value.
 * @param op Comparison operator.
 * @param[out] err_code `RET_OK` on success.
 * @return TagFilterHandle on success.
 */
TagFilterHandle tsfile_tag_filter_create(
    TsFileReader reader, const char* table_name, const char* column_name,
    const char* value, TagFilterOp op, ERRNO* err_code);
/**
 * @brief Creates a BETWEEN / NOT BETWEEN tag filter.
 *
 * @param reader Reader handle.
 * @param table_name Table name.
 * @param column_name TAG column name.
 * @param lower Lower bound.
 * @param upper Upper bound.
 * @param is_not `true` means NOT BETWEEN.
 * @param[out] err_code `RET_OK` on success.
 * @return TagFilterHandle on success.
 */
TagFilterHandle tsfile_tag_filter_between(
    TsFileReader reader, const char* table_name, const char* column_name,
    const char* lower, const char* upper, bool is_not, ERRNO* err_code);
/**
 * @brief Combines filters with AND.
 *
 * @param left Left filter.
 * @param right Right filter.
 * @return Combined filter handle.
 */
TagFilterHandle tsfile_tag_filter_and(TagFilterHandle left, TagFilterHandle right);
/**
 * @brief Combines filters with OR.
 *
 * @param left Left filter.
 * @param right Right filter.
 * @return Combined filter handle.
 */
TagFilterHandle tsfile_tag_filter_or(TagFilterHandle left, TagFilterHandle right);
/**
 * @brief Negates a filter.
 *
 * @param filter Input filter.
 * @return Negated filter handle.
 */
TagFilterHandle tsfile_tag_filter_not(TagFilterHandle filter);
/**
 * @brief Frees a tag filter tree.
 *
 * @param filter Filter handle.
 */
void tsfile_tag_filter_free(TagFilterHandle filter);

/**
 * @brief Queries table with explicit tag filter.
 *
 * @param reader Reader handle.
 * @param table_name Table name.
 * @param columns Selected columns.
 * @param column_num Number of selected columns.
 * @param start_time Start timestamp.
 * @param end_time End timestamp.
 * @param tag_filter Tag filter.
 * @param batch_size <=0 row mode, >0 batch TsBlock mode.
 * @param[out] err_code `RET_OK` on success.
 * @return ResultSet handle on success.
 */
ResultSet tsfile_query_table_with_tag_filter(
    TsFileReader reader, const char* table_name, char** columns,
    uint32_t column_num, Timestamp start_time, Timestamp end_time,
    TagFilterHandle tag_filter, int batch_size, ERRNO* err_code);
```

## Tree model: read

```C
/**
 * @brief Queries tree-model data by full path list in time range.
 *
 * @param reader Reader handle.
 * @param columns Full paths (`device.measurement`).
 * @param column_num Number of paths.
 * @param start_time Start timestamp.
 * @param end_time End timestamp.
 * @param[out] err_code `RET_OK` on success.
 * @return ResultSet handle on success.
 */
ResultSet tsfile_query_table_on_tree(
    TsFileReader reader, char** columns, uint32_t column_num,
    Timestamp start_time, Timestamp end_time, ERRNO* err_code);

/**
 * @brief Queries tree-model data by row with offset/limit.
 *
 * @param reader Reader handle.
 * @param device_ids Device id list.
 * @param device_ids_len Device id count.
 * @param measurement_names Measurement name list.
 * @param measurement_names_len Measurement count.
 * @param offset Rows to skip.
 * @param limit Max rows to return. <0 means unlimited.
 * @param[out] err_code `RET_OK` on success.
 * @return ResultSet handle on success.
 */
ResultSet tsfile_reader_query_tree_by_row(
    TsFileReader reader, char** device_ids, int device_ids_len,
    char** measurement_names, int measurement_names_len, int offset, int limit,
    ERRNO* err_code);
```

`tsfile_query_table_on_tree` uses full path strings in `columns` (e.g. `device.measurement`). `tsfile_reader_query_tree_by_row` passes parallel arrays of device ids and measurement names.

## Metadata and devices (tree-oriented)

```C
/**
 * @brief Gets all devices from current file.
 *
 * @param reader Reader handle.
 * @param[out] out_devices Output device array.
 * @param[out] out_length Output array length.
 * @return `RET_OK` on success.
 */
ERRNO tsfile_reader_get_all_devices(TsFileReader reader, DeviceID** out_devices,
                                    uint32_t* out_length);
/**
 * @brief Frees device array returned by `tsfile_reader_get_all_devices`.
 *
 * @param devices Device array.
 * @param length Device count.
 */
void tsfile_free_device_id_array(DeviceID* devices, uint32_t length);

/**
 * @brief Gets timeseries metadata for all devices.
 *
 * @param reader Reader handle.
 * @param[out] out_map Output metadata map.
 * @return `RET_OK` on success.
 */
ERRNO tsfile_reader_get_timeseries_metadata_all(
    TsFileReader reader, DeviceTimeseriesMetadataMap* out_map);
/**
 * @brief Gets timeseries metadata for selected devices.
 *
 * @param reader Reader handle.
 * @param devices Device array input.
 * @param length Device count.
 * @param[out] out_map Output metadata map.
 * @return `RET_OK` on success.
 */
ERRNO tsfile_reader_get_timeseries_metadata_for_devices(
    TsFileReader reader, const DeviceID* devices, uint32_t length,
    DeviceTimeseriesMetadataMap* out_map);
/**
 * @brief Frees metadata map allocated by metadata query APIs.
 *
 * @param[in,out] map Metadata map pointer.
 */
void tsfile_free_device_timeseries_metadata_map(
    DeviceTimeseriesMetadataMap* map);
```

## `TsFileReader` open/close and table schema

```C
/**
 * @brief Creates a TsFile reader.
 *
 * @param pathname Input `.tsfile` path.
 * @param[out] err_code `RET_OK` on success.
 * @return Reader handle on success.
 */
TsFileReader tsfile_reader_new(const char* pathname, ERRNO* err_code);
/**
 * @brief Closes reader and releases resources.
 *
 * @param reader Reader handle.
 * @return `RET_OK` on success.
 */
ERRNO tsfile_reader_close(TsFileReader reader);

/**
 * @brief Gets one table schema by table name.
 *
 * @param reader Reader handle.
 * @param table_name Table name.
 * @return TableSchema value.
 */
TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                           const char* table_name);
/**
 * @brief Gets all table schemas.
 *
 * @param reader Reader handle.
 * @param[out] size Output schema count.
 * @return Pointer to schema array.
 */
TableSchema* tsfile_reader_get_all_table_schemas(TsFileReader reader,
                                                uint32_t* size);
/**
 * @brief Gets all timeseries schemas grouped by device.
 *
 * @param reader Reader handle.
 * @param[out] size Output device-schema count.
 * @return Pointer to device schema array.
 */
DeviceSchema* tsfile_reader_get_all_timeseries_schemas(TsFileReader reader,
                                                       uint32_t* size);
/**
 * @brief Frees one table schema.
 *
 * @param schema Table schema value.
 */
void free_table_schema(TableSchema schema);
/**
 * @brief Frees one device schema.
 *
 * @param schema Device schema value.
 */
void free_device_schema(DeviceSchema schema);
/* plus free_column_schema, free_timeseries_schema as in the header */
```

## Result set and metadata

```C
/**
 * @brief Gets result-set metadata descriptor.
 *
 * @param result_set ResultSet handle.
 * @return Metadata descriptor.
 */
ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set);
/**
 * @brief Gets number of columns in metadata.
 *
 * @param result_set Metadata descriptor.
 * @return Column count.
 */
int tsfile_result_set_metadata_get_column_num(ResultSetMetaData result_set);
/**
 * @brief Gets column name by 1-based index.
 *
 * @param result_set Metadata descriptor.
 * @param column_index 1-based column index.
 * @return Column name pointer (read-only).
 */
char* tsfile_result_set_metadata_get_column_name(
    ResultSetMetaData result_set, uint32_t column_index);
/**
 * @brief Gets column type by 1-based index.
 *
 * @param result_set Metadata descriptor.
 * @param column_index 1-based column index.
 * @return TSDataType value.
 */
TSDataType tsfile_result_set_metadata_get_data_type(
    ResultSetMetaData result_set, uint32_t column_index);
/**
 * @brief Frees metadata resources.
 *
 * @param result_set_meta_data Metadata descriptor.
 */
void free_result_set_meta_data(ResultSetMetaData result_set_meta_data);

/* 1-based column_index for index-based getters */
#define TSFILE_RESULT_SET_GET_VALUE_BY_NAME(type) /* bool, int32_t, ... */
#define TSFILE_RESULT_SET_GET_VALUE_BY_INDEX(type)

/**
 * @brief Gets current-row string value by column name.
 *
 * @param result_set ResultSet handle.
 * @param column_name Column name.
 * @return Newly allocated string (caller frees).
 */
char* tsfile_result_set_get_value_by_name_string(ResultSet result_set,
                                                 const char* column_name);
/**
 * @brief Gets current-row string value by 1-based column index.
 *
 * @param result_set ResultSet handle.
 * @param column_index 1-based column index.
 * @return Newly allocated string (caller frees).
 */
char* tsfile_result_set_get_value_by_index_string(ResultSet result_set,
                                                  uint32_t column_index);

/**
 * @brief Checks whether current-row value is NULL by column name.
 *
 * @param result_set ResultSet handle.
 * @param column_name Column name.
 * @return `true` if NULL.
 */
bool tsfile_result_set_is_null_by_name(ResultSet result_set,
                                       const char* column_name);
/**
 * @brief Checks whether current-row value is NULL by 1-based column index.
 *
 * @param result_set ResultSet handle.
 * @param column_index 1-based column index.
 * @return `true` if NULL.
 */
bool tsfile_result_set_is_null_by_index(ResultSet result_set,
                                        uint32_t column_index);
```

## Global time / datatype encoding and compression (optional)

```C
/**
 * @brief Gets global time-column encoding.
 *
 * @return Encoding enum value.
 */
uint8_t get_global_time_encoding();
/**
 * @brief Gets global time-column compression.
 *
 * @return Compression enum value.
 */
uint8_t get_global_time_compression();
/**
 * @brief Gets default encoding for a data type.
 *
 * @param data_type Data type enum value.
 * @return Encoding enum value.
 */
uint8_t get_datatype_encoding(uint8_t data_type);
/**
 * @brief Gets global default compression.
 *
 * @return Compression enum value.
 */
uint8_t get_global_compression();
/**
 * @brief Sets global time-column encoding.
 *
 * @param encoding Encoding enum value.
 * @return `RET_OK` on success.
 */
int set_global_time_encoding(uint8_t encoding);
/**
 * @brief Sets global time-column compression.
 *
 * @param compression Compression enum value.
 * @return `RET_OK` on success.
 */
int set_global_time_compression(uint8_t compression);
/**
 * @brief Sets default encoding for one data type.
 *
 * @param data_type Data type enum value.
 * @param encoding Encoding enum value.
 * @return `RET_OK` on success.
 */
int set_datatype_encoding(uint8_t data_type, uint8_t encoding);
/**
 * @brief Sets global default compression.
 *
 * @param compression Compression enum value.
 * @return `RET_OK` on success.
 */
int set_global_compression(uint8_t compression);
```

See `tsfile_cwrapper.h` for supported encodings and compressions per type.
