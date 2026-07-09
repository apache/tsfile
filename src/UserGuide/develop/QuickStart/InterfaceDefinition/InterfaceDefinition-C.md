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



## Schema

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

// Value encoding.
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

// Compression type. LZ4 is the default.
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

// ColumnSchema: Represents the schema of a single column,
// including its name, data type, and category.
// On write, encoding/compression for columns follow the global defaults
// (see "Configuration" below).
typedef struct column_schema {
    char* column_name;
    TSDataType data_type;
    ColumnCategory column_category;
} ColumnSchema;

// TableSchema: Defines the schema of a table, 
// including its name and a list of column schemas.
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

// ResultSetMetaData: Contains metadata for a result set, 
// such as column names and their data types.
typedef struct result_set_meta_data {
    char** column_names;
    TSDataType* data_types;
    int column_num;
} ResultSetMetaData;

typedef struct arrow_schema ArrowSchema;
typedef struct arrow_array ArrowArray;

typedef struct DeviceID {
    char* path;
    char* table_name;
    uint32_t segment_count;
    char** segments;
} DeviceID;

typedef struct TsFileStatisticBase {
    bool has_statistic;
    TSDataType type;
    int32_t row_count;
    int64_t start_time;
    int64_t end_time;
} TsFileStatisticBase;

typedef struct TsFileBoolStatistic { TsFileStatisticBase base; double sum; bool first_bool; bool last_bool; } TsFileBoolStatistic;
typedef struct TsFileIntStatistic { TsFileStatisticBase base; double sum; int64_t min_int64; int64_t max_int64; int64_t first_int64; int64_t last_int64; } TsFileIntStatistic;
typedef struct TsFileFloatStatistic { TsFileStatisticBase base; double sum; double min_float64; double max_float64; double first_float64; double last_float64; } TsFileFloatStatistic;
typedef struct TsFileStringStatistic { TsFileStatisticBase base; char* str_min; char* str_max; char* str_first; char* str_last; } TsFileStringStatistic;
typedef struct TsFileTextStatistic { TsFileStatisticBase base; char* str_first; char* str_last; } TsFileTextStatistic;

typedef union TimeseriesStatisticUnion {
    TsFileBoolStatistic bool_s;
    TsFileIntStatistic int_s;
    TsFileFloatStatistic float_s;
    TsFileStringStatistic string_s;
    TsFileTextStatistic text_s;
} TimeseriesStatisticUnion;

typedef struct TimeseriesStatistic {
    TimeseriesStatisticUnion u;
} TimeseriesStatistic;

#define tsfile_statistic_base(s) ((TsFileStatisticBase*)&(s)->u)

typedef struct TimeseriesMetadata {
    char* measurement_name;
    TSDataType data_type;
    int32_t chunk_meta_count;
    TimeseriesStatistic statistic;
    TimeseriesStatistic timeline_statistic;
} TimeseriesMetadata;

typedef struct DeviceTimeseriesMetadataEntry {
    DeviceID device;
    TimeseriesMetadata* timeseries;
    uint32_t timeseries_count;
} DeviceTimeseriesMetadataEntry;

typedef struct DeviceTimeseriesMetadataMap {
    DeviceTimeseriesMetadataEntry* entries;
    uint32_t device_count;
} DeviceTimeseriesMetadataMap;
```

> `ColumnSchema` does not carry encoding/compression: on write, columns follow the
> global defaults (see [Configuration](#configuration-encoding--compression)); on
> read, each column is decoded with the file's actual settings.
>
> `TimeseriesStatistic` is a tagged union in `tsfile_cwrapper.h`. Read common
> fields through `tsfile_statistic_base(&metadata.statistic)` and then use the
> active typed member (`int_s`, `float_s`, `bool_s`, `string_s`, or `text_s`)
> according to the statistic data type.


## Write Interface

### TsFile WriteFile Create/Close

```C
/**
 * @brief Creates a file for writing.
 *
 * @param pathname     Target file to create.
 * @param err_code     [out] RET_OK(0), or error code in errno_define_c.h.
 *
 * @return WriteFile Valid handle on success.
 *
 * @note Call free_write_file() to release resources.
 * @note Before call free_write_file(), make sure TsFileWriter has been closed.
 */

WriteFile write_file_new(const char* pathname, ERRNO* err_code);

void free_write_file(WriteFile* write_file);
```

### TsFile Writer Create/Close

When creating a TsFile Writer, you specify a `WriteFile` and a `TableSchema`. As
you write, data is buffered in memory and automatically flushed to disk once the
buffered size exceeds `memory_threshold` bytes. `tsfile_writer_new` uses the
default (128 MB); `tsfile_writer_new_with_memory_threshold` lets you override it
— a larger value buffers more before flushing (more memory, larger chunk groups),
a smaller value flushes more often.

```C
/**
 * @brief Creates a TsFileWriter for writing a TsFile.
 *
 * @param file     Target file where the table data will be written.
 * @param schema       Table schema definition.
 *                     - Ownership: Should be freed by the caller.
 * @param err_code     [out] RET_OK(0), or error code in errno_define_c.h.
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
 *                     - Ownership: Should be freed by the caller.
 * @param memory_threshold When the size of written data exceeds
 * this value, the data will be automatically flushed to the disk. 
 * @param err_code     [out] RET_OK(0), or error code in errno_define_c.h.
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
 * @brief Releases resources associated with a TsFileWriter.
 *
 * @param writer [in] Writer handle obtained from tsfile_writer_new().
 *                    After call: handle becomes invalid and must not be reused.
 * @return ERRNO - RET_OK(0) on success, or error code in errno_define_c.h.
 */
ERRNO tsfile_writer_close(TsFileWriter writer);
```



### Tablet Create/Close/Insert data

You can use Tablet to insert data into TsFile in batches, and you need to release the space occupied by the Tablet after use.

```C
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
 * @return ERRNO - RET_OK(0) or error code in errno_define_c.h.
 */
ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           Timestamp timestamp);
                           
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

 // Supports multiple data types
ERRNO tablet_add_value_by_name_int32_t(Tablet tablet, uint32_t row_index,
                                      const char* column_name,
                                      int32_t value);
ERRNO tablet_add_value_by_name_int64_t(Tablet tablet, uint32_t row_index,
                                      const char* column_name,
                                      int64_t value);

ERRNO tablet_add_value_by_name_double(Tablet tablet, uint32_t row_index,
                                      const char* column_name,
                                      double value);

ERRNO tablet_add_value_by_name_float(Tablet tablet, uint32_t row_index,
                                      const char* column_name,
                                      float value);

ERRNO tablet_add_value_by_name_bool(Tablet tablet, uint32_t row_index,
                                      const char* column_name,
                                      bool value);


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


// Supports multiple data types
ERRNO tablet_add_value_by_index_int32_t(Tablet tablet, uint32_t row_index,
                                      uint32_t column_index,
                                      int32_t value);
ERRNO tablet_add_value_by_index_int64_t(Tablet tablet, uint32_t row_index,
                                      uint32_t column_index,
                                      int64_t value);

ERRNO tablet_add_value_by_index_double(Tablet tablet, uint32_t row_index,
                                      uint32_t column_index,
                                      double value);

ERRNO tablet_add_value_by_index_float(Tablet tablet, uint32_t row_index,
                                      uint32_t column_index,
                                      float value);

ERRNO tablet_add_value_by_index_bool(Tablet tablet, uint32_t row_index,
                                      uint32_t column_index,
                                      bool value);

                                       
void free_tablet(Tablet* tablet);
```



###  Write Tablet into TsFile

```C
/**
 * @brief Writes data from a Tablet to the TsFile.
 *
 * @param writer [in] Valid TsFileWriter handle.
 * @param tablet [in] Tablet containing data. Should be freed after successful
 * writing.
 * @return ERRNO - RET_OK(0), or error code in errno_define_c.h.
 *
 */

ERRNO tsfile_writer_write(TsFileWriter writer, Tablet tablet);
```





## Configuration (encoding & compression)

Columns are stored with the **global default** encoding and compression for their
data type (a `ColumnSchema` does not carry codec settings). Change those
defaults *before* creating a writer with the functions below.

Each setter returns `RET_OK` (0) on success, or `RET_NOT_SUPPORT` (40) for an
unsupported data-type/encoding or compression combination.

```C
/* Default value encoding per data type, and default compression. */
int     set_datatype_encoding(uint8_t data_type, uint8_t encoding);
int     set_global_compression(uint8_t compression);
uint8_t get_datatype_encoding(uint8_t data_type);
uint8_t get_global_compression();

/* Time column (the time data type is fixed to INT64). */
int     set_global_time_encoding(uint8_t encoding);
int     set_global_time_compression(uint8_t compression);
uint8_t get_global_time_encoding();
uint8_t get_global_time_compression();
```

Allowed encodings per data type, and the default used when you do not change it:

| Data type | Allowed encodings | Default |
|---|---|---|
| `BOOLEAN` | `PLAIN` | `PLAIN` |
| `INT32`, `INT64`, `DATE` | `PLAIN`, `TS_2DIFF`, `GORILLA`, `ZIGZAG`, `RLE`, `SPRINTZ` | `TS_2DIFF` |
| `FLOAT`, `DOUBLE` | `PLAIN`, `TS_2DIFF`, `GORILLA`, `SPRINTZ` | `GORILLA` |
| `STRING`, `TEXT` | `PLAIN`, `DICTIONARY` | `PLAIN` |

The time column uses the global time configuration and accepts `PLAIN`,
`TS_2DIFF`, `GORILLA`, `ZIGZAG`, `RLE`, or `SPRINTZ`.

Compression applies to any data type: `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZO`,
or `LZ4` (default `LZ4`).

```C
// e.g. write every column with LZ4 compression
ERRNO code = set_global_compression(TS_COMPRESSION_LZ4);
if (code != RET_OK) { /* handle unsupported value */ }
```

## Read  Interface

###  TsFile Reader Create/Close

```C
/**
 * @brief Creates a TsFileReader for reading a TsFile.
 *
 * @param pathname     Source TsFiles path. Must be a valid path.
 * @param err_code     RET_OK(0), or error code in errno_define_c.h.
 * @return TsFileReader Valid handle on success, NULL on failure.
 *
 * @note Call tsfile_reader_close() to release resources.
 */

TsFileReader tsfile_reader_new(const char* pathname, ERRNO* err_code);

/**
 * @brief Releases resources associated with a TsFileReader.
 *
 * @param reader [in] Reader handle obtained from tsfile_reader_new().
 *                    After call:
 *                      Handle becomes invalid and must not be reused.
 *                      Result_set obtained by this handle becomes invalid.
 * @return ERRNO - RET_OK(0) on success, or error code in errno_define_c.h.
 */
ERRNO tsfile_reader_close(TsFileReader reader);
```



###  Query table/get next

```C

/**
 * @brief Query data from the specific table and columns within time range.
 *
 * @param reader [in] Valid TsFileReader handle from tsfile_reader_new().
 * @param table_name [in] Target table name. Must exist in the TsFile.
 * @param columns [in] Array of column names to fetch.
 * @param column_num [in] Number of columns in array.
 * @param start_time [in] Start timestamp.
 * @param end_time [in] End timestamp. Must ≥ start_time.
 * @param err_code [out] RET_OK(0) on success, or error code in errno_define_c.h.
 * @return ResultSet Query results handle. Must be freed with
 * free_tsfile_result_set().
 */
ResultSet tsfile_query_table(TsFileReader reader, const char* table_name,
                             char** columns, uint32_t column_num,
                             Timestamp start_time, Timestamp end_time,
                             ERRNO* err_code);

/**
 * @brief Check and fetch the next row in the ResultSet.
 *
 * @param result_set [in] Valid ResultSet handle.
 * @param error_code RET_OK(0) on success, or error code in errno_define_c.h.
 * @return bool - true: Row available, false: End of data or error.
 */
bool tsfile_result_set_next(ResultSet result_set, ERRNO* error_code);

/**
 * @brief Free Result set 
 *
 * @param result_set [in] Valid ResultSet handle ptr.
 */
void free_tsfile_result_set(ResultSet* result_set);
```


### Tree queries

```C
/**
 * @brief Query tree-model data by measurement names within a time range.
 */
ResultSet tsfile_query_table_on_tree(TsFileReader reader, char** columns,
                                     uint32_t column_num, Timestamp start_time,
                                     Timestamp end_time, ERRNO* err_code);

/**
 * @brief Query tree-model data by row with offset/limit.
 *
 * @param device_ids Array of device identifiers.
 * @param measurement_names Array of measurement names.
 * @param offset Leading rows to skip (>= 0).
 * @param limit Max rows to return; < 0 means unlimited.
 */
ResultSet tsfile_reader_query_tree_by_row(TsFileReader reader,
                                          char** device_ids, int device_ids_len,
                                          char** measurement_names,
                                          int measurement_names_len, int offset,
                                          int limit, ERRNO* err_code);
```


### Filtering by tag

**TAG columns** form the device identity (a joint primary
key) — their values are what distinguish one device from another within a table.
A *tag filter* restricts a query to the devices whose TAG values match a
predicate, so you read only the devices you care about. Build a filter from the
reader, pass it to one of the table-query functions below, then release it with
`tsfile_tag_filter_free()`.

```C
// Opaque handle to a tag filter. Build it with the functions below.
typedef void* TagFilterHandle;

// Comparison operators for a single-column TAG predicate.
typedef enum {
    TAG_FILTER_EQ = 0,          // column == value
    TAG_FILTER_NEQ = 1,         // column != value
    TAG_FILTER_LT = 2,          // column <  value
    TAG_FILTER_LTEQ = 3,        // column <= value
    TAG_FILTER_GT = 4,          // column >  value
    TAG_FILTER_GTEQ = 5,        // column >= value
    TAG_FILTER_REGEXP = 6,      // column matches the regex value
    TAG_FILTER_NOT_REGEXP = 7,  // column does not match the regex value
    TAG_FILTER_IS_NULL = 8,     // column is null
    TAG_FILTER_IS_NOT_NULL = 9, // column is not null
} TagFilterOp;

/**
 * @brief Create a single-column TAG predicate: `<column_name> <op> <value>`.
 *
 * @param reader      [in] Valid TsFileReader handle.
 * @param table_name  [in] Table whose schema defines the TAG columns.
 * @param column_name [in] Name of the TAG column to filter on.
 * @param value       [in] Comparison value (ignored for IS NULL / IS NOT NULL).
 * @param op          [in] Comparison operator (TagFilterOp).
 * @param err_code    [out] RET_OK(0) on success, or error code in errno_define_c.h.
 * @return TagFilterHandle on success; NULL on failure.
 */
TagFilterHandle tsfile_tag_filter_create(TsFileReader reader,
                                         const char* table_name,
                                         const char* column_name,
                                         const char* value, TagFilterOp op,
                                         ERRNO* err_code);

/**
 * @brief Create a range predicate: lower <= column <= upper
 *        (pass is_not = true for NOT BETWEEN).
 */
TagFilterHandle tsfile_tag_filter_between(TsFileReader reader,
                                          const char* table_name,
                                          const char* column_name,
                                          const char* lower, const char* upper,
                                          bool is_not, ERRNO* err_code);

// Convenience builders for common single-column predicates.
TagFilterHandle tsfile_tag_filter_eq(TsFileReader reader, const char* table_name,
                                     const char* column_name, const char* value);
TagFilterHandle tsfile_tag_filter_neq(TsFileReader reader, const char* table_name,
                                      const char* column_name, const char* value);
TagFilterHandle tsfile_tag_filter_lt(TsFileReader reader, const char* table_name,
                                     const char* column_name, const char* value);
TagFilterHandle tsfile_tag_filter_lteq(TsFileReader reader, const char* table_name,
                                       const char* column_name, const char* value);
TagFilterHandle tsfile_tag_filter_gt(TsFileReader reader, const char* table_name,
                                     const char* column_name, const char* value);
TagFilterHandle tsfile_tag_filter_gteq(TsFileReader reader, const char* table_name,
                                       const char* column_name, const char* value);

// Combine predicates. AND/OR/NOT take ownership of their children; free the root only.
TagFilterHandle tsfile_tag_filter_and(TagFilterHandle left, TagFilterHandle right);
TagFilterHandle tsfile_tag_filter_or(TagFilterHandle left, TagFilterHandle right);
TagFilterHandle tsfile_tag_filter_not(TagFilterHandle filter);

// Free a tag filter and all of its children.
void tsfile_tag_filter_free(TagFilterHandle filter);
```

### Table queries with tag filter, paging and batching

These query functions accept an optional `tag_filter` (pass `NULL`
for no filtering) and a `batch_size` (`<= 0` returns rows one by one; `> 0`
returns a block of that size).

```C
/**
 * @brief Query a table by row, with offset/limit pushdown and an optional tag filter.
 *
 * @param reader           [in] Valid TsFileReader handle.
 * @param table_name       [in] Target table name.
 * @param column_names     [in] Requested column names.
 * @param column_names_len [in] Number of requested columns.
 * @param offset           [in] Leading rows to skip (>= 0).
 * @param limit            [in] Max rows to return; < 0 means unlimited.
 * @param tag_filter       [in] TAG predicate, or NULL for no filtering.
 * @param batch_size       [in] <= 0 row-by-row; > 0 block size.
 * @param err_code         [out] RET_OK(0) on success, or error code.
 * @return ResultSet handle; NULL on failure. Free with free_tsfile_result_set().
 */
ResultSet tsfile_reader_query_table_by_row(
    TsFileReader reader, const char* table_name, char** column_names,
    int column_names_len, int offset, int limit, TagFilterHandle tag_filter,
    int batch_size, ERRNO* err_code);

/**
 * @brief Query a table within a time range, with an optional tag filter and batching.
 *
 * @param batch_size <= 0 row-by-row return; > 0 returns a TsBlock of that size.
 */
ResultSet tsfile_query_table_batch(TsFileReader reader, const char* table_name,
                                   char** columns, uint32_t column_num,
                                   Timestamp start_time, Timestamp end_time,
                                   TagFilterHandle tag_filter, int batch_size,
                                   ERRNO* err_code);

/**
 * @brief Query a table with a tag filter (time range + TAG predicate).
 *
 * @param batch_size <= 0 row-by-row return; > 0 returns a TsBlock of that size.
 */
ResultSet tsfile_query_table_with_tag_filter(
    TsFileReader reader, const char* table_name, char** columns,
    uint32_t column_num, Timestamp start_time, Timestamp end_time,
    TagFilterHandle tag_filter, int batch_size, ERRNO* err_code);
```

### Read batch results as Arrow

Batch query result sets (`batch_size > 0`) can be fetched as Arrow C Data
Interface arrays and schemas. The caller owns the returned Arrow objects and
must call their `release` callbacks when finished.

```C
ERRNO tsfile_result_set_get_next_tsblock_as_arrow(ResultSet result_set,
                                                  ArrowArray* out_array,
                                                  ArrowSchema* out_schema);
```

Example — read `temperature` only for devices whose `region` TAG equals
`shanghai`:

```C
ERRNO ec = RET_OK;
TagFilterHandle f = tsfile_tag_filter_create(
    reader, "weather", "region", "shanghai", TAG_FILTER_EQ, &ec);

char* cols[] = {"temperature"};
ResultSet rs = tsfile_reader_query_table_by_row(
    reader, "weather", cols, 1, /*offset*/ 0, /*limit*/ -1, f, /*batch*/ 0, &ec);

// ... iterate rs with tsfile_result_set_next(), then release:
free_tsfile_result_set(&rs);
tsfile_tag_filter_free(f);
```

### Get Data from result set

```c
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
 * @param column_index [in] Column position (1 <= index <= result_column_count).
 * @return bool - true: Value is NULL or index out of range, false: Valid value.
 */
bool tsfile_result_set_is_null_by_index(ResultSet result_set,
                                        uint32_t column_index);

/**
 * @brief Gets string value from current row by column name.
 * @param result_set [in] valid result set handle.
 * @param column_name [in] the name of the column to be checked.
 * @return char* - String pointer. Caller must free this ptr after usage.
 */
char* tsfile_result_set_get_value_by_name_string(ResultSet result_set,
                                                 const char* column_name);

// Supports multiple data types
bool tsfile_result_set_get_value_by_name_bool(ResultSet result_set, const char* 
                                                column_name);
int32_t tsfile_result_set_get_value_by_name_int32_t(ResultSet result_set, const char* 
                                                column_name);
int64_t tsfile_result_set_get_value_by_name_int64_t(ResultSet result_set, const char* 
                                                column_name);
float tsfile_result_set_get_value_by_name_float(ResultSet result_set, const char* 
                                                column_name);
double tsfile_result_set_get_value_by_name_double(ResultSet result_set, const char* 
                                                column_name);

/**
 * @brief Gets string value from current row by column index.
 * @param result_set [in] valid result set handle.
 * @param column_index [in] the index of the column to be checked (1 <= index <= column_num).
 * @return char* - String pointer. Caller must free this ptr after usage.
 */
char* tsfile_result_set_get_value_by_index_string(ResultSet result_set,
                                                  uint32_t column_index);

// Supports multiple data types
int32_t tsfile_result_set_get_value_by_index_int32_t(ResultSet result_set, uint32_t 
                                                    column_index);
int64_t tsfile_result_set_get_value_by_index_int64_t(ResultSet result_set, uint32_t 
                                                    column_index);
float tsfile_result_set_get_value_by_index_float(ResultSet result_set, uint32_t 
                                                    column_index);
double tsfile_result_set_get_value_by_index_double(ResultSet result_set, uint32_t 
                                                    column_index);
bool tsfile_result_set_get_value_by_index_bool(ResultSet result_set, uint32_t 
                                                    column_index);

/**
 * @brief Retrieves metadata describing the ResultSet's schema.
 *
 * @param result_set [in] Valid result set handle.
 * @return ResultSetMetaData Metadata handle. Caller should free the
 * ResultSetMataData after usage.
 * @note Before calling this func, check if the result_set is NULL, which
 * may indicates a failed query execution.
 */
ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set);

/**
 * @brief Gets column name by index from metadata.
 * @param result_set_meta_data [in] Valid result set handle.
 * @param column_index [in] Column position (1 <= index <= column_num).
 * @return const char* Read-only string. NULL if index invalid.
 */
char* tsfile_result_set_metadata_get_column_name(ResultSetMetaData result_set_meta_data,
                                                 uint32_t column_index);

/**
 * @brief Gets column data type by index from metadata.
 * @param result_set_meta_data [in] Valid result set meta data handle.
 * @param column_index [in] Column position (1 <= index <= column_num).
 * @return TSDataType Returns TS_DATATYPE_INVALID(255) if index invalid.
 */
TSDataType tsfile_result_set_metadata_get_data_type(
    ResultSetMetaData result_set_meta_data, uint32_t column_index);

/**
 * @brief Gets total number of columns in the result schema.
 * @param result_set_meta_data [in] Valid result set meta data handle.
 * @return column num in result set metadata.
 */
int tsfile_result_set_metadata_get_column_num(ResultSetMetaData result_set);
```



###  Get Table Schema from TsFile Reader

```C
/**
 * @brief Gets specific table's schema in the tsfile.
 * @param reader [in], valid reader handle.
 * @param table_name [in] Target table name. Must exist in the TsFile.
 * @return TableSchema, contains table and column info.
 * @note Caller should call free_table_schema to free the tableschema.
 */
TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                           const char* table_name);
/**
 * @brief Gets all tables' schema in the tsfile.
 * @param size[out] num of tableschema in return ptr.
 * @return TableSchema*, an array of table schema.
 * @note The caller must call free_table_schema() on each array element
 *  and free to deallocate the array pointer.
 */
TableSchema* tsfile_reader_get_all_table_schemas(TsFileReader reader,
                                                 uint32_t* size);

/**
 * @brief Gets all timeseries schema in the tsfile.
 * @param size[out] number of DeviceSchema elements in the returned array.
 * @return DeviceSchema*, an array of device schemas.
 * @note The caller must call free_device_schema() on each element
 * and free() the array pointer.
 */
DeviceSchema* tsfile_reader_get_all_timeseries_schemas(TsFileReader reader,
                                                       uint32_t* size);

/**
 * @brief Free the tableschema's space.
 * @param schema [in] the table schema to be freed.
 */
void free_table_schema(TableSchema schema);

void free_device_schema(DeviceSchema schema);
```

### Get Devices and Timeseries Metadata

```C
/**
 * @brief Lists all devices in the file.
 *
 * @param out_devices[out] allocated array; free with tsfile_free_device_id_array().
 * @param out_length[out] number of devices in the returned array.
 */
ERRNO tsfile_reader_get_all_devices(TsFileReader reader, DeviceID** out_devices,
                                    uint32_t* out_length);

void tsfile_free_device_id_array(DeviceID* devices, uint32_t length);
void tsfile_device_id_free_contents(DeviceID* d);

/**
 * @brief Timeseries metadata for all devices in the file.
 */
ERRNO tsfile_reader_get_timeseries_metadata_all(
    TsFileReader reader, DeviceTimeseriesMetadataMap* out_map);

/**
 * @brief Timeseries metadata for the specified devices.
 *
 * length == 0 returns an empty map. For non-empty input, each DeviceID.path
 * should contain the canonical device path.
 */
ERRNO tsfile_reader_get_timeseries_metadata_for_devices(
    TsFileReader reader, const DeviceID* devices, uint32_t length,
    DeviceTimeseriesMetadataMap* out_map);

void tsfile_free_device_timeseries_metadata_map(
    DeviceTimeseriesMetadataMap* map);
```


