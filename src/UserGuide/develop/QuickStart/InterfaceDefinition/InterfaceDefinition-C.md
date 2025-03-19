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
# Interface definition



## Schema

```C
typedef enum {
    TS_DATATYPE_BOOLEAN = 0,
    TS_DATATYPE_INT32 = 1,
    TS_DATATYPE_INT64 = 2,
    TS_DATATYPE_FLOAT = 3,
    TS_DATATYPE_DOUBLE = 4,
    TS_DATATYPE_TEXT = 5,
    TS_DATATYPE_STRING = 11
} TSDataType;

typedef enum column_category { TAG = 0, FIELD = 1 } ColumnCategory;

// ColumnSchema: Represents the schema of a single column, 
// including its name, data type, and category.
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

// ResultSetMetaData: Contains metadata for a result set, 
// such as column names and their data types.
typedef struct result_set_meta_data {
    char** column_names;
    TSDataType* data_types;
    int column_num;
} ResultSetMetaData;
```


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

When creating a TsFile Writer, you need to specify WriteFile and TableSchema. You can use the memory_threshold parameter in
tsfile_writer_new_with_memory_threshold to limit the memory usage of the Writer during data writing, but in the current version, this parameter does not take effect.

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
 * @param memory_threshold used to limit the memory size
 *                      of objects. If set to 0, no memory limit is enforced.
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
ERRNO tablet_add_value_by_name_string(Tablet tablet, uint32_t row_index,
                                      const char* column_name,
                                      const char* value);

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
ERRNO tablet_add_value_by_index_string(Tablet tablet, uint32_t row_index,
                                       uint32_t column_index,
                                       const char* value);


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
 * @param column_index [in] the index of the column to be checked.
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
 * @param result_set [in] Valid result set handle.
 * @param column_index [in] Column position (1 <= index <= column_num).
 * @return const char* Read-only string. NULL if index invalid.
 */
char* tsfile_result_set_metadata_get_column_name(ResultSetMetaData result_set,
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
 * @brief Free the tableschema's space.
 * @param schema [in] the table schema to be freed.
 */
void free_table_schema(TableSchema schema);
```








