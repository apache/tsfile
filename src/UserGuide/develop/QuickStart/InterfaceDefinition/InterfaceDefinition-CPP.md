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
# Interface Definitions - C++

## Data Types, Encoding and Compression

```cpp
// Supported measurement/column data types.
enum TSDataType : uint8_t {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    FLOAT = 3,
    DOUBLE = 4,
    TEXT = 5,
    VECTOR = 6,
    UNKNOWN = 7,
    TIMESTAMP = 8,
    DATE = 9,
    BLOB = 10,
    STRING = 11,
    NULL_TYPE = 254,
    INVALID_DATATYPE = 255,
};

// Value encoding. See the table below for which encodings apply to which types.
enum TSEncoding : uint8_t {
    PLAIN = 0,
    DICTIONARY = 1,
    RLE = 2,
    DIFF = 3,
    TS_2DIFF = 4,
    BITMAP = 5,
    GORILLA_V1 = 6,
    REGULAR = 7,
    GORILLA = 8,
    ZIGZAG = 9,
    FREQ = 10,
    SPRINTZ = 12,
    INVALID_ENCODING = 255,
};

// Compression type. SNAPPY/GZIP/LZO/LZ4 depend on build options; LZ4 is the default.
enum CompressionType : uint8_t {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    SDT = 4,
    PAA = 5,
    PLA = 6,
    LZ4 = 7,
    INVALID_COMPRESSION = 255,
};

// Column role within a table schema.
enum class ColumnCategory { TAG = 0, FIELD = 1, ATTRIBUTE = 2, TIME = 3 };
```

Encodings applicable to each data type:

| Encoding | Applicable types |
|---|---|
| `PLAIN` | all types |
| `DICTIONARY` | `TEXT`, `STRING` |
| `RLE` | `INT32`, `INT64`, `TIMESTAMP`, `DATE` |
| `TS_2DIFF` | `INT32`, `INT64`, `TIMESTAMP`, `DATE`, `FLOAT`, `DOUBLE` |
| `GORILLA` | `INT32`, `INT64`, `TIMESTAMP`, `DATE`, `FLOAT`, `DOUBLE` |
| `ZIGZAG` | `INT32`, `INT64` |
| `SPRINTZ` | `INT32`, `INT64`, `FLOAT`, `DOUBLE` |

Default value encoding per type: `BOOLEAN → PLAIN`, `INT32 / INT64 → TS_2DIFF`,
`FLOAT / DOUBLE → GORILLA`, `TEXT / STRING / BLOB → PLAIN`. The default
compression is `LZ4`. See [Configuring encoding and compression](#configuring-encoding-and-compression)
for how to override these.

## Write Interface

### TsFileTableWriter

Used to write data to tsfile

```cpp
/**
 * @brief Facilitates writing structured table data into a TsFile with a specified schema.
 *
 * The TsFileTableWriter class is designed to write structured data, particularly suitable for time-series data,
 * into a file optimized for efficient storage and retrieval (referred to as TsFile here). It allows users to define
 * the schema of the tables they want to write, add rows of data according to that schema, and serialize this data
 * into a TsFile. Additionally, it provides options to limit memory usage during the writing process.
 */
class TsFileTableWriter {
   public:
    /**
     * TsFileTableWriter is used to write table data into a target file with the given schema,
     * optionally limiting the memory usage.
     *
     * @param writer_file Target file where the table data will be written. Must not be null.
     * @param table_schema Used to construct table structures. Defines the schema of the table
     *                     being written.
     * @param memory_threshold Optional parameter. When the size of written
     * data exceeds this value, the data will be automatically flushed to the
     * disk. Default value is 128MB.
     */
    TsFileTableWriter(WriteFile* writer_file,
                      TableSchema* table_schema,
                      uint64_t memory_threshold = 128 * 1024 * 1024);
    ~TsFileTableWriter();
    /**
     * Writes the given tablet data into the target file according to the schema.
     *
     * @param tablet The tablet containing the data to be written. Must not be null.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int write_table(const Tablet& tablet);
    /**
     * Flushes any buffered data to the underlying storage medium, ensuring all data is written out.
     * This method ensures that all pending writes are persisted.
     *
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int flush();
    /**
     * Closes the writer and releases any resources held by it.
     * After calling this method, no further operations should be performed on this instance.
     *
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int close();
};
```

### TableSchema

Describe the data structure of the table schema

```cpp
/**
* @brief Represents the schema information for an entire table.
*
* This class holds the metadata necessary to describe how a specific table is structured,
* including its name and the schemas of all its columns.
*/
class TableSchema {
    public:
    /**
     * Constructs a TableSchema object with the given table name, column schemas, and column categories.
     *
     * @param table_name The name of the table. Must be a non-empty string.
     *                   This name is used to identify the table within the system.
     * @param column_schemas A vector containing ColumnSchema objects.
     *                       Each ColumnSchema defines the schema for one column in the table.
     */
    TableSchema(const std::string& table_name,
                const std::vector<ColumnSchema>& column_schemas);
};


/**
* @brief Represents the schema information for a single column.
*
* This structure holds the metadata necessary to describe how a specific column is stored,
* including its name, data type, category.
*/
struct ColumnSchema {
    std::string column_name_;
    common::TSDataType data_type_;
    common::CompressionType compression_;
    common::TSEncoding encoding_;
    ColumnCategory column_category_;

    /**
     * @brief Constructs a ColumnSchema with explicit compression and encoding.
     *
     * @param column_name The name of the column. Must be a non-empty string.
     * @param data_type The data type of the column (INT32, DOUBLE, TEXT, ...).
     * @param compression The compression applied to the column's chunks.
     * @param encoding The encoding applied to the column's values.
     * @param column_category The role of the column (FIELD, TAG, ...). Defaults to FIELD.
     */
    ColumnSchema(std::string column_name, common::TSDataType data_type,
                 common::CompressionType compression, common::TSEncoding encoding,
                 ColumnCategory column_category = ColumnCategory::FIELD);

    /**
     * @brief Constructs a ColumnSchema using the engine's default encoding and
     * compression for the given data type.
     *
     * @param column_name The name of the column. Must be a non-empty string.
     * @param data_type The data type of the column.
     * @param column_category The role of the column. Defaults to FIELD.
     */
    ColumnSchema(std::string column_name, common::TSDataType data_type,
                 ColumnCategory column_category = ColumnCategory::FIELD);
};
```

> `TAG` columns are the device identifier (joint primary key); their data type is
> always `STRING`. `FIELD` columns hold the measured values. The encoding and
> compression you set on a `ColumnSchema` apply to that column when written; the
> two-argument constructor falls back to the per-type defaults.

### Tablet

Write column memory structure

```cpp
/**
 * @brief Represents a collection of data rows with associated metadata for insertion into a table.
 *
 * This class is used to manage and organize data that will be inserted into a specific target table.
 * It handles the storage of timestamps and values, along with their associated metadata such as column names and types.
 */
class Tablet {
public:
    /**
     * @brief Constructs a Tablet object with the given parameters.
     *
     * @param column_names A vector containing the names of the columns in the tablet.
     *                     Each name corresponds to a column in the target table.
     * @param data_types A vector containing the data types of each column.
     *                   These must match the schema of the target table.
     * @param max_rows The maximum number of rows that this tablet can hold. Defaults to DEFAULT_MAX_ROWS.
     */
    Tablet(const std::vector<std::string> &column_names,
           const std::vector<common::TSDataType> &data_types,
           int max_rows = DEFAULT_MAX_ROWS);

    /**
     * @brief Adds a timestamp to the specified row.
     *
     * @param row_index The index of the row to which the timestamp will be added.
     *                  Must be less than the maximum number of rows.
     * @param timestamp The timestamp value to add.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int add_timestamp(uint32_t row_index, int64_t timestamp);

    /**
     * @brief Template function to add a value of type T to the specified row and column.
     *
     * @tparam T The type of the value to add.
     * @param row_index The index of the row to which the value will be added.
     *                  Must be less than the maximum number of rows.
     * @param schema_index The index of the column schema corresponding to the value being added.
     * @param val The value to add.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    template <typename T>
    int add_value(uint32_t row_index, uint32_t schema_index, T val);

    /**
     * @brief Template function to add a value of type T to the specified row and column by name.
     *
     * @tparam T The type of the value to add.
     * @param row_index The index of the row to which the value will be added.
     *                  Must be less than the maximum number of rows.
     * @param measurement_name The name of the column to which the value will be added.
     *                         Must match one of the column names provided during construction.
     * @param val The value to add.
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    template <typename T>
    int add_value(uint32_t row_index, const std::string &measurement_name, T val);
};
```

### RestorableTsFileIOWriter

```cpp
namespace storage {
/**
 * RestorableTsFileIOWriter is used to open a TsFile and perform optional recovery operations on it.
 * Inherits from TsFileIOWriter and supports continuous writing after file recovery.
 *
 * (1) If the TsFile was closed normally: has_crashed()=false, can_write()=false
 *
 * (2) If the TsFile is incomplete / the program crashed: has_crashed()=true,
 * can_write()=true. The writer will truncate the corrupted data and allow further writing.
 *
 * Implemented based on standard C++11, uses RAII and smart pointers to avoid memory leaks.
 */
class RestorableTsFileIOWriter : public TsFileIOWriter {
   public:
    RestorableTsFileIOWriter();

    /**
     * Opens a TsFile for recovery / appending data.
     * Uses O_RDWR|O_CREAT mode without O_TRUNC, so the original file content is preserved.
     *
     * @param file_path Path of the TsFile
     * @param truncate_corrupted If true, truncate the corrupted data;
     *        If false, do not truncate (the incomplete file remains unchanged)
     * @return E_OK on success, error code on failure
     */
    int open(const std::string& file_path, bool truncate_corrupted = true);

    /**
     * Closes the file
     */
    void close();
};

}  // namespace storage
```

### Configuring encoding and compression

Encoding and compression are chosen **per data type**: each type has a default
(see the table above). You can change those defaults, or pass an explicit
encoding/compression on a schema.

**1. On a schema.** Pass an explicit encoding and compression when you build a
`ColumnSchema`:

```cpp
// Store column "temperature" as TS_2DIFF + LZ4.
common::ColumnSchema col("temperature", common::INT64,
                         common::LZ4, common::TS_2DIFF,
                         common::ColumnCategory::FIELD);
```

**2. Per-type defaults.** Change the defaults *before* creating a writer; they then
apply to any column whose schema does not specify its own encoding/compression.
These helpers live in `common`/`storage` and validate their arguments (returning
`E_NOT_SUPPORT` for an unsupported combination):

```cpp
// Default value encoding per data type and default compression.
int  common::set_datatype_encoding(uint8_t data_type, uint8_t encoding);
int  common::set_global_compression(uint8_t compression);
uint8_t common::get_datatype_encoding(uint8_t data_type);
uint8_t common::get_global_compression();

// Time-column encoding/compression (the data type is fixed to INT64).
int  common::set_global_time_encoding(uint8_t encoding);
int  common::set_global_time_compression(uint8_t compression);
uint8_t common::get_global_time_encoding();
uint8_t common::get_global_time_compression();
```

Global compression accepts `UNCOMPRESSED`, `SNAPPY`, `GZIP`, `LZO`, and `LZ4`.
The codec enum also contains legacy values such as `SDT`, `PAA`, and `PLA`, but
the global compression setter rejects them.

## Read Interface 
### Tsfile Reader
```cpp
/**
 * @brief TsfileReader provides the ability to query all files with the suffix
 * .tsfile
 *
 * TsfileReader is designed to query .tsfile files. It accepts table-model
 * queries and supports querying metadata such as TableSchema.
 */
class TsFileReader {
   public:
    TsFileReader();
    ~TsFileReader();
    /**
     * @brief open the tsfile
     *
     * @param file_path the path of the tsfile which will be opened
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int open(const std::string &file_path);
    /**
     * @brief close the tsfile, this method should be called after the
     * query is finished
     *
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int close();
    /**
     * @brief query the tsfile by the query expression,Users can construct
     * their own query expressions to query tsfile
     *
     * @param [in] qe the query expression
     * @param [out] ret_qds the result set
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int query(storage::QueryExpression *qe, ResultSet *&ret_qds);
    /**
     * @brief query the tsfile by the path list, start time and end time.
     * This method is used to query the tree model.
     *
     * @param [in] path_list the full path list
     * @param [in] start_time the start time
     * @param [in] end_time the end time
     * @param [out] result_set the result set
     */
    int query(std::vector<std::string>& path_list, int64_t start_time,
              int64_t end_time, ResultSet*& result_set);
    /**
     * @brief query the tsfile by the table name, columns names, start time
     * and end time.
     *
     * @param [in] table_name the table name
     * @param [in] columns_names the columns names
     * @param [in] start_time the start time
     * @param [in] end_time the end time
     * @param [out] result_set the result set
     */
    int query(const std::string &table_name,
              const std::vector<std::string> &columns_names, int64_t start_time,
              int64_t end_time, ResultSet *&result_set, int batch_size = -1);

    /**
     * @brief query the tsfile by the table name, columns names, start time
     * and end time, tag filter.
     *
     * @param [in] table_name the table name
     * @param [in] columns_names the columns names
     * @param [in] start_time the start time
     * @param [in] end_time the end time
     * @param [in] tag_filter the tag filter
     * @param [out] result_set the result set
     */
    int query(const std::string& table_name,
              const std::vector<std::string>& columns_names, int64_t start_time,
              int64_t end_time, ResultSet*& result_set, Filter* tag_filter,
              int batch_size = 0);

    /**
     * @brief query tree-model time series by row with offset and limit.
     */
    int queryByRow(std::vector<std::string>& path_list, int offset, int limit,
                   ResultSet*& result_set);

    /**
     * @brief query a table by row, with offset/limit pushdown and an optional
     * tag filter.
     *
     * @param [in] table_name the table name
     * @param [in] column_names the column names
     * @param [in] offset leading rows to skip (>= 0)
     * @param [in] limit max rows to return; < 0 means unlimited
     * @param [out] result_set the result set
     * @param [in] tag_filter optional tag filter built with TagFilterBuilder, or nullptr
     * @param [in] batch_size <= 0 returns rows one by one; > 0 returns blocks of that size
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int queryByRow(const std::string& table_name,
                   const std::vector<std::string>& column_names, int offset,
                   int limit, ResultSet*& result_set,
                   Filter* tag_filter = nullptr, int batch_size = 0);

    /**
     * @brief query tree-model data by measurement names within a time range.
     */
    int query_table_on_tree(const std::vector<std::string>& measurement_names,
                            int64_t start_time, int64_t end_time,
                            ResultSet*& result_set);

    /**
     * @brief destroy the result set, this method should be called after the
     * query is finished and result_set
     *
     * @param qds the result set
     */
    void destroy_query_data_set(ResultSet *qds);

    ResultSet* read_timeseries(
        const std::shared_ptr<IDeviceID>& device_id,
        const std::vector<std::string>& measurement_name);

    std::vector<std::shared_ptr<IDeviceID>> get_all_devices(
        std::string table_name);

    std::vector<std::shared_ptr<IDeviceID>> get_all_device_ids();

    std::vector<std::shared_ptr<IDeviceID>> get_all_devices();

    int get_timeseries_schema(std::shared_ptr<IDeviceID> device_id,
                              std::vector<MeasurementSchema>& result);

    DeviceTimeseriesMetadataMap get_timeseries_metadata(
        const std::vector<std::shared_ptr<IDeviceID>>& device_ids);

    DeviceTimeseriesMetadataMap get_timeseries_metadata();

    /**
     * @brief get the table schema by the table name
     *
     * @param table_name the table name
     * @return std::shared_ptr<TableSchema> the table schema
     */
    std::shared_ptr<TableSchema> get_table_schema(
        const std::string &table_name);
    /**
     * @brief get all table schemas in the tsfile
     *
     * @return std::vector<std::shared_ptr<TableSchema>> the table schema list
     */
    std::vector<std::shared_ptr<TableSchema>> get_all_table_schemas();
};
```

`DeviceTimeseriesMetadataMap` is the metadata map returned by
`get_timeseries_metadata()`: `std::map<std::shared_ptr<IDeviceID>,
std::vector<std::shared_ptr<ITimeseriesIndex>>, IDeviceIDComparator>`.

### ResultSet
```cpp
/**
 * @brief ResultSet is the query result of the TsfileReader. It provides access
 * to the results.
 *
 * ResultSet is a virtual class. Convert it to the corresponding implementation
 * class when used.
 * @note The concrete type is TableResultSet.
 */
class ResultSet {
   public:
    ResultSet() {}
    virtual ~ResultSet() {}
    /**
     * @brief Get the next row of the result set
     *
     * @param[out] has_next a boolean value indicating if there is a next row
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    virtual int next(bool& has_next) = 0;
    /**
     * @brief Check if the value of the column is null by column name
     *
     * @param column_name the name of the column
     * @return true if the value is null, false otherwise
     */
    virtual bool is_null(const std::string& column_name) = 0;
    /**
     * @brief Check if the value of the column is null by column index
     *
     * @param column_index the index of the column starting from 1
     * @return true if the value is null, false otherwise
     */
    virtual bool is_null(uint32_t column_index) = 0;

    /**
     * @brief Get the value of the column by column name
     *
     * @param column_name the name of the column
     * @return the value of the column
     */
    template <typename T>
    T get_value(const std::string& column_name);
    /**
     * @brief Get the value of the column by column index
     *
     * @param column_index the index of the column starting from 1
     * @return the value of the column
     */
    template <typename T>
    T get_value(uint32_t column_index);
    /**
     * @brief Get the row record of the result set
     *
     * @return the row record
     */
    virtual RowRecord* get_row_record() = 0;
    /**
     * @brief Get the metadata of the result set
     *
     * @return std::shared_ptr<ResultSetMetadata> the metadata of the result set
     */
    virtual std::shared_ptr<ResultSetMetadata> get_metadata() = 0;
    /**
     * @brief Close the result set
     *
     * @note this method should be called after the result set is no longer
     * needed.
     */
    virtual void close() = 0;
};
```
### ResultMeta
```cpp
/**
 * @brief metadata of result set
 *
 * user can obtain the metadata from ResultSetMetadata, including all column
 * names and data types. When a user uses the table model, the first column
 * defaults to the time column.
 */
class ResultSetMetadata {
   public:
    /**
     * @brief constructor of ResultSetMetadata
     *
     * @param column_names the column names
     * @param column_types the column types
     */
    ResultSetMetadata(const std::vector<std::string>& column_names,
                      const std::vector<common::TSDataType>& column_types);
    /**
     * @brief get the column type
     *
     * @param column_index the column index starting from 1
     * @return the column type
     */
    common::TSDataType get_column_type(uint32_t column_index);
    /**
     * @brief get the column name
     *
     * @param column_index the column index starting from 1
     * @return the column name
     */
    std::string get_column_name(uint32_t column_index);
    /**
     * @brief get the column count
     *
     * @return the column count by uint32_t
     */
    uint32_t get_column_count();
};
```
### Filter
#### TagFilterBuilder
Used to construct tag-based filters for querying data
```cpp
class TagFilterBuilder {
   public:
    explicit TagFilterBuilder(TableSchema* schema);

    Filter* eq(const std::string& columnName, const std::string& value);
    Filter* neq(const std::string& columnName, const std::string& value);
    Filter* lt(const std::string& columnName, const std::string& value);
    Filter* lteq(const std::string& columnName, const std::string& value);
    Filter* gt(const std::string& columnName, const std::string& value);
    Filter* gteq(const std::string& columnName, const std::string& value);
    Filter* reg_exp(const std::string& columnName, const std::string& value);
    Filter* not_reg_exp(const std::string& columnName,
                        const std::string& value);
    Filter* is_null(const std::string& columnName);
    Filter* is_not_null(const std::string& columnName);
    Filter* between_and(const std::string& columnName, const std::string& lower,
                        const std::string& upper);
    Filter* not_between_and(const std::string& columnName,
                            const std::string& lower, const std::string& upper);

    // Logical operations
    static Filter* and_filter(Filter* left, Filter* right);
    static Filter* or_filter(Filter* left, Filter* right);
    static Filter* not_filter(Filter* filter);
};
```
