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

## Write Interface

### TsFileTableWriter

Used to write data to tsfile

```cpp
namespace storage {
class RestorableTsFileIOWriter;

/**
 * @brief Supports writing structured table data to TsFile according to the specified table schema
 *
 * The TsFileTableWriter class is used to write structured data (especially suitable for time-series data)
 * to TsFile optimized for efficient storage and querying.
 * Users can define the structure of the table to be written, add data rows according to the structure,
 * and serialize the data into TsFile.
 * Meanwhile, this class provides the ability to limit memory usage during the writing process.
 */
class TsFileTableWriter {
   public:
    /**
     * TsFileTableWriter is used to write table data to the target file according to the specified table schema,
     * and can optionally limit the memory usage.
     *
     * @param writer_file Target file for writing table data, cannot be a null pointer
     * @param table_schema Used to construct the table structure and define the schema of the table to be written
     * @param memory_threshold Optional parameter. When the written data volume exceeds this threshold,
     *                         data will be automatically flushed to disk. The default value is 128MB
     */
    template <typename T>
    explicit TsFileTableWriter(storage::WriteFile* writer_file, T* table_schema,
                               uint64_t memory_threshold = 128 * 1024 * 1024) {
        static_assert(!std::is_same<T, std::nullptr_t>::value,
                      "table_schema cannot be nullptr");
        tsfile_writer_ = std::make_shared<TsFileWriter>();
        tsfile_writer_->init(writer_file);
        tsfile_writer_->set_generate_table_schema(false);

        // Perform a deep copy. The source TableSchema object may be allocated on the stack/heap
        auto table_schema_ptr = std::make_shared<TableSchema>(*table_schema);
        error_number = tsfile_writer_->register_table(table_schema_ptr);
        exclusive_table_name_ = table_schema->get_table_name();
        common::g_config_value_.chunk_group_size_threshold_ = memory_threshold;
    }

    /**
     * Constructs TsFileTableWriter from a restorable TsFileIOWriter,
     * supporting appending table data after failure recovery.
     * The schema is read from the recovered file without additional TableSchema input.
     *
     * @param restorable_writer Recovered I/O writer; cannot be a null pointer,
     *                          and must be opened in truncate mode to ensure can_write() returns true
     * @param memory_threshold Optional memory threshold for cached data
     */
    explicit TsFileTableWriter(
        storage::RestorableTsFileIOWriter* restorable_writer,
        uint64_t memory_threshold = 128 * 1024 * 1024);

    /**
     * Registers a table schema with the writer
     *
     * @param table_schema The table schema to be registered, cannot be a null pointer
     * @return Returns 0 on success, non-zero error code on failure
     */
    int register_table(const std::shared_ptr<TableSchema>& table_schema);

    /**
     * Writes the specified Tablet data to the target file according to the table schema
     *
     * @param tablet Tablet containing the data to be written, cannot be a null pointer
     * @return Returns 0 on success, non-zero error code on failure
     */
    int write_table(Tablet& tablet) const;

    /**
     * Flushes all cached data to the underlying storage medium to ensure all data is persisted.
     * This method guarantees that all pending data is written to disk.
     *
     * @return Returns 0 on success, non-zero error code on failure
     */
    int flush();

    /**
     * Closes the writer and releases all resources it occupies.
     * No subsequent operations should be performed on the current instance after calling this method.
     *
     * @return Returns 0 on success, non-zero error code on failure
     */
    int close();
};

}  // namespace storage
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
    ColumnCategory column_category_;

    /**
     * @brief Constructs a ColumnSchema object with the given parameters.
     *
     * @param column_name The name of the column. Must be a non-empty string.
     *                    This name is used to identify the column within the table.
     * @param data_type The data type of the measurement, such as INT32, DOUBLE, TEXT, etc.
     *                  This determines how the data will be stored and interpreted.
     * @param column_category The category of the column indicating its role or type
     *                        within the schema, e.g., FIELD, TAG.
     *                        Defaults to ColumnCategory::FIELD if not specified.
     * @note It is the responsibility of the caller to ensure that `column_name` is not empty.
     */
    ColumnSchema(std::string column_name, common::TSDataType data_type,
                 ColumnCategory column_category = ColumnCategory::FIELD) : column_name_(std::move(column_name)),
                                                                           data_type_(data_type),
                                                                           column_category_(column_category) {
    }
};

/**
 * @brief Represents the data type of a measurement.
 *
 * This enumeration defines the supported data types for measurements in the system.
 */
enum TSDataType : uint8_t {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    FLOAT = 3,
    DOUBLE = 4,
    TEXT = 5,
    STRING = 11
};

```

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
> V2.3.1

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



## Read Interface 
### Tsfile Reader
use to execute query in tsfile and return value by ResultSet.
```cpp
namespace storage {
/**
 * @brief TsFileReader provides the ability to query all files with the .tsfile suffix
 *
 * TsFileReader is designed specifically for querying .tsfile files, supporting both tree-model queries and table-model queries.
 * It also supports querying metadata such as table schemas (TableSchema) and time-series schemas (TimeseriesSchema).
 */
class TsFileReader {
   public:
    TsFileReader();
    /**
     * @brief Opens a TsFile
     *
     * @param file_path Path of the TsFile to be opened
     * @return 0 on success, non-zero error code on failure
     */
    int open(const std::string& file_path);
    /**
     * @brief Closes the TsFile. This method should be called after queries are completed.
     *
     * @return 0 on success, non-zero error code on failure
     */
    int close();
    /**
     * @brief Queries the TsFile using a query expression. Users can construct custom query expressions for execution.
     *
     * @param [in] qe Query expression
     * @param [out] ret_qds Result set
     * @return 0 on success, non-zero error code on failure
     */
    int query(storage::QueryExpression* qe, ResultSet*& ret_qds);
    /**
     * @brief Queries the TsFile by path list, start time, and end time.
     * This method is used for tree-model queries on TsFile.
     *
     * @param [in] path_list Path list
     * @param [in] start_time Start timestamp
     * @param [in] end_time End timestamp
     * @param [out] result_set Result set
     * @return 0 on success, non-zero error code on failure
     */
    int query(std::vector<std::string>& path_list, int64_t start_time,
              int64_t end_time, ResultSet*& result_set);
    /**
     * @brief Queries the TsFile by table name, column names, start time, and end time.
     * This method is used for table-model queries on TsFile.
     *
     * @param [in] table_name Table name
     * @param [in] columns_names List of column names
     * @param [in] start_time Start timestamp
     * @param [in] end_time End timestamp
     * @param [out] result_set Result set
     * @param [in] batch_size ≤ 0 for row-by-row mode;
     *             > 0 to return TsBlock chunks of the specified size
     * @return 0 on success, non-zero error code on failure
     */
    int query(const std::string& table_name,
              const std::vector<std::string>& columns_names, int64_t start_time,
              int64_t end_time, ResultSet*& result_set, int batch_size = -1);

    /**
     * @brief Queries the TsFile by table name, column names, start time, end time, and tag filter conditions.
     * This method is used for table-model queries on TsFile.
     *
     * @param [in] table_name Table name
     * @param [in] columns_names List of column names
     * @param [in] start_time Start timestamp
     * @param [in] end_time End timestamp
     * @param [in] tag_filter Tag filter condition
     * @param [out] result_set Result set
     * @param [in] batch_size Batch reading size
     * @return 0 on success, non-zero error code on failure
     */
    int query(const std::string& table_name,
              const std::vector<std::string>& columns_names, int64_t start_time,
              int64_t end_time, ResultSet*& result_set, Filter* tag_filter,
              int batch_size = 0);

    /**
     * @brief Queries tree-model time-series data by row with offset and row limit.
     *
     * @param path_list  Full paths to query (device.measurement)
     * @param offset     Number of starting rows to skip (>= 0)
     * @param limit      Maximum number of rows to return; no limit if < 0
     * @param[out] result_set  Result set to store query results
     * @return 0 on success, non-zero error code on failure
     */
    int queryByRow(std::vector<std::string>& path_list, int offset, int limit,
                   ResultSet*& result_set);

    /**
     * @brief Queries table-model data by row with pushed-down offset and row limit.
     *
     * For dense devices (all columns have the same row count),
     * offset/limit are pushed down to the data block/page level via SSI,
     * skipping entire blocks/pages without decoding.
     * For sparse devices, offset/limit take effect during row merging.
     * Entire devices can be skipped directly if their total rows fall within the offset range.
     *
     * @param table_name     Table name to query
     * @param column_names   Column names to query
     * @param offset         Number of starting rows to skip (>= 0)
     * @param limit          Maximum number of rows to return; no limit if < 0
     * @param[out] result_set  Result set to store query results
     * @param tag_filter     Optional tag filter condition for filtering data by tag columns
     * @param batch_size     Batch reading size
     * @return 0 on success, non-zero error code on failure
     */
    int queryByRow(const std::string& table_name,
                   const std::vector<std::string>& column_names, int offset,
                   int limit, ResultSet*& result_set,
                   Filter* tag_filter = nullptr, int batch_size = 0);

    /**
     * @brief Performs a table query on the tree model.
     *
     * @param measurement_names List of measurement names
     * @param start_time Start timestamp
     * @param end_time End timestamp
     * @param result_set Result set
     * @return 0 on success, non-zero error code on failure
     */
    int query_table_on_tree(const std::vector<std::string>& measurement_names,
                            int64_t start_time, int64_t end_time,
                            ResultSet*& result_set);
    /**
     * @brief Destroys the result set. This method should be called after the query is completed and the result set is no longer used.
     *
     * @param qds Result set object
     */
    void destroy_query_data_set(ResultSet* qds);
    /**
     * @brief Reads time-series data by device ID and measurement names.
     *
     * @param device_id Device ID
     * @param measurement_name List of measurement names
     * @return Result set object
     */
    ResultSet* read_timeseries(
        const std::shared_ptr<IDeviceID>& device_id,
        const std::vector<std::string>& measurement_name);
    /**
     * @brief Gets all devices in the TsFile for a specified table.
     *
     * @param table_name Table name
     * @return List of device IDs
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices(
        std::string table_name);

    /**
     * @brief Gets all device IDs in the TsFile.
     *
     * @return List of device IDs
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_device_ids();

    /**
     * @brief Gets all device IDs in the file (functionally identical to get_all_device_ids).
     *
     * @return List of devices
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices();

    /**
     * @brief Gets time-series schemas by device ID and measurement names.
     *
     * @param [in] device_id Device ID
     * @param [out] result List of measurement schemas
     * @return 0 on success, non-zero error code on failure
     */
    int get_timeseries_schema(std::shared_ptr<IDeviceID> device_id,
                              std::vector<MeasurementSchema>& result);

    /**
     * @brief Gets time-series metadata for specified devices.
     *
     * Only devices existing in the file are included in the result.
     * Returns an empty map if the device ID list is empty.
     *
     * @param device_ids List of devices to query
     * @return Mapping: Device ID -> List of time-series metadata (existing entries only)
     */
    DeviceTimeseriesMetadataMap get_timeseries_metadata(
        const std::vector<std::shared_ptr<IDeviceID>>& device_ids);

    /**
     * @brief Gets time-series metadata for all devices in the file.
     *
     * @return Mapping: Device ID -> List of time-series metadata
     */
    DeviceTimeseriesMetadataMap get_timeseries_metadata();

    /**
     * @brief Gets the table schema by table name.
     *
     * @param table_name Table name
     * @return Shared pointer to the table schema
     */
    std::shared_ptr<TableSchema> get_table_schema(
        const std::string& table_name);
    /**
     * @brief Gets all table schemas in the TsFile.
     *
     * @return List of table schemas
     */
    std::vector<std::shared_ptr<TableSchema>> get_all_table_schemas();
};
```
### ResultSet
A collection of query.Support iterator to get data, and directly through the column name or index to get specific data.
```cpp
/**
 * @brief ResultSet is the query result of the TsfileReader. It provides access
 * to the results.
 *
 * ResultSet is a virtual class. Convert it to the corresponding implementation
 * class when used
 * @note When using the tree model and the filter is a global time filter,
 * it should be cast as QDSWithoutTimeGenerator.
 * @note When using the tree model and the filter is not a global time filter,
 * it should be QDSWithTimeGenerator.
 * @note If the query uses the table model, the cast should be TableResultSet
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
user can obtain the metadata from ResultSetMetadata, including all columnnames and data types. When a user uses a table model, the first columndefaults to the time column.
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