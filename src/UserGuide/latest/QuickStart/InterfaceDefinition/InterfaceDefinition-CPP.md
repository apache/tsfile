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

Headers live under the C++ `src/reader` and `src/writer` directories in the source tree. This page splits APIs by **table model** (SQL-like table and columns) and **tree model** (device + measurement hierarchy). `storage::TsFileTreeReader` / `storage::TsFileTreeWriter` in `tsfile_tree_reader.h` / `tsfile_tree_writer.h` are the dedicated tree APIs; `storage::TsFileReader` is the main **table** query entry and also exposes some path/tree helpers on the same file.

## Table model: write

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

## Table model: read — `TsFileReader`

Table-oriented queries use `table_name`, column names, optional `Filter` (tag), and optional `batch_size` for TsBlock / batch mode. See `tsfile_reader.h` for full declarations.

```cpp
namespace storage {

class TsFileReader {
   public:
    /**
     * @brief Construct a TsFileReader instance.
     */
    TsFileReader();
    /**
     * @brief Destroy TsFileReader and release held resources.
     */
    ~TsFileReader();

    /**
     * @brief Open a TsFile.
     *
     * @param file_path Path to the target `.tsfile`.
     * @return Returns 0 on success, non-zero on failure.
     */
    int open(const std::string& file_path);
    /**
     * @brief Close the currently opened TsFile.
     *
     * @return Returns 0 on success, non-zero on failure.
     */
    int close();

    /**
     * @brief Query by expression tree.
     *
     * @param qe Input query expression.
     * @param[out] ret_qds Output result set.
     * @return Returns 0 on success, non-zero on failure.
     */
    int query(storage::QueryExpression* qe, ResultSet*& ret_qds);

    /**
     * @brief Query tree-style full paths in a time range.
     *
     * @param path_list Full path list, e.g. `device.measurement`.
     * @param start_time Start timestamp (inclusive).
     * @param end_time End timestamp (inclusive).
     * @param[out] result_set Output result set.
     * @return Returns 0 on success, non-zero on failure.
     */
    int query(std::vector<std::string>& path_list, int64_t start_time,
              int64_t end_time, ResultSet*& result_set);

    /**
     * @brief Query table data in a time range.
     *
     * @param table_name Target table name.
     * @param columns_names Requested columns.
     * @param start_time Start timestamp (inclusive).
     * @param end_time End timestamp (inclusive).
     * @param[out] result_set Output result set.
     * @param batch_size <= 0 means row-by-row mode, > 0 means TsBlock mode.
     * @return Returns 0 on success, non-zero on failure.
     */
    int query(const std::string& table_name,
              const std::vector<std::string>& columns_names, int64_t start_time,
              int64_t end_time, ResultSet*& result_set, int batch_size = -1);

    /**
     * @brief Query table data in a time range with tag filter.
     *
     * @param table_name Target table name.
     * @param columns_names Requested columns.
     * @param start_time Start timestamp (inclusive).
     * @param end_time End timestamp (inclusive).
     * @param[out] result_set Output result set.
     * @param tag_filter Optional filter on TAG columns.
     * @param batch_size <= 0 means row-by-row mode, > 0 means TsBlock mode.
     * @return Returns 0 on success, non-zero on failure.
     */
    int query(const std::string& table_name,
              const std::vector<std::string>& columns_names, int64_t start_time,
              int64_t end_time, ResultSet*& result_set, Filter* tag_filter,
              int batch_size = 0);

    /**
     * @brief Query tree full paths by row with offset/limit.
     *
     * @param path_list Full path list.
     * @param offset Number of leading rows to skip (>= 0).
     * @param limit Maximum rows to return. < 0 means unlimited.
     * @param[out] result_set Output result set.
     * @return Returns 0 on success, non-zero on failure.
     */
    int queryByRow(std::vector<std::string>& path_list, int offset, int limit,
                   ResultSet*& result_set);

    /**
     * @brief Query table by row with offset/limit.
     *
     * @param table_name Target table name.
     * @param column_names Requested columns.
     * @param offset Number of leading rows to skip (>= 0).
     * @param limit Maximum rows to return. < 0 means unlimited.
     * @param[out] result_set Output result set.
     * @param tag_filter Optional filter on TAG columns.
     * @param batch_size <= 0 means row-by-row mode, > 0 means TsBlock mode.
     * @return Returns 0 on success, non-zero on failure.
     */
    int queryByRow(const std::string& table_name,
                   const std::vector<std::string>& column_names, int offset,
                   int limit, ResultSet*& result_set,
                   Filter* tag_filter = nullptr, int batch_size = 0);

    /**
     * @brief Query table view over tree measurements.
     *
     * @param measurement_names Measurement names to select.
     * @param start_time Start timestamp (inclusive).
     * @param end_time End timestamp (inclusive).
     * @param[out] result_set Output result set.
     * @return Returns 0 on success, non-zero on failure.
     */
    int query_table_on_tree(const std::vector<std::string>& measurement_names,
                            int64_t start_time, int64_t end_time,
                            ResultSet*& result_set);

    /**
     * @brief Destroy a result set created by this reader.
     *
     * @param qds Result set pointer to destroy.
     */
    void destroy_query_data_set(ResultSet* qds);

    /**
     * @brief Read timeseries for one device with selected measurements.
     *
     * @param device_id Target device id.
     * @param measurement_name Measurement names to read.
     * @return ResultSet pointer on success.
     */
    ResultSet* read_timeseries(
        const std::shared_ptr<IDeviceID>& device_id,
        const std::vector<std::string>& measurement_name);

    /**
     * @brief Get all devices under a specified table.
     *
     * @param table_name Target table name.
     * @return Device list.
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices(std::string table_name);
    /**
     * @brief Get all device IDs in file.
     *
     * @return Device list.
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_device_ids();
    /**
     * @brief Get all devices in file (alias form).
     *
     * @return Device list.
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices();

    /**
     * @brief Get timeseries schema for one device.
     *
     * @param device_id Target device.
     * @param[out] result Output measurement schemas.
     * @return Returns 0 on success, non-zero on failure.
     */
    int get_timeseries_schema(std::shared_ptr<IDeviceID> device_id,
                              std::vector<MeasurementSchema>& result);

    /**
     * @brief Get timeseries metadata for specified devices.
     *
     * @param device_ids Devices to query.
     * @return Map of device -> timeseries metadata.
     */
    DeviceTimeseriesMetadataMap get_timeseries_metadata(
        const std::vector<std::shared_ptr<IDeviceID>>& device_ids);
    /**
     * @brief Get timeseries metadata for all devices.
     *
     * @return Map of device -> timeseries metadata.
     */
    DeviceTimeseriesMetadataMap get_timeseries_metadata();

    /**
     * @brief Get one table schema by name.
     *
     * @param table_name Target table name.
     * @return Table schema pointer.
     */
    std::shared_ptr<TableSchema> get_table_schema(const std::string& table_name);
    /**
     * @brief Get all table schemas in file.
     *
     * @return List of table schemas.
     */
    std::vector<std::shared_ptr<TableSchema>> get_all_table_schemas();
};

}  // namespace storage
```

## Tree model: `TsFileTreeReader` and `TsFileTreeWriter`

Use `tsfile_tree_reader.h` and `tsfile_tree_writer.h`. `TsFileTreeReader` holds a `std::shared_ptr<TsFileReader>` and exposes device + measurement queries; `TsFileTreeWriter` wraps a `std::shared_ptr<TsFileWriter>` for device/measurement registration and `Tablet` / `TsRecord` writes.

```cpp
namespace storage {

class TsFileTreeReader {
   public:
    /**
     * @brief Construct a TsFileTreeReader instance.
     */
    TsFileTreeReader();
    /**
     * @brief Destroy TsFileTreeReader and release resources.
     */
    ~TsFileTreeReader();

    /**
     * @brief Open a TsFile.
     *
     * @param file_path Path to the target `.tsfile`.
     * @return Returns 0 on success, non-zero on failure.
     */
    int open(const std::string& file_path);
    /**
     * @brief Close current TsFile.
     *
     * @return Returns 0 on success, non-zero on failure.
     */
    int close();

    /**
     * @brief Query tree data by device list + measurement list in time range.
     *
     * @param device_ids Device identifiers.
     * @param measurement_names Measurements to query.
     * @param start_time Start timestamp (inclusive).
     * @param end_time End timestamp (inclusive).
     * @param[out] result_set Output result set.
     * @return Returns 0 on success, non-zero on failure.
     */
    int query(const std::vector<std::string>& device_ids,
              const std::vector<std::string>& measurement_names,
              int64_t start_time, int64_t end_time, ResultSet*& result_set);

    /**
     * @brief Query tree data by row with offset/limit.
     *
     * @param device_ids Device identifiers.
     * @param measurement_names Measurements to query.
     * @param offset Number of leading rows to skip (>= 0).
     * @param limit Maximum rows to return. < 0 means unlimited.
     * @param[out] result_set Output result set.
     * @return Returns 0 on success, non-zero on failure.
     */
    int queryByRow(const std::vector<std::string>& device_ids,
                   const std::vector<std::string>& measurement_names,
                   int offset, int limit, ResultSet*& result_set);

    /**
     * @brief Destroy query result set allocated by tree reader.
     *
     * @param result_set Result set pointer.
     */
    void destroy_query_data_set(ResultSet* result_set);

    /**
     * @brief Get measurement schema of one device.
     *
     * @param device_id Device identifier string.
     * @return Measurement schema list.
     */
    std::vector<MeasurementSchema> get_device_schema(const std::string& device_id);
    /**
     * @brief Get all device IDs in string form.
     *
     * @return Device ID string list.
     */
    std::vector<std::string> get_all_device_ids();
    /**
     * @brief Get all devices in IDeviceID form.
     *
     * @return Device list.
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices();

    /**
     * @brief Get timeseries metadata for selected devices.
     *
     * @param device_ids Devices to query.
     * @return Map of device -> timeseries metadata.
     */
    DeviceTimeseriesMetadataMap get_timeseries_metadata(
        const std::vector<std::shared_ptr<IDeviceID>>& device_ids);
    /**
     * @brief Get timeseries metadata for all devices.
     *
     * @return Map of device -> timeseries metadata.
     */
    DeviceTimeseriesMetadataMap get_timeseries_metadata();
};

class TsFileTreeWriter {
   public:
    /**
     * @brief Construct tree writer from target file.
     *
     * @param writer_file Target write file handle.
     * @param memory_threshold Buffered memory threshold before auto flush.
     */
    explicit TsFileTreeWriter(WriteFile* writer_file,
                              uint64_t memory_threshold = 128 * 1024 * 1024);
    /**
     * @brief Construct tree writer from restorable writer.
     *
     * @param restorable_writer Restored writer handle.
     * @param memory_threshold Buffered memory threshold before auto flush.
     */
    explicit TsFileTreeWriter(RestorableTsFileIOWriter* restorable_writer,
                              uint64_t memory_threshold = 128 * 1024 * 1024);

    /**
     * @brief Register one non-aligned timeseries for a device.
     *
     * @param device_id Device identifier.
     * @param schema Measurement schema pointer.
     * @return Returns 0 on success, non-zero on failure.
     */
    int register_timeseries(std::string& device_id, MeasurementSchema* schema);
    /**
     * @brief Register aligned timeseries list for a device.
     *
     * @param device_id Device identifier.
     * @param schemas Measurement schema pointer list.
     * @return Returns 0 on success, non-zero on failure.
     */
    int register_timeseries(std::string& device_id,
                            std::vector<MeasurementSchema*> schemas);
    /**
     * @brief Write one batch tablet.
     *
     * @param tablet Input tablet.
     * @return Returns 0 on success, non-zero on failure.
     */
    int write(const Tablet& tablet);
    /**
     * @brief Write one row record.
     *
     * @param record Input record.
     * @return Returns 0 on success, non-zero on failure.
     */
    int write(const TsRecord& record);
    /**
     * @brief Flush buffered data to storage.
     *
     * @return Returns 0 on success, non-zero on failure.
     */
    int flush();
    /**
     * @brief Close writer and release resources.
     *
     * @return Returns 0 on success, non-zero on failure.
     */
    int close();
};

}  // namespace storage
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