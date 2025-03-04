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
# Interface Definitions

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
     * @param memory_threshold Optional parameter used to limit the memory size of objects.
     *                         If set to 0, no memory limit is enforced.
     */
    TsFileTableWriter(WriteFile* writer_file,
                      TableSchema* table_schema,
                      uint64_t memory_threshold = 0);
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

## Read Interface 
### Tsfile Reader
use to execute query in tsfile and return value by ResultSet.
```cpp
/**
 * @brief TsfileReader provides the ability to query all files with the suffix
 * .tsfile
 *
 * TsfileReader is designed to query .tsfile files, it accepts tree model
 * queries and table model queries, and supports querying metadata such as
 * TableSchema and TimeseriesSchema.
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
     * @brief query the tsfile by the path list, start time and end time
     * this method is used to query the tsfile by the tree model.
     *
     * @param [in] path_list the path list
     * @param [in] start_time the start time
     * @param [in] end_time the end time
     * @param [out] result_set the result set
     */
    int query(std::vector<std::string> &path_list, int64_t start_time,
              int64_t end_time, ResultSet *&result_set);
    /**
     * @brief query the tsfile by the table name, columns names, start time
     * and end time. this method is used to query the tsfile by the table
     * model.
     *
     * @param [in] table_name the table name
     * @param [in] columns_names the columns names
     * @param [in] start_time the start time
     * @param [in] end_time the end time
     * @param [out] result_set the result set
     */
    int query(const std::string &table_name,
              const std::vector<std::string> &columns_names, int64_t start_time,
              int64_t end_time, ResultSet *&result_set);
    /**
     * @brief destroy the result set, this method should be called after the
     * query is finished and result_set
     *
     * @param qds the result set
     */
    void destroy_query_data_set(ResultSet *qds);
    ResultSet *read_timeseries(
        const std::shared_ptr<IDeviceID> &device_id,
        const std::vector<std::string> &measurement_name);
    /**
     * @brief get all devices in the tsfile
     *
     * @param table_name the table name
     * @return std::vector<std::shared_ptr<IDeviceID>> the device id list
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices(
        std::string table_name);
    /**
     * @brief get the timeseries schema by the device id and measurement name
     *
     * @param [in] device_id the device id
     * @param [out] result std::vector<MeasurementSchema> the measurement schema
     * list
     * @return Returns 0 on success, or a non-zero error code on failure.
     */
    int get_timeseries_schema(std::shared_ptr<IDeviceID> device_id,
                              std::vector<MeasurementSchema> &result);
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