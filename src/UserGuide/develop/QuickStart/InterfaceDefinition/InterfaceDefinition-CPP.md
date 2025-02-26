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