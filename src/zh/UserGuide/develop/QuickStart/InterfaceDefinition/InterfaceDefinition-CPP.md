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
# 接口定义 - Cpp

## 写入接口

### TsFileTableWriter

用于写入 TsFile.

```cpp
/**
 * @brief 用于将结构化表格数据写入具有指定模式的 TsFile。
 *
 * TsFileTableWriter 类被设计用于写入结构化数据，特别适合时序数据，
 * 数据将被写入一种为高效存储与检索优化的文件格式（即 TsFile）。该类允许用户定义
 * 所需写入表的模式，按照该模式添加数据行，并将这些数据序列化写入 TsFile。
 * 此外，还提供了在写入过程中限制内存使用的选项。
 */
class TsFileTableWriter {
   public:
    /**
     * TsFileTableWriter 用于将表格数据写入具有指定模式的目标文件，
     * 可选地限制内存使用。
     *
     * @param writer_file 要写入表数据的目标文件。不能为空。
     * @param table_schema 用于构造表结构，定义正在写入表的模式。
     * @param memory_threshold 可选参数。当写入数据的大小超过该值时，
     * 数据将自动刷新到磁盘。默认值为 128MB。
     */

    TsFileTableWriter(WriteFile* writer_file,
                      TableSchema* table_schema,
                      uint64_t memory_threshold = 128 * 1024 * 1024);
    ~TsFileTableWriter();
    /**
     * 将给定的 Tablet 数据按照表的模式写入目标文件。
     *
     * @param tablet 包含待写入数据的 Tablet。不能为空。
     * @return 成功时返回 0，失败时返回非零错误码。
     */

    int write_table(const Tablet& tablet);
    /**
     * 将所有缓冲数据刷新到底层存储介质，确保所有数据都已写出。
     * 此方法确保所有未完成的写入操作被持久化。
     *
     * @return 成功时返回 0，失败时返回非零错误码。
     */

    int flush();
    /**
     * 关闭写入器并释放其占用的所有资源。
     * 调用此方法后，不应再对该实例执行任何操作。
     *
     * @return 成功时返回 0，失败时返回非零错误码。
     */

    int close();
};
```

### 表模式

描述表模式（schema）的数据结构。

```cpp
/**
* @brief 表示整个表的模式信息。
*
* 此类包含描述特定表结构所需的元数据，
* 包括表名以及所有列的模式信息。
*/
class TableSchema {
    public:
    /**
     * 使用给定的表名和列模式构造一个 TableSchema 对象。
     *
     * @param table_name 表的名称，必须为非空字符串。
     *                   此名称用于在系统中标识该表。
     * @param column_schemas 一个包含 ColumnSchema 对象的向量。
     *                       每个 ColumnSchema 定义表中一列的模式。
     */
    TableSchema(const std::string& table_name,
                const std::vector<ColumnSchema>& column_schemas);
};


/**
* @brief 表示单个列的模式信息。
*
* 此结构体包含描述特定列存储方式所需的元数据，
* 包括列名、数据类型和列类别。
*/
struct ColumnSchema {
    std::string column_name_;
    common::TSDataType data_type_;
    ColumnCategory column_category_;

   /**
     * @brief 使用给定参数构造一个 ColumnSchema 对象。
     *
     * @param column_name 列的名称，必须为非空字符串。
     *                    此名称用于在表中标识该列。
     * @param data_type 该列的数据类型，例如 INT32、DOUBLE、TEXT 等。
     *                  数据类型决定了数据的存储与解释方式。
     * @param column_category 列的类别，用于标识其在模式中的角色或类型，
     *                        例如 FIELD（字段）、TAG（标签）。
     *                        如果未指定，默认为 ColumnCategory::FIELD。
     * @note 调用者有责任确保 `column_name` 非空。
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


```cpp
/**
 * @brief 表示用于插入到表中的数据行集合及其相关元数据。
 *
 * 此类用于管理和组织将要插入特定目标表的数据。
 * 它负责存储时间戳和值，以及相关的元数据，如列名和数据类型。
 */
class Tablet {
public:
    /**
     * @brief 使用给定参数构造一个 Tablet 对象。
     *
     * @param column_names 一个包含该 Tablet 中列名的向量。
     *                     每个名称对应目标表中的一列。
     * @param data_types 一个包含每列数据类型的向量。
     *                   这些类型必须与目标表的模式相匹配。
     * @param max_rows 该 Tablet 可容纳的最大行数，默认为 DEFAULT_MAX_ROWS。
     */
    Tablet(const std::vector<std::string> &column_names,
           const std::vector<common::TSDataType> &data_types,
           int max_rows = DEFAULT_MAX_ROWS);

    /**
     * @brief 向指定行添加时间戳。
     *
     * @param row_index 要添加时间戳的行索引，
     *                  必须小于最大行数。
     * @param timestamp 要添加的时间戳值。
     * @return 成功时返回 0，失败时返回非零错误码。
     */
    int add_timestamp(uint32_t row_index, int64_t timestamp);

    /**
     * @brief 模板函数，用于向指定的行和列添加类型为 T 的值。
     *
     * @tparam T 要添加的值的类型。
     * @param row_index 要添加值的行索引，
     *                  必须小于最大行数。
     * @param schema_index 要添加的值对应的列模式索引。
     * @param val 要添加的值。
     * @return 成功时返回 0，失败时返回非零错误码。
     */

    template <typename T>
    int add_value(uint32_t row_index, uint32_t schema_index, T val);

    /**
     * @brief 模板函数，用于通过列名向指定的行和列添加类型为 T 的值。
     *
     * @tparam T 要添加的值的类型。
     * @param row_index 要添加值的行索引，
     *                  必须小于最大行数。
     * @param measurement_name 要添加值的列名，
     *                         必须与构造时提供的列名之一匹配。
     * @param val 要添加的值。
     * @return 成功时返回 0，失败时返回非零错误码。
     */
    template <typename T>
    int add_value(uint32_t row_index, const std::string &measurement_name, T val);
};
```

## 读接口
### Tsfile Reader
```cpp
/**
 * @brief TsFileReader 提供了查询所有以 .tsfile 为后缀的文件的能力。
 *
 * TsFileReader 旨在用于查询 .tsfile 文件，它支持树模型查询和表模型查询，
 * 并支持查询元数据信息，如 TableSchema 和 TimeseriesSchema。
 */

class TsFileReader {
   public:
    TsFileReader();
    ~TsFileReader();
    /**
     * @brief 打开 tsfile 文件。
     *
     * @param file_path 要打开的 tsfile 文件路径。
     * @return 成功时返回 0，失败时返回非零错误码。
     */

    int open(const std::string &file_path);
    /**
     * @brief 关闭 tsfile，查询完成后应调用此方法。
     *
     * @return 成功时返回 0，失败时返回非零错误码。
     */
    int close();
    /**
     * @brief 通过查询表达式对 tsfile 进行查询，用户可以自行构造查询表达式来查询 tsfile。
     *
     * @param [in] qe 查询表达式。
     * @param [out] ret_qds 查询结果集。
     * @return 成功时返回 0，失败时返回非零错误码。
     */
    int query(storage::QueryExpression *qe, ResultSet *&ret_qds);
    /**
     * @brief 通过路径列表、起始时间和结束时间查询 tsfile，
     * 本方法使用树模型进行查询。
     *
     * @param [in] path_list 路径列表。
     * @param [in] start_time 起始时间。
     * @param [in] end_time 结束时间。
     * @param [out] result_set 查询结果集。
     */
    int query(std::vector<std::string> &path_list, int64_t start_time,
              int64_t end_time, ResultSet *&result_set);
    /**
     * @brief 通过表名、列名、起始时间和结束时间查询 tsfile，
     * 本方法使用表模型进行查询。
     *
     * @param [in] table_name 表名。
     * @param [in] columns_names 列名列表。
     * @param [in] start_time 起始时间。
     * @param [in] end_time 结束时间。
     * @param [out] result_set 查询结果集。
     */
    int query(const std::string &table_name,
              const std::vector<std::string> &columns_names, int64_t start_time,
              int64_t end_time, ResultSet *&result_set);
    /**
     * @brief 销毁结果集，该方法应在查询完成并使用完 result_set 后调用。
     *
     * @param qds 查询结果集。
     */
    void destroy_query_data_set(ResultSet *qds);
    ResultSet *read_timeseries(
        const std::shared_ptr<IDeviceID> &device_id,
        const std::vector<std::string> &measurement_name);
    /**
     * @brief 获取 tsfile 中的所有设备。
     *
     * @param table_name 表名。
     * @return std::vector<std::shared_ptr<IDeviceID>> 设备 ID 列表。
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices(
        std::string table_name);
    /**
     * @brief 根据设备 ID 和测量名称获取时间序列模式信息。
     *
     * @param [in] device_id 设备 ID。
     * @param [out] result std::vector<MeasurementSchema> 测量模式列表。
     * @return 成功时返回 0，失败时返回非零错误码。
     */
    int get_timeseries_schema(std::shared_ptr<IDeviceID> device_id,
                              std::vector<MeasurementSchema> &result);
    /**
     * @brief 根据表名获取表的模式信息。
     *
     * @param table_name 表名。
     * @return std::shared_ptr<TableSchema> 表的模式信息。
     */
    std::shared_ptr<TableSchema> get_table_schema(
        const std::string &table_name);
    /**
     * @brief 获取 tsfile 中所有表的模式信息。
     *
     * @return std::vector<std::shared_ptr<TableSchema>> 表模式信息列表。
     */
    std::vector<std::shared_ptr<TableSchema>> get_all_table_schemas();
};
```
### ResultSet
```cpp
/**
 * @brief ResultSet 是 TsFileReader 的查询结果集，用于访问查询结果。
 *
 * ResultSet 是一个虚类，使用时应转换为相应的实现类。
 * @note 当使用树模型且过滤器是全局时间过滤器时，应转换为 QDSWithoutTimeGenerator。
 * @note 当使用树模型但过滤器不是全局时间过滤器时，应转换为 QDSWithTimeGenerator。
 * @note 如果查询使用的是表模型，则应转换为 TableResultSet。
 */
class ResultSet {
   public:
    ResultSet() {}
    virtual ~ResultSet() {}

    /**
     * @brief 获取结果集的下一行。
     *
     * @param[out] has_next 布尔值，指示是否还有下一行。
     * @return 成功时返回 0，失败时返回非零错误码。
     */
    virtual int next(bool& has_next) = 0;

    /**
     * @brief 根据列名检查该列的值是否为 null。
     *
     * @param column_name 列名。
     * @return 如果值为 null 返回 true，否则返回 false。
     */
    virtual bool is_null(const std::string& column_name) = 0;

    /**
     * @brief 根据列索引检查该列的值是否为 null。
     *
     * @param column_index 从 1 开始的列索引。
     * @return 如果值为 null 返回 true，否则返回 false。
     */
    virtual bool is_null(uint32_t column_index) = 0;

    /**
     * @brief 根据列名获取该列的值。
     *
     * @param column_name 列名。
     * @return 该列的值。
     */
    template <typename T>
    T get_value(const std::string& column_name);

    /**
     * @brief 根据列索引获取该列的值。
     *
     * @param column_index 从 1 开始的列索引。
     * @return 该列的值。
     */
    template <typename T>
    T get_value(uint32_t column_index);

    /**
     * @brief 获取当前行的 RowRecord。
     *
     * @return 当前行的 RowRecord。
     */
    virtual RowRecord* get_row_record() = 0;

    /**
     * @brief 获取结果集的元数据。
     *
     * @return std::shared_ptr<ResultSetMetadata> 结果集的元数据。
     */
    virtual std::shared_ptr<ResultSetMetadata> get_metadata() = 0;

    /**
     * @brief 关闭结果集。
     *
     * @note 当不再需要结果集时应调用此方法。
     */
    virtual void close() = 0;
};

```
### ResultMeta
```cpp
/**
 * @brief 结果集的元数据信息。
 *
 * 用户可以通过 ResultSetMetadata 获取结果集的元数据，
 * 包括所有列名和数据类型。当用户使用表模型时，第一列默认是时间列。
 */
class ResultSetMetadata {
   public:
    /**
     * @brief ResultSetMetadata 的构造函数。
     *
     * @param column_names 列名列表。
     * @param column_types 列类型列表。
     */
    ResultSetMetadata(const std::vector<std::string>& column_names,
                      const std::vector<common::TSDataType>& column_types);

    /**
     * @brief 获取指定索引的列类型。
     *
     * @param column_index 从 1 开始的列索引。
     * @return 对应的列类型。
     */
    common::TSDataType get_column_type(uint32_t column_index);

    /**
     * @brief 获取指定索引的列名。
     *
     * @param column_index 从 1 开始的列索引。
     * @return 对应的列名。
     */
    std::string get_column_name(uint32_t column_index);

    /**
     * @brief 获取列的总数量。
     *
     * @return 列的数量（uint32_t 类型）。
     */
    uint32_t get_column_count();
};

```