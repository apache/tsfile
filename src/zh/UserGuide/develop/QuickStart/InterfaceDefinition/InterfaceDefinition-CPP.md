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
# 接口定义 - C++

## 写入接口

### TsFileTableWriter

用于写入 TsFile.

```cpp
namespace storage {
class RestorableTsFileIOWriter;

/**
 * @brief 支持按照指定表结构，将结构化表数据写入 TsFile 文件
 *
 * TsFileTableWriter 类用于将结构化数据（特别适用于时序数据）
 * 写入专为高效存储与查询优化的 TsFile 文件。
 * 使用者可定义待写入表的结构，按照该结构添加数据行，
 * 并将数据序列化为 TsFile。
 * 同时，该类提供写入过程中的内存使用限制能力。
 */
class TsFileTableWriter {
   public:
    /**
     * TsFileTableWriter 用于根据指定的表结构，将表数据写入目标文件，
     * 并可选择性地限制内存使用量。
     *
     * @param writer_file 表数据的目标写入文件，不能为空指针
     * @param table_schema 用于构建表结构，定义待写入表的 schema
     * @param memory_threshold 可选参数。当已写入数据量超过该阈值时，
     *                         数据将自动刷新到磁盘。默认值为 128MB
     */
    template <typename T>
    explicit TsFileTableWriter(storage::WriteFile* writer_file, T* table_schema,
                               uint64_t memory_threshold = 128 * 1024 * 1024) {
        static_assert(!std::is_same<T, std::nullptr_t>::value,
                      "table_schema cannot be nullptr");
        tsfile_writer_ = std::make_shared<TsFileWriter>();
        tsfile_writer_->init(writer_file);
        tsfile_writer_->set_generate_table_schema(false);

        // 执行深拷贝。源 TableSchema 对象可能分配在栈/堆上
        auto table_schema_ptr = std::make_shared<TableSchema>(*table_schema);
        error_number = tsfile_writer_->register_table(table_schema_ptr);
        exclusive_table_name_ = table_schema->get_table_name();
        common::g_config_value_.chunk_group_size_threshold_ = memory_threshold;
    }

    /**
     * 通过可恢复的 TsFileIOWriter 构建 TsFileTableWriter，
     * 支持在故障恢复后追加表数据。Schema 从已恢复的文件中读取，
     * 无需额外传入 TableSchema。
     *
     * @param restorable_writer 已恢复的 I/O 写入器；不能为空指针，
     *                          且必须以截断模式打开，保证 can_write() 返回 true
     * @param memory_threshold 可选的缓存数据内存阈值
     */
    explicit TsFileTableWriter(
        storage::RestorableTsFileIOWriter* restorable_writer,
        uint64_t memory_threshold = 128 * 1024 * 1024);

    /**
     * 向写入器注册表结构
     *
     * @param table_schema 待注册的表结构，不能为空指针
     * @return 成功返回 0，失败返回非零错误码
     */
    int register_table(const std::shared_ptr<TableSchema>& table_schema);
    /**
     * 根据表结构，将指定的 Tablet 数据写入目标文件
     *
     * @param tablet 包含待写入数据的 Tablet，不能为空指针
     * @return 成功返回 0，失败返回非零错误码
     */
    int write_table(Tablet& tablet) const;
    /**
     * 将所有缓存数据刷新到底层存储介质，确保所有数据都被持久化。
     * 该方法保证所有待写入数据都被落盘。
     *
     * @return 成功返回 0，失败返回非零错误码
     */
    int flush();
    /**
     * 关闭写入器并释放其占用的所有资源。
     * 调用该方法后，不应对当前实例执行任何后续操作。
     *
     * @return 成功返回 0，失败返回非零错误码
     */
    int close();
};

}  // namespace storage
```

### TableSchema

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
     * @return 成功时返回 0，失败时返回 errno_define.h 中的非零错误码。
     */
    int add_timestamp(uint32_t row_index, int64_t timestamp);

    /**
     * @brief 模板函数，用于向指定的行和列添加类型为 T 的值。
     *
     * @tparam T 要添加的值的类型, 如 int32_t, int64_t, double,
     *         float, bool, char*, std::tm.
     * @param row_index 要添加值的行索引，
     *                  必须小于最大行数。
     * @param schema_index 要添加的值对应的列模式索引。
     * @param val 要添加的值。
     * @return 成功时返回 0，失败时返回 errno_define.h 中的非零错误码。
     */

    template <typename T>
    int add_value(uint32_t row_index, uint32_t schema_index, T val);

    /**
     * @brief 模板函数，用于通过列名向指定的行和列添加类型为 T 的值。
     *
     * @tparam T 要添加的值的类型, 如 int32_t, int64_t, double,
     *         float, bool, char*, std::tm.
     * @param row_index 要添加值的行索引，
     *                  必须小于最大行数。
     * @param measurement_name 要添加值的列名，
     *                         必须与构造时提供的列名之一匹配。
     * @param val 要添加的值。
     * @return 成功时返回 0，失败时返回 errno_define.h 中的非零错误码。
     */
    template <typename T>
    int add_value(uint32_t row_index, const std::string &measurement_name, T val);
};
```

### RestorableTsFileIOWriter
> V2.3.0

```cpp
namespace storage {
/**
 * RestorableTsFileIOWriter 用于打开 TsFile 并对其进行可选的恢复操作
 * 继承自 TsFileIOWriter，支持在文件恢复后继续写入
 *
 * (1) 若 TsFile 正常关闭：has_crashed()=false，can_write()=false
 *
 * (2) 若 TsFile 不完整/程序崩溃：has_crashed()=true，
 * can_write()=true，写入器会截断损坏数据并允许继续写入
 *
 * 基于标准 C++11 实现，通过 RAII 和智能指针避免内存泄漏
 */
class RestorableTsFileIOWriter : public TsFileIOWriter {
   public:
    RestorableTsFileIOWriter();

    /**
     * 打开 TsFile 用于恢复/追加写入
     * 使用 O_RDWR|O_CREAT 模式，不使用 O_TRUNC，因此会保留文件原有内容
     *
     * @param file_path TsFile 文件路径
     * @param truncate_corrupted 若为 true，则截断损坏的数据；
     *        若为 false，则不截断（不完整文件保持原样）
     * @return 成功返回 E_OK，失败返回错误码
     */
    int open(const std::string& file_path, bool truncate_corrupted = true);

    /**
     * 关闭文件
     */
    void close();
};

}  // namespace storage
```


## 读取接口
### Tsfile Reader
```cpp
/**
 * @brief TsFileReader 提供查询所有后缀为 .tsfile 的文件的能力
 *
 * TsFileReader 专为查询 .tsfile 文件设计，支持树模型查询和表模型查询，
 * 同时支持查询表结构（TableSchema）、时间序列结构（TimeseriesSchema）等元数据。
 */
class TsFileReader {
   public:
    TsFileReader();
    /**
     * @brief 打开 tsfile 文件
     *
     * @param file_path 待打开的 tsfile 文件路径
     * @return 成功返回0，失败返回非零错误码
     */
    int open(const std::string& file_path);
    /**
     * @brief 关闭 tsfile 文件，该方法应在查询完成后调用
     *
     * @return 成功返回0，失败返回非零错误码
     */
    int close();
    /**
     * @brief 通过查询表达式查询 tsfile 文件，用户可自行构造查询表达式进行查询
     *
     * @param [in] qe 查询表达式
     * @param [out] ret_qds 结果集
     * @return 成功返回0，失败返回非零错误码
     */
    int query(storage::QueryExpression* qe, ResultSet*& ret_qds);
    /**
     * @brief 通过路径列表、起始时间和结束时间查询 tsfile 文件
     * 该方法用于树模型下的 tsfile 文件查询
     *
     * @param [in] path_list 路径列表
     * @param [in] start_time 起始时间
     * @param [in] end_time 结束时间
     * @param [out] result_set 结果集
     */
    int query(std::vector<std::string>& path_list, int64_t start_time,
              int64_t end_time, ResultSet*& result_set);
    /**
     * @brief 通过表名、列名、起始时间和结束时间查询 tsfile 文件
     * 该方法用于表模型下的 tsfile 文件查询
     *
     * @param [in] table_name 表名
     * @param [in] columns_names 列名列表
     * @param [in] start_time 起始时间
     * @param [in] end_time 结束时间
     * @param [out] result_set 结果集
     * @param [in] batch_size 小于等于0表示逐行返回模式，
     *             大于0表示按指定大小返回TsBlock数据块
     */
    int query(const std::string& table_name,
              const std::vector<std::string>& columns_names, int64_t start_time,
              int64_t end_time, ResultSet*& result_set, int batch_size = -1);

    /**
     * @brief 通过表名、列名、起始时间、结束时间和标签过滤条件查询 tsfile 文件
     * 该方法用于表模型下的 tsfile 文件查询
     *
     * @param [in] table_name 表名
     * @param [in] columns_names 列名列表
     * @param [in] start_time 起始时间
     * @param [in] end_time 结束时间
     * @param [in] tag_filter 标签过滤条件
     * @param [out] result_set 结果集
     */
    int query(const std::string& table_name,
              const std::vector<std::string>& columns_names, int64_t start_time,
              int64_t end_time, ResultSet*& result_set, Filter* tag_filter,
              int batch_size = 0);

    /**
     * @brief 基于偏移量和限制条数，按行查询树模型时间序列数据
     *
     * @param path_list  待查询的完整路径（设备.测量项）
     * @param offset     需要跳过的起始行数（>=0）
     * @param limit      最大返回行数，小于0表示无限制
     * @param[out] result_set  存储查询结果的结果集
     * @return 成功返回0，失败返回非零错误码
     */
    int queryByRow(std::vector<std::string>& path_list, int offset, int limit,
                   ResultSet*& result_set);

    /**
     * @brief 基于偏移量和限制条数下推，按行查询表模型数据
     *
     * 对于密集型设备（所有列行数相同），
     * 偏移量/限制条数会通过SSI下推至数据块/数据页级别，
     * 无需解码即可跳过整个数据块/数据页。
     * 对于稀疏型设备，偏移量/限制条数在行合并阶段生效。
     * 当设备总行数处于偏移量范围内时，可直接跳过整个设备。
     *
     * @param table_name     待查询的表名
     * @param column_names   待查询的列名
     * @param offset         需要跳过的起始行数（>=0）
     * @param limit          最大返回行数，小于0表示无限制
     * @param[out] result_set  存储查询结果的结果集
     * @param tag_filter     可选的标签过滤条件，用于按标签列过滤数据
     * @return 成功返回0，失败返回非零错误码
     */
    int queryByRow(const std::string& table_name,
                   const std::vector<std::string>& column_names, int offset,
                   int limit, ResultSet*& result_set,
                   Filter* tag_filter = nullptr, int batch_size = 0);

    /**
     * @brief 在树模型上执行表查询
     *
     * @param measurement_names 测量项名称列表
     * @param star_time 起始时间
     * @param end_time 结束时间
     * @param result_set 结果集
     */
    int query_table_on_tree(const std::vector<std::string>& measurement_names,
                            int64_t star_time, int64_t end_time,
                            ResultSet*& result_set);
    /**
     * @brief 销毁结果集，该方法应在查询完成、使用完结果集后调用
     *
     * @param qds 结果集对象
     */
    void destroy_query_data_set(ResultSet* qds);
    /**
     * @brief 根据设备ID和测量项名称读取时间序列数据
     *
     * @param device_id 设备ID
     * @param measurement_name 测量项名称列表
     * @return 结果集对象
     */
    ResultSet* read_timeseries(
        const std::shared_ptr<IDeviceID>& device_id,
        const std::vector<std::string>& measurement_name);
    /**
     * @brief 获取 tsfile 文件中的所有设备
     *
     * @param table_name 表名
     * @return 设备ID列表
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices(
        std::string table_name);

    /**
     * @brief 获取 tsfile 文件中的所有设备
     *
     * @return 设备ID列表
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_device_ids();

    /**
     * @brief 获取文件中的所有设备ID（与get_all_device_ids功能一致）
     *
     * @return 设备列表
     */
    std::vector<std::shared_ptr<IDeviceID>> get_all_devices();

    /**
     * @brief 根据设备ID和测量项名称获取时间序列结构
     *
     * @param [in] device_id 设备ID
     * @param [out] result 测量项结构列表
     * @return 成功返回0，失败返回非零错误码
     */
    int get_timeseries_schema(std::shared_ptr<IDeviceID> device_id,
                              std::vector<MeasurementSchema>& result);

    /**
     * @brief 获取指定设备的时间序列元数据
     *
     * 仅文件中存在的设备会被包含在结果中
     * 若设备ID列表为空，返回空映射表
     *
     * @param device_ids 待查询的设备列表
     * @return 映射关系：设备ID -> 时间序列元数据列表（仅包含存在的数据）
     */
    DeviceTimeseriesMetadataMap get_timeseries_metadata(
        const std::vector<std::shared_ptr<IDeviceID>>& device_ids);

    /**
     * @brief 获取文件中所有设备的时间序列元数据
     *
     * @return 映射关系：设备ID -> 时间序列元数据列表
     */
    DeviceTimeseriesMetadataMap get_timeseries_metadata();

    /**
     * @brief 根据表名获取表结构
     *
     * @param table_name 表名
     * @return 表结构智能指针
     */
    std::shared_ptr<TableSchema> get_table_schema(
        const std::string& table_name);
    /**
     * @brief 获取 tsfile 文件中的所有表结构
     *
     * @return 表结构列表
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
     * @return 成功时返回 0，失败时返回 errno_define.h 中的非零错误码。
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
### Filter
#### TagFilterBuilder
用于构建基于Tag的过滤器以查询数据
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

    // 逻辑操作
    static Filter* and_filter(Filter* left, Filter* right);
    static Filter* or_filter(Filter* left, Filter* right);
    static Filter* not_filter(Filter* filter);
};
```