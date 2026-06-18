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

# 接口定义 - C


## Schema（模式定义）

```C++
typedef enum {
    TS_DATATYPE_BOOLEAN = 0,
    TS_DATATYPE_INT32 = 1,
    TS_DATATYPE_INT64 = 2,
    TS_DATATYPE_FLOAT = 3,
    TS_DATATYPE_DOUBLE = 4,
    TS_DATATYPE_TEXT = 5,
    TS_DATATYPE_TIMESTAMP = 8,
    TS_DATATYPE_DATE = 9,
    TS_DATATYPE_BLOB = 10,
    TS_DATATYPE_STRING = 11,
    TS_DATATYPE_INVALID = 255
} TSDataType;

// 值编码
typedef enum {
    TS_ENCODING_PLAIN = 0,
    TS_ENCODING_DICTIONARY = 1,
    TS_ENCODING_RLE = 2,
    TS_ENCODING_TS_2DIFF = 4,
    TS_ENCODING_GORILLA = 8,
    TS_ENCODING_ZIGZAG = 9,
    TS_ENCODING_SPRINTZ = 12,
    TS_ENCODING_INVALID = 255
} TSEncoding;

// 压缩类型，默认值为 LZ4。
typedef enum {
    TS_COMPRESSION_UNCOMPRESSED = 0,
    TS_COMPRESSION_SNAPPY = 1,
    TS_COMPRESSION_GZIP = 2,
    TS_COMPRESSION_LZO = 3,
    TS_COMPRESSION_LZ4 = 7,
    TS_COMPRESSION_INVALID = 255
} CompressionType;

typedef enum column_category {
    TAG = 0,
    FIELD = 1,
    ATTRIBUTE = 2,
    TIME = 3
} ColumnCategory;

// ColumnSchema：表示单个列的模式，包括列名、数据类型和分类。
// 列的编码/压缩遵循全局默认值（见下文“配置”）。
typedef struct column_schema {
    char* column_name;
    TSDataType data_type;
    ColumnCategory column_category;
} ColumnSchema;

// TableSchema：定义一个表的模式，包括表名和列模式数组。
typedef struct table_schema {
    char* table_name;
    ColumnSchema* column_schemas;
    int column_num;
} TableSchema;

// ResultSetMetaData：结果集的元数据，包括列名和数据类型。
typedef struct result_set_meta_data {
    char** column_names;
    TSDataType* data_types;
    int column_num;
} ResultSetMetaData;
```

> `ColumnSchema` 不携带编码/压缩——它们遵循全局默认值（见[配置](#配置编码与压缩)）。

## 写入接口

### 创建/关闭 TsFile 写入文件

```C++
/**
 * @brief 创建写入文件。
 *
 * @param pathname     目标文件路径。
 * @param err_code     [输出] RET_OK(0)，或 errno_define_c.h 中的错误码。
 *
 * @return WriteFile 成功返回有效句柄。
 *
 * @note 调用 free_write_file() 释放资源。
 * @note 调用 free_write_file() 前，需确保 TsFileWriter 已关闭。
 */

WriteFile write_file_new(const char* pathname, ERRNO* err_code);

void free_write_file(WriteFile* write_file);
```


### 创建/关闭 TsFile Writer

在创建 TsFile Writer 时，需要指定 WriteFile 和 TableSchema。你可以使用 tsfile_writer_new_with_memory_threshold 设置内存阈值。

```C++
/**
 * @brief 创建 TsFileWriter。
 *
 * @param file     写入目标文件。
 * @param schema   表结构定义。
 *                 - 由调用者负责释放。
 * @param err_code [输出] RET_OK(0)，或 errno_define_c.h 中的错误码。
 *
 * @return TsFileWriter 成功返回有效句柄，失败返回 NULL。
 *
 * @note 使用完毕后需调用 tsfile_writer_close() 释放资源。
 */
TsFileWriter tsfile_writer_new(WriteFile file, TableSchema* schema,
                               ERRNO* err_code);

/**
 * @brief 创建 TsFileWriter，带内存阈值限制。
 *
 * @param file     写入目标文件。
 * @param schema   表结构定义。
 *                 - 由调用者负责释放。
 * @param memory_threshold 超过该内存阈值时会自动刷盘。
 * @param err_code [输出] RET_OK(0)，或 errno_define_c.h 中的错误码。
 *
 * @return TsFileWriter 成功返回有效句柄，失败返回 NULL。
 *
 * @note 使用完毕后需调用 tsfile_writer_close() 释放资源。
 */
TsFileWriter tsfile_writer_new_with_memory_threshold(WriteFile file,
                                                     TableSchema* schema,
                                                     uint64_t memory_threshold,
                                                     ERRNO* err_code);

/**
 * @brief 释放 TsFileWriter 相关资源。
 *
 * @param writer [输入] 来自 tsfile_writer_new() 的 Writer 句柄。
 *               调用后该句柄失效，不可重复使用。
 * @return ERRNO - 成功返回 RET_OK(0)，否则返回错误码。
 */
ERRNO tsfile_writer_close(TsFileWriter writer);
```



### 创建/关闭/插入数据 Tablet

可以使用 Tablet 批量插入数据。数据写入后需释放对应内存资源。
```C
/**
 * @brief 创建用于批量写入的 Tablet。
 *
 * @param column_name_list [输入] 列名数组，长度为 column_num。
 * @param data_types [输入] 数据类型数组，长度为 column_num。
 * @param column_num [输入] 列数量，必须 ≥ 1。
 * @param max_rows [输入] 预分配的最大行数，必须 ≥ 1。
 * @return Tablet 成功返回有效句柄。
 * @note 使用完后需调用 free_tablet() 释放资源。
 */
Tablet tablet_new(char** column_name_list, TSDataType* data_types,
                  uint32_t column_num, uint32_t max_rows);
/**
 * @brief 获取当前 Tablet 中的行数。
 *
 * @param tablet [输入] 有效 Tablet 句柄。
 * @return uint32_t 当前行数 (0 到 max_rows-1)。
 */
uint32_t tablet_get_cur_row_size(Tablet tablet);

/**
 * @brief 向指定行设置时间戳。
 *
 * @param tablet [输入] 有效 Tablet。
 * @param row_index [输入] 目标行号 (0 ≤ index < max_rows)。
 * @param timestamp [输入] 时间戳 (int64_t 类型)。
 * @return ERRNO - 成功返回 RET_OK(0)，否则返回错误码。
 */
ERRNO tablet_add_timestamp(Tablet tablet, uint32_t row_index,
                           Timestamp timestamp);


/**
 * @brief 按列名为指定行添加字符串值。
 *
 * @param value [输入] 以 '\0' 结尾的字符串，调用者保留所有权。
 */
ERRNO tablet_add_value_by_name_string(Tablet tablet, uint32_t row_index,
                                      const char* column_name,
                                      const char* value);

 // 支持多种数据类型插入
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
 * @brief 按列索引为指定行添加值（支持多种数据类型）。
 *
 * @param value [输入] 字符串会被内部复制。
 */
ERRNO tablet_add_value_by_index_string(Tablet tablet, uint32_t row_index,
                                       uint32_t column_index,
                                       const char* value);


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





###  将 Tablet 写入 TsFile

```C
/**
 * @brief 将 Tablet 中的数据写入 TsFile。
 *
 * @param writer [输入] 有效的 TsFileWriter 句柄。
 * @param tablet [输入] 含数据的 Tablet，写入后可释放。
 */

ERRNO tsfile_writer_write(TsFileWriter writer, Tablet tablet);
```




## 配置（编码与压缩）

列按其数据类型的 **全局默认** 编码与压缩存储（`ColumnSchema` 不携带编解码设置）。
请在创建写入器 *之前* 用下列函数修改这些默认值。

每个 setter 成功返回 `RET_OK`（0），遇到不支持的数据类型/编码或压缩组合返回 `RET_NOT_SUPPORT`（40）。

```C
/* 按数据类型的默认值编码，以及默认压缩。 */
int     set_datatype_encoding(uint8_t data_type, uint8_t encoding);
int     set_global_compression(uint8_t compression);
uint8_t get_datatype_encoding(uint8_t data_type);
uint8_t get_global_compression();

/* 时间列（时间数据类型固定为 INT64）。 */
int     set_global_time_encoding(uint8_t encoding);
int     set_global_time_compression(uint8_t compression);
uint8_t get_global_time_encoding();
uint8_t get_global_time_compression();
```

允许的取值：编码方面，`BOOLEAN` 仅 `PLAIN`；`INT32`/`INT64`/`DATE` 为
`PLAIN`/`TS_2DIFF`/`GORILLA`/`ZIGZAG`/`RLE`/`SPRINTZ`；`FLOAT`/`DOUBLE` 为
`PLAIN`/`TS_2DIFF`/`GORILLA`/`SPRINTZ`；`STRING`/`TEXT` 为 `PLAIN`/`DICTIONARY`。
压缩可取 `UNCOMPRESSED`、`SNAPPY`、`GZIP`、`LZO`、`LZ4`。

```C
// 例如：所有列均以 LZ4 压缩写入
ERRNO code = set_global_compression(TS_COMPRESSION_LZ4);
if (code != RET_OK) { /* 处理不支持的取值 */ }
```

## 读取接口

###  TsFile Reader 创建/关闭

```C
/**
 * @brief 创建用于读取 TsFile 的 TsFileReader。
 *
 * @param pathname [输入] TsFile 路径。
 * @param err_code [输出] RET_OK(0) 或错误码。
 * @return 成功时返回有效句柄。
 *
 * @note 使用 tsfile_reader_close() 释放资源。
 */

TsFileReader tsfile_reader_new(const char* pathname, ERRNO* err_code);

/**
 * @brief 释放 TsFileReader 占用的资源。
 *
 * @note 调用后句柄失效，且由此句柄获得的结果集也失效。
 */
ERRNO tsfile_reader_close(TsFileReader reader);
```



###  查询表 / 获取下一行

```C

/**
 * @brief 在指定时间范围内，从指定表和列中查询数据。
 *
 * @param reader [输入] 通过 tsfile_reader_new() 获取的有效 TsFileReader 句柄。
 * @param table_name [输入] 目标表名，必须存在于 TsFile 中。
 * @param columns [输入] 要查询的列名数组。
 * @param column_num [输入] 列名数组中的列数。
 * @param start_time [输入] 起始时间戳。
 * @param end_time [输入] 结束时间戳，必须大于等于 start_time。
 * @param err_code [输出] 成功返回 RET_OK(0)，否则返回 errno_define_c.h 中定义的错误码。
 * @return ResultSet 查询结果集句柄。使用完成后必须通过 free_tsfile_result_set() 释放。
 */
ResultSet tsfile_query_table(TsFileReader reader, const char* table_name,
                             char** columns, uint32_t column_num,
                             Timestamp start_time, Timestamp end_time,
                             ERRNO* err_code);

/**
 * @brief 检查并获取结果集中的下一行数据。
 *
 * @param result_set [输入] 有效的 ResultSet 句柄。
 * @param error_code [输出] 成功返回 RET_OK(0)，否则返回 errno_define_c.h 中的错误码。
 * @return bool - true：存在下一行数据，false：已到达末尾或发生错误。
 */
bool tsfile_result_set_next(ResultSet result_set, ERRNO* error_code);

/**
 * @brief 释放结果集资源。
 *
 * @param result_set [输入] 有效的 ResultSet 句柄指针。
 */
void free_tsfile_result_set(ResultSet* result_set);
```



### 按标签过滤

**标签列（TAG）** 构成设备的唯一标识（联合主键）——正是它们的取值在一个表内
区分不同的设备。*标签过滤器* 把查询限定到标签取值满足条件的设备，从而只读取你关心的设备数据。
用 reader 构造一个过滤器，传给下文的表查询函数，用完再用 `tsfile_tag_filter_free()` 释放。

```C
// 标签过滤器的不透明句柄，用下面的函数构造。
typedef void* TagFilterHandle;

// 单列标签谓词的比较运算符。
typedef enum {
    TAG_FILTER_EQ = 0,          // 列 == 值
    TAG_FILTER_NEQ = 1,         // 列 != 值
    TAG_FILTER_LT = 2,          // 列 <  值
    TAG_FILTER_LTEQ = 3,        // 列 <= 值
    TAG_FILTER_GT = 4,          // 列 >  值
    TAG_FILTER_GTEQ = 5,        // 列 >= 值
    TAG_FILTER_REGEXP = 6,      // 列匹配正则 值
    TAG_FILTER_NOT_REGEXP = 7,  // 列不匹配正则 值
} TagFilterOp;

/**
 * @brief 创建单列标签谓词：`<column_name> <op> <value>`。
 *
 * @param reader      [输入] 有效的 TsFileReader 句柄。
 * @param table_name  [输入] 其 schema 定义了这些标签列的表名。
 * @param column_name [输入] 要过滤的标签列名。
 * @param value       [输入] 比较值（标签列为 STRING 类型）。
 * @param op          [输入] 比较运算符（TagFilterOp）。
 * @param err_code    [输出] 成功返回 RET_OK(0)，否则返回 errno_define_c.h 中的错误码。
 * @return 成功返回 TagFilterHandle，失败返回 NULL。
 */
TagFilterHandle tsfile_tag_filter_create(TsFileReader reader,
                                         const char* table_name,
                                         const char* column_name,
                                         const char* value, TagFilterOp op,
                                         ERRNO* err_code);

/**
 * @brief 创建范围谓词：lower <= 列 <= upper（is_not 为 true 表示 NOT BETWEEN）。
 */
TagFilterHandle tsfile_tag_filter_between(TsFileReader reader,
                                          const char* table_name,
                                          const char* column_name,
                                          const char* lower, const char* upper,
                                          bool is_not, ERRNO* err_code);

// 组合谓词。AND/OR/NOT 会接管其子节点的所有权，只需释放根节点。
TagFilterHandle tsfile_tag_filter_and(TagFilterHandle left, TagFilterHandle right);
TagFilterHandle tsfile_tag_filter_or(TagFilterHandle left, TagFilterHandle right);
TagFilterHandle tsfile_tag_filter_not(TagFilterHandle filter);

// 释放标签过滤器及其全部子节点。
void tsfile_tag_filter_free(TagFilterHandle filter);
```

### 带标签过滤、分页与分批的表查询

下列查询函数接受一个可选的 `tag_filter`（传 `NULL` 表示不过滤）和 `batch_size`
（`<= 0` 逐行返回；`> 0` 按该大小返回数据块）。

```C
/**
 * @brief 按行查询表，支持偏移量/行数限制下推与可选的标签过滤。
 *
 * @param reader           [输入] 有效的 TsFileReader 句柄。
 * @param table_name       [输入] 目标表名。
 * @param column_names     [输入] 要查询的列名数组。
 * @param column_names_len [输入] 要查询的列数量。
 * @param offset           [输入] 需要跳过的起始行数（>= 0）。
 * @param limit            [输入] 最多返回行数；< 0 表示不限制。
 * @param tag_filter       [输入] 标签谓词，NULL 表示不过滤。
 * @param batch_size       [输入] <= 0 逐行；> 0 数据块大小。
 * @param err_code         [输出] 成功返回 RET_OK(0)，否则返回错误码。
 * @return 成功返回 ResultSet 句柄，失败返回 NULL。用 free_tsfile_result_set() 释放。
 */
ResultSet tsfile_reader_query_table_by_row(
    TsFileReader reader, const char* table_name, char** column_names,
    int column_names_len, int offset, int limit, TagFilterHandle tag_filter,
    int batch_size, ERRNO* err_code);

/**
 * @brief 在时间范围内查询表，支持可选的标签过滤与分批。
 *
 * @param batch_size <= 0 逐行返回；> 0 返回该大小的 TsBlock。
 */
ResultSet tsfile_query_table_batch(TsFileReader reader, const char* table_name,
                                   char** columns, uint32_t column_num,
                                   Timestamp start_time, Timestamp end_time,
                                   TagFilterHandle tag_filter, int batch_size,
                                   ERRNO* err_code);

/**
 * @brief 带标签过滤的表查询（时间范围 + 标签谓词）。
 *
 * @param batch_size <= 0 逐行返回；> 0 返回该大小的 TsBlock。
 */
ResultSet tsfile_query_table_with_tag_filter(
    TsFileReader reader, const char* table_name, char** columns,
    uint32_t column_num, Timestamp start_time, Timestamp end_time,
    TagFilterHandle tag_filter, int batch_size, ERRNO* err_code);
```

示例——只读取 `region` 标签等于 `shanghai` 的设备的 `temperature`：

```C
ERRNO ec = RET_OK;
TagFilterHandle f = tsfile_tag_filter_create(
    reader, "weather", "region", "shanghai", TAG_FILTER_EQ, &ec);

char* cols[] = {"temperature"};
ResultSet rs = tsfile_reader_query_table_by_row(
    reader, "weather", cols, 1, /*offset*/ 0, /*limit*/ -1, f, /*batch*/ 0, &ec);

// ... 用 tsfile_result_set_next() 遍历 rs，然后释放：
free_tsfile_result_set(&rs);
tsfile_tag_filter_free(f);
```

### 从结果集中获取数据

```c
/**
 * @brief 判断当前行中指定列（按列名）对应的值是否为 NULL。
 *
 * @param result_set [输入] 当前处于有效行的 ResultSet（需在调用 next() 返回 true 之后）。
 * @param column_name [输入] 结果集模式中存在的列名。
 * @return bool - true：值为 NULL 或找不到该列，false：值有效。
 */
bool tsfile_result_set_is_null_by_name(ResultSet result_set,
                                       const char* column_name);

/**
 * @brief 判断当前行中指定列（按列索引）对应的值是否为 NULL。
 *
 * @param column_index [输入] 列的位置（1 <= 索引 <= 结果集列数）。
 * @return bool - true：值为 NULL 或索引超出范围，false：值有效。
 */
bool tsfile_result_set_is_null_by_index(ResultSet result_set,
                                        uint32_t column_index);

/**
 * @brief 根据列名获取当前行中的字符串值。
 *
 * @param result_set [输入] 有效的结果集句柄。
 * @param column_name [输入] 要获取的列名。
 * @return char* - 字符串指针。调用者在使用完后必须手动释放该指针。
 */
char* tsfile_result_set_get_value_by_name_string(ResultSet result_set,
                                                 const char* column_name);

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
 * @brief 根据列索引获取当前行中的字符串值。
 *
 * @param result_set [输入] 有效的结果集句柄。
 * @param column_index [输入] 要获取的列索引（1 <= 索引 <= 列总数）。
 * @return char* - 字符串指针。调用者在使用完后必须手动释放该指针。
 */
char* tsfile_result_set_get_value_by_index_string(ResultSet result_set,
                                                  uint32_t column_index);

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
 * @brief 获取描述结果集模式的元数据信息。
 *
 * @param result_set [输入] 有效的结果集句柄。
 * @return ResultSetMetaData 元数据句柄。调用者在使用后应释放该元数据。
 * @note 调用该函数前应检查 result_set 是否为 NULL，NULL 可能表示查询执行失败。
 */
ResultSetMetaData tsfile_result_set_get_metadata(ResultSet result_set);

/**
 * @brief 根据索引从元数据中获取列名。
 *
 * @param result_set_meta_data [输入] 有效的结果集元数据句柄。
 * @param column_index [输入] 列的位置（1 <= 索引 <= 列总数）。
 * @return const char* 只读字符串。如果索引无效则返回 NULL。
 */

char* tsfile_result_set_metadata_get_column_name(ResultSetMetaData result_set_meta_data,
                                                 uint32_t column_index);

/**
 * @brief 根据索引从元数据中获取列的数据类型。
 *
 * @param result_set_meta_data [输入] 有效的结果集元数据句柄。
 * @param column_index [输入] 列的位置（1 <= 索引 <= 列总数）。
 * @return TSDataType 若索引无效则返回 TS_DATATYPE_INVALID（255）。
 */
TSDataType tsfile_result_set_metadata_get_data_type(
    ResultSetMetaData result_set_meta_data, uint32_t column_index);

/**
 * @brief 获取结果集模式中的列总数。
 *
 * @param result_set_meta_data [输入] 有效的结果集元数据句柄。
 * @return int 结果集中列的数量。
 */
int tsfile_result_set_metadata_get_column_num(ResultSetMetaData result_set);
```



###  从 TsFile Reader 中获取表定义

```C
/**
 * @brief 获取 TsFile 中指定表的模式信息。
 *
 * @param reader [输入] 有效的读取器句柄。
 * @param table_name [输入] 目标表名，必须存在于 TsFile 中。
 * @return TableSchema 包含表及其列的信息。
 * @note 调用者应调用 free_table_schema 释放返回的 TableSchema 资源。
 */

TableSchema tsfile_reader_get_table_schema(TsFileReader reader,
                                           const char* table_name);
/**
 * @brief 获取 TsFile 中所有表的模式信息。
 *
 * @param size [输出] 返回的表模式数组中的元素数量。
 * @return TableSchema* 表模式数组指针。
 * @note 调用者必须对数组中的每个元素调用 free_table_schema()，
 *       并对整个数组指针调用 free() 进行释放。
 */

TableSchema* tsfile_reader_get_all_table_schemas(TsFileReader reader,
                                                 uint32_t* size);

/**
 * @brief 释放 TableSchema 占用的内存空间。
 *
 * @param schema [输入] 需要释放的表模式结构。
 */

void free_table_schema(TableSchema schema);
```

