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

# LabVIEW 批量 I/O 现代化设计规格

## 概述

将 `Young-Leo/tsfile:labview` 中的 LabVIEW C ABI 迁移到最新 Apache
TsFile `develop` 分支，同时不改写、不移动原始 `labview` 分支的提交线。
所有现有 LabVIEW 导出函数及其 row-major 输入布局保持兼容；数值块写入在
DLL 内部进行优化，并新增向后兼容的批量查询与数值块读取 API。

所有工作均在新分支 `feat/labview-bulk-io` 上进行。该分支创建自
`origin/develop` 的 `2c2d416e2764460cdee8aabbeb80b1afbe639e1c`。
旧 LabVIEW 提交 `4bf0be2` 和 `e5cde40` 只作为迁移参考，不在本次工作中
更新、rebase、force-push 或以其他方式修改任何远端分支。

## 目标

- 将 LabVIEW Wrapper 源码、构建集成、文档和源码测试迁移到当前 TsFile
  C++ 实现。
- 保留所有现有 LabVIEW 导出符号、参数类型、状态码约定、U64 Handle 和
  row-major 数值输入约定。
- 从稳定状态下的数值块写入路径中消除每批 Tablet 分配和逐单元格
  C Wrapper 调用。
- 新增适合 LabVIEW 的批量读取路径：一次 DLL 调用消费一个 TsFile
  `TsBlock`，并填充调用方拥有的扁平数组。
- 使用标准 TsFile Reader 和旧版 LabVIEW 逐行 API 验证文件格式兼容性与
  读取结果等价性。
- 保留明确的性能测试，分别测量写入数据整理、编码、逐行读取、批量读取、
  flush 和 close。

## 非目标

- 修改现有 LabVIEW ABI，或把现有接口改成 column-major 输入。
- 在首个版本支持混合类型或字符串列的批量读取。
- 向 LabVIEW 暴露 Arrow 结构体或内存分配器所有权。
- 让同一个 Writer 或 ResultSet Handle 支持并发使用与并发关闭。
- 将 `writer_close` 改造成异步接口。
- 在 macOS 上构建或替换仓库中的 Windows x86/x64 DLL。
- 更新、rebase、force-push 或修改 `Young-Leo/tsfile:labview`。

## 分支迁移策略

1. 将 `ly/labview` 获取为只读 remote-tracking reference。
2. 保持 `feat/labview-bulk-io` 基于最新 `origin/develop`，不向旧分支合并
   `develop`。
3. 把 `4bf0be2` 和 `e5cde40` 中的逻辑修改重新移植为易于审查的提交。
   对已经移动或重构的 Reader/File 代码，按照新的上游结构解决冲突，
   不恢复已废弃的旧文件。
4. 以源码测试目标的形式恢复原有 smoke、read-only、block-write 和 codec
   测试程序。正确性测试接入 CTest，防止正常构建过程中被静默跳过。
5. 不复制旧 DLL 或可执行文件并将其冒充为新源码的构建产物。源码稳定后，
   再通过独立 Windows 构建刷新二进制分发包。

迁移前，干净的 `origin/develop` 必须能够成功构建。在当前开发机器上，
`./mvnw -P with-cpp clean verify` 已完成：952 个测试通过、0 个失败，其余
报告项均为上游预期的 skipped 或 disabled 测试。

## 兼容性约定

现有数值块写入函数的签名和行为保持不变：

```c
LV_Status lv_tsfile_write_block_i32(LV_Handle writer, const int64_t* ts,
                                    const int32_t* data, int32_t nrows,
                                    int32_t ncols);
LV_Status lv_tsfile_write_block_f32(LV_Handle writer, const int64_t* ts,
                                    const float* data, int32_t nrows,
                                    int32_t ncols);
LV_Status lv_tsfile_write_block_f64(LV_Handle writer, const int64_t* ts,
                                    const double* data, int32_t nrows,
                                    int32_t ncols);
```

`ts` 包含 `nrows` 个时间戳。`data` 包含 `nrows * ncols` 个同类型数值，
采用 row-major 布局：单元格 `(row, col)` 位于
`data[row * ncols + col]`。函数只在调用期间借用这两个缓冲区，不保存
LabVIEW 所拥有的指针。

所有旧版查询函数、`rs_next`、标量 Getter、字符串 Getter 和释放函数继续
保留。新读取接口只做增量扩展。

## 优化后的写入路径

每个 `WriterCtx` 拥有一个可复用数值 Tablet，以及一个可复用、带类型的
column-major 暂存区。首次数值块写入时分配这些缓冲区。后续写入的
`nrows` 不超过当前容量时直接复用；只有容量不足时才重新创建。

每次写入依次执行：

1. 校验 Writer Handle、指针、维度、乘法溢出、Schema 列数和同类型数值
   约束。
2. 重置缓存的 Tablet，但不释放其底层存储。
3. 通过一次 `set_timestamps` 批量复制所有时间戳。
4. 将调用方的 row-major 数值拆分到可复用 column-major 暂存区。
5. 每一列只调用一次 `set_column_values`，并设置为全部非空。
6. 通过 `tsfile_writer_write` 提交 Tablet。

C Wrapper 增加少量 Tablet 批量辅助函数，分别用于 reset、时间戳复制和
定长列复制。这些函数委托给现有的 `Tablet::reset`、
`Tablet::set_timestamps` 和 `Tablet::set_column_values`，因此 LabVIEW
模块不依赖 Tablet 私有字段。

该方案明确接受每个数值进行一次 row-major 到 column-major 的拆分，并在
设置 Tablet 列时进行一次连续复制。若要消除后一次复制，就必须把公开
LabVIEW 布局改为 column-major，或让 Wrapper 直接耦合 Tablet 私有字段；
二者都不符合兼容性和分层约束。

Writer 关闭时释放缓存 Tablet 和暂存区。错误路径必须释放未完成构造的
存储；除非底层 TsFile Writer 报告不可恢复错误，否则不能让仍然打开的
Writer 失效。

## 批量读取 API

批量查询创建由现有 TsFile `TsBlock` Reader 支撑的 batch-mode ResultSet：

```c
LV_Status lv_tsfile_query_table_batch(
    LV_Handle reader, const char* table_name,
    const char* columns_newline_separated, int64_t start_time,
    int64_t end_time, int32_t batch_rows, LV_Handle* out_result_set);
```

`batch_rows` 必须大于零。查询列沿用现有换行符分隔格式，用于指定数值列；
返回元数据中第 0 列仍然是时间戳。

新增三个同类型数值块读取函数：

```c
LV_Status lv_tsfile_rs_read_block_i32(
    LV_Handle rs, int64_t* out_ts, int32_t* out_data,
    uint8_t* out_is_null, int32_t capacity_rows, int32_t ncols,
    int32_t* out_rows);
LV_Status lv_tsfile_rs_read_block_f32(
    LV_Handle rs, int64_t* out_ts, float* out_data,
    uint8_t* out_is_null, int32_t capacity_rows, int32_t ncols,
    int32_t* out_rows);
LV_Status lv_tsfile_rs_read_block_f64(
    LV_Handle rs, int64_t* out_ts, double* out_data,
    uint8_t* out_is_null, int32_t capacity_rows, int32_t ncols,
    int32_t* out_rows);
```

调用方提供可容纳 `capacity_rows` 个时间戳，以及
`capacity_rows * ncols` 个数值的空间。`out_data` 不包含时间戳，采用
row-major 布局。

当 `out_is_null` 非空时，它与 `out_data` 具有相同的 row-major 元素数量：
`0` 表示有效值，`1` 表示 null。数值 null 在 `out_data` 中写为零，从而
保证输出确定性。如果调用方不需要 null 信息，可以传入空的
`out_is_null`。

`capacity_rows` 必须至少等于创建查询时配置的 `batch_rows`。Wrapper 在
获取 `TsBlock` 之前完成该校验，保证容量错误不会消耗尚未读取的数据。
成功时，`out_rows` 返回实际复制的行数；读取结束使用
`E_OK + *out_rows == 0` 表示。

ResultSet 会记录自身是 row mode 还是 batch mode。标量行函数对 batch
Handle 返回 `E_INVALID_ARG`，块函数对 row Handle 返回 `E_INVALID_ARG`。
查询中的数值列必须全部匹配所调用块函数的类型。混合类型或字符串查询继续
使用旧版逐行 API。

## 内部读取路径

LabVIEW Wrapper 使用现有 batch query 和 `TsBlock` 能力，但不把数据转换
为 Arrow。C Wrapper 增加一个小型数值块复制入口，一次将一个 `TsBlock`
复制到调用方缓冲区。它负责校验 batch mode、时间戳类型、数值列数、同类型
约束、输出容量和长度运算。

对于全部有效的定长列，复制循环直接读取连续的列向量并写入 row-major
目标。对于包含 null 的列，循环读取列 bitmap/iterator，为 null 写入零，
同时填充可选的逐单元格 null 数组。这样既消除了逐行的宿主/DLL 往返，也
避免每批创建 Arrow Schema/Array。

## 配置正确性

编码和压缩配置会直接影响写入性能。迁移后的 Wrapper 必须先初始化 TsFile
配置，再应用 LabVIEW 的压缩或编码 Setter。测试必须证明：第一次打开
Writer 之前设置的选项不会在 Writer 初始化时被恢复成默认值。Benchmark
必须报告实际生效的编码和压缩方式，而不能只假设请求的设置已经生效。

## 错误处理与所有权

- 所有可能失败的 LabVIEW 函数返回 `LV_Status`，C++ 异常不得跨越 C ABI。
- 无效 Handle、必填空指针、非正维度、row/batch mode 不匹配和列数不匹配
  返回 `E_INVALID_ARG`。
- 数据类型不匹配返回现有类型错误状态。
- 容量不足和长度运算溢出返回相应的范围/参数错误，并且不消耗下一批数据。
- 输出 Handle 和 `out_rows` 在入口处先置零，失败时不会暴露旧值。
- 宿主缓冲区只在函数调用期间借用。
- Writer、Reader、Tablet 和 ResultSet 资源继续由 U64 Registry Handle
  拥有，并通过对应 close/free 函数释放。
- 同一 Handle 上的调用，特别是正常调用与 close，必须由调用方串行执行；
  修改 Registry 并发语义不在本次范围内。

## 测试策略

实现采用测试驱动开发。每项行为都必须先建立失败测试，再修改生产代码。

迁移测试：

- 在当前 `develop` 上构建并加载 LabVIEW Shim。
- 执行原有 smoke、read-only 和 block-write 用例。
- 使用标准 TsFile Reader 读取 Wrapper 写出的文件。
- 如果存在可移植 fixture，使用迁移后的 Wrapper 读取旧版 Wrapper 文件。

写入测试：

- 验证 i32、f32、f64 在单次和多次 row-major 写入下的结果。
- 验证更小和等容量批次能够复用，超过容量时能够安全扩容。
- close 并重新打开后，精确校验时间戳和值。
- 验证无效 Handle、空指针、零/负维度、乘法溢出、列数不匹配和类型不匹配。
- 通过测试专用分配计数或缓冲区身份，证明相同容量下的稳定写入不会重新创建
  Tablet。

读取测试：

- 验证 i32、f32、f64 的多批读取和 EOF。
- 将批量输出与旧 `rs_next`、标量 Getter 结果逐值比较。
- 验证空结果、最后一个不足批次、null 单元格、确定性零填充、可选 null 输出、
  类型不匹配、mode 不匹配、容量不足和非法维度。
- 验证容量被拒绝后，下一批数据没有被消费。

仓库级验证：

- 迭代时运行聚焦的 LabVIEW/C Wrapper 测试。
- 运行格式和 License 检查。
- 完成前运行 `./mvnw -P with-cpp clean verify`。
- 发布 DLL 前分别执行 Windows x64 和 x86 构建及测试。

## 性能测试与验收

性能 Benchmark 只提供信息，不作为基于耗时的 CI 测试。Benchmark 固定
数据、Codec、行数、列数和预热方式，并通过多次运行的中位数报告结果。
Tablet 数据整理与 Codec/I/O 分开计时，读取解码与宿主缓冲区转换也分开
计时。

对比对象包括：

- 旧版逐单元格 Tablet 构造。
- `e5cde40` 中的旧 Block 写入实现。
- 可复用、批量化的 Block 写入实现。
- 旧版逐行/标量读取。
- 新版批量读取。
- 单独计时的 `flush` 和 `writer_close`。

正确性是硬性验收条件。在当前开发机器上，目标是：10,000 行 × 9 列写入
数据整理阶段相对 `e5cde40` 至少加速 2 倍；150,000 行 × 9 列通过批量
读取 API 相对旧 LabVIEW 逐行 API 至少加速 3 倍。如果 Codec 或存储耗时
主导端到端结果，则优先评估分阶段计时，而不是总耗时比例。发现任何性能
回退都必须先调查原因，再决定是否接受。

## 文档与交付

Wrapper README 和 LabVIEW 手册将说明：

- 现有连续写入用法和保持不变的 row-major 布局。
- 推荐批次大小及 Writer 生命周期。
- 新 batch query 和 block read 函数签名。
- LabVIEW Call Library Function Node 参数映射。
- 缓冲区尺寸、EOF、null、所有权和错误语义。
- 采集和文件轮转场景下的 Producer/Consumer 建议。
- 使用真实设备数据进行 Codec Benchmark 的建议。

完成后的分支包含源码、测试、Benchmark 和文档。远端发布以及刷新 Windows
二进制分发包属于后续独立操作，必须获得明确授权后执行。
