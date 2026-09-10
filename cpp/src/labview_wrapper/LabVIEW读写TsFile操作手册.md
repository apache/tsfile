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

# LabVIEW 读写 TsFile 操作手册

## 1. 适用范围

本 Wrapper 面向 LabVIEW Call Library Function Node（CLFN），对外只使用
固定宽度整数、C 字符串、Array Data Pointer 和 U64 不透明句柄。旧版逐行
接口继续兼容；新增的块写入和块读取适合 INT32、FLOAT、DOUBLE 同类型通道。
混合类型、TAG、字符串或布尔列仍使用 Tablet Builder 与逐行 ResultSet API。

LabVIEW 进程与动态库必须同位数：32 位 LabVIEW 使用 x86 DLL，64 位
LabVIEW 使用 x64 DLL。`LV_Handle` 即使在 32 位 LabVIEW 中也配置为 U64。

## 2. CLFN 通用映射

| C 类型 | LabVIEW CLFN 配置 |
|---|---|
| `LV_Status` / `int32_t` | Signed 32-bit Integer，by value |
| `LV_Handle` / `uint64_t` | Unsigned 64-bit Integer；输出时 Pointer to Value |
| `const char*` | String，C String Pointer |
| `const int64_t*` / `int64_t*` | 1D I64 Array，Array Data Pointer |
| `const int32_t*` / `int32_t*` | 1D I32 Array，Array Data Pointer |
| `const float*` / `float*` | 1D SGL Array，Array Data Pointer |
| `const double*` / `double*` | 1D DBL Array，Array Data Pointer |
| `uint8_t*` | 1D U8 Array，Array Data Pointer；可选参数可传空指针 |
| `int32_t* out_rows` | Signed 32-bit Integer，Pointer to Value |

所有可能失败的调用都检查返回状态：`0` 表示成功，其他值来自 TsFile
`ERRNO`。输入数组只在一次调用期间被借用，DLL 不保存 LabVIEW 数组地址。

## 3. 连续块写入

推荐初始化顺序：

1. `lv_tsfile_set_global_compression` 和
   `lv_tsfile_set_datatype_encoding`（如果需要覆盖默认 Codec）。
2. `lv_tsfile_schema_builder_new` 创建 Schema Builder。
3. 每列调用 `lv_tsfile_schema_builder_add_column`。
4. `lv_tsfile_writer_open` 获取 Writer U64 Handle。
5. 释放 Schema Builder。
6. 每个采集块调用匹配类型的 `lv_tsfile_write_block_*`。
7. 可在受控时机调用 `lv_tsfile_writer_flush`；它同步执行且 Writer 仍可继续写。
8. 文件轮转或停止时选择同步 `lv_tsfile_writer_close`，或使用下一节的两阶段
   异步关闭；随后把本地 Writer Handle 置零。

块写入参数：

```c
lv_tsfile_write_block_f64(writer, ts, data, nrows, ncols);
```

`ts` 有 `nrows` 个 I64 时间戳。`data` 有 `nrows * ncols` 个元素，采用
row-major 布局：

```text
data[row * ncols + column]
```

例如 3 行、2 通道的扁平数组为
`[r0c0, r0c1, r1c0, r1c1, r2c0, r2c1]`。Wrapper 内部把它拆分为列数据，
缓存 Tablet 和暂存区；相同或更小的后续批次不会重新创建 Tablet，只有更大
批次才扩容。建议从每批 1,000～10,000 行开始，用真实采样数据调优。

## 4. 单线程异步关闭

异步开关只影响 close。块写入和显式 flush 仍然同步执行，并继续由同一个
LabVIEW 写入循环串行调用。

异步提交接口：

```c
lv_tsfile_writer_close_ex(writer, async_close, &close_task);
```

CLFN 参数配置：

| 参数 | LabVIEW 配置 |
|---|---|
| 返回值 | Signed 32-bit Integer，by value |
| `writer` | Unsigned 64-bit Integer，by value |
| `async_close` | Signed 32-bit Integer，by value；只能为 0 或 1 |
| `out_close_task` | Unsigned 64-bit Integer，Pointer to Value |

`async_close=0` 时函数同步关闭，`close_task` 返回零。`async_close=1` 时
Writer Handle 立即失效，函数返回非零 Close Task Handle，文件由进程内唯一
的后台 close 线程收尾。调用返回后必须立即把 Writer shift register 置零，
不能再对它执行 write 或 flush。

等待接口：

```c
status = lv_tsfile_close_task_wait(close_task);
```

`close_task` 配置为 U64 by value。该调用不检查或接受等待时间，会一直阻塞到
文件关闭完成；返回值是底层 close 的最终状态。返回后 Close Task 已被消费，
必须把对应 shift register 置零，不能重复等待。

整个进程最多有一个后台 close 线程。如果提交新异步 close 时上一次仍未
结束，新提交会同步等待上一条线程结束，再启动本次 close，不会继续增加
线程。推荐轮转流程：

```text
写当前文件
    |
close_ex(writer, 1, &new_task) ──> Writer 立即置零
    |
open 下一文件并继续写
    |
下一次轮转前 wait(previous_task) ──> 检查状态，Task 置零
```

程序最终停止或卸载 DLL 前必须等待最后一个 Close Task。显式 wait 应放在
非 DAQ 定时循环中；库退出时的内部 join 只是安全网，不能替代错误检查。

## 5. 批量读取

首先创建 batch-mode ResultSet：

```c
lv_tsfile_query_table_batch(reader, table, "x\ny\nz",
                            start_time, end_time, batch_rows, &result_set);
```

列字符串用实际 LF 字节分隔，不包含时间戳列。然后预分配：

- `out_ts`：至少 `capacity_rows` 个 I64；
- `out_data`：至少 `capacity_rows * ncols` 个对应数值；
- `out_is_null`：可选，至少 `capacity_rows * ncols` 个 U8；
- `out_rows`：一个 I32 输出值。

读取 DOUBLE 示例：

```c
lv_tsfile_rs_read_block_f64(result_set, out_ts, out_data, out_is_null,
                            capacity_rows, ncols, &out_rows);
```

`out_data` 同样是 row-major。`out_is_null[index] == 1` 表示 null，此时
`out_data[index]` 确定性写为零；不关心 null 时可传空指针。返回状态为 0 且
`out_rows == 0` 表示 EOF，不是错误。

`capacity_rows` 必须大于等于创建查询时的 `batch_rows`。容量不足会在获取
下一 TsBlock 之前返回错误，因此修正容量后仍能读取原来的下一批。不要在
batch ResultSet 上调用 `lv_tsfile_rs_next`/标量 Getter，也不要在 row-mode
ResultSet 上调用块读取函数。批量路径仅支持所选值列全部为同一种 i32、f32
或 f64；混合类型查询使用旧逐行接口。

读取结束后依次调用 `lv_tsfile_rs_free` 和 `lv_tsfile_reader_close`。

## 6. 为什么通道不多，close 仍可能阻塞

通道数只影响工作量的一部分。`writer_close` 还可能执行：

- 把尚未达到内存阈值的 Page/Chunk 编码并写出；
- 汇总 Chunk/ChunkGroup 元数据；
- 构建或序列化文件索引、统计信息、Bloom Filter 和 footer；
- 触发运行库及文件系统写入，耗时会受磁盘、杀毒软件和系统缓存影响。

因此，即使设备和通道较少，只要单文件累计时间长、页/Chunk 多，或大量数据
仍留在内存，close 都可能出现数百毫秒尾延迟。显式 flush 可把数据页落盘与
最终 close 分开观察，但 flush 本身也是同步调用，而且 close 仍需写最终索引。

采集程序使用单一 Writer Owner：

```text
DAQ 采集循环 ──> 有界 Queue/FIFO ──> TsFile 写入循环
                                      ├─ block write
                                      ├─ 可控 flush
                                      └─ 文件轮转与 close
```

DAQ 循环只负责采集和入队，不能直接 close。Queue 必须有明确容量、溢出策略
和水位监控；Writer Handle 只在写入循环使用，避免并发 write/flush/close。

## 7. 性能测量

先执行小规模自检：

```bash
python3 cpp/src/labview_wrapper/benchmark_block.py --smoke
python3 cpp/src/labview_wrapper/benchmark_block.py --smoke --async-close
python3 cpp/src/labview_wrapper/benchmark_read_block.py --smoke
```

正式对比：

```bash
python3 cpp/src/labview_wrapper/benchmark_block.py \
  --rows 10000 --cols 9 --batches 30 --repeats 3
python3 cpp/src/labview_wrapper/benchmark_read_block.py \
  --rows 150000 --cols 9 --batch-rows 10000 --repeats 3
```

写基准分别报告宿主数组准备、block write、flush 和 close；读基准分别报告
旧逐行耗时、批量 DLL 解码/复制调用耗时和宿主遍历耗时。基准会校验实际生效
的 Codec，并验证逐行与批量结果一致。Codec 选择必须用真实设备数据比较：
合成正弦数据、噪声数据与现场振动波形的压缩率和 CPU 开销可能完全不同。

写基准加 `--async-close` 后会把 close 拆成提交耗时、阻塞等待耗时和总耗时，
用于观察关闭成本转移，不使用固定耗时阈值判断正确性。

## 8. 文件与错误处理

- 输出目录必须存在；Writer 使用新文件路径，轮转时生成唯一文件名。
- Handle 为 0 表示无效。成功 close/free 后立即把 LabVIEW Shift Register
  中的 Handle 清零。
- 输出 Handle 和 `out_rows` 在失败路径保持为零，不能继续使用旧值。
- 同一 Writer、Reader 或 ResultSet Handle 的调用由调用方串行化。
- 出错时先停止继续写入，记录状态码；仍持有有效 Writer 时在写入线程中执行
  清理，不能让 C++ 异常或资源所有权越过 CLFN 边界。
