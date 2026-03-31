# TsFile C++ 架构与逻辑报告

> 最终版 (final branch) — 合并自 experiment (写入优化)、tablet_refine (Tablet 写入优化)、read_opt (读取优化)

---

## 1. 项目概览

TsFile C++ 是 Apache TsFile 时序数据文件格式的 C++ 实现，提供高性能的时序数据读写能力。

```
代码规模：157 个头文件 + 54 个源文件 = 211 个文件
```

### 1.1 目录结构

```
src/
├── common/                     # 公共基础设施
│   ├── allocator/              # 内存管理 (ByteStream, PageArena, MyString)
│   ├── cache/                  # LRU 缓存
│   ├── config/                 # 全局配置
│   ├── constant/               # TsFile 常量定义
│   ├── container/              # 容器 (BitMap, Array, HashTable, List)
│   ├── datatype/               # 日期转换等数据类型工具
│   ├── logger/                 # 日志 (elog)
│   ├── mutex/                  # 互斥锁封装
│   └── tsblock/                # TsBlock 列式数据块
│       └── vector/             # 列向量 (FixedLengthVector, VariableLengthVector)
├── compress/                   # 压缩 (LZ4, Snappy, LZO, Uncompressed)
├── cwrapper/                   # C 语言封装层
├── encoding/                   # 编码 (Plain, RLE, TS2Diff, Gorilla, Sprintz, Dictionary, ZigZag)
├── file/                       # 文件 I/O (ReadFile, WriteFile, TsFileIOReader/Writer)
├── parser/                     # 路径解析 (ANTLR4 生成)
├── reader/                     # 读取引擎
│   ├── block/                  # TsBlock 读取器
│   ├── filter/                 # 时间/值过滤器
│   └── task/                   # 查询任务
├── utils/                      # 工具函数
└── writer/                     # 写入引擎
```

---

## 2. 核心数据结构

### 2.1 Tablet — 写入数据容器

```
Tablet 是用户向 TsFile 写入数据的主要接口。

内存布局:
┌──────────────┐
│ timestamps_  │ → int64_t[] 时间戳数组
├──────────────┤
│ value_matrix_│ → ValueMatrixEntry[] 列值数组
│   ├─ bool*   │   (每列一个 entry，类型由 union 区分)
│   ├─ int32*  │
│   ├─ int64*  │
│   ├─ float*  │
│   ├─ double* │
│   └─ StringColumn*  ← [优化] Arrow 风格连续字符串存储
├──────────────┤
│ bitmaps_     │ → BitMap[] 空值位图 (bit=1 表示 null)
└──────────────┘
```

**StringColumn 优化** (来自 experiment 分支):
- 替代原来的 `String*` 数组 (每个字符串独立分配)
- 采用 Arrow 风格的 `offsets[] + buffer` 连续存储
- `string[i] = buffer + offsets[i]`, `len = offsets[i+1] - offsets[i]`
- 支持 `append()` 自动扩容，`reset()` 零拷贝重置
- 支持 `set_column_string_repeated()` 批量填充相同字符串

**批量写入 API**:
- `set_timestamps(ptr, count)` — 批量拷贝时间戳
- `set_column_values(idx, data, bitmap, count)` — 批量拷贝列数据
- `set_column_string_repeated(idx, str, len, count)` — 批量填充字符串列

### 2.2 TsBlock — 读取数据容器

```
TsBlock 是列式内存数据块，用于读取路径返回数据。

┌─────────────┐
│ TupleDesc   │ → 列描述 (列名 + 数据类型)
├─────────────┤
│ Columns[]   │ → FixedLengthVector / VariableLengthVector
│   └─ data   │   每列存储对应类型的数组
│   └─ nulls  │   空值位图
├─────────────┤
│ row_count   │ → 当前行数
│ capacity    │ → 最大容量
└─────────────┘

ColAppender → 向列追加数据
RowAppender → 管理行提交
ColIterator → 读取列数据
```

### 2.3 ByteStream — 分页式字节流

```
ByteStream 是一个链式分页缓冲区，用于编码和 I/O。

┌──────┐   ┌──────┐   ┌──────┐
│ Page │──→│ Page │──→│ Page │──→ NULL
│ buf  │   │ buf  │   │ buf  │
└──────┘   └──────┘   └──────┘
  ↑ head                  ↑ tail

特性:
- OptionalAtomic<uint32_t> total_size_ (可选原子操作，支持并发写入)
- 支持 wrap_from() 零拷贝包装外部缓冲区
- page_mask_ 位运算取模优化 (代替 %)
- AllocModID 追踪内存分配模块
```

### 2.4 BitMap — 空值位图

```
BitMap 用于标记 null 值 (bit=1 表示 null)。

[优化] 来自 read_opt 分支:
- count_set_bits(): 使用 __builtin_popcount 快速统计 null 数量
- next_set_bit(from, total): 使用 __builtin_ctz 快速跳过非 null 字节
  → 用于读取时跳过 null 行，时间复杂度与 null 字节数成正比
```

---

## 3. 写入路径 (Write Path)

### 3.1 整体架构

```
用户接口                    写入引擎                   文件格式
┌──────────┐           ┌───────────────┐          ┌──────────────┐
│ Tablet / │  write_   │ TsFileWriter  │  flush   │ TsFileIO     │
│ TsRecord │──tablet──→│               │─────────→│ Writer       │
└──────────┘  write_   │  ThreadPool   │          │              │
              table    │  (6 threads)  │          │ WriteFile    │
                       └───────┬───────┘          └──────────────┘
                               │
                 ┌─────────────┼─────────────┐
                 ↓             ↓             ↓
          ┌─────────────┐ ┌────────────┐ ┌──────────────┐
          │ ChunkWriter │ │ TimeChunk  │ │ ValueChunk   │
          │ (non-align) │ │ Writer     │ │ Writer       │
          └──────┬──────┘ └─────┬──────┘ └──────┬───────┘
                 ↓              ↓               ↓
          ┌─────────────┐ ┌────────────┐ ┌──────────────┐
          │ PageWriter  │ │ TimePage   │ │ ValuePage    │
          │             │ │ Writer     │ │ Writer       │
          └─────────────┘ └────────────┘ └──────────────┘
```

### 3.2 写入模式

**三种写入模式**:

| 模式 | 方法 | Chunk 类型 | 说明 |
|------|------|-----------|------|
| Tree 非对齐 | `write_tree()` / `write_tablet()` | ChunkWriter | 每个 measurement 独立时间戳 |
| Tree 对齐 | `write_tablet_aligned()` | TimeChunkWriter + ValueChunkWriter | 共享时间戳列 |
| Table 模型 | `write_table()` | TimeChunkWriter + ValueChunkWriter | 按 TAG 列自动分组设备 |

**Table 写入流程**:
1. `split_tablet_by_device()` — 根据 TAG 列 (ID columns) 找到设备边界
2. `find_all_device_boundaries()` — 使用 uint64 位图 + `__builtin_ctzll` 快速扫描边界
3. 对每个设备子区间，调用 `time_write_column_batch()` + `value_write_column_batch()`

### 3.3 批量写入优化 (experiment 分支)

```
write_tablet() / write_table()
    │
    ├─ [快速路径] 无 null → write_column_batch()
    │   └─ chunk_writer->write_batch(timestamps, values, count)
    │       └─ page_writer_.write_batch(timestamps, values, count)
    │           └─ 直接 memcpy 或批量编码
    │
    └─ [慢速路径] 有 null → write_column() (逐行)
        └─ for each row: chunk_writer->write(timestamp, value, is_null)

value_write_column_batch():
    └─ value_chunk_writer->write_batch(timestamps, values, bitmap, start, count)
        └─ 支持所有基本类型的批量写入
        └─ STRING/TEXT/BLOB 回退到逐行
```

### 3.4 ThreadPool (experiment 分支)

```cpp
// 写入路径使用通用线程池 (6 线程)
common::ThreadPool thread_pool_{6};

// 支持 std::future 获取结果
auto future = thread_pool_.submit([&] { return do_work(); });
auto result = future.get();
```

---

## 4. 读取路径 (Read Path)

### 4.1 整体架构

```
用户接口                     读取引擎                    文件格式
┌──────────────┐        ┌─────────────────┐        ┌───────────────┐
│ TsFileReader │───────→│ QueryExecutor   │───────→│ TsFileIO      │
│              │  query │ QueryDataSet    │  read  │ Reader        │
└──────────────┘        └────────┬────────┘        │               │
                                 │                 │ ReadFile      │
               ┌─────────────────┤                 └───────────────┘
               ↓                 ↓
      ┌─────────────────┐  ┌───────────────────┐
      │ TsBlockReader   │  │ QdsWithTime       │
      │ (Table 模型)    │  │ Generator         │
      │                 │  │ (Tree 模型)       │
      └────────┬────────┘  └───────────────────┘
               ↓
      ┌──────────────────────────┐
      │ SingleDeviceTsBlockReader│
      │ ├─ MeasurementColumnCtx  │
      │ │  ├─ SingleMeasurement  │ → 单列
      │ │  └─ VectorMeasurement  │ → 多列 (Aligned)
      │ └─ IdColumnCtx           │ → TAG 列
      └──────────┬───────────────┘
                 ↓
      ┌─────────────────────────┐
      │ TsFileSeriesScanIterator│ ← 时间序列扫描迭代器
      │ ├─ ChunkReader          │    (非对齐)
      │ └─ AlignedChunkReader   │    (对齐)
      └─────────────────────────┘
```

### 4.2 读取优化 (read_opt 分支)

#### 4.2.1 Aligned 快速路径

```
当所有查询列都来自 aligned chunk group 时:

has_next() → has_next_aligned()
    │
    └─ 避免逐行 merge-sort
    └─ 批量 bulk_copy_into() 直接拷贝 TsBlock 数据
    └─ 利用 available_rows() 计算可批量拷贝的行数
```

#### 4.2.2 行级 Offset/Limit 下推

```
TsFileSeriesScanIterator:
    set_row_range(offset, limit)
        │
        ├─ should_skip_chunk_by_offset(cm)
        │   └─ 利用 chunk 统计信息 (count) 跳过整个 chunk
        │
        └─ get_next_page() 6-param overload
            └─ row_offset, row_limit 传入 AlignedChunkReader
            └─ 利用 page 统计信息跳过整个 page
```

#### 4.2.3 多值列合并读取

```
AlignedChunkReader:
    load_by_aligned_meta_multi(time_meta, value_metas[])
        │
        └─ 一次加载 time chunk + N 个 value chunk
        └─ get_next_page_multi() → 批量解码多列
        └─ 避免 N 次独立 chunk 加载开销
```

#### 4.2.4 DecodeThreadPool

```cpp
// 读取路径使用轻量级解码线程池
class DecodeThreadPool {
    submit(task);   // 提交解码任务
    wait_all();     // 等待所有解码完成
};
```

---

## 5. 编码子系统

### 5.1 编码器层次

```
Encoder (base)
├── PlainEncoder           # 无编码，直接存储
├── RleEncoder             # Run-Length Encoding
├── Ts2DiffEncoder         # 时间序列二阶差分 + zigzag
├── GorillaEncoder         # Gorilla 压缩 (浮点数 XOR)
├── SprintzEncoder         # Sprintz 编码
│   ├── Int32SprintzEncoder
│   ├── Int64SprintzEncoder
│   ├── FloatSprintzEncoder
│   └── DoubleSprintzEncoder
├── DictionaryEncoder      # 字典编码
└── ZigZagEncoder          # ZigZag 变长编码

Decoder (对称结构)
├── PlainDecoder
├── RleDecoder
├── Ts2DiffDecoder
├── GorillaDecoder
├── SprintzDecoder
├── DictionaryDecoder
└── ZigZagDecoder
```

### 5.2 批量编码支持

```
PlainEncoder:
    write_batch(values, count)  → 直接 memcpy

其他编码器:
    支持 write_batch() → 内部循环调用 encode()
```

### 5.3 SIMD 支持

通过 `simde` (SIMDe) 库提供跨平台 SIMD 支持，用于编码/解码加速。

---

## 6. 压缩子系统

```
Compressor (base)
├── UncompressedCompressor  # 无压缩
├── LZ4Compressor           # LZ4 压缩
├── SnappyCompressor        # Snappy 压缩
└── LZOCompressor           # LZO 压缩
```

---

## 7. 内存管理

### 7.1 分配器体系

```
AllocBase (g_base_allocator)
    │
    ├─ mem_alloc(size, mod_id)    # 带模块标记的分配
    ├─ mem_free(ptr)              # 释放
    ├─ mem_realloc(ptr, size)     # 重分配
    │
    └─ PageArena                  # 页面级内存池
        ├─ alloc(size)            # 从当前页分配
        └─ reset()               # 整体回收 (不逐个释放)

AllocModID (内存模块标记):
    MOD_DEFAULT, MOD_TABLET, MOD_CW_PAGES_DATA,
    MOD_TSFILE_WRITER, ...
```

### 7.2 ByteStream 内存追踪

```
ByteStream(AllocModID mid)
    └─ get_mid() → 传播给内部分配
    └─ SerializationUtil 使用 mid 进行所有中间分配
```

---

## 8. 文件格式

### 8.1 TsFile 结构

```
┌───────────────────┐
│ Magic ("TsFile")  │ 6 bytes
├───────────────────┤
│ Chunk Group 1     │ ← 一个设备的数据
│   ├─ Chunk 1      │   ← 一个 measurement 的数据
│   │  ├─ Header    │
│   │  ├─ Page 1    │   ← 一个数据页
│   │  ├─ Page 2    │
│   │  └─ ...       │
│   ├─ Chunk 2      │
│   └─ ...          │
├───────────────────┤
│ Chunk Group 2     │
├───────────────────┤
│ ...               │
├───────────────────┤
│ Index Area        │ ← 时间序列索引
│   ├─ MetaData     │
│   ├─ BloomFilter  │
│   └─ IndexTree    │
├───────────────────┤
│ Footer            │
│   └─ Magic        │
└───────────────────┘
```

### 8.2 Aligned Chunk Group

```
对齐写入时，一个设备的所有 measurement 共享时间戳:

Aligned Chunk Group:
├── TimeChunk          → 时间戳列 (只存一份)
│   ├── TimePage 1
│   └── TimePage 2
├── ValueChunk (col1)  → 值列1
│   ├── ValuePage 1
│   └── ValuePage 2
└── ValueChunk (col2)  → 值列2
    ├── ValuePage 1
    └── ValuePage 2
```

---

## 9. Final 分支合并内容总结

| 来源分支 | 主要变更 |
|---------|---------|
| **experiment** | Tablet StringColumn 优化、批量写入 API (set_timestamps, set_column_values, set_column_string_repeated)、write_column_batch/time_write_column_batch/value_write_column_batch 快速路径、ThreadPool (6 线程写入并发)、ByteStream 构造器参数化 mid、SerializationUtil 使用 mem_alloc 替代 malloc |
| **tablet_refine** | Tablet 写入优化 (已被 experiment 完全覆盖) |
| **read_opt** | BitMap count_set_bits/next_set_bit 优化、AlignedChunkReader 6-param get_next_page (offset/limit 下推)、aligned fast path (has_next_aligned + bulk_copy_into)、多值列合并读取 (load_by_aligned_meta_multi)、DecodeThreadPool、TsFileSeriesScanIterator row_range 支持 |

---

## 10. 关键数据流

### 写入流程 (Table 模型)

```
1. 用户创建 Tablet，调用 set_timestamps() + set_column_values()
2. TsFileWriter::write_table(tablet)
3. split_tablet_by_device() → 按 TAG 列分组
4. 对每个设备:
   a. do_check_schema_table() → 获取/创建 ChunkWriter
   b. time_write_column_batch() → 批量写入时间戳
   c. value_write_column_batch() → 批量写入各列值
5. check_memory_size_and_may_flush_chunks() → 内存超限则 flush
6. flush() → TsFileIOWriter 写入磁盘
7. close() → 写入索引和 footer
```

### 读取流程 (Table 模型)

```
1. TsFileReader::query(columns, filter)
2. QueryExecutor → SingleDeviceTsBlockReader
3. 对每个 measurement:
   a. 创建 MeasurementColumnContext
   b. 初始化 TsFileSeriesScanIterator
   c. 加载 TimeseriesIndex
4. has_next():
   a. [aligned fast path] → bulk_copy_into() 批量拷贝
   b. [merge path] → 逐行合并多列
5. next() → 返回 TsBlock
6. 用户遍历 TsBlock 的 ColIterator
```
