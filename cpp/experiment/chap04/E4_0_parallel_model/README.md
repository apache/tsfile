# E4-0：并行读写机制设计说明

本文详细说明 TsFile C++ 实现中写入路径与读取路径的列级并行机制，
作为 E4-1 / E4-2 实验的理论背景。

---

## 1. 总体结构

```
┌─────────────────────────────────────────────────────────────┐
│                     common/thread_pool.h                     │
│                      ThreadPool (共享基础设施)                │
│  workers_[N]  ──  tasks_queue  ──  cv_work_ / cv_done_      │
│  submit<F>() → std::future<R>     wait_all() → 阻塞至全完   │
└──────────────────────┬──────────────────────────────────────┘
                       │  写路径持有                读路径借用
          ┌────────────┴─────────────┐
          ▼                          ▼
 writer/tsfile_writer.h      reader/tsfile_series_scan_iterator.h
 TsFileWriter::thread_pool_  TsFileSeriesScanIterator::decode_pool_
 (成员，构造时创建)           (new/delete per SSI，传给 AlignedChunkReader)
```

配置入口（`common/config/config.h` → `common/global.cc`）：

| 参数 | 默认值 | 用途 |
|------|--------|------|
| `write_thread_count_`    | 6 | 写线程池大小（构造时固定） |
| `parallel_write_enabled_` | true | 写路径并行开关 |
| `read_thread_count_`     | 4 | 读线程池上限 |
| `parallel_read_enabled_`  | true | 读路径并行开关 |

编译开关：`-DENABLE_THREADS=ON`（所有并行代码均在 `#ifdef ENABLE_THREADS` 内）。

---

## 2. 公共组件：ThreadPool

```
class ThreadPool {
    workers_[N]          // N 个 std::thread，构造时启动，析构时 join
    tasks_               // std::queue<std::function<void()>>
    mu_                  // 单把锁，保护 tasks_ 和 active_
    cv_work_             // 有任务 → 唤醒 worker
    cv_done_             // active_==0 → 唤醒 wait_all() 调用方
    active_              // 已提交未完成的任务计数
}
```

两种提交接口：

```
// fire-and-forget（读路径使用）
void submit(std::function<void()> task);
void wait_all();                    // 阻塞直到 active_==0

// future 返回（写路径使用）
template<F>
std::future<R> submit(F&& f);       // 返回 packaged_task 的 future
// 调用方逐个 .get() 收集结果
```

线程安全保证：worker_loop 持锁取任务 → 释放锁 → 无锁执行任务 → 持锁递减 active_。

---

## 3. 写入路径

### 3.1 调用链

```
用户
  └─ TsFileTableWriter::write_table(tablet)
       └─ TsFileWriter::write_table(tablet)
            └─ split_tablet_by_device(tablet)   // 按 device 分段
                 for each device segment:
                   ├── [Table/Aligned 模型] write_table_aligned_segment()
                   └── [Tree 模型]          write_tree_segment()
```

### 3.2 Table/Aligned 模型的并行编码（`write_table` 内核）

```
                    Tablet (rows[si..ei])
                         │
           ┌─────────────┴──────────────────────────┐
           │ do_check_schema_table()                 │
           │ 获取 time_chunk_writer + value_writers  │
           └─────────────┬──────────────────────────┘
                         │
          ┌──────────────▼──────────────┐
          │  tasks.size() >= 2          │  (列数 < 2 时退化为串行)
          │  && parallel_write_enabled_ │
          └──────────────┬──────────────┘
                         │  YES
         ┌───────────────┴───────────────────────────────┐
         │                thread_pool_.submit(...)        │
         │                                                │
         │  future<int> time_future ◄──────────────────┐ │
         │    └─ time_write_column_batch(              │ │
         │           time_chunk_writer, tablet, si, ei)│ │
         │                                              │ │
         │  future<int> val_future[0] ◄──────────────┐ │ │
         │    └─ value_write_column_batch(           │ │ │
         │           vcw[0], tablet, col[0], si, ei) │ │ │
         │  future<int> val_future[1] ◄──────────────┤ │ │
         │    └─ value_write_column_batch(           │ │ │
         │           vcw[1], tablet, col[1], si, ei) │ │ │
         │  ...                                       │ │ │
         │  future<int> val_future[N-1] ◄─────────── ┘ │ │
         │                                              │ │
         └──────────────────────────────────────────────┘ │
                         │                                 │
              time_future.get()  ◄────────────────────────┘
              val_future[0].get()
              val_future[1].get()
              ...                  // 逐个收集，任一失败立即返回
```

### 3.3 Tree 模型的并行编码

```
         ┌───────────────────────────────────────────────┐
         │  chunk_writers.size() >= 2                    │
         │  && parallel_write_enabled_                   │
         └──────────────┬────────────────────────────────┘
                        │  YES
         ┌──────────────▼──────────────────────────────┐
         │  for c in 0..chunk_writers.size()-1:         │
         │    futures[c] = thread_pool_.submit(         │
         │      [cw, &tablet, c, si, ei]() {            │
         │          return write_column_batch(          │
         │                   cw, tablet, c, si, ei);   │
         │      });                                     │
         └──────────────┬──────────────────────────────┘
                        │
              futures[0].get()
              futures[1].get()
              ...
```

### 3.4 锁自由保证

并行任务之间**零共享状态**：

```
Tablet（只读）
   │
   ├── timestamps_[i]       ← 所有列共用，只读
   ├── value_matrix_[0]     ← 列 0 数据，只读
   ├── value_matrix_[1]     ← 列 1 数据，只读
   └── ...

ChunkWriter / ValueChunkWriter（每列独占）
   ├── encoder_             ← per-column，无共享
   ├── statistic_           ← per-column，无共享
   ├── data_buffer_         ← per-column ByteStream，无共享
   └── page_writer_         ← per-column，无共享
```

因此，`write_column_batch()` / `value_write_column_batch()` 可以完全并发，无需任何锁。

### 3.5 线程池生命周期

```
TsFileWriter 构造
  └─ thread_pool_{ g_config_value_.write_thread_count_ }
        └─ 启动 N 个 worker 线程，等待任务

       (每次 write_table 调用) ───→ submit × (1+N_cols)
                               ───→ .get() 收集

TsFileWriter 析构
  └─ ThreadPool::~ThreadPool()
        └─ stop_=true → notify_all → join 所有 worker
```

> **注意**：线程池大小在 TsFileWriter **构造时**从 `write_thread_count_` 读取并固定。
> 若要改变线程数，必须销毁并重建 TsFileWriter。

---

## 4. 读取路径

### 4.1 调用链

```
用户
  └─ TsFileReader::query(...)
       └─ TsFileSeriesScanIterator (SSI)
            └─ AlignedChunkReader (ACR)
                 └─ decode_cur_value_pages_multi()
```

### 4.2 SSI 初始化：线程池创建

```
TsFileSeriesScanIterator::init_chunk_reader()
  │
  ├── num_cols = itimeseries_index_->get_value_column_count()
  │
  ├── [#ifdef ENABLE_THREADS]
  │   if num_cols > 1 && parallel_read_enabled_:
  │     nthreads = min(num_cols, read_thread_count_)
  │     decode_pool_ = new ThreadPool(nthreads)   // SSI 拥有
  │     acr->set_decode_pool(decode_pool_)        // ACR 借用，不拥有
  │
  └── 初始化时间游标 / chunk meta 游标
```

### 4.3 两阶段并行解码（`decode_cur_value_pages_multi`）

```
每次读取一个 Page 时：

Phase 1：串行 IO（必须串行——文件读取/缓冲区管理有状态）
──────────────────────────────────────────────────────────────
  for c in 0..N-1:
    ensure_value_page_loaded(*value_columns_[c])
      └─ 检查 page 是否已在内存 → 按需从文件读入 chunk buffer

Phase 2：并行 CPU（解压 + bitmap 解析 + decoder 重置）
──────────────────────────────────────────────────────────────
  if N > 1 && decode_pool_ != nullptr:

    ┌──────── decode_pool_.submit() × N ─────────────────────┐
    │  col[0]: decompress_and_parse_value_page(col[0])        │
    │  col[1]: decompress_and_parse_value_page(col[1])        │
    │  ...                                                    │
    │  col[N-1]: decompress_and_parse_value_page(col[N-1])   │
    └─────────────────────────────────────────────────────────┘
              │
    decode_pool_.wait_all()    // 阻塞直到所有列解码完成
              │
    for c: if col_rets[c] != OK → return error
              │
              ▼
         调用方继续 merge time + values → TsBlock
```

### 4.4 decompress_and_parse_value_page 的工作内容

```
per-column ValueColumnState（各列独占，无共享）：
  ┌──────────────────────────────────────────────────┐
  │  in_stream          ← 压缩数据 ByteStream        │
  │  compressor_        ← 解压器（每列独立实例）     │
  │  data_buffer_       ← 解压后输出 buffer          │
  │  bitmap_            ← null bitmap 解析结果       │
  │  decoder_           ← 值解码器（每列独立实例）   │
  └──────────────────────────────────────────────────┘

decompress_and_parse_value_page(col):
  1. compressor_->uncompress(in_stream → data_buffer_)
  2. parse null bitmap from data_buffer_
  3. decoder_->reset(data_buffer_)   // 准备好，供 next_batch() 使用
```

### 4.5 线程池生命周期（读路径）

```
SSI 构造 / init_chunk_reader()
  └─ decode_pool_ = new ThreadPool(nthreads)

       (每次读取一个 chunk 的 page) ──→ submit × N_cols
                                  ──→ wait_all()

SSI 析构
  └─ delete decode_pool_   // stop + join
```

与写路径的区别：读线程池在 SSI 层按需创建，每个 SSI 独立持有，
池大小 = `min(value_column_count, read_thread_count_)`，
**不超过实际列数**（避免空闲线程开销）。

---

## 5. 写路径 vs 读路径对比

```
┌────────────────────┬─────────────────────────────┬──────────────────────────────────┐
│ 维度               │ 写路径                       │ 读路径                           │
├────────────────────┼─────────────────────────────┼──────────────────────────────────┤
│ 并行粒度           │ 列（ChunkWriter）            │ 列（ValueColumnState）           │
│ 并行内容           │ 编码 + Page 打包             │ 解压 + bitmap解析 + decoder重置  │
│ 串行部分           │ 无（全列并发）               │ Phase 1 IO（顺序读 page 数据）   │
│ 线程池持有者       │ TsFileWriter（成员变量）     │ TsFileSeriesScanIterator（new/delete）│
│ 线程池生命周期     │ 与 TsFileWriter 相同         │ 与 SSI 相同（每次查询创建）      │
│ 线程数配置         │ write_thread_count_（固定）  │ min(cols, read_thread_count_)    │
│ 等待机制           │ future.get() 逐列收集         │ wait_all() 统一等待              │
│ 错误传播           │ 第一个失败立即 return         │ wait_all 后统一检查 col_rets     │
│ 锁                 │ 无（任务间零共享）           │ 无（每列 ValueColumnState 独占） │
└────────────────────┴─────────────────────────────┴──────────────────────────────────┘
```

---

## 6. Amdahl 定律分析

设 α 为串行占比（不可并行化的部分）：

```
Speedup(n) = 1 / (α + (1-α)/n)
```

**写路径**：理论上 α 极小（仅 `split_tablet_by_device` + `do_check_schema` 等准备开销），
主要编码时间全部可并行。预期 α < 0.05，8 线程加速比可达 5–7×（列数足够多时）。

**读路径**：Phase 1（串行 IO）构成硬性串行下界，α 取决于 IO 时间占总时间比例。
SSD 上 IO 快，α 小，并行收益大；HDD 上 IO 慢，α 大，并行收益有限。

```
写路径（α ≈ 0.05）：
  n=1: 1.00×   n=2: 1.90×   n=4: 3.48×   n=8: 5.93×

读路径（α ≈ 0.20，SSD, 压缩密集型）：
  n=1: 1.00×   n=2: 1.67×   n=4: 2.50×   n=8: 3.33×

读路径（α ≈ 0.50，压缩轻量型）：
  n=1: 1.00×   n=2: 1.33×   n=4: 1.60×   n=8: 1.78×
```

实测 α 由 E4-3 实验从 `throughput_1t` 与 `throughput_8t` 反推。

---

## 7. 关键源文件索引

| 文件 | 作用 |
|------|------|
| `src/common/thread_pool.h` | ThreadPool 实现（写路径和读路径共用） |
| `src/common/config/config.h` | ConfigValue 结构体（四个并行参数） |
| `src/common/global.cc` | 参数默认值（write=6, read=4） |
| `src/writer/tsfile_writer.h` | TsFileWriter 成员 `thread_pool_`（L198） |
| `src/writer/tsfile_writer.cc` | 并行 write_table（L931–1016） |
| `src/reader/tsfile_series_scan_iterator.cc` | decode_pool_ 创建（L218–222） |
| `src/reader/aligned_chunk_reader.cc` | 两阶段并行解码（L1571–1601） |
