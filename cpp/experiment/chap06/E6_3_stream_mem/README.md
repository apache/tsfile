# E6-3：流式迭代内存控制

## 状态：TODO

**产出**：F6-1（内存峰值 vs 文件大小折线图）

## 方法

Python `tsfile.to_dataframe(as_iterator=True)`，监测进程 RSS 峰值。

## 程序：`stream_mem_bench`（C++）或 `bench_stream.py`（Python）

优先用 Python 脚本（`/proc/self/status` VmPeak 或 `resource.getrusage`），避免单独写 C++ 程序。若 Python binding 未就绪则用 C++ 实现同等逻辑。

### 编译配置

**C4**（生产配置）

## 实验参数矩阵

| 变量 | 取值 |
|------|------|
| 文件大小 | ~100 MB / ~1 GB / ~10 GB |
| batch_size（W5） | 4,096 / 16,384 / 65,536 |

### 数据文件估算（8 FIELD INT64 SNAPPY）

| 目标大小 | 约需行数 | 说明 |
|---------|---------|------|
| 100 MB | ~5M 行 | SSD 上生成 |
| 1 GB | ~50M 行 | SSD 上生成 |
| 10 GB | ~500M 行 | 需挂载 HDD（`/dev/sda`），提前生成 |

## 输出

- `stream_mem_results.csv`：`file_size_mb, batch_size, peak_rss_mb`
- `plot_stream_mem.py`：x = 文件大小（对数轴），y = 内存峰值，每条线一个 batch_size（预期为近水平线）

## 预期结论

内存峰值仅由 batch_size 决定，与文件大小无关（水平线）：每次只在内存中保持 batch_size 行的数据，老数据由 GC 或手动释放。
