# E3-1：写入内存模型验证

## 状态

- **E3-1b（主实验）**：✅ 已完成，数据在 `../../write_memory/`
- **E3-1a（无 flush 基线）**：TODO，需新增程序 `no_flush_bench`

---

## E3-1a：无 flush 基线（动机实验）

**产出**：F3-0（无 flush 时内存单调增长曲线）

**目的**：作为对照，直观说明"不做内存管理就会爆"，与 F3-1/F3-2 形成 before/after 对比。

### 编译配置

**C5**（ENABLE_MEM_STAT=ON，需要 ModStat 追踪各模块内存）

### 程序：`no_flush_bench`

与现有 `write_memory.cpp` 逻辑基本相同，唯一区别：

- `memory_threshold` 设为极大值（如 `UINT64_MAX`），关闭自动 flush
- 不手动调用 `flush()`
- 内存超过某个上限（如 2 GB）时提前终止，避免 OOM

### 实验配置

与 E3-1b 完全一致（W0 基线：10 设备，200M 行，SNAPPY，batch_size=65536），仅 flush 行为不同。

### 预期现象

$M_{\text{data}}$ 和 $M_{\text{meta}}$ 均单调递增，总内存持续攀升不回落。与 F3-1（有 flush 的锯齿形曲线）并排展示，对比鲜明。

### 输出

- `no_flush_stats.csv`：格式与 `write_memory_stats.csv` 相同
- `plot_no_flush.py`（或复用 `../../write_memory/plot_memory.py`）

---

## E3-1b：有 flush 写入（主实验）

已完成，见 `../../write_memory/`：
- 程序：`write_memory.cpp`，`memory_threshold=50MB`
- 产出：F3-1（顺序模式）、F3-2（混合模式）、T3-1~T3-4
